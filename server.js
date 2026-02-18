const path = require("path");
const fs = require("fs/promises");
const express = require("express");
const session = require("express-session");
const bcrypt = require("bcryptjs");
const { Pool } = require("pg");
const pgSession = require("connect-pg-simple")(session);

const app = express();
const PORT = process.env.PORT || 3000;
const isProd = process.env.NODE_ENV === "production";
const PARTNERS_API_URL =
  process.env.PARTNERS_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/partner/getpartners";
const PARTNERS_SESSION_API_URL =
  process.env.PARTNERS_SESSION_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/client/getsession";
const PARTNERS_API_AUTH =
  process.env.PARTNERS_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const PARTNER_CLUSTER_MIN = 0;
const PARTNER_CLUSTER_MAX = 15;
const PARTNER_CODES_CACHE_FILE = path.join(__dirname, "data", "partner-codes-cache.json");

if (!process.env.DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Add it to your environment.");
}

if (isProd) {
  // Trust Render's proxy so secure cookies are set correctly.
  app.set("trust proxy", 1);
}

const shouldUseSsl =
  isProd ||
  process.env.DATABASE_SSL === "true" ||
  /render\.com/i.test(process.env.DATABASE_URL || "") ||
  /sslmode=require/i.test(process.env.DATABASE_URL || "");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: shouldUseSsl ? { rejectUnauthorized: false } : undefined
});

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(
  session({
    store: new pgSession({
      pool,
      tableName: "user_sessions",
      createTableIfMissing: true
    }),
    secret: process.env.SESSION_SECRET || "dev-secret-change-me",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: isProd
    }
  })
);

app.use("/public", express.static(path.join(__dirname, "public")));

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      display_name TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS screens (
      id SERIAL PRIMARY KEY,
      key TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_endpoints (
      id SERIAL PRIMARY KEY,
      title TEXT NOT NULL,
      method TEXT NOT NULL,
      path TEXT NOT NULL,
      target_url TEXT,
      description TEXT,
      body TEXT,
      headers TEXT,
      params TEXT,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_targets (
      id SERIAL PRIMARY KEY,
      url TEXT UNIQUE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  // Backward-compatible schema upgrades for existing databases.
  await pool.query(`
    ALTER TABLE api_endpoints
      ADD COLUMN IF NOT EXISTS target_url TEXT,
      ADD COLUMN IF NOT EXISTS description TEXT,
      ADD COLUMN IF NOT EXISTS body TEXT,
      ADD COLUMN IF NOT EXISTS headers TEXT,
      ADD COLUMN IF NOT EXISTS params TEXT,
      ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  // Fill sort order for legacy rows where this value is missing/default.
  await pool.query(`
    WITH ranked AS (
      SELECT id, row_number() OVER (ORDER BY id DESC) AS ord
      FROM api_endpoints
    )
    UPDATE api_endpoints AS target
    SET sort_order = ranked.ord
    FROM ranked
    WHERE target.id = ranked.id
      AND (target.sort_order IS NULL OR target.sort_order = 0)
  `);

  // Migrate legacy endpoint target_url values into shared target list.
  await pool.query(`
    INSERT INTO api_targets (url)
    SELECT DISTINCT trim(target_url)
    FROM api_endpoints
    WHERE target_url IS NOT NULL
      AND trim(target_url) <> ''
    ON CONFLICT (url) DO NOTHING
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_requests (
      id SERIAL PRIMARY KEY,
      endpoint_id INTEGER NOT NULL,
      target_url TEXT NOT NULL,
      method TEXT NOT NULL,
      path TEXT NOT NULL,
      headers TEXT,
      params TEXT,
      body TEXT,
      response_status INTEGER,
      response_text TEXT,
      response_headers TEXT,
      duration_ms INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      FOREIGN KEY (endpoint_id) REFERENCES api_endpoints(id) ON DELETE CASCADE
    )
  `);

  await pool.query(`
    ALTER TABLE api_requests
      ADD COLUMN IF NOT EXISTS target_url TEXT,
      ADD COLUMN IF NOT EXISTS headers TEXT,
      ADD COLUMN IF NOT EXISTS params TEXT,
      ADD COLUMN IF NOT EXISTS body TEXT,
      ADD COLUMN IF NOT EXISTS response_status INTEGER,
      ADD COLUMN IF NOT EXISTS response_text TEXT,
      ADD COLUMN IF NOT EXISTS response_headers TEXT,
      ADD COLUMN IF NOT EXISTS duration_ms INTEGER,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_screen_permissions (
      user_id INTEGER NOT NULL,
      screen_id INTEGER NOT NULL,
      can_view BOOLEAN NOT NULL DEFAULT false,
      PRIMARY KEY (user_id, screen_id),
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (screen_id) REFERENCES screens(id)
    )
  `);

  const userCount = await pool.query("SELECT COUNT(*)::int AS count FROM users");
  if (userCount.rows[0].count === 0) {
    const passwordHash = await bcrypt.hash("admin123", 10);
    await pool.query(
      "INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3)",
      ["admin", passwordHash, "Admin"]
    );
    console.log("Seed user created: admin / admin123");
  }

  const screenCount = await pool.query("SELECT COUNT(*)::int AS count FROM screens");
  if (screenCount.rows[0].count === 0) {
    const defaults = [
      ["overview", "Genel Özet"],
      ["reports", "Raporlar"],
      ["settings", "Ayarlar"]
    ];
    for (const [key, name] of defaults) {
      await pool.query("INSERT INTO screens (key, name) VALUES ($1, $2)", [key, name]);
    }
  }
}

initDb().catch((err) => {
  console.error("DB init error:", err);
});

function requireAuth(req, res, next) {
  if (!req.session.user) {
    if (req.path.startsWith("/api/")) {
      return res.status(401).json({ ok: false, error: "Oturum süresi doldu." });
    }
    return res.redirect("/login");
  }
  next();
}

function normalizeTargetUrl(input) {
  let raw = String(input || "").trim();
  if (!raw) return "";
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    raw = `https://${raw}`;
  }
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (err) {
    return "";
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return "";
  }
  parsed.hash = "";
  const pathname = parsed.pathname && parsed.pathname !== "/" ? parsed.pathname.replace(/\/+$/, "") : "";
  const search = parsed.search || "";
  return `${parsed.origin}${pathname}${search}`;
}

async function ensureTargetsTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_targets (
      id SERIAL PRIMARY KEY,
      url TEXT UNIQUE NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

function extractPartnerCodes(payload) {
  const candidateLists = [];

  if (Array.isArray(payload)) {
    candidateLists.push(payload);
  }

  if (payload && typeof payload === "object") {
    if (Array.isArray(payload.data)) candidateLists.push(payload.data);
    if (Array.isArray(payload.items)) candidateLists.push(payload.items);
    if (Array.isArray(payload.result)) candidateLists.push(payload.result);
    if (Array.isArray(payload.partners)) candidateLists.push(payload.partners);

    if (payload.data && typeof payload.data === "object") {
      if (Array.isArray(payload.data.items)) candidateLists.push(payload.data.items);
      if (Array.isArray(payload.data.partners)) candidateLists.push(payload.data.partners);
      if (Array.isArray(payload.data.result)) candidateLists.push(payload.data.result);
    }
  }

  const rows = candidateLists.find((list) => list.length > 0) || [];
  const uniqueCodes = new Set();

  rows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    if (Number(row.status) !== 1) return;

    const code = String(row.code || "").trim();
    if (!code) return;
    uniqueCodes.add(code);
  });

  return Array.from(uniqueCodes).sort((a, b) => a.localeCompare(b, "tr"));
}

function normalizeTokenName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function findNestedValue(node, keySet) {
  if (node === null || node === undefined) return undefined;
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findNestedValue(item, keySet);
      if (found !== undefined) return found;
    }
    return undefined;
  }

  if (typeof node !== "object") return undefined;

  for (const [key, value] of Object.entries(node)) {
    if (keySet.has(normalizeTokenName(key)) && value !== undefined && value !== null) {
      const normalized = String(value).trim();
      if (normalized) return normalized;
    }
  }

  for (const value of Object.values(node)) {
    const found = findNestedValue(value, keySet);
    if (found !== undefined) return found;
  }

  return undefined;
}

function parseJsonSafe(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

function buildClusterPartnerUrls(baseUrl) {
  const raw = String(baseUrl || "").trim();
  if (!raw) return [];

  if (!/cluster\d+/i.test(raw)) {
    return [raw];
  }

  const urls = [];
  for (let cluster = PARTNER_CLUSTER_MIN; cluster <= PARTNER_CLUSTER_MAX; cluster += 1) {
    urls.push(raw.replace(/cluster\d+/i, `cluster${cluster}`));
  }
  return urls;
}

function extractClusterLabel(url) {
  const match = String(url || "").match(/cluster\d+/i);
  return match ? match[0].toLowerCase() : "cluster";
}

function buildSessionUrlForPartnerUrl(partnerUrl) {
  try {
    const parsed = new URL(String(partnerUrl || ""));
    parsed.pathname = "/api/client/getsession";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return PARTNERS_SESSION_API_URL;
  }
}

function normalizeCodes(codes) {
  const unique = new Set();
  (Array.isArray(codes) ? codes : []).forEach((code) => {
    const normalized = String(code || "").trim();
    if (normalized) unique.add(normalized);
  });
  return Array.from(unique).sort((a, b) => a.localeCompare(b, "tr"));
}

async function loadPartnerCodesCache() {
  try {
    const raw = await fs.readFile(PARTNER_CODES_CACHE_FILE, "utf8");
    const parsed = parseJsonSafe(raw);
    if (!parsed || typeof parsed !== "object") return null;
    const codes = normalizeCodes(parsed.codes);
    if (codes.length === 0) return null;
    return {
      codes,
      updatedAt: String(parsed.updatedAt || "")
    };
  } catch (err) {
    return null;
  }
}

async function savePartnerCodesCache(codes) {
  const normalizedCodes = normalizeCodes(codes);
  if (normalizedCodes.length === 0) return;

  const payload = {
    updatedAt: new Date().toISOString(),
    codes: normalizedCodes
  };

  try {
    await fs.mkdir(path.dirname(PARTNER_CODES_CACHE_FILE), { recursive: true });
    await fs.writeFile(PARTNER_CODES_CACHE_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (err) {
    console.error("Partner cache write error:", err);
  }
}

async function fetchPartnerSessionCredentials(sessionUrl, signal) {
  const payload = {
    type: 1,
    connection: {
      "ip-address": "212.156.219.182",
      port: "5117"
    },
    browser: {
      name: "Chrome"
    }
  };

  const response = await fetch(sessionUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: PARTNERS_API_AUTH
    },
    body: JSON.stringify(payload),
    signal
  });

  const raw = await response.text();
  const parsed = parseJsonSafe(raw);

  if (!response.ok) {
    const reason =
      (parsed && typeof parsed === "object" && String(parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";
    return {
      sessionId: "",
      deviceId: "",
      error: `GetSession HTTP ${response.status}: ${reason}`
    };
  }

  if (!parsed) {
    return {
      sessionId: "",
      deviceId: "",
      error: "GetSession JSON parse edilemedi."
    };
  }

  const sessionId =
    findNestedValue(parsed, new Set(["sessionid"])) ||
    findNestedValue(parsed, new Set(["sessionid", "session"])) ||
    "";
  const deviceId =
    findNestedValue(parsed, new Set(["deviceid"])) ||
    findNestedValue(parsed, new Set(["deviceid", "device"])) ||
    "";

  if (!sessionId || !deviceId) {
    return {
      sessionId,
      deviceId,
      error: "GetSession yanıtında session-id veya device-id bulunamadı."
    };
  }

  return {
    sessionId,
    deviceId,
    error: null
  };
}

async function fetchPartnerCodesFromCluster(partnerUrl, signal) {
  const clusterLabel = extractClusterLabel(partnerUrl);
  const sessionUrl = buildSessionUrlForPartnerUrl(partnerUrl);

  try {
    const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, signal);
    if (sessionResult.error) {
      return { clusterLabel, codes: [], error: `${clusterLabel}: ${sessionResult.error}` };
    }

    const partnerRequestBody = {
      data: "BusTicketProvider",
      "device-session": {
        "session-id": sessionResult.sessionId,
        "device-id": sessionResult.deviceId
      },
      date: "2016-03-11T11:33:00",
      language: "tr-TR"
    };

    const response = await fetch(partnerUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: PARTNERS_API_AUTH
      },
      body: JSON.stringify(partnerRequestBody),
      signal
    });

    const raw = await response.text();
    const payload = parseJsonSafe(raw);

    if (!response.ok) {
      const apiError =
        (payload &&
          typeof payload === "object" &&
          String(payload.message || payload.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        clusterLabel,
        codes: [],
        error: `${clusterLabel}: Partner API HTTP ${response.status}: ${apiError}`
      };
    }

    if (!payload) {
      return {
        clusterLabel,
        codes: [],
        error: `${clusterLabel}: Partner API JSON parse edilemedi.`
      };
    }

    return {
      clusterLabel,
      codes: extractPartnerCodes(payload),
      error: null
    };
  } catch (err) {
    return {
      clusterLabel,
      codes: [],
      error: `${clusterLabel}: ${err?.message || "Partner API fetch hatası"}`
    };
  }
}

async function fetchPartnerCodes() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    const partnerUrls = buildClusterPartnerUrls(PARTNERS_API_URL);
    if (partnerUrls.length === 0) {
      return { codes: [], error: "Partner URL yapılandırması boş." };
    }

    const results = await Promise.all(
      partnerUrls.map((partnerUrl) => fetchPartnerCodesFromCluster(partnerUrl, controller.signal))
    );

    const errors = [];
    const mergedCodes = new Set();

    results.forEach((result) => {
      if (result.error) errors.push(result.error);
      result.codes.forEach((code) => mergedCodes.add(code));
    });

    const codes = normalizeCodes(Array.from(mergedCodes));
    if (codes.length > 0) {
      await savePartnerCodesCache(codes);
      if (errors.length > 0) {
        return { codes, error: `${errors.length}/${partnerUrls.length} cluster alınamadı.` };
      }
      return { codes, error: null };
    }

    const cache = await loadPartnerCodesCache();
    if (cache && cache.codes.length > 0) {
      const cacheDate = cache.updatedAt ? new Date(cache.updatedAt).toLocaleString("tr-TR") : "";
      const suffix = cacheDate ? ` (${cacheDate})` : "";
      return {
        codes: cache.codes,
        error: `Canlı veriler alınamadı, önbellekten gösteriliyor${suffix}.`
      };
    }

    if (errors.length > 0) {
      console.error("Partner API cluster errors:", errors);
      return { codes: [], error: errors[0] };
    }

    return { codes: [], error: "Partner API sonucu boş (status=1 ve code bulunan kayıt yok)." };
  } catch (err) {
    const message = `Partner API fetch hatası: ${err?.message || "Bilinmeyen hata"}`;
    console.error("Partner API fetch error:", err);
    const cache = await loadPartnerCodesCache();
    if (cache && cache.codes.length > 0) {
      const cacheDate = cache.updatedAt ? new Date(cache.updatedAt).toLocaleString("tr-TR") : "";
      const suffix = cacheDate ? ` (${cacheDate})` : "";
      return {
        codes: cache.codes,
        error: `Canlı veriler alınamadı, önbellekten gösteriliyor${suffix}.`
      };
    }
    return { codes: [], error: message };
  } finally {
    clearTimeout(timeout);
  }
}

app.get("/", (req, res) => {
  if (req.session.user) return res.redirect("/dashboard");
  res.redirect("/login");
});

app.get("/login", (req, res) => {
  if (req.session.user) return res.redirect("/dashboard");
  res.render("login", { error: null });
});

app.post("/login", async (req, res) => {
  const { username, password } = req.body;

  if (!username || !password) {
    return res.status(400).render("login", { error: "Kullanıcı adı ve şifre gerekli." });
  }

  try {
    const result = await pool.query("SELECT * FROM users WHERE username = $1", [username]);
    const user = result.rows[0];
    if (!user) {
      return res.status(401).render("login", { error: "Hatalı giriş." });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.status(401).render("login", { error: "Hatalı giriş." });
    }

    req.session.user = {
      id: user.id,
      username: user.username,
      displayName: user.display_name
    };
    res.redirect("/dashboard?login=1");
  } catch (err) {
    console.error(err);
    res.status(500).render("login", { error: "Sunucu hatası." });
  }
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

app.get("/dashboard", requireAuth, (req, res) => {
  res.render("dashboard", {
    user: req.session.user,
    active: "dashboard",
    loginSuccess: req.query.login === "1"
  });
});

app.get("/change-password", requireAuth, (req, res) => {
  res.render("change-password", {
    user: req.session.user,
    error: null,
    ok: req.query.ok === "1",
    active: "password"
  });
});

app.get("/reports/sales", requireAuth, async (req, res) => {
  const normalizeDate = (value) => {
    if (typeof value !== "string") return "";
    return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
  };

  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const today = `${yyyy}-${mm}-${dd}`;
  const startDate = normalizeDate(req.query.startDate) || today;
  const endDate = normalizeDate(req.query.endDate) || today;
  const requestedCompany = typeof req.query.company === "string" ? req.query.company : "all";
  const { codes: partnerCodes, error: partnerError } = await fetchPartnerCodes();
  const companies = [{ value: "all", label: "Tümü" }].concat(
    partnerCodes.map((code) => ({ value: code, label: code }))
  );
  if (partnerError) {
    companies.push({
      value: "__partner_error__",
      label: `Hata: ${partnerError}`,
      disabled: true
    });
  }

  const company = companies.some((item) => !item.disabled && item.value === requestedCompany)
    ? requestedCompany
    : "all";

  res.render("reports-sales", {
    user: req.session.user,
    active: "sales",
    filters: {
      startDate,
      endDate,
      company
    },
    companies
  });
});

app.post("/change-password", requireAuth, async (req, res) => {
  const { currentPassword, newPassword, confirmPassword } = req.body;
  if (!currentPassword || !newPassword || !confirmPassword) {
    return res.status(400).render("change-password", {
      user: req.session.user,
      error: "Tüm alanlar zorunludur.",
      ok: false
    });
  }
  if (newPassword !== confirmPassword) {
    return res.status(400).render("change-password", {
      user: req.session.user,
      error: "Yeni şifreler eşleşmiyor.",
      ok: false
    });
  }

  try {
    const result = await pool.query("SELECT password_hash FROM users WHERE id = $1", [
      req.session.user.id
    ]);
    const user = result.rows[0];
    if (!user) {
      return res.status(404).render("change-password", {
        user: req.session.user,
        error: "Kullanıcı bulunamadı.",
        ok: false
      });
    }

    const ok = await bcrypt.compare(currentPassword, user.password_hash);
    if (!ok) {
      return res.status(401).render("change-password", {
        user: req.session.user,
        error: "Mevcut şifre hatalı.",
        ok: false
      });
    }

    const passwordHash = await bcrypt.hash(newPassword, 10);
    await pool.query("UPDATE users SET password_hash = $1 WHERE id = $2", [
      passwordHash,
      req.session.user.id
    ]);
    res.redirect("/change-password?ok=1");
  } catch (err) {
    console.error(err);
    res.status(500).render("change-password", {
      user: req.session.user,
      error: "Sunucu hatası.",
      ok: false
    });
  }
});

app.get("/users", requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT id, username, display_name, created_at FROM users ORDER BY id DESC"
    );
    res.render("users", {
      user: req.session.user,
      users: result.rows,
      error: null,
      ok: req.query.ok === "1",
      active: "users"
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sunucu hatası");
  }
});

app.post("/users", requireAuth, async (req, res) => {
  const { username, displayName, password } = req.body;
  if (!username || !displayName || !password) {
    return res.status(400).send("Eksik alan");
  }
  try {
    const passwordHash = await bcrypt.hash(password, 10);
    await pool.query(
      "INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3)",
      [username.trim(), passwordHash, displayName.trim()]
    );
    res.redirect("/users?ok=1");
  } catch (err) {
    console.error(err);
    res.status(400).send("Kullanıcı oluşturulamadı");
  }
});

app.get("/screens", requireAuth, async (req, res) => {
  try {
    const result = await pool.query("SELECT id, key, name FROM screens ORDER BY id DESC");
    res.render("screens", {
      user: req.session.user,
      screens: result.rows,
      ok: req.query.ok === "1",
      active: "screens"
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sunucu hatası");
  }
});

app.post("/screens", requireAuth, async (req, res) => {
  const { key, name } = req.body;
  if (!key || !name) {
    return res.status(400).send("Eksik alan");
  }
  try {
    await pool.query("INSERT INTO screens (key, name) VALUES ($1, $2)", [key.trim(), name.trim()]);
    res.redirect("/screens?ok=1");
  } catch (err) {
    console.error(err);
    res.status(400).send("Ekran eklenemedi");
  }
});

app.get("/permissions/:userId", requireAuth, async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) return res.status(400).send("Geçersiz kullanıcı");

  try {
    const userResult = await pool.query(
      "SELECT id, username, display_name FROM users WHERE id = $1",
      [userId]
    );
    const targetUser = userResult.rows[0];
    if (!targetUser) return res.status(404).send("Kullanıcı bulunamadı");

    const sql = `
      SELECT s.id, s.key, s.name, COALESCE(usp.can_view, false) AS can_view
      FROM screens s
      LEFT JOIN user_screen_permissions usp
        ON usp.screen_id = s.id AND usp.user_id = $1
      ORDER BY s.id ASC
    `;
    const screensResult = await pool.query(sql, [userId]);
    res.render("permissions", {
      user: req.session.user,
      targetUser,
      screens: screensResult.rows,
      ok: req.query.ok === "1",
      active: "users"
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sunucu hatası");
  }
});

app.post("/permissions/:userId", requireAuth, async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) return res.status(400).send("Geçersiz kullanıcı");
  const { screenId, canView } = req.body;
  const screenIdNum = Number(screenId);
  const canViewBool = canView === "on";

  try {
    await pool.query(
      `INSERT INTO user_screen_permissions (user_id, screen_id, can_view)
       VALUES ($1, $2, $3)
       ON CONFLICT(user_id, screen_id)
       DO UPDATE SET can_view = EXCLUDED.can_view`,
      [userId, screenIdNum, canViewBool]
    );
    res.redirect(`/permissions/${userId}?ok=1`);
  } catch (err) {
    console.error(err);
    res.status(500).send("Güncelleme hatası");
  }
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false });
  }
});

app.get("/api/endpoints", requireAuth, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT id, title, method, path, target_url, description, body, headers, params, sort_order
       FROM api_endpoints
       ORDER BY sort_order ASC, id DESC`
    );
    res.json({ ok: true, items: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Endpoint listesi alınamadı." });
  }
});

app.get("/api/targets", requireAuth, async (req, res) => {
  try {
    await ensureTargetsTable();
    const result = await pool.query(
      `SELECT id, url, created_at, updated_at
       FROM api_targets
       ORDER BY updated_at DESC, id DESC`
    );
    res.json({ ok: true, items: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Hedef URL listesi alınamadı." });
  }
});

app.post("/api/targets", requireAuth, async (req, res) => {
  const normalized = normalizeTargetUrl(req.body?.url);
  if (!normalized) {
    return res.status(400).json({ ok: false, error: "Geçerli bir Hedef URL girin." });
  }
  try {
    await ensureTargetsTable();
    const result = await pool.query(
      `INSERT INTO api_targets (url)
       VALUES ($1)
       ON CONFLICT (url)
       DO UPDATE SET updated_at = now()
       RETURNING id, url, created_at, updated_at`,
      [normalized]
    );
    res.json({ ok: true, item: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Hedef URL kaydedilemedi." });
  }
});

app.post("/api/endpoints", requireAuth, async (req, res) => {
  const { title, method, path, targetUrl, description, body, headers, params } = req.body;
  if (!title || !method || !path) {
    return res.status(400).json({ ok: false, error: "Eksik alan" });
  }
  try {
    const result = await pool.query(
      `INSERT INTO api_endpoints (title, method, path, target_url, description, body, headers, params, sort_order)
       VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8,
         COALESCE((SELECT MIN(sort_order) - 1 FROM api_endpoints), 1)
       )
       RETURNING id, title, method, path, target_url, description, body, headers, params, sort_order`,
      [
        title.trim(),
        method.trim().toUpperCase(),
        path.trim(),
        targetUrl ? targetUrl.trim() : null,
        description ? description.trim() : null,
        body || "{}",
        headers || "{\n  \"Content-Type\": \"application/json\"\n}",
        params || "{}"
      ]
    );
    res.json({ ok: true, item: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Endpoint kaydedilemedi." });
  }
});

app.post("/api/endpoints/reorder", requireAuth, async (req, res) => {
  const ids =
    Array.isArray(req.body?.ids) && req.body.ids.length
      ? req.body.ids.map((value) => Number(value)).filter((id) => Number.isInteger(id))
      : [];

  if (!ids.length) {
    return res.status(400).json({ ok: false, error: "Sıralama için endpoint id listesi gerekli." });
  }

  try {
    const uniqueIds = Array.from(new Set(ids));
    if (uniqueIds.length !== ids.length) {
      return res.status(400).json({ ok: false, error: "Endpoint listesinde tekrar eden id var." });
    }

    const totalResult = await pool.query("SELECT COUNT(*)::int AS count FROM api_endpoints");
    const total = totalResult.rows[0]?.count || 0;
    if (uniqueIds.length !== total) {
      return res.status(400).json({ ok: false, error: "Tüm endpointler sıralama listesinde olmalı." });
    }

    const checkResult = await pool.query(
      "SELECT COUNT(*)::int AS count FROM api_endpoints WHERE id = ANY($1::int[])",
      [uniqueIds]
    );
    if (checkResult.rows[0]?.count !== total) {
      return res.status(400).json({ ok: false, error: "Geçersiz endpoint id listesi." });
    }

    await pool.query(
      `UPDATE api_endpoints AS target
       SET sort_order = ordered.ord::int,
           updated_at = now()
       FROM (
         SELECT id, ord
         FROM unnest($1::int[]) WITH ORDINALITY AS list(id, ord)
       ) AS ordered
       WHERE target.id = ordered.id`,
      [uniqueIds]
    );

    const result = await pool.query(
      `SELECT id, title, method, path, target_url, description, body, headers, params, sort_order
       FROM api_endpoints
       ORDER BY sort_order ASC, id DESC`
    );
    res.json({ ok: true, items: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Endpoint sıralaması güncellenemedi." });
  }
});

app.put("/api/endpoints/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) {
    return res.status(400).json({ ok: false, error: "Geçersiz id" });
  }
  const { title, method, path, description, body, headers, params, targetUrl, sortOrder } =
    req.body || {};
  try {
    const existingResult = await pool.query(
      `SELECT id, title, method, path, target_url, description, body, headers, params, sort_order
       FROM api_endpoints
       WHERE id = $1`,
      [id]
    );
    const existing = existingResult.rows[0];
    if (!existing) {
      return res.status(404).json({ ok: false, error: "Endpoint bulunamadı." });
    }

    const nextTitle =
      typeof title === "string" && title.trim() ? title.trim() : existing.title;
    const nextMethod =
      typeof method === "string" && method.trim() ? method.trim().toUpperCase() : existing.method;
    const nextPath = typeof path === "string" && path.trim() ? path.trim() : existing.path;
    const nextDescription =
      description === undefined
        ? existing.description
        : String(description || "").trim() || null;
    const nextBody = body === undefined ? existing.body : body || "{}";
    const nextHeaders =
      headers === undefined
        ? existing.headers
        : headers || "{\n  \"Content-Type\": \"application/json\"\n}";
    const nextParams = params === undefined ? existing.params : params || "{}";
    const nextTargetUrl =
      targetUrl === undefined ? existing.target_url : String(targetUrl || "").trim() || null;
    const nextSortOrder =
      sortOrder === undefined || !Number.isInteger(Number(sortOrder))
        ? existing.sort_order
        : Number(sortOrder);

    const result = await pool.query(
      `UPDATE api_endpoints
       SET title = $1,
           method = $2,
           path = $3,
           description = $4,
           body = $5,
           headers = $6,
           params = $7,
            target_url = $8,
           sort_order = $9,
           updated_at = now()
       WHERE id = $10
       RETURNING id, title, method, path, target_url, description, body, headers, params, sort_order`,
      [
        nextTitle,
        nextMethod,
        nextPath,
        nextDescription,
        nextBody,
        nextHeaders,
        nextParams,
        nextTargetUrl,
        nextSortOrder,
        id
      ]
    );
    res.json({ ok: true, item: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Endpoint güncellenemedi." });
  }
});

app.post("/api/execute", requireAuth, async (req, res) => {
  const { endpointId, targetUrl, path, method, headers, params, body } = req.body || {};
  if (!targetUrl && !path) {
    return res.status(400).json({ ok: false, error: "Hedef URL eksik." });
  }
  if (!Number.isInteger(Number(endpointId))) {
    return res.status(400).json({ ok: false, error: "Endpoint seçilmeli." });
  }

  let url;
  try {
    if (path && /^https?:\/\//i.test(path)) {
      url = new URL(path);
    } else if (targetUrl) {
      url = new URL(path || "", targetUrl);
    } else {
      url = new URL(path);
    }
  } catch (err) {
    return res.status(400).json({ ok: false, error: "Geçersiz URL." });
  }

  if (params && typeof params === "object") {
    Object.entries(params).forEach(([key, value]) => {
      if (value === undefined || value === null) return;
      if (Array.isArray(value)) {
        value.forEach((item) => url.searchParams.append(key, String(item)));
      } else {
        url.searchParams.set(key, String(value));
      }
    });
  }

  const finalHeaders = {};
  if (headers && typeof headers === "object") {
    Object.entries(headers).forEach(([key, value]) => {
      if (!key) return;
      finalHeaders[key] = String(value);
    });
  }

  const httpMethod = (method || "GET").toUpperCase();
  const hasBody =
    body !== undefined &&
    body !== null &&
    String(body).trim().length > 0 &&
    httpMethod !== "GET" &&
    httpMethod !== "HEAD";

  if (hasBody) {
    const hasContentType = Object.keys(finalHeaders).some(
      (key) => key.toLowerCase() === "content-type"
    );
    if (!hasContentType) {
      finalHeaders["Content-Type"] = "application/json";
    }
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  const startedAt = Date.now();

  try {
    const response = await fetch(url.toString(), {
      method: httpMethod,
      headers: finalHeaders,
      body: hasBody ? (typeof body === "string" ? body : JSON.stringify(body)) : undefined,
      signal: controller.signal
    });

    const text = await response.text();
    clearTimeout(timeout);

    const durationMs = Date.now() - startedAt;
    try {
      await pool.query(
        `INSERT INTO api_requests
         (endpoint_id, target_url, method, path, headers, params, body, response_status, response_text, response_headers, duration_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          Number(endpointId),
          url.origin,
          httpMethod,
          url.pathname,
          JSON.stringify(finalHeaders),
          params ? JSON.stringify(params) : "{}",
          body ? String(body) : "",
          response.status,
          text,
          JSON.stringify(Object.fromEntries(response.headers.entries())),
          durationMs
        ]
      );
    } catch (err) {
      console.error("Request log insert error:", err);
    }

    res.json({
      ok: response.ok,
      status: response.status,
      statusText: response.statusText,
      url: response.url,
      durationMs,
      body: text,
      headers: Object.fromEntries(response.headers.entries())
    });
  } catch (err) {
    clearTimeout(timeout);
    try {
      await pool.query(
        `INSERT INTO api_requests
         (endpoint_id, target_url, method, path, headers, params, body, response_status, response_text, response_headers, duration_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
        [
          Number(endpointId),
          url.origin,
          httpMethod,
          url.pathname,
          JSON.stringify(finalHeaders),
          params ? JSON.stringify(params) : "{}",
          body ? String(body) : "",
          null,
          err.message || "İstek hatası.",
          "{}",
          Date.now() - startedAt
        ]
      );
    } catch (logErr) {
      console.error("Request log insert error:", logErr);
    }
    res.status(502).json({
      ok: false,
      error: "İstek hatası.",
      details: err.message
    });
  }
});

app.get("/api/requests/:endpointId", requireAuth, async (req, res) => {
  const endpointId = Number(req.params.endpointId);
  if (!Number.isInteger(endpointId)) {
    return res.status(400).json({ ok: false, error: "Geçersiz endpoint." });
  }
  try {
    const result = await pool.query(
      `SELECT id, method, path, target_url, response_status, duration_ms, created_at, body, headers, params, response_text
       FROM api_requests
       WHERE endpoint_id = $1
       ORDER BY id DESC
       LIMIT 50`,
      [endpointId]
    );
    res.json({ ok: true, items: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

app.get("/api/requests/item/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) {
    return res.status(400).json({ ok: false, error: "Geçersiz kayıt." });
  }
  try {
    const result = await pool.query(
      `SELECT id, endpoint_id, method, path, target_url, headers, params, body,
              response_status, response_text, response_headers, duration_ms, created_at
       FROM api_requests
       WHERE id = $1`,
      [id]
    );
    const item = result.rows[0];
    if (!item) return res.status(404).json({ ok: false, error: "Kayıt bulunamadı." });
    res.json({ ok: true, item });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

app.delete("/api/requests/:endpointId", requireAuth, async (req, res) => {
  const endpointId = Number(req.params.endpointId);
  if (!Number.isInteger(endpointId)) {
    return res.status(400).json({ ok: false, error: "Geçersiz endpoint." });
  }
  try {
    await pool.query("DELETE FROM api_requests WHERE endpoint_id = $1", [endpointId]);
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
