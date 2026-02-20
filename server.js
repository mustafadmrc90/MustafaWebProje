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
const REPORTING_API_URL =
  process.env.REPORTING_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/reporting/obiletsalesreport";
const REPORTING_API_AUTH =
  process.env.REPORTING_API_AUTH || "Basic TXVyb011aG9BbGlPZ2lIYXJ1bk96YW4K";
const PARTNER_CLUSTER_MIN = 0;
const PARTNER_CLUSTER_MAX = 15;
const PARTNER_CODES_CACHE_FILE = path.join(__dirname, "data", "partner-codes-cache.json");
const SLACK_COUNTS_FILE = path.join(__dirname, "slack-counts.json");
const SLACK_MONTHLY_ANALYSIS_FILE = path.join(__dirname, "data", "slack-monthly-analysis.json");
const SLACK_API_BASE_URL = "https://slack.com/api";
const SLACK_DEFAULT_LIMIT = 200;
const SLACK_MAX_RATE_LIMIT_RETRY = 8;
const SLACK_API_TIMEOUT_MS = Number.parseInt(process.env.SLACK_API_TIMEOUT_MS || "15000", 10) || 15000;
const SLACK_ANALYSIS_CHANNEL_TYPES_RAW = process.env.SLACK_ANALYSIS_CHANNEL_TYPES || "private_channel";
const SLACK_ANALYSIS_CACHE_TTL_MS =
  Number.parseInt(process.env.SLACK_ANALYSIS_CACHE_TTL_MS || "120000", 10) || 120000;
const SLACK_ANALYSIS_MAX_RUNTIME_MS =
  Number.parseInt(process.env.SLACK_ANALYSIS_MAX_RUNTIME_MS || "45000", 10) || 45000;
const SLACK_ANALYSIS_CHANNEL_CONCURRENCY =
  Number.parseInt(process.env.SLACK_ANALYSIS_CHANNEL_CONCURRENCY || "2", 10) || 2;
const SLACK_ANALYSIS_THREAD_CONCURRENCY =
  Number.parseInt(process.env.SLACK_ANALYSIS_THREAD_CONCURRENCY || "4", 10) || 4;
const SLACK_ANALYSIS_MAX_CHANNELS = Number.parseInt(process.env.SLACK_ANALYSIS_MAX_CHANNELS || "80", 10) || 80;
const SLACK_ANALYSIS_MAX_HISTORY_PAGES =
  Number.parseInt(process.env.SLACK_ANALYSIS_MAX_HISTORY_PAGES || "8", 10) || 8;
const SLACK_ANALYSIS_MAX_THREADS_PER_CHANNEL =
  Number.parseInt(process.env.SLACK_ANALYSIS_MAX_THREADS_PER_CHANNEL || "120", 10) || 120;
const SLACK_ANALYSIS_MAX_REPLY_PAGES =
  Number.parseInt(process.env.SLACK_ANALYSIS_MAX_REPLY_PAGES || "6", 10) || 6;
const SLACK_SELECTED_USERS = [
  { id: "U03M921BDCJ", name: "Onur Uğur - Corp" },
  { id: "U03MBE47P1A", name: "Çağatay Atalay - Corp" },
  { id: "U03M2DS4WCW", name: "Üveys Turgut - Corp" },
  { id: "U03M2DME9U6", name: "Samet Ateş - Corp" },
  { id: "U03M64AKWPP", name: "Osman Ağca - Corp" },
  { id: "U03MMMBN1Q9", name: "Uğurcan CORP" },
  { id: "U03M91THFA6", name: "Denizhan Arslan - CORP" }
];
const SIDEBAR_MENU_REGISTRY = [
  {
    key: "general",
    type: "section",
    label: "Genel",
    parentKey: null,
    route: null,
    routeKey: null,
    sortOrder: 10,
    iconKey: "folder"
  },
  {
    key: "dashboard",
    type: "item",
    label: "Obus API",
    parentKey: "general",
    route: "/dashboard",
    routeKey: "dashboard",
    sortOrder: 11,
    iconKey: "dashboard"
  },
  {
    key: "reports",
    type: "section",
    label: "Raporlar",
    parentKey: null,
    route: null,
    routeKey: null,
    sortOrder: 20,
    iconKey: "folder"
  },
  {
    key: "sales",
    type: "item",
    label: "Satışlar",
    parentKey: "reports",
    route: "/reports/sales",
    routeKey: "sales",
    sortOrder: 21,
    iconKey: "sales"
  },
  {
    key: "all-companies",
    type: "item",
    label: "Tüm Firmalar",
    parentKey: "reports",
    route: "/reports/all-companies",
    routeKey: "all-companies",
    sortOrder: 22,
    iconKey: "all-companies"
  },
  {
    key: "slack-analysis",
    type: "item",
    label: "Slack Analiz",
    parentKey: "reports",
    route: "/reports/slack-analysis",
    routeKey: "slack-analysis",
    sortOrder: 23,
    iconKey: "slack-analysis"
  },
  {
    key: "management",
    type: "section",
    label: "Yönetim",
    parentKey: null,
    route: null,
    routeKey: null,
    sortOrder: 30,
    iconKey: "folder"
  },
  {
    key: "users",
    type: "item",
    label: "Kullanıcılar",
    parentKey: "management",
    route: "/users",
    routeKey: "users",
    sortOrder: 31,
    iconKey: "users"
  },
  {
    key: "password",
    type: "item",
    label: "Şifre Değiştir",
    parentKey: "management",
    route: "/change-password",
    routeKey: "password",
    sortOrder: 32,
    iconKey: "password"
  }
];
const slackReplyReportCache = new Map();

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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sidebar_menu_items (
      key TEXT PRIMARY KEY,
      label TEXT NOT NULL,
      type TEXT NOT NULL,
      parent_key TEXT REFERENCES sidebar_menu_items(key) ON DELETE CASCADE,
      route TEXT,
      route_key TEXT,
      sort_order INTEGER NOT NULL DEFAULT 0,
      icon_key TEXT,
      is_active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_sidebar_permissions (
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      menu_key TEXT NOT NULL REFERENCES sidebar_menu_items(key) ON DELETE CASCADE,
      can_view BOOLEAN NOT NULL DEFAULT true,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (user_id, menu_key)
    )
  `);

  await pool.query(`
    ALTER TABLE sidebar_menu_items
      ADD COLUMN IF NOT EXISTS route_key TEXT,
      ADD COLUMN IF NOT EXISTS icon_key TEXT,
      ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    ALTER TABLE user_sidebar_permissions
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS slack_reply_analysis_runs (
      id SERIAL PRIMARY KEY,
      start_date DATE NOT NULL,
      end_date DATE NOT NULL,
      total_requests INTEGER NOT NULL DEFAULT 0,
      total_replies INTEGER NOT NULL DEFAULT 0,
      row_count INTEGER NOT NULL DEFAULT 0,
      save_count INTEGER NOT NULL DEFAULT 1,
      source TEXT,
      created_by INTEGER REFERENCES users(id),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE slack_reply_analysis_runs
      ADD COLUMN IF NOT EXISTS total_requests INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS save_count INTEGER NOT NULL DEFAULT 1,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS slack_reply_analysis_items (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES slack_reply_analysis_runs(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL,
      user_name TEXT NOT NULL,
      request_count INTEGER NOT NULL DEFAULT 0,
      reply_count INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE slack_reply_analysis_items
      ADD COLUMN IF NOT EXISTS request_count INTEGER NOT NULL DEFAULT 0
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_slack_reply_analysis_items_run_id
    ON slack_reply_analysis_items (run_id)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_slack_reply_analysis_runs_lookup
    ON slack_reply_analysis_runs (start_date, end_date, created_by, id)
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

  await syncSidebarMenusAndPermissions();
}

initDb().catch((err) => {
  console.error("DB init error:", err);
});

function toSidebarBool(value, fallback = true) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value > 0;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "t" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "f" || normalized === "0" || normalized === "no") return false;
  return fallback;
}

function compareSidebarEntries(a, b) {
  const orderA = Number.isFinite(Number(a.sortOrder)) ? Number(a.sortOrder) : 0;
  const orderB = Number.isFinite(Number(b.sortOrder)) ? Number(b.sortOrder) : 0;
  if (orderA !== orderB) return orderA - orderB;
  const byLabel = String(a.label || "").localeCompare(String(b.label || ""), "tr");
  if (byLabel !== 0) return byLabel;
  return String(a.key || "").localeCompare(String(b.key || ""), "tr");
}

function normalizeSidebarRows(rows) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => {
      const key = String(row.key || "").trim();
      const label = String(row.label || row.name || "").trim();
      const type = row.type === "section" ? "section" : "item";
      const parentKeyRaw = row.parent_key ?? row.parentKey ?? null;
      const parentKey = parentKeyRaw === null || parentKeyRaw === undefined ? null : String(parentKeyRaw).trim() || null;
      const routeRaw = row.route ?? "";
      const route = routeRaw ? String(routeRaw).trim() : "";
      const routeKeyRaw = row.route_key ?? row.routeKey ?? key;
      const routeKey = routeKeyRaw ? String(routeKeyRaw).trim() : key;
      const sortOrderRaw = row.sort_order ?? row.sortOrder ?? 0;
      const sortOrder = Number.isFinite(Number(sortOrderRaw)) ? Number(sortOrderRaw) : 0;
      const iconKeyRaw = row.icon_key ?? row.iconKey ?? "folder";
      const iconKey = iconKeyRaw ? String(iconKeyRaw).trim() : "folder";
      const canViewRaw = row.can_view ?? row.canView ?? true;
      const canView = toSidebarBool(canViewRaw, true);

      return {
        key,
        label,
        type,
        parentKey,
        route,
        routeKey,
        sortOrder,
        iconKey,
        canView
      };
    })
    .filter((row) => row.key && row.label)
    .sort(compareSidebarEntries);
}

function buildSidebarModelFromRows(rows) {
  const normalizedRows = normalizeSidebarRows(rows);
  const sectionRows = normalizedRows.filter((row) => row.type === "section");
  const itemRows = normalizedRows.filter((row) => row.type === "item");
  const itemsByParent = new Map();
  const allowedMenuKeys = new Set();
  const allowedRouteKeys = new Set();

  itemRows.forEach((item) => {
    if (!item.parentKey) return;
    const current = itemsByParent.get(item.parentKey) || [];
    current.push(item);
    itemsByParent.set(item.parentKey, current);
  });

  const sections = sectionRows
    .filter((section) => section.canView)
    .map((section) => {
      const visibleItems = (itemsByParent.get(section.key) || [])
        .filter((item) => item.canView && item.route)
        .sort(compareSidebarEntries)
        .map((item) => {
          allowedMenuKeys.add(item.key);
          allowedRouteKeys.add(item.routeKey || item.key);
          return {
            key: item.key,
            label: item.label,
            route: item.route,
            routeKey: item.routeKey || item.key,
            iconKey: item.iconKey || "folder"
          };
        });

      if (!visibleItems.length) return null;

      allowedMenuKeys.add(section.key);
      return {
        key: section.key,
        label: section.label,
        iconKey: section.iconKey || "folder",
        items: visibleItems
      };
    })
    .filter(Boolean);

  return {
    sections,
    allowedMenuKeys: Array.from(allowedMenuKeys),
    allowedRouteKeys: Array.from(allowedRouteKeys)
  };
}

function buildSidebarFallbackModel() {
  const rows = SIDEBAR_MENU_REGISTRY.map((item) => ({
    key: item.key,
    label: item.label,
    type: item.type,
    parent_key: item.parentKey,
    route: item.route,
    route_key: item.routeKey,
    sort_order: item.sortOrder,
    icon_key: item.iconKey,
    can_view: true
  }));
  return buildSidebarModelFromRows(rows);
}

async function syncSidebarMenusAndPermissions() {
  const menuItems = SIDEBAR_MENU_REGISTRY.map((item) => ({
    ...item,
    key: String(item.key || "").trim(),
    label: String(item.label || "").trim(),
    type: item.type === "section" ? "section" : "item"
  })).filter((item) => item.key && item.label);

  if (!menuItems.length) return;

  const registryKeys = menuItems.map((item) => item.key);
  const sections = menuItems.filter((item) => item.type === "section").sort(compareSidebarEntries);
  const entries = menuItems.filter((item) => item.type !== "section").sort(compareSidebarEntries);
  const orderedRows = sections.concat(entries);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `
        UPDATE sidebar_menu_items
        SET is_active = false, updated_at = now()
        WHERE NOT (key = ANY($1::text[]))
      `,
      [registryKeys]
    );

    for (const item of orderedRows) {
      await client.query(
        `
          INSERT INTO sidebar_menu_items (
            key,
            label,
            type,
            parent_key,
            route,
            route_key,
            sort_order,
            icon_key,
            is_active,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, now())
          ON CONFLICT (key)
          DO UPDATE SET
            label = EXCLUDED.label,
            type = EXCLUDED.type,
            parent_key = EXCLUDED.parent_key,
            route = EXCLUDED.route,
            route_key = EXCLUDED.route_key,
            sort_order = EXCLUDED.sort_order,
            icon_key = EXCLUDED.icon_key,
            is_active = true,
            updated_at = now()
        `,
        [
          item.key,
          item.label,
          item.type,
          item.parentKey || null,
          item.route || null,
          item.routeKey || null,
          Number.isFinite(Number(item.sortOrder)) ? Number(item.sortOrder) : 0,
          item.iconKey || "folder"
        ]
      );
    }

    await client.query(`
      INSERT INTO user_sidebar_permissions (user_id, menu_key, can_view)
      SELECT u.id, m.key, true
      FROM users u
      CROSS JOIN sidebar_menu_items m
      WHERE m.is_active = true
      ON CONFLICT (user_id, menu_key) DO NOTHING
    `);

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function ensureSidebarPermissionsForUser(userId) {
  const userIdNum = Number(userId);
  if (!Number.isInteger(userIdNum)) return;
  await pool.query(
    `
      INSERT INTO user_sidebar_permissions (user_id, menu_key, can_view)
      SELECT $1, m.key, true
      FROM sidebar_menu_items m
      WHERE m.is_active = true
      ON CONFLICT (user_id, menu_key) DO NOTHING
    `,
    [userIdNum]
  );
}

async function loadSidebarForUser(userId) {
  const userIdNum = Number(userId);
  if (!Number.isInteger(userIdNum)) {
    return buildSidebarFallbackModel();
  }

  await ensureSidebarPermissionsForUser(userIdNum);
  const result = await pool.query(
    `
      SELECT
        m.key,
        m.label,
        m.type,
        m.parent_key,
        m.route,
        m.route_key,
        m.sort_order,
        m.icon_key,
        COALESCE(usp.can_view, true) AS can_view
      FROM sidebar_menu_items m
      LEFT JOIN user_sidebar_permissions usp
        ON usp.menu_key = m.key
       AND usp.user_id = $1
      WHERE m.is_active = true
      ORDER BY m.sort_order ASC, m.key ASC
    `,
    [userIdNum]
  );
  return buildSidebarModelFromRows(result.rows);
}

function getFirstAccessibleRoute(sidebar) {
  const sections = Array.isArray(sidebar?.sections) ? sidebar.sections : [];
  for (const section of sections) {
    const items = Array.isArray(section?.items) ? section.items : [];
    for (const item of items) {
      if (item?.route) return item.route;
    }
  }
  return null;
}

function hasMenuAccess(sidebar, menuKey) {
  const allowedMenuKeys = new Set(Array.isArray(sidebar?.allowedMenuKeys) ? sidebar.allowedMenuKeys : []);
  const allowedRouteKeys = new Set(Array.isArray(sidebar?.allowedRouteKeys) ? sidebar.allowedRouteKeys : []);
  return allowedMenuKeys.has(menuKey) || allowedRouteKeys.has(menuKey);
}

function buildPermissionSections(rows) {
  const normalizedRows = normalizeSidebarRows(rows);
  const sectionRows = normalizedRows.filter((row) => row.type === "section");
  const itemRows = normalizedRows.filter((row) => row.type === "item");
  const sectionByKey = new Map();

  sectionRows.forEach((section) => {
    sectionByKey.set(section.key, {
      key: section.key,
      label: section.label,
      route: section.route || "",
      routeKey: section.routeKey || "",
      iconKey: section.iconKey || "folder",
      sortOrder: section.sortOrder,
      canView: section.canView,
      items: []
    });
  });

  itemRows.forEach((item) => {
    const targetSection = sectionByKey.get(item.parentKey || "");
    if (!targetSection) return;
    targetSection.items.push({
      key: item.key,
      label: item.label,
      route: item.route || "",
      routeKey: item.routeKey || item.key,
      iconKey: item.iconKey || "folder",
      sortOrder: item.sortOrder,
      canView: item.canView
    });
  });

  return Array.from(sectionByKey.values()).map((section) => ({
    ...section,
    items: section.items.sort(compareSidebarEntries)
  }));
}

async function loadSidebarPermissionSectionsForUser(userId) {
  const userIdNum = Number(userId);
  if (!Number.isInteger(userIdNum)) return [];
  await ensureSidebarPermissionsForUser(userIdNum);
  const result = await pool.query(
    `
      SELECT
        m.key,
        m.label,
        m.type,
        m.parent_key,
        m.route,
        m.route_key,
        m.sort_order,
        m.icon_key,
        COALESCE(usp.can_view, true) AS can_view
      FROM sidebar_menu_items m
      LEFT JOIN user_sidebar_permissions usp
        ON usp.menu_key = m.key
       AND usp.user_id = $1
      WHERE m.is_active = true
      ORDER BY m.sort_order ASC, m.key ASC
    `,
    [userIdNum]
  );
  return buildPermissionSections(result.rows);
}

function requireMenuAccess(menuKey) {
  return async (req, res, next) => {
    if (!req.session?.user) {
      if (req.path.startsWith("/api/")) {
        return res.status(401).json({ ok: false, error: "Oturum süresi doldu." });
      }
      return res.redirect("/login");
    }

    try {
      let sidebar = req.sidebar;
      if (!sidebar) {
        sidebar = await loadSidebarForUser(req.session.user.id);
        req.sidebar = sidebar;
        res.locals.sidebar = sidebar;
      }

      if (hasMenuAccess(sidebar, menuKey)) {
        return next();
      }

      const fallbackRoute = getFirstAccessibleRoute(sidebar);
      if (req.path.startsWith("/api/")) {
        return res.status(403).json({ ok: false, error: "Bu alana erişim yetkiniz yok." });
      }
      if (fallbackRoute && fallbackRoute !== req.path) {
        return res.redirect(fallbackRoute);
      }
      return res.status(403).send("Bu sayfayı görüntüleme yetkiniz yok.");
    } catch (err) {
      console.error("Menu access check error:", err);
      return res.status(500).send("Yetki kontrolü başarısız.");
    }
  };
}

app.use(async (req, res, next) => {
  if (!req.session?.user) return next();
  if (req.path.startsWith("/api/")) return next();

  try {
    const sidebar = await loadSidebarForUser(req.session.user.id);
    req.sidebar = sidebar;
    res.locals.sidebar = sidebar;
  } catch (err) {
    console.error("Sidebar load error:", err);
    const fallback = buildSidebarFallbackModel();
    req.sidebar = fallback;
    res.locals.sidebar = fallback;
  }
  return next();
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

function normalizePartnerItems(items) {
  const byKey = new Map();

  (Array.isArray(items) ? items : []).forEach((item) => {
    let code = "";
    let id = "";
    let cluster = "";

    if (typeof item === "string") {
      code = String(item).trim();
    } else if (item && typeof item === "object") {
      code = String(item.code || item.value || "").trim();
      id = String(item.id || "").trim();
      cluster = String(item.cluster || "").trim().toLowerCase();
    }

    if (!code) return;

    const key = `${code}__${id}__${cluster}`;
    if (!byKey.has(key)) {
      byKey.set(key, { code, id, cluster });
    }
  });

  return Array.from(byKey.values()).sort((a, b) => {
    const byCode = a.code.localeCompare(b.code, "tr");
    if (byCode !== 0) return byCode;

    const byId = a.id.localeCompare(b.id, "tr");
    if (byId !== 0) return byId;

    return a.cluster.localeCompare(b.cluster, "tr");
  });
}

function extractPartnerItems(payload, clusterLabel) {
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
  const items = [];

  rows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    if (Number(row.status) !== 1) return;

    const code = String(row.code || row.Code || "").trim();
    if (!code) return;

    const rawId =
      row.id ??
      row.ID ??
      row.partner_id ??
      row.partnerId ??
      row.partnerID ??
      row.provider_id ??
      row.providerId;
    const id = rawId === undefined || rawId === null ? "" : String(rawId).trim();

    items.push({
      code,
      id,
      cluster: String(clusterLabel || "").trim().toLowerCase()
    });
  });

  return normalizePartnerItems(items);
}

function extractPartnerRawRows(payload, clusterLabel) {
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

  const rows = [];
  candidateLists.forEach((list) => {
    if (!Array.isArray(list)) return;
    list.forEach((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return;
      rows.push({
        source_cluster: String(clusterLabel || "").trim().toLowerCase(),
        ...item
      });
    });
  });

  const uniqueRows = new Map();
  rows.forEach((row) => {
    const key = JSON.stringify(row);
    if (!uniqueRows.has(key)) uniqueRows.set(key, row);
  });
  return Array.from(uniqueRows.values());
}

function formatPartnerCellValue(value) {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "";
  if (typeof value === "boolean") return value ? "true" : "false";
  try {
    return JSON.stringify(value);
  } catch (err) {
    return String(value);
  }
}

function buildPartnerRawColumns(rows) {
  const allColumns = new Set();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    if (!row || typeof row !== "object") return;
    Object.keys(row).forEach((key) => allColumns.add(key));
  });

  const preferred = ["source_cluster", "status", "code", "id"];
  const orderedPreferred = preferred.filter((key) => allColumns.has(key));
  const orderedRest = Array.from(allColumns)
    .filter((key) => !preferred.includes(key))
    .sort((a, b) => a.localeCompare(b, "tr"));
  return orderedPreferred.concat(orderedRest);
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

function buildUrlForCluster(baseUrl, clusterLabel) {
  const raw = String(baseUrl || "").trim();
  const cluster = String(clusterLabel || "").trim().toLowerCase();
  if (!raw) return "";
  if (!cluster) return raw;

  if (/cluster\d+/i.test(raw)) {
    return raw.replace(/cluster\d+/i, cluster);
  }
  return raw;
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

function buildCompanyOptionValue(item) {
  const code = String(item?.code || "").trim();
  const id = String(item?.id || "").trim();
  const cluster = String(item?.cluster || "").trim().toLowerCase();
  return `${code}|||${id}|||${cluster}`;
}

function parseCompanyOptionValue(value) {
  const raw = String(value || "").trim();
  if (!raw || raw === "all") return null;

  const parts = raw.split("|||");
  if (parts.length === 3) {
    const [codeRaw, idRaw, clusterRaw] = parts;
    const code = String(codeRaw || "").trim();
    const id = String(idRaw || "").trim();
    const cluster = String(clusterRaw || "").trim().toLowerCase();
    if (code && cluster) {
      return { code, id, cluster };
    }
  }

  // Backward compatibility for older values shown as "Code - ID - clusterX".
  const legacyMatch = raw.match(/^(.*?)\s*-\s*(.*?)\s*-\s*(cluster\d+)$/i);
  if (legacyMatch) {
    const code = String(legacyMatch[1] || "").trim();
    const idToken = String(legacyMatch[2] || "").trim();
    const cluster = String(legacyMatch[3] || "").trim().toLowerCase();
    const id = /^n\/?a$/i.test(idToken) ? "" : idToken;
    if (code && cluster) {
      return { code, id, cluster };
    }
  }

  return null;
}

async function loadPartnerCodesCache() {
  try {
    const raw = await fs.readFile(PARTNER_CODES_CACHE_FILE, "utf8");
    const parsed = parseJsonSafe(raw);
    if (!parsed || typeof parsed !== "object") return null;

    // Backward compatible: old cache may only have `codes`.
    const partnersSource = Array.isArray(parsed.partners)
      ? parsed.partners
      : Array.isArray(parsed.codes)
        ? parsed.codes.map((code) => ({ code, id: "", cluster: "" }))
        : [];
    const partners = normalizePartnerItems(partnersSource);
    if (partners.length === 0) return null;

    return {
      partners,
      updatedAt: String(parsed.updatedAt || "")
    };
  } catch (err) {
    return null;
  }
}

async function savePartnerCodesCache(partners) {
  const normalizedPartners = normalizePartnerItems(partners);
  if (normalizedPartners.length === 0) return;

  const payload = {
    updatedAt: new Date().toISOString(),
    partners: normalizedPartners.map((item) => ({
      code: item.code,
      id: item.id,
      cluster: item.cluster
    }))
  };

  try {
    await fs.mkdir(path.dirname(PARTNER_CODES_CACHE_FILE), { recursive: true });
    await fs.writeFile(PARTNER_CODES_CACHE_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (err) {
    console.error("Partner cache write error:", err);
  }
}

async function fetchPartnerSessionCredentials(
  sessionUrl,
  signal,
  authorization = PARTNERS_API_AUTH
) {
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
      Authorization: authorization || PARTNERS_API_AUTH
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
      return { clusterLabel, partners: [], error: `${clusterLabel}: ${sessionResult.error}` };
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
        partners: [],
        error: `${clusterLabel}: Partner API HTTP ${response.status}: ${apiError}`
      };
    }

    if (!payload) {
      return {
        clusterLabel,
        partners: [],
        error: `${clusterLabel}: Partner API JSON parse edilemedi.`
      };
    }

    return {
      clusterLabel,
      partners: extractPartnerItems(payload, clusterLabel),
      error: null
    };
  } catch (err) {
    return {
      clusterLabel,
      partners: [],
      error: `${clusterLabel}: ${err?.message || "Partner API fetch hatası"}`
    };
  }
}

async function fetchPartnerRawRowsFromCluster(partnerUrl, signal) {
  const clusterLabel = extractClusterLabel(partnerUrl);
  const sessionUrl = buildSessionUrlForPartnerUrl(partnerUrl);

  try {
    const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, signal);
    if (sessionResult.error) {
      return { clusterLabel, rows: [], error: `${clusterLabel}: ${sessionResult.error}` };
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
        rows: [],
        error: `${clusterLabel}: Partner API HTTP ${response.status}: ${apiError}`
      };
    }

    if (!payload) {
      return {
        clusterLabel,
        rows: [],
        error: `${clusterLabel}: Partner API JSON parse edilemedi.`
      };
    }

    return {
      clusterLabel,
      rows: extractPartnerRawRows(payload, clusterLabel),
      error: null
    };
  } catch (err) {
    return {
      clusterLabel,
      rows: [],
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
      return { partners: [], error: "Partner URL yapılandırması boş." };
    }

    const results = await Promise.all(
      partnerUrls.map((partnerUrl) => fetchPartnerCodesFromCluster(partnerUrl, controller.signal))
    );

    const errors = [];
    const mergedPartners = [];

    results.forEach((result) => {
      if (result.error) errors.push(result.error);
      result.partners.forEach((item) => mergedPartners.push(item));
    });

    const partners = normalizePartnerItems(mergedPartners);
    if (partners.length > 0) {
      await savePartnerCodesCache(partners);
      if (errors.length > 0) {
        return { partners, error: `${errors.length}/${partnerUrls.length} cluster alınamadı.` };
      }
      return { partners, error: null };
    }

    const cache = await loadPartnerCodesCache();
    if (cache && cache.partners.length > 0) {
      const cacheDate = cache.updatedAt ? new Date(cache.updatedAt).toLocaleString("tr-TR") : "";
      const suffix = cacheDate ? ` (${cacheDate})` : "";
      return {
        partners: cache.partners,
        error: `Canlı veriler alınamadı, önbellekten gösteriliyor${suffix}.`
      };
    }

    if (errors.length > 0) {
      console.error("Partner API cluster errors:", errors);
      return { partners: [], error: errors[0] };
    }

    return { partners: [], error: "Partner API sonucu boş (status=1 ve code bulunan kayıt yok)." };
  } catch (err) {
    const message = `Partner API fetch hatası: ${err?.message || "Bilinmeyen hata"}`;
    console.error("Partner API fetch error:", err);
    const cache = await loadPartnerCodesCache();
    if (cache && cache.partners.length > 0) {
      const cacheDate = cache.updatedAt ? new Date(cache.updatedAt).toLocaleString("tr-TR") : "";
      const suffix = cacheDate ? ` (${cacheDate})` : "";
      return {
        partners: cache.partners,
        error: `Canlı veriler alınamadı, önbellekten gösteriliyor${suffix}.`
      };
    }
    return { partners: [], error: message };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAllPartnerRows() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 90000);

  try {
    const partnerUrls = buildClusterPartnerUrls(PARTNERS_API_URL);
    if (partnerUrls.length === 0) {
      return { columns: [], rows: [], error: "Partner URL yapılandırması boş.", clusterCount: 0 };
    }

    const results = [];
    for (const partnerUrl of partnerUrls) {
      // Must iterate cluster0..cluster15 in order as requested.
      const result = await fetchPartnerRawRowsFromCluster(partnerUrl, controller.signal);
      results.push(result);
    }

    const mergedRows = [];
    const errors = [];
    results.forEach((result) => {
      if (result.error) errors.push(result.error);
      (result.rows || []).forEach((row) => mergedRows.push(row));
    });

    const clusterRank = (clusterText) => {
      const match = String(clusterText || "").match(/cluster(\d+)/i);
      if (!match) return Number.MAX_SAFE_INTEGER;
      const parsed = Number.parseInt(match[1], 10);
      return Number.isFinite(parsed) ? parsed : Number.MAX_SAFE_INTEGER;
    };

    mergedRows.sort((a, b) => {
      const byCluster = clusterRank(a?.source_cluster) - clusterRank(b?.source_cluster);
      if (byCluster !== 0) return byCluster;
      const byCode = String(a?.code || "").localeCompare(String(b?.code || ""), "tr");
      if (byCode !== 0) return byCode;
      return String(a?.id || "").localeCompare(String(b?.id || ""), "tr");
    });

    const columns = buildPartnerRawColumns(mergedRows);
    const rows = mergedRows.map((row) => {
      const normalized = {};
      columns.forEach((column) => {
        normalized[column] = formatPartnerCellValue(row[column]);
      });
      return normalized;
    });

    let error = null;
    if (errors.length > 0) {
      error = `${errors.length}/${partnerUrls.length} cluster alınamadı.`;
    }
    if (!error && rows.length === 0) {
      error = "Partner API sonucunda gösterilecek kayıt bulunamadı.";
    }

    return {
      columns,
      rows,
      error,
      clusterCount: partnerUrls.length
    };
  } catch (err) {
    return {
      columns: [],
      rows: [],
      error: `Partner verileri alınamadı: ${err?.message || "Bilinmeyen hata"}`,
      clusterCount: 0
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function readJsonFileSafe(filePath) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return {
      exists: true,
      value: parseJsonSafe(raw),
      error: null
    };
  } catch (err) {
    if (err && err.code === "ENOENT") {
      return {
        exists: false,
        value: null,
        error: null
      };
    }
    return {
      exists: false,
      value: null,
      error: err?.message || "Dosya okunamadı."
    };
  }
}

function getCurrentMonthKey() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function normalizeSlackMonth(value) {
  if (value === undefined || value === null) return "";

  if (typeof value === "number" && Number.isFinite(value)) {
    const ts = value > 100000000000 ? value : value * 1000;
    const date = new Date(ts);
    if (!Number.isNaN(date.getTime())) {
      const year = date.getUTCFullYear();
      const month = String(date.getUTCMonth() + 1).padStart(2, "0");
      return `${year}-${month}`;
    }
    return "";
  }

  if (typeof value === "object") {
    const year = Number.parseInt(value.year || value.yil, 10);
    const month = Number.parseInt(value.month || value.ay, 10);
    if (Number.isFinite(year) && Number.isFinite(month) && month >= 1 && month <= 12) {
      return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`;
    }
    return "";
  }

  const raw = String(value).trim();
  if (!raw) return "";
  if (/^\d{4}-\d{2}$/.test(raw)) return raw;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw.slice(0, 7);

  const match = raw.match(/^(\d{4})[./](\d{1,2})(?:[./]\d{1,2})?$/);
  if (match) {
    const year = String(match[1]).padStart(4, "0");
    const month = String(match[2]).padStart(2, "0");
    return `${year}-${month}`;
  }

  const date = new Date(raw);
  if (!Number.isNaN(date.getTime())) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    return `${year}-${month}`;
  }

  return "";
}

function toCountInteger(value) {
  const numeric = toNumber(value);
  if (numeric === null) return 0;
  const parsed = Math.trunc(numeric);
  if (!Number.isFinite(parsed) || parsed < 0) return 0;
  return parsed;
}

function parseSlackRecord(node, monthOverride = "") {
  if (!node || typeof node !== "object") return null;
  const userId = String(node.user_id || node.userId || node.id || "").trim();
  if (!userId) return null;

  const month =
    normalizeSlackMonth(
      monthOverride || node.month || node.ay || node.period || node.date || node.startDate || node.start_date
    ) || getCurrentMonthKey();

  return {
    userId,
    name: String(node.name || node.display_name || node.displayName || "").trim(),
    count: toCountInteger(node.count ?? node.message_count ?? node.messages ?? node.total ?? 0),
    month
  };
}

function extractSlackRecords(payload) {
  const records = [];

  const walk = (node, inheritedMonth = "") => {
    if (!node) return;

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, inheritedMonth));
      return;
    }

    if (typeof node !== "object") return;

    const localMonth =
      normalizeSlackMonth(node.month || node.ay || node.period || node.date || node.startDate || node.start_date) ||
      inheritedMonth;

    const directRecord = parseSlackRecord(node, localMonth);
    if (directRecord) {
      records.push(directRecord);
      return;
    }

    const nestedKeys = ["users", "items", "data", "rows", "months", "result", "results", "list"];
    nestedKeys.forEach((key) => {
      if (Array.isArray(node[key])) {
        walk(node[key], localMonth);
      }
    });
  };

  walk(payload, "");
  return records;
}

function formatSlackMonthLabel(monthKey) {
  if (!/^\d{4}-\d{2}$/.test(String(monthKey || ""))) {
    return String(monthKey || "");
  }
  const date = new Date(`${monthKey}-01T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return String(monthKey || "");
  return new Intl.DateTimeFormat("tr-TR", {
    month: "long",
    year: "numeric",
    timeZone: "UTC"
  }).format(date);
}

async function buildSlackAnalysisReport() {
  const selectedUsers = SLACK_SELECTED_USERS.map((item) => ({ ...item }));
  const selectedIds = new Set(selectedUsers.map((item) => item.id));
  const displayNameById = new Map(selectedUsers.map((item) => [item.id, item.name]));
  const fallbackMonth = getCurrentMonthKey();

  const notices = [];
  let source = "";
  let sourceMissing = false;
  let records = [];
  let error = null;

  const monthlyFile = await readJsonFileSafe(SLACK_MONTHLY_ANALYSIS_FILE);
  if (monthlyFile.error) {
    notices.push(`Aylık kaynak okunamadı: ${monthlyFile.error}`);
  } else if (monthlyFile.exists) {
    source = "data/slack-monthly-analysis.json";
    records = extractSlackRecords(monthlyFile.value);
    if (records.length === 0) {
      notices.push("Aylık kaynak dosyasında gösterilecek kayıt bulunamadı.");
    }
  } else {
    sourceMissing = true;
  }

  if (records.length === 0) {
    const countFile = await readJsonFileSafe(SLACK_COUNTS_FILE);
    if (countFile.error) {
      error = `Slack sonuç dosyası okunamadı: ${countFile.error}`;
    } else if (countFile.exists) {
      source = "slack-counts.json";
      records = extractSlackRecords(countFile.value).map((row) => ({
        ...row,
        month: row.month || fallbackMonth
      }));
      if (sourceMissing) {
        notices.push(
          "Aylık dosya bulunamadı. Mevcut toplam sonuçlar tek ay satırı olarak gösteriliyor."
        );
      }
    } else if (sourceMissing) {
      error = "Slack analiz dosyası bulunamadı. Önce script çıktısını kaydedin.";
    }
  }

  const monthlyUserCounts = new Map();
  const totalByUser = new Map(selectedUsers.map((item) => [item.id, 0]));

  records.forEach((record) => {
    if (!selectedIds.has(record.userId)) return;
    if (record.name) {
      displayNameById.set(record.userId, record.name);
    }
    const month = normalizeSlackMonth(record.month) || fallbackMonth;
    if (!monthlyUserCounts.has(month)) {
      monthlyUserCounts.set(month, new Map());
    }
    const monthMap = monthlyUserCounts.get(month);
    const current = monthMap.get(record.userId) || 0;
    const nextValue = current + toCountInteger(record.count);
    monthMap.set(record.userId, nextValue);
    totalByUser.set(record.userId, (totalByUser.get(record.userId) || 0) + toCountInteger(record.count));
  });

  selectedUsers.forEach((item) => {
    item.name = displayNameById.get(item.id) || item.name;
  });

  const monthKeys = Array.from(monthlyUserCounts.keys()).sort((a, b) => a.localeCompare(b, "tr"));
  const rows = monthKeys.map((monthKey) => {
    const monthMap = monthlyUserCounts.get(monthKey) || new Map();
    const counts = selectedUsers.map((selected) => monthMap.get(selected.id) || 0);
    const total = counts.reduce((sum, count) => sum + count, 0);
    return {
      monthKey,
      monthLabel: formatSlackMonthLabel(monthKey),
      counts,
      total
    };
  });

  const totalCounts = selectedUsers.map((selected) => totalByUser.get(selected.id) || 0);
  const grandTotal = totalCounts.reduce((sum, count) => sum + count, 0);

  return {
    users: selectedUsers,
    rows,
    totals: {
      counts: totalCounts,
      grandTotal
    },
    source,
    notice: notices.length > 0 ? notices.join(" ") : null,
    error
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getTodayIsoDate() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeIsoDateInput(value) {
  if (typeof value !== "string") return "";
  const trimmed = value.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : "";
}

function parseIsoDateToUtc(value, endOfDay = false) {
  const normalized = normalizeIsoDateInput(value);
  if (!normalized) return null;
  const [yearRaw, monthRaw, dayRaw] = normalized.split("-");
  const year = Number.parseInt(yearRaw, 10);
  const month = Number.parseInt(monthRaw, 10);
  const day = Number.parseInt(dayRaw, 10);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  const date = new Date(
    Date.UTC(year, month - 1, day, endOfDay ? 23 : 0, endOfDay ? 59 : 0, endOfDay ? 59 : 0, endOfDay ? 999 : 0)
  );
  if (Number.isNaN(date.getTime())) return null;

  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() + 1 !== month ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date;
}

function validateSlackDateRange(startDate, endDate) {
  const start = parseIsoDateToUtc(startDate, false);
  const end = parseIsoDateToUtc(endDate, true);
  if (!start || !end) {
    return "Başlangıç ve bitiş tarihi YYYY-AA-GG formatında olmalıdır.";
  }
  if (start.getTime() > end.getTime()) {
    return "Başlangıç tarihi bitiş tarihinden büyük olamaz.";
  }
  return null;
}

function shouldCountSlackMessage(message) {
  if (!message || typeof message !== "object") return false;
  if (!message.user) return false;
  if (message.subtype) return false;
  return true;
}

async function slackApiGet(method, token, params = {}, retryCount = 0) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });

  const url = `${SLACK_API_BASE_URL}/${method}${query.toString() ? `?${query.toString()}` : ""}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, SLACK_API_TIMEOUT_MS));

  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        "Content-Type": "application/json"
      }
    });

    if (response.status === 429) {
      if (retryCount >= SLACK_MAX_RATE_LIMIT_RETRY) {
        throw new Error(`${method} HTTP 429 (max retry aşıldı)`);
      }
      const retryAfterRaw = response.headers.get("retry-after");
      const retryAfterSeconds = Number.parseInt(retryAfterRaw || "1", 10);
      const waitMs = (Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : 1) * 1000 + 250;
      await sleep(waitMs);
      return slackApiGet(method, token, params, retryCount + 1);
    }

    const raw = await response.text();
    const data = parseJsonSafe(raw);

    if (!response.ok) {
      const apiMessage =
        (data && typeof data === "object" && String(data.error || data.message || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      throw new Error(`${method} HTTP ${response.status}: ${apiMessage}`);
    }

    if (!data || typeof data !== "object") {
      throw new Error(`${method} API yanıtı JSON parse edilemedi.`);
    }

    if (!data.ok) {
      const needed = data.needed ? ` needed=${data.needed}` : "";
      const provided = data.provided ? ` provided=${data.provided}` : "";
      throw new Error(`${method} API error: ${data.error || "unknown_error"}${needed}${provided}`);
    }

    return data;
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(`${method} timeout (${Math.round(Math.max(1000, SLACK_API_TIMEOUT_MS) / 1000)}s)`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

function toBoundedInt(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < min) return min;
  if (parsed > max) return max;
  return parsed;
}

function normalizeSlackChannelTypes(raw) {
  const allowed = new Set(["public_channel", "private_channel", "mpim", "im"]);
  const tokens = String(raw || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter((item) => allowed.has(item));
  if (tokens.length === 0) return "private_channel";
  return Array.from(new Set(tokens)).join(",");
}

function getSlackReplyReportCacheKey(startDate, endDate) {
  const version = "v2";
  return `${version}__${startDate || ""}__${endDate || ""}__${SLACK_SELECTED_USERS.map((item) => item.id).join(",")}`;
}

function getCachedSlackReplyReport(cacheKey) {
  const cached = slackReplyReportCache.get(cacheKey);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    slackReplyReportCache.delete(cacheKey);
    return null;
  }
  return cached.value;
}

function setCachedSlackReplyReport(cacheKey, value) {
  const ttl = Math.max(1000, SLACK_ANALYSIS_CACHE_TTL_MS);
  slackReplyReportCache.set(cacheKey, {
    expiresAt: Date.now() + ttl,
    value
  });
}

async function runWithConcurrency(items, concurrency, worker, shouldStop) {
  const list = Array.isArray(items) ? items : [];
  const results = new Array(list.length);
  const workerCount = Math.max(1, Math.min(list.length || 1, toBoundedInt(concurrency, 1, 1, 20)));
  let index = 0;

  const runner = async () => {
    while (true) {
      if (typeof shouldStop === "function" && shouldStop()) return;
      const current = index;
      index += 1;
      if (current >= list.length) return;
      try {
        results[current] = await worker(list[current], current);
      } catch (err) {
        results[current] = { error: err };
      }
    }
  };

  await Promise.all(Array.from({ length: workerCount }, () => runner()));
  return results;
}

async function listSlackChannelsForAnalysis(token, channelTypes) {
  const channels = [];
  let cursor = "";
  const types = normalizeSlackChannelTypes(channelTypes);

  do {
    const data = await slackApiGet("conversations.list", token, {
      types,
      exclude_archived: "true",
      limit: SLACK_DEFAULT_LIMIT,
      cursor
    });

    (data.channels || []).forEach((channel) => channels.push(channel));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor);

  return channels;
}

async function listSlackChannelMessagesForAnalysis(token, channelId, oldest, latest, maxPages) {
  const messages = [];
  let cursor = "";
  let pageCount = 0;
  const pageLimit = toBoundedInt(maxPages, SLACK_ANALYSIS_MAX_HISTORY_PAGES, 1, 50);

  do {
    pageCount += 1;
    const data = await slackApiGet("conversations.history", token, {
      channel: channelId,
      limit: SLACK_DEFAULT_LIMIT,
      oldest,
      latest,
      inclusive: "true",
      cursor
    });
    (data.messages || []).forEach((message) => messages.push(message));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor && pageCount < pageLimit);

  return {
    messages,
    truncated: Boolean(cursor)
  };
}

async function listSlackThreadRepliesForAnalysis(token, channelId, threadTs, oldest, latest, maxPages) {
  const replies = [];
  let cursor = "";
  let pageCount = 0;
  const pageLimit = toBoundedInt(maxPages, SLACK_ANALYSIS_MAX_REPLY_PAGES, 1, 50);

  do {
    pageCount += 1;
    const data = await slackApiGet("conversations.replies", token, {
      channel: channelId,
      ts: threadTs,
      limit: SLACK_DEFAULT_LIMIT,
      oldest,
      latest,
      inclusive: "true",
      cursor
    });
    (data.messages || []).forEach((message) => replies.push(message));
    cursor = data.response_metadata?.next_cursor || "";
  } while (cursor && pageCount < pageLimit);

  return {
    replies,
    truncated: Boolean(cursor)
  };
}

function buildSlackReplyRowsFromCounts(
  replyCountByUserId,
  requestCountByUserId = new Map(),
  nameByUserId = new Map()
) {
  return SLACK_SELECTED_USERS.map((selected) => ({
    userId: selected.id,
    name: String(nameByUserId.get(selected.id) || selected.name || "").trim() || selected.id,
    requestCount: toCountInteger(requestCountByUserId.get(selected.id) || 0),
    replyCount: toCountInteger(replyCountByUserId.get(selected.id) || 0),
    count: toCountInteger(replyCountByUserId.get(selected.id) || 0)
  })).sort(
    (a, b) =>
      b.replyCount - a.replyCount || b.requestCount - a.requestCount || a.name.localeCompare(b.name, "tr")
  );
}

function buildSlackReplyReportModel({
  requested = false,
  rows = [],
  totalRequests = null,
  source = "",
  notice = null,
  error = null,
  meta = null
} = {}) {
  const allowedIds = new Set(SLACK_SELECTED_USERS.map((item) => item.id));
  const providedRows = Array.isArray(rows) ? rows : [];
  const hasProvidedRows = providedRows.length > 0;
  const replyCountByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, 0]));
  const requestCountByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, 0]));
  const nameByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, item.name]));

  providedRows.forEach((row) => {
    const userId = String(row?.userId || row?.user_id || row?.id || "").trim();
    if (!allowedIds.has(userId)) return;
    const replyCount = toCountInteger(row?.replyCount ?? row?.reply_count ?? row?.count);
    const requestCount = toCountInteger(row?.requestCount ?? row?.request_count ?? 0);
    replyCountByUserId.set(userId, replyCount);
    requestCountByUserId.set(userId, requestCount);
    const nameText = String(row?.name || row?.userName || row?.user_name || "").trim();
    if (nameText) {
      nameByUserId.set(userId, nameText);
    }
  });

  const normalizedRows = hasProvidedRows
    ? buildSlackReplyRowsFromCounts(replyCountByUserId, requestCountByUserId, nameByUserId)
    : [];
  const computedTotalRequests = normalizedRows.reduce((sum, row) => sum + row.requestCount, 0);
  const normalizedTotalRequests =
    totalRequests === null || totalRequests === undefined
      ? computedTotalRequests
      : toCountInteger(totalRequests);
  const totalReplies = normalizedRows.reduce((sum, row) => sum + row.replyCount, 0);

  const normalizedMeta = {
    channelsTotal: Number.isFinite(Number(meta?.channelsTotal)) ? Number(meta.channelsTotal) : 0,
    channelsScanned: Number.isFinite(Number(meta?.channelsScanned)) ? Number(meta.channelsScanned) : 0,
    threadsScanned: Number.isFinite(Number(meta?.threadsScanned)) ? Number(meta.threadsScanned) : 0
  };

  return {
    requested: Boolean(requested),
    rows: requested ? normalizedRows : [],
    rowsJson: JSON.stringify(normalizedRows),
    totalRequests: normalizedTotalRequests,
    totalReplies,
    source: String(source || ""),
    notice: notice ? String(notice) : null,
    error: error ? String(error) : null,
    meta: normalizedMeta
  };
}

async function fetchSlackReplyReportForRange(startDate, endDate) {
  const token = String(process.env.SLACK_BOT_TOKEN || "").trim();
  if (!token) {
    return buildSlackReplyReportModel({
      requested: true,
      error: "SLACK_BOT_TOKEN gerekli."
    });
  }

  const startUtc = parseIsoDateToUtc(startDate, false);
  const endUtc = parseIsoDateToUtc(endDate, true);
  if (!startUtc || !endUtc) {
    return buildSlackReplyReportModel({
      requested: true,
      error: "Tarih aralığı geçersiz."
    });
  }

  const cacheKey = getSlackReplyReportCacheKey(startDate, endDate);
  const cached = getCachedSlackReplyReport(cacheKey);
  if (cached) {
    return {
      ...cached,
      notice: cached.notice ? `${cached.notice} (Önbellek)` : "Önbellekten gösteriliyor."
    };
  }

  const oldest = String(Math.floor(startUtc.getTime() / 1000));
  const latest = String(Math.floor(endUtc.getTime() / 1000));
  const channelTypes = normalizeSlackChannelTypes(SLACK_ANALYSIS_CHANNEL_TYPES_RAW);
  const maxRuntimeMs = toBoundedInt(SLACK_ANALYSIS_MAX_RUNTIME_MS, 45000, 5000, 180000);
  const maxChannels = toBoundedInt(SLACK_ANALYSIS_MAX_CHANNELS, 80, 1, 500);
  const maxThreadsPerChannel = toBoundedInt(SLACK_ANALYSIS_MAX_THREADS_PER_CHANNEL, 120, 1, 2000);
  const maxHistoryPages = toBoundedInt(SLACK_ANALYSIS_MAX_HISTORY_PAGES, 8, 1, 50);
  const maxReplyPages = toBoundedInt(SLACK_ANALYSIS_MAX_REPLY_PAGES, 6, 1, 50);
  const channelConcurrency = toBoundedInt(SLACK_ANALYSIS_CHANNEL_CONCURRENCY, 2, 1, 8);
  const threadConcurrency = toBoundedInt(SLACK_ANALYSIS_THREAD_CONCURRENCY, 4, 1, 10);
  const startedAt = Date.now();
  const deadlineAt = startedAt + maxRuntimeMs;
  const isDeadlineExceeded = () => Date.now() >= deadlineAt;

  const selectedIds = new Set(SLACK_SELECTED_USERS.map((item) => item.id));
  const replyCountByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, 0]));
  let totalRequestCount = 0;
  const nameByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, item.name]));
  const seenRequestMessages = new Set();

  let channels = [];
  try {
    channels = await listSlackChannelsForAnalysis(token, channelTypes);
  } catch (err) {
    return buildSlackReplyReportModel({
      requested: true,
      error: `Slack kanal listesi alınamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }

  if (!channels.length) {
    return buildSlackReplyReportModel({
      requested: true,
      rows: [],
      source: "Slack API",
      notice: "Erişilebilen kanal bulunamadı."
    });
  }

  let warningCount = 0;
  const warningSamples = [];
  const addWarning = (message) => {
    warningCount += 1;
    if (warningSamples.length < 3) {
      warningSamples.push(String(message || "").trim());
    }
  };

  let historyTruncatedChannels = 0;
  let replyTruncatedThreads = 0;
  let threadLimitAppliedChannels = 0;
  let runtimeStopped = false;
  let runtimeSkippedChannels = 0;
  const scannedChannelNames = new Set();

  const metrics = {
    channelsTotal: channels.length,
    channelsScanned: 0,
    threadsScanned: 0
  };

  const limitedChannels = channels.slice(0, maxChannels);
  const skippedByChannelLimit = Math.max(0, channels.length - limitedChannels.length);

  await runWithConcurrency(
    limitedChannels,
    channelConcurrency,
    async (channel) => {
      if (isDeadlineExceeded()) {
        runtimeStopped = true;
        runtimeSkippedChannels += 1;
        return;
      }

      const channelId = String(channel?.id || "").trim();
      const channelName = String(channel?.name || channelId || "").trim();
      if (!channelId) return;

      metrics.channelsScanned += 1;
      if (channelName) {
        scannedChannelNames.add(channelName);
      }

      let messagesResult;
      try {
        messagesResult = await listSlackChannelMessagesForAnalysis(
          token,
          channelId,
          oldest,
          latest,
          maxHistoryPages
        );
      } catch (err) {
        addWarning(`${channelName}: ${err?.message || "history okunamadı"}`);
        return;
      }

      if (messagesResult.truncated) {
        historyTruncatedChannels += 1;
      }

      const threadRoots = (messagesResult.messages || []).filter((message) => {
        if (shouldCountSlackMessage(message)) {
          const messageTs = String(message.ts || "").trim();
          const threadTs = String(message.thread_ts || "").trim();
          const isThreadReply = Boolean(threadTs && threadTs !== messageTs);
          if (messageTs && !isThreadReply) {
            const requestKey = `${channelId}:${messageTs}`;
            if (!seenRequestMessages.has(requestKey)) {
              seenRequestMessages.add(requestKey);
              totalRequestCount += 1;
            }
          }
        }

        const replyCount = Number(message?.reply_count || 0);
        return Number.isFinite(replyCount) && replyCount > 0 && String(message?.ts || "").trim();
      });

      if (threadRoots.length > maxThreadsPerChannel) {
        threadLimitAppliedChannels += 1;
      }

      const limitedThreads = threadRoots.slice(0, maxThreadsPerChannel);

      await runWithConcurrency(
        limitedThreads,
        threadConcurrency,
        async (message) => {
          if (isDeadlineExceeded()) {
            runtimeStopped = true;
            return;
          }

          metrics.threadsScanned += 1;

          let repliesResult;
          try {
            repliesResult = await listSlackThreadRepliesForAnalysis(
              token,
              channelId,
              message.ts,
              oldest,
              latest,
              maxReplyPages
            );
          } catch (err) {
            addWarning(`${channelName} thread ${message?.ts || "-"}: ${err?.message || "replies okunamadı"}`);
            return;
          }

          if (repliesResult.truncated) {
            replyTruncatedThreads += 1;
          }

          // Same thread + same user counts once, different users each count once.
          const uniqueUsersInThread = new Set();

          for (const reply of repliesResult.replies || []) {
            if (!reply || reply.ts === message.ts) continue;
            if (!shouldCountSlackMessage(reply)) continue;

            const userId = String(reply.user || "").trim();
            if (!selectedIds.has(userId)) continue;
            uniqueUsersInThread.add(userId);
          }

          for (const userId of uniqueUsersInThread) {
            replyCountByUserId.set(userId, (replyCountByUserId.get(userId) || 0) + 1);
          }
        },
        isDeadlineExceeded
      );
    },
    isDeadlineExceeded
  );

  if (isDeadlineExceeded()) {
    runtimeStopped = true;
    runtimeSkippedChannels += Math.max(0, limitedChannels.length - metrics.channelsScanned);
  }

  const requestCountByUserId = new Map(
    SLACK_SELECTED_USERS.map((item) => [item.id, totalRequestCount])
  );
  const rows = buildSlackReplyRowsFromCounts(replyCountByUserId, requestCountByUserId, nameByUserId);
  const noticeParts = [];

  if (runtimeStopped) {
    noticeParts.push(
      `Süre limiti (${Math.round(maxRuntimeMs / 1000)} sn) nedeniyle kısmi sonuç gösteriliyor.`
    );
  }
  if (runtimeSkippedChannels > 0) {
    noticeParts.push(`${runtimeSkippedChannels} kanal süre limiti nedeniyle atlandı.`);
  }
  if (skippedByChannelLimit > 0) {
    noticeParts.push(`Kanal limiti nedeniyle ${skippedByChannelLimit} kanal taranmadı.`);
  }
  if (historyTruncatedChannels > 0) {
    noticeParts.push(`${historyTruncatedChannels} kanalda geçmiş mesajlar sayfa limiti nedeniyle kısıtlandı.`);
  }
  if (threadLimitAppliedChannels > 0) {
    noticeParts.push(`${threadLimitAppliedChannels} kanalda thread limiti uygulandı.`);
  }
  if (replyTruncatedThreads > 0) {
    noticeParts.push(`${replyTruncatedThreads} thread'de yanıtlar sayfa limiti nedeniyle kısıtlandı.`);
  }
  if (warningCount > 0) {
    noticeParts.push(
      `${warningCount} kanal/thread çağrısı hatalı. ${warningSamples[0] ? `İlk hata: ${warningSamples[0]}` : ""}`
    );
  }

  const channelNames = Array.from(scannedChannelNames)
    .map((item) => String(item || "").trim())
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b, "tr"));
  if (channelNames.length > 0) {
    const maxNamesInNotice = 25;
    const visibleNames = channelNames.slice(0, maxNamesInNotice);
    const extraCount = Math.max(0, channelNames.length - visibleNames.length);
    noticeParts.push(
      `Kanallar: ${visibleNames.join(", ")}${extraCount > 0 ? ` (+${extraCount})` : ""}`
    );
  }

  noticeParts.push(
    `Tip: ${channelTypes}, Kanal: ${metrics.channelsScanned}/${metrics.channelsTotal}, Thread: ${metrics.threadsScanned}`
  );

  const notice = noticeParts.filter(Boolean).join(" ");

  const report = buildSlackReplyReportModel({
    requested: true,
    rows,
    totalRequests: totalRequestCount,
    source: "Slack API",
    notice,
    error: null,
    meta: metrics
  });
  setCachedSlackReplyReport(cacheKey, report);
  return report;
}

function normalizeSqlDateValue(value) {
  if (!value) return "";
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  const raw = String(value).trim();
  const matched = raw.match(/^(\d{4}-\d{2}-\d{2})/);
  return matched ? matched[1] : raw;
}

function normalizeSqlDateTimeValue(value) {
  if (!value) return "";
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }
  return String(value).trim();
}

async function fetchSlackSavedReports({ startDate, endDate, limit = 25 }) {
  const normalizedStartDate = normalizeIsoDateInput(startDate);
  const normalizedEndDate = normalizeIsoDateInput(endDate);

  if (!normalizedStartDate || !normalizedEndDate) {
    return {
      rows: [],
      error: "SQL sorgu tarihleri geçersiz.",
      filters: {
        startDate: normalizedStartDate || getTodayIsoDate(),
        endDate: normalizedEndDate || getTodayIsoDate()
      }
    };
  }

  const limitValue = toBoundedInt(limit, 25, 1, 100);

  try {
    const runResult = await pool.query(
      `
        SELECT
          r.id,
          r.start_date,
          r.end_date,
          r.total_requests,
          r.total_replies,
          r.row_count,
          COALESCE(r.save_count, 1) AS save_count,
          r.created_at,
          r.updated_at,
          COALESCE(u.display_name, u.username, '-') AS created_by_name
        FROM slack_reply_analysis_runs r
        LEFT JOIN users u ON u.id = r.created_by
        WHERE r.start_date <= $2
          AND r.end_date >= $1
        ORDER BY r.start_date DESC, r.end_date DESC, r.id DESC
        LIMIT $3
      `,
      [normalizedStartDate, normalizedEndDate, limitValue]
    );

    const runs = runResult.rows || [];
    if (runs.length === 0) {
      return {
        rows: [],
        error: null,
        filters: {
          startDate: normalizedStartDate,
          endDate: normalizedEndDate
        }
      };
    }

    const runIds = runs.map((run) => Number(run.id)).filter((id) => Number.isInteger(id));
    const itemResult = await pool.query(
      `
        SELECT
          run_id,
          user_id,
          user_name,
          request_count,
          reply_count
        FROM slack_reply_analysis_items
        WHERE run_id = ANY($1::int[])
        ORDER BY run_id DESC, reply_count DESC, request_count DESC, user_name ASC
      `,
      [runIds]
    );

    const itemsByRunId = new Map();
    (itemResult.rows || []).forEach((item) => {
      const runId = Number(item.run_id);
      if (!Number.isInteger(runId)) return;
      if (!itemsByRunId.has(runId)) {
        itemsByRunId.set(runId, []);
      }
      itemsByRunId.get(runId).push({
        userId: String(item.user_id || "").trim(),
        userName: String(item.user_name || "").trim(),
        requestCount: toCountInteger(item.request_count),
        replyCount: toCountInteger(item.reply_count)
      });
    });

    const rows = runs.map((run) => {
      const runId = Number(run.id);
      const itemRows = itemsByRunId.get(runId) || [];
      return {
        runId,
        startDate: normalizeSqlDateValue(run.start_date),
        endDate: normalizeSqlDateValue(run.end_date),
        totalRequests: toCountInteger(run.total_requests),
        totalReplies: toCountInteger(run.total_replies),
        rowCount: toCountInteger(run.row_count),
        saveCount: toCountInteger(run.save_count) || 1,
        createdByName: String(run.created_by_name || "-").trim(),
        createdAt: normalizeSqlDateTimeValue(run.created_at),
        updatedAt: normalizeSqlDateTimeValue(run.updated_at),
        items: itemRows
      };
    });

    return {
      rows,
      error: null,
      filters: {
        startDate: normalizedStartDate,
        endDate: normalizedEndDate
      }
    };
  } catch (err) {
    return {
      rows: [],
      error: `SQL kayıtları alınamadı: ${err?.message || "Bilinmeyen hata"}`,
      filters: {
        startDate: normalizedStartDate,
        endDate: normalizedEndDate
      }
    };
  }
}

async function saveSlackReplyReportToDb({ startDate, endDate, rows, userId, totalRequests = null }) {
  const normalizedReport = buildSlackReplyReportModel({
    requested: true,
    rows,
    totalRequests
  });
  const normalizedRows = normalizedReport.rows;
  const normalizedTotalRequests = normalizedReport.totalRequests;
  const totalReplies = normalizedRows.reduce((sum, row) => sum + toCountInteger(row.replyCount), 0);
  const ownerId = userId || null;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const existingRunResult = await client.query(
      `
        SELECT id, COALESCE(save_count, 1) AS save_count
        FROM slack_reply_analysis_runs
        WHERE start_date = $1
          AND end_date = $2
          AND created_by IS NOT DISTINCT FROM $3
        ORDER BY id DESC
        LIMIT 1
      `,
      [startDate, endDate, ownerId]
    );

    let runId = null;
    let saveCount = 1;
    let mode = "inserted";

    if (existingRunResult.rows[0]?.id) {
      runId = Number(existingRunResult.rows[0].id);
      saveCount = toCountInteger(existingRunResult.rows[0].save_count) + 1;
      mode = "updated";

      await client.query(
        `
          UPDATE slack_reply_analysis_runs
          SET
            total_requests = $2,
            total_replies = $3,
            row_count = $4,
            source = $5,
            save_count = $6,
            updated_at = now()
          WHERE id = $1
        `,
        [runId, normalizedTotalRequests, totalReplies, normalizedRows.length, "slack-analysis-ui", saveCount]
      );

      await client.query(
        `
          DELETE FROM slack_reply_analysis_items
          WHERE run_id = $1
        `,
        [runId]
      );
    } else {
      const runResult = await client.query(
        `
          INSERT INTO slack_reply_analysis_runs (
            start_date,
            end_date,
            total_requests,
            total_replies,
            row_count,
            save_count,
            source,
            created_by
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          RETURNING id, COALESCE(save_count, 1) AS save_count
        `,
        [
          startDate,
          endDate,
          normalizedTotalRequests,
          totalReplies,
          normalizedRows.length,
          1,
          "slack-analysis-ui",
          ownerId
        ]
      );
      runId = Number(runResult.rows[0]?.id);
      saveCount = toCountInteger(runResult.rows[0]?.save_count) || 1;
    }

    for (const row of normalizedRows) {
      await client.query(
        `
          INSERT INTO slack_reply_analysis_items (
            run_id,
            user_id,
            user_name,
            request_count,
            reply_count
          )
          VALUES ($1, $2, $3, $4, $5)
        `,
        [runId, row.userId, row.name, row.requestCount, row.replyCount]
      );
    }

    await client.query("COMMIT");
    return {
      runId,
      rowCount: normalizedRows.length,
      totalRequests: normalizedTotalRequests,
      totalReplies,
      saveCount,
      mode
    };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

function stringifyPayload(payload) {
  if (typeof payload === "string") return payload;
  try {
    return JSON.stringify(payload ?? {}, null, 2);
  } catch (err) {
    return String(payload ?? "");
  }
}

function getObjectValueByNormalizedKey(object, normalizedKeyList) {
  if (!object || typeof object !== "object") return undefined;
  const normalizedKeys = new Set(normalizedKeyList.map((key) => normalizeTokenName(key)));

  for (const [key, value] of Object.entries(object)) {
    if (normalizedKeys.has(normalizeTokenName(key))) {
      return value;
    }
  }

  return undefined;
}

function getDeepValueByNormalizedKey(node, normalizedKeyList, maxDepth = 3) {
  if (maxDepth < 0 || node === null || node === undefined) return undefined;
  if (typeof node !== "object") return undefined;

  const direct = getObjectValueByNormalizedKey(node, normalizedKeyList);
  if (direct !== undefined) return direct;

  for (const value of Object.values(node)) {
    const found = getDeepValueByNormalizedKey(value, normalizedKeyList, maxDepth - 1);
    if (found !== undefined) return found;
  }

  return undefined;
}

function getObjectValueByKeyMatcher(object, matcher) {
  if (!object || typeof object !== "object") return undefined;

  for (const [key, value] of Object.entries(object)) {
    const normalizedKey = normalizeTokenName(key);
    if (matcher(normalizedKey, key, value)) {
      return value;
    }
  }

  return undefined;
}

function getDeepValueByKeyMatcher(node, matcher, maxDepth = 3) {
  if (maxDepth < 0 || node === null || node === undefined) return undefined;
  if (typeof node !== "object") return undefined;

  const direct = getObjectValueByKeyMatcher(node, matcher);
  if (direct !== undefined) return direct;

  for (const value of Object.values(node)) {
    const found = getDeepValueByKeyMatcher(value, matcher, maxDepth - 1);
    if (found !== undefined) return found;
  }

  return undefined;
}

function findFirstNumericLikeValue(node, maxDepth = 3) {
  if (maxDepth < 0 || node === null || node === undefined) return undefined;

  if (typeof node === "number") {
    return Number.isFinite(node) ? node : undefined;
  }

  if (typeof node === "string") {
    return node.trim() ? node : undefined;
  }

  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findFirstNumericLikeValue(item, maxDepth - 1);
      if (found !== undefined) return found;
    }
    return undefined;
  }

  if (typeof node === "object") {
    const preferred = [
      "amount",
      "value",
      "total",
      "sum",
      "count",
      "websitesaleamount",
      "obiletsaleamount"
    ];
    for (const [key, value] of Object.entries(node)) {
      if (preferred.includes(normalizeTokenName(key))) {
        const found = findFirstNumericLikeValue(value, maxDepth - 1);
        if (found !== undefined) return found;
      }
    }
    for (const value of Object.values(node)) {
      const found = findFirstNumericLikeValue(value, maxDepth - 1);
      if (found !== undefined) return found;
    }
  }

  return undefined;
}

function toNumber(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "object") {
    const scalar = findFirstNumericLikeValue(value, 4);
    if (scalar !== undefined && scalar !== value) {
      return toNumber(scalar);
    }
    return null;
  }

  let raw = String(value).trim();
  if (!raw) return null;
  raw = raw.replace(/\s+/g, "");

  if (raw.includes(",") && raw.includes(".")) {
    if (raw.lastIndexOf(",") > raw.lastIndexOf(".")) {
      raw = raw.replace(/\./g, "").replace(",", ".");
    } else {
      raw = raw.replace(/,/g, "");
    }
  } else if (raw.includes(",") && !raw.includes(".")) {
    raw = raw.replace(",", ".");
  }

  const parsed = Number.parseFloat(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatCurrencyTry(value) {
  const amount = toNumber(value);
  if (amount === null) return "";
  return `${new Intl.NumberFormat("tr-TR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(amount)} TL`;
}

function formatDateParts(year, month, day) {
  const yyyy = String(year).padStart(4, "0");
  const mm = String(month).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeDateToDay(value) {
  if (value === undefined || value === null) return "";

  if (Array.isArray(value)) {
    if (value.length >= 3) {
      const year = Number.parseInt(value[0], 10);
      const month = Number.parseInt(value[1], 10);
      const day = Number.parseInt(value[2], 10);
      if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
        return formatDateParts(year, month, day);
      }
    }
    for (const item of value) {
      const normalized = normalizeDateToDay(item);
      if (normalized) return normalized;
    }
  }

  if (typeof value === "object") {
    const yearValue = getObjectValueByKeyMatcher(
      value,
      (key) => key === "year" || key === "yil" || key.endsWith("year")
    );
    const monthValue = getObjectValueByKeyMatcher(
      value,
      (key) => key === "month" || key === "ay" || key.endsWith("month")
    );
    const dayValue = getObjectValueByKeyMatcher(
      value,
      (key) => key === "day" || key === "gun" || key.endsWith("day")
    );

    const year = Number.parseInt(yearValue, 10);
    const month = Number.parseInt(monthValue, 10);
    const day = Number.parseInt(dayValue, 10);
    if (Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)) {
      return formatDateParts(year, month, day);
    }

    const rawObjectDate = getDeepValueByKeyMatcher(
      value,
      (key) => key.includes("date") || key.includes("tarih"),
      3
    );
    const normalizedObjectDate = normalizeDateToDay(rawObjectDate);
    if (normalizedObjectDate) return normalizedObjectDate;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    const numeric = Math.trunc(value);
    const abs = Math.abs(numeric);

    if (abs >= 10000101 && abs <= 99991231) {
      const raw = String(abs).padStart(8, "0");
      const year = raw.slice(0, 4);
      const month = raw.slice(4, 6);
      const day = raw.slice(6, 8);
      return formatDateParts(year, month, day);
    }

    if (abs >= 100001 && abs <= 999912) {
      const raw = String(abs).padStart(6, "0");
      const year = raw.slice(0, 4);
      const month = raw.slice(4, 6);
      return formatDateParts(year, month, 1);
    }

    if (abs >= 1000000000 && abs < 100000000000) {
      const secondsDate = new Date(numeric * 1000);
      if (!Number.isNaN(secondsDate.getTime())) {
        return formatDateParts(secondsDate.getFullYear(), secondsDate.getMonth() + 1, secondsDate.getDate());
      }
    }

    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return formatDateParts(date.getFullYear(), date.getMonth() + 1, date.getDate());
    }
  }

  const raw = String(value).trim();
  if (!raw) return "";

  const isoMatch = raw.match(/(\d{4}-\d{2}-\d{2})/);
  if (isoMatch) return isoMatch[1];

  const compactIsoMatch = raw.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (compactIsoMatch) {
    return formatDateParts(compactIsoMatch[1], compactIsoMatch[2], compactIsoMatch[3]);
  }

  const yearMonthMatch = raw.match(/^(\d{4})[-./](\d{2})$/);
  if (yearMonthMatch) {
    return formatDateParts(yearMonthMatch[1], yearMonthMatch[2], 1);
  }

  const compactYearMonthMatch = raw.match(/^(\d{4})(\d{2})$/);
  if (compactYearMonthMatch) {
    return formatDateParts(compactYearMonthMatch[1], compactYearMonthMatch[2], 1);
  }

  const trMatch = raw.match(/^(\d{2})[./-](\d{2})[./-](\d{4})$/);
  if (trMatch) {
    return formatDateParts(trMatch[3], trMatch[2], trMatch[1]);
  }

  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return formatDateParts(parsed.getFullYear(), parsed.getMonth() + 1, parsed.getDate());
  }

  return "";
}

function parseIsoDateUtc(value) {
  const normalized = normalizeDateToDay(value);
  if (!normalized) return null;
  const [yearText, monthText, dayText] = normalized.split("-");
  const year = Number.parseInt(yearText, 10);
  const month = Number.parseInt(monthText, 10);
  const day = Number.parseInt(dayText, 10);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  const parsed = new Date(Date.UTC(year, month - 1, day));
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed;
}

function formatUtcDate(date) {
  return formatDateParts(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
}

function buildDailyRequestRanges(startDate, endDate) {
  const start = parseIsoDateUtc(startDate);
  const end = parseIsoDateUtc(endDate);
  if (!start || !end) return [];

  const from = start.getTime() <= end.getTime() ? start : end;
  const to = start.getTime() <= end.getTime() ? end : start;
  const cursor = new Date(from.getTime());
  const ranges = [];

  while (cursor.getTime() <= to.getTime()) {
    const day = formatUtcDate(cursor);
    ranges.push({
      label: day,
      startDate: day,
      endDate: day
    });
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    if (ranges.length > 4000) break;
  }

  return ranges;
}

function buildMonthlyRequestRanges(startDate, endDate) {
  const start = parseIsoDateUtc(startDate);
  const end = parseIsoDateUtc(endDate);
  if (!start || !end) return [];

  const from = start.getTime() <= end.getTime() ? start : end;
  const to = start.getTime() <= end.getTime() ? end : start;
  const ranges = [];
  let cursor = new Date(from.getTime());

  while (cursor.getTime() <= to.getTime()) {
    const year = cursor.getUTCFullYear();
    const month = cursor.getUTCMonth();
    const monthLabel = `${String(year).padStart(4, "0")}-${String(month + 1).padStart(2, "0")}`;
    const monthEnd = new Date(Date.UTC(year, month + 1, 0));
    const rangeEnd = monthEnd.getTime() <= to.getTime() ? monthEnd : to;

    ranges.push({
      label: monthLabel,
      startDate: formatUtcDate(cursor),
      endDate: formatUtcDate(rangeEnd)
    });

    const next = new Date(rangeEnd.getTime());
    next.setUTCDate(next.getUTCDate() + 1);
    cursor = next;
    if (ranges.length > 600) break;
  }

  return ranges;
}

function extractSalesRowsFromPayload(payload) {
  const rows = [];
  const codeKeys = ["code", "partner-code", "partner_code", "partnercode"];
  const websiteKeys = ["WebsiteSaleAmount", "website-sale-amount", "website_sale_amount"];
  const obiletKeys = ["ObiletSaleAmount", "oBiletSaleAmount", "obilet-sale-amount", "obilet_sale_amount"];

  const pushRow = (node) => {
    if (!node || typeof node !== "object") return false;

    const codeValue = getObjectValueByNormalizedKey(node, codeKeys);
    let websiteValue = getObjectValueByNormalizedKey(node, websiteKeys);
    let obiletValue = getObjectValueByNormalizedKey(node, obiletKeys);

    const code = String(codeValue || "").trim();
    if (!code) return false;

    // Some responses nest amount fields under inner objects of the same row.
    if (websiteValue === undefined) {
      websiteValue = getDeepValueByNormalizedKey(node, websiteKeys, 2);
    }
    if (obiletValue === undefined) {
      obiletValue = getDeepValueByNormalizedKey(node, obiletKeys, 2);
    }
    if (websiteValue === undefined && obiletValue === undefined) return false;

    rows.push({
      code,
      websiteSaleAmountValue: toNumber(websiteValue) ?? 0,
      obiletSaleAmountValue: toNumber(obiletValue) ?? 0,
      websiteSaleAmount: formatCurrencyTry(websiteValue),
      obiletSaleAmount: formatCurrencyTry(obiletValue)
    });
    return true;
  };

  const walk = (node) => {
    if (node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) {
          walk(parsed);
        }
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item));
      return;
    }
    if (typeof node !== "object") return;

    if (pushRow(node)) return;
    Object.values(node).forEach((value) => walk(value));
  };

  walk(payload);

  const uniqueByRow = new Map();
  rows.forEach((row) => {
    const key = `${row.code}__${row.websiteSaleAmountValue}__${row.obiletSaleAmountValue}`;
    if (!uniqueByRow.has(key)) uniqueByRow.set(key, row);
  });
  return Array.from(uniqueByRow.values());
}

function groupSalesRowsByCode(rows) {
  const byCode = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    if (!row || typeof row !== "object") return;
    const code = String(row.code || "").trim();
    if (!code) return;

    const website = toNumber(row.websiteSaleAmountValue) ?? 0;
    const obilet = toNumber(row.obiletSaleAmountValue) ?? 0;
    const current = byCode.get(code) || { code, websiteSaleAmountValue: 0, obiletSaleAmountValue: 0 };
    current.websiteSaleAmountValue += website;
    current.obiletSaleAmountValue += obilet;
    byCode.set(code, current);
  });

  return Array.from(byCode.values())
    .sort((a, b) => a.code.localeCompare(b.code, "tr"))
    .map((row) => ({
      code: row.code,
      websiteSaleAmountValue: row.websiteSaleAmountValue,
      obiletSaleAmountValue: row.obiletSaleAmountValue,
      websiteSaleAmount: formatCurrencyTry(row.websiteSaleAmountValue),
      obiletSaleAmount: formatCurrencyTry(row.obiletSaleAmountValue)
    }));
}

function buildSalesListTotals(rows) {
  const totals = (Array.isArray(rows) ? rows : []).reduce(
    (acc, row) => {
      acc.website += toNumber(row?.websiteSaleAmountValue) ?? 0;
      acc.obilet += toNumber(row?.obiletSaleAmountValue) ?? 0;
      return acc;
    },
    { website: 0, obilet: 0 }
  );

  return {
    websiteValue: totals.website,
    obiletValue: totals.obilet,
    websiteText: formatCurrencyTry(totals.website),
    obiletText: formatCurrencyTry(totals.obilet)
  };
}

function extractSalesTimePointsFromPayload(payload) {
  const points = [];
  const dateKeys = [
    "date",
    "sale-date",
    "sale_date",
    "report-date",
    "report_date",
    "journey-date",
    "journey_date",
    "day",
    "tarih"
  ];
  const websiteKeys = ["WebsiteSaleAmount", "website-sale-amount", "website_sale_amount"];
  const obiletKeys = ["ObiletSaleAmount", "oBiletSaleAmount", "obilet-sale-amount", "obilet_sale_amount"];
  const dateKeyMatcher = (normalizedKey) =>
    normalizedKey.includes("date") ||
    normalizedKey.includes("tarih") ||
    normalizedKey === "day" ||
    normalizedKey.endsWith("day") ||
    normalizedKey.endsWith("month") ||
    normalizedKey.includes("month");
  const websiteMatcher = (normalizedKey) =>
    normalizedKey.includes("website") &&
    (normalizedKey.includes("sale") || normalizedKey.includes("amount") || normalizedKey.includes("tutar"));
  const obiletMatcher = (normalizedKey) =>
    normalizedKey.includes("obilet") &&
    (normalizedKey.includes("sale") || normalizedKey.includes("amount") || normalizedKey.includes("tutar"));

  const pushPoint = (node) => {
    if (!node || typeof node !== "object") return false;

    let dateValue = getObjectValueByNormalizedKey(node, dateKeys);
    let websiteValue = getObjectValueByNormalizedKey(node, websiteKeys);
    let obiletValue = getObjectValueByNormalizedKey(node, obiletKeys);

    if (dateValue === undefined) {
      dateValue = getDeepValueByNormalizedKey(node, dateKeys, 2);
    }
    if (dateValue === undefined) {
      dateValue = getObjectValueByKeyMatcher(node, dateKeyMatcher);
    }
    if (dateValue === undefined) {
      dateValue = getDeepValueByKeyMatcher(node, dateKeyMatcher, 4);
    }
    if (websiteValue === undefined) {
      websiteValue = getDeepValueByNormalizedKey(node, websiteKeys, 2);
    }
    if (websiteValue === undefined) {
      websiteValue = getObjectValueByKeyMatcher(node, websiteMatcher);
    }
    if (websiteValue === undefined) {
      websiteValue = getDeepValueByKeyMatcher(node, websiteMatcher, 4);
    }
    if (obiletValue === undefined) {
      obiletValue = getDeepValueByNormalizedKey(node, obiletKeys, 2);
    }
    if (obiletValue === undefined) {
      obiletValue = getObjectValueByKeyMatcher(node, obiletMatcher);
    }
    if (obiletValue === undefined) {
      obiletValue = getDeepValueByKeyMatcher(node, obiletMatcher, 4);
    }

    const day = normalizeDateToDay(dateValue);
    if (!day) return false;

    const website = toNumber(websiteValue) ?? 0;
    const obilet = toNumber(obiletValue) ?? 0;
    if (website === 0 && obilet === 0) return false;

    points.push({ day, website, obilet });
    return true;
  };

  const walk = (node) => {
    if (node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) {
          walk(parsed);
        }
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item));
      return;
    }
    if (typeof node !== "object") return;

    if (pushPoint(node)) return;
    Object.values(node).forEach((value) => walk(value));
  };

  walk(payload);
  return points;
}

function extractSalesTotalsFromPayload(payload) {
  let website = 0;
  let obilet = 0;

  const listRows = extractSalesRowsFromPayload(payload);
  if (listRows.length > 0) {
    listRows.forEach((row) => {
      website += toNumber(row.websiteSaleAmountValue) ?? 0;
      obilet += toNumber(row.obiletSaleAmountValue) ?? 0;
    });
    return { website, obilet };
  }

  const points = extractSalesTimePointsFromPayload(payload);
  points.forEach((point) => {
    website += toNumber(point.website) ?? 0;
    obilet += toNumber(point.obilet) ?? 0;
  });
  return { website, obilet };
}

function buildChartSeries(rows) {
  const sortedRows = Array.from(Array.isArray(rows) ? rows : []).sort((a, b) => {
    const aCode = String(a?.code || "").trim();
    const bCode = String(b?.code || "").trim();
    if (aCode || bCode) {
      const byCode = aCode.localeCompare(bCode, "tr");
      if (byCode !== 0) return byCode;
    }

    const aStart = normalizeDateToDay(a?.periodStartDate);
    const bStart = normalizeDateToDay(b?.periodStartDate);
    if (aStart || bStart) {
      const byStart = aStart.localeCompare(bStart, "tr");
      if (byStart !== 0) return byStart;
    }

    return String(a?.label || "").localeCompare(String(b?.label || ""), "tr");
  });

  const normalizedRows = sortedRows.map((row) => ({
    label: String(row.label || ""),
    code: String(row.code || ""),
    periodStartDate: normalizeDateToDay(row.periodStartDate) || "",
    website: toNumber(row.website) ?? 0,
    obilet: toNumber(row.obilet) ?? 0
  }));

  const maxValue = normalizedRows.reduce((max, row) => Math.max(max, row.website, row.obilet), 0);
  const minVisiblePct = 3;

  return normalizedRows.map((row) => {
    const websitePct =
      maxValue > 0 && row.website > 0 ? Math.max((row.website / maxValue) * 100, minVisiblePct) : 0;
    const obiletPct =
      maxValue > 0 && row.obilet > 0 ? Math.max((row.obilet / maxValue) * 100, minVisiblePct) : 0;
    return {
      label: row.label,
      website: row.website,
      obilet: row.obilet,
      websiteText: formatCurrencyTry(row.website),
      obiletText: formatCurrencyTry(row.obilet),
      websitePct: Number(websitePct.toFixed(2)),
      obiletPct: Number(obiletPct.toFixed(2))
    };
  });
}

async function fetchSalesReportFromCluster({
  clusterLabel,
  reportUrl,
  startDate,
  endDate,
  partnerId,
  signal,
  sessionCache = null
}) {
  const cluster = extractClusterLabel(clusterLabel || reportUrl);
  const sessionUrl = buildSessionUrlForPartnerUrl(reportUrl);

  let sessionResult = null;
  if (sessionCache && sessionCache.has(sessionUrl)) {
    sessionResult = sessionCache.get(sessionUrl);
  } else {
    sessionResult = await fetchPartnerSessionCredentials(sessionUrl, signal, REPORTING_API_AUTH);
    if (!sessionResult.error && sessionCache) {
      sessionCache.set(sessionUrl, sessionResult);
    }
  }

  if (sessionResult.error) {
    return {
      cluster,
      reportUrl,
      ok: false,
      status: 0,
      error: sessionResult.error,
      payload: null
    };
  }

  const body = {
    data: {
      "start-date": startDate,
      "end-date": endDate,
      "partner-id": String(partnerId || "")
    },
    "device-session": {
      "session-id": sessionResult.sessionId,
      "device-id": sessionResult.deviceId
    },
    date: "2016-03-11T11:33:00",
    language: "tr-TR"
  };

  try {
    const response = await fetch(reportUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: REPORTING_API_AUTH
      },
      body: JSON.stringify(body),
      signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        cluster,
        reportUrl,
        ok: false,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        payload: parsed ?? raw
      };
    }

    return {
      cluster,
      reportUrl,
      ok: true,
      status: response.status,
      error: null,
      payload: parsed ?? raw
    };
  } catch (err) {
    return {
      cluster,
      reportUrl,
      ok: false,
      status: 0,
      error: err?.message || "Satış raporu isteği başarısız.",
      payload: null
    };
  }
}

async function fetchSalesReports({ startDate, endDate, selectedCompany }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 180000);

  try {
    const targets = [];

    if (selectedCompany && selectedCompany.cluster) {
      const reportUrl = buildUrlForCluster(REPORTING_API_URL, selectedCompany.cluster);
      targets.push({
        clusterLabel: selectedCompany.cluster,
        reportUrl,
        partnerId: selectedCompany.id || ""
      });
    } else {
      const reportUrls = buildClusterPartnerUrls(REPORTING_API_URL);
      reportUrls.forEach((reportUrl) => {
        targets.push({
          clusterLabel: extractClusterLabel(reportUrl),
          reportUrl,
          partnerId: ""
        });
      });
    }

    const sessionCache = new Map();
    const dailyRanges = buildDailyRequestRanges(startDate, endDate);
    const monthlyRanges = buildMonthlyRequestRanges(startDate, endDate);
    const normalizedStart = dailyRanges[0]?.startDate || startDate;
    const normalizedEnd = dailyRanges[dailyRanges.length - 1]?.endDate || endDate;

    const runRequestsForRanges = async (
      ranges,
      { collectItems = false, collectListRows = false, groupByCode = false } = {}
    ) => {
      const rowsByLabel = new Map();
      const items = [];
      const listRows = [];
      const errors = [];

      if (!groupByCode) {
        (Array.isArray(ranges) ? ranges : []).forEach((range) => {
          const label = String(range?.label || "").trim();
          if (!label) return;
          if (!rowsByLabel.has(label)) {
            rowsByLabel.set(label, {
              label,
              code: "",
              periodStartDate: String(range?.startDate || ""),
              website: 0,
              obilet: 0
            });
          }
        });
      }

      for (const range of Array.isArray(ranges) ? ranges : []) {
        if (!range || !range.startDate || !range.endDate) continue;

        const results = await Promise.all(
          targets.map((target) =>
            fetchSalesReportFromCluster({
              clusterLabel: target.clusterLabel,
              reportUrl: target.reportUrl,
              startDate: range.startDate,
              endDate: range.endDate,
              partnerId: target.partnerId,
              signal: controller.signal,
              sessionCache
            })
          )
        );

        results.forEach((result, index) => {
          const target = targets[index];
          if (collectItems) {
            items.push({
              cluster: result.cluster,
              reportUrl: result.reportUrl,
              ok: result.ok,
              status: result.status,
              error: result.error,
              payloadText: stringifyPayload(result.payload),
              periodLabel: range.label,
              periodStartDate: range.startDate,
              periodEndDate: range.endDate
            });
          }

          if (!result.ok) {
            const clusterText = result.cluster || target.clusterLabel || "cluster";
            const detail = result.error || "Bilinmeyen hata";
            errors.push(`${range.startDate}..${range.endDate} ${clusterText}: ${detail}`);
            return;
          }

          if (!result.payload || typeof result.payload !== "object") return;

          if (groupByCode) {
            const groupedRows = groupSalesRowsByCode(extractSalesRowsFromPayload(result.payload));
            if (groupedRows.length > 0) {
              groupedRows.forEach((item) => {
                const code = String(item.code || "").trim();
                if (!code) return;
                const rowKey = `${range.label}__${code}`;
                const label = `${range.label} - ${code}`;
                const row = rowsByLabel.get(rowKey) || {
                  label,
                  code,
                  periodStartDate: String(range.startDate || ""),
                  website: 0,
                  obilet: 0
                };
                row.website += toNumber(item.websiteSaleAmountValue) ?? 0;
                row.obilet += toNumber(item.obiletSaleAmountValue) ?? 0;
                rowsByLabel.set(rowKey, row);
              });
            } else {
              const fallbackKey = String(range.label || "");
              const row = rowsByLabel.get(fallbackKey) || {
                label: fallbackKey,
                code: "",
                periodStartDate: String(range.startDate || ""),
                website: 0,
                obilet: 0
              };
              const totals = extractSalesTotalsFromPayload(result.payload);
              row.website += totals.website;
              row.obilet += totals.obilet;
              rowsByLabel.set(fallbackKey, row);
            }
          } else {
            const row = rowsByLabel.get(range.label) || {
              label: String(range.label || ""),
              code: "",
              periodStartDate: String(range.startDate || ""),
              website: 0,
              obilet: 0
            };
            const totals = extractSalesTotalsFromPayload(result.payload);
            row.website += totals.website;
            row.obilet += totals.obilet;
            rowsByLabel.set(range.label, row);
          }

          if (collectListRows) {
            extractSalesRowsFromPayload(result.payload).forEach((item) => listRows.push(item));
          }
        });
      }

      return {
        items,
        listRows,
        seriesRows: Array.from(rowsByLabel.values()),
        errors
      };
    };

    const listResult = await runRequestsForRanges(
      [
        {
          label: "list",
          startDate: normalizedStart,
          endDate: normalizedEnd
        }
      ],
      { collectItems: true, collectListRows: true }
    );

    const listRowsGrouped = groupSalesRowsByCode(listResult.listRows);
    const listTotals = buildSalesListTotals(listRowsGrouped);
    const shouldGroupSeriesByCode = !selectedCompany;
    const dailyResult = await runRequestsForRanges(dailyRanges, { groupByCode: shouldGroupSeriesByCode });
    const monthlyResult = await runRequestsForRanges(monthlyRanges, { groupByCode: shouldGroupSeriesByCode });

    const allErrors = [...listResult.errors, ...dailyResult.errors, ...monthlyResult.errors];
    const error =
      allErrors.length > 0
        ? `Bazı rapor istekleri alınamadı: ${allErrors.slice(0, 2).join(" | ")}${
            allErrors.length > 2 ? ` (+${allErrors.length - 2} hata)` : ""
          }`
        : null;

    return {
      items: listResult.items,
      listRows: listRowsGrouped,
      listTotals,
      dailySeries: buildChartSeries(dailyResult.seriesRows),
      monthlySeries: buildChartSeries(monthlyResult.seriesRows),
      error
    };
  } catch (err) {
    return {
      items: [],
      listRows: [],
      listTotals: buildSalesListTotals([]),
      dailySeries: [],
      monthlySeries: [],
      error: `Satış raporu alınamadı: ${err?.message || "Bilinmeyen hata"}`
    };
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

app.get("/dashboard", requireAuth, requireMenuAccess("dashboard"), (req, res) => {
  res.render("dashboard", {
    user: req.session.user,
    active: "dashboard",
    loginSuccess: req.query.login === "1"
  });
});

app.get("/change-password", requireAuth, requireMenuAccess("password"), (req, res) => {
  res.render("change-password", {
    user: req.session.user,
    error: null,
    ok: req.query.ok === "1",
    active: "password"
  });
});

app.get("/reports/sales", requireAuth, requireMenuAccess("sales"), async (req, res) => {
  const normalizeDate = (value) => {
    if (typeof value !== "string") return "";
    return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : "";
  };

  const shouldFetchReport = req.query.run === "1";
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const today = `${yyyy}-${mm}-${dd}`;
  const startDate = normalizeDate(req.query.startDate) || today;
  const endDate = normalizeDate(req.query.endDate) || today;
  const requestedCompany = typeof req.query.company === "string" ? req.query.company.trim() : "all";
  const { partners: partnerItems, error: partnerError } = await fetchPartnerCodes();
  const companies = [{ value: "all", label: "Tümü" }].concat(
    partnerItems.map((item) => {
      const idText = item.id || "N/A";
      const clusterText = item.cluster || "cluster";
      const label = `${item.code} - ${idText} - ${clusterText}`;
      const value = buildCompanyOptionValue(item);
      return {
        value,
        label,
        meta: item
      };
    })
  );
  if (partnerError) {
    companies.push({
      value: "__partner_error__",
      label: `Hata: ${partnerError}`,
      disabled: true
    });
  }

  const selectedCompanyOption = companies.find(
    (item) => !item.disabled && item.value === requestedCompany && item.meta
  );
  const parsedCompany = parseCompanyOptionValue(requestedCompany);
  const matchedParsedCompany =
    parsedCompany &&
    partnerItems.find(
      (item) =>
        item.code === parsedCompany.code &&
        String(item.id || "") === String(parsedCompany.id || "") &&
        String(item.cluster || "").toLowerCase() === String(parsedCompany.cluster || "").toLowerCase()
    );

  const selectedCompanyMeta = selectedCompanyOption?.meta || matchedParsedCompany || null;
  const company = selectedCompanyMeta ? buildCompanyOptionValue(selectedCompanyMeta) : "all";
  const invalidCompanySelection = requestedCompany !== "all" && !selectedCompanyMeta;

  let reportItems = [];
  let reportListRows = [];
  let reportListTotals = buildSalesListTotals([]);
  let reportDailySeries = [];
  let reportMonthlySeries = [];
  let reportError = null;
  if (invalidCompanySelection) {
    reportError = "Seçilen firma bilgisi bulunamadı. Lütfen listeden tekrar seçim yapın.";
  } else if (shouldFetchReport) {
    const reportResult = await fetchSalesReports({
      startDate,
      endDate,
      selectedCompany: company === "all" ? null : selectedCompanyMeta
    });
    reportItems = reportResult.items;
    reportListRows = reportResult.listRows || [];
    reportListTotals = reportResult.listTotals || buildSalesListTotals([]);
    reportDailySeries = reportResult.dailySeries || [];
    reportMonthlySeries = reportResult.monthlySeries || [];
    reportError = reportResult.error;
  }

  res.render("reports-sales", {
    user: req.session.user,
    active: "sales",
    filters: {
      startDate,
      endDate,
      company
    },
    companies,
    report: {
      requested: shouldFetchReport,
      items: reportItems,
      listRows: reportListRows,
      listTotals: reportListTotals,
      dailySeries: reportDailySeries,
      monthlySeries: reportMonthlySeries,
      error: reportError
    }
  });
});

app.get("/reports/all-companies", requireAuth, requireMenuAccess("all-companies"), async (req, res) => {
  const result = await fetchAllPartnerRows();
  res.render("reports-all-companies", {
    user: req.session.user,
    active: "all-companies",
    report: {
      columns: result.columns || [],
      rows: result.rows || [],
      error: result.error,
      clusterCount: result.clusterCount || 0
    }
  });
});

app.get("/reports/slack-analysis", requireAuth, requireMenuAccess("slack-analysis"), async (req, res) => {
  const shouldFetchReport = req.query.run === "1";
  const shouldQuerySql = req.query.dbRun === "1";
  const today = getTodayIsoDate();
  const startDate = normalizeIsoDateInput(req.query.startDate) || today;
  const endDate = normalizeIsoDateInput(req.query.endDate) || today;
  const dbStartDate = normalizeIsoDateInput(req.query.dbStartDate) || startDate;
  const dbEndDate = normalizeIsoDateInput(req.query.dbEndDate) || endDate;

  let report = buildSlackReplyReportModel({ requested: false });
  let sqlQuery = {
    requested: false,
    filters: {
      startDate: dbStartDate,
      endDate: dbEndDate
    },
    rows: [],
    error: null
  };

  if (shouldFetchReport) {
    const validationError = validateSlackDateRange(startDate, endDate);
    if (validationError) {
      report = buildSlackReplyReportModel({
        requested: true,
        error: validationError
      });
    } else {
      report = await fetchSlackReplyReportForRange(startDate, endDate);
    }
  }

  if (shouldQuerySql) {
    const dbValidationError = validateSlackDateRange(dbStartDate, dbEndDate);
    if (dbValidationError) {
      sqlQuery = {
        requested: true,
        filters: {
          startDate: dbStartDate,
          endDate: dbEndDate
        },
        rows: [],
        error: dbValidationError
      };
    } else {
      const sqlResult = await fetchSlackSavedReports({
        startDate: dbStartDate,
        endDate: dbEndDate,
        limit: 25
      });
      sqlQuery = {
        requested: true,
        filters: sqlResult.filters || {
          startDate: dbStartDate,
          endDate: dbEndDate
        },
        rows: sqlResult.rows || [],
        error: sqlResult.error || null
      };
    }
  }

  res.render("reports-slack-analysis", {
    user: req.session.user,
    active: "slack-analysis",
    filters: {
      startDate,
      endDate
    },
    report,
    sqlQuery
  });
});

app.post("/reports/slack-analysis/save", requireAuth, requireMenuAccess("slack-analysis"), async (req, res) => {
  const today = getTodayIsoDate();
  const startDate = normalizeIsoDateInput(req.body.startDate) || today;
  const endDate = normalizeIsoDateInput(req.body.endDate) || today;
  const source = String(req.body.source || "Slack API").trim() || "Slack API";
  const totalRequestsInput = toCountInteger(req.body.totalRequests);
  const dbStartDate = normalizeIsoDateInput(req.body.dbStartDate) || startDate;
  const dbEndDate = normalizeIsoDateInput(req.body.dbEndDate) || endDate;
  const parsedRows = parseJsonSafe(String(req.body.rowsJson || ""));
  const rows = Array.isArray(parsedRows) ? parsedRows : [];

  let sqlQuery = {
    requested: false,
    filters: {
      startDate: dbStartDate,
      endDate: dbEndDate
    },
    rows: [],
    error: null
  };

  let report = buildSlackReplyReportModel({
    requested: true,
    rows,
    totalRequests: totalRequestsInput,
    source
  });

  const validationError = validateSlackDateRange(startDate, endDate);
  if (validationError) {
    report.error = validationError;
    return res.status(400).render("reports-slack-analysis", {
      user: req.session.user,
      active: "slack-analysis",
      filters: {
        startDate,
        endDate
      },
      report,
      sqlQuery
    });
  }

  if (!report.rows.length) {
    report.error = "Kaydedilecek sonuç bulunamadı. Önce filtre ile rapor oluşturun.";
    return res.status(400).render("reports-slack-analysis", {
      user: req.session.user,
      active: "slack-analysis",
      filters: {
        startDate,
        endDate
      },
      report,
      sqlQuery
    });
  }

  try {
    const saveResult = await saveSlackReplyReportToDb({
      startDate,
      endDate,
      rows: report.rows,
      totalRequests: report.totalRequests,
      userId: req.session.user?.id || null
    });
    if (saveResult.mode === "updated") {
      report.notice = `${saveResult.saveCount}. kez aynı kayıt güncellendi. Kayıt No: ${saveResult.runId} | Toplam Talep: ${saveResult.totalRequests} | Toplam Yanıt: ${saveResult.totalReplies}`;
    } else {
      report.notice = `SQL'e kaydedildi. Kayıt No: ${saveResult.runId} | Toplam Talep: ${saveResult.totalRequests} | Toplam Yanıt: ${saveResult.totalReplies}`;
    }
  } catch (err) {
    report.error = `SQL kaydı başarısız: ${err?.message || "Bilinmeyen hata"}`;
  }

  const sqlResult = await fetchSlackSavedReports({
    startDate: dbStartDate,
    endDate: dbEndDate,
    limit: 25
  });
  sqlQuery = {
    requested: true,
    filters: sqlResult.filters || {
      startDate: dbStartDate,
      endDate: dbEndDate
    },
    rows: sqlResult.rows || [],
    error: sqlResult.error || null
  };

  res.render("reports-slack-analysis", {
    user: req.session.user,
    active: "slack-analysis",
    filters: {
      startDate,
      endDate
    },
    report,
    sqlQuery
  });
});

app.post("/change-password", requireAuth, requireMenuAccess("password"), async (req, res) => {
  const { currentPassword, newPassword, confirmPassword } = req.body;
  if (!currentPassword || !newPassword || !confirmPassword) {
    return res.status(400).render("change-password", {
      user: req.session.user,
      error: "Tüm alanlar zorunludur.",
      ok: false,
      active: "password"
    });
  }
  if (newPassword !== confirmPassword) {
    return res.status(400).render("change-password", {
      user: req.session.user,
      error: "Yeni şifreler eşleşmiyor.",
      ok: false,
      active: "password"
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
        ok: false,
        active: "password"
      });
    }

    const ok = await bcrypt.compare(currentPassword, user.password_hash);
    if (!ok) {
      return res.status(401).render("change-password", {
        user: req.session.user,
        error: "Mevcut şifre hatalı.",
        ok: false,
        active: "password"
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
      ok: false,
      active: "password"
    });
  }
});

app.get("/users", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const errorMessages = {
    missing_fields: "Kullanıcı adı ve görünen isim zorunludur.",
    invalid_user: "Geçersiz kullanıcı seçimi.",
    user_not_found: "Kullanıcı bulunamadı.",
    username_exists: "Bu kullanıcı adı zaten kullanılıyor.",
    update_failed: "Kullanıcı güncellenemedi.",
    create_failed: "Kullanıcı oluşturulamadı."
  };

  const okValue = String(req.query.ok || "").trim();
  const errValue = String(req.query.err || "").trim();
  const notice =
    okValue === "1" ? "Kullanıcı oluşturuldu." : okValue === "2" ? "Kullanıcı güncellendi." : null;
  const error = errorMessages[errValue] || null;
  const editUserId = Number(req.query.edit);
  const editingUserId = Number.isInteger(editUserId) ? editUserId : null;

  try {
    const result = await pool.query(
      "SELECT id, username, display_name, created_at FROM users ORDER BY id DESC"
    );
    res.render("users", {
      user: req.session.user,
      users: result.rows,
      notice,
      error,
      editingUserId,
      active: "users"
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sunucu hatası");
  }
});

app.post("/users", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const { username, displayName, password } = req.body;
  if (!username || !displayName || !password) {
    return res.redirect("/users?err=missing_fields");
  }
  try {
    const passwordHash = await bcrypt.hash(password, 10);
    const insertResult = await pool.query(
      "INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3) RETURNING id",
      [username.trim(), passwordHash, displayName.trim()]
    );
    const newUserId = Number(insertResult.rows?.[0]?.id);
    if (Number.isInteger(newUserId)) {
      await ensureSidebarPermissionsForUser(newUserId);
    }
    res.redirect("/users?ok=1");
  } catch (err) {
    console.error(err);
    if (err?.code === "23505") {
      return res.redirect("/users?err=username_exists");
    }
    res.redirect("/users?err=create_failed");
  }
});

app.post("/users/:userId/update", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) {
    return res.redirect("/users?err=invalid_user");
  }

  const username = String(req.body.username || "").trim();
  const displayName = String(req.body.displayName || "").trim();
  const rawPassword = typeof req.body.password === "string" ? req.body.password : "";
  const password = rawPassword.trim();

  if (!username || !displayName) {
    return res.redirect(`/users?edit=${userId}&err=missing_fields`);
  }

  try {
    let queryResult;
    if (password) {
      const passwordHash = await bcrypt.hash(password, 10);
      queryResult = await pool.query(
        "UPDATE users SET username = $1, display_name = $2, password_hash = $3 WHERE id = $4",
        [username, displayName, passwordHash, userId]
      );
    } else {
      queryResult = await pool.query(
        "UPDATE users SET username = $1, display_name = $2 WHERE id = $3",
        [username, displayName, userId]
      );
    }

    if ((queryResult?.rowCount || 0) === 0) {
      return res.redirect("/users?err=user_not_found");
    }

    if (Number(req.session.user?.id) === userId) {
      req.session.user.username = username;
      req.session.user.displayName = displayName;
    }

    return res.redirect("/users?ok=2");
  } catch (err) {
    console.error(err);
    if (err?.code === "23505") {
      return res.redirect(`/users?edit=${userId}&err=username_exists`);
    }
    return res.redirect(`/users?edit=${userId}&err=update_failed`);
  }
});

app.get("/screens", requireAuth, requireMenuAccess("users"), async (req, res) => {
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

app.post("/screens", requireAuth, requireMenuAccess("users"), async (req, res) => {
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

app.get("/permissions/:userId", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) return res.status(400).send("Geçersiz kullanıcı");

  try {
    const userResult = await pool.query(
      "SELECT id, username, display_name FROM users WHERE id = $1",
      [userId]
    );
    const targetUser = userResult.rows[0];
    if (!targetUser) return res.status(404).send("Kullanıcı bulunamadı");
    const menuSections = await loadSidebarPermissionSectionsForUser(userId);
    res.render("permissions", {
      user: req.session.user,
      targetUser,
      menuSections,
      ok: req.query.ok === "1",
      active: "users"
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Sunucu hatası");
  }
});

app.post("/permissions/:userId", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) return res.status(400).send("Geçersiz kullanıcı");
  const { canView } = req.body;
  const menuKey = String(req.body.menuKey || "").trim();
  const canViewBool = canView === "on";

  if (!menuKey) return res.status(400).send("Geçersiz menü seçimi");

  try {
    const menuResult = await pool.query(
      "SELECT key FROM sidebar_menu_items WHERE key = $1 AND is_active = true",
      [menuKey]
    );
    if (!menuResult.rows.length) {
      return res.status(400).send("Menü bulunamadı");
    }

    await pool.query(
      `INSERT INTO user_sidebar_permissions (user_id, menu_key, can_view, updated_at)
       VALUES ($1, $2, $3)
       ON CONFLICT(user_id, menu_key)
       DO UPDATE SET can_view = EXCLUDED.can_view, updated_at = now()`,
      [userId, menuKey, canViewBool]
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
