const path = require("path");
const express = require("express");
const session = require("express-session");
const bcrypt = require("bcryptjs");
const { Pool } = require("pg");
const pgSession = require("connect-pg-simple")(session);

const app = express();
const PORT = process.env.PORT || 3000;
const isProd = process.env.NODE_ENV === "production";

if (!process.env.DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Add it to your environment.");
}

if (isProd) {
  // Trust Render's proxy so secure cookies are set correctly.
  app.set("trust proxy", 1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProd ? { rejectUnauthorized: false } : undefined
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
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
    return res.redirect("/login");
  }
  next();
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
      `SELECT id, title, method, path, target_url, description, body, headers, params
       FROM api_endpoints
       ORDER BY id DESC`
    );
    res.json({ ok: true, items: result.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

app.post("/api/endpoints", requireAuth, async (req, res) => {
  const { title, method, path, targetUrl, description, body, headers, params } = req.body;
  if (!title || !method || !path) {
    return res.status(400).json({ ok: false, error: "Eksik alan" });
  }
  try {
    const result = await pool.query(
      `INSERT INTO api_endpoints (title, method, path, target_url, description, body, headers, params)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       RETURNING id, title, method, path, target_url, description, body, headers, params`,
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
    res.status(500).json({ ok: false });
  }
});

app.put("/api/endpoints/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id)) {
    return res.status(400).json({ ok: false, error: "Geçersiz id" });
  }
  const { body, headers, params } = req.body;
  try {
    await pool.query(
      `UPDATE api_endpoints
       SET body = $1, headers = $2, params = $3, updated_at = now()
       WHERE id = $4`,
      [body || "{}", headers || "{\n  \"Content-Type\": \"application/json\"\n}", params || "{}", id]
    );
    res.json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
