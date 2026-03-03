#!/usr/bin/env node

const bcrypt = require("bcryptjs");
const fs = require("fs");
const path = require("path");

function loadLocalEnvFile(filePath = path.join(__dirname, "..", ".env")) {
  try {
    if (!fs.existsSync(filePath)) return;
    const raw = fs.readFileSync(filePath, "utf8");
    const lines = String(raw || "")
      .replace(/^\uFEFF/, "")
      .split(/\r?\n/);

    for (const line of lines) {
      const trimmed = String(line || "").trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const normalizedLine = trimmed.startsWith("export ") ? trimmed.slice(7).trim() : trimmed;
      const eqIndex = normalizedLine.indexOf("=");
      if (eqIndex <= 0) continue;

      const key = normalizedLine.slice(0, eqIndex).trim();
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) continue;
      if (process.env[key] !== undefined) continue;

      let value = normalizedLine.slice(eqIndex + 1).trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      process.env[key] = value;
    }
  } catch (_err) {
    // no-op
  }
}

function parseArgs(argv) {
  const parsed = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = String(argv[i] || "").trim();
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || String(next).startsWith("--")) {
      parsed[key] = "true";
      continue;
    }
    parsed[key] = String(next);
    i += 1;
  }
  return parsed;
}

async function main() {
  loadLocalEnvFile();
  const args = parseArgs(process.argv.slice(2));
  if (args.help === "true" || args.h === "true") {
    console.log(
      "Kullanim: node scripts/reset-user-password.js --database-url \"<DATABASE_URL>\" [--username admin] [--password admin123] [--display-name Admin]"
    );
    process.exit(0);
  }

  const databaseUrl = String(args["database-url"] || process.env.DATABASE_URL || "").trim();
  const username = String(args.username || "admin").trim();
  const password = String(args.password || "admin123");
  const displayName = String(args["display-name"] || "Admin").trim() || username;

  if (!databaseUrl) {
    throw new Error("DATABASE_URL gerekli.");
  }
  if (!username) {
    throw new Error("Kullanici adi bos olamaz.");
  }
  if (!password) {
    throw new Error("Sifre bos olamaz.");
  }

  const { createDatabasePool } = require("../db");
  const pool = createDatabasePool(databaseUrl);
  try {
    await pool.query(`
      IF OBJECT_ID(N'users', N'U') IS NULL
      BEGIN
        CREATE TABLE users (
          id INT IDENTITY(1,1) PRIMARY KEY,
          username NVARCHAR(255) UNIQUE NOT NULL,
          password_hash NVARCHAR(255) NOT NULL,
          display_name NVARCHAR(255) NOT NULL,
          created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        )
      END
    `);

    const hash = await bcrypt.hash(password, 10);
    const existing = await pool.query("SELECT id FROM users WHERE username = $1", [username]);
    if (existing.rows[0]?.id) {
      await pool.query("UPDATE users SET password_hash = $1, display_name = $2 WHERE id = $3", [
        hash,
        displayName,
        existing.rows[0].id
      ]);
      console.log(`Sifre guncellendi: ${username}`);
    } else {
      await pool.query("INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3)", [
        username,
        hash,
        displayName
      ]);
      console.log(`Kullanici olusturuldu: ${username}`);
    }
  } finally {
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
