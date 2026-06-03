#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const net = require("net");
const express = require("express");
const mssql = require("mssql");

function loadLocalEnvFile(filePath = path.join(__dirname, "..", ".env")) {
  try {
    if (!fs.existsSync(filePath)) return;
    const raw = fs.readFileSync(filePath, "utf8");
    String(raw || "")
      .replace(/^\uFEFF/, "")
      .split(/\r?\n/)
      .forEach((line) => {
        const trimmed = String(line || "").trim();
        if (!trimmed || trimmed.startsWith("#")) return;
        const normalizedLine = trimmed.startsWith("export ") ? trimmed.slice(7).trim() : trimmed;
        const eqIndex = normalizedLine.indexOf("=");
        if (eqIndex <= 0) return;

        const key = normalizedLine.slice(0, eqIndex).trim();
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) return;
        if (String(process.env[key] || "").trim()) return;

        let value = normalizedLine.slice(eqIndex + 1).trim();
        if (
          (value.startsWith('"') && value.endsWith('"')) ||
          (value.startsWith("'") && value.endsWith("'"))
        ) {
          const quote = value[0];
          value = value.slice(1, -1);
          if (quote === '"') {
            value = value
              .replace(/\\n/g, "\n")
              .replace(/\\r/g, "\r")
              .replace(/\\t/g, "\t")
              .replace(/\\"/g, '"')
              .replace(/\\\\/g, "\\");
          }
        }

        process.env[key] = value;
      });
  } catch (err) {
    console.warn(`.env could not be read: ${err?.message || "unknown error"}`);
  }
}

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || String(value).trim() === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  return fallback;
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function readEnv(name, fallback = "") {
  const value = String(process.env[name] || "").trim();
  return value || fallback;
}

loadLocalEnvFile();

const SQL_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_HOST", "3.66.204.108");
const SQL_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PORT, 1433);
const SQL_DATABASE = readEnv("OBUS_USER_DEACTIVATE_SQL_DATABASE", "b2b-production");
const SQL_USERNAME = readEnv("OBUS_USER_DEACTIVATE_SQL_USERNAME", "ors_mdemirci");
const SQL_PASSWORD = String(process.env.OBUS_USER_DEACTIVATE_SQL_PASSWORD || "");
const SQL_TIMEOUT_MS = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS, 45000);
const SQL_ENCRYPT = parseBooleanFlag(process.env.OBUS_USER_DEACTIVATE_SQL_ENCRYPT, !net.isIP(SQL_HOST));
const PROXY_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_HOST", "0.0.0.0");
const PROXY_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_PORT, 3015);
const PROXY_TOKEN = String(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN || "");

let poolPromise = null;
let poolKey = "";

function getRequiredSqlConfigError() {
  const missing = [];
  if (!SQL_HOST) missing.push("OBUS_USER_DEACTIVATE_SQL_HOST");
  if (!SQL_DATABASE) missing.push("OBUS_USER_DEACTIVATE_SQL_DATABASE");
  if (!SQL_USERNAME) missing.push("OBUS_USER_DEACTIVATE_SQL_USERNAME");
  if (!SQL_PASSWORD) missing.push("OBUS_USER_DEACTIVATE_SQL_PASSWORD");
  return missing.length > 0 ? `Missing SQL config: ${missing.join(", ")}` : "";
}

async function getSqlPool() {
  const configError = getRequiredSqlConfigError();
  if (configError) throw new Error(configError);

  const nextPoolKey = [SQL_HOST, SQL_PORT, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD].join("\u0000");
  if (poolPromise && poolKey === nextPoolKey) return poolPromise;

  const pool = new mssql.ConnectionPool({
    user: SQL_USERNAME,
    password: SQL_PASSWORD,
    server: SQL_HOST,
    port: SQL_PORT,
    database: SQL_DATABASE,
    connectionTimeout: SQL_TIMEOUT_MS,
    requestTimeout: SQL_TIMEOUT_MS,
    pool: {
      min: 0,
      max: 4,
      idleTimeoutMillis: 30000
    },
    options: {
      encrypt: SQL_ENCRYPT,
      trustServerCertificate: true,
      ...(net.isIP(SQL_HOST)
        ? {
            servername: "",
            serverName: "",
            cryptoCredentialsDetails: {
              servername: ""
            }
          }
        : {})
    }
  });

  poolKey = nextPoolKey;
  poolPromise = pool.connect().catch((err) => {
    if (poolKey === nextPoolKey) {
      poolPromise = null;
      poolKey = "";
    }
    throw err;
  });
  return poolPromise;
}

async function fetchUserRows(usernameFilter = "") {
  const pool = await getSqlPool();
  const request = pool.request();
  request.input("usernameFilter", mssql.NVarChar, `%${String(usernameFilter || "").trim()}%`);
  const result = await request.query(`
    select u.ID, p.Code,u.Username from b2b.[user] u
    left join partner p on p.ID = u.PartnerId
    where username like @usernameFilter
  `);
  return Array.isArray(result?.recordset) ? result.recordset : [];
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    ID: row?.ID ?? row?.Id ?? row?.id ?? "",
    Code: row?.Code ?? row?.code ?? "",
    Username: row?.Username ?? row?.username ?? ""
  }));
}

function requireProxyToken(req, res, next) {
  if (!PROXY_TOKEN) return next();
  const authHeader = String(req.get("authorization") || "").trim();
  const bearerToken = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (bearerToken === PROXY_TOKEN) return next();
  return res.status(401).json({ ok: false, error: "Unauthorized." });
}

const app = express();
app.use(express.json({ limit: "64kb" }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "obus-user-deactivate-sql-proxy"
  });
});

app.post("/obus-user-deactivate/users", requireProxyToken, async (req, res) => {
  try {
    const usernameFilter = String(req.body?.usernameFilter || "").trim();
    const rows = normalizeRows(await fetchUserRows(usernameFilter));
    return res.json({
      ok: true,
      count: rows.length,
      rows
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || "SQL user listing failed."
    });
  }
});

app.listen(PROXY_PORT, PROXY_HOST, () => {
  console.log(`Obus user deactivate SQL proxy listening on ${PROXY_HOST}:${PROXY_PORT}`);
});
