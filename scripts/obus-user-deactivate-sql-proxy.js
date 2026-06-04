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
const PROXY_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_HOST", "127.0.0.1");
const PROXY_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_PORT, 3015);
const PROXY_TOKEN = String(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN || "");
const PROXY_ALLOWED_ORIGINS = parseAllowedOrigins(readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_ALLOWED_ORIGIN", "*"));

function parseAllowedOrigins(value = "") {
  const origins = String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return origins.length > 0 ? origins : ["*"];
}

function resolveAllowedOrigin(requestOrigin = "") {
  const origin = String(requestOrigin || "").trim();
  if (PROXY_ALLOWED_ORIGINS.includes("*")) return "*";
  if (origin && PROXY_ALLOWED_ORIGINS.includes(origin)) return origin;
  return "";
}

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

function isLoopbackAddress(address = "") {
  const value = String(address || "").trim();
  return value === "127.0.0.1" || value === "::1" || value === "::ffff:127.0.0.1";
}

function requireProxyToken(req, res, next) {
  if (!PROXY_TOKEN) return next();
  if (isLoopbackAddress(req.socket?.remoteAddress)) return next();
  const authHeader = String(req.get("authorization") || "").trim();
  const bearerToken = authHeader.replace(/^Bearer\s+/i, "").trim();
  if (bearerToken === PROXY_TOKEN) return next();
  return res.status(401).json({ ok: false, error: "Unauthorized." });
}

function readSqlErrorCode(err = {}) {
  const candidates = [
    err?.code,
    err?.number,
    err?.originalError?.code,
    err?.originalError?.number,
    ...(Array.isArray(err?.precedingErrors)
      ? err.precedingErrors.flatMap((item) => [item?.code, item?.number])
      : [])
  ];
  return candidates.map((item) => String(item || "").trim()).find(Boolean) || "";
}

function classifySqlProxyError(err = {}) {
  const message = String(err?.message || "").trim();
  const code = readSqlErrorCode(err);
  if (/missing sql config/i.test(message)) return "sql-config";
  if (code === "ELOGIN" || /login failed for user/i.test(message)) return "sql-auth";
  return "sql-error";
}

const app = express();
app.use((req, res, next) => {
  const allowedOrigin = resolveAllowedOrigin(req.get("origin"));
  if (allowedOrigin) {
    res.setHeader("Access-Control-Allow-Origin", allowedOrigin);
    if (allowedOrigin !== "*") {
      res.setHeader("Vary", "Origin");
    }
  }
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type,Authorization");
  res.setHeader("Access-Control-Allow-Private-Network", "true");
  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }
  return next();
});
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
    const errorType = classifySqlProxyError(err);
    return res.status(500).json({
      ok: false,
      error: err?.message || "SQL user listing failed.",
      errorType,
      errorCode: readSqlErrorCode(err),
      retryable: errorType !== "sql-auth" && errorType !== "sql-config"
    });
  }
});

const server = app.listen(PROXY_PORT, PROXY_HOST, () => {
  console.log(`Obus user deactivate SQL proxy listening on ${PROXY_HOST}:${PROXY_PORT}`);
});

server.on("error", (err) => {
  console.error(
    `Obus user deactivate SQL proxy could not listen on ${PROXY_HOST}:${PROXY_PORT}: ${err?.message || "unknown error"}`
  );
  process.exit(1);
});
