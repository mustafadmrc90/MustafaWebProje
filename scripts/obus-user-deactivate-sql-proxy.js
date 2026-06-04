#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const net = require("net");
const { execFileSync } = require("child_process");
const express = require("express");
const mssql = require("mssql");

const MACOS_KEYCHAIN_ACCOUNT = "default";
const MACOS_KEYCHAIN_SERVICE_PREFIX = "MustafaWebProje";

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
  if (value && !isPlaceholderConfigValue(value)) return value;
  const fallbackValue = String(fallback || "").trim();
  return fallbackValue && !isPlaceholderConfigValue(fallbackValue) ? fallbackValue : "";
}

function parseMssqlDatabaseUrl(value = "") {
  const text = String(value || "").trim();
  if (!text) return {};

  if (/^(mssql|sqlserver):\/\//i.test(text)) {
    try {
      const parsed = new URL(text);
      const host = String(parsed.hostname || "").trim();
      if (!host) return {};
      const encryptRaw = parsed.searchParams.get("encrypt") || parsed.searchParams.get("ssl");
      return {
        host,
        port: parsePositiveInt(parsed.port, 1433),
        database: String(parsed.pathname || "").replace(/^\//, "") || "",
        username: decodeURIComponent(parsed.username || ""),
        password: decodeURIComponent(parsed.password || ""),
        encrypt: parseBooleanFlag(encryptRaw, !net.isIP(host))
      };
    } catch (err) {
      return {};
    }
  }

  if (!/^[a-z0-9 _-]+\s*=/i.test(text) || !text.includes(";")) return {};

  const config = {};
  text.split(";").forEach((part) => {
    const eqIndex = part.indexOf("=");
    if (eqIndex <= 0) return;
    const key = part.slice(0, eqIndex).replace(/\s+/g, " ").trim().toLowerCase();
    const itemValue = part.slice(eqIndex + 1).trim();
    if (!itemValue) return;

    if (key === "server" || key === "data source" || key === "addr" || key === "address") {
      let host = "";
      let portRaw = "";
      if (itemValue.includes(",")) {
        [host, portRaw] = itemValue.split(",");
      } else if (/^[^:]+:\d+$/.test(itemValue)) {
        [host, portRaw] = itemValue.split(":");
      } else {
        host = itemValue;
      }
      config.host = String(host || "").replace(/^tcp:/i, "").trim();
      if (portRaw) config.port = parsePositiveInt(portRaw, 0);
      return;
    }
    if (key === "database" || key === "initial catalog") {
      config.database = itemValue;
      return;
    }
    if (key === "user id" || key === "uid" || key === "user") {
      config.username = itemValue;
      return;
    }
    if (key === "password" || key === "pwd") {
      config.password = itemValue;
      return;
    }
    if (key === "encrypt") {
      config.encrypt = parseBooleanFlag(itemValue, null);
    }
  });

  return config;
}

function normalizeSecretValue(rawValue, { trim = true } = {}) {
  const text = String(rawValue == null ? "" : rawValue).replace(/\r?\n$/, "");
  return trim ? text.trim() : text;
}

function isPlaceholderConfigValue(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return (
    normalized === "change-me" ||
    normalized === "your-password" ||
    normalized === "your-token" ||
    normalized === "password" ||
    normalized.startsWith("your-")
  );
}

function readMacOsKeychainSecret(secretName, { trim = true } = {}) {
  if (process.platform !== "darwin") return "";
  const normalizedSecretName = String(secretName || "").trim();
  if (!normalizedSecretName) return "";

  try {
    return normalizeSecretValue(
      execFileSync(
        "/usr/bin/security",
        [
          "find-generic-password",
          "-a",
          MACOS_KEYCHAIN_ACCOUNT,
          "-s",
          `${MACOS_KEYCHAIN_SERVICE_PREFIX}/${normalizedSecretName}`,
          "-w"
        ],
        {
          encoding: "utf8",
          stdio: ["ignore", "pipe", "pipe"]
        }
      ),
      { trim }
    );
  } catch (err) {
    return "";
  }
}

function resolveSecret(name, { trim = true, fallback = "" } = {}) {
  const keychainValue = readMacOsKeychainSecret(name, { trim });
  if (keychainValue && !isPlaceholderConfigValue(keychainValue)) return keychainValue;

  const envValue = normalizeSecretValue(process.env[name], { trim });
  if (envValue && !isPlaceholderConfigValue(envValue)) return envValue;

  const fallbackValue = normalizeSecretValue(fallback, { trim });
  if (fallbackValue && !isPlaceholderConfigValue(fallbackValue)) return fallbackValue;

  return "";
}

loadLocalEnvFile();

const DATABASE_MSSQL_CONFIG = parseMssqlDatabaseUrl(process.env.DATABASE_URL);
const SQL_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_HOST", DATABASE_MSSQL_CONFIG.host || "");
const SQL_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PORT, DATABASE_MSSQL_CONFIG.port || 1433);
const SQL_DATABASE = readEnv("OBUS_USER_DEACTIVATE_SQL_DATABASE", DATABASE_MSSQL_CONFIG.database || "");
const SQL_USERNAME = readEnv("OBUS_USER_DEACTIVATE_SQL_USERNAME", DATABASE_MSSQL_CONFIG.username || "");
const SQL_PASSWORD = resolveSecret("OBUS_USER_DEACTIVATE_SQL_PASSWORD", {
  trim: false,
  fallback: DATABASE_MSSQL_CONFIG.password || ""
});
const SQL_TIMEOUT_MS = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS, 45000);
const SQL_ENCRYPT = parseBooleanFlag(
  process.env.OBUS_USER_DEACTIVATE_SQL_ENCRYPT,
  typeof DATABASE_MSSQL_CONFIG.encrypt === "boolean" ? DATABASE_MSSQL_CONFIG.encrypt : !net.isIP(SQL_HOST)
);
const PROXY_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_HOST", "127.0.0.1");
const PROXY_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_PORT, 3015);
const PROXY_TOKEN = resolveSecret("OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN", { trim: false });
const PROXY_ALLOWED_ORIGINS = parseAllowedOrigins(
  readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_ALLOWED_ORIGIN", "http://localhost:3000,https://*.onrender.com")
);

function parseAllowedOrigins(value = "") {
  const origins = String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  return origins.length > 0 ? origins : ["*"];
}

function parseOriginUrl(value = "") {
  try {
    return new URL(String(value || "").trim());
  } catch (err) {
    return null;
  }
}

function isLoopbackOrigin(origin = "") {
  const parsed = parseOriginUrl(origin);
  const hostname = String(parsed?.hostname || "").replace(/^\[|\]$/g, "").toLowerCase();
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
}

function isDefaultTrustedBrowserOrigin(origin = "") {
  const parsed = parseOriginUrl(origin);
  if (!parsed) return false;
  const protocol = String(parsed.protocol || "").toLowerCase();
  const hostname = String(parsed.hostname || "").toLowerCase();
  if (isLoopbackOrigin(origin)) return true;
  return (protocol === "https:" || protocol === "http:") && hostname.endsWith(".onrender.com");
}

function matchesAllowedOriginPattern(origin = "", pattern = "") {
  const normalizedOrigin = String(origin || "").trim();
  const normalizedPattern = String(pattern || "").trim();
  if (!normalizedOrigin || !normalizedPattern) return false;
  if (normalizedPattern === "*") return true;
  if (normalizedPattern === normalizedOrigin) return true;

  const wildcardMatch = normalizedPattern.match(/^(https?:)\/\/\*\.(.+)$/i);
  if (!wildcardMatch) return false;

  const parsedOrigin = parseOriginUrl(normalizedOrigin);
  if (!parsedOrigin) return false;

  const expectedProtocol = wildcardMatch[1].toLowerCase();
  const expectedHostSuffix = wildcardMatch[2].replace(/\/+$/, "").toLowerCase();
  const hostname = String(parsedOrigin.hostname || "").toLowerCase();
  return (
    String(parsedOrigin.protocol || "").toLowerCase() === expectedProtocol &&
    (hostname === expectedHostSuffix || hostname.endsWith(`.${expectedHostSuffix}`))
  );
}

const deniedCorsOrigins = new Set();

function resolveAllowedOrigin(requestOrigin = "") {
  const origin = String(requestOrigin || "").trim();
  if (!origin) return "";
  if (PROXY_ALLOWED_ORIGINS.some((allowedOrigin) => matchesAllowedOriginPattern(origin, allowedOrigin))) {
    return origin;
  }
  if (isDefaultTrustedBrowserOrigin(origin)) return origin;

  if (!deniedCorsOrigins.has(origin)) {
    deniedCorsOrigins.add(origin);
    console.warn(
      `Obus user deactivate SQL proxy CORS origin rejected: ${origin}. ` +
        "Set OBUS_USER_DEACTIVATE_SQL_PROXY_ALLOWED_ORIGIN=* or include this origin."
    );
  }
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

function maskSecretForLog(value = "") {
  const text = String(value || "");
  if (!text) return "missing";
  return `${text.length} chars`;
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
  console.log(
    [
      `SQL target ${SQL_HOST}:${SQL_PORT}/${SQL_DATABASE}`,
      `user=${SQL_USERNAME || "missing"}`,
      `password=${maskSecretForLog(SQL_PASSWORD)}`,
      `encrypt=${SQL_ENCRYPT ? "true" : "false"}`
    ].join(" | ")
  );
});

server.on("error", (err) => {
  console.error(
    `Obus user deactivate SQL proxy could not listen on ${PROXY_HOST}:${PROXY_PORT}: ${err?.message || "unknown error"}`
  );
  process.exit(1);
});
