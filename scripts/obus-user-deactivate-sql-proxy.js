#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const net = require("net");
const os = require("os");
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

function resolveFirstSecret(names = [], { trim = true, fallback = "" } = {}) {
  const normalizedNames = (Array.isArray(names) ? names : [names])
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  for (const name of normalizedNames) {
    const value = resolveSecret(name, { trim });
    if (value && !isPlaceholderConfigValue(value)) return value;
  }

  const fallbackValue = normalizeSecretValue(fallback, { trim });
  return fallbackValue && !isPlaceholderConfigValue(fallbackValue) ? fallbackValue : "";
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
const SQL_ENCRYPT =
  SQL_HOST && net.isIP(SQL_HOST)
    ? false
    : parseBooleanFlag(
        process.env.OBUS_USER_DEACTIVATE_SQL_ENCRYPT,
        typeof DATABASE_MSSQL_CONFIG.encrypt === "boolean" ? DATABASE_MSSQL_CONFIG.encrypt : true
      );
const PROXY_HOST = readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_HOST", "127.0.0.1");
const PROXY_PORT = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_PORT, 3015);
const PROXY_TOKEN = resolveSecret("OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN", { trim: false });
const PROXY_ALLOWED_ORIGINS = parseAllowedOrigins(
  readEnv("OBUS_USER_DEACTIVATE_SQL_PROXY_ALLOWED_ORIGIN", "http://localhost:3000,https://*.onrender.com")
);
const OBUS_API_AUTH = readEnv(
  "OBUS_USER_DEACTIVATE_API_AUTH",
  readEnv("PARTNERS_API_AUTH", "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt")
);
const PARTNERS_SESSION_API_URL = readEnv(
  "PARTNERS_SESSION_API_URL",
  "https://api-coreprod-cluster0.obus.com.tr/api/client/getsession"
);
const OBUS_SESSION_CONNECTION_IP_ADDRESS = readEnv(
  "OBUS_SESSION_CONNECTION_IP_ADDRESS",
  process.env.OBUS_CONNECTION_IP_ADDRESS || ""
);
const OBUS_SESSION_CONNECTION_PORT = readEnv("OBUS_SESSION_CONNECTION_PORT", "5117");
const OBUS_USER_DELETE_REQUEST_DATE =
  readEnv("OBUS_USER_DELETE_REQUEST_DATE", "2016-03-11T11:33:00") || "2016-03-11T11:33:00";
const OBUS_USER_DEACTIVATE_TIMEOUT_MS = parsePositiveInt(process.env.OBUS_USER_DEACTIVATE_TIMEOUT_MS, 45000);
const OBUS_LOGIN_USERNAME =
  resolveFirstSecret(["OBUS_SERVICE_LOGIN_USERNAME", "OBUS_USER_CREATE_LOGIN_USERNAME"], {
    trim: true,
    fallback: "busproductapp"
  }) || "busproductapp";
const OBUS_LOGIN_PASSWORD = resolveFirstSecret(["OBUS_SERVICE_LOGIN_PASSWORD", "OBUS_USER_CREATE_LOGIN_PASSWORD"], {
  trim: false
});

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
    select u.ID, u.PartnerId, p.Code, u.Username from b2b.[user] u
    left join partner p on p.ID = u.PartnerId
    where username like @usernameFilter
  `);
  return Array.isArray(result?.recordset) ? result.recordset : [];
}

function normalizeRows(rows = []) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    ID: row?.ID ?? row?.Id ?? row?.id ?? "",
    PartnerId: row?.PartnerId ?? row?.PartnerID ?? row?.partnerId ?? row?.partnerID ?? row?.partner_id ?? "",
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

function parseJsonSafe(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

function normalizeTargetUrl(input) {
  let raw = String(input || "").trim();
  if (!raw) return "";
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) raw = `https://${raw}`;
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
    parsed.hash = "";
    const pathname = parsed.pathname && parsed.pathname !== "/" ? parsed.pathname.replace(/\/+$/, "") : "";
    return `${parsed.origin}${pathname}${parsed.search || ""}`;
  } catch (err) {
    return "";
  }
}

function normalizeTokenName(value = "") {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function findNestedValue(node, normalizedKeys) {
  if (node === null || node === undefined) return "";
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findNestedValue(item, normalizedKeys);
      if (found) return found;
    }
    return "";
  }
  if (typeof node !== "object") return "";

  for (const [key, value] of Object.entries(node)) {
    if (normalizedKeys.has(normalizeTokenName(key)) && value !== undefined && value !== null) {
      const text = String(value).trim();
      if (text) return text;
    }
  }
  for (const value of Object.values(node)) {
    const found = findNestedValue(value, normalizedKeys);
    if (found) return found;
  }
  return "";
}

function isUsableLocalIpv4Address(address = "") {
  const value = String(address || "").trim();
  return Boolean(value && value !== "127.0.0.1" && !value.startsWith("169.254.") && net.isIPv4(value));
}

let localMachineIpAddressCache = "";

function getPrimaryLocalMachineIpAddress() {
  if (localMachineIpAddressCache) return localMachineIpAddressCache;
  const candidates = [];
  Object.values(os.networkInterfaces()).forEach((items) => {
    (Array.isArray(items) ? items : []).forEach((item) => {
      const address = String(item?.address || "").trim();
      if (!item?.internal && isUsableLocalIpv4Address(address)) candidates.push(address);
    });
  });
  localMachineIpAddressCache = candidates[0] || "127.0.0.1";
  return localMachineIpAddressCache;
}

function getObusSessionConnectionIpAddress() {
  return OBUS_SESSION_CONNECTION_IP_ADDRESS || getPrimaryLocalMachineIpAddress();
}

function buildObusSessionRequestBody() {
  return {
    type: 1,
    connection: {
      "ip-address": getObusSessionConnectionIpAddress(),
      port: OBUS_SESSION_CONNECTION_PORT
    },
    browser: {
      name: "Chrome"
    }
  };
}

function extractClusterLabel(value = "") {
  const match = String(value || "").match(/cluster\d+/i);
  return match ? match[0].toLowerCase() : "";
}

function normalizeObusClusterLabel(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return /^cluster\d+$/.test(normalized) ? normalized : "";
}

function buildUrlForCluster(baseUrl = "", clusterLabel = "") {
  const raw = String(baseUrl || "").trim();
  const cluster = normalizeObusClusterLabel(clusterLabel);
  if (!raw || !cluster) return raw;
  return /cluster\d+/i.test(raw) ? raw.replace(/cluster\d+/i, cluster) : raw;
}

function buildFallbackBaseUrlForCluster(clusterLabel = "") {
  const cluster = normalizeObusClusterLabel(clusterLabel) || "cluster4";
  return `https://api-coreprod-${cluster}.obus.com.tr/api`;
}

function buildSessionUrlForPartnerUrl(partnerUrl = "", clusterLabel = "") {
  const cluster =
    normalizeObusClusterLabel(clusterLabel) ||
    normalizeObusClusterLabel(extractClusterLabel(partnerUrl)) ||
    "cluster4";
  const clusteredSessionUrl = normalizeTargetUrl(buildUrlForCluster(PARTNERS_SESSION_API_URL, cluster));
  if (clusteredSessionUrl) return clusteredSessionUrl;

  try {
    const parsed = new URL(String(partnerUrl || buildFallbackBaseUrlForCluster(cluster)));
    parsed.pathname = "/api/client/getsession";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildMembershipUserLoginUrl(baseUrl = "") {
  try {
    const parsed = new URL(String(baseUrl || ""));
    parsed.pathname = "/api/membership/userlogin";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildMembershipDeleteUserUrl(baseUrl = "", clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/membership/deleteuser";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildObusUserDeleteRequestBody({ userIds = [], sessionId = "", deviceId = "", token = "" } = {}) {
  return {
    data: userIds.map((item) => Number(item)).filter((item) => Number.isInteger(item) && item > 0),
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    token: String(token || "").trim(),
    date: OBUS_USER_DELETE_REQUEST_DATE,
    language: "tr-TR"
  };
}

function extractTokenFromHeaders(headers = {}) {
  const source = headers && typeof headers === "object" ? headers : {};
  const direct =
    String(
      source.token ||
        source["x-token"] ||
        source["x-auth-token"] ||
        source["access-token"] ||
        source["authorization-token"] ||
        ""
    ).trim();
  if (direct) return direct.replace(/^Bearer\s+/i, "").trim();

  const authorization = String(source.authorization || source["x-authorization"] || "").trim();
  const bearerMatch = authorization.match(/Bearer\s+(.+)$/i);
  if (bearerMatch) return String(bearerMatch[1] || "").trim();
  return authorization.length > 20 ? authorization : "";
}

function extractTokenFromPayload(payload) {
  if (!payload || typeof payload !== "object") return "";
  const direct =
    String(payload?.token?.data || "").trim() ||
    String(payload?.token?.token || "").trim() ||
    String(payload?.token || "").trim() ||
    String(payload?.data?.token?.data || "").trim() ||
    findNestedValue(payload, new Set(["accesstoken", "authorizationtoken", "bearertoken", "jwttoken"]));
  if (direct) return direct;

  const queue = [payload];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (Array.isArray(current)) {
      current.forEach((item) => queue.push(item));
      continue;
    }
    for (const [key, value] of Object.entries(current)) {
      const normalizedKey = normalizeTokenName(key);
      if (normalizedKey.includes("token")) {
        if (value && typeof value === "object") {
          const nested = String(value.data || value.value || value.token || "").trim();
          if (nested && nested.length > 7) return nested;
          queue.push(value);
        } else {
          const text = String(value || "").trim();
          if (text && text.length > 7 && !/\s/.test(text)) return text;
        }
      } else if (value && typeof value === "object") {
        queue.push(value);
      }
    }
  }
  return "";
}

function isSuccessStatusPayload(payload) {
  if (!payload || typeof payload !== "object") return true;
  if (payload.success === false || payload.ok === false) return false;
  const statusValue = payload.status ?? payload.Status ?? payload["status-code"] ?? payload.statusCode;
  if (statusValue === undefined || statusValue === null || statusValue === "") return true;
  if (typeof statusValue === "boolean") return statusValue;
  const text = String(statusValue).trim().toLowerCase();
  if (["true", "success", "ok", "200", "0", "1"].includes(text)) return true;
  if (["false", "error", "fail", "failed"].includes(text)) return false;
  const numberValue = Number.parseInt(text, 10);
  return Number.isFinite(numberValue) ? numberValue >= 200 && numberValue < 300 : true;
}

function sanitizeDebugBody(value) {
  const parsed = typeof value === "string" ? parseJsonSafe(value) : value;
  if (!parsed || typeof parsed !== "object") return String(value || "").trim();
  const sanitize = (node) => {
    if (Array.isArray(node)) return node.map((item) => sanitize(item));
    if (!node || typeof node !== "object") return node;
    const output = {};
    Object.entries(node).forEach(([key, itemValue]) => {
      output[key] = /password|token|authorization|cookie|secret/i.test(key) ? "***" : sanitize(itemValue);
    });
    return output;
  };
  return JSON.stringify(sanitize(parsed), null, 2);
}

function buildFailedRequestPreview({ service = "", requestUrl = "", status = null, requestBody = {}, responseBody = "", error = "", companyLabel = "" } = {}) {
  return {
    service,
    status: Number.isFinite(Number(status)) ? Number(status) : null,
    requestUrl: String(requestUrl || "").trim(),
    requestBody: sanitizeDebugBody(requestBody || {}),
    responseBody: sanitizeDebugBody(responseBody || error || "-") || "-",
    companyLabel: String(companyLabel || "").trim()
  };
}

function normalizeDeactivateUser(row = {}) {
  const userId = Number.parseInt(String(row?.userId || "").trim(), 10);
  const code = String(row?.code || "").trim();
  const partnerId = String(row?.partnerId || "").trim();
  const clusterLabel = normalizeObusClusterLabel(row?.clusterLabel || extractClusterLabel(row?.clusterUrl || ""));
  if (!Number.isInteger(userId) || userId <= 0 || !code || !partnerId || !clusterLabel) return null;
  return {
    key: String(row?.key || "").trim(),
    userId,
    username: String(row?.username || "").trim(),
    code,
    partnerId,
    branchId: String(row?.branchId || partnerId).trim(),
    clusterLabel,
    clusterUrl: normalizeTargetUrl(row?.clusterUrl || buildFallbackBaseUrlForCluster(clusterLabel))
  };
}

function groupDeactivateUsersByCompany(users = []) {
  const groups = new Map();
  (Array.isArray(users) ? users : []).forEach((row) => {
    const user = normalizeDeactivateUser(row);
    if (!user) return;
    const key = [user.code, user.partnerId, user.branchId, user.clusterLabel, user.clusterUrl].join("|||");
    if (!groups.has(key)) {
      groups.set(key, {
        company: {
          code: user.code,
          partnerId: user.partnerId,
          branchId: user.branchId,
          clusterLabel: user.clusterLabel,
          clusterUrl: user.clusterUrl
        },
        users: []
      });
    }
    groups.get(key).users.push(user);
  });
  return Array.from(groups.values());
}

async function fetchObusSessionForCompany(company = {}, signal) {
  const sessionUrl = buildSessionUrlForPartnerUrl(company.clusterUrl, company.clusterLabel);
  const requestBody = buildObusSessionRequestBody();
  const response = await fetch(sessionUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: OBUS_API_AUTH
    },
    body: JSON.stringify(requestBody),
    signal
  });
  const raw = await response.text();
  const parsed = parseJsonSafe(raw);
  if (!response.ok) {
    throw Object.assign(new Error(`GetSession HTTP ${response.status}: ${response.statusText || "Hata"}`), {
      failedRequestPreview: buildFailedRequestPreview({
        service: "GetSession",
        requestUrl: sessionUrl,
        status: response.status,
        requestBody,
        responseBody: parsed ?? raw,
        companyLabel: `${company.code} / ${company.partnerId} / ${company.clusterLabel}`
      })
    });
  }

  const sessionId = findNestedValue(parsed, new Set(["sessionid"]));
  const deviceId = findNestedValue(parsed, new Set(["deviceid"]));
  if (!sessionId || !deviceId) {
    throw Object.assign(new Error("GetSession yanıtında session-id veya device-id bulunamadı."), {
      failedRequestPreview: buildFailedRequestPreview({
        service: "GetSession",
        requestUrl: sessionUrl,
        status: response.status,
        requestBody,
        responseBody: parsed ?? raw,
        companyLabel: `${company.code} / ${company.partnerId} / ${company.clusterLabel}`
      })
    });
  }

  return { sessionId, deviceId };
}

async function loginObusForCompany(company = {}, signal) {
  if (!OBUS_LOGIN_USERNAME || !OBUS_LOGIN_PASSWORD) {
    throw new Error("Local proxy Obus login bilgileri eksik: OBUS_SERVICE_LOGIN_USERNAME / OBUS_SERVICE_LOGIN_PASSWORD.");
  }

  const baseUrl = normalizeTargetUrl(company.clusterUrl || buildFallbackBaseUrlForCluster(company.clusterLabel));
  const loginUrl = buildMembershipUserLoginUrl(baseUrl);
  const session = await fetchObusSessionForCompany(company, signal);
  const requestBody = {
    data: {
      username: OBUS_LOGIN_USERNAME,
      password: OBUS_LOGIN_PASSWORD,
      "remember-me": 0,
      "partner-code": company.code,
      ...(company.branchId ? { "branch-id": company.branchId } : {})
    },
    "device-session": {
      "session-id": session.sessionId,
      "device-id": session.deviceId
    },
    date: "2020-02-24T18:03:00",
    language: "tr-TR"
  };

  const response = await fetch(loginUrl, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: OBUS_API_AUTH
    },
    body: JSON.stringify(requestBody),
    signal
  });
  const raw = await response.text();
  const parsed = parseJsonSafe(raw);
  const responseHeaders = {};
  response.headers.forEach((value, key) => {
    responseHeaders[String(key || "").toLowerCase()] = String(value || "");
  });

  if (!response.ok) {
    throw Object.assign(new Error(`UserLogin HTTP ${response.status}: ${response.statusText || "Hata"}`), {
      failedRequestPreview: buildFailedRequestPreview({
        service: "Membership UserLogin",
        requestUrl: loginUrl,
        status: response.status,
        requestBody,
        responseBody: parsed ?? raw,
        companyLabel: `${company.code} / ${company.partnerId} / ${company.clusterLabel}`
      })
    });
  }

  const token = extractTokenFromPayload(parsed) || extractTokenFromHeaders(responseHeaders);
  if (!token) {
    throw Object.assign(new Error("UserLogin token bulunamadı."), {
      failedRequestPreview: buildFailedRequestPreview({
        service: "Membership UserLogin",
        requestUrl: loginUrl,
        status: response.status,
        requestBody,
        responseBody: parsed ?? raw,
        companyLabel: `${company.code} / ${company.partnerId} / ${company.clusterLabel}`
      })
    });
  }

  return { ...session, token, loginUrl };
}

async function deactivateObusUsersLocally(group = {}) {
  const company = group.company || {};
  const users = Array.isArray(group.users) ? group.users : [];
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_USER_DEACTIVATE_TIMEOUT_MS);
  const companyLabel = `${company.code || "-"} / ${company.partnerId || "-"} / ${company.clusterLabel || "-"}`;
  try {
    const login = await loginObusForCompany(company, controller.signal);
    const requestUrl = buildMembershipDeleteUserUrl(company.clusterUrl || buildFallbackBaseUrlForCluster(company.clusterLabel), company.clusterLabel);
    const requestBody = buildObusUserDeleteRequestBody({
      userIds: users.map((item) => item.userId),
      sessionId: login.sessionId,
      deviceId: login.deviceId,
      token: login.token
    });
    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_API_AUTH
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });
    const raw = await response.text();
    const parsed = parseJsonSafe(raw);

    if (!response.ok || !isSuccessStatusPayload(parsed)) {
      const reason =
        (parsed && typeof parsed === "object" && String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "DeleteUser başarısız.";
      return {
        ok: false,
        company,
        users,
        status: response.status,
        error: `DeleteUser HTTP ${response.status}: ${reason}`,
        failedRequestPreview: buildFailedRequestPreview({
          service: "Membership DeleteUser",
          requestUrl,
          status: response.status,
          requestBody,
          responseBody: parsed ?? raw,
          companyLabel
        })
      };
    }

    return {
      ok: true,
      company,
      users,
      status: response.status,
      responseBody: parsed ?? raw
    };
  } catch (err) {
    return {
      ok: false,
      company,
      users,
      status: null,
      error: err?.name === "AbortError" ? "Local Obus isteği zaman aşımına uğradı." : err?.message || "Local Obus isteği başarısız.",
      failedRequestPreview: err?.failedRequestPreview || null
    };
  } finally {
    clearTimeout(timeout);
  }
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

app.post("/obus-user-deactivate/deactivate", requireProxyToken, async (req, res) => {
  try {
    const selectedUsers = Array.isArray(req.body?.users) ? req.body.users : [];
    const groupedTargets = groupDeactivateUsersByCompany(selectedUsers);
    if (selectedUsers.length === 0 || groupedTargets.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Pasife alınacak geçerli kullanıcı bulunamadı.",
        retryable: false
      });
    }

    const results = [];
    for (const group of groupedTargets) {
      // Local VPN/IP akışında sırayla çalıştırıyoruz; aynı local proxy üzerinde oturum çakışması riskini azaltır.
      results.push(await deactivateObusUsersLocally(group));
    }

    const successResults = results.filter((item) => item?.ok === true);
    const failureResults = results.filter((item) => item?.ok !== true);
    const updatedRows = successResults.flatMap((item) =>
      (Array.isArray(item.users) ? item.users : []).map((user) => ({
        key: String(user?.key || "").trim(),
        userId: String(user?.userId || "").trim(),
        username: String(user?.username || "").trim(),
        code: String(item?.company?.code || "").trim(),
        partnerId: String(item?.company?.partnerId || "").trim(),
        clusterLabel: String(item?.company?.clusterLabel || "").trim(),
        isActive: false,
        isActiveText: "false"
      }))
    );
    const failures = failureResults.flatMap((item) =>
      (Array.isArray(item.users) ? item.users : []).map((user) => ({
        key: String(user?.key || "").trim(),
        userId: String(user?.userId || "").trim(),
        username: String(user?.username || "").trim(),
        code: String(item?.company?.code || "").trim(),
        partnerId: String(item?.company?.partnerId || "").trim(),
        clusterLabel: String(item?.company?.clusterLabel || "").trim(),
        status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
        error: String(item?.error || "Local Obus DeleteUser başarısız.").trim()
      }))
    );
    const failedRequestPreview =
      failureResults.find((item) => item?.failedRequestPreview)?.failedRequestPreview || null;

    if (updatedRows.length === 0) {
      return res.status(502).json({
        ok: false,
        error: String(failureResults[0]?.error || "Seçilen kullanıcılar local Obus proxy ile pasife alınamadı.").trim(),
        failures,
        failedRequestPreview
      });
    }

    return res.status(failures.length > 0 ? 207 : 200).json({
      ok: true,
      successCount: updatedRows.length,
      failureCount: failures.length,
      updatedRows,
      failures,
      failedRequestPreview,
      userMessage:
        failures.length > 0
          ? `${updatedRows.length} kullanıcı local VPN proxy ile pasife alındı. ${failures.length} kullanıcı için hata oluştu.`
          : `${updatedRows.length} kullanıcı local VPN proxy ile pasife alındı.`
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err?.message || "Local Obus pasife alma işlemi tamamlanamadı.",
      retryable: true
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
