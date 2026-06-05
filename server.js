const path = require("path");
const os = require("os");
const net = require("net");
const fsSync = require("fs");
const fs = require("fs/promises");
const { execFileSync } = require("child_process");
const express = require("express");
const session = require("express-session");
const bcrypt = require("bcryptjs");
const mssql = require("mssql");
const { createDatabasePool } = require("./db");

function loadLocalEnvFile(filePath = path.join(__dirname, ".env")) {
  try {
    if (!fsSync.existsSync(filePath)) return;
    const raw = fsSync.readFileSync(filePath, "utf8");
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
      if (String(process.env[key] || "").trim()) continue;

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
            .replace(/\\\"/g, '"')
            .replace(/\\\\/g, "\\");
        }
      }

      process.env[key] = value;
    }
  } catch (err) {
    console.warn(`.env okunamadı: ${err?.message || "Bilinmeyen hata"}`);
  }
}

loadLocalEnvFile();

function readLocalEnvFileSecret(secretNames = [], { trim = true, filePath = path.join(__dirname, ".env") } = {}) {
  const targetNames = new Set(
    (Array.isArray(secretNames) ? secretNames : [secretNames])
      .map((item) => String(item || "").trim())
      .filter(Boolean)
  );
  if (targetNames.size === 0) return "";

  try {
    if (!fsSync.existsSync(filePath)) return "";
    const raw = fsSync.readFileSync(filePath, "utf8");
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
      if (!targetNames.has(key)) continue;

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
            .replace(/\\\"/g, '"')
            .replace(/\\\\/g, "\\");
        }
      }

      const normalizedValue = normalizeMacOsKeychainSecretValue(value, trim);
      if (normalizedValue) return normalizedValue;
    }
  } catch (err) {
    return "";
  }

  return "";
}

const MACOS_KEYCHAIN_ACCOUNT = "default";
const MACOS_KEYCHAIN_SERVICE_PREFIX = "MustafaWebProje";
const macOsKeychainSecretCache = new Map();

function buildMacOsKeychainServiceName(secretName = "") {
  return `${MACOS_KEYCHAIN_SERVICE_PREFIX}/${String(secretName || "").trim()}`;
}

function normalizeMacOsKeychainSecretValue(rawValue, trim = true) {
  const text = String(rawValue == null ? "" : rawValue).replace(/\r?\n$/, "");
  return trim ? text.trim() : text;
}

function readLegacyLocalSecret(secretNames = [], { trim = true } = {}) {
  const names = Array.isArray(secretNames) ? secretNames : [secretNames];
  for (const name of names) {
    const normalizedName = String(name || "").trim();
    if (!normalizedName) continue;
    const rawValue = process.env[normalizedName];
    const normalizedValue = normalizeMacOsKeychainSecretValue(rawValue, trim);
    if (normalizedValue) return normalizedValue;
  }
  return readLocalEnvFileSecret(names, { trim });
}

function readMacOsKeychainSecret(secretName, { trim = true } = {}) {
  if (process.platform !== "darwin") return "";

  const normalizedSecretName = String(secretName || "").trim();
  if (!normalizedSecretName) return "";

  const cacheKey = `${normalizedSecretName}:${trim ? "trim" : "raw"}`;
  if (macOsKeychainSecretCache.has(cacheKey)) {
    return macOsKeychainSecretCache.get(cacheKey);
  }

  try {
    const serviceName = buildMacOsKeychainServiceName(normalizedSecretName);
    const rawValue = execFileSync(
      "/usr/bin/security",
      ["find-generic-password", "-a", MACOS_KEYCHAIN_ACCOUNT, "-s", serviceName, "-w"],
      {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"]
      }
    );
    const normalizedValue = normalizeMacOsKeychainSecretValue(rawValue, trim);
    macOsKeychainSecretCache.set(cacheKey, normalizedValue);
    return normalizedValue;
  } catch (err) {
    macOsKeychainSecretCache.set(cacheKey, "");
    return "";
  }
}

function writeMacOsKeychainSecret(secretName, secretValue, { trim = true } = {}) {
  if (process.platform !== "darwin") return false;

  const normalizedSecretName = String(secretName || "").trim();
  const normalizedSecretValue = normalizeMacOsKeychainSecretValue(secretValue, trim);
  if (!normalizedSecretName || !normalizedSecretValue) return false;

  try {
    const serviceName = buildMacOsKeychainServiceName(normalizedSecretName);
    execFileSync(
      "/usr/bin/security",
      ["add-generic-password", "-U", "-a", MACOS_KEYCHAIN_ACCOUNT, "-s", serviceName, "-w", normalizedSecretValue],
      {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"]
      }
    );
    macOsKeychainSecretCache.set(`${normalizedSecretName}:trim`, normalizedSecretValue.trim());
    macOsKeychainSecretCache.set(`${normalizedSecretName}:raw`, normalizedSecretValue);
    return true;
  } catch (err) {
    console.warn(`macOS Keychain yazma hatasi (${normalizedSecretName}): ${err?.message || "Bilinmeyen hata"}`);
    return false;
  }
}

function resolveMacOsKeychainSecret(secretName, { trim = true, legacyEnvNames = [] } = {}) {
  const keychainValue = readMacOsKeychainSecret(secretName, { trim });
  if (keychainValue) return keychainValue;

  const fallbackValue = readLegacyLocalSecret(legacyEnvNames, { trim });
  if (!fallbackValue) return "";

  writeMacOsKeychainSecret(secretName, fallbackValue, { trim });
  return fallbackValue;
}

function resolveObusCredentialSecret(secretNames = [], { trim = true } = {}) {
  const names = (Array.isArray(secretNames) ? secretNames : [secretNames])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  if (names.length === 0) return "";

  for (const secretName of names) {
    const keychainValue = readMacOsKeychainSecret(secretName, { trim });
    if (keychainValue) return keychainValue;
  }

  const fallbackValue = readLegacyLocalSecret(names, { trim });
  if (!fallbackValue) return "";

  writeMacOsKeychainSecret(names[0], fallbackValue, { trim });
  return fallbackValue;
}

function joinSecretNamesForDisplay(secretNames = []) {
  const names = (Array.isArray(secretNames) ? secretNames : [])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  if (names.length === 0) return "";
  if (names.length === 1) return names[0];
  if (names.length === 2) return `${names[0]} ve ${names[1]}`;
  return `${names.slice(0, -1).join(", ")} ve ${names[names.length - 1]}`;
}

function buildMissingMacOsKeychainSecretsMessage(secretNames = []) {
  const secretText = joinSecretNamesForDisplay(secretNames);
  if (!secretText) {
    return "Obus servis giris bilgileri tanimli degil.";
  }
  return `Obus servis giris bilgileri eksik: ${secretText}.`;
}

const OBUS_SERVICE_LOGIN_USERNAME_SECRET_NAMES = Object.freeze([
  "OBUS_SERVICE_LOGIN_USERNAME",
  "OBUS_USER_CREATE_LOGIN_USERNAME",
  "INVENTORY_BRANCHES_LOGIN_USERNAME",
  "OBUS_JOB_FIXED_USERNAME"
]);
const OBUS_SERVICE_LOGIN_PASSWORD_SECRET_NAMES = Object.freeze([
  "OBUS_SERVICE_LOGIN_PASSWORD",
  "OBUS_USER_CREATE_LOGIN_PASSWORD",
  "INVENTORY_BRANCHES_LOGIN_PASSWORD",
  "OBUS_JOB_FIXED_PASSWORD"
]);
const OBUS_SERVICE_LOGIN_USERNAME_FALLBACK = "busproductapp";
const OBUS_SERVICE_LOGIN_PASSWORD_BCRYPT_HASH = String(
  process.env.OBUS_SERVICE_LOGIN_PASSWORD_BCRYPT_HASH ||
    "$2a$12$U55obMMMb21LYx.OFPmbxeOHHv45iHDbHdb4bGWIMRT3YVai8h9cu"
).trim();

function buildInvalidObusServiceLoginPasswordMessage() {
  return "Obus servis sifresi dogrulanamadi. macOS Keychain'deki OBUS_SERVICE_LOGIN_PASSWORD degerini guncelleyin.";
}

function buildObusServiceLoginConfigurationMessage(credentials = null) {
  const explicitError = credentials && typeof credentials === "object" ? String(credentials.error || "").trim() : "";
  if (explicitError) return explicitError;
  return buildMissingMacOsKeychainSecretsMessage([
    OBUS_SERVICE_LOGIN_USERNAME_SECRET_NAMES[0],
    OBUS_SERVICE_LOGIN_PASSWORD_SECRET_NAMES[0]
  ]);
}

function getObusServiceLoginCredentials() {
  const username =
    resolveObusCredentialSecret(OBUS_SERVICE_LOGIN_USERNAME_SECRET_NAMES, { trim: true }) ||
    OBUS_SERVICE_LOGIN_USERNAME_FALLBACK;
  const password = resolveObusCredentialSecret(OBUS_SERVICE_LOGIN_PASSWORD_SECRET_NAMES, { trim: false });

  if (!password) {
    return {
      username,
      password: "",
      error: ""
    };
  }

  if (OBUS_SERVICE_LOGIN_PASSWORD_BCRYPT_HASH) {
    try {
      if (!bcrypt.compareSync(password, OBUS_SERVICE_LOGIN_PASSWORD_BCRYPT_HASH)) {
        return {
          username,
          password: "",
          error: buildInvalidObusServiceLoginPasswordMessage()
        };
      }
    } catch (err) {
      return {
        username,
        password: "",
        error: "Obus servis sifresi hash dogrulamasi tamamlanamadi."
      };
    }
  }

  return {
    username,
    password,
    error: ""
  };
}

function getInventoryBranchesLoginCredentials() {
  return getObusServiceLoginCredentials();
}

function getObusJobFixedCredentials() {
  return getObusServiceLoginCredentials();
}

function getObusUserCreateLoginCredentials() {
  return getObusServiceLoginCredentials();
}

const app = express();
const PORT = process.env.PORT || 3000;
const isProd = process.env.NODE_ENV === "production";
const REQUEST_BODY_LIMIT = String(process.env.REQUEST_BODY_LIMIT || "5mb").trim() || "5mb";
const REQUEST_BODY_PARAMETER_LIMIT =
  Number.parseInt(process.env.REQUEST_BODY_PARAMETER_LIMIT || "50000", 10) || 50000;
const initDbOnly = String(process.env.INIT_DB_ONLY || "")
  .trim()
  .toLowerCase() === "true";
const PARTNERS_API_URL =
  process.env.PARTNERS_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/partner/getpartners";
const PARTNERS_REQUIRED_EXTRA_API_URL = "https://api-preprod.obus.com.tr/api";
const PARTNERS_REQUIRED_EXTRA_ONLY_ID = "1016";
const PARTNERS_EXTRA_API_URLS_RAW = String(
  process.env.PARTNERS_EXTRA_API_URLS || PARTNERS_REQUIRED_EXTRA_API_URL
).trim();
const PARTNERS_SESSION_API_URL =
  process.env.PARTNERS_SESSION_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/client/getsession";
const PARTNERS_API_AUTH =
  process.env.PARTNERS_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const OBUS_SESSION_CONNECTION_IP_ADDRESS =
  String(process.env.OBUS_SESSION_CONNECTION_IP_ADDRESS || process.env.OBUS_CONNECTION_IP_ADDRESS || "").trim();
const OBUS_SESSION_CONNECTION_PORT =
  String(process.env.OBUS_SESSION_CONNECTION_PORT || "5117").trim() || "5117";
const REPORTING_API_URL =
  process.env.REPORTING_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/reporting/obiletsalesreport";
const REPORTING_API_AUTH =
  process.env.REPORTING_API_AUTH || "Basic TXVyb011aG9BbGlPZ2lIYXJ1bk96YW4K";
const SALES_REPORT_TIMEOUT_MS = Number.parseInt(process.env.SALES_REPORT_TIMEOUT_MS || "180000", 10) || 180000;
const ALL_COMPANIES_FETCH_TIMEOUT_MS =
  Number.parseInt(process.env.ALL_COMPANIES_FETCH_TIMEOUT_MS || "180000", 10) || 180000;
const ALL_COMPANIES_SERVICE_PREVIEW_TTL_MS =
  Number.parseInt(process.env.ALL_COMPANIES_SERVICE_PREVIEW_TTL_MS || "1800000", 10) || 1800000;
const ALL_COMPANIES_CLUSTER_CONCURRENCY =
  Number.parseInt(process.env.ALL_COMPANIES_CLUSTER_CONCURRENCY || "6", 10) || 6;
const SALES_REPORT_RANGE_CONCURRENCY =
  Number.parseInt(process.env.SALES_REPORT_RANGE_CONCURRENCY || "4", 10) || 4;
const SALES_REPORT_TARGET_CONCURRENCY =
  Number.parseInt(process.env.SALES_REPORT_TARGET_CONCURRENCY || "4", 10) || 4;
const SALES_REPORT_SESSION_CONCURRENCY =
  Number.parseInt(process.env.SALES_REPORT_SESSION_CONCURRENCY || "8", 10) || 8;
const AUTHORIZED_LINES_API_URL =
  process.env.AUTHORIZED_LINES_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/uetds/UpdateValidRouteCodes";
const OBUS_JOBS_API_URL =
  process.env.OBUS_JOBS_API_URL ||
  "https://api-coreprod-cluster0.obus.com.tr/api/scheduledtask/getscheduledtasks";
const OBUS_JOBS_REQUEST_DATE =
  String(process.env.OBUS_JOBS_REQUEST_DATE || "2016-03-11T11:33:00").trim() || "2016-03-11T11:33:00";
const OBUS_JOBS_REQUEST_LANGUAGE =
  String(process.env.OBUS_JOBS_REQUEST_LANGUAGE || "tr-TR").trim() || "tr-TR";
const OBUS_JOBS_TIMEOUT_MS = Number.parseInt(process.env.OBUS_JOBS_TIMEOUT_MS || "90000", 10) || 90000;
const OBUS_JOBS_CLUSTER_CONCURRENCY = Number.parseInt(process.env.OBUS_JOBS_CLUSTER_CONCURRENCY || "4", 10) || 4;
const OBUS_JOBS_DEFAULT_SLACK_MENTION_TARGETS_RAW = String(
  process.env.OBUS_JOBS_DEFAULT_SLACK_MENTION_TARGETS || "<@U03M90JM0CB>"
).trim();
const JOURNEY_SEARCH_API_AUTH =
  String(process.env.JOURNEY_SEARCH_API_AUTH || "Basic RXJ0dVNlcmRhclNlbWloRGF2aWROdXJl").trim() ||
  "Basic RXJ0dVNlcmRhclNlbWloRGF2aWROdXJl";
const JOURNEY_SEARCH_REQUEST_DATE =
  String(process.env.JOURNEY_SEARCH_REQUEST_DATE || "2019-12-13T15:11:01.6608738+03:00").trim() ||
  "2019-12-13T15:11:01.6608738+03:00";
const JOURNEY_SEARCH_REQUEST_LANGUAGE =
  String(process.env.JOURNEY_SEARCH_REQUEST_LANGUAGE || "tr-TR").trim() || "tr-TR";
const JOURNEY_SEARCH_TIMEOUT_MS = Number.parseInt(process.env.JOURNEY_SEARCH_TIMEOUT_MS || "90000", 10) || 90000;
const UETDS_PRICES_TASK_HINT =
  String(
    process.env.UETDS_PRICES_TASK_HINT ||
      process.env.UETDS_PRICES_TASK_DATA ||
      "AddAllFeeSchedule-f888ccc1-7a94-496d-9ceb-c96f08ccc70e"
  ).trim() ||
  "AddAllFeeSchedule-f888ccc1-7a94-496d-9ceb-c96f08ccc70e";
const UETDS_PRICES_REQUEST_DATE =
  String(process.env.UETDS_PRICES_REQUEST_DATE || "2019-12-23T11:33:00").trim() || "2019-12-23T11:33:00";
const UETDS_PRICES_REQUEST_LANGUAGE =
  String(process.env.UETDS_PRICES_REQUEST_LANGUAGE || "tr-TR").trim() || "tr-TR";
const OBUS_PARTNER_RULE_CREATE_API_URL =
  process.env.OBUS_PARTNER_RULE_CREATE_API_URL ||
  "https://api-coreprod-cluster1.obus.com.tr/api/Rule/CreatePartnerRule";
const OBUS_PARTNER_RULE_UPDATE_API_URL =
  process.env.OBUS_PARTNER_RULE_UPDATE_API_URL ||
  "https://api-coreprod-cluster1.obus.com.tr/api/Rule/UpdatePartnerRule";
const OBUS_PARTNER_RULE_CREATE_API_AUTH =
  process.env.OBUS_PARTNER_RULE_CREATE_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS =
  Number.parseInt(process.env.OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS || "90000", 10) || 90000;
const OBUS_PARTNER_RULE_CREATE_CONCURRENCY =
  Number.parseInt(process.env.OBUS_PARTNER_RULE_CREATE_CONCURRENCY || "4", 10) || 4;
const OBUS_USER_CREATE_API_URL =
  process.env.OBUS_USER_CREATE_API_URL ||
  "https://api-coreprod-cluster3.obus.com.tr/api/membership/createuser";
const OBUS_USER_CREATE_API_AUTH =
  process.env.OBUS_USER_CREATE_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const OBUS_USER_CREATE_TIMEOUT_MS =
  Number.parseInt(process.env.OBUS_USER_CREATE_TIMEOUT_MS || "90000", 10) || 90000;
const OBUS_USER_CREATE_LOGIN_CONCURRENCY =
  Number.parseInt(process.env.OBUS_USER_CREATE_LOGIN_CONCURRENCY || "6", 10) || 6;
const OBUS_USER_CREATE_REQUEST_CONCURRENCY =
  Number.parseInt(process.env.OBUS_USER_CREATE_REQUEST_CONCURRENCY || "10", 10) || 10;

function parseObusUserDeactivateBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null || String(value).trim() === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  return fallback;
}

function parseObusUserDeactivatePositiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function isObusUserDeactivatePlaceholderConfigValue(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  return (
    normalized === "change-me" ||
    normalized === "your-password" ||
    normalized === "your-token" ||
    normalized === "password" ||
    normalized.startsWith("your-")
  );
}

function readObusUserDeactivateConfigValue(name, fallback = "") {
  const value = String(process.env[name] || "").trim();
  if (value && !isObusUserDeactivatePlaceholderConfigValue(value)) return value;
  const fallbackValue = String(fallback || "").trim();
  return fallbackValue && !isObusUserDeactivatePlaceholderConfigValue(fallbackValue) ? fallbackValue : "";
}

function parseObusUserDeactivateMssqlDatabaseUrl(value = "") {
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
        port: parseObusUserDeactivatePositiveInt(parsed.port, 1433),
        database: String(parsed.pathname || "").replace(/^\//, "") || "",
        username: decodeURIComponent(parsed.username || ""),
        password: decodeURIComponent(parsed.password || ""),
        encrypt: parseObusUserDeactivateBooleanFlag(encryptRaw, !net.isIP(host))
      };
    } catch (err) {
      return {};
    }
  }

  if (!/^[a-z0-9 _-]+\s*=/i.test(text) || !text.includes(";")) return {};

  const config = {};
  String(text)
    .split(";")
    .map((part) => String(part || "").trim())
    .filter(Boolean)
    .forEach((part) => {
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
        if (portRaw) config.port = parseObusUserDeactivatePositiveInt(portRaw, 0);
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
        config.encrypt = parseObusUserDeactivateBooleanFlag(itemValue, null);
      }
    });

  return config;
}

const OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG = parseObusUserDeactivateMssqlDatabaseUrl(process.env.DATABASE_URL);
const OBUS_USER_DEACTIVATE_API_URL =
  process.env.OBUS_USER_DEACTIVATE_API_URL ||
  "https://api-coreprod-cluster4.obus.com.tr/api/Membership/GetUsersWithoutPermissions";
const OBUS_USER_DEACTIVATE_API_AUTH =
  process.env.OBUS_USER_DEACTIVATE_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const OBUS_USER_DEACTIVATE_TIMEOUT_MS =
  Number.parseInt(process.env.OBUS_USER_DEACTIVATE_TIMEOUT_MS || "45000", 10) || 45000;
const OBUS_USER_DEACTIVATE_COMPANY_CONCURRENCY =
  Number.parseInt(process.env.OBUS_USER_DEACTIVATE_COMPANY_CONCURRENCY || "8", 10) || 8;
const OBUS_USER_DEACTIVATE_SQL_HOST =
  readObusUserDeactivateConfigValue("OBUS_USER_DEACTIVATE_SQL_HOST", OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.host || "");
const OBUS_USER_DEACTIVATE_SQL_PORT =
  parseObusUserDeactivatePositiveInt(
    process.env.OBUS_USER_DEACTIVATE_SQL_PORT,
    OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.port || 1433
  );
const OBUS_USER_DEACTIVATE_SQL_DATABASE =
  readObusUserDeactivateConfigValue(
    "OBUS_USER_DEACTIVATE_SQL_DATABASE",
    OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.database || ""
  );
const OBUS_USER_DEACTIVATE_SQL_USERNAME =
  readObusUserDeactivateConfigValue(
    "OBUS_USER_DEACTIVATE_SQL_USERNAME",
    OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.username || ""
  );
const OBUS_USER_DEACTIVATE_SQL_PASSWORD_SECRET_NAMES = Object.freeze([
  "OBUS_USER_DEACTIVATE_SQL_PASSWORD"
]);
const OBUS_USER_DEACTIVATE_SQL_PROXY_URL =
  String(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_URL || "").trim();
const OBUS_USER_DEACTIVATE_SQL_PROXY_PORT =
  Number.parseInt(process.env.OBUS_USER_DEACTIVATE_SQL_PROXY_PORT || "3015", 10) || 3015;
const OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN_SECRET_NAMES = Object.freeze([
  "OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN"
]);
const OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS =
  Number.parseInt(process.env.OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS || "45000", 10) || 45000;
const OBUS_USER_DEACTIVATE_REQUEST_DATE =
  String(process.env.OBUS_USER_DEACTIVATE_REQUEST_DATE || "2026-05-13 08:30:02").trim() || "2026-05-13 08:30:02";
const OBUS_USER_DELETE_REQUEST_DATE =
  String(process.env.OBUS_USER_DELETE_REQUEST_DATE || "2016-03-11T11:33:00").trim() || "2016-03-11T11:33:00";
const OBUS_USER_DELETE_COMPANY_CONCURRENCY =
  Number.parseInt(process.env.OBUS_USER_DELETE_COMPANY_CONCURRENCY || "4", 10) || 4;
const OBUS_PARTNER_RULE_DEFAULT_RULE_ID =
  Number.parseInt(process.env.OBUS_PARTNER_RULE_DEFAULT_RULE_ID || "2", 10) || 2;
const OBUS_LIVE_JOB_TTL_MS = Number.parseInt(process.env.OBUS_LIVE_JOB_TTL_MS || "1800000", 10) || 1800000;
const OBUS_LIVE_JOB_MAX_EVENTS = Number.parseInt(process.env.OBUS_LIVE_JOB_MAX_EVENTS || "10000", 10) || 10000;
const INVENTORY_BRANCHES_API_URL =
  process.env.INVENTORY_BRANCHES_API_URL ||
  "https://api-coreprod-cluster4.obus.com.tr/api/inventory/getbranches";
const INVENTORY_BRANCHES_API_AUTH =
  process.env.INVENTORY_BRANCHES_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
const INVENTORY_BRANCHES_CLUSTER_CONCURRENCY =
  Number.parseInt(process.env.INVENTORY_BRANCHES_CLUSTER_CONCURRENCY || "4", 10) || 4;
const STATION_PASSENGER_INFO_API_URL =
  process.env.STATION_PASSENGER_INFO_API_URL ||
  "https://api-coreprod-cluster3.obus.com.tr/api/Inventory/GetDailyJourneySummaries";
const STATION_PASSENGER_INFO_API_AUTH =
  String(process.env.STATION_PASSENGER_INFO_API_AUTH || INVENTORY_BRANCHES_API_AUTH).trim() ||
  INVENTORY_BRANCHES_API_AUTH;
const STATION_PASSENGER_INFO_REQUEST_LANGUAGE =
  String(process.env.STATION_PASSENGER_INFO_REQUEST_LANGUAGE || "tr-TR").trim() || "tr-TR";
const STATION_PASSENGER_INFO_TIMEOUT_MS =
  Number.parseInt(process.env.STATION_PASSENGER_INFO_TIMEOUT_MS || "45000", 10) || 45000;
const STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL =
  String(
    process.env.STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL ||
      "https://api-coreprod-cluster3.obus.com.tr/api/inventory/getjourneystations"
  ).trim() || "https://api-coreprod-cluster3.obus.com.tr/api/inventory/getjourneystations";
const STATION_PASSENGER_INFO_WEB_STATIONS_API_URL =
  String(
    process.env.STATION_PASSENGER_INFO_WEB_STATIONS_API_URL ||
      "https://api-coreprod-cluster3.obus.com.tr/api/web/getstations"
  ).trim() || "https://api-coreprod-cluster3.obus.com.tr/api/web/getstations";
const STATION_PASSENGER_INFO_WEB_STATIONS_API_AUTH =
  String(process.env.STATION_PASSENGER_INFO_WEB_STATIONS_API_AUTH || JOURNEY_SEARCH_API_AUTH).trim() ||
  JOURNEY_SEARCH_API_AUTH;
const STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL =
  String(
    process.env.STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL ||
      "https://api-coreprod-cluster3.obus.com.tr/api/payment/GetPassengerStateHistory"
  ).trim() || "https://api-coreprod-cluster3.obus.com.tr/api/payment/GetPassengerStateHistory";
const STATION_PASSENGER_INFO_TIME_ZONE =
  String(process.env.STATION_PASSENGER_INFO_TIME_ZONE || "Europe/Istanbul").trim() || "Europe/Istanbul";
const STATION_PASSENGER_INFO_AUTH_CACHE_TTL_MS =
  Number.parseInt(process.env.STATION_PASSENGER_INFO_AUTH_CACHE_TTL_MS || "900000", 10) || 900000;
const STATION_PASSENGER_INFO_WEB_STATIONS_CACHE_TTL_MS =
  Number.parseInt(process.env.STATION_PASSENGER_INFO_WEB_STATIONS_CACHE_TTL_MS || "3600000", 10) || 3600000;
const STATION_PASSENGER_INFO_TARGET_COMPANY_CODE =
  String(process.env.STATION_PASSENGER_INFO_TARGET_COMPANY_CODE || "envergecgel").trim() || "envergecgel";
const STATION_PASSENGER_INFO_TARGET_COMPANY_ID =
  String(process.env.STATION_PASSENGER_INFO_TARGET_COMPANY_ID || "669").trim() || "669";
const ALL_COMPANIES_OBUS_ENRICH_CONCURRENCY =
  Number.parseInt(
    process.env.ALL_COMPANIES_OBUS_ENRICH_CONCURRENCY || String(INVENTORY_BRANCHES_CLUSTER_CONCURRENCY || 4),
    10
  ) || Math.max(1, Number(INVENTORY_BRANCHES_CLUSTER_CONCURRENCY || 4));
const PARTNER_CLUSTER_MIN = 0;
const PARTNER_CLUSTER_MAX = 15;
const PARTNER_CLUSTER_TOTAL = PARTNER_CLUSTER_MAX - PARTNER_CLUSTER_MIN + 1;
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
const SLACK_ANALYSIS_TARGET_CHANNELS_RAW = String(
  process.env.SLACK_ANALYSIS_TARGET_CHANNELS || "G03M9294B50"
).trim();
const SLACK_ANALYSIS_AUTO_SAVE_TIME = String(process.env.SLACK_ANALYSIS_AUTO_SAVE_TIME || "23:59").trim();
const SLACK_CREW_CHANNEL = String(process.env.SLACK_CREW_CHANNEL || "corp-crew").trim() || "corp-crew";
const SLACK_CORP_REQUEST_TAG = String(process.env.SLACK_CORP_REQUEST_TAG || "@corpproduct")
  .trim()
  .toLowerCase();
const SLACK_CORP_REQUEST_CHANNELS_RAW = String(process.env.SLACK_CORP_REQUEST_CHANNELS || "").trim();
const SLACK_REQUIRED_CHANNEL_COLUMNS = ["sales-corpcx"];
const JIRA_BASE_URL = String(process.env.JIRA_BASE_URL || "").trim();
const JIRA_EMAIL = String(process.env.JIRA_EMAIL || "").trim();
const JIRA_API_TOKEN = String(process.env.JIRA_API_TOKEN || "").trim();
const JIRA_API_TIMEOUT_MS = Number.parseInt(process.env.JIRA_API_TIMEOUT_MS || "20000", 10) || 20000;
const JIRA_MAX_RESULTS = Number.parseInt(process.env.JIRA_MAX_RESULTS || "50", 10) || 50;
const JIRA_BOARD_MAX_ITEMS = Number.parseInt(process.env.JIRA_BOARD_MAX_ITEMS || "1000", 10) || 1000;
const JIRA_EPIC_CACHE_TTL_MS = Number.parseInt(process.env.JIRA_EPIC_CACHE_TTL_MS || "600000", 10) || 600000;
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || process.env.CHATGPT_API_KEY || "").trim();
const OPENAI_MODEL = String(process.env.OPENAI_MODEL || "gpt-4.1-mini").trim() || "gpt-4.1-mini";
const OPENAI_API_TIMEOUT_MS = Number.parseInt(process.env.OPENAI_API_TIMEOUT_MS || "45000", 10) || 45000;
const OPENAI_API_BASE_URL = String(process.env.OPENAI_API_BASE_URL || "https://api.openai.com/v1")
  .trim()
  .replace(/\/+$/, "");
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
    key: "authorized-lines-upload",
    type: "item",
    label: "İzinli Hatları & UETDS Fiyatlarını Güncelle",
    parentKey: "general",
    route: "/general/authorized-lines-upload",
    routeKey: "authorized-lines-upload",
    sortOrder: 15,
    iconKey: "authorized-lines-upload"
  },
  {
    key: "obus-jobs",
    type: "item",
    label: "Obus Joblar",
    parentKey: "general",
    route: "/general/obus-jobs",
    routeKey: "obus-jobs",
    sortOrder: 16,
    iconKey: "obus-jobs"
  },
  {
    key: "journey-search",
    type: "item",
    label: "Sefer Sorgula",
    parentKey: "general",
    route: "/general/journey-search",
    routeKey: "journey-search",
    sortOrder: 17,
    iconKey: "journey-search"
  },
  {
    key: "journey-update",
    type: "item",
    label: "Sefer Güncelleme",
    parentKey: "general",
    route: "/general/journey-update",
    routeKey: "journey-update",
    sortOrder: 17,
    iconKey: "journey-update"
  },
  {
    key: "station-passenger-info",
    type: "item",
    label: "Durak Yolcu Bilgisi",
    parentKey: "obus",
    route: "/obus/station-passenger-info",
    routeKey: "station-passenger-info",
    sortOrder: 27,
    iconKey: "station-passenger-info"
  },
  {
    key: "obus-rule-define",
    type: "item",
    label: "Obus Kural Tanımla",
    parentKey: "general",
    route: "/general/obus-rule-define",
    routeKey: "obus-rule-define",
    sortOrder: 19,
    iconKey: "obus-rule-define"
  },
  {
    key: "obus-user-create",
    type: "item",
    label: "Obus Kullanıcı Oluştur",
    parentKey: "general",
    route: "/general/obus-user-create",
    routeKey: "obus-user-create",
    sortOrder: 20,
    iconKey: "obus-user-create"
  },
  {
    key: "obus-user-deactivate",
    type: "item",
    label: "Obus Kullanıcı Pasife Al",
    parentKey: "general",
    route: "/general/obus-user-deactivate",
    routeKey: "obus-user-deactivate",
    sortOrder: 21,
    iconKey: "obus-user-deactivate"
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
    key: "jira-analysis",
    type: "item",
    label: "Jira Analiz",
    parentKey: "reports",
    route: "/reports/jira-analysis",
    routeKey: "jira-analysis",
    sortOrder: 24,
    iconKey: "jira-analysis"
  },
  {
    key: "jira-board",
    type: "item",
    label: "Jira Board",
    parentKey: "reports",
    route: "/reports/jira-board",
    routeKey: "jira-board",
    sortOrder: 25,
    iconKey: "jira-board"
  },
  {
    key: "obus",
    type: "section",
    label: "Otobüs",
    parentKey: null,
    route: null,
    routeKey: null,
    sortOrder: 26,
    iconKey: "folder"
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
  },
  {
    key: "menti",
    type: "item",
    label: "Menti",
    parentKey: "management",
    route: "/menti",
    routeKey: "menti",
    sortOrder: 33,
    iconKey: "menti"
  }
];

const SCREEN_ACTION_LOG_SKIP_PATTERNS = [
  /^\/api\/obus-live\/[^/]+/i,
  /^\/api\/screen-logs\/[^/]+/i
];
const OBUS_JOBS_AUTO_RUN_ENABLED =
  String(process.env.OBUS_JOBS_AUTO_RUN_ENABLED || "true").trim().toLowerCase() !== "false";
const OBUS_JOBS_AUTO_RUN_TIME = String(process.env.OBUS_JOBS_AUTO_RUN_TIME || "10:00").trim();
const APP_TIME_ZONE = String(process.env.TZ || Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Istanbul")
  .trim() || "Europe/Istanbul";
const OBUS_JOBS_AUTO_RUN_SOURCE = "obus-jobs-auto";
const slackReplyReportCache = new Map();
const slackUserLookupCache = {
  expiresAt: 0,
  value: null
};
const allCompaniesServicePreviewCache = new Map();
const obusLiveJobs = new Map();
const jiraEpicMetaCache = new Map();
const stationPassengerWebStationsCache = new Map();
const OBUS_BULK_USER_TEMPLATE_NAME_MAX_LENGTH = 120;
const OBUS_BULK_USER_TEMPLATE_FIELD_MAX_LENGTH = 160;
const OBUS_BULK_USER_TEMPLATE_ENTRY_LIMIT = 250;
const OBUS_USER_CREATE_PERMISSION_TYPES = [
  "CanSeePassengerInformation",
  "CanSeeAgentName",
  "CanSearchTickets",
  "CanViewExpiredJourney",
  "CanViewJourneyActivity",
  "CanViewCancelledJourney",
  "CanRefundOpenTicket",
  "CanMatchSidelinedTicketToJourney",
  "IgnoreMaximumSalesParameters",
  "CanTransferAtOtherBranch",
  "CanEditOnlineTicket",
  "CanRefundOnlineTicket",
  "AllowRefundOptionExpiredTicketsForTransfer",
  "AllowRefundOptionExpiredTickets",
  "CanRefundOtherSalesAtOwnBranch",
  "CanRefundOwnSalesAtOwnBranch",
  "CanTransferAtOwnBranch",
  "CanRefundObiletTicket",
  "CanTransferObiletTickets",
  "CanRefundWebTicket",
  "PermittedAllBranchStations"
];
const obusJobsAutoState = {
  timerId: null,
  isRunning: false
};
const slackAutoSaveState = {
  timerId: null,
  isRunning: false
};

if (!process.env.DATABASE_URL) {
  console.warn("DATABASE_URL is not set. Add it to your environment.");
}

if (isProd) {
  // Trust Render's proxy so secure cookies are set correctly.
  app.set("trust proxy", 1);
}

const rawDatabaseUrl = String(process.env.DATABASE_URL || "").trim();
const databaseSslEnabled = String(process.env.DATABASE_SSL || "")
  .trim()
  .toLowerCase() === "true";
const databaseSslRejectUnauthorized =
  String(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED || "")
    .trim()
    .toLowerCase() === "true";

let databaseHost = "";
if (rawDatabaseUrl) {
  if (/^[a-z0-9 _-]+=/i.test(rawDatabaseUrl) && rawDatabaseUrl.includes(";")) {
    const serverMatch = rawDatabaseUrl.match(/(?:^|;)server\s*=\s*([^;]+)/i);
    if (serverMatch?.[1]) {
      databaseHost = String(serverMatch[1]).split(",")[0].trim().toLowerCase();
    }
  } else {
    try {
      databaseHost = new URL(rawDatabaseUrl).hostname.toLowerCase();
    } catch (err) {
      console.warn(`DATABASE_URL parse edilemedi: ${err?.message || "Bilinmeyen hata"}`);
    }
  }
}

const isManagedDatabaseHost =
  /render\.com$/i.test(databaseHost) ||
  /neon\.tech$/i.test(databaseHost) ||
  /supabase\.(co|com)$/i.test(databaseHost) ||
  /pooler\.supabase\.com$/i.test(databaseHost);

if (databaseSslEnabled && !isManagedDatabaseHost) {
  console.warn("DATABASE_SSL=true ayarlı. SQL Server tarafında Encrypt/TrustServerCertificate ayarlarını kontrol edin.");
}
if (databaseSslRejectUnauthorized && isManagedDatabaseHost) {
  console.warn("DATABASE_SSL_REJECT_UNAUTHORIZED=true ayarlı. Yönetilen SQL sağlayıcılarında bağlantı sorununa neden olabilir.");
}

const pool = createDatabasePool(process.env.DATABASE_URL);
const dbRuntimeState = {
  initStartedAt: new Date().toISOString(),
  initCompletedAt: null,
  initOk: false,
  initError: null,
  initErrorRaw: null,
  initErrorCode: null
};

function summarizeErrorMessage(err) {
  if (!err) return "Bilinmeyen hata";
  if (typeof err === "string") return err.trim() || "Bilinmeyen hata";
  if (typeof err.message === "string" && err.message.trim()) return err.message.trim();
  try {
    return JSON.stringify(err);
  } catch (_serializeErr) {
    return String(err);
  }
}

function classifyDbErrorForUser(err) {
  const summary = summarizeErrorMessage(err);
  const normalized = summary.toLowerCase();
  if (!normalized) return "Bilinmeyen veritabani hatasi.";

  if (normalized.includes("login failed for user")) {
    return "Veritabani kullanici adi veya sifresi hatali.";
  }
  if (
    normalized.includes("failed to connect") ||
    normalized.includes("could not connect") ||
    normalized.includes("enotfound") ||
    normalized.includes("econnrefused") ||
    normalized.includes("etimedout") ||
    normalized.includes("timeout")
  ) {
    return "Veritabanina baglanti kurulamiyor.";
  }
  if (normalized.includes("invalid object name")) {
    return "Gerekli DB tablolari bulunamadi. scripts/init-db-only.sh komutunu calistirin.";
  }
  if (normalized.includes("illegal arguments: string, undefined")) {
    return "Kullanici sifre kaydi eksik veya bozuk. scripts/reset-user-password.js komutunu calistirin.";
  }
  if (
    normalized.includes("permission denied") ||
    normalized.includes("not authorized") ||
    normalized.includes("access denied")
  ) {
    return "Veritabani kullanicisinin tablo olusturma/guncelleme yetkisi yok.";
  }
  if (
    normalized.includes("compute time quota") ||
    normalized.includes("exceeded the compute time quota") ||
    (normalized.includes("quota") && normalized.includes("project"))
  ) {
    return "Veritabani kotasi dolmus veya askiya alinmis. Canli ortamdaki DATABASE_URL baglantisini ve saglayici plan limitlerini kontrol edin.";
  }

  return summary;
}

function normalizeMentiChatHistoryEntries(entries, limit = 12) {
  if (!Array.isArray(entries)) return [];
  return entries
    .map((entry) => {
      const roleRaw = String(entry?.role || "").trim().toLowerCase();
      const text = String(entry?.text || "").trim();
      if (!text) return null;
      const role = roleRaw === "assistant" || roleRaw === "model" ? "assistant" : "user";
      return {
        role,
        content: text
      };
    })
    .filter(Boolean)
    .slice(-Math.max(0, limit));
}

function extractOpenAIText(payload) {
  const choice = Array.isArray(payload?.choices) ? payload.choices[0] : null;
  const content = choice?.message?.content;

  if (typeof content === "string") {
    return content.trim();
  }

  if (!Array.isArray(content)) {
    return "";
  }

  return content
    .map((part) => {
      if (typeof part === "string") return part.trim();
      return String(part?.text || "").trim();
    })
    .filter(Boolean)
    .join("\n\n")
    .trim();
}

app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));

app.use(express.json({ limit: REQUEST_BODY_LIMIT }));
app.use(
  express.urlencoded({
    extended: false,
    limit: REQUEST_BODY_LIMIT,
    parameterLimit: REQUEST_BODY_PARAMETER_LIMIT
  })
);
app.use(
  session({
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
      allowed_computer_enabled BOOLEAN NOT NULL DEFAULT false,
      login_input_lock_enabled BOOLEAN NOT NULL DEFAULT false,
      login_input_lock_version INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE users
      ADD COLUMN IF NOT EXISTS allowed_computer_enabled BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS login_input_lock_enabled BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS login_input_lock_version INTEGER NOT NULL DEFAULT 1
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_login_devices (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      ip_address TEXT,
      mac_address TEXT,
      approved BOOLEAN NOT NULL DEFAULT false,
      ip_enabled BOOLEAN NOT NULL DEFAULT false,
      mac_enabled BOOLEAN NOT NULL DEFAULT false,
      last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_login_result TEXT,
      last_user_agent TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE user_login_devices
      ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      ADD COLUMN IF NOT EXISTS ip_address TEXT,
      ADD COLUMN IF NOT EXISTS mac_address TEXT,
      ADD COLUMN IF NOT EXISTS approved BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS ip_enabled BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS mac_enabled BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS last_login_result TEXT,
      ADD COLUMN IF NOT EXISTS last_user_agent TEXT,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_user_login_devices_user_id
    ON user_login_devices (user_id)
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
  try {
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
  } catch (err) {
    // Legacy migration; if this fails, startup can continue safely.
    console.warn("api_endpoints sort_order migration skipped:", summarizeErrorMessage(err));
  }

  // Migrate legacy endpoint target_url values into shared target list.
  await pool.query(`
    INSERT INTO api_targets (url)
    SELECT DISTINCT trim(ep.target_url)
    FROM api_endpoints ep
    WHERE ep.target_url IS NOT NULL
      AND trim(ep.target_url) <> ''
      AND NOT EXISTS (
        SELECT 1
        FROM api_targets t
        WHERE t.url = trim(ep.target_url)
      )
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
      can_view_logs BOOLEAN NOT NULL DEFAULT false,
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
      ADD COLUMN IF NOT EXISTS can_view_logs BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS screen_action_logs (
      id SERIAL PRIMARY KEY,
      menu_key TEXT NOT NULL,
      action_key TEXT NOT NULL,
      request_method TEXT NOT NULL,
      request_path TEXT NOT NULL,
      status_code INTEGER,
      level TEXT NOT NULL DEFAULT 'info',
      message TEXT NOT NULL,
      detail_text TEXT,
      meta_json TEXT,
      created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE screen_action_logs
      ADD COLUMN IF NOT EXISTS menu_key TEXT,
      ADD COLUMN IF NOT EXISTS action_key TEXT,
      ADD COLUMN IF NOT EXISTS request_method TEXT,
      ADD COLUMN IF NOT EXISTS request_path TEXT,
      ADD COLUMN IF NOT EXISTS status_code INTEGER,
      ADD COLUMN IF NOT EXISTS level TEXT NOT NULL DEFAULT 'info',
      ADD COLUMN IF NOT EXISTS message TEXT,
      ADD COLUMN IF NOT EXISTS detail_text TEXT,
      ADD COLUMN IF NOT EXISTS meta_json TEXT,
      ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_screen_action_logs_menu_key_created_at
    ON screen_action_logs (menu_key, created_at DESC)
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS obus_jobs_runs (
      id SERIAL PRIMARY KEY,
      request_key TEXT NOT NULL,
      company_code TEXT,
      company_id TEXT,
      company_cluster TEXT,
      endpoint_url TEXT,
      requested_cluster_count INTEGER NOT NULL DEFAULT 0,
      success_cluster_count INTEGER NOT NULL DEFAULT 0,
      error_cluster_count INTEGER NOT NULL DEFAULT 0,
      job_column_count INTEGER NOT NULL DEFAULT 0,
      job_item_count INTEGER NOT NULL DEFAULT 0,
      source TEXT,
      created_by INTEGER REFERENCES users(id),
      summary_error TEXT,
      payload_json TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE obus_jobs_runs
      ADD COLUMN IF NOT EXISTS request_key TEXT,
      ADD COLUMN IF NOT EXISTS company_code TEXT,
      ADD COLUMN IF NOT EXISTS company_id TEXT,
      ADD COLUMN IF NOT EXISTS company_cluster TEXT,
      ADD COLUMN IF NOT EXISTS endpoint_url TEXT,
      ADD COLUMN IF NOT EXISTS requested_cluster_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS success_cluster_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS error_cluster_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS job_column_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS job_item_count INTEGER NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS source TEXT,
      ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES users(id),
      ADD COLUMN IF NOT EXISTS summary_error TEXT,
      ADD COLUMN IF NOT EXISTS payload_json TEXT,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS obus_jobs_items (
      id SERIAL PRIMARY KEY,
      run_id INTEGER NOT NULL REFERENCES obus_jobs_runs(id) ON DELETE CASCADE,
      cluster_label TEXT NOT NULL,
      job_id TEXT,
      last_execution TEXT,
      last_job_state TEXT,
      is_yesterday BOOLEAN NOT NULL DEFAULT false,
      cluster_error TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE obus_jobs_items
      ADD COLUMN IF NOT EXISTS run_id INTEGER REFERENCES obus_jobs_runs(id) ON DELETE CASCADE,
      ADD COLUMN IF NOT EXISTS cluster_label TEXT,
      ADD COLUMN IF NOT EXISTS job_id TEXT,
      ADD COLUMN IF NOT EXISTS last_execution TEXT,
      ADD COLUMN IF NOT EXISTS last_job_state TEXT,
      ADD COLUMN IF NOT EXISTS is_yesterday BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS cluster_error TEXT,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_obus_jobs_items_run_id
    ON obus_jobs_items (run_id)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS obus_merkez_branches (
      partner_id TEXT PRIMARY KEY,
      branch_id TEXT NOT NULL,
      name TEXT NOT NULL DEFAULT 'OBUSMERKEZ',
      source_cluster TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE obus_merkez_branches
      ADD COLUMN IF NOT EXISTS branch_id TEXT,
      ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'OBUSMERKEZ',
      ADD COLUMN IF NOT EXISTS source_cluster TEXT,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS all_companies_cache (
      id TEXT NOT NULL,
      code TEXT NOT NULL,
      source TEXT NOT NULL,
      obilet_partner_id TEXT,
      biletall_partner_id TEXT,
      url TEXT,
      is_abroad BOOLEAN,
      obus_merkez_sube_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (id, source, code)
    )
  `);

  await pool.query(`
    ALTER TABLE all_companies_cache
      ADD COLUMN IF NOT EXISTS obilet_partner_id TEXT,
      ADD COLUMN IF NOT EXISTS biletall_partner_id TEXT,
      ADD COLUMN IF NOT EXISTS url TEXT,
      ADD COLUMN IF NOT EXISTS is_abroad BOOLEAN,
      ADD COLUMN IF NOT EXISTS obus_merkez_sube_id TEXT,
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_all_companies_cache_source_code
    ON all_companies_cache (source, code, id)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS obus_bulk_user_templates (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      entries_json TEXT NOT NULL,
      created_by INTEGER REFERENCES users(id),
      updated_by INTEGER REFERENCES users(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE obus_bulk_user_templates
      ADD COLUMN IF NOT EXISTS name TEXT,
      ADD COLUMN IF NOT EXISTS entries_json TEXT,
      ADD COLUMN IF NOT EXISTS created_by INTEGER REFERENCES users(id),
      ADD COLUMN IF NOT EXISTS updated_by INTEGER REFERENCES users(id),
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_obus_bulk_user_templates_name_lookup
    ON obus_bulk_user_templates ((lower(name)))
  `);

  const userCount = await pool.query("SELECT CAST(COUNT(*) AS INT) AS count FROM users");
  if (userCount.rows[0].count === 0) {
    const passwordHash = await bcrypt.hash("admin123", 10);
    await pool.query(
      "INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3)",
      ["admin", passwordHash, "Admin"]
    );
    console.log("Seed user created: admin / admin123");
  }

  const screenCount = await pool.query("SELECT CAST(COUNT(*) AS INT) AS count FROM screens");
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

initDb()
  .then(async () => {
    dbRuntimeState.initOk = true;
    dbRuntimeState.initError = null;
    dbRuntimeState.initErrorRaw = null;
    dbRuntimeState.initErrorCode = null;
    dbRuntimeState.initCompletedAt = new Date().toISOString();
    if (initDbOnly) {
      console.log("DB init tamamlandi (INIT_DB_ONLY=true).");
      await pool.end();
      process.exit(0);
      return;
    }
  startSlackAutoSaveScheduler();
  startObusJobsAutoScheduler();
  })
  .catch(async (err) => {
    dbRuntimeState.initOk = false;
    dbRuntimeState.initError = classifyDbErrorForUser(err);
    dbRuntimeState.initErrorRaw = summarizeErrorMessage(err);
    dbRuntimeState.initErrorCode = String(err?.code || err?.originalError?.code || "");
    dbRuntimeState.initCompletedAt = new Date().toISOString();
    console.error("DB init error:", err);
    console.error("DB init error summary:", dbRuntimeState.initError);
    if (initDbOnly) {
      try {
        await pool.end();
      } catch (closeErr) {
        console.error("DB pool kapatma hatasi:", closeErr);
      }
      process.exit(1);
    }
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
      const canViewRaw = row.can_view ?? row.canView ?? false;
      const canView = toSidebarBool(canViewRaw, false);
      const canViewLogsRaw = row.can_view_logs ?? row.canViewLogs ?? false;
      const canViewLogs = toSidebarBool(canViewLogsRaw, false);

      return {
        key,
        label,
        type,
        parentKey,
        route,
        routeKey,
        sortOrder,
        iconKey,
        canView,
        canViewLogs
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
    can_view: true,
    can_view_logs: true
  }));
  return buildSidebarModelFromRows(rows);
}

function ensureCriticalSidebarRows(rows) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const normalizedRows = sourceRows.map((row) => ({ ...row }));
  const rowByKey = new Map(
    normalizedRows
      .map((row) => [String(row?.key || "").trim(), row])
      .filter(([key]) => key)
  );

  const upsertFromRegistry = (key, forceCanView = false) => {
    const registryItem = SIDEBAR_MENU_REGISTRY.find((item) => String(item.key || "").trim() === key);
    if (!registryItem) return;

    if (!rowByKey.has(key)) {
      const injected = {
        key: registryItem.key,
        label: registryItem.label,
        type: registryItem.type,
        parent_key: registryItem.parentKey || null,
        route: registryItem.route || null,
        route_key: registryItem.routeKey || null,
        sort_order: Number.isFinite(Number(registryItem.sortOrder)) ? Number(registryItem.sortOrder) : 0,
        icon_key: registryItem.iconKey || "folder",
        can_view: forceCanView ? true : registryItem.type === "section",
        can_view_logs: false
      };
      normalizedRows.push(injected);
      rowByKey.set(key, injected);
      return;
    }

    if (forceCanView) {
      rowByKey.get(key).can_view = true;
    }
  };

  upsertFromRegistry("general", true);
  upsertFromRegistry("station-passenger-info");

  return normalizedRows;
}

function buildSidebarEmptyModel() {
  return {
    sections: [],
    allowedMenuKeys: [],
    allowedRouteKeys: []
  };
}

function buildInClausePlaceholders(values, startIndex = 1) {
  const list = Array.isArray(values) ? values : [];
  return list.map((_, idx) => `$${startIndex + idx}`).join(", ");
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
  const registryKeyPlaceholders = buildInClausePlaceholders(registryKeys, 1);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `
        UPDATE sidebar_menu_items
        SET is_active = false, updated_at = now()
        WHERE key NOT IN (${registryKeyPlaceholders})
      `,
      registryKeys
    );

    for (const item of orderedRows) {
      const updateResult = await client.query(
        `
          UPDATE sidebar_menu_items
          SET label = $2,
              type = $3,
              parent_key = $4,
              route = $5,
              route_key = $6,
              sort_order = $7,
              icon_key = $8,
              is_active = true,
              updated_at = now()
          WHERE key = $1
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
      if (!updateResult.rowCount) {
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
    }

    await client.query(`
      INSERT INTO user_sidebar_permissions (user_id, menu_key, can_view, can_view_logs)
      SELECT
        u.id,
        m.key,
        CASE
          WHEN m.type = 'section' THEN true
          WHEN m.key IN ('obus-rule-define', 'obus-user-create', 'obus-user-deactivate', 'journey-search', 'journey-update') THEN true
          WHEN lower(u.username) = 'admin' THEN true
          ELSE false
        END,
        CASE
          WHEN lower(u.username) = 'admin' THEN true
          ELSE false
        END
      FROM users u
      CROSS JOIN sidebar_menu_items m
      WHERE m.is_active = true
        AND NOT EXISTS (
          SELECT 1
          FROM user_sidebar_permissions usp
          WHERE usp.user_id = u.id
            AND usp.menu_key = m.key
        )
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
      INSERT INTO user_sidebar_permissions (user_id, menu_key, can_view, can_view_logs)
      SELECT
        u.id,
        m.key,
        CASE
          WHEN m.type = 'section' THEN true
          WHEN m.key IN ('obus-rule-define', 'obus-user-create', 'obus-user-deactivate', 'journey-search', 'journey-update') THEN true
          WHEN lower(u.username) = 'admin' THEN true
          ELSE false
        END,
        CASE
          WHEN lower(u.username) = 'admin' THEN true
          ELSE false
        END
      FROM users u
      CROSS JOIN sidebar_menu_items m
      WHERE m.is_active = true
        AND u.id = $1
        AND NOT EXISTS (
          SELECT 1
          FROM user_sidebar_permissions usp
          WHERE usp.user_id = u.id
            AND usp.menu_key = m.key
        )
    `,
    [userIdNum]
  );

}

async function loadSidebarForUser(userId, options = {}) {
  const isAdmin = options?.isAdmin === true;
  if (isAdmin) {
    return buildSidebarFallbackModel();
  }
  const userIdNum = Number(userId);
  if (!Number.isInteger(userIdNum)) {
    return buildSidebarEmptyModel();
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
        COALESCE(usp.can_view, false) AS can_view,
        COALESCE(usp.can_view_logs, false) AS can_view_logs
      FROM sidebar_menu_items m
      LEFT JOIN user_sidebar_permissions usp
        ON usp.menu_key = m.key
       AND usp.user_id = $1
      WHERE m.is_active = true
      ORDER BY m.sort_order ASC, m.key ASC
    `,
    [userIdNum]
  );
  return buildSidebarModelFromRows(ensureCriticalSidebarRows(result.rows));
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

async function resolveInitialRouteForUser(user) {
  const sessionUser = user || {};
  const isAdmin = String(sessionUser.username || "").toLowerCase() === "admin";
  if (isAdmin) {
    return "/dashboard";
  }

  const userIdNum = Number(sessionUser.id);
  if (!Number.isInteger(userIdNum)) {
    return "/no-permission-home";
  }

  try {
    const sidebar = await loadSidebarForUser(userIdNum, { isAdmin });
    return getFirstAccessibleRoute(sidebar) || "/no-permission-home";
  } catch (err) {
    console.error("Initial route resolve error:", err);
    return "/no-permission-home";
  }
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
      canView: item.canView,
      canViewLogs: item.canViewLogs
    });
  });

  return Array.from(sectionByKey.values())
    .map((section) => {
      const sortedItems = section.items.sort(compareSidebarEntries);
      return {
        ...section,
        canView: section.canView || sortedItems.some((item) => item.canView),
        items: sortedItems
      };
    })
    .sort(compareSidebarEntries)
    .filter((section) => section.items.length > 0);
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
        COALESCE(usp.can_view, false) AS can_view,
        COALESCE(usp.can_view_logs, false) AS can_view_logs
      FROM sidebar_menu_items m
      LEFT JOIN user_sidebar_permissions usp
        ON usp.menu_key = m.key
       AND usp.user_id = $1
      WHERE m.is_active = true
      ORDER BY m.sort_order ASC, m.key ASC
    `,
    [userIdNum]
  );
  return buildPermissionSections(ensureCriticalSidebarRows(result.rows));
}

function truncateScreenLogText(value, maxLength = 800) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  const limit = Number.isFinite(Number(maxLength)) ? Math.max(16, Number(maxLength)) : 800;
  return text.length > limit ? `${text.slice(0, limit - 3)}...` : text;
}

function sanitizeScreenLogMeta(value, depth = 0) {
  if (depth > 4) return "[depth-limit]";
  if (value === null || value === undefined) return value;
  if (typeof value === "string") return truncateScreenLogText(value, 240);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) {
    return value.slice(0, 20).map((item) => sanitizeScreenLogMeta(item, depth + 1));
  }
  if (typeof value === "object") {
    return Object.entries(value)
      .slice(0, 25)
      .reduce((acc, [key, entryValue]) => {
        acc[key] = sanitizeScreenLogMeta(entryValue, depth + 1);
        return acc;
      }, {});
  }
  return truncateScreenLogText(String(value), 240);
}

function stringifyScreenLogMeta(value) {
  if (!value || typeof value !== "object") return "";
  try {
    return JSON.stringify(sanitizeScreenLogMeta(value));
  } catch (err) {
    return "";
  }
}

function getScreenActionRegistryItem(menuKey) {
  const normalizedMenuKey = String(menuKey || "").trim();
  if (!normalizedMenuKey) return null;
  return (
    SIDEBAR_MENU_REGISTRY.find((item) => item.type === "item" && String(item.key || "").trim() === normalizedMenuKey) ||
    null
  );
}

function shouldSkipScreenActionLog(req) {
  const pathname = String(req.path || req.originalUrl || "").trim();
  if (!pathname) return true;
  return SCREEN_ACTION_LOG_SKIP_PATTERNS.some((pattern) => pattern.test(pathname));
}

function extractScreenActionRequestMeta(req) {
  const queryEntries = Object.entries(req.query || {});
  const hasQuery = queryEntries.some(([, value]) => value !== undefined && value !== null && String(value).trim() !== "");
  const hasBody =
    req.body &&
    typeof req.body === "object" &&
    Object.keys(req.body).some((key) => req.body[key] !== undefined && req.body[key] !== null && String(req.body[key]).trim() !== "");

  return {
    query: hasQuery ? req.query : undefined,
    body: hasBody ? req.body : undefined
  };
}

function determineScreenActionLogLevel(statusCode) {
  const code = Number(statusCode);
  if (code >= 500) return "error";
  if (code >= 400) return "warning";
  return "info";
}

function buildScreenActionLogMessage(req, capturedPayload) {
  const payload = capturedPayload && typeof capturedPayload === "object" ? capturedPayload : null;
  const explicitMessage = String(
    payload?.message || payload?.error || payload?.notice || payload?.details || ""
  ).trim();
  if (explicitMessage) {
    return truncateScreenLogText(explicitMessage, 180);
  }

  const pathname = String(req.path || req.originalUrl || "").trim() || "/";
  if (req.method === "GET") {
    return `Ekran açıldı: ${pathname}`;
  }
  return `${String(req.method || "GET").toUpperCase()} ${pathname}`;
}

function buildScreenActionLogDetailText(req, capturedPayload) {
  const detailParts = [];
  const payload = capturedPayload && typeof capturedPayload === "object" ? capturedPayload : null;
  const payloadDetail = String(payload?.details || payload?.errorDetail || payload?.detail || "").trim();
  if (payloadDetail) {
    detailParts.push(payloadDetail);
  }

  const requestMeta = extractScreenActionRequestMeta(req);
  if (requestMeta.query) {
    detailParts.push(`Query: ${truncateScreenLogText(JSON.stringify(sanitizeScreenLogMeta(requestMeta.query)), 320)}`);
  }
  if (requestMeta.body) {
    detailParts.push(`Body: ${truncateScreenLogText(JSON.stringify(sanitizeScreenLogMeta(requestMeta.body)), 320)}`);
  }

  return truncateScreenLogText(detailParts.join("\n"), 1200);
}

async function insertScreenActionLogEntry({
  menuKey,
  actionKey,
  requestMethod,
  requestPath,
  statusCode,
  level,
  message,
  detailText,
  metaJson,
  userId
}) {
  const normalizedMenuKey = String(menuKey || "").trim();
  const normalizedActionKey = String(actionKey || "").trim() || "screen-action";
  const normalizedMethod = String(requestMethod || "GET").trim().toUpperCase();
  const normalizedPath = String(requestPath || "").trim() || "/";
  const normalizedLevel = String(level || "info").trim() || "info";
  const normalizedMessage = truncateScreenLogText(message, 180) || `${normalizedMethod} ${normalizedPath}`;
  const normalizedDetailText = truncateScreenLogText(detailText, 1200) || null;
  const normalizedMetaJson = truncateScreenLogText(metaJson, 2000) || null;
  const normalizedUserId = Number.isInteger(Number(userId)) ? Number(userId) : null;
  if (!normalizedMenuKey) return;

  try {
    await pool.query(
      `
        INSERT INTO screen_action_logs (
          menu_key,
          action_key,
          request_method,
          request_path,
          status_code,
          level,
          message,
          detail_text,
          meta_json,
          created_by
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      `,
      [
        normalizedMenuKey,
        normalizedActionKey,
        normalizedMethod,
        normalizedPath,
        Number.isFinite(Number(statusCode)) ? Number(statusCode) : null,
        normalizedLevel,
        normalizedMessage,
        normalizedDetailText,
        normalizedMetaJson,
        normalizedUserId
      ]
    );
  } catch (err) {
    console.error("Screen action log insert error:", err);
  }
}

async function canUserViewScreenLogs(user, menuKey) {
  const normalizedMenuKey = String(menuKey || "").trim();
  if (!normalizedMenuKey) return false;
  if (String(user?.username || "").toLowerCase() === "admin") return true;
  const userIdNum = Number(user?.id);
  if (!Number.isInteger(userIdNum)) return false;

  try {
    const result = await pool.query(
      `
        SELECT COALESCE(can_view_logs, false) AS can_view_logs
        FROM user_sidebar_permissions
        WHERE user_id = $1
          AND menu_key = $2
      `,
      [userIdNum, normalizedMenuKey]
    );
    return toSidebarBool(result.rows?.[0]?.can_view_logs, false);
  } catch (err) {
    console.error("Screen log permission read error:", err);
    return false;
  }
}

function buildEmptyScreenLogPanelModel() {
  return {
    visible: false,
    menuKey: "",
    title: "",
    apiPath: "",
    items: []
  };
}

function mapScreenActionLogRow(row) {
  const createdAt = row?.created_at instanceof Date ? row.created_at.toISOString() : String(row?.created_at || "").trim();
  return {
    id: Number(row?.id || 0),
    menuKey: String(row?.menu_key || "").trim(),
    actionKey: String(row?.action_key || "").trim(),
    requestMethod: String(row?.request_method || "").trim(),
    requestPath: String(row?.request_path || "").trim(),
    statusCode: Number.isFinite(Number(row?.status_code)) ? Number(row.status_code) : null,
    level: String(row?.level || "info").trim() || "info",
    message: String(row?.message || "").trim(),
    detailText: String(row?.detail_text || "").trim(),
    metaJson: String(row?.meta_json || "").trim(),
    createdByName: String(row?.created_by_name || "-").trim() || "-",
    createdAt
  };
}

async function loadScreenLogPanelForUser(user, menuKey, limit = 20) {
  const registryItem = getScreenActionRegistryItem(menuKey);
  if (!registryItem) return buildEmptyScreenLogPanelModel();

  const allowed = await canUserViewScreenLogs(user, registryItem.key);
  if (!allowed) return buildEmptyScreenLogPanelModel();

  try {
    const result = await pool.query(
      `
        SELECT
          l.*,
          COALESCE(u.display_name, u.username, '-') AS created_by_name
        FROM screen_action_logs l
        LEFT JOIN users u ON u.id = l.created_by
        WHERE l.menu_key = $1
        ORDER BY l.created_at DESC, l.id DESC
        LIMIT $2
      `,
      [registryItem.key, Math.max(1, Math.min(50, Number(limit) || 20))]
    );

    return {
      visible: true,
      menuKey: registryItem.key,
      title: `${registryItem.label} Logları`,
      apiPath: `/api/screen-logs/${encodeURIComponent(registryItem.key)}`,
      items: result.rows.map(mapScreenActionLogRow)
    };
  } catch (err) {
    console.error("Screen log panel load error:", err);
    return {
      visible: true,
      menuKey: registryItem.key,
      title: `${registryItem.label} Logları`,
      apiPath: `/api/screen-logs/${encodeURIComponent(registryItem.key)}`,
      items: []
    };
  }
}

function requireMenuAccess(menuKey) {
  return async (req, res, next) => {
    req.screenMenuKey = String(menuKey || "").trim();
    if (!req.session?.user) {
      if (req.path.startsWith("/api/")) {
        return res.status(401).json({ ok: false, error: "Oturum süresi doldu." });
      }
      return res.redirect("/login");
    }

    if (String(req.session.user?.username || "").toLowerCase() === "admin") {
      return next();
    }

    try {
      let sidebar = req.sidebar;
      if (!sidebar) {
        sidebar = await loadSidebarForUser(req.session.user.id, {
          isAdmin: String(req.session.user?.username || "").toLowerCase() === "admin"
        });
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
      if (req.method === "GET") {
        return res.status(200).render("no-permission-home", {
          user: req.session.user,
          active: ""
        });
      }
      return res.status(403).send("Bu sayfayı görüntüleme yetkiniz yok.");
    } catch (err) {
      console.error("Menu access check error:", err);
      return res.status(500).send("Yetki kontrolü başarısız.");
    }
  };
}

app.use((req, res, next) => {
  const originalRender = res.render.bind(res);
  res.render = function patchedRender(view, locals, callback) {
    let renderLocals = locals;
    let renderCallback = callback;

    if (typeof renderLocals === "function") {
      renderCallback = renderLocals;
      renderLocals = {};
    }

    const safeLocals =
      renderLocals && typeof renderLocals === "object" ? { ...renderLocals } : {};
    const activeMenuKey = String(safeLocals.active || req.screenMenuKey || "").trim();

    void loadScreenLogPanelForUser(req.session?.user, activeMenuKey)
      .then((screenLogPanel) => {
        safeLocals.screenLogPanel = screenLogPanel;
        originalRender(view, safeLocals, renderCallback);
      })
      .catch((err) => {
        console.error("Screen log render injection error:", err);
        safeLocals.screenLogPanel = buildEmptyScreenLogPanelModel();
        originalRender(view, safeLocals, renderCallback);
      });
  };
  return next();
});

app.use((req, res, next) => {
  const startedAt = Date.now();
  let capturedJsonPayload = null;
  const originalJson = res.json.bind(res);

  res.json = function patchedJson(payload) {
    capturedJsonPayload = payload;
    return originalJson(payload);
  };

  res.on("finish", () => {
    if (!req.session?.user) return;
    if (!req.screenMenuKey) return;
    if (shouldSkipScreenActionLog(req)) return;

    const level = determineScreenActionLogLevel(res.statusCode);
    const metaJson = stringifyScreenLogMeta({
      durationMs: Date.now() - startedAt,
      statusCode: res.statusCode,
      method: req.method,
      path: req.originalUrl || req.path || "/"
    });

    void insertScreenActionLogEntry({
      menuKey: req.screenMenuKey,
      actionKey: `${String(req.method || "GET").toUpperCase()} ${String(req.route?.path || req.path || "/").trim()}`,
      requestMethod: req.method,
      requestPath: req.originalUrl || req.path || "/",
      statusCode: res.statusCode,
      level,
      message: buildScreenActionLogMessage(req, capturedJsonPayload),
      detailText: buildScreenActionLogDetailText(req, capturedJsonPayload),
      metaJson,
      userId: req.session.user?.id
    });
  });

  return next();
});

app.use(async (req, res, next) => {
  if (!req.session?.user) return next();
  if (req.path.startsWith("/api/")) return next();

  try {
    const sidebar = await loadSidebarForUser(req.session.user.id, {
      isAdmin: String(req.session.user?.username || "").toLowerCase() === "admin"
    });
    req.sidebar = sidebar;
    res.locals.sidebar = sidebar;
  } catch (err) {
    console.error("Sidebar load error:", err);
    const fallback = buildSidebarEmptyModel();
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
    let url = "";
    let branchId = "";
    let isAbroad = null;

    if (typeof item === "string") {
      code = String(item).trim();
    } else if (item && typeof item === "object") {
      code = String(item.code || item.value || "").trim();
      id = String(item.id || "").trim();
      cluster = String(item.cluster || "").trim().toLowerCase();
      url = normalizeTargetUrl(item.url || "");
      branchId = String(item.branchId || "").trim();
      isAbroad = parseAllCompaniesBooleanValue(item.isAbroad ?? item.isabroad ?? item.is_abroad);
    }

    if (!code) return;

    const key = `${code}__${id}__${cluster}`;
    if (!byKey.has(key)) {
      byKey.set(key, { code, id, cluster, url, branchId, isAbroad });
      return;
    }

    const current = byKey.get(key);
    if (!current) return;

    let next = current;
    if (!next.url && url) {
      next = { ...next, url };
    }
    if (!next.branchId && branchId) {
      next = { ...next, branchId };
    }
    if (next.isAbroad === null && isAbroad !== null) {
      next = { ...next, isAbroad };
    }
    if (next !== current) {
      byKey.set(key, next);
    }
  });

  return Array.from(byKey.values()).sort((a, b) => {
    const byCode = a.code.localeCompare(b.code, "tr");
    if (byCode !== 0) return byCode;

    const byId = a.id.localeCompare(b.id, "tr");
    if (byId !== 0) return byId;

    const byCluster = a.cluster.localeCompare(b.cluster, "tr");
    if (byCluster !== 0) return byCluster;

    const byUrl = String(a.url || "").localeCompare(String(b.url || ""), "tr");
    if (byUrl !== 0) return byUrl;

    return String(a.branchId || "").localeCompare(String(b.branchId || ""), "tr");
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
      row["partner-id"] ??
      row.partner_id ??
      row.partnerid ??
      row.partnerId ??
      row.partnerID ??
      row["provider-id"] ??
      row.provider_id ??
      row.providerId;
    const id = rawId === undefined || rawId === null ? "" : String(rawId).trim();
    const rawBranchId =
      row["branch-id"] ??
      row.branch_id ??
      row.branchId ??
      row.branchID ??
      row.default_branch_id ??
      row.defaultBranchId ??
      row.active_branch_id ??
      row.activeBranchId ??
      row.selected_branch_id ??
      row.selectedBranchId;
    const branchId = rawBranchId === undefined || rawBranchId === null ? "" : String(rawBranchId).trim();
    const rawUrl =
      row.url ??
      row.URL ??
      row.api_url ??
      row.apiUrl ??
      row.endpoint_url ??
      row.endpointUrl ??
      row.base_url ??
      row.baseUrl;
    let url = normalizeTargetUrl(rawUrl);
    if (!url) {
      const urlEntry = Object.entries(row).find(([key, value]) => {
        if (!/url/i.test(String(key || ""))) return false;
        const normalized = normalizeTargetUrl(value);
        return Boolean(normalized);
      });
      if (urlEntry) {
        url = normalizeTargetUrl(urlEntry[1]);
      }
    }

    items.push({
      code,
      id,
      cluster: String(clusterLabel || "").trim().toLowerCase(),
      url,
      branchId
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

function parseAllCompaniesBooleanValue(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (value === 1) return true;
    if (value === 0) return false;
    return null;
  }
  const normalized = normalizeTokenName(value);
  if (!normalized) return null;
  if (["true", "1", "yes", "evet", "t", "y", "on"].includes(normalized)) return true;
  if (["false", "0", "no", "hayir", "f", "n", "off"].includes(normalized)) return false;
  return null;
}

function formatAllCompaniesBooleanValue(value) {
  const parsed = parseAllCompaniesBooleanValue(value);
  if (parsed === null) return "";
  return parsed ? "true" : "false";
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

function readPartnerRawValueByAliases(row, aliases = []) {
  if (!row || typeof row !== "object") return undefined;
  const aliasSet = new Set(
    (Array.isArray(aliases) ? aliases : [])
      .map((item) => normalizeTokenName(item))
      .filter(Boolean)
  );
  if (aliasSet.size === 0) return undefined;

  for (const [key, value] of Object.entries(row)) {
    if (aliasSet.has(normalizeTokenName(key))) return value;
  }
  return undefined;
}

const ALL_COMPANIES_EXCLUDED_EXACT_CODES = Object.freeze([
  "admin",
  "aou2",
  "dashboard",
  "corp",
  "esbeylikduzud2",
  "mutlularsimsekseyahat",
  "ozcagdastravelturizm",
  "varan",
  "eskisinopbirlik"
]);

const ALL_COMPANIES_EXCLUDED_EXACT_CODE_SET = new Set(
  ALL_COMPANIES_EXCLUDED_EXACT_CODES.map((code) => normalizeTokenName(code))
);

const ALL_COMPANIES_EXCLUDED_RULE_DESCRIPTIONS = Object.freeze([
  "Parçalarından biri `test` olan code'lar",
  "Parçalarından biri `old` olan code'lar",
  "`test` ile başlayan veya biten code'lar",
  "`old` ile başlayan veya biten code'lar"
]);

function shouldExcludeAllCompaniesCode(codeValue) {
  const rawCode = String(codeValue || "").trim().toLocaleLowerCase("tr");
  if (!rawCode) return false;

  const normalizedCode = normalizeTokenName(rawCode);
  if (ALL_COMPANIES_EXCLUDED_EXACT_CODE_SET.has(normalizedCode)) return true;
  const normalizedSegments = rawCode.split(/[^a-z0-9]+/i).filter(Boolean);
  if (normalizedSegments.includes("test") || normalizedSegments.includes("old")) return true;
  if (rawCode.startsWith("test") || rawCode.endsWith("test")) return true;
  if (rawCode.startsWith("old") || rawCode.endsWith("old")) return true;

  return false;
}

function buildAllCompaniesExclusionSummary() {
  return {
    exactCodes: Array.from(ALL_COMPANIES_EXCLUDED_EXACT_CODES),
    automaticRules: Array.from(ALL_COMPANIES_EXCLUDED_RULE_DESCRIPTIONS)
  };
}

function attachAllCompaniesMissingObusDebug(rows, fallbackMessage = "") {
  const detailText = String(fallbackMessage || "").trim();
  if (!Array.isArray(rows) || rows.length === 0 || !detailText) {
    return Array.isArray(rows) ? rows : [];
  }

  return rows.map((row) => {
    const branchId = String(row?.ObusMerkezSubeID || "").trim();
    const existingDebug = String(row?.ObusMerkezSubeIDDebug || "").trim();
    if (branchId || existingDebug) return row;
    return {
      ...row,
      ObusMerkezSubeIDDebug: detailText
    };
  });
}

function isAllCompaniesObusMerkezDebugTarget(row) {
  const code = normalizeTokenName(row?.code);
  if (code === "corp") return true;
  const url = String(row?.url || "").trim().toLocaleLowerCase("tr");
  return url.includes("corp.obus.com.tr");
}

function logAllCompaniesObusMerkezDebug(stage, details = {}) {
  const normalizedStage = String(stage || "").trim() || "unknown";
  const payload = details && typeof details === "object" ? details : { detail: String(details || "") };
  let serialized = "";
  try {
    serialized = JSON.stringify(payload);
  } catch (err) {
    serialized = String(payload);
  }
  console.log(
    `[AllCompanies][ObusMerkezSubeID][corp] ${normalizedStage}${serialized ? ` | ${serialized}` : ""}`
  );
}

function normalizeAllCompaniesReportRows(rows) {
  const reportColumns = [
    "id",
    "code",
    "source",
    "obilet-partner-id",
    "biletall-partner-id",
    "url",
    "isabroad",
    "ObusMerkezSubeID"
  ];

  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .filter((row) => {
      const statusRaw = readPartnerRawValueByAliases(row, ["status", "status-code", "status_code"]);
      if (Number(statusRaw) !== 1) return false;
      const codeRaw = readPartnerRawValueByAliases(row, ["code"]);
      if (shouldExcludeAllCompaniesCode(codeRaw)) return false;
      return true;
    })
    .map((row) => ({
      id: formatPartnerCellValue(
        readPartnerRawValueByAliases(row, [
          "id",
          "partner-id",
          "partner_id",
          "partnerid",
          "partnerId",
          "partnerID",
          "provider-id",
          "provider_id",
          "providerid",
          "providerId",
          "providerID"
        ])
      ),
      code: formatPartnerCellValue(readPartnerRawValueByAliases(row, ["code"])),
      source: formatPartnerCellValue(
        readPartnerRawValueByAliases(row, ["source", "source_cluster", "sourcecluster", "cluster"])
      ),
      "obilet-partner-id": formatPartnerCellValue(
        readPartnerRawValueByAliases(row, [
          "obilet-partner-id",
          "obilet_partner_id",
          "obiletpartnerid",
          "obiletPartnerId",
          "obiletPartnerID"
        ])
      ),
      "biletall-partner-id": formatPartnerCellValue(
        readPartnerRawValueByAliases(row, [
          "biletall-partner-id",
          "biletall_partner_id",
          "biletallpartnerid",
          "biletallPartnerId",
          "biletallPartnerID"
        ])
      ),
      url: formatPartnerCellValue(
        readPartnerRawValueByAliases(row, ["url", "api_url", "apiUrl", "endpoint_url", "endpointUrl", "base_url", "baseUrl"])
      ),
      isabroad: formatAllCompaniesBooleanValue(
        readPartnerRawValueByAliases(row, ["isabroad", "is_abroad", "is-abroad", "isAbroad", "IsAbroad"])
      ),
      ObusMerkezSubeID: "",
      ObusMerkezSubeIDDebug: ""
    }));

  return {
    columns: reportColumns,
    rows: normalizedRows
  };
}

function buildAllCompaniesCacheRowKey(row) {
  const source = extractClusterLabel(String(row?.source || "").trim());
  const id = String(row?.id || "").trim();
  const code = String(row?.code || "").trim();
  return `${source}|||${id}|||${code}`;
}

function normalizeAllCompaniesCacheRow(row) {
  const id = formatPartnerCellValue(row?.id);
  const code = formatPartnerCellValue(row?.code);
  if (shouldExcludeAllCompaniesCode(code)) return null;
  const source = extractClusterLabel(formatPartnerCellValue(row?.source));
  return {
    id,
    code,
    source,
    "obilet-partner-id": formatPartnerCellValue(row?.["obilet-partner-id"] ?? row?.obilet_partner_id),
    "biletall-partner-id": formatPartnerCellValue(row?.["biletall-partner-id"] ?? row?.biletall_partner_id),
    url: formatPartnerCellValue(row?.url),
    isabroad: formatAllCompaniesBooleanValue(row?.isabroad ?? row?.is_abroad ?? row?.["is-abroad"]),
    ObusMerkezSubeID: formatPartnerCellValue(row?.ObusMerkezSubeID ?? row?.obus_merkez_sube_id),
    ObusMerkezSubeIDDebug: formatPartnerCellValue(row?.ObusMerkezSubeIDDebug ?? row?.obus_merkez_sube_id_debug)
  };
}

function normalizeAllCompaniesCacheRows(rows) {
  const deduped = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const normalized = normalizeAllCompaniesCacheRow(row);
    if (!normalized) return;
    const key = buildAllCompaniesCacheRowKey(normalized);
    deduped.set(key, normalized);
  });
  return Array.from(deduped.values()).sort((a, b) => {
    const byCluster = String(a.source || "").localeCompare(String(b.source || ""), "tr");
    if (byCluster !== 0) return byCluster;
    const byCode = String(a.code || "").localeCompare(String(b.code || ""), "tr");
    if (byCode !== 0) return byCode;
    return String(a.id || "").localeCompare(String(b.id || ""), "tr");
  });
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

function extractBranchIdFromText(raw) {
  const text = String(raw || "");
  if (!text.trim()) return "";
  const match = text.match(/branch[^0-9]{0,20}(\d{1,12})/i);
  return match ? String(match[1] || "").trim() : "";
}

function parseJwtPayload(token) {
  const rawToken = String(token || "").trim();
  if (!rawToken) return null;
  const parts = rawToken.split(".");
  if (parts.length < 2) return null;
  const payloadPart = parts[1];
  if (!payloadPart) return null;
  try {
    const normalized = payloadPart.replace(/-/g, "+").replace(/_/g, "/");
    const padLength = (4 - (normalized.length % 4)) % 4;
    const padded = normalized + "=".repeat(padLength);
    const decoded = Buffer.from(padded, "base64").toString("utf8");
    return parseJsonSafe(decoded);
  } catch (err) {
    return null;
  }
}

function extractBranchIdFromToken(token) {
  const payload = parseJwtPayload(token);
  if (!payload) return "";
  return (
    findNestedValue(payload, new Set(["branchid", "defaultbranchid", "activebranchid", "selectedbranchid"])) ||
    findNestedValue(payload, new Set(["branch", "branchcode"])) ||
    ""
  );
}

function extractBranchIdFromHeaders(headers) {
  const normalizedHeaders = headers && typeof headers === "object" ? headers : {};
  const matchingEntry = Object.entries(normalizedHeaders).find(([key]) =>
    /branch[-_]?id|x[-_]?branch/i.test(String(key || ""))
  );
  if (!matchingEntry) return "";
  const [, value] = matchingEntry;
  const direct = String(value || "").trim();
  if (direct) return direct;
  return "";
}

function buildObusMerkezPartnerClusterKey(partnerId = "", clusterLabel = "") {
  const normalizedPartnerId = String(partnerId || "").trim();
  const normalizedClusterLabel = extractClusterLabel(clusterLabel);
  if (!normalizedPartnerId || !normalizedClusterLabel) return "";
  return `${normalizedClusterLabel}|||${normalizedPartnerId}`;
}

function extractMembershipTokenDataFromPayload(payload) {
  if (!payload || typeof payload !== "object") return "";

  const normalizeString = (value) => {
    if (value === undefined || value === null) return "";
    const text = String(value).trim();
    return text;
  };

  const isLikelyTokenString = (value, minLen = 8) => {
    const text = normalizeString(value);
    if (!text) return false;
    if (/^(null|undefined|true|false)$/i.test(text)) return false;
    if (/\s/.test(text)) return false;
    if (text.length >= Math.max(8, minLen)) return true;
    return /^eyJ[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}\.[a-zA-Z0-9_-]{8,}$/.test(text);
  };

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
      if (normalizedKey === "token") {
        if (value && typeof value === "object" && !Array.isArray(value)) {
          const tokenCandidates = [
            readPartnerRawValueByAliases(value, [
              "data",
              "value",
              "token",
              "access-token",
              "access_token",
              "accessToken",
              "authorization-token",
              "authorization_token",
              "authorizationToken",
              "bearer",
              "jwt",
              "id"
            ]),
            ...Object.values(value || {})
          ];
          for (const candidate of tokenCandidates) {
            const tokenData = normalizeString(candidate);
            if (isLikelyTokenString(tokenData, 8)) return tokenData;
          }
        } else if (Array.isArray(value)) {
          for (const item of value) {
            const tokenData =
              normalizeString(readPartnerRawValueByAliases(item, ["data", "value", "token"])) || normalizeString(item);
            if (isLikelyTokenString(tokenData, 8)) return tokenData;
          }
        } else if (isLikelyTokenString(value, 8)) {
          return normalizeString(value);
        }
      }

      if (
        normalizedKey.includes("token") ||
        normalizedKey === "accesstoken" ||
        normalizedKey === "authorizationtoken" ||
        normalizedKey === "jwttoken" ||
        normalizedKey === "bearertoken"
      ) {
        if (value && typeof value === "object" && !Array.isArray(value)) {
          const tokenData = normalizeString(
            readPartnerRawValueByAliases(value, [
              "data",
              "value",
              "token",
              "access-token",
              "access_token",
              "accessToken",
              "authorization-token",
              "authorization_token",
              "authorizationToken",
              "bearer",
              "jwt",
              "id"
            ])
          );
          if (isLikelyTokenString(tokenData, 8)) return tokenData;
        } else if (isLikelyTokenString(value, 8)) {
          return normalizeString(value);
        }
      }

      if (value && typeof value === "object") {
        queue.push(value);
      }
    }
  }

  return "";
}

function extractTokenFromHeaders(headers) {
  const source = headers && typeof headers === "object" ? headers : {};
  const directTokenHeader =
    String(
      source.token ||
        source.Token ||
        source["x-token"] ||
        source["x-auth-token"] ||
        source["access-token"] ||
        source["access_token"] ||
        source["authorization-token"] ||
        source["authorization_token"] ||
        ""
    ).trim();
  if (directTokenHeader) {
    const tokenValue = directTokenHeader.replace(/^Bearer\s+/i, "").trim();
    if (tokenValue) return tokenValue;
  }

  const authorizationHeader =
    String(
      source.authorization ||
        source.Authorization ||
        source["x-authorization"] ||
        source["x-access-token"] ||
        ""
    ).trim();
  if (authorizationHeader) {
    const bearerMatch = authorizationHeader.match(/Bearer\s+(.+)$/i);
    if (bearerMatch && String(bearerMatch[1] || "").trim()) {
      return String(bearerMatch[1] || "").trim();
    }
    if (authorizationHeader.length > 20) return authorizationHeader;
  }

  const cookieHeader =
    String(source["set-cookie"] || source["Set-Cookie"] || source.cookie || source.Cookie || "").trim();
  if (cookieHeader) {
    const tokenMatch = cookieHeader.match(/(?:^|[;,]\s*)(?:token|access[_-]?token|authorization)\s*=\s*([^;,\s]+)/i);
    if (tokenMatch && String(tokenMatch[1] || "").trim()) {
      return String(tokenMatch[1] || "")
        .trim()
        .replace(/^"+|"+$/g, "");
    }
  }

  return "";
}

function extractTokenFromRawText(raw) {
  const text = String(raw || "");
  if (!text.trim()) return "";

  const patterns = [
    /"token"\s*:\s*\{[^}]*"data"\s*:\s*"([^"]+)"/i,
    /"data"\s*:\s*\{[^}]*"token"\s*:\s*\{[^}]*"data"\s*:\s*"([^"]+)"/i,
    /"access[_-]?token"\s*:\s*"([^"]+)"/i,
    /"authorization[_-]?token"\s*:\s*"([^"]+)"/i,
    /"token"\s*:\s*"([^"]+)"/i,
    /Bearer\s+([A-Za-z0-9\-._~+/=]+)/i
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match && String(match[1] || "").trim()) {
      return String(match[1] || "").trim();
    }
  }

  const jwtMatch = text.match(/eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}/);
  if (jwtMatch && String(jwtMatch[0] || "").trim()) {
    return String(jwtMatch[0] || "").trim();
  }

  return "";
}

function truncateObusDebugText(value, maxLength = 220) {
  const text = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 3))}...`;
}

function isSensitiveObusDebugKey(key) {
  return /password|token|authorization|cookie|secret/i.test(String(key || "").trim());
}

function sanitizeObusDebugStructure(value) {
  if (Array.isArray(value)) {
    return value.map((item) => sanitizeObusDebugStructure(item));
  }
  if (!value || typeof value !== "object") {
    return value;
  }

  const output = {};
  Object.entries(value).forEach(([key, itemValue]) => {
    output[key] = isSensitiveObusDebugKey(key) ? "***" : sanitizeObusDebugStructure(itemValue);
  });
  return output;
}

function maskSensitiveObusDebugText(value) {
  let text = String(value || "");
  if (!text.trim()) return "";

  text = text.replace(/("password"\s*:\s*")([^"]*)(")/gi, '$1***$3');
  text = text.replace(
    /("(?:token|access[_-]?token|authorization[_-]?token|cookie|authorization|secret)"\s*:\s*")([^"]*)(")/gi,
    '$1***$3'
  );
  text = text.replace(/(Bearer\s+)([A-Za-z0-9\-._~+/=]+)/gi, "$1***");
  text = text.replace(/((?:token|access[_-]?token|authorization|cookie)=)([^;,\s]+)/gi, "$1***");
  return text;
}

function normalizeObusDebugPayload(value) {
  if (value === null || value === undefined) return "";

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return "";
    const parsed = parseJsonSafe(trimmed);
    if (parsed !== null && typeof parsed === "object") {
      try {
        return JSON.stringify(sanitizeObusDebugStructure(parsed));
      } catch (err) {
        return truncateObusDebugText(maskSensitiveObusDebugText(trimmed), 1200);
      }
    }
    return truncateObusDebugText(maskSensitiveObusDebugText(trimmed), 1200);
  }

  if (typeof value === "object") {
    try {
      return JSON.stringify(sanitizeObusDebugStructure(value));
    } catch (err) {
      return truncateObusDebugText(maskSensitiveObusDebugText(String(value || "")), 1200);
    }
  }

  return truncateObusDebugText(maskSensitiveObusDebugText(String(value || "")), 1200);
}

function buildObusServiceTraceEntry({
  service = "",
  url = "",
  status = null,
  requestBody = "",
  responseBody = "",
  error = "",
  note = ""
} = {}) {
  const parsedStatus =
    typeof status === "number" ? status : Number.parseInt(String(status ?? "").trim(), 10);
  const normalizedStatus = Number.isFinite(parsedStatus) ? parsedStatus : null;
  return {
    service: String(service || "").trim() || "UnknownService",
    url: String(url || "").trim(),
    status: normalizedStatus,
    requestBody: normalizeObusDebugPayload(requestBody),
    responseBody: normalizeObusDebugPayload(responseBody),
    error: truncateObusDebugText(error, 260),
    note: truncateObusDebugText(note, 260)
  };
}

function getLastObusServiceTrace(serviceLogs) {
  const logs = Array.isArray(serviceLogs)
    ? serviceLogs.filter((item) => item && typeof item === "object")
    : [];
  if (logs.length === 0) return null;
  return logs[logs.length - 1];
}

function getFirstObusServiceTrace(serviceLogs) {
  const logs = Array.isArray(serviceLogs)
    ? serviceLogs.filter((item) => item && typeof item === "object")
    : [];
  return logs[0] || null;
}

function buildObusRequestPreviewFromTrace(trace, fallback = {}) {
  const entry = trace && typeof trace === "object" ? trace : null;
  const fallbackValue = fallback && typeof fallback === "object" ? fallback : {};
  const statusValue = entry?.status ?? fallbackValue.status;
  const parsedStatus =
    typeof statusValue === "number" ? statusValue : Number.parseInt(String(statusValue ?? "").trim(), 10);
  const service = String(entry?.service || fallbackValue.service || "").trim();
  const requestUrl = String(entry?.url || fallbackValue.requestUrl || "").trim();
  const requestBody = String(entry?.requestBody || fallbackValue.requestBody || "").trim();
  const responseBody = String(entry?.responseBody || entry?.error || fallbackValue.responseBody || fallbackValue.error || "").trim();
  if (!service && !requestUrl && !requestBody && !responseBody) {
    return null;
  }
  const preview = {
    service,
    status: Number.isFinite(parsedStatus) ? parsedStatus : null,
    requestUrl,
    requestBody: requestBody || "{}",
    responseBody: responseBody || "-"
  };
  return preview;
}

function buildObusServiceTraceText(trace, fallbackError = "", { bodyMaxLen = 160, responseMaxLen = 220 } = {}) {
  const entry = trace && typeof trace === "object" ? trace : null;
  const parts = [];

  if (entry) {
    parts.push(`servis=${String(entry.service || "").trim() || "-"}`);
    if (entry.status !== null) parts.push(`status=${entry.status}`);
    if (entry.url) parts.push(`url=${truncateObusDebugText(entry.url, 120)}`);
    if (entry.note) parts.push(`not=${entry.note}`);
    if (entry.error) parts.push(`hata=${entry.error}`);
    if (entry.requestBody) parts.push(`body=${truncateObusDebugText(entry.requestBody, bodyMaxLen)}`);
    if (entry.responseBody) parts.push(`response=${truncateObusDebugText(entry.responseBody, responseMaxLen)}`);
  }

  const normalizedFallbackError = truncateObusDebugText(fallbackError, 220);
  if (normalizedFallbackError && !parts.some((item) => item.startsWith("hata="))) {
    parts.push(`hata=${normalizedFallbackError}`);
  }

  return parts.join(" | ");
}

function extractObusApiLogDetail(payload, rawBody = "", fallbackText = "") {
  const normalizedPayload = payload && typeof payload === "object" ? payload : null;
  const logCandidate = normalizedPayload
    ? getDeepValueByKeyMatcher(
        normalizedPayload,
        (normalizedKey) =>
          [
            "log",
            "logs",
            "servicelog",
            "servicelogs",
            "failedservicelog",
            "failedservicelogs",
            "debug",
            "trace",
            "traces"
          ].includes(normalizedKey)
      )
    : undefined;
  const detailCandidate =
    logCandidate !== undefined
      ? logCandidate
      : normalizedPayload
        ? getDeepValueByKeyMatcher(
            normalizedPayload,
            (normalizedKey) => ["detail", "details", "description", "reason", "note", "notes"].includes(normalizedKey)
          )
        : undefined;

  const detailText = truncateObusDebugText(
    normalizeObusDebugPayload(detailCandidate !== undefined ? detailCandidate : ""),
    260
  );
  const fallbackDetail = truncateObusDebugText(maskSensitiveObusDebugText(rawBody), 260);
  const fallbackError = truncateObusDebugText(String(fallbackText || "").trim(), 220);

  if (detailText && detailText !== fallbackError) return detailText;
  if (fallbackDetail && fallbackDetail !== fallbackError) return `Ham yanıt: ${fallbackDetail}`;
  return "";
}

function buildUserLoginTokenMissingDetail({
  loginUrl,
  sessionId,
  deviceId,
  responseStatus,
  parsedBody,
  responseHeaders,
  rawBody
}) {
  const statusText = Number.isFinite(Number(responseStatus)) ? `HTTP ${Number(responseStatus)}` : "HTTP bilinmiyor";
  const headerKeys = Object.keys(responseHeaders && typeof responseHeaders === "object" ? responseHeaders : {})
    .filter((key) => /token|auth|cookie/i.test(String(key || "")))
    .sort((a, b) => String(a).localeCompare(String(b), "tr"));
  const rootKeys =
    parsedBody && typeof parsedBody === "object" && !Array.isArray(parsedBody)
      ? Object.keys(parsedBody).slice(0, 12)
      : [];
  const rawPreview = truncateObusDebugText(rawBody, 260);
  const normalizedUrl = String(loginUrl || "").trim();
  const parts = [
    "Adım 1 tamam: Session bilgisi alındı.",
    `session-id: ${sessionId ? "var" : "yok"}, device-id: ${deviceId ? "var" : "yok"}.`,
    `Adım 2 tamam: Membership UserLogin yanıtı alındı (${statusText}).`,
    "Adım 3 başarısız: token çıkarılamadı.",
    "Kontrol edilen kaynaklar: payload token alanları, nested token alanları, response header/cookie ve ham yanıt metni."
  ];

  if (headerKeys.length > 0) {
    parts.push(`Token ile ilişkili header anahtarları: ${headerKeys.join(", ")}.`);
  } else {
    parts.push("Token ile ilişkili header anahtarı bulunamadı.");
  }

  if (rootKeys.length > 0) {
    parts.push(`Yanıt üst seviye alanları: ${rootKeys.join(", ")}.`);
  }

  if (rawPreview) {
    parts.push(`Yanıt önizleme: ${rawPreview}.`);
  }

  if (normalizedUrl) {
    parts.push(`UserLogin URL: ${normalizedUrl}.`);
  }

  return parts.join(" ");
}

function extractPartnerCodeFromPayload(payload) {
  if (!payload || typeof payload !== "object") return "";

  const readValue = (value) => {
    if (value === undefined || value === null) return "";
    return String(value).trim();
  };

  const direct =
    readValue(payload["partner-code"]) ||
    readValue(payload.partnerCode) ||
    readValue(payload.partner_code);
  if (direct) return direct;

  if (payload.data && typeof payload.data === "object") {
    const nested =
      readValue(payload.data["partner-code"]) ||
      readValue(payload.data.partnerCode) ||
      readValue(payload.data.partner_code);
    if (nested) return nested;
  }

  return "";
}

function isSuccessStatusPayload(payload) {
  if (!payload || typeof payload !== "object") return false;

  const statusText = String(payload.status || "").trim().toLowerCase();
  if (statusText === "success" || statusText === "ok" || statusText === "1") return true;

  const statusValue = Number(payload.status);
  if (Number.isFinite(statusValue) && statusValue === 1) return true;

  if (payload.success === true) return true;

  const statusCode = Number(payload["status-code"]);
  if (Number.isFinite(statusCode) && statusCode === 1) return true;

  return false;
}

function collectUserLoginBranchCandidates(node, candidates) {
  if (!node || typeof node !== "object") return;

  if (Array.isArray(node)) {
    node.forEach((item) => collectUserLoginBranchCandidates(item, candidates));
    return;
  }

  const keySet = new Set(["branchid", "defaultbranchid", "activebranchid", "selectedbranchid"]);
  const direct = findNestedValue(node, keySet);
  if (direct) {
    candidates.push(String(direct).trim());
  }

  Object.entries(node).forEach(([key, value]) => {
    if (!value || typeof value !== "object") return;
    const normalized = normalizeTokenName(key);

    if (normalized.includes("branch") && Array.isArray(value)) {
      value.forEach((item) => {
        if (!item || typeof item !== "object" || Array.isArray(item)) return;
        const branchId =
          findNestedValue(item, new Set(["branchid"])) ||
          findNestedValue(item, new Set(["id"]));
        if (branchId) {
          candidates.push(String(branchId).trim());
        }
      });
    }

    collectUserLoginBranchCandidates(value, candidates);
  });
}

function extractBranchIdFromUserLoginPayload(payload) {
  const candidates = [];
  collectUserLoginBranchCandidates(payload, candidates);
  const firstValid = candidates.find((item) => String(item || "").trim().length > 0);
  return firstValid ? String(firstValid).trim() : "";
}

function extractObusMerkezBranchKeyFromBranchListNode(node) {
  const normalizeText = (value) => String(value === undefined || value === null ? "" : value).trim();
  const isObusMerkez = (value) => normalizeTokenName(value) === "obusmerkez";
  const parseStructuredText = (value) => {
    const text = normalizeText(value);
    if (!text) return null;
    if ((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"))) {
      return parseJsonSafe(text);
    }
    return null;
  };

  const walk = (current, entryKey = "") => {
    if (current === null || current === undefined) return "";
    if (typeof current === "string") {
      const parsed = parseStructuredText(current);
      return parsed === null ? "" : walk(parsed, entryKey);
    }
    if (Array.isArray(current)) {
      for (const item of current) {
        const found = walk(item);
        if (found) return found;
      }
      return "";
    }
    if (typeof current !== "object") return "";

    const labelValue =
      readPartnerRawValueByAliases(current, [
        "value",
        "name",
        "label",
        "title",
        "text",
        "branch-name",
        "branch_name",
        "branchName"
      ]) || "";
    if (isObusMerkez(labelValue)) {
      const keyValue =
        readPartnerRawValueByAliases(current, [
          "key",
          "id",
          "branch-id",
          "branch_id",
          "branchid",
          "branch-key",
          "branch_key",
          "branchkey"
        ]) || entryKey;
      const normalizedKey = normalizeText(keyValue);
      if (normalizedKey) return normalizedKey;
    }

    for (const [key, value] of Object.entries(current)) {
      const keyText = normalizeText(key);
      if (isObusMerkez(value) && keyText) {
        return keyText;
      }
      const found = walk(value, keyText);
      if (found) return found;
    }

    return "";
  };

  return walk(node);
}

function extractObusMerkezBranchKeyFromUserLoginPayload(payload) {
  if (!payload || typeof payload !== "object") return "";
  const queue = [{ node: payload, multipleBranchesContext: false }];
  const visited = new Set();

  while (queue.length > 0) {
    const { node: current, multipleBranchesContext } = queue.shift() || {};
    if (!current || typeof current !== "object") continue;
    if (visited.has(current)) continue;
    visited.add(current);

    if (Array.isArray(current)) {
      current.forEach((item) => {
        if (item && typeof item === "object") {
          queue.push({ node: item, multipleBranchesContext });
          return;
        }
        if (typeof item === "string") {
          const parsed = parseJsonSafe(item);
          if (parsed && typeof parsed === "object") {
            queue.push({ node: parsed, multipleBranchesContext });
          }
        }
      });
      continue;
    }

    const stateValue =
      String(readPartnerRawValueByAliases(current, ["state", "login-state", "login_state"]) || "").trim();
    const normalizedState = normalizeTokenName(stateValue);
    const hasBranchSelectionState =
      normalizedState === "multiplebranches" || normalizedState === "multipleusermodules";
    const currentContext = Boolean(multipleBranchesContext || hasBranchSelectionState);

    if (currentContext) {
      for (const [key, value] of Object.entries(current)) {
        const normalizedKey = normalizeTokenName(key);
        if (
          normalizedKey === "branches" ||
          normalizedKey === "brancheslist" ||
          normalizedKey === "branchlist" ||
          normalizedKey === "branchoptions" ||
          normalizedKey === "branchoptionlist" ||
          normalizedKey.includes("branches") ||
          normalizedKey.includes("branch")
        ) {
          const key = extractObusMerkezBranchKeyFromBranchListNode(value);
          if (key) return key;
        }
      }
    }

    Object.values(current).forEach((value) => {
      if (value && typeof value === "object") {
        queue.push({ node: value, multipleBranchesContext: currentContext });
        return;
      }
      if (typeof value === "string") {
        const parsed = parseJsonSafe(value);
        if (parsed && typeof parsed === "object") {
          queue.push({ node: parsed, multipleBranchesContext: currentContext });
        }
      }
    });
  }

  return "";
}

function buildUserLoginBaseUrls(companyUrl, endpointUrl) {
  const values = [companyUrl, endpointUrl]
    .map((item) => normalizeTargetUrl(item))
    .filter(Boolean);
  const deduped = [];
  values.forEach((value) => {
    if (!deduped.includes(value)) deduped.push(value);
  });
  return deduped;
}

function normalizePartnerIdForRouting(partnerId) {
  const text = String(partnerId || "").trim();
  if (!/^-?\d+$/.test(text)) return "";
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? String(parsed) : "";
}

function shouldUsePreprodUserLoginForPartner({ partnerCode, partnerId } = {}) {
  if (normalizeTokenName(partnerCode) !== "corp") return false;
  const requiredPartnerId = normalizePartnerIdForRouting(PARTNERS_REQUIRED_EXTRA_ONLY_ID);
  const normalizedPartnerId = normalizePartnerIdForRouting(partnerId);
  return Boolean(requiredPartnerId && normalizedPartnerId && normalizedPartnerId === requiredPartnerId);
}

function buildUserLoginBaseUrlsWithOverrides({ companyUrl, endpointUrl, partnerCode, partnerId } = {}) {
  const baseUrls = buildUserLoginBaseUrls(companyUrl, endpointUrl);
  if (!shouldUsePreprodUserLoginForPartner({ partnerCode, partnerId })) return baseUrls;

  const preprodBaseUrl = normalizeTargetUrl(PARTNERS_REQUIRED_EXTRA_API_URL);
  if (!preprodBaseUrl) return baseUrls;

  const preferredBaseUrls = [preprodBaseUrl];
  baseUrls.forEach((item) => {
    const normalized = normalizeTargetUrl(item);
    if (!normalized) return;
    if (!preferredBaseUrls.includes(normalized)) preferredBaseUrls.push(normalized);
  });
  return preferredBaseUrls;
}

function buildUniqueLoginBranchCandidates(...values) {
  const unique = [];
  values.flat().forEach((value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    if (!unique.includes(normalized)) unique.push(normalized);
  });
  return unique;
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

function normalizePartnerGetPartnersUrl(baseUrl) {
  const raw = String(baseUrl || "").trim();
  if (!raw) return "";
  const normalized = normalizeTargetUrl(raw);
  if (!normalized) return "";

  try {
    const parsed = new URL(normalized);
    const pathname = String(parsed.pathname || "/");

    if (/\/api\/partner\/getpartners\/?$/i.test(pathname)) {
      parsed.pathname = "/api/partner/getpartners";
    } else if (pathname === "/" || /\/api\/?$/i.test(pathname)) {
      parsed.pathname = "/api/partner/getpartners";
    }

    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildPartnerFetchUrls() {
  const extraBaseUrls = String(PARTNERS_EXTRA_API_URLS_RAW || "")
    .split(",")
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  const sourceBaseUrls = [PARTNERS_API_URL, PARTNERS_REQUIRED_EXTRA_API_URL, ...extraBaseUrls];
  const expandedUrls = [];
  sourceBaseUrls.forEach((sourceUrl) => {
    buildClusterPartnerUrls(sourceUrl).forEach((expandedUrl) => {
      expandedUrls.push(expandedUrl);
    });
  });

  const deduped = [];
  expandedUrls.forEach((url) => {
    const normalized = normalizePartnerGetPartnersUrl(url);
    if (!normalized) return;
    if (!deduped.includes(normalized)) deduped.push(normalized);
  });
  return deduped;
}

function resolvePartnerFetchUrlByCluster(clusterLabel) {
  const normalizedCluster = extractClusterLabel(clusterLabel);
  const partnerUrls = buildPartnerFetchUrls();
  if (!Array.isArray(partnerUrls) || partnerUrls.length === 0) return "";

  const exactMatch = partnerUrls.find((url) => extractClusterLabel(url) === normalizedCluster);
  if (exactMatch) return exactMatch;

  if (/^cluster\d+$/i.test(normalizedCluster)) {
    const clusterShifted = partnerUrls
      .map((url) => buildUrlForCluster(url, normalizedCluster))
      .map((url) => normalizePartnerGetPartnersUrl(url))
      .find(Boolean);
    if (clusterShifted) return clusterShifted;
  }

  return partnerUrls[0] || "";
}

function isRequiredExtraPartnerFetchUrl(partnerUrl) {
  const normalizedPartnerUrl = normalizePartnerGetPartnersUrl(partnerUrl);
  if (!normalizedPartnerUrl) return false;
  const normalizedRequiredUrl = normalizePartnerGetPartnersUrl(PARTNERS_REQUIRED_EXTRA_API_URL);
  return normalizedRequiredUrl ? normalizedPartnerUrl === normalizedRequiredUrl : false;
}

function filterAllCompaniesRowsForSource(partnerUrl, rows) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  if (!isRequiredExtraPartnerFetchUrl(partnerUrl)) return sourceRows;

  return sourceRows.filter((row) => {
    const partnerId = readPartnerRawValueByAliases(row, [
      "id",
      "partner-id",
      "partner_id",
      "partnerid",
      "partnerId",
      "partnerID",
      "provider-id",
      "provider_id",
      "providerid",
      "providerId",
      "providerID"
    ]);
    return String(partnerId ?? "").trim() === PARTNERS_REQUIRED_EXTRA_ONLY_ID;
  });
}

function buildUrlForCluster(baseUrl, clusterLabel) {
  const raw = String(baseUrl || "").trim();
  const cluster = String(clusterLabel || "").trim().toLowerCase();
  if (!raw) return "";
  if (!cluster) return raw;

  if (/cluster\d+/i.test(raw) && /^cluster\d+$/i.test(cluster)) {
    return raw.replace(/cluster\d+/i, cluster);
  }
  return raw;
}

function extractClusterLabel(url) {
  const match = String(url || "").match(/cluster\d+/i);
  return match ? match[0].toLowerCase() : "cluster";
}

function normalizeObusClusterLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return /^cluster\d+$/.test(normalized) ? normalized : "";
}

function buildSessionUrlForPartnerUrl(partnerUrl, sessionClusterLabel = "") {
  const normalizedCluster =
    normalizeObusClusterLabel(sessionClusterLabel) ||
    normalizeObusClusterLabel(extractClusterLabel(partnerUrl)) ||
    "";
  const clusteredSessionUrl = normalizedCluster
    ? normalizeTargetUrl(buildUrlForCluster(PARTNERS_SESSION_API_URL, normalizedCluster))
    : "";
  if (clusteredSessionUrl) {
    return clusteredSessionUrl;
  }

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

function buildJourneySearchStationsUrl(companyUrl, clusterLabel = "") {
  const fallbackUrl = buildUrlForCluster(PARTNERS_API_URL, clusterLabel);
  const normalizedBaseUrl = normalizeTargetUrl(companyUrl) || normalizeTargetUrl(fallbackUrl);
  if (!normalizedBaseUrl) return "";

  try {
    const parsed = new URL(String(normalizedBaseUrl || ""));
    const pathname = String(parsed.pathname || "/");
    const apiMatch = pathname.match(/^(.+?\/api\/?)/i);
    const apiPrefixRaw = apiMatch ? apiMatch[1] : "/api/";
    const apiPrefix = apiPrefixRaw.endsWith("/") ? apiPrefixRaw : `${apiPrefixRaw}/`;
    parsed.pathname = normalizeApiPath(`${apiPrefix}web/getstations`, "/api/web/getstations");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildJourneySearchGetJourneysUrl(companyUrl, clusterLabel = "") {
  const fallbackUrl = buildUrlForCluster(PARTNERS_API_URL, clusterLabel);
  const normalizedBaseUrl = normalizeTargetUrl(companyUrl) || normalizeTargetUrl(fallbackUrl);
  if (!normalizedBaseUrl) return "";

  try {
    const parsed = new URL(String(normalizedBaseUrl || ""));
    const pathname = String(parsed.pathname || "/");
    const apiMatch = pathname.match(/^(.+?\/api\/?)/i);
    const apiPrefixRaw = apiMatch ? apiMatch[1] : "/api/";
    const apiPrefix = apiPrefixRaw.endsWith("/") ? apiPrefixRaw : `${apiPrefixRaw}/`;
    parsed.pathname = normalizeApiPath(`${apiPrefix}web/getjourneys`, "/api/web/getjourneys");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildJourneySearchJourneysRequestBody({
  origin = "",
  destination = "",
  from = "",
  to = "",
  sessionId = "",
  deviceId = "",
  usePlaceholders = false
} = {}) {
  const normalizedOrigin = typeof origin === "number" ? origin : String(origin || "").trim();
  const normalizedDestination = typeof destination === "number" ? destination : String(destination || "").trim();
  return {
    data: {
      origin: normalizedOrigin,
      destination: normalizedDestination,
      from: String(from || "").trim() || JOURNEY_SEARCH_REQUEST_DATE,
      to: String(to || "").trim() || JOURNEY_SEARCH_REQUEST_DATE
    },
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    date: JOURNEY_SEARCH_REQUEST_DATE,
    language: JOURNEY_SEARCH_REQUEST_LANGUAGE
  };
}

function buildJourneySearchDateRange(dateValue) {
  const normalized = String(dateValue || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    return null;
  }
  return {
    from: `${normalized}T00:00:00.000Z`,
    to: `${normalized}T23:59:59.000Z`
  };
}

function normalizeApiPath(input, fallbackPath) {
  const fallback = String(fallbackPath || "").trim() || "/";
  const raw = String(input || "").trim();
  if (!raw) return fallback;
  if (/^https?:\/\//i.test(raw)) {
    try {
      const parsed = new URL(raw);
      return parsed.pathname || fallback;
    } catch (err) {
      return fallback;
    }
  }
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function buildMembershipUserLoginUrl(partnerUrl) {
  try {
    const parsed = new URL(String(partnerUrl || ""));
    const pathname = String(parsed.pathname || "/");
    const apiMatch = pathname.match(/^(.+?\/api\/?)/i);
    const apiPrefixRaw = apiMatch ? apiMatch[1] : "/api/";
    const apiPrefix = apiPrefixRaw.endsWith("/") ? apiPrefixRaw : `${apiPrefixRaw}/`;
    parsed.pathname = normalizeApiPath(`${apiPrefix}membership/userlogin`, "/api/membership/userlogin");
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildMembershipCreateUserUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/membership/createuser";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildMembershipGetUsersWithoutPermissionsUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Membership/GetUsersWithoutPermissions";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildMembershipDeleteUserUrl(baseUrl, clusterLabel = "") {
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

function buildObusPartnerRuleCreateUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Rule/CreatePartnerRule";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildObusPartnerRuleUpdateUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Rule/UpdatePartnerRule";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildAuthorizedLinesUploadUrl(baseUrl, clusterLabel) {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/uetds/UpdateValidRouteCodes";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildUetdsPricesUpdateUrl(baseUrl) {
  try {
    const parsed = new URL(String(baseUrl || ""));
    parsed.pathname = "/api/scheduledtask/TriggerScheduledTask";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildJourneyUpdateDailySummariesUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Inventory/GetDailyJourneySummaries";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildJourneyUpdateDetailUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Inventory/GetJourneyDetail";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildJourneyUpdateUpdateUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/Inventory/UpdateJourney";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
  }
}

function buildObusJobsUrl(baseUrl, clusterLabel = "") {
  const clusteredUrl = buildUrlForCluster(baseUrl, clusterLabel);
  try {
    const parsed = new URL(String(clusteredUrl || ""));
    parsed.pathname = "/api/scheduledtask/getscheduledtasks";
    parsed.search = "";
    parsed.hash = "";
    return parsed.toString();
  } catch (err) {
    return "";
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

function parseSelectedCompanyValuesFromInput(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  const raw = String(value || "").trim();
  if (!raw) return [];

  const parsed = parseJsonSafe(raw);
  if (Array.isArray(parsed)) {
    return parsed.map((item) => String(item || "").trim()).filter(Boolean);
  }

  return raw
    .split(",")
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function extractPartnerIdFromCompanyLabel(label) {
  const text = String(label || "").trim();
  if (!text) return "";

  // Expected format: "Code - 123 - clusterX"
  const strictMatch = text.match(/^\s*.+?\s*-\s*([0-9]+)\s*-\s*cluster\d+\s*$/i);
  if (strictMatch && strictMatch[1]) {
    return String(strictMatch[1]).trim();
  }

  // Fallback: first numeric token after a hyphen.
  const looseMatch = text.match(/-\s*([0-9]+)\b/);
  if (looseMatch && looseMatch[1]) {
    return String(looseMatch[1]).trim();
  }

  return "";
}

function resolveSelectedPartnerId({ selectedCompanyMeta, selectedCompanyValue, companies }) {
  const selectedOption =
    Array.isArray(companies) &&
    companies.find((item) => !item?.disabled && String(item?.value || "") === String(selectedCompanyValue || ""));
  const labelId = extractPartnerIdFromCompanyLabel(selectedOption?.label || "");
  if (labelId) return labelId;

  const parsedValue = parseCompanyOptionValue(selectedCompanyValue);
  const valueId = String(parsedValue?.id || "").trim();
  if (valueId) return valueId;

  const metaId = String(selectedCompanyMeta?.id || "").trim();
  if (metaId) return metaId;

  return "";
}

function normalizeAuthorizedLinesSubmitAction(value) {
  return String(value || "").trim().toLowerCase() === "uetds-prices" ? "uetds-prices" : "authorized-lines";
}

function buildAuthorizedLinesLoginRequestBodyPreview({
  partnerCode = "",
  username = "",
  loginBranchId = ""
} = {}) {
  const body = {
    data: {
      username: String(username || "").trim(),
      password: "***",
      "remember-me": 0,
      "partner-code": String(partnerCode || "").trim()
    },
    "device-session": {
      "session-id": "<session-id>",
      "device-id": "<device-id>"
    },
    date: "2020-02-24T18:03:00",
    language: "tr-TR"
  };
  const normalizedLoginBranchId = String(loginBranchId || "").trim();
  if (normalizedLoginBranchId) {
    body.data["branch-id"] = normalizedLoginBranchId;
  }
  return JSON.stringify(body, null, 2);
}

async function loadAuthorizedLinesCompanies() {
  const cacheResult = await fetchAllCompaniesRowsFromCache();
  const cacheRows = Array.isArray(cacheResult?.rows) ? cacheResult.rows : [];
  const partnerItems = normalizePartnerItems(
    cacheRows.map((row) => ({
      code: String(row?.code || "").trim(),
      id: String(row?.id || "").trim(),
      cluster: extractClusterLabel(String(row?.source || "").trim()),
      url: normalizeTargetUrl(row?.url || ""),
      branchId: String(row?.ObusMerkezSubeID || row?.obus_merkez_sube_id || "").trim(),
      isAbroad: parseAllCompaniesBooleanValue(row?.isabroad ?? row?.is_abroad)
    }))
  );

  const companies = [{ value: "", label: "Firma seçiniz" }].concat(
    partnerItems.map((item) => {
      const idText = item.id || "N/A";
      const clusterText = item.cluster || "cluster";
      const obusMerkezSubeId = String(item.branchId || "").trim();
      const label = `${item.code} - ${idText} - ${clusterText} - ObusMerkezSubeID: ${obusMerkezSubeId || "-"}`;
      const value = buildCompanyOptionValue(item);
      return {
        value,
        label,
        meta: item
      };
    })
  );

  let partnerError = cacheResult?.error || null;
  if (!partnerError && partnerItems.length === 0) {
    partnerError =
      "Firma listesi SQL'de boş. Önce Tüm Firmalar ekranında 'Servisten Güncelle' ve 'SQL'e Kaydet' çalıştırın.";
  }

  if (partnerError) {
    companies.push({
      value: "__partner_error__",
      label: `Hata: ${partnerError}`,
      disabled: true
    });
  }

  return {
    companies,
    partnerItems,
    partnerError
  };
}

async function loadJourneySearchCompanies() {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  const companies = [{ value: "", label: "Firma seçiniz" }];

  partnerItems.forEach((item) => {
    const value = buildCompanyOptionValue(item);
    const label = String(item?.code || "").trim();
    if (!value || !label) return;
    companies.push({
      value,
      label,
      meta: item
    });
  });

  if (partnerError) {
    companies.push({
      value: "__partner_error__",
      label: `Hata: ${partnerError}`,
      disabled: true
    });
  }

  return {
    companies,
    partnerError
  };
}

function buildJourneySearchStationsRequestBody({ sessionId = "", deviceId = "", usePlaceholders = false } = {}) {
  return {
    data: null,
    token: null,
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    date: JOURNEY_SEARCH_REQUEST_DATE,
    language: JOURNEY_SEARCH_REQUEST_LANGUAGE
  };
}

function collectJourneySearchStationArrays(node, collector, depth = 0) {
  if (depth > 6 || node === null || node === undefined) return;

  if (Array.isArray(node)) {
    if (node.some((item) => item && typeof item === "object" && !Array.isArray(item))) {
      collector.push(node);
    }
    node.forEach((item) => collectJourneySearchStationArrays(item, collector, depth + 1));
    return;
  }

  if (typeof node !== "object") return;
  Object.values(node).forEach((value) => collectJourneySearchStationArrays(value, collector, depth + 1));
}

function extractJourneySearchStations(payload) {
  const candidateLists = [];
  collectJourneySearchStationArrays(payload, candidateLists);

  const seen = new Set();
  const items = [];

  candidateLists.forEach((list) => {
    list.forEach((row) => {
      if (!row || typeof row !== "object" || Array.isArray(row)) return;

      const name = formatPartnerCellValue(
        readPartnerRawValueByAliases(row, [
          "name",
          "label",
          "station-name",
          "station_name",
          "stationname",
          "city-name",
          "city_name",
          "cityname",
          "description",
          "text",
          "title"
        ])
      ).trim();
      const id = formatPartnerCellValue(
        readPartnerRawValueByAliases(row, [
          "id",
          "station-id",
          "station_id",
          "stationid",
          "station-code",
          "station_code",
          "stationcode",
          "code",
          "value",
          "city-id",
          "city_id",
          "cityid"
        ])
      ).trim();

      if (!name || name.startsWith("{") || name.startsWith("[")) return;

      const normalizedValue = id || name;
      const key = `${normalizedValue.toLocaleLowerCase("tr")}|||${name.toLocaleLowerCase("tr")}`;
      if (seen.has(key)) return;
      seen.add(key);

      items.push({
        value: normalizedValue,
        label: name,
        id,
        name
      });
    });
  });

  return items.sort(
    (a, b) =>
      String(a.name || a.label || "").localeCompare(String(b.name || b.label || ""), "tr") ||
      String(a.id || a.value || "").localeCompare(String(b.id || b.value || ""), "tr")
  );
}

async function fetchJourneySearchStations({ company } = {}) {
  const partnerCode = String(company?.code || "").trim();
  const clusterLabel = extractClusterLabel(String(company?.cluster || company?.url || "").trim());
  const companyUrl = normalizeTargetUrl(company?.url || "");
  const baseClusterUrl = buildUrlForCluster(PARTNERS_API_URL, clusterLabel) || companyUrl;
  const sessionUrl = buildSessionUrlForPartnerUrl(baseClusterUrl);
  const requestUrl = buildJourneySearchStationsUrl(baseClusterUrl, clusterLabel);
  const companyRef = `${partnerCode || "code?"}${clusterLabel ? ` / ${clusterLabel}` : ""}`;
  const requestBodyTemplate = JSON.stringify(buildJourneySearchStationsRequestBody({ usePlaceholders: true }), null, 2);

  if (!partnerCode) {
    return {
      items: [],
      error: "Seçilen firma için PartnerCode bulunamadı.",
      status: null,
      requestUrl,
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  if (!requestUrl) {
    return {
      items: [],
      error: "Seçilen firma için cluster URL bulunamadı.",
      status: null,
      requestUrl: "",
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), JOURNEY_SEARCH_TIMEOUT_MS);

  try {
    const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, controller.signal);
    if (sessionResult.error) {
      return {
        items: [],
        error: `GetSession başarısız: ${sessionResult.error}`,
        status: null,
        requestUrl,
        step: "getsession",
        detail: buildObusServiceTraceText(sessionResult.debug, sessionResult.error, {
          bodyMaxLen: 120,
          responseMaxLen: 180
        }),
        requestBody: requestBodyTemplate,
        responseBody: String(sessionResult?.debug?.responseBody || sessionResult.error || "").trim()
      };
    }

    const body = buildJourneySearchStationsRequestBody({
      sessionId: sessionResult.sessionId,
      deviceId: sessionResult.deviceId
    });
    const requestBodyText = JSON.stringify(body, null, 2);

    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: JOURNEY_SEARCH_API_AUTH,
        PartnerCode: partnerCode
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBodyText =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();
    const getStationsTrace = buildObusServiceTraceEntry({
      service: "GetStations",
      url: requestUrl,
      status: response.status,
      requestBody: body,
      responseBody: parsed ?? raw,
      note: `partnerCode=${partnerCode}${clusterLabel ? `, cluster=${clusterLabel}` : ""}`
    });
    const hasExplicitStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    const responseErrorText =
      (parsed &&
        typeof parsed === "object" &&
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      return {
        items: [],
        error: `GetStations HTTP ${response.status}: ${responseErrorText}`,
        status: response.status,
        requestUrl,
        step: "getstations",
        detail: buildObusServiceTraceText(getStationsTrace, responseErrorText, {
          bodyMaxLen: 120,
          responseMaxLen: 220
        }),
        requestBody: requestBodyText,
        responseBody: responseBodyText || ""
      };
    }

    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      return {
        items: [],
        error: responseErrorText || "İstasyon servisi başarısız döndü.",
        status: response.status,
        requestUrl,
        step: "getstations",
        detail: buildObusServiceTraceText(getStationsTrace, responseErrorText, {
          bodyMaxLen: 120,
          responseMaxLen: 220
        }),
        requestBody: requestBodyText,
        responseBody: responseBodyText || ""
      };
    }

    const items = extractJourneySearchStations(parsed);
    if (items.length === 0) {
      return {
        items: [],
        error: "GetStations yanıtında istasyon bulunamadı.",
        status: response.status,
        requestUrl,
        step: "parse",
        detail: buildObusServiceTraceText(getStationsTrace, "İstasyon listesi boş veya beklenen formatta değil.", {
          bodyMaxLen: 120,
          responseMaxLen: 220
        }),
        requestBody: requestBodyText,
        responseBody: responseBodyText || ""
      };
    }

    return {
      items,
      error: null,
      status: response.status,
      requestUrl,
      step: "done",
      detail: `Firma=${companyRef} | istasyon=${items.length} | url=${truncateObusDebugText(requestUrl, 120)}`,
      requestBody: requestBodyText,
      responseBody: responseBodyText || ""
    };
  } catch (err) {
    const errorTrace = buildObusServiceTraceEntry({
      service: "GetStations",
      url: requestUrl,
      requestBody: {
        data: null,
        token: null,
        "device-session": {
          "session-id": "<runtime>",
          "device-id": "<runtime>"
        },
        date: JOURNEY_SEARCH_REQUEST_DATE,
        language: JOURNEY_SEARCH_REQUEST_LANGUAGE
      },
      responseBody: "",
      error: err?.message || "İstek gönderilemedi.",
      note: `partnerCode=${partnerCode}${clusterLabel ? `, cluster=${clusterLabel}` : ""}`
    });
    return {
      items: [],
      error: err?.name === "AbortError" ? "İstasyon listesi zaman aşımına uğradı." : err?.message || "İstek gönderilemedi.",
      status: null,
      requestUrl,
      step: "exception",
      detail: buildObusServiceTraceText(errorTrace, err?.message || "İstek gönderilemedi.", {
        bodyMaxLen: 120,
        responseMaxLen: 220
      }),
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJourneySearchJourneys({
  company,
  originId = "",
  destinationId = "",
  dateRange = {}
} = {}) {
  const partnerCode = String(company?.code || "").trim();
  const clusterLabel = extractClusterLabel(String(company?.cluster || company?.url || "").trim());
  const companyUrl = normalizeTargetUrl(company?.url || "");
  const baseClusterUrl = buildUrlForCluster(PARTNERS_API_URL, clusterLabel) || companyUrl;
  const sessionUrl = buildSessionUrlForPartnerUrl(baseClusterUrl);
  const requestUrl = buildJourneySearchGetJourneysUrl(baseClusterUrl, clusterLabel);
  const companyRef = `${partnerCode || "code?"}${clusterLabel ? ` / ${clusterLabel}` : ""}`;
  const requestBodyTemplate = JSON.stringify(
    buildJourneySearchJourneysRequestBody({
      origin: originId,
      destination: destinationId,
      from: dateRange.from,
      to: dateRange.to,
      usePlaceholders: true
    }),
    null,
    2
  );

  if (!partnerCode) {
    return {
      error: "Seçilen firma için PartnerCode bulunamadı.",
      status: null,
      requestUrl,
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  if (!originId || !destinationId) {
    return {
      error: "Kalkış ve varış istasyonlarını belirtmelisiniz.",
      status: null,
      requestUrl,
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  if (!dateRange?.from || !dateRange?.to) {
    return {
      error: "Geçerli bir tarih seçmelisiniz.",
      status: null,
      requestUrl,
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  if (!requestUrl) {
    return {
      error: "Seçilen firma için cluster URL bulunamadı.",
      status: null,
      requestUrl: "",
      step: "validation",
      detail: `Firma=${companyRef}`,
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), JOURNEY_SEARCH_TIMEOUT_MS);

  try {
    const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, controller.signal);
    if (sessionResult.error) {
      return {
        error: `GetSession başarısız: ${sessionResult.error}`,
        status: null,
        requestUrl,
        step: "getsession",
        detail: buildObusServiceTraceText(sessionResult.debug, sessionResult.error, {
          bodyMaxLen: 120,
          responseMaxLen: 180
        }),
        requestBody: requestBodyTemplate,
        responseBody: String(sessionResult?.debug?.responseBody || sessionResult.error || "").trim()
      };
    }

    const body = buildJourneySearchJourneysRequestBody({
      origin: originId,
      destination: destinationId,
      from: dateRange.from,
      to: dateRange.to,
      sessionId: sessionResult.sessionId,
      deviceId: sessionResult.deviceId
    });
    const requestBodyText = JSON.stringify(body, null, 2);

    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: JOURNEY_SEARCH_API_AUTH,
        PartnerCode: partnerCode
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBodyText =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();
    const traceEntry = buildObusServiceTraceEntry({
      service: "GetJourneys",
      url: requestUrl,
      status: response.status,
      requestBody: body,
      responseBody: parsed ?? raw,
      note: `partnerCode=${partnerCode}${clusterLabel ? `, cluster=${clusterLabel}` : ""}`
    });
    const responseErrorText =
      (parsed && typeof parsed === "object" && String(parsed?.message || parsed?.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      return {
        error: `GetJourneys HTTP ${response.status}: ${responseErrorText}`,
        status: response.status,
        requestUrl,
        step: "getjourneys",
        detail: buildObusServiceTraceText(traceEntry, responseErrorText, {
          bodyMaxLen: 120,
          responseMaxLen: 220
        }),
        requestBody: requestBodyText,
        responseBody: responseBodyText || ""
      };
    }

    return {
      ok: true,
      status: response.status,
      requestUrl,
      step: "done",
      detail: buildObusServiceTraceText(traceEntry, `Firma=${companyRef}`, {
        bodyMaxLen: 120,
        responseMaxLen: 220
      }),
      requestBody: requestBodyText,
      responseBody: responseBodyText || ""
    };
  } catch (err) {
    const errorTrace = buildObusServiceTraceEntry({
      service: "GetJourneys",
      url: requestUrl,
      requestBody: buildJourneySearchJourneysRequestBody({
        origin: originId,
        destination: destinationId,
        from: dateRange?.from,
        to: dateRange?.to,
        sessionId: "<runtime>",
        deviceId: "<runtime>"
      }),
      responseBody: "",
      error: err?.message || "İstek gönderilemedi.",
      note: `partnerCode=${partnerCode}${clusterLabel ? `, cluster=${clusterLabel}` : ""}`
    });
    return {
      error: err?.name === "AbortError" ? "GetJourneys isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi.",
      status: null,
      requestUrl,
      step: "exception",
      detail: buildObusServiceTraceText(errorTrace, err?.message || "İstek gönderilemedi.", {
        bodyMaxLen: 120,
        responseMaxLen: 220
      }),
      requestBody: requestBodyTemplate,
      responseBody: ""
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function resolveAuthorizedLinesLoginResultWithBranchFallback({
  endpointUrl,
  companyUrl,
  partnerCode,
  partnerId,
  username,
  password,
  fallbackBranchId,
  sessionClusterLabel = "",
  authorization = PARTNERS_API_AUTH,
  timeoutMs = 90000,
  sessionCache = null
}) {
  const initialResult = await fetchAuthorizedLinesLoginInfo({
    endpointUrl,
    companyUrl,
    partnerCode,
    partnerId,
    username,
    password,
    fallbackBranchId,
    sessionClusterLabel,
    authorization,
    timeoutMs,
    sessionCache
  });
  const initialToken = String(initialResult?.token || "").trim();
  const retryBranchCandidates = buildUniqueLoginBranchCandidates(
    initialResult?.obusMerkezBranchKey,
    initialResult?.branchId,
    fallbackBranchId,
    partnerId
  );

  if (!(initialResult?.ok === true && !initialToken && retryBranchCandidates.length > 0)) {
    return initialResult;
  }

  const retryResults = [];
  for (const branchCandidate of retryBranchCandidates) {
    const retryResult = await fetchAuthorizedLinesLoginInfo({
      endpointUrl,
      companyUrl,
      partnerCode,
      partnerId,
      username,
      password,
      fallbackBranchId,
      loginBranchId: branchCandidate,
      sessionClusterLabel,
      authorization,
      timeoutMs,
      sessionCache
    });
    retryResults.push(retryResult);
    const retryToken = String(retryResult?.token || "").trim();
    if (retryResult?.ok === true && retryToken) {
      return retryResult;
    }
  }

  const mergedDetail = [
    String(initialResult?.tokenMissingDetail || initialResult?.errorDetail || "").trim(),
    ...retryResults.map((item) => String(item?.tokenMissingDetail || item?.errorDetail || "").trim())
  ]
    .filter(Boolean)
    .join(" | ");
  const finalRetryResult = retryResults[retryResults.length - 1] || null;

  return {
    ok: false,
    error: String(finalRetryResult?.error || "").trim() || "UserLogin branch seçimi sonrası token alınamadı.",
    errorDetail: mergedDetail,
    sessionId: String(finalRetryResult?.sessionId || initialResult?.sessionId || "").trim(),
    deviceId: String(finalRetryResult?.deviceId || initialResult?.deviceId || "").trim(),
    branchId: String(finalRetryResult?.branchId || initialResult?.branchId || retryBranchCandidates[0] || "").trim(),
    token: String(finalRetryResult?.token || "").trim(),
    obusMerkezBranchKey: String(finalRetryResult?.obusMerkezBranchKey || initialResult?.obusMerkezBranchKey || "").trim(),
    tokenMissingDetail: mergedDetail,
    rawLoginBody: String(finalRetryResult?.rawLoginBody || initialResult?.rawLoginBody || "").trim(),
    loginUrl: String(finalRetryResult?.loginUrl || initialResult?.loginUrl || "").trim(),
    serviceLogs: [
      ...(Array.isArray(initialResult?.serviceLogs) ? initialResult.serviceLogs : []),
      ...retryResults.flatMap((item) => (Array.isArray(item?.serviceLogs) ? item.serviceLogs : []))
    ],
    failedServiceLog:
      finalRetryResult?.failedServiceLog ||
      retryResults.find((item) => item?.failedServiceLog)?.failedServiceLog ||
      initialResult?.failedServiceLog ||
      null
  };
}

function normalizeStationPassengerPlate(value) {
  return String(value || "")
    .toLocaleUpperCase("tr")
    .replace(/[^A-Z0-9]/g, "");
}

function formatStationPassengerPlateDisplay(value) {
  return String(value || "")
    .toLocaleUpperCase("tr")
    .replace(/[^A-Z0-9]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildStationPassengerRequestDateParts(date = new Date()) {
  const isoDate =
    formatDateToIsoInTimeZone(date, STATION_PASSENGER_INFO_TIME_ZONE) || formatDateToIsoLocal(date);
  return {
    isoDate,
    obusDate: isoDate ? `${isoDate} 00:00:00` : ""
  };
}

function formatDateTimeToSecondPrecisionInTimeZone(
  date = new Date(),
  timeZone = STATION_PASSENGER_INFO_TIME_ZONE
) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: String(timeZone || "").trim() || STATION_PASSENGER_INFO_TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23"
    }).formatToParts(date);
    const year = String(parts.find((part) => part.type === "year")?.value || "").trim();
    const month = String(parts.find((part) => part.type === "month")?.value || "").trim();
    const day = String(parts.find((part) => part.type === "day")?.value || "").trim();
    const hour = String(parts.find((part) => part.type === "hour")?.value || "").trim();
    const minute = String(parts.find((part) => part.type === "minute")?.value || "").trim();
    const second = String(parts.find((part) => part.type === "second")?.value || "").trim();
    if (year && month && day && hour && minute && second) {
      return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
    }
  } catch (err) {
    // Ignore and fallback to local formatting.
  }
  const isoDate = formatDateToIsoLocal(date);
  if (!isoDate) return "";
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  const second = String(date.getSeconds()).padStart(2, "0");
  return `${isoDate} ${hour}:${minute}:${second}`;
}

function buildStationPassengerDailySummariesRequestBody({
  sessionId = "",
  deviceId = "",
  token = "",
  dateValue = "",
  includeDateField = true
} = {}) {
  const normalizedDateValue = String(dateValue || "").trim();
  const payload = {
    data: normalizedDateValue,
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE,
    token: String(token || "").trim()
  };
  if (includeDateField) {
    payload.date = normalizedDateValue ? `${normalizedDateValue} 00:00:00` : "";
  }
  return payload;
}

function normalizeStationPassengerJourneyIdForRequest(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (/^-?\d+$/.test(text)) {
    const parsed = Number.parseInt(text, 10);
    if (Number.isSafeInteger(parsed)) return parsed;
  }
  return text;
}

function buildStationPassengerJourneyStationsRequestBody({
  journeyId = "",
  sessionId = "",
  deviceId = "",
  token = "",
  dateValue = ""
} = {}) {
  return {
    data: normalizeStationPassengerJourneyIdForRequest(journeyId),
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    token: String(token || "").trim(),
    date: String(dateValue || "").trim(),
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE
  };
}

function buildJourneyUpdateDetailRequestBody({
  journeyId = "",
  sessionId = "",
  deviceId = "",
  token = "",
  dateValue = ""
} = {}) {
  const normalizedDateValue = String(dateValue || "").trim();
  return {
    data: normalizeStationPassengerJourneyIdForRequest(journeyId),
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE,
    token: String(token || "").trim(),
    date: normalizedDateValue ? `${normalizedDateValue} 00:00:00` : ""
  };
}

function buildJourneyUpdateUpdateRequestBody({
  journeyId = "",
  parameters = [],
  dataPayload = null,
  sessionId = "",
  deviceId = "",
  token = "",
  dateValue = "",
  usePlaceholders = false
} = {}) {
  const normalizedParameters = normalizeJourneyUpdateUpdateParameters(parameters);
  const normalizedDateValue = String(dateValue || "").trim() || buildJourneyUpdateUpdateRequestDate();
  const clonedDataPayload = cloneJsonCompatibleValue(dataPayload);
  const normalizedDataPayload =
    clonedDataPayload && typeof clonedDataPayload === "object" && !Array.isArray(clonedDataPayload)
      ? clonedDataPayload
      : {
          id: normalizeStationPassengerJourneyIdForRequest(journeyId),
          parameters: normalizedParameters
        };

  if (normalizedDataPayload.id === undefined || normalizedDataPayload.id === null || String(normalizedDataPayload.id).trim() === "") {
    normalizedDataPayload.id = normalizeStationPassengerJourneyIdForRequest(journeyId);
  }
  if (!Array.isArray(normalizedDataPayload.parameters)) {
    normalizedDataPayload.parameters = normalizedParameters;
  }

  return {
    data: normalizedDataPayload,
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    token: usePlaceholders ? "{{token}}" : String(token || "").trim(),
    date: normalizedDateValue,
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE
  };
}

function formatDateTimeToOffsetPrecisionInTimeZone(
  date = new Date(),
  timeZone = STATION_PASSENGER_INFO_TIME_ZONE,
  offset = "+03:00"
) {
  const base = formatDateTimeToSecondPrecisionInTimeZone(date, timeZone);
  if (!base) return JOURNEY_SEARCH_REQUEST_DATE;
  return `${base.replace(" ", "T")}.0000000${String(offset || "+03:00").trim() || "+03:00"}`;
}

function buildStationPassengerWebStationsRequestBody({
  sessionId = "",
  deviceId = "",
  dateValue = ""
} = {}) {
  return {
    data: null,
    token: null,
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    date: String(dateValue || "").trim() || JOURNEY_SEARCH_REQUEST_DATE,
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE
  };
}

function buildStationPassengerPassengerStateHistoryRequestBody({
  journeyId = "",
  sessionId = "",
  deviceId = "",
  token = "",
  dateValue = ""
} = {}) {
  return {
    data: {
      "journey-id": normalizeStationPassengerJourneyIdForRequest(journeyId),
      "seat-number": null
    },
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    token: String(token || "").trim(),
    date: String(dateValue || "").trim(),
    language: STATION_PASSENGER_INFO_REQUEST_LANGUAGE
  };
}

function buildStationPassengerAuthContext({
  endpointUrl = "",
  companyCode = "",
  companyId = "",
  companyUrl = "",
  cluster = "",
  loginResult = null
} = {}) {
  return {
    endpointUrl: String(endpointUrl || "").trim(),
    companyCode: String(companyCode || "").trim(),
    companyId: String(companyId || "").trim(),
    companyUrl: String(companyUrl || "").trim(),
    cluster: extractClusterLabel(cluster),
    sessionId: String(loginResult?.sessionId || "").trim(),
    deviceId: String(loginResult?.deviceId || "").trim(),
    token: String(loginResult?.token || "").trim(),
    savedAt: Date.now()
  };
}

function normalizeStationPassengerAuthContext(value) {
  if (!value || typeof value !== "object") return null;
  const sessionId = String(value.sessionId || "").trim();
  const deviceId = String(value.deviceId || "").trim();
  const token = String(value.token || "").trim();
  if (!sessionId || !deviceId || !token) return null;

  const savedAt = Number.parseInt(String(value.savedAt || ""), 10) || Date.now();
  return {
    endpointUrl: String(value.endpointUrl || "").trim(),
    companyCode: String(value.companyCode || "").trim(),
    companyId: String(value.companyId || "").trim(),
    companyUrl: String(value.companyUrl || "").trim(),
    cluster: extractClusterLabel(value.cluster || value.endpointUrl || value.companyUrl || ""),
    sessionId,
    deviceId,
    token,
    savedAt
  };
}

function getStationPassengerAuthContextFromSession(req) {
  const authContext = normalizeStationPassengerAuthContext(req?.session?.stationPassengerInfoAuth);
  if (!authContext) return null;
  if (authContext.savedAt + Math.max(60000, STATION_PASSENGER_INFO_AUTH_CACHE_TTL_MS) <= Date.now()) {
    if (req?.session?.stationPassengerInfoAuth) {
      delete req.session.stationPassengerInfoAuth;
    }
    return null;
  }
  return authContext;
}

function saveStationPassengerAuthContextToSession(req, authContext) {
  const normalized = normalizeStationPassengerAuthContext(authContext);
  if (!req?.session) return;
  if (!normalized) {
    delete req.session.stationPassengerInfoAuth;
    return;
  }
  req.session.stationPassengerInfoAuth = normalized;
}

function clearStationPassengerAuthContextFromSession(req) {
  if (req?.session?.stationPassengerInfoAuth) {
    delete req.session.stationPassengerInfoAuth;
  }
}

async function resolveStationPassengerTargetCandidate({ endpointUrl = "", clusterLabel = "" } = {}) {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  const targetCode = STATION_PASSENGER_INFO_TARGET_COMPANY_CODE;
  const targetId = STATION_PASSENGER_INFO_TARGET_COMPANY_ID;
  const normalizedClusterLabel = extractClusterLabel(clusterLabel || endpointUrl);
  const targetCandidateFromCache = (Array.isArray(partnerItems) ? partnerItems : []).find((item) => {
    const itemCode = String(item?.code || "").trim().toLocaleLowerCase("tr");
    const itemId = String(item?.id || "").trim();
    const itemCluster = String(item?.cluster || "").trim().toLocaleLowerCase("tr");
    return (
      itemCode === targetCode.toLocaleLowerCase("tr") &&
      itemId === targetId &&
      (!itemCluster || itemCluster === normalizedClusterLabel)
    );
  });
  const baseCandidate =
    targetCandidateFromCache || {
      code: targetCode,
      id: targetId,
      cluster: normalizedClusterLabel,
      url: endpointUrl,
      branchId: ""
    };

  let resolvedBranchId = String(baseCandidate?.branchId || "").trim();
  let branchLookupError = "";
  if (!resolvedBranchId && normalizedClusterLabel && targetCode && targetId) {
    const branchLookupResult = await fetchObusMerkezBranchMapForTarget({
      clusterLabel: normalizedClusterLabel,
      partnerCode: targetCode,
      fallbackPartnerId: targetId
    });
    branchLookupError = String(branchLookupResult?.error || "").trim();
    resolvedBranchId =
      String(branchLookupResult?.map?.get?.(targetId) || "").trim() ||
      String(
        (Array.isArray(branchLookupResult?.rows)
          ? branchLookupResult.rows.find((item) => String(item?.partnerId || "").trim() === targetId)
          : null)?.branchId || ""
      ).trim();
  }

  return {
    candidate: {
      ...baseCandidate,
      branchId: resolvedBranchId || String(baseCandidate?.branchId || "").trim() || targetId
    },
    fromCache: Boolean(targetCandidateFromCache),
    partnerError: [String(partnerError || "").trim(), branchLookupError].filter(Boolean).join(" | ")
  };
}

function extractStationPassengerTextValue(value) {
  const text = formatPartnerCellValue(value).trim();
  if (!text || /^(null|undefined)$/i.test(text)) return "";
  if ((text.startsWith("{") && text.endsWith("}")) || (text.startsWith("[") && text.endsWith("]"))) {
    return "";
  }
  return text;
}

function readStationPassengerTextByAliases(node, aliases = [], maxDepth = 3) {
  if (!node || typeof node !== "object") return "";
  const direct = extractStationPassengerTextValue(readPartnerRawValueByAliases(node, aliases));
  if (direct) return direct;
  return extractStationPassengerTextValue(getDeepValueByNormalizedKey(node, aliases, maxDepth));
}

function collectStationPassengerTextValuesByMatcher(node, matcher, maxDepth = 3, collected = [], visited = new Set()) {
  if (maxDepth < 0 || node === null || node === undefined) return collected;
  if (typeof node === "string") {
    const normalizedText = extractStationPassengerTextValue(node);
    if (normalizedText) collected.push(normalizedText);
    return collected;
  }
  if (typeof node !== "object") return collected;
  if (visited.has(node)) return collected;
  visited.add(node);

  if (Array.isArray(node)) {
    node.forEach((item) => collectStationPassengerTextValuesByMatcher(item, matcher, maxDepth - 1, collected, visited));
    return collected;
  }

  Object.entries(node).forEach(([key, value]) => {
    const normalizedKey = normalizeTokenName(key);
    if (matcher(normalizedKey, key, value)) {
      const textValue = extractStationPassengerTextValue(value);
      if (textValue) collected.push(textValue);
    }
    collectStationPassengerTextValuesByMatcher(value, matcher, maxDepth - 1, collected, visited);
  });

  return collected;
}

function collectStationPassengerPlateCandidates(node) {
  const values = collectStationPassengerTextValuesByMatcher(
    node,
    (normalizedKey) =>
      normalizedKey.includes("plate") ||
      normalizedKey.includes("plaka") ||
      normalizedKey.includes("plateno") ||
      normalizedKey.includes("plakano"),
    4
  );
  const deduped = [];
  values.forEach((value) => {
    const normalized = normalizeStationPassengerPlate(value);
    if (!normalized) return;
    if (deduped.some((item) => normalizeStationPassengerPlate(item) === normalized)) return;
    deduped.push(String(value || "").trim());
  });
  return deduped;
}

function formatStationPassengerDepartureTime(value) {
  const text = extractStationPassengerTextValue(value);
  if (!text) return "";
  const dateMatch = text.match(/(?:T|\s)(\d{2}:\d{2})(?::\d{2})?/);
  if (dateMatch?.[1]) return dateMatch[1];
  const plainMatch = text.match(/\b(\d{2}:\d{2})(?::\d{2})?\b/);
  if (plainMatch?.[1]) return plainMatch[1];
  return text;
}

function extractStationPassengerDepartureTime(node) {
  if (!node || typeof node !== "object") return "";
  const primaryAliases = [
    "departure-time",
    "departure_time",
    "departuretime",
    "departure-hour",
    "departure_hour",
    "departurehour",
    "journey-time",
    "journey_time",
    "journeytime",
    "start-time",
    "start_time",
    "starttime",
    "time",
    "hour"
  ];
  const primaryValue = readPartnerRawValueByAliases(node, primaryAliases) || getDeepValueByNormalizedKey(node, primaryAliases, 3);
  const primaryTime = formatStationPassengerDepartureTime(primaryValue);
  if (primaryTime) return primaryTime;

  return formatStationPassengerDepartureTime(
    getDeepValueByNormalizedKey(
      node,
      [
        "departure-date-time",
        "departure_date_time",
        "departuredatetime",
        "departure-date",
        "departure_date",
        "departuredate",
        "start-date-time",
        "start_date_time",
        "startdatetime"
      ],
      3
    )
  );
}

function buildStationPassengerRouteInfoFromRouteValue(routeValue) {
  if (Array.isArray(routeValue)) {
    const stopNames = routeValue
      .map((item) =>
        readStationPassengerTextByAliases(
          item,
          [
            "name",
            "label",
            "station-name",
            "station_name",
            "stationname",
            "location-name",
            "location_name",
            "locationname",
            "city-name",
            "city_name",
            "cityname"
          ],
          1
        )
      )
      .filter(Boolean);
    if (stopNames.length >= 2) {
      return `${stopNames[0]} -> ${stopNames[stopNames.length - 1]}`;
    }
    if (stopNames.length === 1) {
      return stopNames[0];
    }
    return "";
  }

  if (routeValue && typeof routeValue === "object") {
    const origin = readStationPassengerTextByAliases(
      routeValue,
      [
        "origin-name",
        "origin_name",
        "originname",
        "from-name",
        "from_name",
        "fromname",
        "departure-station-name",
        "departure_station_name",
        "departurestationname",
        "origin",
        "from"
      ],
      2
    );
    const destination = readStationPassengerTextByAliases(
      routeValue,
      [
        "destination-name",
        "destination_name",
        "destinationname",
        "to-name",
        "to_name",
        "toname",
        "arrival-station-name",
        "arrival_station_name",
        "arrivalstationname",
        "destination",
        "to"
      ],
      2
    );
    if (origin && destination) return `${origin} -> ${destination}`;
    if (origin || destination) return origin || destination;
  }

  return extractStationPassengerTextValue(routeValue);
}

function extractStationPassengerRouteInfo(node) {
  if (!node || typeof node !== "object") return "";

  const directRoute = readStationPassengerTextByAliases(
    node,
    [
      "route-info",
      "route_info",
      "routeinfo",
      "route-name",
      "route_name",
      "routename",
      "line-name",
      "line_name",
      "linename",
      "line-info",
      "line_info",
      "lineinfo"
    ],
    3
  );
  if (directRoute) return directRoute;

  const origin = readStationPassengerTextByAliases(
    node,
    [
      "origin-name",
      "origin_name",
      "originname",
      "from-name",
      "from_name",
      "fromname",
      "departure-station-name",
      "departure_station_name",
      "departurestationname",
      "origin",
      "from"
    ],
    3
  );
  const destination = readStationPassengerTextByAliases(
    node,
    [
      "destination-name",
      "destination_name",
      "destinationname",
      "to-name",
      "to_name",
      "toname",
      "arrival-station-name",
      "arrival_station_name",
      "arrivalstationname",
      "destination",
      "to"
    ],
    3
  );
  if (origin && destination) return `${origin} -> ${destination}`;
  if (origin || destination) return origin || destination;

  const routeValue = getDeepValueByNormalizedKey(node, ["route", "routes", "journey-route", "journeyroute"], 3);
  return buildStationPassengerRouteInfoFromRouteValue(routeValue);
}

function extractStationPassengerJourneyId(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "journey-id",
      "journey_id",
      "journeyid",
      "trip-id",
      "trip_id",
      "tripid",
      "sefer-id",
      "sefer_id",
      "seferid",
      "id"
    ],
    2
  );
}

function parseStationPassengerJourneyStationOrder(value) {
  const text = String(value || "").trim();
  if (!/^-?\d+$/.test(text)) return null;
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function extractStationPassengerJourneyStationOrder(node) {
  if (!node || typeof node !== "object") return null;
  return parseStationPassengerJourneyStationOrder(
    readStationPassengerTextByAliases(
      node,
      [
        "order",
        "sequence",
        "sort-order",
        "sort_order",
        "sortorder"
      ],
      2
    )
  );
}

function extractStationPassengerJourneyStationId(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "station-id",
      "station_id",
      "stationid",
      "station-key",
      "station_key",
      "stationkey"
    ],
    2
  );
}

function extractStationPassengerJourneyStationDepartureTime(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "departure-time",
      "departure_time",
      "departuretime",
      "time",
      "date"
    ],
    2
  );
}

function extractStationPassengerJourneyStationName(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "station-name",
      "station_name",
      "stationname",
      "stop-name",
      "stop_name",
      "stopname",
      "station",
      "name",
      "label",
      "title",
      "location-name",
      "location_name",
      "locationname",
      "city-name",
      "city_name",
      "cityname"
    ],
    2
  );
}

function getStationPassengerJourneyStationRootPayload(payload) {
  if (typeof payload === "string") {
    const trimmed = payload.trim();
    if (!trimmed) return null;
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      return parseJsonSafe(trimmed);
    }
    return null;
  }

  if (!payload || typeof payload !== "object") return null;
  if (!Array.isArray(payload) && Object.prototype.hasOwnProperty.call(payload, "data")) {
    return payload.data;
  }
  return payload;
}

function collectStationPassengerJourneyStationBlocks(payload) {
  const rootPayload = getStationPassengerJourneyStationRootPayload(payload);
  if (!rootPayload) return [];

  if (Array.isArray(rootPayload)) {
    return rootPayload.filter((item) => item && typeof item === "object" && !Array.isArray(item));
  }

  if (typeof rootPayload !== "object") return [];

  const directBlocks = Object.entries(rootPayload)
    .filter(([key, value]) => {
      const normalizedKey = normalizeTokenName(key);
      return normalizedKey.startsWith("id") && value && typeof value === "object" && !Array.isArray(value);
    })
    .map(([, value]) => value);

  if (directBlocks.length > 0) return directBlocks;

  const nestedData = getDeepValueByNormalizedKey(rootPayload, ["data"], 2);
  if (nestedData && nestedData !== rootPayload) {
    return collectStationPassengerJourneyStationBlocks(nestedData);
  }

  return [];
}

function extractStationPassengerJourneyStations(payload, { journeyId = "" } = {}) {
  const collected = [];
  const seen = new Map();
  const visited = new Set();

  const pushCandidate = (node) => {
    const itemJourneyId = extractStationPassengerJourneyId(node) || String(journeyId || "").trim();
    const order = extractStationPassengerJourneyStationOrder(node);
    const stationId = extractStationPassengerJourneyStationId(node);
    const stationName = extractStationPassengerJourneyStationName(node);
    const departureTime = extractStationPassengerJourneyStationDepartureTime(node);
    const hasUsefulData =
      Number.isFinite(order) ||
      Boolean(String(stationName || "").trim()) ||
      Boolean(String(stationId || "").trim()) ||
      Boolean(String(departureTime || "").trim());

    if (!hasUsefulData) return;

    const key = [
      String(itemJourneyId || "").trim(),
      Number.isFinite(order) ? String(order) : "",
      String(stationId || "").trim(),
      String(departureTime || "").trim()
    ]
      .join("|||")
      .toLocaleLowerCase("tr");

    const candidate = {
      journeyId: String(itemJourneyId || "").trim(),
      tripId: String(itemJourneyId || "").trim(),
      seferId: String(itemJourneyId || "").trim(),
      "journey-id": String(itemJourneyId || "").trim(),
      order: Number.isFinite(order) ? order : null,
      stationName: String(stationName || "").trim(),
      "station-name": String(stationName || "").trim(),
      stationId: String(stationId || "").trim(),
      "station-id": String(stationId || "").trim(),
      departureTime: String(departureTime || "").trim(),
      "departure-time": String(departureTime || "").trim(),
      raw: node
    };

    const existingIndex = seen.get(key);
    if (Number.isInteger(existingIndex) && existingIndex >= 0 && collected[existingIndex]) {
      const existingItem = collected[existingIndex];
      collected[existingIndex] = {
        ...existingItem,
        order: existingItem.order ?? candidate.order,
        stationName: existingItem.stationName || candidate.stationName,
        "station-name": existingItem["station-name"] || candidate["station-name"],
        stationId: existingItem.stationId || candidate.stationId,
        "station-id": existingItem["station-id"] || candidate["station-id"],
        departureTime: existingItem.departureTime || candidate.departureTime,
        "departure-time": existingItem["departure-time"] || candidate["departure-time"],
        raw: existingItem.raw || candidate.raw
      };
      return;
    }

    seen.set(key, collected.length);
    collected.push(candidate);
  };

  const directBlocks = collectStationPassengerJourneyStationBlocks(payload);
  if (directBlocks.length > 0) {
    directBlocks.forEach((block) => {
      pushCandidate(block);
    });
    return collected.sort((a, b) => {
      const orderA = Number.isFinite(a.order) ? a.order : Number.MAX_SAFE_INTEGER;
      const orderB = Number.isFinite(b.order) ? b.order : Number.MAX_SAFE_INTEGER;
      if (orderA !== orderB) return orderA - orderB;
      const byStationId = String(a.stationId || "").localeCompare(String(b.stationId || ""), "tr");
      if (byStationId !== 0) return byStationId;
      return String(a.departureTime || "").localeCompare(String(b.departureTime || ""), "tr");
    });
  }

  const walk = (node, depth = 0) => {
    if (depth > 7 || node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) walk(parsed, depth + 1);
      }
      return;
    }
    if (typeof node !== "object") return;
    if (visited.has(node)) return;
    visited.add(node);

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1));
      return;
    }

    pushCandidate(node);
    Object.values(node).forEach((value) => walk(value, depth + 1));
  };

  walk(payload);

  return collected.sort((a, b) => {
    const orderA = Number.isFinite(a.order) ? a.order : Number.MAX_SAFE_INTEGER;
    const orderB = Number.isFinite(b.order) ? b.order : Number.MAX_SAFE_INTEGER;
    if (orderA !== orderB) return orderA - orderB;
    const byStationId = String(a.stationId || "").localeCompare(String(b.stationId || ""), "tr");
    if (byStationId !== 0) return byStationId;
    return String(a.departureTime || "").localeCompare(String(b.departureTime || ""), "tr");
  });
}

function extractStationPassengerPassengerName(node) {
  if (!node || typeof node !== "object") return "";

  const fullName = readStationPassengerTextByAliases(
    node,
    [
      "full-name",
      "full_name",
      "fullname",
      "passenger-name",
      "passenger_name",
      "passengername",
      "customer-name",
      "customer_name",
      "customername",
      "name-surname",
      "name_surname",
      "namesurname",
      "display-name",
      "display_name",
      "displayname"
    ],
    1
  );
  if (fullName) return fullName;

  const firstName = readStationPassengerTextByAliases(
    node,
    [
      "first-name",
      "first_name",
      "firstname",
      "name"
    ],
    1
  );
  const lastName = readStationPassengerTextByAliases(
    node,
    [
      "last-name",
      "last_name",
      "lastname",
      "surname",
      "family-name",
      "family_name",
      "familyname"
    ],
    1
  );

  return [firstName, lastName].filter(Boolean).join(" ").trim();
}

function extractStationPassengerPassengerSeatNumber(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "seat-number",
      "seat_number",
      "seatnumber",
      "seat-no",
      "seat_no",
      "seatno",
      "seat"
    ],
    1
  );
}

function extractStationPassengerPassengerOriginId(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "origin-id",
      "origin_id",
      "originid",
      "from-id",
      "from_id",
      "fromid",
      "departure-station-id",
      "departure_station_id",
      "departurestationid"
    ],
    1
  );
}

function extractStationPassengerPassengerDestinationId(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "destination-id",
      "destination_id",
      "destinationid",
      "to-id",
      "to_id",
      "toid",
      "arrival-station-id",
      "arrival_station_id",
      "arrivalstationid"
    ],
    1
  );
}

function extractStationPassengerPassengerTicketNumber(node) {
  if (!node || typeof node !== "object") return "";
  return readStationPassengerTextByAliases(
    node,
    [
      "ticket-number",
      "ticket_number",
      "ticketnumber",
      "ticket-no",
      "ticket_no",
      "ticketno",
      "pnr",
      "reservation-number",
      "reservation_number",
      "reservationnumber"
    ],
    1
  );
}

function buildStationPassengerPassengerDisplayLabel({
  passengerName = "",
  seatNumber = "",
  ticketNumber = ""
} = {}) {
  const parts = [];
  const normalizedName = String(passengerName || "").trim();
  const normalizedSeatNumber = String(seatNumber || "").trim();
  const normalizedTicketNumber = String(ticketNumber || "").trim();

  parts.push(normalizedName || (normalizedTicketNumber ? `Bilet ${normalizedTicketNumber}` : "Yolcu"));
  if (normalizedSeatNumber) {
    parts.push(`Koltuk ${normalizedSeatNumber}`);
  }

  return parts.join(" • ").trim();
}

function parseStationPassengerSeatSortValue(value) {
  const text = String(value || "").trim();
  if (!/^\d+$/.test(text)) return Number.MAX_SAFE_INTEGER;
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? parsed : Number.MAX_SAFE_INTEGER;
}

function extractStationPassengerPassengerStateItems(payload, { journeyId = "" } = {}) {
  const rootPayload =
    payload && typeof payload === "object" && !Array.isArray(payload) && Object.prototype.hasOwnProperty.call(payload, "data")
      ? payload.data
      : payload;
  const collected = [];
  const seen = new Set();
  const visited = new Set();

  const pushCandidate = (node) => {
    if (!node || typeof node !== "object" || Array.isArray(node)) return;

    const originId = extractStationPassengerPassengerOriginId(node);
    const destinationId = extractStationPassengerPassengerDestinationId(node);
    const seatNumber = extractStationPassengerPassengerSeatNumber(node);
    const passengerName = extractStationPassengerPassengerName(node);
    const ticketNumber = extractStationPassengerPassengerTicketNumber(node);
    const itemJourneyId = extractStationPassengerJourneyId(node) || String(journeyId || "").trim();
    const hasRouting = Boolean(String(originId || "").trim() || String(destinationId || "").trim());
    const hasIdentity = Boolean(
      String(passengerName || "").trim() ||
        String(seatNumber || "").trim() ||
        String(ticketNumber || "").trim()
    );

    if (!hasRouting || !hasIdentity) return;

    const dedupeKey = [
      String(itemJourneyId || "").trim(),
      String(passengerName || "").trim(),
      String(seatNumber || "").trim(),
      String(ticketNumber || "").trim(),
      String(originId || "").trim(),
      String(destinationId || "").trim()
    ]
      .join("|||")
      .toLocaleLowerCase("tr");

    if (seen.has(dedupeKey)) return;
    seen.add(dedupeKey);

    collected.push({
      journeyId: String(itemJourneyId || "").trim(),
      tripId: String(itemJourneyId || "").trim(),
      seferId: String(itemJourneyId || "").trim(),
      passengerName: String(passengerName || "").trim(),
      seatNumber: String(seatNumber || "").trim(),
      ticketNumber: String(ticketNumber || "").trim(),
      originId: String(originId || "").trim(),
      "origin-id": String(originId || "").trim(),
      destinationId: String(destinationId || "").trim(),
      "destination-id": String(destinationId || "").trim(),
      label: buildStationPassengerPassengerDisplayLabel({
        passengerName,
        seatNumber,
        ticketNumber
      }),
      raw: node
    });
  };

  const walk = (node, depth = 0, historyScopeActive = false) => {
    if (depth > 8 || node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) {
          walk(parsed, depth + 1, historyScopeActive);
        }
      }
      return;
    }
    if (typeof node !== "object") return;
    if (visited.has(node)) return;
    visited.add(node);

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1, historyScopeActive));
      return;
    }

    if (!historyScopeActive) {
      pushCandidate(node);
    }

    Object.entries(node).forEach(([key, value]) => {
      const nextHistoryScopeActive = historyScopeActive || normalizeTokenName(key).includes("history");
      walk(value, depth + 1, nextHistoryScopeActive);
    });
  };

  walk(rootPayload);

  return collected.sort((a, b) => {
    const bySeat = parseStationPassengerSeatSortValue(a.seatNumber) - parseStationPassengerSeatSortValue(b.seatNumber);
    if (bySeat !== 0) return bySeat;
    const byName = String(a.passengerName || "").localeCompare(String(b.passengerName || ""), "tr");
    if (byName !== 0) return byName;
    return String(a.ticketNumber || "").localeCompare(String(b.ticketNumber || ""), "tr");
  });
}

function filterStationPassengerPassengerStateByStationId(items, { stationId = "" } = {}) {
  const normalizedStationId = String(stationId || "").trim();
  const normalizedItems = Array.isArray(items) ? items : [];
  if (!normalizedStationId) {
    return {
      boardingPassengers: [],
      dropoffPassengers: []
    };
  }

  return {
    boardingPassengers: normalizedItems.filter(
      (item) => String(item?.originId || item?.["origin-id"] || "").trim() === normalizedStationId
    ),
    dropoffPassengers: normalizedItems.filter(
      (item) => String(item?.destinationId || item?.["destination-id"] || "").trim() === normalizedStationId
    )
  };
}

function buildStationPassengerComparableDateTimeKey(value) {
  const text = String(value || "").trim();
  if (!text) return "";

  const directMatch = text.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}):(\d{2})(?::(\d{2}))?)?/
  );
  if (directMatch) {
    const year = directMatch[1];
    const month = directMatch[2];
    const day = directMatch[3];
    const hour = directMatch[4] || "00";
    const minute = directMatch[5] || "00";
    const second = directMatch[6] || "00";
    return `${year}${month}${day}${hour}${minute}${second}`;
  }

  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return "";

  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: STATION_PASSENGER_INFO_TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23"
    }).formatToParts(parsed);
    const year = String(parts.find((part) => part.type === "year")?.value || "").trim();
    const month = String(parts.find((part) => part.type === "month")?.value || "").trim();
    const day = String(parts.find((part) => part.type === "day")?.value || "").trim();
    const hour = String(parts.find((part) => part.type === "hour")?.value || "").trim();
    const minute = String(parts.find((part) => part.type === "minute")?.value || "").trim();
    const second = String(parts.find((part) => part.type === "second")?.value || "").trim();
    if (year && month && day && hour && minute && second) {
      return `${year}${month}${day}${hour}${minute}${second}`;
    }
  } catch (err) {
    // Ignore and fallback to local date parts below.
  }

  const year = String(parsed.getFullYear()).padStart(4, "0");
  const month = String(parsed.getMonth() + 1).padStart(2, "0");
  const day = String(parsed.getDate()).padStart(2, "0");
  const hour = String(parsed.getHours()).padStart(2, "0");
  const minute = String(parsed.getMinutes()).padStart(2, "0");
  const second = String(parsed.getSeconds()).padStart(2, "0");
  return `${year}${month}${day}${hour}${minute}${second}`;
}

function findStationPassengerNextStationAfterRequest(items, requestDate) {
  const normalizedItems = Array.isArray(items) ? items : [];
  const requestKey = buildStationPassengerComparableDateTimeKey(requestDate);
  if (!requestKey) return null;

  return normalizedItems.reduce((bestItem, item) => {
    const departureKey = buildStationPassengerComparableDateTimeKey(
      item?.departureTime || item?.["departure-time"]
    );
    if (!departureKey || departureKey <= requestKey) {
      return bestItem;
    }

    if (!bestItem) return item;

    const bestDepartureKey = buildStationPassengerComparableDateTimeKey(
      bestItem?.departureTime || bestItem?.["departure-time"]
    );
    if (!bestDepartureKey || departureKey < bestDepartureKey) {
      return item;
    }

    if (departureKey === bestDepartureKey) {
      const bestOrder = Number.isFinite(bestItem?.order) ? Number(bestItem.order) : Number.MAX_SAFE_INTEGER;
      const currentOrder = Number.isFinite(item?.order) ? Number(item.order) : Number.MAX_SAFE_INTEGER;
      if (currentOrder < bestOrder) {
        return item;
      }
    }

    return bestItem;
  }, null);
}

function getStationPassengerSortMinutes(value) {
  const text = String(value || "").trim();
  const match = text.match(/^(\d{2}):(\d{2})$/);
  if (!match) return Number.MAX_SAFE_INTEGER;
  return Number(match[1]) * 60 + Number(match[2]);
}

function parseStationPassengerStatusValue(value) {
  if (value === true) return true;
  if (value === false) return false;
  if (Number.isFinite(Number(value))) {
    const numericValue = Number(value);
    if (numericValue === 1) return true;
    if (numericValue === 0) return false;
  }

  const normalized = String(value || "").trim().toLocaleLowerCase("tr");
  if (!normalized) return null;
  if (["true", "1", "ok", "active", "aktif"].includes(normalized)) return true;
  if (["false", "0", "inactive", "pasif"].includes(normalized)) return false;
  return null;
}

function extractStationPassengerJourneyItems(payload, { companyCode = "", cluster = "" } = {}) {
  const collected = [];
  const seen = new Set();
  const visited = new Set();

  const pushCandidate = (node, { statusScopeActive = false, directStatus = null } = {}) => {
    if (directStatus === false) return;
    if (!statusScopeActive && directStatus !== true) return;

    const departureTime = extractStationPassengerDepartureTime(node) || "-";
    const routeInfo = extractStationPassengerRouteInfo(node) || "-";
    const journeyId = extractStationPassengerJourneyId(node);
    const plateCandidates = collectStationPassengerPlateCandidates(node);
    plateCandidates.forEach((plateText) => {
      const normalizedPlate = normalizeStationPassengerPlate(plateText);
      if (!normalizedPlate) return;

      const key = [
        normalizedPlate,
        String(journeyId || "").trim(),
        String(departureTime || "").trim(),
        String(routeInfo || "").trim()
      ]
        .join("|||")
        .toLocaleLowerCase("tr");

      if (seen.has(key)) return;
      seen.add(key);

      collected.push({
        id: String(journeyId || "").trim(),
        tripId: String(journeyId || "").trim(),
        journeyId: String(journeyId || "").trim(),
        seferId: String(journeyId || "").trim(),
        plate: formatStationPassengerPlateDisplay(plateText) || String(plateText || "").trim() || normalizedPlate,
        normalizedPlate,
        departureTime,
        routeInfo,
        companyCode: String(companyCode || "").trim(),
        cluster: extractClusterLabel(cluster),
        raw: node
      });
    });
  };

  const walk = (node, depth = 0, statusScopeActive = false) => {
    if (depth > 7 || node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) walk(parsed, depth + 1, statusScopeActive);
      }
      return;
    }
    if (typeof node !== "object") return;
    if (visited.has(node)) return;
    visited.add(node);

    const directStatus = parseStationPassengerStatusValue(readPartnerRawValueByAliases(node, ["status"]));
    const nextStatusScopeActive =
      directStatus === true ? true : directStatus === false ? false : statusScopeActive;

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1, nextStatusScopeActive));
      return;
    }

    pushCandidate(node, {
      statusScopeActive: nextStatusScopeActive,
      directStatus
    });
    Object.values(node).forEach((value) => walk(value, depth + 1, nextStatusScopeActive));
  };

  walk(payload);

  return collected.sort((a, b) => {
    const byTime = getStationPassengerSortMinutes(a.departureTime) - getStationPassengerSortMinutes(b.departureTime);
    if (byTime !== 0) return byTime;
    const byRoute = String(a.routeInfo || "").localeCompare(String(b.routeInfo || ""), "tr");
    if (byRoute !== 0) return byRoute;
    return String(a.plate || "").localeCompare(String(b.plate || ""), "tr");
  });
}

async function resolveStationPassengerLoginResult({
  endpointUrl,
  companyUrl,
  partnerCode = "",
  partnerId = "",
  username,
  password,
  fallbackBranchId = "",
  allowEmptyPartnerCode = false,
  loginBranchId = "",
  authorization = STATION_PASSENGER_INFO_API_AUTH,
  timeoutMs = STATION_PASSENGER_INFO_TIMEOUT_MS
}) {
  const initialResult = await fetchAuthorizedLinesLoginInfo({
    endpointUrl,
    companyUrl,
    partnerCode,
    partnerId,
    username,
    password,
    fallbackBranchId,
    allowEmptyPartnerCode,
    loginBranchId,
    authorization,
    timeoutMs
  });
  const initialToken = String(initialResult?.token || "").trim();
  const retryBranchCandidates = buildUniqueLoginBranchCandidates(
    loginBranchId,
    initialResult?.obusMerkezBranchKey,
    initialResult?.branchId,
    fallbackBranchId,
    partnerId
  ).filter((item) => item !== String(loginBranchId || "").trim());

  if (!(initialResult?.ok === true && !initialToken && retryBranchCandidates.length > 0)) {
    return initialResult;
  }

  const retryResults = [];
  for (const branchCandidate of retryBranchCandidates) {
    const retryResult = await fetchAuthorizedLinesLoginInfo({
      endpointUrl,
      companyUrl,
      partnerCode,
      partnerId,
      username,
      password,
      fallbackBranchId,
      allowEmptyPartnerCode,
      loginBranchId: branchCandidate,
      authorization,
      timeoutMs
    });
    retryResults.push(retryResult);
    const retryToken = String(retryResult?.token || "").trim();
    if (retryResult?.ok === true && retryToken) {
      return retryResult;
    }
  }

  const mergedDetail = [
    String(initialResult?.tokenMissingDetail || initialResult?.errorDetail || "").trim(),
    ...retryResults.map((item) => String(item?.tokenMissingDetail || item?.errorDetail || "").trim())
  ]
    .filter(Boolean)
    .join(" | ");
  const finalRetryResult = retryResults[retryResults.length - 1] || null;

  return {
    ok: false,
    error: String(finalRetryResult?.error || "").trim() || "UserLogin branch seçimi sonrası token alınamadı.",
    errorDetail: mergedDetail,
    sessionId: String(finalRetryResult?.sessionId || initialResult?.sessionId || "").trim(),
    deviceId: String(finalRetryResult?.deviceId || initialResult?.deviceId || "").trim(),
    branchId: String(finalRetryResult?.branchId || initialResult?.branchId || retryBranchCandidates[0] || "").trim(),
    token: String(finalRetryResult?.token || "").trim(),
    obusMerkezBranchKey: String(
      finalRetryResult?.obusMerkezBranchKey || initialResult?.obusMerkezBranchKey || ""
    ).trim(),
    tokenMissingDetail: mergedDetail,
    rawLoginBody: String(finalRetryResult?.rawLoginBody || initialResult?.rawLoginBody || "").trim(),
    loginUrl: String(finalRetryResult?.loginUrl || initialResult?.loginUrl || "").trim(),
    serviceLogs: [
      ...(Array.isArray(initialResult?.serviceLogs) ? initialResult.serviceLogs : []),
      ...retryResults.flatMap((item) => (Array.isArray(item?.serviceLogs) ? item.serviceLogs : []))
    ],
    failedServiceLog:
      finalRetryResult?.failedServiceLog ||
      retryResults.find((item) => item?.failedServiceLog)?.failedServiceLog ||
      initialResult?.failedServiceLog ||
      null
  };
}

async function fetchStationPassengerDailyJourneySummaries({
  endpointUrl,
  sessionId,
  deviceId,
  token,
  plateQuery = "",
  dateValue = "",
  includeDateField = true,
  companyCode = "",
  cluster = "",
  authorization = STATION_PASSENGER_INFO_API_AUTH
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();
  const normalizedPlateQuery = normalizeStationPassengerPlate(plateQuery);
  const fallbackRequestDateParts = buildStationPassengerRequestDateParts(new Date());
  const normalizedRequestDate = normalizeIsoDateInput(String(dateValue || "").trim()) || fallbackRequestDateParts.isoDate;
  const requestBody = buildStationPassengerDailySummariesRequestBody({
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    token: normalizedToken,
    dateValue: normalizedRequestDate,
    includeDateField
  });
  const requestBodyPreview = includeDateField
    ? {
        data: normalizedRequestDate,
        date: normalizedRequestDate ? `${normalizedRequestDate} 00:00:00` : ""
      }
    : {
        data: normalizedRequestDate
      };

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      items: [],
      allItemsCount: 0,
      sampleRawPlates: [],
      sampleActivePlates: [],
      requestUrl: "",
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      error: "GetDailyJourneySummaries URL oluşturulamadı.",
      detail: ""
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId || !normalizedToken) {
    return {
      ok: false,
      items: [],
      allItemsCount: 0,
      sampleRawPlates: [],
      sampleActivePlates: [],
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      error: "GetDailyJourneySummaries için session/device/token eksik.",
      detail: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetDailyJourneySummaries",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        items: [],
        allItemsCount: 0,
        sampleRawPlates: [],
        sampleActivePlates: [],
        requestUrl: normalizedEndpointUrl,
        requestDate: normalizedRequestDate,
        requestBodyPreview,
        responsePayload: parsed ?? raw,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 140,
          responseMaxLen: 220
        })
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          items: [],
          allItemsCount: 0,
          sampleRawPlates: [],
          sampleActivePlates: [],
          requestUrl: normalizedEndpointUrl,
          requestDate: normalizedRequestDate,
          requestBodyPreview,
          responsePayload: parsed ?? raw,
          status: response.status,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 140,
            responseMaxLen: 220
          })
        };
      }
    }

    const sampleRawPlates = collectStationPassengerPlateCandidates(parsed ?? raw);
    const allItems = extractStationPassengerJourneyItems(parsed ?? raw, {
      companyCode,
      cluster
    });
    const sampleActivePlates = allItems.map((item) => item.plate).filter(Boolean);
    const items = normalizedPlateQuery
      ? allItems.filter((item) => item.normalizedPlate === normalizedPlateQuery)
      : allItems;

    return {
      ok: true,
      items,
      allItemsCount: allItems.length,
      sampleRawPlates,
      sampleActivePlates,
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: parsed ?? raw,
      status: response.status,
      error: "",
      detail: allItems.length
        ? ""
        : buildObusServiceTraceText(trace, extractObusApiLogDetail(parsed, raw, ""), {
            bodyMaxLen: 140,
            responseMaxLen: 200
          })
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetDailyJourneySummaries",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "GetDailyJourneySummaries isteği başarısız."
    });
    return {
      ok: false,
      items: [],
      allItemsCount: 0,
      sampleRawPlates: [],
      sampleActivePlates: [],
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      error: err?.message || "GetDailyJourneySummaries isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "GetDailyJourneySummaries isteği başarısız.", {
        bodyMaxLen: 140,
        responseMaxLen: 180
      })
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJourneyUpdateDetailPayload({
  endpointUrl,
  sessionId,
  deviceId,
  token,
  journeyId = "",
  dateValue = "",
  authorization = STATION_PASSENGER_INFO_API_AUTH
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();
  const normalizedJourneyId = String(journeyId || "").trim();
  const fallbackRequestDateParts = buildStationPassengerRequestDateParts(new Date());
  const normalizedRequestDate = normalizeIsoDateInput(String(dateValue || "").trim()) || fallbackRequestDateParts.isoDate;
  const requestBody = buildJourneyUpdateDetailRequestBody({
    journeyId: normalizedJourneyId,
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    token: normalizedToken,
    dateValue: normalizedRequestDate
  });
  const requestBodyPreview = {
    data: normalizeStationPassengerJourneyIdForRequest(normalizedJourneyId),
    date: normalizedRequestDate ? `${normalizedRequestDate} 00:00:00` : ""
  };

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      requestUrl: "",
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      status: null,
      error: "GetJourneyDetail URL oluşturulamadı.",
      detail: ""
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId || !normalizedToken) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      status: null,
      error: "GetJourneyDetail için session/device/token eksik.",
      detail: ""
    };
  }

  if (!normalizedJourneyId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      status: null,
      error: "GetJourneyDetail için journey-id zorunludur.",
      detail: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetJourneyDetail",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        requestUrl: normalizedEndpointUrl,
        requestDate: normalizedRequestDate,
        requestBodyPreview,
        responsePayload: parsed ?? raw,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 140,
          responseMaxLen: 220
        })
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          requestUrl: normalizedEndpointUrl,
          requestDate: normalizedRequestDate,
          requestBodyPreview,
          responsePayload: parsed ?? raw,
          status: response.status,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 140,
            responseMaxLen: 220
          })
        };
      }
    }

    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: parsed ?? raw,
      status: response.status,
      error: "",
      detail: ""
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetJourneyDetail",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "GetJourneyDetail isteği başarısız."
    });
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestDate: normalizedRequestDate,
      requestBodyPreview,
      responsePayload: null,
      status: null,
      error: err?.message || "GetJourneyDetail isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "GetJourneyDetail isteği başarısız.", {
        bodyMaxLen: 140,
        responseMaxLen: 180
      })
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJourneyUpdateUpdatePayload({
  endpointUrl,
  sessionId,
  deviceId,
  token,
  journeyId = "",
  parameters = [],
  dataPayload = null,
  authorization = STATION_PASSENGER_INFO_API_AUTH
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();
  const normalizedJourneyId = String(journeyId || "").trim();
  const normalizedParameters = Array.isArray(parameters) ? parameters : [];
  const requestDate = buildJourneyUpdateUpdateRequestDate();
  const requestBody = buildJourneyUpdateUpdateRequestBody({
    journeyId: normalizedJourneyId,
    parameters: normalizedParameters,
    dataPayload,
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    token: normalizedToken,
    dateValue: requestDate
  });

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      requestUrl: "",
      requestBody,
      responsePayload: null,
      status: null,
      error: "UpdateJourney URL oluşturulamadı.",
      detail: ""
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId || !normalizedToken) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responsePayload: null,
      status: null,
      error: "UpdateJourney için session/device/token eksik.",
      detail: ""
    };
  }

  if (!normalizedJourneyId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responsePayload: null,
      status: null,
      error: "UpdateJourney için id zorunludur.",
      detail: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Inventory UpdateJourney",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responsePayload: parsed ?? raw,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 180,
          responseMaxLen: 240
        })
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          requestUrl: normalizedEndpointUrl,
          requestBody,
          responsePayload: parsed ?? raw,
          status: response.status,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 180,
            responseMaxLen: 240
          })
        };
      }
    }

    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responsePayload: parsed ?? raw,
      status: response.status,
      error: "",
      detail: ""
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Inventory UpdateJourney",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "UpdateJourney isteği başarısız."
    });
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responsePayload: null,
      status: null,
      error: err?.message || "UpdateJourney isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "UpdateJourney isteği başarısız.", {
        bodyMaxLen: 180,
        responseMaxLen: 200
      })
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchStationPassengerJourneyStations({
  endpointUrl,
  sessionId,
  deviceId,
  token,
  journeyId,
  authorization = STATION_PASSENGER_INFO_API_AUTH
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();
  const normalizedJourneyId = String(journeyId || "").trim();
  const requestDate = formatDateTimeToSecondPrecisionInTimeZone(new Date(), STATION_PASSENGER_INFO_TIME_ZONE);
  const requestBody = buildStationPassengerJourneyStationsRequestBody({
    journeyId: normalizedJourneyId,
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    token: normalizedToken,
    dateValue: requestDate
  });

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      requestUrl: "",
      error: "GetJourneyStations URL oluşturulamadı.",
      detail: "",
      items: [],
      requestDate,
      nextStation: null,
      status: null
    };
  }

  if (!normalizedJourneyId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: "journey-id zorunludur.",
      detail: "",
      items: [],
      requestDate,
      nextStation: null,
      status: null
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId || !normalizedToken) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: "GetJourneyStations için session/device/token eksik.",
      detail: "",
      items: [],
      requestDate,
      nextStation: null,
      status: null
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetJourneyStations",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        requestUrl: normalizedEndpointUrl,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 140,
          responseMaxLen: 220
        }),
        items: [],
        requestDate,
        nextStation: null,
        status: response.status
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          requestUrl: normalizedEndpointUrl,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 140,
            responseMaxLen: 220
          }),
          items: [],
          requestDate,
          nextStation: null,
          status: response.status
        };
      }
    }

    const items = extractStationPassengerJourneyStations(parsed ?? raw, {
      journeyId: normalizedJourneyId
    });
    const nextStation = findStationPassengerNextStationAfterRequest(items, requestDate);

    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      error: "",
      detail: "",
      items,
      requestDate,
      nextStation,
      status: response.status
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Inventory GetJourneyStations",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "GetJourneyStations isteği başarısız."
    });
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: err?.message || "GetJourneyStations isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "GetJourneyStations isteği başarısız.", {
        bodyMaxLen: 140,
        responseMaxLen: 180
      }),
      items: [],
      requestDate,
      nextStation: null,
      status: null
    };
  } finally {
    clearTimeout(timeout);
  }
}

function findStationPassengerStationCatalogItem(items, stationId = "") {
  const normalizedStationId = String(stationId || "").trim();
  if (!normalizedStationId) return null;
  const normalizedItems = Array.isArray(items) ? items : [];
  return (
    normalizedItems.find((item) => {
      const candidateId = String(item?.id || item?.value || "").trim();
      return candidateId === normalizedStationId;
    }) || null
  );
}

function applyStationPassengerJourneyStationNames(items, stationCatalogItems = []) {
  const normalizedItems = Array.isArray(items) ? items : [];
  const stationNameMap = new Map();

  (Array.isArray(stationCatalogItems) ? stationCatalogItems : []).forEach((item) => {
    const stationId = String(item?.id || item?.value || "").trim();
    const stationName = String(item?.name || item?.label || "").trim();
    if (!stationId || !stationName || stationNameMap.has(stationId)) return;
    stationNameMap.set(stationId, stationName);
  });

  return normalizedItems.map((item) => {
    const stationId = String(item?.stationId || item?.["station-id"] || "").trim();
    const existingStationName = String(item?.stationName || item?.["station-name"] || "").trim();
    const resolvedStationName = existingStationName || stationNameMap.get(stationId) || "";
    if (!resolvedStationName) return item;
    return {
      ...item,
      stationName: resolvedStationName,
      "station-name": resolvedStationName
    };
  });
}

function findStationPassengerJourneyStationByIdentity(items, candidate) {
  if (!candidate || typeof candidate !== "object") return null;
  const normalizedItems = Array.isArray(items) ? items : [];
  const candidateOrder = Number.isFinite(Number(candidate?.order)) ? Number(candidate.order) : null;
  const candidateStationId = String(candidate?.stationId || candidate?.["station-id"] || "").trim();
  const candidateDepartureTime = String(candidate?.departureTime || candidate?.["departure-time"] || "").trim();

  return (
    normalizedItems.find((item) => {
      const itemOrder = Number.isFinite(Number(item?.order)) ? Number(item.order) : null;
      const itemStationId = String(item?.stationId || item?.["station-id"] || "").trim();
      const itemDepartureTime = String(item?.departureTime || item?.["departure-time"] || "").trim();
      return itemOrder === candidateOrder && itemStationId === candidateStationId && itemDepartureTime === candidateDepartureTime;
    }) ||
    normalizedItems.find((item) => {
      const itemStationId = String(item?.stationId || item?.["station-id"] || "").trim();
      return candidateStationId && itemStationId === candidateStationId;
    }) ||
    null
  );
}

async function fetchStationPassengerWebStations({
  endpointUrl,
  sessionId,
  deviceId,
  authorization = STATION_PASSENGER_INFO_WEB_STATIONS_API_AUTH,
  partnerCode = STATION_PASSENGER_INFO_TARGET_COMPANY_CODE
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedPartnerCode = String(partnerCode || "").trim() || STATION_PASSENGER_INFO_TARGET_COMPANY_CODE;
  const cacheKey = normalizedPartnerCode.toLocaleLowerCase("tr");
  const cached = stationPassengerWebStationsCache.get(cacheKey);
  if (
    cached &&
    cached.expiresAt > Date.now() &&
    Array.isArray(cached.items) &&
    cached.items.length > 0
  ) {
    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      items: cached.items,
      fromCache: true,
      error: "",
      detail: ""
    };
  }

  const requestDate = formatDateTimeToOffsetPrecisionInTimeZone(new Date(), STATION_PASSENGER_INFO_TIME_ZONE, "+03:00");
  const requestBody = buildStationPassengerWebStationsRequestBody({
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    dateValue: requestDate
  });

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      requestUrl: "",
      items: [],
      fromCache: false,
      error: "GetStations URL oluşturulamadı.",
      detail: ""
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      items: [],
      fromCache: false,
      error: "GetStations için session/device eksik.",
      detail: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization,
        PartnerCode: normalizedPartnerCode
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Web GetStations",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        requestUrl: normalizedEndpointUrl,
        items: [],
        fromCache: false,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 140,
          responseMaxLen: 220
        })
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          requestUrl: normalizedEndpointUrl,
          items: [],
          fromCache: false,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 140,
            responseMaxLen: 220
          })
        };
      }
    }

    const items = extractJourneySearchStations(parsed ?? raw);
    if (items.length > 0) {
      stationPassengerWebStationsCache.set(cacheKey, {
        items,
        expiresAt: Date.now() + Math.max(60000, STATION_PASSENGER_INFO_WEB_STATIONS_CACHE_TTL_MS)
      });
    }

    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      items,
      fromCache: false,
      error: "",
      detail: ""
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Web GetStations",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "GetStations isteği başarısız."
    });
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      items: [],
      fromCache: false,
      error: err?.message || "GetStations isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "GetStations isteği başarısız.", {
        bodyMaxLen: 140,
        responseMaxLen: 180
      })
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchStationPassengerPassengerStateHistory({
  endpointUrl,
  sessionId,
  deviceId,
  token,
  journeyId,
  stationId,
  authorization = STATION_PASSENGER_INFO_API_AUTH
}) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();
  const normalizedJourneyId = String(journeyId || "").trim();
  const normalizedStationId = String(stationId || "").trim();
  const requestDate =
    formatDateTimeToSecondPrecisionInTimeZone(new Date(), STATION_PASSENGER_INFO_TIME_ZONE).replace(" ", "T");
  const requestBody = buildStationPassengerPassengerStateHistoryRequestBody({
    journeyId: normalizedJourneyId,
    sessionId: normalizedSessionId,
    deviceId: normalizedDeviceId,
    token: normalizedToken,
    dateValue: requestDate
  });

  if (!normalizedEndpointUrl) {
    return {
      ok: false,
      requestUrl: "",
      error: "GetPassengerStateHistory URL oluşturulamadı.",
      detail: "",
      items: [],
      boardingPassengers: [],
      dropoffPassengers: [],
      requestDate,
      status: null
    };
  }

  if (!normalizedJourneyId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: "journey-id zorunludur.",
      detail: "",
      items: [],
      boardingPassengers: [],
      dropoffPassengers: [],
      requestDate,
      status: null
    };
  }

  if (!normalizedStationId) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: "station-id zorunludur.",
      detail: "",
      items: [],
      boardingPassengers: [],
      dropoffPassengers: [],
      requestDate,
      status: null
    };
  }

  if (!normalizedSessionId || !normalizedDeviceId || !normalizedToken) {
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: "GetPassengerStateHistory için session/device/token eksik.",
      detail: "",
      items: [],
      boardingPassengers: [],
      dropoffPassengers: [],
      requestDate,
      status: null
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(STATION_PASSENGER_INFO_TIMEOUT_MS, 45000, 5000, 180000)
  );

  try {
    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const trace = buildObusServiceTraceEntry({
      service: "Payment GetPassengerStateHistory",
      url: normalizedEndpointUrl,
      status: response.status,
      requestBody,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        ok: false,
        requestUrl: normalizedEndpointUrl,
        error: `HTTP ${response.status}: ${reason}`,
        detail: buildObusServiceTraceText(trace, reason, {
          bodyMaxLen: 140,
          responseMaxLen: 220
        }),
        items: [],
        boardingPassengers: [],
        dropoffPassengers: [],
        requestDate,
        status: response.status
      };
    }

    if (parsed && typeof parsed === "object") {
      const hasExplicitStatusField = "status" in parsed || "success" in parsed || "status-code" in parsed;
      if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
        const reason =
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
        return {
          ok: false,
          requestUrl: normalizedEndpointUrl,
          error: reason,
          detail: buildObusServiceTraceText(trace, reason, {
            bodyMaxLen: 140,
            responseMaxLen: 220
          }),
          items: [],
          boardingPassengers: [],
          dropoffPassengers: [],
          requestDate,
          status: response.status
        };
      }
    }

    const items = extractStationPassengerPassengerStateItems(parsed ?? raw, {
      journeyId: normalizedJourneyId
    });
    const { boardingPassengers, dropoffPassengers } = filterStationPassengerPassengerStateByStationId(items, {
      stationId: normalizedStationId
    });

    return {
      ok: true,
      requestUrl: normalizedEndpointUrl,
      items,
      boardingPassengers,
      dropoffPassengers,
      requestDate,
      status: response.status,
      error: "",
      detail:
        items.length > 0
          ? ""
          : buildObusServiceTraceText(trace, extractObusApiLogDetail(parsed, raw, ""), {
              bodyMaxLen: 140,
              responseMaxLen: 200
            })
    };
  } catch (err) {
    const trace = buildObusServiceTraceEntry({
      service: "Payment GetPassengerStateHistory",
      url: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      error: err?.message || "GetPassengerStateHistory isteği başarısız."
    });
    return {
      ok: false,
      requestUrl: normalizedEndpointUrl,
      error: err?.message || "GetPassengerStateHistory isteği başarısız.",
      detail: buildObusServiceTraceText(trace, err?.message || "GetPassengerStateHistory isteği başarısız.", {
        bodyMaxLen: 140,
        responseMaxLen: 180
      }),
      items: [],
      boardingPassengers: [],
      dropoffPassengers: [],
      requestDate,
      status: null
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function searchStationPassengerJourneysByPlate(plateQuery) {
  const normalizedPlate = normalizeStationPassengerPlate(plateQuery);
  const displayPlate = formatStationPassengerPlateDisplay(plateQuery) || String(plateQuery || "").trim();
  const inventoryLogin = getInventoryBranchesLoginCredentials();
  const endpointUrl = normalizeTargetUrl(STATION_PASSENGER_INFO_API_URL);
  const clusterLabel = extractClusterLabel(endpointUrl || STATION_PASSENGER_INFO_API_URL) || "cluster3";
  const requestDateParts = buildStationPassengerRequestDateParts(new Date());
  const requestDate = requestDateParts.isoDate;
  const requestDateTime = formatDateTimeToSecondPrecisionInTimeZone(new Date(), STATION_PASSENGER_INFO_TIME_ZONE);
  const targetCode = STATION_PASSENGER_INFO_TARGET_COMPANY_CODE;
  const targetId = STATION_PASSENGER_INFO_TARGET_COMPANY_ID;
  const sourceCompanyLabel = `${targetCode} / ${targetId}`;

  if (!normalizedPlate) {
    return {
      ok: false,
      items: [],
      requestUrl: endpointUrl,
      requestDate,
      requestDateTime,
      error: "Plaka girilmesi zorunludur.",
      detail: ""
    };
  }

  if (!inventoryLogin.username || !inventoryLogin.password) {
    return {
      ok: false,
      items: [],
      requestUrl: endpointUrl,
      requestDate,
      requestDateTime,
      error: buildObusServiceLoginConfigurationMessage(inventoryLogin),
      detail: ""
    };
  }

  const targetResolution = await resolveStationPassengerTargetCandidate({
    endpointUrl,
    clusterLabel
  });
  const targetCandidate = targetResolution.candidate;
  const targetCandidateFromCache = targetResolution.fromCache;
  const partnerError = targetResolution.partnerError;

  const loginResult = await resolveStationPassengerLoginResult({
    endpointUrl,
    companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
    partnerCode: String(targetCandidate.code || "").trim(),
    partnerId: String(targetCandidate.id || "").trim(),
    username: inventoryLogin.username,
    password: inventoryLogin.password,
    fallbackBranchId: String(targetCandidate.branchId || targetCandidate.id || "").trim(),
    allowEmptyPartnerCode: false,
    authorization: STATION_PASSENGER_INFO_API_AUTH,
    timeoutMs: STATION_PASSENGER_INFO_TIMEOUT_MS
  });

  if (!(loginResult?.ok && String(loginResult.token || "").trim())) {
    const loginError = String(loginResult?.error || "").trim() || "UserLogin başarısız.";
    const loginDetail =
      String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim() ||
      (!targetCandidateFromCache && String(partnerError || "").trim()) ||
      "";
    return {
      ok: false,
      items: [],
      requestUrl: endpointUrl,
      requestDate,
      requestDateTime,
      searchedPlate: displayPlate,
      sourceCompany: sourceCompanyLabel,
      sourceMode: "company",
      error: loginError,
      detail: loginDetail
    };
  }

  const fetchResult = await fetchStationPassengerDailyJourneySummaries({
    endpointUrl,
    sessionId: loginResult.sessionId,
    deviceId: loginResult.deviceId,
    token: loginResult.token,
    plateQuery: normalizedPlate,
    companyCode: String(targetCandidate.code || "").trim(),
    cluster: clusterLabel,
    authorization: STATION_PASSENGER_INFO_API_AUTH
  });

  if (!fetchResult.ok) {
    return {
      ok: false,
      items: [],
      requestUrl: fetchResult.requestUrl || endpointUrl,
      requestDate,
      requestDateTime,
      searchedPlate: displayPlate,
      sourceCompany: sourceCompanyLabel,
      sourceMode: "company",
      error: String(fetchResult.error || "").trim() || "GetDailyJourneySummaries başarısız.",
      detail: String(fetchResult.detail || "").trim()
    };
  }

  return {
    ...fetchResult,
    ok: true,
    searchedPlate: displayPlate,
    sourceCompany: sourceCompanyLabel,
    sourceMode: "company",
    requestDateTime,
    detail: String(fetchResult.detail || "").trim(),
    authContext: buildStationPassengerAuthContext({
      endpointUrl,
      companyCode: String(targetCandidate.code || "").trim(),
      companyId: String(targetCandidate.id || "").trim(),
      companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
      cluster: clusterLabel,
      loginResult
    })
  };
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

let localMachineIpAddressCache = null;

function isUsableLocalIpv4Address(value = "") {
  const address = String(value || "").trim();
  if (net.isIP(address) !== 4) return false;

  const [firstOctetRaw, secondOctetRaw] = address.split(".");
  const firstOctet = Number.parseInt(firstOctetRaw, 10);
  const secondOctet = Number.parseInt(secondOctetRaw, 10);
  if (firstOctet === 0 || firstOctet === 127) return false;
  if (firstOctet === 169 && secondOctet === 254) return false;
  return true;
}

function getPrimaryLocalMachineIpAddress() {
  if (localMachineIpAddressCache !== null) {
    return localMachineIpAddressCache;
  }

  const candidates = [];
  const networkMap = os.networkInterfaces();
  Object.values(networkMap || {}).forEach((items) => {
    (Array.isArray(items) ? items : []).forEach((item) => {
      if (!item || item.internal) return;
      const address = String(item.address || "").trim();
      const family = String(item.family || "").toLowerCase();
      if ((family === "ipv4" || item.family === 4) && isUsableLocalIpv4Address(address)) {
        candidates.push(address);
      }
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

function buildObusSessionRequestBodyText() {
  return JSON.stringify(buildObusSessionRequestBody(), null, 2);
}

function buildExecuteRequestBody(body) {
  const bodyText = typeof body === "string" ? body : JSON.stringify(body);
  const parsedBody = parseJsonSafe(bodyText);
  if (!parsedBody || typeof parsedBody !== "object" || Array.isArray(parsedBody)) {
    return bodyText;
  }

  const connection = parsedBody.connection;
  if (
    !connection ||
    typeof connection !== "object" ||
    Array.isArray(connection) ||
    !Object.prototype.hasOwnProperty.call(connection, "ip-address")
  ) {
    return bodyText;
  }

  connection["ip-address"] = getObusSessionConnectionIpAddress();
  return JSON.stringify(parsedBody);
}

async function fetchPartnerSessionCredentials(
  sessionUrl,
  signal,
  authorization = PARTNERS_API_AUTH
) {
  const payload = buildObusSessionRequestBody();

  try {
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
    const debug = buildObusServiceTraceEntry({
      service: "GetSession",
      url: sessionUrl,
      status: response.status,
      requestBody: payload,
      responseBody: parsed ?? raw
    });

    if (!response.ok) {
      const reason =
        (parsed && typeof parsed === "object" && String(parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        sessionId: "",
        deviceId: "",
        error: `GetSession HTTP ${response.status}: ${reason}`,
        debug
      };
    }

    if (!parsed) {
      return {
        sessionId: "",
        deviceId: "",
        error: "GetSession JSON parse edilemedi.",
        debug
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
        error: "GetSession yanıtında session-id veya device-id bulunamadı.",
        debug
      };
    }

    return {
      sessionId,
      deviceId,
      error: null,
      debug
    };
  } catch (err) {
    const debug = buildObusServiceTraceEntry({
      service: "GetSession",
      url: sessionUrl,
      requestBody: payload,
      responseBody: "",
      error: err?.message || "GetSession isteği başarısız."
    });
    return {
      sessionId: "",
      deviceId: "",
      error: err?.message || "GetSession isteği başarısız.",
      debug
    };
  }
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
      rows: filterAllCompaniesRowsForSource(partnerUrl, extractPartnerRawRows(payload, clusterLabel)),
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

function extractObusMerkezBranchRowsFromPayload(payload, fallbackPartnerId = "", clusterLabel = "") {
  const rows = [];
  const normalizedFallbackPartnerId = String(fallbackPartnerId || "").trim();
  const normalizedClusterLabel = extractClusterLabel(clusterLabel);
  const partnerIdAliases = [
    "partner-id",
    "partner_id",
    "partnerid",
    "partnerId",
    "partnerID",
    "provider-id",
    "provider_id",
    "providerid",
    "providerId",
    "providerID"
  ];
  const branchIdAliases = [
    "id",
    "branch-id",
    "branch_id",
    "branchid",
    "branch-key",
    "branch_key",
    "branchkey"
  ];
  const branchNameAliases = [
    "name",
    "branch-name",
    "branch_name",
    "branchname",
    "label",
    "title",
    "text",
    "value"
  ];

  const walk = (node) => {
    if (node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) walk(parsed);
      }
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item));
      return;
    }
    if (typeof node !== "object") return;

    const branchName = formatPartnerCellValue(readPartnerRawValueByAliases(node, branchNameAliases));
    if (normalizeTokenName(branchName) === "obusmerkez") {
      const partnerId =
        formatPartnerCellValue(readPartnerRawValueByAliases(node, partnerIdAliases)) || normalizedFallbackPartnerId;
      const branchId = formatPartnerCellValue(readPartnerRawValueByAliases(node, branchIdAliases));
      if (partnerId && branchId) {
        rows.push({
          partnerId,
          name: "OBUSMERKEZ",
          branchId,
          cluster: normalizedClusterLabel
        });
      }
    }

    Object.values(node).forEach((value) => walk(value));
  };

  walk(payload);
  return rows;
}

function extractObusMerkezBranchMapFromRows(rows) {
  const map = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const partnerId = String(row?.partnerId || "").trim();
    const branchId = String(row?.branchId || "").trim();
    const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.cluster);
    if (!partnerClusterKey || !branchId) return;
    if (!map.has(partnerClusterKey)) map.set(partnerClusterKey, branchId);
  });
  return map;
}

function rememberResolvedObusMerkezBranchIds(targetMap, payload) {
  if (!(targetMap instanceof Map) || !payload || typeof payload !== "object") return;

  if (payload.map instanceof Map) {
    payload.map.forEach((branchIdValue, partnerClusterKeyValue) => {
      const partnerClusterKey = String(partnerClusterKeyValue || "").trim();
      const branchId = String(branchIdValue || "").trim();
      if (!partnerClusterKey || !branchId || targetMap.has(partnerClusterKey)) return;
      targetMap.set(partnerClusterKey, branchId);
    });
  }

  if (Array.isArray(payload.rows)) {
    payload.rows.forEach((row) => {
      const partnerId = String(row?.partnerId || "").trim();
      const branchId = String(row?.branchId || "").trim();
      const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.cluster);
      if (!partnerClusterKey || !branchId || targetMap.has(partnerClusterKey)) return;
      targetMap.set(partnerClusterKey, branchId);
    });
  }
}

async function fetchObusMerkezBranchMapForTarget({
  clusterLabel,
  partnerCode,
  fallbackPartnerId = "",
  signal
}) {
  const inventoryLogin = getInventoryBranchesLoginCredentials();
  const cluster = extractClusterLabel(clusterLabel);
  const endpointUrl = normalizeTargetUrl(buildUrlForCluster(INVENTORY_BRANCHES_API_URL, cluster));
  const normalizedPartnerCode = String(partnerCode || "").trim();
  const normalizedFallbackPartnerId = String(fallbackPartnerId || "").trim();

  if (!endpointUrl) {
    return {
      cluster,
      map: new Map(),
      rows: [],
      error: "GetBranches endpoint URL geçersiz.",
      serviceLogs: [],
      failedServiceLog: null
    };
  }

  if (!inventoryLogin.username || !inventoryLogin.password) {
    return {
      cluster,
      map: new Map(),
      rows: [],
      error: buildObusServiceLoginConfigurationMessage(inventoryLogin),
      serviceLogs: [],
      failedServiceLog: null
    };
  }

  const loginResult = await fetchAuthorizedLinesLoginInfo({
    endpointUrl,
    companyUrl: endpointUrl,
    partnerCode: normalizedPartnerCode,
    partnerId: normalizedFallbackPartnerId,
    username: inventoryLogin.username,
    password: inventoryLogin.password,
    fallbackBranchId: normalizedFallbackPartnerId,
    timeoutMs: 20000,
    authorization: INVENTORY_BRANCHES_API_AUTH,
    allowEmptyPartnerCode: false
  });
  const serviceLogs = Array.isArray(loginResult?.serviceLogs) ? [...loginResult.serviceLogs] : [];

  if (!loginResult.ok) {
    return {
      cluster,
      map: new Map(),
      rows: [],
      error: `UserLogin başarısız: ${loginResult.error || "Bilinmeyen hata"}`,
      serviceLogs,
      failedServiceLog: loginResult?.failedServiceLog || getLastObusServiceTrace(serviceLogs)
    };
  }

  const sessionId = String(loginResult.sessionId || "").trim();
  const deviceId = String(loginResult.deviceId || "").trim();
  const token = String(loginResult.token || "").trim();
  const loginObusMerkezBranchKey = String(loginResult.obusMerkezBranchKey || "").trim();
  if (loginObusMerkezBranchKey) {
    const map = new Map();
    const rows = [];
    const partnerClusterKey = buildObusMerkezPartnerClusterKey(normalizedFallbackPartnerId, cluster);
    if (normalizedFallbackPartnerId) {
      if (partnerClusterKey) {
        map.set(partnerClusterKey, loginObusMerkezBranchKey);
      }
      rows.push({
        partnerId: normalizedFallbackPartnerId,
        name: "OBUSMERKEZ",
        branchId: loginObusMerkezBranchKey,
        cluster
      });
    } else {
      rows.push({
        partnerId: "",
        name: "OBUSMERKEZ",
        branchId: loginObusMerkezBranchKey,
        cluster
      });
    }
    return { cluster, map, rows, error: null, serviceLogs, failedServiceLog: null };
  }

  const missingFields = [];
  if (!sessionId) missingFields.push("session-id");
  if (!deviceId) missingFields.push("device-id");
  if (!token) missingFields.push("token");
  if (missingFields.length > 0) {
    return {
      cluster,
      map: new Map(),
      rows: [],
      error: `UserLogin sonucu eksik alan: ${missingFields.join(", ")}.`,
      serviceLogs,
      failedServiceLog: getLastObusServiceTrace(serviceLogs)
    };
  }

  const body = {
    data: "{}",
    "device-session": {
      "session-id": sessionId,
      "device-id": deviceId
    },
    token,
    date: "2016-03-11T11:33:00",
    language: "tr-TR"
  };

  try {
    const response = await fetch(endpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: INVENTORY_BRANCHES_API_AUTH
      },
      body: JSON.stringify(body),
      signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const getBranchesTrace = buildObusServiceTraceEntry({
      service: "GetBranches",
      url: endpointUrl,
      status: response.status,
      requestBody: body,
      responseBody: parsed ?? raw
    });
    serviceLogs.push(getBranchesTrace);
    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        cluster,
        map: new Map(),
        rows: [],
        error: `GetBranches HTTP ${response.status}: ${reason}`,
        serviceLogs,
        failedServiceLog: getBranchesTrace
      };
    }

    const rows = extractObusMerkezBranchRowsFromPayload(parsed ?? raw, normalizedFallbackPartnerId, cluster);
    const map = extractObusMerkezBranchMapFromRows(rows);
    return {
      cluster,
      map,
      rows,
      error: null,
      serviceLogs,
      failedServiceLog: rows.length > 0 ? null : getBranchesTrace
    };
  } catch (err) {
    const getBranchesTrace = buildObusServiceTraceEntry({
      service: "GetBranches",
      url: endpointUrl,
      requestBody: body,
      responseBody: "",
      error: err?.message || "GetBranches isteği başarısız."
    });
    serviceLogs.push(getBranchesTrace);
    return {
      cluster,
      map: new Map(),
      rows: [],
      error: err?.message || "GetBranches isteği başarısız.",
      serviceLogs,
      failedServiceLog: getBranchesTrace
    };
  }
}

async function enrichAllCompaniesRowsWithObusMerkezSubeId(rows, signal) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const inventoryLogin = getInventoryBranchesLoginCredentials();
  if (sourceRows.length === 0) {
    return { rows: [], notice: null };
  }

  if (!inventoryLogin.username || !inventoryLogin.password) {
    return {
      rows: sourceRows,
      notice: buildObusServiceLoginConfigurationMessage(inventoryLogin)
    };
  }

  const errors = [];
  const compactErrorText = (value, maxLen = 180) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text) return "";
    if (text.length <= maxLen) return text;
    return `${text.slice(0, Math.max(0, maxLen - 3))}...`;
  };
  const enrichedRows = sourceRows.map((row) => ({
    ...row,
    ObusMerkezSubeID: String(row?.ObusMerkezSubeID || "").trim()
  }));
  const resolvedByPartnerClusterKey = new Map();
  enrichedRows.forEach((row) => {
    const partnerId = String(row?.id || "").trim();
    const branchId = String(row?.ObusMerkezSubeID || "").trim();
    const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.source);
    if (!partnerClusterKey || !branchId) return;
    if (!resolvedByPartnerClusterKey.has(partnerClusterKey)) {
      resolvedByPartnerClusterKey.set(partnerClusterKey, branchId);
    }
  });
  const fetchResultPromiseCache = new Map();

  const enrichResults = await runWithConcurrency(
    enrichedRows,
    ALL_COMPANIES_OBUS_ENRICH_CONCURRENCY,
    async (row) => {
      if (Boolean(signal?.aborted)) {
        return {
          branchId: "",
          errorMessage: "zaman aşımı nedeniyle kısmi sonuç üretildi"
        };
      }

      const existingBranchId = String(row?.ObusMerkezSubeID || "").trim();
      if (existingBranchId) {
        return { branchId: existingBranchId, errorMessage: null };
      }

      const partnerId = String(row?.id || "").trim();
      const partnerCode = String(row?.code || "").trim();
      const clusterLabel = extractClusterLabel(row?.source);
      const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, clusterLabel);
      const rowRef = `${clusterLabel || "cluster?"} / ${partnerCode || "code?"} / ${partnerId || "id?"}`;
      const isDebugTarget = isAllCompaniesObusMerkezDebugTarget(row);
      if (isDebugTarget) {
        logAllCompaniesObusMerkezDebug("start-row", {
          rowRef,
          source: String(row?.source || "").trim(),
          partnerCode,
          partnerId
        });
      }

      if (!partnerId) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("missing-partner-id", { rowRef });
        }
        return {
          branchId: "",
          errorMessage: `${rowRef}: partner-id boş.`
        };
      }
      const preResolvedBranchId = String(resolvedByPartnerClusterKey.get(partnerClusterKey) || "").trim();
      if (preResolvedBranchId) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("resolved-from-shared-cache-before-fetch", {
            rowRef,
            branchId: preResolvedBranchId
          });
        }
        return {
          branchId: preResolvedBranchId,
          errorMessage: null
        };
      }
      if (!partnerCode) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("missing-partner-code", { rowRef });
        }
        return {
          branchId: "",
          errorMessage: `${rowRef}: partner-code boş.`
        };
      }
      if (!clusterLabel) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("missing-cluster", { rowRef });
        }
        return {
          branchId: "",
          errorMessage: `${rowRef}: cluster bilgisi boş.`
        };
      }

      const fetchCacheKey = `${clusterLabel}|||${partnerCode}|||${partnerId}`;
      if (!fetchResultPromiseCache.has(fetchCacheKey)) {
        fetchResultPromiseCache.set(
          fetchCacheKey,
          fetchObusMerkezBranchMapForTarget({
            clusterLabel,
            partnerCode,
            fallbackPartnerId: partnerId,
            signal
          })
        );
      }
      const result = await fetchResultPromiseCache.get(fetchCacheKey);
      rememberResolvedObusMerkezBranchIds(resolvedByPartnerClusterKey, result);
      const resultTraceText = buildObusServiceTraceText(
        result?.failedServiceLog || getLastObusServiceTrace(result?.serviceLogs),
        result?.error || ""
      );
      if (isDebugTarget) {
        logAllCompaniesObusMerkezDebug("fetch-result", {
          rowRef,
          resultCluster: String(result?.cluster || "").trim(),
          mapSize: result?.map instanceof Map ? result.map.size : 0,
          rowCount: Array.isArray(result?.rows) ? result.rows.length : 0,
          error: String(result?.error || "").trim(),
          trace: resultTraceText
        });
      }

      if (result.error) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("fetch-error", {
            rowRef,
            error: compactErrorText(result.error, 320),
            trace: resultTraceText
          });
        }
        return {
          branchId: "",
          errorMessage: `${rowRef}: ${compactErrorText(result.error)}`,
          debugDetail: resultTraceText || compactErrorText(result.error, 520)
        };
      }

      const sharedResolvedBranchId = String(resolvedByPartnerClusterKey.get(partnerClusterKey) || "").trim();
      if (sharedResolvedBranchId) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("resolved-from-shared-cache-after-fetch", {
            rowRef,
            branchId: sharedResolvedBranchId
          });
        }
        return {
          branchId: sharedResolvedBranchId,
          errorMessage: null
        };
      }

      const mapBranchId =
        result.map instanceof Map ? String(result.map.get(partnerClusterKey) || "").trim() : "";
      if (mapBranchId) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("resolved-from-map", { rowRef, branchId: mapBranchId });
        }
        return {
          branchId: mapBranchId,
          errorMessage: null
        };
      }

      const rowBranchId = Array.isArray(result.rows)
        ? String(
            (result.rows.find((item) => String(item?.partnerId || "").trim() === partnerId) || {}).branchId || ""
          ).trim()
        : "";
      if (rowBranchId) {
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("resolved-from-rows", { rowRef, branchId: rowBranchId });
        }
        return {
          branchId: rowBranchId,
          errorMessage: null
        };
      }

      const notFoundDebugDetail =
        buildObusServiceTraceText(
          result?.failedServiceLog || getLastObusServiceTrace(result?.serviceLogs),
          "Eşleşen OBUSMERKEZ kaydı bulunamadı."
        ) || "Eşleşen OBUSMERKEZ kaydı bulunamadı.";

      if (isDebugTarget) {
        const mapKeys =
          result?.map instanceof Map
            ? Array.from(result.map.keys())
                .map((item) => String(item || "").trim())
                .filter(Boolean)
            : [];
        const rowPartnerIds = Array.isArray(result?.rows)
          ? Array.from(
              new Set(
                result.rows
                  .map((item) => String(item?.partnerId || "").trim())
                  .filter(Boolean)
              )
            )
          : [];
        const rowBranchIds = Array.isArray(result?.rows)
          ? Array.from(
              new Set(
                result.rows
                  .map((item) => String(item?.branchId || "").trim())
                  .filter(Boolean)
              )
            )
          : [];
        logAllCompaniesObusMerkezDebug("not-found-in-fetch-result", {
          rowRef,
          mapSize: result?.map instanceof Map ? result.map.size : 0,
          mapKeys: mapKeys.slice(0, 10),
          rowCount: Array.isArray(result?.rows) ? result.rows.length : 0,
          rowPartnerIds: rowPartnerIds.slice(0, 10),
          rowBranchIds: rowBranchIds.slice(0, 10),
          trace: notFoundDebugDetail
        });
      }
      return {
        branchId: "",
        errorMessage: `${rowRef}: Eşleşen OBUSMERKEZ kaydı bulunamadı.`,
        debugDetail: notFoundDebugDetail
      };
    },
    () => Boolean(signal?.aborted)
  );

  const unresolvedItems = [];
  enrichResults.forEach((result, index) => {
    const row = enrichedRows[index];
    if (!row) return;
    const partnerId = String(row?.id || "").trim();
    const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.source);
    const isDebugTarget = isAllCompaniesObusMerkezDebugTarget(row);

    const resolvedBranchId = String(result?.branchId || "").trim();
    if (resolvedBranchId) {
      row.ObusMerkezSubeID = resolvedBranchId;
      row.ObusMerkezSubeIDDebug = "";
      if (partnerClusterKey && !resolvedByPartnerClusterKey.has(partnerClusterKey)) {
        resolvedByPartnerClusterKey.set(partnerClusterKey, resolvedBranchId);
      }
      if (isDebugTarget) {
        logAllCompaniesObusMerkezDebug("resolved-after-worker", {
          source: String(row?.source || "").trim(),
          partnerCode: String(row?.code || "").trim(),
          partnerId,
          branchId: resolvedBranchId
        });
      }
      return;
    }

    if (partnerId) {
      const siblingResolvedBranchId = String(resolvedByPartnerClusterKey.get(partnerClusterKey) || "").trim();
      if (siblingResolvedBranchId) {
        row.ObusMerkezSubeID = siblingResolvedBranchId;
        row.ObusMerkezSubeIDDebug = "";
        if (isDebugTarget) {
          logAllCompaniesObusMerkezDebug("resolved-from-sibling", {
            source: String(row?.source || "").trim(),
            partnerCode: String(row?.code || "").trim(),
            partnerId,
            branchId: siblingResolvedBranchId
          });
        }
        return;
      }
    }

    let errorMessage = "";
    if (typeof result?.errorMessage === "string" && result.errorMessage.trim()) {
      errorMessage = result.errorMessage.trim();
    } else {
      const runtimeError = result?.error;
      if (runtimeError) {
        const partnerCode = String(row?.code || "").trim();
        const clusterLabel = extractClusterLabel(row?.source);
        const rowRef = `${clusterLabel || "cluster?"} / ${partnerCode || "code?"} / ${partnerId || "id?"}`;
        errorMessage = `${rowRef}: ${compactErrorText(runtimeError?.message || runtimeError)}`;
      }
    }

    unresolvedItems.push({
      index,
      partnerId,
      errorMessage,
      isDebugTarget,
      debugDetail: String(result?.debugDetail || "").trim()
    });
  });

  const unresolvedAfterSiblingPass = unresolvedItems.filter(
    (item) => !String(enrichedRows[item.index]?.ObusMerkezSubeID || "").trim()
  );
  let fallbackErrorMessage = "";
  let fallbackDebugDetail = "";
  if (unresolvedAfterSiblingPass.length > 0 && !Boolean(signal?.aborted)) {
    const hasDebugTarget = unresolvedAfterSiblingPass.some((item) => item.isDebugTarget === true);
    if (hasDebugTarget) {
      logAllCompaniesObusMerkezDebug("fallback-start", {
        unresolvedCount: unresolvedAfterSiblingPass.length
      });
    }
    const fallbackResult = await collectObusMerkezBranchRowsForAllCompanies(enrichedRows);
    const fallbackRows = Array.isArray(fallbackResult?.rows) ? fallbackResult.rows : [];
    const fallbackMapByPartnerClusterKey = new Map();
    fallbackRows.forEach((row) => {
      const partnerId = String(row?.partnerId || "").trim();
      const branchId = String(row?.branchId || "").trim();
      const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.cluster);
      if (!partnerClusterKey || !branchId) return;
      if (!fallbackMapByPartnerClusterKey.has(partnerClusterKey)) {
        fallbackMapByPartnerClusterKey.set(partnerClusterKey, branchId);
      }
    });

    unresolvedAfterSiblingPass.forEach((item) => {
      const row = enrichedRows[item.index];
      if (!row || String(row?.ObusMerkezSubeID || "").trim()) return;
      const partnerId = String(item.partnerId || "").trim();
      const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.source);
      if (!partnerClusterKey) return;
      const fallbackBranchId = String(fallbackMapByPartnerClusterKey.get(partnerClusterKey) || "").trim();
      if (!fallbackBranchId) return;
      row.ObusMerkezSubeID = fallbackBranchId;
      row.ObusMerkezSubeIDDebug = "";
      if (!resolvedByPartnerClusterKey.has(partnerClusterKey)) {
        resolvedByPartnerClusterKey.set(partnerClusterKey, fallbackBranchId);
      }
      if (item.isDebugTarget === true) {
        logAllCompaniesObusMerkezDebug("resolved-from-fallback", {
          source: String(row?.source || "").trim(),
          partnerCode: String(row?.code || "").trim(),
          partnerId,
          branchId: fallbackBranchId
        });
      }
    });

    fallbackErrorMessage = String(fallbackResult?.error || "").trim();
    fallbackDebugDetail = String(fallbackResult?.debugDetail || "").trim();
    if (hasDebugTarget) {
      logAllCompaniesObusMerkezDebug("fallback-result", {
        rowCount: fallbackRows.length,
        error: compactErrorText(fallbackErrorMessage, 320),
        trace: fallbackDebugDetail
      });
    }
  }

  const finalUnresolvedItems = unresolvedItems.filter(
    (item) => !String(enrichedRows[item.index]?.ObusMerkezSubeID || "").trim()
  );
  finalUnresolvedItems.forEach((item) => {
    if (item.errorMessage) {
      errors.push(item.errorMessage);
    }
    const row = enrichedRows[item.index];
    if (!row) return;
    const debugSource = [item.debugDetail, fallbackDebugDetail]
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .join(" || fallback=");
    const debugText = `Detay: ${compactErrorText(
      debugSource || item.errorMessage || fallbackErrorMessage || "Bilinmeyen hata",
      760
    )}`;
    row.ObusMerkezSubeIDDebug = debugText;
    if (item.isDebugTarget === true) {
      logAllCompaniesObusMerkezDebug("final-unresolved", {
        source: String(row?.source || "").trim(),
        partnerCode: String(row?.code || "").trim(),
        partnerId: String(row?.id || "").trim(),
        error: item.errorMessage,
        debugDetail: item.debugDetail,
        fallbackDebugDetail,
        debugText
      });
    }
  });
  if (fallbackErrorMessage && finalUnresolvedItems.length > 0) {
    errors.push(`GetBranches fallback: ${compactErrorText(fallbackErrorMessage, 240)}`);
  }

  if (Boolean(signal?.aborted)) {
    errors.push("zaman aşımı nedeniyle kısmi sonuç üretildi");
  }

  const uniqueErrors = Array.from(new Set(errors.filter(Boolean)));
  const notice =
    uniqueErrors.length > 0
      ? `ObusMerkezSubeID: ${uniqueErrors.slice(0, 2).join(" | ")}${uniqueErrors.length > 2 ? ` (+${uniqueErrors.length - 2} hata)` : ""}`
      : null;

  return {
    rows: enrichedRows,
    notice
  };
}

async function fetchPartnerCodes() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);

  try {
    const partnerUrls = buildPartnerFetchUrls();
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

async function fetchAllPartnerRows({ includeObusMerkezSubeId = false } = {}) {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(30000, ALL_COMPANIES_FETCH_TIMEOUT_MS));
  let lastKnownClusterCount = 0;

  try {
    const partnerUrls = buildPartnerFetchUrls();
    lastKnownClusterCount = partnerUrls.length;
    if (partnerUrls.length === 0) {
      return {
        columns: [],
        rows: [],
        error: "Partner URL yapılandırması boş.",
        clusterCount: 0,
        metrics: {
          totalMs: Date.now() - startedAt,
          clusterFetchMs: 0,
          normalizeMs: 0,
          enrichMs: 0,
          rawRowCount: 0,
          rowCount: 0
        }
      };
    }

    const clusterFetchStartedAt = Date.now();
    const results = await runWithConcurrency(
      partnerUrls,
      ALL_COMPANIES_CLUSTER_CONCURRENCY,
      async (partnerUrl) => fetchPartnerRawRowsFromCluster(partnerUrl, controller.signal),
      () => Boolean(controller.signal.aborted)
    );
    const clusterFetchMs = Date.now() - clusterFetchStartedAt;

    const normalizeStartedAt = Date.now();
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

    const normalizedReport = normalizeAllCompaniesReportRows(mergedRows);
    const normalizeMs = Date.now() - normalizeStartedAt;
    const columns = normalizedReport.columns;
    let rows = normalizedReport.rows;
    let obusNotice = null;
    let enrichMs = 0;
    if (includeObusMerkezSubeId) {
      const enrichStartedAt = Date.now();
      const obusEnriched = await enrichAllCompaniesRowsWithObusMerkezSubeId(normalizedReport.rows, controller.signal);
      rows = obusEnriched.rows;
      obusNotice = obusEnriched.notice;
      enrichMs = Date.now() - enrichStartedAt;
    } else {
      rows = attachAllCompaniesMissingObusDebug(
        rows,
        "Detay: ObusMerkezSubeID servis verisinde henuz yok. 'ObusMerkezSubeID Guncelle' butonunu kullanin."
      );
    }

    const errorParts = [];
    if (errors.length > 0) {
      errorParts.push(`${errors.length}/${partnerUrls.length} cluster alınamadı.`);
    }
    if (obusNotice) {
      errorParts.push(obusNotice);
    }
    if (errorParts.length === 0 && rows.length === 0) {
      errorParts.push("Partner API sonucunda gösterilecek kayıt bulunamadı.");
    }
    const error = errorParts.length > 0 ? errorParts.join(" ") : null;

    return {
      columns,
      rows,
      error,
      clusterCount: partnerUrls.length,
      metrics: {
        totalMs: Date.now() - startedAt,
        clusterFetchMs,
        normalizeMs,
        enrichMs,
        rawRowCount: mergedRows.length,
        rowCount: rows.length
      }
    };
  } catch (err) {
    return {
      columns: [],
      rows: [],
      error: `Partner verileri alınamadı: ${err?.message || "Bilinmeyen hata"}`,
      clusterCount: 0,
      metrics: {
        totalMs: Date.now() - startedAt,
        clusterFetchMs: 0,
        normalizeMs: 0,
        enrichMs: 0,
        rawRowCount: 0,
        rowCount: 0,
        clusterCount: lastKnownClusterCount
      }
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAllCompaniesRowsFromCache() {
  try {
    const query = `
      SELECT
        id,
        code,
        source,
        obilet_partner_id,
        biletall_partner_id,
        url,
        is_abroad,
        obus_merkez_sube_id,
        updated_at
      FROM all_companies_cache
      ORDER BY source ASC, code ASC, id ASC
    `;
    const result = await pool.query(query);
    const rows = attachAllCompaniesMissingObusDebug(
      normalizeAllCompaniesCacheRows(result.rows || []),
      "Detay: SQL kaydinda ObusMerkezSubeID yok. Guncelleme yapilmadi veya eslesme bulunamadi."
    );
    const columns = normalizeAllCompaniesReportRows([]).columns;
    const clusterCount = new Set(rows.map((row) => extractClusterLabel(row?.source)).filter(Boolean)).size;
    return {
      columns,
      rows,
      clusterCount,
      error: null
    };
  } catch (err) {
    const columns = normalizeAllCompaniesReportRows([]).columns;
    return {
      columns,
      rows: [],
      clusterCount: 0,
      error: `Tüm firmalar önbelleği okunamadı: ${err?.message || "Bilinmeyen hata"}`
    };
  }
}

async function fetchAllCompaniesObusMerkezSubeIdMap() {
  try {
    const query = `
      SELECT
        code,
        id,
        source,
        obus_merkez_sube_id
      FROM all_companies_cache
      WHERE COALESCE(NULLIF(TRIM(obus_merkez_sube_id), ''), '') <> ''
    `;
    const result = await pool.query(query);
    const map = new Map();

    (result.rows || []).forEach((row) => {
      const code = String(row?.code || "").trim();
      const id = String(row?.id || "").trim();
      const source = extractClusterLabel(String(row?.source || "").trim());
      const obusMerkezSubeId = String(row?.obus_merkez_sube_id || "").trim();
      if (!code || !source || !obusMerkezSubeId) return;
      const key = `${code}|||${id}|||${source}`;
      map.set(key, obusMerkezSubeId);
    });

    return {
      map,
      error: null
    };
  } catch (err) {
    return {
      map: new Map(),
      error: `ObusMerkezSubeID önbelleği okunamadı: ${err?.message || "Bilinmeyen hata"}`
    };
  }
}

function mergeKnownObusMerkezSubeIdsIntoAllCompaniesRows(rows, obusMerkezSubeIdByKey) {
  const normalizedRows = normalizeAllCompaniesCacheRows(rows);
  if (normalizedRows.length === 0 || !(obusMerkezSubeIdByKey instanceof Map) || obusMerkezSubeIdByKey.size === 0) {
    return normalizedRows;
  }

  return normalizedRows.map((row) => {
    const existingBranchId = String(row?.ObusMerkezSubeID || "").trim();
    if (existingBranchId) {
      if (!String(row?.ObusMerkezSubeIDDebug || "").trim()) return row;
      return {
        ...row,
        ObusMerkezSubeIDDebug: ""
      };
    }

    const knownBranchId = String(obusMerkezSubeIdByKey.get(buildAllCompaniesCacheRowKey(row)) || "").trim();
    if (!knownBranchId) return row;

    return {
      ...row,
      ObusMerkezSubeID: knownBranchId,
      ObusMerkezSubeIDDebug: ""
    };
  });
}

async function attachKnownObusMerkezSubeIdsToAllCompaniesReport(report = {}) {
  const normalizedRows = normalizeAllCompaniesCacheRows(report.rows || []);
  if (normalizedRows.length === 0) {
    return {
      report: {
        ...report,
        rows: normalizedRows
      },
      obusCacheError: null
    };
  }

  const { map, error } = await fetchAllCompaniesObusMerkezSubeIdMap();
  return {
    report: {
      ...report,
      rows: mergeKnownObusMerkezSubeIdsIntoAllCompaniesRows(normalizedRows, map)
    },
    obusCacheError: error
  };
}

async function upsertAllCompaniesCacheRows(rows, options = {}) {
  const pruneMissing = options?.pruneMissing === true;
  const normalizedRows = normalizeAllCompaniesCacheRows(rows).filter((row) => {
    const code = String(row?.code || "").trim();
    const source = String(row?.source || "").trim();
    return Boolean(code && source);
  });

  if (normalizedRows.length === 0) {
    return {
      savedCount: 0,
      deletedCount: 0,
      error: null
    };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const row of normalizedRows) {
      const id = String(row?.id || "").trim();
      const code = String(row?.code || "").trim();
      const source = extractClusterLabel(row?.source);
      const obiletPartnerId = String(row?.["obilet-partner-id"] || "").trim() || null;
      const biletallPartnerId = String(row?.["biletall-partner-id"] || "").trim() || null;
      const url = String(row?.url || "").trim() || null;
      const isAbroad = parseAllCompaniesBooleanValue(row?.isabroad ?? row?.is_abroad);
      const obusMerkezSubeId = String(row?.ObusMerkezSubeID || "").trim() || null;
      const updateResult = await client.query(
        `
          UPDATE all_companies_cache
          SET obilet_partner_id = $4,
              biletall_partner_id = $5,
              url = $6,
              is_abroad = COALESCE($7, is_abroad),
              obus_merkez_sube_id = COALESCE(NULLIF($8, ''), obus_merkez_sube_id),
              updated_at = now()
          WHERE id = $1
            AND source = $2
            AND code = $3
        `,
        [
          id,
          source,
          code,
          obiletPartnerId,
          biletallPartnerId,
          url,
          isAbroad,
          obusMerkezSubeId
        ]
      );
      if (updateResult.rowCount) continue;

      await client.query(
        `
          INSERT INTO all_companies_cache (
            id,
            code,
            source,
            obilet_partner_id,
            biletall_partner_id,
            url,
            is_abroad,
            obus_merkez_sube_id,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
        `,
        [
          id,
          code,
          source,
          obiletPartnerId,
          biletallPartnerId,
          url,
          isAbroad,
          obusMerkezSubeId
        ]
      );
    }

    let deletedCount = 0;
    if (pruneMissing) {
      const keepKeySet = new Set(normalizedRows.map((row) => buildAllCompaniesCacheRowKey(row)));
      const existingKeysResult = await client.query(
        `
          SELECT id, source, code
          FROM all_companies_cache
        `
      );

      const rowsToDelete = (existingKeysResult.rows || [])
        .map((row) => ({
          id: String(row?.id || "").trim(),
          source: extractClusterLabel(row?.source),
          code: String(row?.code || "").trim()
        }))
        .filter((row) => row.id && row.source && row.code)
        .filter((row) => !keepKeySet.has(buildAllCompaniesCacheRowKey(row)));

      const deleteChunkSize = 200;
      for (let i = 0; i < rowsToDelete.length; i += deleteChunkSize) {
        const chunk = rowsToDelete.slice(i, i + deleteChunkSize);
        const params = [];
        const whereParts = [];

        chunk.forEach((row, index) => {
          const base = index * 3;
          whereParts.push(`(id = $${base + 1} AND source = $${base + 2} AND code = $${base + 3})`);
          params.push(row.id, row.source, row.code);
        });

        if (whereParts.length > 0) {
          await client.query(
            `
              DELETE FROM all_companies_cache
              WHERE ${whereParts.join(" OR ")}
            `,
            params
          );
        }
      }

      deletedCount = rowsToDelete.length;
    }

    await client.query("COMMIT");
    return {
      savedCount: normalizedRows.length,
      deletedCount,
      error: null
    };
  } catch (err) {
    await client.query("ROLLBACK");
    return {
      savedCount: 0,
      deletedCount: 0,
      error: `Tüm firmalar önbelleği yazılamadı: ${err?.message || "Bilinmeyen hata"}`
    };
  } finally {
    client.release();
  }
}

async function syncAllCompaniesCacheFromService() {
  const cachedReport = await fetchAllCompaniesRowsFromCache();
  const cachedRows = Array.isArray(cachedReport.rows) ? cachedReport.rows : [];
  const existingKeySet = new Set(cachedRows.map((row) => buildAllCompaniesCacheRowKey(row)));

  const liveResult = await fetchAllPartnerRows({ includeObusMerkezSubeId: false });
  const liveRows = normalizeAllCompaniesCacheRows(liveResult.rows || []);
  const newRows = liveRows.filter((row) => !existingKeySet.has(buildAllCompaniesCacheRowKey(row)));

  const saveResult = await upsertAllCompaniesCacheRows(newRows);
  const errorParts = [];
  if (liveResult.error) errorParts.push(liveResult.error);
  if (saveResult.error) errorParts.push(saveResult.error);

  return {
    fetchedCount: liveRows.length,
    newCount: newRows.length,
    savedCount: saveResult.savedCount,
    clusterCount: liveResult.clusterCount || 0,
    error: errorParts.length > 0 ? errorParts.join(" | ") : null
  };
}

function buildEmptyAllCompaniesReport(clusterCount = PARTNER_CLUSTER_TOTAL) {
  const normalized = normalizeAllCompaniesReportRows([]);
  const resolvedClusterCount = Number.isFinite(Number(clusterCount)) ? Number(clusterCount) : PARTNER_CLUSTER_TOTAL;
  return {
    columns: normalized.columns,
    rows: [],
    error: null,
    clusterCount: Math.max(0, resolvedClusterCount),
    requested: false
  };
}

function normalizeAllCompaniesPreviewUserId(userId) {
  const parsed = Number(userId);
  return Number.isInteger(parsed) ? parsed : null;
}

function setAllCompaniesServicePreviewForUser(userId, report = {}) {
  const userIdNum = normalizeAllCompaniesPreviewUserId(userId);
  if (!userIdNum) return;

  const normalizedRows = normalizeAllCompaniesCacheRows(report.rows || []);
  const normalizedColumns = Array.isArray(report.columns) && report.columns.length > 0
    ? report.columns
    : normalizeAllCompaniesReportRows([]).columns;

  allCompaniesServicePreviewCache.set(userIdNum, {
    rows: normalizedRows,
    columns: normalizedColumns,
    clusterCount: Number.isFinite(Number(report.clusterCount)) ? Number(report.clusterCount) : 0,
    createdAt: Date.now()
  });
}

function getAllCompaniesServicePreviewForUser(userId) {
  const userIdNum = normalizeAllCompaniesPreviewUserId(userId);
  if (!userIdNum) return null;

  const snapshot = allCompaniesServicePreviewCache.get(userIdNum);
  if (!snapshot) return null;

  if (Date.now() - Number(snapshot.createdAt || 0) > ALL_COMPANIES_SERVICE_PREVIEW_TTL_MS) {
    allCompaniesServicePreviewCache.delete(userIdNum);
    return null;
  }

  return snapshot;
}

function buildAllCompaniesObusUpdateRowLabel(row) {
  const clusterLabel = extractClusterLabel(row?.source);
  const code = String(row?.code || "").trim();
  const partnerId = String(row?.id || "").trim();
  return `${clusterLabel || "cluster?"} / ${code || "code?"} / ${partnerId || "id?"}`;
}

function buildAllCompaniesObusUpdateMissingBranchDetails(rows) {
  const itemsByKey = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const branchId = String(row?.ObusMerkezSubeID || "").trim();
    const detailText = String(row?.ObusMerkezSubeIDDebug || "").trim();
    if (branchId || !detailText) return;
    const key = buildAllCompaniesCacheRowKey(row);
    if (!key || itemsByKey.has(key)) return;
    itemsByKey.set(key, {
      key,
      detailText
    });
  });
  return Array.from(itemsByKey.values());
}

function attachAllCompaniesObusUpdateMissingBranchDetails(rows, detailItems) {
  if (!Array.isArray(rows) || rows.length === 0 || !Array.isArray(detailItems) || detailItems.length === 0) {
    return Array.isArray(rows) ? rows : [];
  }

  const detailMap = new Map();
  detailItems.forEach((item) => {
    const key = String(item?.key || "").trim();
    const detailText = String(item?.detailText || "").trim();
    if (!key || !detailText || detailMap.has(key)) return;
    detailMap.set(key, detailText);
  });

  if (detailMap.size === 0) {
    return rows;
  }

  return rows.map((row) => {
    const branchId = String(row?.ObusMerkezSubeID || "").trim();
    if (branchId) return row;

    const detailText = String(detailMap.get(buildAllCompaniesCacheRowKey(row)) || "").trim();
    if (!detailText) return row;

    return {
      ...row,
      ObusMerkezSubeIDDebug: detailText
    };
  });
}

function buildAllCompaniesObusUpdateJobSummary(job) {
  const summary = job && typeof job?.summary === "object" ? job.summary : {};
  const scanned = Number.isFinite(Number(summary?.scanned))
    ? Math.max(0, Number(summary.scanned))
    : Math.max(0, Number(job?.totalCount || 0));
  const filled = Number.isFinite(Number(summary?.filled))
    ? Math.max(0, Number(summary.filled))
    : Math.max(0, Number(job?.successCount || 0));
  const remaining = Number.isFinite(Number(summary?.remaining))
    ? Math.max(0, Number(summary.remaining))
    : Math.max(0, Number(job?.failureCount || 0));
  const missingBranchDetails = Array.isArray(summary?.missingBranchDetails)
    ? summary.missingBranchDetails
        .map((item) => ({
          key: String(item?.key || "").trim(),
          detailText: String(item?.detailText || "").trim()
        }))
        .filter((item) => item.key && item.detailText)
    : [];
  return {
    scanned,
    filled,
    remaining,
    partial: summary?.partial === true || remaining > 0,
    notice: String(summary?.notice || "").trim(),
    missingBranchDetails
  };
}

async function runAllCompaniesServiceSyncJob(job, ownerUserId) {
  if (!job || typeof job !== "object") return;

  try {
    const fetchedResult = await fetchAllPartnerRows({ includeObusMerkezSubeId: false });
    const { report: liveResult, obusCacheError } = await attachKnownObusMerkezSubeIdsToAllCompaniesReport(
      fetchedResult
    );
    if (obusCacheError) {
      console.error("All companies service sync ObusMerkezSubeID cache read error:", obusCacheError);
    }
    setAllCompaniesServicePreviewForUser(ownerUserId, liveResult);

    const rowCount = Array.isArray(liveResult?.rows) ? liveResult.rows.length : 0;
    job.summary = {
      rowCount,
      clusterCount: Number(liveResult?.clusterCount || 0),
      error: String(liveResult?.error || "").trim()
    };

    pushObusLiveJobEvent(job, {
      key: "all-companies-service-sync",
      label: "Servisten Güncelle",
      ok: true,
      message: `${rowCount} kayıt hazır.`,
      error: "",
      errorDetail: String(liveResult?.error || "").trim()
    });
    finishObusLiveJob(job, null);
  } catch (err) {
    console.error("All companies service sync background error:", err);
    finishObusLiveJob(job, `Servisten güncelleme tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`);
  }
}

async function runAllCompaniesObusMerkezUpdateJob(job, targetRows) {
  if (!job || typeof job !== "object") return;

  try {
    const normalizedTargetRows = normalizeAllCompaniesCacheRows(Array.isArray(targetRows) ? targetRows : []);
    job.totalCount = normalizedTargetRows.length;
    job.updatedAt = Date.now();

    if (normalizedTargetRows.length === 0) {
      job.summary = {
        scanned: 0,
        filled: 0,
        remaining: 0,
        partial: false,
        notice: ""
      };
      finishObusLiveJob(job, null);
      return;
    }

    const rowsNeedingService = normalizedTargetRows.filter((row) => !String(row?.ObusMerkezSubeID || "").trim());
    const enriched =
      rowsNeedingService.length > 0
        ? await enrichAllCompaniesRowsWithObusMerkezSubeId(normalizedTargetRows)
        : { rows: normalizedTargetRows, notice: null };

    const missingBranchDetails = buildAllCompaniesObusUpdateMissingBranchDetails(enriched.rows || normalizedTargetRows);
    const finalRows = normalizeAllCompaniesCacheRows(enriched.rows || normalizedTargetRows);
    const saveResult = await upsertAllCompaniesCacheRows(finalRows);
    if (saveResult.error) {
      console.error("All companies ObusMerkezSubeID background save error:", saveResult.error);
      finishObusLiveJob(job, `ObusMerkezSubeID güncellemesi kaydedilemedi: ${saveResult.error}`);
      return;
    }

    const filledCount = finalRows.reduce((sum, row) => sum + (String(row?.ObusMerkezSubeID || "").trim() ? 1 : 0), 0);
    const remainingCount = Math.max(0, normalizedTargetRows.length - filledCount);
    job.successCount = filledCount;
    job.failureCount = remainingCount;
    job.processedCount = normalizedTargetRows.length;
    job.updatedAt = Date.now();
    job.summary = {
      scanned: normalizedTargetRows.length,
      filled: filledCount,
      remaining: remainingCount,
      partial: Boolean(enriched.notice) || remainingCount > 0,
      notice: String(enriched.notice || "").trim(),
      missingBranchDetails
    };
    finishObusLiveJob(job, null);
  } catch (err) {
    console.error("All companies ObusMerkezSubeID background update error:", err);
    finishObusLiveJob(job, `ObusMerkezSubeID güncellemesi tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`);
  }
}

function buildObusMerkezBranchServiceReport(overrides = {}) {
  return {
    requested: false,
    rows: [],
    count: 0,
    sourceRowCount: 0,
    clusterCount: PARTNER_CLUSTER_TOTAL,
    notice: null,
    error: null,
    failures: [],
    saved: null,
    ...overrides
  };
}

function normalizeObusMerkezBranchRows(rows, { dedupeByPartnerId = true } = {}) {
  const recordByPartnerId = new Map();
  const normalizedRows = [];
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const partnerId = String(row?.partnerId || row?.["partner-id"] || "").trim();
    const branchId = String(row?.branchId || row?.id || "").trim();
    const cluster = String(row?.cluster || row?.source || "").trim().toLowerCase();
    const branchName = String(row?.name || "").trim();

    if (!partnerId || !branchId) return;
    if (normalizeTokenName(branchName) !== "obusmerkez") return;
    const normalized = {
      partnerId,
      name: "OBUSMERKEZ",
      branchId,
      cluster
    };

    if (!dedupeByPartnerId) {
      normalizedRows.push(normalized);
      return;
    }
    if (recordByPartnerId.has(partnerId)) return;
    recordByPartnerId.set(partnerId, normalized);
  });

  const outputRows = dedupeByPartnerId ? Array.from(recordByPartnerId.values()) : normalizedRows;
  return outputRows.sort((a, b) =>
    String(a.partnerId || "").localeCompare(String(b.partnerId || ""), "tr", { numeric: true })
  );
}

async function collectObusMerkezBranchRowsForAllCompanies(
  allCompanyRows,
  { clusterCount = PARTNER_CLUSTER_TOTAL, baseError = null } = {}
) {
  const targetClusterLabel = "cluster0";
  const sourceRows = Array.isArray(allCompanyRows) ? allCompanyRows : [];
  const compactErrorText = (value, maxLen = 180) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    if (!text) return "";
    if (text.length <= maxLen) return text;
    return `${text.slice(0, Math.max(0, maxLen - 3))}...`;
  };
  const clusterRank = (clusterLabel) => {
    const match = String(clusterLabel || "").match(/cluster(\d+)/i);
    if (!match) return Number.MAX_SAFE_INTEGER;
    const parsed = Number.parseInt(match[1], 10);
    return Number.isFinite(parsed) ? parsed : Number.MAX_SAFE_INTEGER;
  };

  if (sourceRows.length === 0) {
    return {
      rows: [],
      sourceRowCount: 0,
      clusterCount: 1,
      error: String(baseError || "").trim() || "GetBranches için firma listesi boş.",
      failures: [],
      debugDetail: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(30000, ALL_COMPANIES_FETCH_TIMEOUT_MS));

  try {
    const clusterLoginCodesByLabel = new Map();
    sourceRows.forEach((row) => {
      const cluster = extractClusterLabel(row?.source);
      const code = String(row?.code || "").trim();
      if (!cluster || !code) return;
      if (!clusterLoginCodesByLabel.has(cluster)) clusterLoginCodesByLabel.set(cluster, []);
      const codeList = clusterLoginCodesByLabel.get(cluster);
      if (!Array.isArray(codeList)) return;
      if (!codeList.includes(code)) codeList.push(code);
    });

    const clusterTargets = [
      {
        clusterLabel: targetClusterLabel,
        partnerCodes: Array.isArray(clusterLoginCodesByLabel.get(targetClusterLabel))
          ? clusterLoginCodesByLabel
              .get(targetClusterLabel)
              .map((code) => String(code || "").trim())
              .filter(Boolean)
          : []
      }
    ].sort((a, b) => clusterRank(a.clusterLabel) - clusterRank(b.clusterLabel));

    const collectedRows = [];
    const errors = [];
    let lastDebugDetail = "";

    for (const target of clusterTargets) {
      if (Boolean(controller.signal.aborted)) break;
      const attemptCodes = Array.from(
        new Set((Array.isArray(target.partnerCodes) ? target.partnerCodes : []).map((code) => String(code || "").trim()))
      ).filter(Boolean);
      if (attemptCodes.length === 0) {
        errors.push(`${target.clusterLabel}: Kullanılabilir partner-code bulunamadı.`);
        continue;
      }

      const attemptErrors = [];
      let resolved = false;
      let resolvedWithMatch = false;

      for (const partnerCode of attemptCodes) {
        if (Boolean(controller.signal.aborted)) break;
        const partnerCodeLabel = partnerCode ? partnerCode : "(boş)";

        const result = await fetchObusMerkezBranchMapForTarget({
          clusterLabel: target.clusterLabel,
          partnerCode,
          signal: controller.signal
        });
        const traceText = buildObusServiceTraceText(
          result?.failedServiceLog || getLastObusServiceTrace(result?.serviceLogs),
          result?.error || ""
        );
        if (traceText) {
          lastDebugDetail = traceText;
        }

        if (result.error) {
          attemptErrors.push(`${partnerCodeLabel}: ${compactErrorText(result.error)}`);
          continue;
        }

        resolved = true;
        if (Array.isArray(result.rows) && result.rows.length > 0) {
          result.rows.forEach((item) => {
            const partnerId = String(item?.partnerId || "").trim();
            const branchId = String(item?.branchId || "").trim();
            const name = String(item?.name || "OBUSMERKEZ").trim();
            if (!partnerId || !branchId || normalizeTokenName(name) !== "obusmerkez") return;
            collectedRows.push({
              partnerId,
              name: "OBUSMERKEZ",
              branchId,
              cluster: target.clusterLabel
            });
          });
        }

        const rowSize = Array.isArray(result.rows) ? result.rows.length : 0;
        if (rowSize === 0 && traceText) {
          lastDebugDetail = buildObusServiceTraceText(
            result?.failedServiceLog || getLastObusServiceTrace(result?.serviceLogs),
            "Eşleşen OBUSMERKEZ kaydı bulunamadı."
          );
        }
        if (rowSize > 0) {
          resolvedWithMatch = true;
          break;
        }
      }

      if (resolvedWithMatch) continue;
      if (resolved) {
        errors.push(`${target.clusterLabel}: Eşleşen OBUSMERKEZ kaydı bulunamadı.`);
        continue;
      }

      const uniqueAttemptErrors = Array.from(new Set(attemptErrors.filter(Boolean)));
      errors.push(
        `${target.clusterLabel}: ${
          uniqueAttemptErrors.length > 0
            ? `${uniqueAttemptErrors.slice(0, 2).join(" | ")}${
                uniqueAttemptErrors.length > 2 ? ` (+${uniqueAttemptErrors.length - 2} hata)` : ""
              }`
            : "UserLogin/GetBranches başarısız."
        }`
      );
    }

    const rows = normalizeObusMerkezBranchRows(collectedRows, { dedupeByPartnerId: false });
    const uniqueErrors = Array.from(new Set(errors.filter(Boolean)));
    const warningParts = [];
    if (String(baseError || "").trim()) warningParts.push(compactErrorText(baseError, 220));
    if (uniqueErrors.length > 0) {
      warningParts.push(`GetBranches: ${uniqueErrors.length} başarısız istek var.`);
    }
    if (Boolean(controller.signal.aborted)) warningParts.push("zaman aşımı nedeniyle kısmi sonuç üretildi");

    return {
      rows,
      sourceRowCount: sourceRows.length,
      clusterCount: 1,
      error: warningParts.length > 0 ? warningParts.join(" | ") : null,
      failures: uniqueErrors,
      debugDetail: lastDebugDetail
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function saveObusMerkezBranchRows(rows) {
  const normalizedRows = normalizeObusMerkezBranchRows(rows, { dedupeByPartnerId: true });
  if (normalizedRows.length === 0) {
    return {
      savedCount: 0,
      insertedCount: 0,
      updatedCount: 0,
      error: "Kaydedilecek OBUSMERKEZ kaydı bulunamadı."
    };
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const partnerIds = normalizedRows.map((item) => item.partnerId);
    const partnerIdPlaceholders = buildInClausePlaceholders(partnerIds, 1);
    const existingResult = await client.query(
      `SELECT partner_id FROM obus_merkez_branches WHERE partner_id IN (${partnerIdPlaceholders})`,
      partnerIds
    );
    const existingSet = new Set(
      (existingResult.rows || []).map((row) => String(row?.partner_id || "").trim()).filter(Boolean)
    );

    let insertedCount = 0;
    let updatedCount = 0;

    for (const row of normalizedRows) {
      const updateResult = await client.query(
        `
          UPDATE obus_merkez_branches
          SET branch_id = $2,
              name = $3,
              source_cluster = $4,
              updated_at = now()
          WHERE partner_id = $1
        `,
        [row.partnerId, row.branchId, row.name, row.cluster || null]
      );
      if (updateResult.rowCount) {
        updatedCount += 1;
        continue;
      }
      await client.query(
        `
          INSERT INTO obus_merkez_branches (partner_id, branch_id, name, source_cluster, updated_at)
          VALUES ($1, $2, $3, $4, now())
        `,
        [row.partnerId, row.branchId, row.name, row.cluster || null]
      );
      if (existingSet.has(row.partnerId)) updatedCount += 1;
      else insertedCount += 1;
    }

    await client.query("COMMIT");

    return {
      savedCount: normalizedRows.length,
      insertedCount,
      updatedCount,
      error: null
    };
  } catch (err) {
    await client.query("ROLLBACK");
    return {
      savedCount: 0,
      insertedCount: 0,
      updatedCount: 0,
      error: `SQL kayıt hatası: ${err?.message || "Bilinmeyen hata"}`
    };
  } finally {
    client.release();
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

function getIsoDateDaysAgo(days = 0) {
  const parsedDays = Number.parseInt(days, 10);
  const safeDays = Number.isFinite(parsedDays) ? Math.max(0, Math.min(parsedDays, 3650)) : 0;
  const date = new Date();
  date.setDate(date.getDate() - safeDays);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeJiraBaseUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const withProtocol = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
  try {
    const parsed = new URL(withProtocol);
    const cleanPath = parsed.pathname.replace(/\/+$/, "");
    return `${parsed.origin}${cleanPath}`;
  } catch (err) {
    return "";
  }
}

function normalizeJiraProjectKey(value) {
  const raw = String(value || "")
    .trim()
    .toUpperCase();
  if (!raw) return "";
  if (!/^[A-Z][A-Z0-9_-]{0,49}$/.test(raw)) return "";
  return raw;
}

function buildJiraJql({ projectKey = "", startDate = "", endDate = "", customJql = "" } = {}) {
  const custom = String(customJql || "").trim();
  if (custom) return custom;

  const clauses = [];
  if (projectKey) clauses.push(`project = "${projectKey}"`);
  if (startDate) clauses.push(`updated >= "${startDate}"`);
  if (endDate) clauses.push(`updated <= "${endDate}"`);

  if (!clauses.length) return "ORDER BY updated DESC";
  return `${clauses.join(" AND ")} ORDER BY updated DESC`;
}

function formatJiraDateTime(value) {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString("tr-TR");
}

function extractJiraErrorDetails(payload) {
  if (!payload || typeof payload !== "object") return "";
  const messages = [];

  if (Array.isArray(payload.errorMessages)) {
    payload.errorMessages.forEach((item) => {
      const text = String(item || "").trim();
      if (text) messages.push(text);
    });
  }

  if (payload.errors && typeof payload.errors === "object") {
    Object.values(payload.errors).forEach((item) => {
      const text = String(item || "").trim();
      if (text) messages.push(text);
    });
  }

  return messages.join(" | ");
}

function buildJiraIssueBrowseUrl(baseUrl, key) {
  const normalizedBaseUrl = normalizeJiraBaseUrl(baseUrl);
  const normalizedKey = String(key || "").trim();
  if (!normalizedBaseUrl || !normalizedKey) return "";
  return `${normalizedBaseUrl}/browse/${encodeURIComponent(normalizedKey)}`;
}

function buildJiraAuthValue(email, apiToken) {
  const normalizedEmail = String(email || "").trim();
  const normalizedApiToken = String(apiToken || "").trim();
  if (!normalizedEmail || !normalizedApiToken) return "";
  return Buffer.from(`${normalizedEmail}:${normalizedApiToken}`, "utf8").toString("base64");
}

function normalizeJiraEpicColorKey(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return /^color_\d+$/.test(normalized) ? normalized : "";
}

function resolveJiraEpicPalette(colorKey) {
  const normalizedColorKey = normalizeJiraEpicColorKey(colorKey);
  const paletteMap = {
    color_1: { backgroundColor: "#FFEBE6", borderColor: "#FFBDAD", textColor: "#5D1F1A" },
    color_2: { backgroundColor: "#FFF3EB", borderColor: "#FEC195", textColor: "#702E00" },
    color_3: { backgroundColor: "#FFF7D6", borderColor: "#F5CD47", textColor: "#533F04" },
    color_4: { backgroundColor: "#E3FCEF", borderColor: "#79F2C0", textColor: "#164B35" },
    color_5: { backgroundColor: "#E6FCFF", borderColor: "#79E2F2", textColor: "#0C5460" },
    color_6: { backgroundColor: "#DEEBFF", borderColor: "#B3D4FF", textColor: "#0747A6" },
    color_7: { backgroundColor: "#EAE6FF", borderColor: "#C0B6F2", textColor: "#403294" },
    color_8: { backgroundColor: "#FFECF8", borderColor: "#F797D2", textColor: "#943D73" },
    color_9: { backgroundColor: "#F4F5F7", borderColor: "#DFE1E6", textColor: "#172B4D" }
  };
  return paletteMap[normalizedColorKey] || paletteMap.color_9;
}

function getDefaultJiraLinkPalette() {
  return {
    backgroundColor: "#E3FCEF",
    borderColor: "#79F2C0",
    textColor: "#164B35"
  };
}

function resolveJiraCardLinkBadge(issueType, epicColorKey) {
  const normalizedType = String(issueType || "").trim().toLowerCase();
  if (normalizedType === "epic") {
    if (!normalizeJiraEpicColorKey(epicColorKey)) {
      return {
        colorKey: "",
        ...getDefaultJiraLinkPalette()
      };
    }
    return {
      colorKey: normalizeJiraEpicColorKey(epicColorKey),
      ...resolveJiraEpicPalette(epicColorKey)
    };
  }

  return {
    colorKey: "",
    ...getDefaultJiraLinkPalette()
  };
}

function parseJiraVersionParts(value) {
  const text = String(value || "").trim();
  if (!text) return [];
  const match = text.match(/(\d+)(?:\.(\d+))?(?:\.(\d+))?/);
  if (!match) return [];
  return match
    .slice(1)
    .filter((item) => item !== undefined)
    .map((item) => Number.parseInt(item, 10))
    .filter((item) => Number.isFinite(item));
}

function compareJiraVersionParts(left, right) {
  const maxLength = Math.max(left.length, right.length);
  for (let index = 0; index < maxLength; index += 1) {
    const leftValue = Number.isFinite(left[index]) ? left[index] : -1;
    const rightValue = Number.isFinite(right[index]) ? right[index] : -1;
    if (leftValue !== rightValue) {
      return rightValue - leftValue;
    }
  }
  return 0;
}

function getJiraCardLinkVersionParts(cardLinks) {
  const links = Array.isArray(cardLinks) ? cardLinks : [];
  const candidates = links
    .map((link) => parseJiraVersionParts(link?.summary))
    .filter((parts) => parts.length > 0);
  if (!candidates.length) return [];
  return candidates.sort(compareJiraVersionParts)[0];
}

function hasJiraEpicCardLink(card) {
  return (Array.isArray(card?.cardLinks) ? card.cardLinks : []).some((link) =>
    /^epic$/i.test(String(link?.issueType || "").trim())
  );
}

function getPrimaryJiraCardLink(card) {
  const links = Array.isArray(card?.cardLinks) ? card.cardLinks : [];
  if (!links.length) return null;
  const epicLinks = links.filter((link) => /^epic$/i.test(String(link?.issueType || "").trim()));
  const candidateLinks = epicLinks.length > 0 ? epicLinks : links;
  return [...candidateLinks].sort((left, right) => {
    const leftSummary = String(left?.summary || left?.key || "").trim();
    const rightSummary = String(right?.summary || right?.key || "").trim();
    const summaryCompare = leftSummary.localeCompare(rightSummary, "tr", { sensitivity: "base" });
    if (summaryCompare !== 0) return summaryCompare;
    return String(left?.key || "").localeCompare(String(right?.key || ""), "tr");
  })[0];
}

function sortJiraBoardCards(cards, options = {}) {
  const epicFirst = Boolean(options?.epicFirst);
  return [...(Array.isArray(cards) ? cards : [])].sort((left, right) => {
    if (epicFirst) {
      const leftEpicRank = hasJiraEpicCardLink(left) ? 1 : 0;
      const rightEpicRank = hasJiraEpicCardLink(right) ? 1 : 0;
      if (leftEpicRank !== rightEpicRank) {
        return rightEpicRank - leftEpicRank;
      }
    }

    const leftPrimaryLink = getPrimaryJiraCardLink(left);
    const rightPrimaryLink = getPrimaryJiraCardLink(right);
    const leftPrimaryName = String(leftPrimaryLink?.summary || leftPrimaryLink?.key || "").trim();
    const rightPrimaryName = String(rightPrimaryLink?.summary || rightPrimaryLink?.key || "").trim();
    if (leftPrimaryName || rightPrimaryName) {
      if (!leftPrimaryName) return 1;
      if (!rightPrimaryName) return -1;
      const primaryNameCompare = leftPrimaryName.localeCompare(rightPrimaryName, "tr", { sensitivity: "base" });
      if (primaryNameCompare !== 0) return primaryNameCompare;
    }

    const versionCompare = compareJiraVersionParts(
      getJiraCardLinkVersionParts(left?.cardLinks),
      getJiraCardLinkVersionParts(right?.cardLinks)
    );
    if (versionCompare !== 0) return versionCompare;

    const leftUpdated = Date.parse(String(left?.updatedAt || ""));
    const rightUpdated = Date.parse(String(right?.updatedAt || ""));
    if (Number.isFinite(leftUpdated) && Number.isFinite(rightUpdated) && leftUpdated !== rightUpdated) {
      return rightUpdated - leftUpdated;
    }

    return String(left?.key || "").localeCompare(String(right?.key || ""), "tr");
  });
}

function normalizeJiraLinkedIssue(issue, baseUrl) {
  if (!issue || typeof issue !== "object") return null;
  const key = String(issue.key || "").trim();
  if (!key) return null;
  const fields = issue.fields && typeof issue.fields === "object" ? issue.fields : {};
  const issueType = fields.issuetype && typeof fields.issuetype === "object" ? fields.issuetype : null;

  return {
    key,
    summary: String(fields.summary || "").trim() || "-",
    issueType: String(issueType?.name || "").trim() || "-",
    issueUrl: buildJiraIssueBrowseUrl(baseUrl, key)
  };
}

function extractRelevantJiraIssueLinks(links, baseUrl) {
  const items = [];
  const seen = new Set();
  (Array.isArray(links) ? links : []).forEach((link) => {
    if (!link || typeof link !== "object") return;
    const candidate = normalizeJiraLinkedIssue(link.outwardIssue || link.inwardIssue, baseUrl);
    if (!candidate) return;
    const normalizedType = String(candidate.issueType || "").trim().toLowerCase();
    if (normalizedType !== "epic" && normalizedType !== "task") return;
    if (seen.has(candidate.key)) return;
    seen.add(candidate.key);
    items.push(candidate);
  });
  return items;
}

async function fetchJiraEpicMeta({ baseUrl, email, apiToken, epicKey }) {
  const normalizedBaseUrl = normalizeJiraBaseUrl(baseUrl);
  const normalizedEpicKey = String(epicKey || "").trim();
  const authValue = buildJiraAuthValue(email, apiToken);
  if (!normalizedBaseUrl || !normalizedEpicKey || !authValue) {
    return {
      key: normalizedEpicKey,
      colorKey: ""
    };
  }

  const cached = jiraEpicMetaCache.get(normalizedEpicKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.value;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(3000, JIRA_API_TIMEOUT_MS));

  try {
    const url = `${normalizedBaseUrl}/rest/agile/1.0/epic/${encodeURIComponent(normalizedEpicKey)}`;
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        Authorization: `Basic ${authValue}`
      }
    });
    const raw = await response.text();
    const payload = parseJsonSafe(raw);
    const value = {
      key: normalizedEpicKey,
      colorKey: normalizeJiraEpicColorKey(payload?.color?.key)
    };

    if (response.ok) {
      jiraEpicMetaCache.set(normalizedEpicKey, {
        value,
        expiresAt: Date.now() + JIRA_EPIC_CACHE_TTL_MS
      });
      return value;
    }
  } catch (_err) {
    return {
      key: normalizedEpicKey,
      colorKey: ""
    };
  } finally {
    clearTimeout(timeout);
  }

  return {
    key: normalizedEpicKey,
    colorKey: ""
  };
}

async function enrichJiraIssueLinksWithBadges({ issues, baseUrl, email, apiToken }) {
  const issueList = Array.isArray(issues) ? issues : [];
  if (!issueList.length) return issueList;

  const epicKeys = Array.from(
    new Set(
      issueList
        .flatMap((issue) => (Array.isArray(issue?.cardLinks) ? issue.cardLinks : []))
        .filter((link) => /^epic$/i.test(String(link?.issueType || "").trim()))
        .map((link) => String(link.key || "").trim())
        .filter(Boolean)
    )
  );

  const epicMetaMap = new Map();
  await Promise.all(
    epicKeys.map(async (epicKey) => {
      const meta = await fetchJiraEpicMeta({
        baseUrl,
        email,
        apiToken,
        epicKey
      });
      epicMetaMap.set(epicKey, meta);
    })
  );

  return issueList.map((issue) => {
    const cardLinks = Array.isArray(issue?.cardLinks) ? issue.cardLinks : [];
    return {
      ...issue,
      cardLinks: cardLinks.map((link) => {
        const epicMeta = /^epic$/i.test(String(link?.issueType || "").trim())
          ? epicMetaMap.get(String(link.key || "").trim())
          : null;
        const badge = resolveJiraCardLinkBadge(link?.issueType, epicMeta?.colorKey);
        return {
          ...link,
          epicColorKey: epicMeta?.colorKey || "",
          badgeBackgroundColor: badge.backgroundColor,
          badgeBorderColor: badge.borderColor,
          badgeTextColor: badge.textColor
        };
      })
    };
  });
}

function normalizeJiraIssue(issue, baseUrl) {
  if (!issue || typeof issue !== "object") return null;
  const key = String(issue.key || "").trim();
  if (!key) return null;

  const fields = issue.fields && typeof issue.fields === "object" ? issue.fields : {};
  const issueType = fields.issuetype && typeof fields.issuetype === "object" ? fields.issuetype : null;
  const status = fields.status && typeof fields.status === "object" ? fields.status : null;
  const priority = fields.priority && typeof fields.priority === "object" ? fields.priority : null;
  const assignee = fields.assignee && typeof fields.assignee === "object" ? fields.assignee : null;
  const parent = fields.parent && typeof fields.parent === "object" ? fields.parent : null;
  const parentIssue = normalizeJiraLinkedIssue(parent, baseUrl);
  const relatedIssues = extractRelevantJiraIssueLinks(fields.issuelinks, baseUrl);
  const cardLinks = [];
  if (parentIssue) {
    cardLinks.push({
      label: /^epic$/i.test(parentIssue.issueType) ? "Epic" : parentIssue.issueType,
      ...parentIssue
    });
  }
  relatedIssues.forEach((item) => {
    if (cardLinks.some((link) => link.key === item.key)) return;
    cardLinks.push({
      label: item.issueType,
      ...item
    });
  });

  return {
    key,
    summary: String(fields.summary || "").trim() || "-",
    issueType: String(issueType?.name || "").trim() || "-",
    status: String(status?.name || "").trim() || "-",
    priority: String(priority?.name || "").trim() || "-",
    assignee: String(assignee?.displayName || assignee?.emailAddress || "").trim() || "Atanmamış",
    createdAt: formatJiraDateTime(fields.created),
    updatedAt: formatJiraDateTime(fields.updated),
    resolvedAt: formatJiraDateTime(fields.resolutiondate),
    issueUrl: buildJiraIssueBrowseUrl(baseUrl, key),
    cardLinks
  };
}

function escapeJiraJqlValue(value) {
  return String(value || "").replace(/\\/g, "\\\\").replace(/"/g, '\\"').trim();
}

function buildJiraBoardCardFromIssue(issue) {
  if (!issue || typeof issue !== "object") return null;
  const key = String(issue.key || "").trim();
  const title = String(issue.summary || "").trim();
  if (!key || !title) return null;
  const assignee = String(issue.assignee || "Atanmamış").trim() || "Atanmamış";
  const assigneeInitials = assignee
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => String(part[0] || "").toUpperCase())
    .join("") || "?";

  return {
    key,
    title,
    summary: String(issue.summary || "-").trim() || "-",
    issueType: String(issue.issueType || "-").trim() || "-",
    priority: String(issue.priority || "-").trim() || "-",
    assignee,
    assigneeInitials,
    status: String(issue.status || "-").trim() || "-",
    updatedAt: String(issue.updatedAt || "-").trim() || "-",
    issueUrl: String(issue.issueUrl || "").trim(),
    cardLinks: Array.isArray(issue.cardLinks) ? issue.cardLinks : [],
    blocked: /^block/i.test(String(issue.status || "").trim())
  };
}

async function fetchJiraBoardCards({
  baseUrl,
  email,
  apiToken,
  projectKey,
  statuses = [],
  issueType = "Task",
  maxResults = JIRA_MAX_RESULTS,
  sortOptions = {}
}) {
  const normalizedProjectKey = normalizeJiraProjectKey(projectKey);
  if (!normalizedProjectKey) {
    return {
      cards: [],
      jql: "",
      source: "Jira API",
      error: "Proje anahtarı geçersiz."
    };
  }

  const statusList = Array.isArray(statuses)
    ? statuses.map((item) => escapeJiraJqlValue(item)).filter(Boolean)
    : [];
  const issueTypeValue = escapeJiraJqlValue(issueType);

  const clauses = [`project = "${normalizedProjectKey}"`];
  if (issueTypeValue) {
    clauses.push(`issuetype = "${issueTypeValue}"`);
  }
  if (statusList.length > 0) {
    clauses.push(`status in (${statusList.map((item) => `"${item}"`).join(", ")})`);
  }
  const jql = `${clauses.join(" AND ")} ORDER BY updated DESC`;

  const jiraResult = await fetchAllJiraIssues({
    baseUrl,
    email,
    apiToken,
    jql,
    pageSize: Math.min(200, Math.max(1, Number.parseInt(maxResults, 10) || JIRA_MAX_RESULTS)),
    maxItems: JIRA_BOARD_MAX_ITEMS
  });
  const issuesWithBadges = await enrichJiraIssueLinksWithBadges({
    issues: jiraResult.issues,
    baseUrl,
    email,
    apiToken
  });

  return {
    cards: sortJiraBoardCards(issuesWithBadges.map(buildJiraBoardCardFromIssue).filter(Boolean), sortOptions),
    jql,
    source: String(jiraResult.source || "Jira API"),
    error: jiraResult.error || null
  };
}

async function fetchAllJiraIssues({
  baseUrl,
  email,
  apiToken,
  jql,
  pageSize = 200,
  maxItems = JIRA_BOARD_MAX_ITEMS
}) {
  const normalizedPageSize = toBoundedInt(pageSize, 200, 1, 200);
  const normalizedMaxItems = toBoundedInt(maxItems, JIRA_BOARD_MAX_ITEMS, 1, 5000);
  const issues = [];
  const seenKeys = new Set();
  let total = 0;
  let startAt = 0;
  let source = "Jira API";

  while (issues.length < normalizedMaxItems) {
    const remaining = normalizedMaxItems - issues.length;
    const pageResult = await fetchJiraIssues({
      baseUrl,
      email,
      apiToken,
      jql,
      maxResults: Math.min(normalizedPageSize, remaining),
      startAt
    });

    source = String(pageResult.source || "Jira API");
    if (pageResult.error) {
      return {
        issues,
        total,
        startAt,
        maxResults: normalizedPageSize,
        error: pageResult.error,
        source
      };
    }

    const pageIssues = Array.isArray(pageResult.issues) ? pageResult.issues : [];
    total = Number.isFinite(Number(pageResult.total)) ? Number(pageResult.total) : total;
    pageIssues.forEach((issue) => {
      const key = String(issue?.key || "").trim();
      if (!key || seenKeys.has(key)) return;
      seenKeys.add(key);
      issues.push(issue);
    });

    if (!pageIssues.length) break;
    startAt = Number.isFinite(Number(pageResult.startAt))
      ? Number(pageResult.startAt) + pageIssues.length
      : startAt + pageIssues.length;

    if (total > 0 && startAt >= total) break;
  }

  return {
    issues,
    total: total || issues.length,
    startAt: 0,
    maxResults: normalizedPageSize,
    error: null,
    source
  };
}

async function fetchJiraIssues({ baseUrl, email, apiToken, jql, maxResults = JIRA_MAX_RESULTS, startAt = 0 }) {
  const normalizedBaseUrl = normalizeJiraBaseUrl(baseUrl);
  const normalizedEmail = String(email || "").trim();
  const normalizedApiToken = String(apiToken || "").trim();
  const normalizedJql = String(jql || "").trim();
  const normalizedMaxResults = toBoundedInt(maxResults, JIRA_MAX_RESULTS, 1, 200);
  const normalizedStartAt = Math.max(0, Number.parseInt(startAt, 10) || 0);

  if (!normalizedBaseUrl || !normalizedEmail || !normalizedApiToken) {
    return {
      issues: [],
      total: 0,
      startAt: normalizedStartAt,
      maxResults: normalizedMaxResults,
      error: "Jira bağlantı bilgileri eksik.",
      source: "Jira API"
    };
  }

  if (!normalizedJql) {
    return {
      issues: [],
      total: 0,
      startAt: normalizedStartAt,
      maxResults: normalizedMaxResults,
      error: "JQL sorgusu boş.",
      source: "Jira API"
    };
  }

  const url = `${normalizedBaseUrl}/rest/api/3/search/jql`;
  const authValue = buildJiraAuthValue(normalizedEmail, normalizedApiToken);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(3000, JIRA_API_TIMEOUT_MS));

  try {
    const requestBody = {
      jql: normalizedJql,
      maxResults: normalizedMaxResults,
      fields: [
        "summary",
        "status",
        "assignee",
        "priority",
        "issuetype",
        "created",
        "updated",
        "resolutiondate",
        "parent",
        "issuelinks"
      ]
    };

    const response = await fetch(url, {
      method: "POST",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: `Basic ${authValue}`
      },
      body: JSON.stringify(requestBody)
    });
    const raw = await response.text();
    const payload = parseJsonSafe(raw);

    if (!response.ok) {
      const apiMessage = extractJiraErrorDetails(payload);
      const reason = apiMessage || response.statusText || "Bilinmeyen hata";
      return {
        issues: [],
        total: 0,
        startAt: normalizedStartAt,
        maxResults: normalizedMaxResults,
        error: `Jira API HTTP ${response.status}: ${reason}`,
        source: "Jira API"
      };
    }

    if (!payload || typeof payload !== "object") {
      return {
        issues: [],
        total: 0,
        startAt: normalizedStartAt,
        maxResults: normalizedMaxResults,
        error: "Jira API JSON parse edilemedi.",
        source: "Jira API"
      };
    }

    const issues = (Array.isArray(payload.issues) ? payload.issues : [])
      .map((item) => normalizeJiraIssue(item, normalizedBaseUrl))
      .filter(Boolean);

    return {
      issues,
      total: Number.isFinite(Number(payload.total)) ? Number(payload.total) : issues.length,
      startAt: Number.isFinite(Number(payload.startAt)) ? Number(payload.startAt) : normalizedStartAt,
      maxResults: Number.isFinite(Number(payload.maxResults)) ? Number(payload.maxResults) : normalizedMaxResults,
      error: null,
      source: "Jira API"
    };
  } catch (err) {
    if (err?.name === "AbortError") {
      return {
        issues: [],
        total: 0,
        startAt: normalizedStartAt,
        maxResults: normalizedMaxResults,
        error: `Jira API timeout (${Math.round(Math.max(3000, JIRA_API_TIMEOUT_MS) / 1000)}s).`,
        source: "Jira API"
      };
    }
    return {
      issues: [],
      total: 0,
      startAt: normalizedStartAt,
      maxResults: normalizedMaxResults,
      error: `Jira API isteği başarısız: ${err?.message || "Bilinmeyen hata"}`,
      source: "Jira API"
    };
  } finally {
    clearTimeout(timeout);
  }
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

async function slackApiPost(method, token, payload = {}, retryCount = 0) {
  const url = `${SLACK_API_BASE_URL}/${method}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, SLACK_API_TIMEOUT_MS));

  try {
    const response = await fetch(url, {
      method: "POST",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload && typeof payload === "object" ? payload : {})
    });

    if (response.status === 429) {
      if (retryCount >= SLACK_MAX_RATE_LIMIT_RETRY) {
        throw new Error(`${method} HTTP 429 (max retry aşıldı)`);
      }
      const retryAfterRaw = response.headers.get("retry-after");
      const retryAfterSeconds = Number.parseInt(retryAfterRaw || "1", 10);
      const waitMs = (Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : 1) * 1000 + 250;
      await sleep(waitMs);
      return slackApiPost(method, token, payload, retryCount + 1);
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

function normalizeSlackChannelLabel(value) {
  return String(value || "").trim();
}

function normalizeSlackUserLookupValue(value) {
  return String(value || "")
    .replace(/^<@([UW][A-Z0-9]+)(?:\|[^>]+)?>$/i, "$1")
    .replace(/^@+/, "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .toLocaleLowerCase("tr");
}

function isLikelySlackUserId(value) {
  return /^[uw][a-z0-9]{8,}$/i.test(String(value || "").trim());
}

function parseSlackMentionTargets(raw) {
  return String(raw || "")
    .split(/[\r\n,;]+/)
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function dedupeStringList(items = []) {
  const seen = new Set();
  const results = [];
  (Array.isArray(items) ? items : []).forEach((item) => {
    const normalized = String(item || "").trim();
    if (!normalized) return;
    const key = normalized.toLocaleLowerCase("tr");
    if (seen.has(key)) return;
    seen.add(key);
    results.push(normalized);
  });
  return results;
}

function buildSlackMentionFallback(rawTarget) {
  const normalized = String(rawTarget || "").trim();
  if (!normalized) return "";
  if (normalized.startsWith("@") || normalized.startsWith("<@")) return normalized;
  return `@${normalized}`;
}

function buildSlackUserLookupKeys(member) {
  const profile = member && typeof member.profile === "object" ? member.profile : {};
  const values = [
    member?.id,
    member?.name,
    profile.display_name,
    profile.display_name_normalized,
    profile.real_name,
    profile.real_name_normalized,
    profile.email
  ];
  const expanded = [];
  values.forEach((value) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    expanded.push(normalized);
    const roleTrimmed = normalized.split(/\s+-\s+/)[0]?.trim();
    if (roleTrimmed && roleTrimmed !== normalized) {
      expanded.push(roleTrimmed);
    }
  });
  return dedupeStringList(expanded).map((value) => normalizeSlackUserLookupValue(value)).filter(Boolean);
}

function buildSlackUserLookup(members = []) {
  const byId = new Map();
  const byKey = new Map();
  (Array.isArray(members) ? members : []).forEach((member) => {
    const memberId = String(member?.id || "").trim().toUpperCase();
    if (!memberId) return;
    if (!byId.has(memberId)) {
      byId.set(memberId, member);
    }
    buildSlackUserLookupKeys(member).forEach((key) => {
      if (!byKey.has(key)) {
        byKey.set(key, member);
      }
    });
  });
  return {
    byId,
    byKey
  };
}

async function listSlackUsers(token) {
  const members = [];
  let cursor = "";

  do {
    const data = await slackApiGet("users.list", token, {
      limit: SLACK_DEFAULT_LIMIT,
      cursor
    });

    (Array.isArray(data.members) ? data.members : []).forEach((member) => {
      if (!member || typeof member !== "object") return;
      if (!String(member.id || "").trim()) return;
      if (member.deleted === true) return;
      members.push(member);
    });

    cursor = String(data.response_metadata?.next_cursor || "").trim();
  } while (cursor);

  return members;
}

async function getSlackUserLookup(token) {
  if (slackUserLookupCache.value && Number(slackUserLookupCache.expiresAt || 0) > Date.now()) {
    return slackUserLookupCache.value;
  }

  const members = await listSlackUsers(token);
  const lookup = buildSlackUserLookup(members);
  slackUserLookupCache.value = lookup;
  slackUserLookupCache.expiresAt = Date.now() + Math.max(1000, SLACK_ANALYSIS_CACHE_TTL_MS);
  return lookup;
}

function resolveSlackMentionFromLookup(lookup, rawTarget, lookupError = null) {
  const normalizedTarget = String(rawTarget || "").trim();
  const fallbackText = buildSlackMentionFallback(normalizedTarget);
  if (!normalizedTarget) {
    return {
      rawTarget: normalizedTarget,
      mentionText: "",
      resolved: false,
      userId: "",
      error: lookupError ? summarizeErrorMessage(lookupError) : ""
    };
  }

  const explicitMentionMatch = normalizedTarget.match(/^<@([UW][A-Z0-9]+)(?:\|[^>]+)?>$/i);
  if (explicitMentionMatch?.[1]) {
    const userId = String(explicitMentionMatch[1] || "").trim().toUpperCase();
    return {
      rawTarget: normalizedTarget,
      mentionText: `<@${userId}>`,
      resolved: true,
      userId,
      error: ""
    };
  }

  const directId = normalizedTarget.replace(/^@+/, "").trim().toUpperCase();
  if (isLikelySlackUserId(directId)) {
    return {
      rawTarget: normalizedTarget,
      mentionText: `<@${directId}>`,
      resolved: true,
      userId: directId,
      error: ""
    };
  }

  if (!lookup || !(lookup.byKey instanceof Map)) {
    return {
      rawTarget: normalizedTarget,
      mentionText: fallbackText,
      resolved: false,
      userId: "",
      error: lookupError ? summarizeErrorMessage(lookupError) : ""
    };
  }

  const normalizedLookupKey = normalizeSlackUserLookupValue(normalizedTarget);
  const matchedMember = normalizedLookupKey ? lookup.byKey.get(normalizedLookupKey) : null;
  const matchedUserId = String(matchedMember?.id || "").trim().toUpperCase();
  if (!matchedUserId) {
    return {
      rawTarget: normalizedTarget,
      mentionText: fallbackText,
      resolved: false,
      userId: "",
      error: lookupError ? summarizeErrorMessage(lookupError) : ""
    };
  }

  return {
    rawTarget: normalizedTarget,
    mentionText: `<@${matchedUserId}>`,
    resolved: true,
    userId: matchedUserId,
    error: ""
  };
}

async function resolveSlackMentions(token, rawTargets = []) {
  const uniqueTargets = dedupeStringList(rawTargets);
  if (!uniqueTargets.length) return [];

  let lookup = null;
  let lookupError = null;
  try {
    lookup = await getSlackUserLookup(token);
  } catch (err) {
    lookupError = err;
  }

  return uniqueTargets.map((target) => resolveSlackMentionFromLookup(lookup, target, lookupError));
}

function isLikelySlackChannelId(value) {
  return /^[cg][a-z0-9]{8,}$/i.test(String(value || "").trim());
}

function parseSlackChannelFilter(raw) {
  const parsed = String(raw || "")
    .split(",")
    .map((item) => normalizeSlackChannelLabel(item).toLowerCase())
    .filter(Boolean);
  return new Set(parsed);
}

function getPreferredSlackChannelFilterLabels(filterSet, channels = []) {
  const values = Array.from(filterSet.values())
    .map((item) => normalizeSlackChannelLabel(item))
    .filter(Boolean);
  if (!values.length) return [];

  const channelNameById = new Map();
  const channelNameByLower = new Map();
  (Array.isArray(channels) ? channels : []).forEach((channel) => {
    const channelId = normalizeSlackChannelLabel(channel?.id).toLowerCase();
    const channelName = normalizeSlackChannelLabel(channel?.name);
    if (channelId && channelName) {
      channelNameById.set(channelId, channelName);
    }
    if (channelName) {
      channelNameByLower.set(channelName.toLowerCase(), channelName);
    }
  });

  const seen = new Set();
  const preferredLabels = [];
  values.forEach((value) => {
    const lowerValue = value.toLowerCase();
    const resolvedLabel = normalizeSlackChannelLabel(
      (isLikelySlackChannelId(value) ? channelNameById.get(lowerValue) : null)
      || channelNameByLower.get(lowerValue)
      || value
    );
    if (!resolvedLabel) return;
    const key = resolvedLabel.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    preferredLabels.push(resolvedLabel);
  });

  return preferredLabels;
}

const SLACK_CORP_REQUEST_CHANNEL_FILTER = parseSlackChannelFilter(SLACK_CORP_REQUEST_CHANNELS_RAW);
const SLACK_ANALYSIS_TARGET_CHANNEL_FILTER = parseSlackChannelFilter(SLACK_ANALYSIS_TARGET_CHANNELS_RAW);

function getMandatorySlackChannelColumns(channels = []) {
  const mergedFilter = new Set([
    ...Array.from(SLACK_CORP_REQUEST_CHANNEL_FILTER.values()),
    ...SLACK_REQUIRED_CHANNEL_COLUMNS.map((item) => normalizeSlackChannelLabel(item).toLowerCase()).filter(Boolean)
  ]);
  return getPreferredSlackChannelFilterLabels(mergedFilter, channels);
}

function shouldApplyCorpRequestRuleForChannel(channelId, channelName) {
  if (!SLACK_CORP_REQUEST_CHANNEL_FILTER.size) return true;
  const id = normalizeSlackChannelLabel(channelId).toLowerCase();
  const name = normalizeSlackChannelLabel(channelName).toLowerCase();
  return SLACK_CORP_REQUEST_CHANNEL_FILTER.has(id) || SLACK_CORP_REQUEST_CHANNEL_FILTER.has(name);
}

function shouldAnalyzeSlackChannel(channelId, channelName) {
  if (!SLACK_ANALYSIS_TARGET_CHANNEL_FILTER.size) return true;
  const id = normalizeSlackChannelLabel(channelId).toLowerCase();
  const name = normalizeSlackChannelLabel(channelName).toLowerCase();
  return SLACK_ANALYSIS_TARGET_CHANNEL_FILTER.has(id) || SLACK_ANALYSIS_TARGET_CHANNEL_FILTER.has(name);
}

function messageContainsCorpRequestTag(message, tagValue = SLACK_CORP_REQUEST_TAG) {
  const tag = String(tagValue || "").trim().toLowerCase();
  if (!tag) return false;
  const text = String(message?.text || "").trim().toLowerCase();
  if (!text) return false;
  return text.includes(tag);
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
  const version = "v7";
  const channelTypes = normalizeSlackChannelTypes(SLACK_ANALYSIS_CHANNEL_TYPES_RAW);
  const corpChannels = Array.from(SLACK_CORP_REQUEST_CHANNEL_FILTER.values()).sort((a, b) => a.localeCompare(b, "tr"));
  const targetChannels = Array.from(SLACK_ANALYSIS_TARGET_CHANNEL_FILTER.values()).sort((a, b) =>
    a.localeCompare(b, "tr")
  );
  const requiredColumns = SLACK_REQUIRED_CHANNEL_COLUMNS.slice().sort((a, b) => a.localeCompare(b, "tr"));
  return [
    version,
    startDate || "",
    endDate || "",
    SLACK_SELECTED_USERS.map((item) => item.id).join(","),
    channelTypes,
    targetChannels.join(","),
    String(SLACK_ANALYSIS_MAX_CHANNELS || ""),
    String(SLACK_CORP_REQUEST_TAG || ""),
    corpChannels.join(","),
    requiredColumns.join(",")
  ].join("__");
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

function cleanupObusLiveJobs() {
  const now = Date.now();
  for (const [jobId, job] of obusLiveJobs.entries()) {
    if (!job || typeof job !== "object") {
      obusLiveJobs.delete(jobId);
      continue;
    }
    const expiresAt = Number(job.expiresAt || 0);
    if (Number.isFinite(expiresAt) && expiresAt > 0 && expiresAt <= now) {
      obusLiveJobs.delete(jobId);
    }
  }
}

function createObusLiveJob({ type, ownerUserId, totalCount = 0 }) {
  cleanupObusLiveJobs();
  const createdAt = Date.now();
  const safeTotalCount = Number.isFinite(Number(totalCount)) ? Math.max(0, Number(totalCount)) : 0;
  const jobId = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  const job = {
    id: jobId,
    type: String(type || "").trim() || "obus-action",
    ownerUserId: Number(ownerUserId) || 0,
    totalCount: safeTotalCount,
    processedCount: 0,
    successCount: 0,
    failureCount: 0,
    done: false,
    error: null,
    createdAt,
    updatedAt: createdAt,
    finishedAt: null,
    expiresAt: createdAt + Math.max(60000, OBUS_LIVE_JOB_TTL_MS),
    nextSeq: 1,
    events: []
  };
  obusLiveJobs.set(jobId, job);
  return job;
}

function pushObusLiveJobEvent(job, event) {
  if (!job || typeof job !== "object") return null;
  const normalizedEvent = event && typeof event === "object" ? event : {};
  const now = Date.now();
  const seq = Number(job.nextSeq || 1);
  const rawStatusKind = String(normalizedEvent.statusKind || "").trim().toLocaleLowerCase("tr");
  const inferredStatusKind =
    rawStatusKind === "progress" ||
    rawStatusKind === "info" ||
    rawStatusKind === "pending" ||
    rawStatusKind === "missing" ||
    rawStatusKind === "existing"
      ? rawStatusKind
      : normalizedEvent.ok === true
        ? "success"
        : "failure";
  const finalize =
    Object.prototype.hasOwnProperty.call(normalizedEvent, "finalize")
      ? normalizedEvent.finalize === true
      : inferredStatusKind === "success" ||
          inferredStatusKind === "failure" ||
          inferredStatusKind === "missing" ||
          inferredStatusKind === "existing";
  const ok =
    typeof normalizedEvent.ok === "boolean"
      ? normalizedEvent.ok
      : inferredStatusKind === "failure"
        ? false
        : inferredStatusKind === "success" || inferredStatusKind === "missing" || inferredStatusKind === "existing"
          ? true
          : null;
  const record = {
    seq,
    key: String(normalizedEvent.key || "").trim(),
    label: String(normalizedEvent.label || "").trim(),
    ok,
    finalize,
    statusKind: inferredStatusKind,
    message: String(normalizedEvent.message || "").trim(),
    error: String(normalizedEvent.error || "").trim(),
    errorDetail: String(normalizedEvent.errorDetail || "").trim(),
    detailText: String(normalizedEvent.detailText || "").trim(),
    exists:
      normalizedEvent?.exists === true ? true : normalizedEvent?.exists === false ? false : null,
    meta:
      normalizedEvent?.meta && typeof normalizedEvent.meta === "object" && !Array.isArray(normalizedEvent.meta)
        ? normalizedEvent.meta
        : null,
    logLines: (Array.isArray(normalizedEvent.logLines) ? normalizedEvent.logLines : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    updatedAt: now
  };

  job.nextSeq = seq + 1;
  job.events.push(record);
  const maxEvents = Math.max(1000, Number(OBUS_LIVE_JOB_MAX_EVENTS || 10000));
  if (job.events.length > maxEvents) {
    job.events.splice(0, job.events.length - maxEvents);
  }

  if (finalize) {
    if (ok === true) {
      job.successCount += 1;
    } else if (ok === false) {
      job.failureCount += 1;
    }
    job.processedCount = job.successCount + job.failureCount;
  }
  job.updatedAt = now;
  return record;
}

function finishObusLiveJob(job, errorMessage = null) {
  if (!job || typeof job !== "object") return;
  const now = Date.now();
  job.done = true;
  job.error = errorMessage ? String(errorMessage).trim() : null;
  job.finishedAt = now;
  job.updatedAt = now;
  job.expiresAt = now + Math.max(60000, OBUS_LIVE_JOB_TTL_MS);
}

function setObusLiveJobSummary(job, summary = {}) {
  if (!job || typeof job !== "object") return;
  job.summary = summary && typeof summary === "object" ? summary : {};
  job.updatedAt = Date.now();
}

function finalizeObusLiveJobSingleResult(job, ok = true) {
  if (!job || typeof job !== "object") return;
  job.totalCount = Math.max(1, Number(job.totalCount || 0));
  job.processedCount = 1;
  job.successCount = ok ? 1 : 0;
  job.failureCount = ok ? 0 : 1;
  job.updatedAt = Date.now();
}

function readObusLiveJob(jobId, ownerUserId) {
  cleanupObusLiveJobs();
  const job = obusLiveJobs.get(String(jobId || "").trim());
  if (!job) return null;
  if (Number(job.ownerUserId || 0) !== Number(ownerUserId || 0)) return null;
  return job;
}

function readObusLiveJobSnapshot(job, cursor = 0) {
  const safeCursor = Number.isFinite(Number(cursor)) ? Math.max(0, Number(cursor)) : 0;
  const sourceEvents = Array.isArray(job?.events) ? job.events : [];
  let startIndex = sourceEvents.length;
  let low = 0;
  let high = sourceEvents.length - 1;
  while (low <= high) {
    const mid = Math.floor((low + high) / 2);
    const seq = Number(sourceEvents[mid]?.seq || 0);
    if (seq > safeCursor) {
      startIndex = mid;
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  const events = startIndex < sourceEvents.length ? sourceEvents.slice(startIndex) : [];
  const lastSeq = events.length > 0 ? Number(events[events.length - 1]?.seq || safeCursor) : safeCursor;
  return {
    ok: true,
    jobId: String(job?.id || "").trim(),
    type: String(job?.type || "").trim(),
    done: Boolean(job?.done),
    error: String(job?.error || "").trim() || null,
    createdAt: Number.isFinite(Number(job?.createdAt)) ? Number(job.createdAt) : 0,
    updatedAt: Number.isFinite(Number(job?.updatedAt)) ? Number(job.updatedAt) : 0,
    finishedAt: Number.isFinite(Number(job?.finishedAt)) ? Number(job.finishedAt) : 0,
    totalCount: Number.isFinite(Number(job?.totalCount)) ? Number(job.totalCount) : 0,
    processedCount: Number.isFinite(Number(job?.processedCount)) ? Number(job.processedCount) : 0,
    successCount: Number.isFinite(Number(job?.successCount)) ? Number(job.successCount) : 0,
    failureCount: Number.isFinite(Number(job?.failureCount)) ? Number(job.failureCount) : 0,
    summary: job?.summary && typeof job.summary === "object" ? job.summary : null,
    events,
    cursor: lastSeq
  };
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

async function resolveSlackConversationId(token, channelRef, channelTypes = SLACK_ANALYSIS_CHANNEL_TYPES_RAW) {
  const normalizedRef = normalizeSlackChannelLabel(channelRef).replace(/^#/, "");
  if (!normalizedRef) {
    throw new Error("Slack kanal adı gerekli.");
  }

  if (isLikelySlackChannelId(normalizedRef)) {
    return {
      channelId: normalizedRef,
      channelLabel: normalizedRef
    };
  }

  const channels = await listSlackChannelsForAnalysis(token, channelTypes);
  const normalizedLookup = normalizedRef.toLowerCase();
  const matchedChannel = channels.find(
    (channel) => normalizeSlackChannelLabel(channel?.name).toLowerCase() === normalizedLookup
  );
  if (!matchedChannel?.id) {
    throw new Error(`Slack kanalı bulunamadı: ${normalizedRef}`);
  }

  return {
    channelId: String(matchedChannel.id || "").trim(),
    channelLabel: normalizeSlackChannelLabel(matchedChannel.name) || normalizedRef
  };
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
  nameByUserId = new Map(),
  channelReplyByUserId = new Map(),
  channelColumns = []
) {
  const normalizedChannelColumns = (Array.isArray(channelColumns) ? channelColumns : [])
    .map((item) => normalizeSlackChannelLabel(item))
    .filter(Boolean);

  return SLACK_SELECTED_USERS.map((selected) => {
    const channelReplyMapRaw = channelReplyByUserId.get(selected.id);
    const channelReplyMap = channelReplyMapRaw instanceof Map ? channelReplyMapRaw : new Map();
    const channelReplyCounts = {};
    normalizedChannelColumns.forEach((channelName) => {
      channelReplyCounts[channelName] = toCountInteger(channelReplyMap.get(channelName) || 0);
    });

    return {
      userId: selected.id,
      name: String(nameByUserId.get(selected.id) || selected.name || "").trim() || selected.id,
      requestCount: toCountInteger(requestCountByUserId.get(selected.id) || 0),
      replyCount: toCountInteger(replyCountByUserId.get(selected.id) || 0),
      count: toCountInteger(replyCountByUserId.get(selected.id) || 0),
      channelReplyCounts
    };
  }).sort(
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
  const channelReplyByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, new Map()]));
  const channelColumnsSet = new Set();

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

    const rawChannelCounts =
      row?.channelReplyCounts && typeof row.channelReplyCounts === "object"
        ? row.channelReplyCounts
        : row?.channel_reply_counts && typeof row.channel_reply_counts === "object"
          ? row.channel_reply_counts
          : null;

    if (rawChannelCounts) {
      const channelMap = channelReplyByUserId.get(userId) || new Map();
      Object.entries(rawChannelCounts).forEach(([rawChannelName, rawCount]) => {
        const channelName = normalizeSlackChannelLabel(rawChannelName);
        if (!channelName) return;
        channelMap.set(channelName, toCountInteger(rawCount));
        channelColumnsSet.add(channelName);
      });
      channelReplyByUserId.set(userId, channelMap);
    }
  });

  getMandatorySlackChannelColumns().forEach((channelName) => {
    const normalizedName = normalizeSlackChannelLabel(channelName);
    if (normalizedName) channelColumnsSet.add(normalizedName);
  });

  const channelColumns = Array.from(channelColumnsSet).sort((a, b) => a.localeCompare(b, "tr"));
  const normalizedRows = hasProvidedRows
    ? buildSlackReplyRowsFromCounts(
      replyCountByUserId,
      requestCountByUserId,
      nameByUserId,
      channelReplyByUserId,
      channelColumns
    )
    : [];
  const computedTotalRequests = normalizedRows.reduce((sum, row) => sum + row.requestCount, 0);
  const normalizedTotalRequests =
    totalRequests === null || totalRequests === undefined
      ? computedTotalRequests
      : toCountInteger(totalRequests);
  const totalReplies = normalizedRows.reduce((sum, row) => sum + row.replyCount, 0);
  const channelReplyTotals = {};
  channelColumns.forEach((channelName) => {
    channelReplyTotals[channelName] = normalizedRows.reduce(
      (sum, row) => sum + toCountInteger(row?.channelReplyCounts?.[channelName] || 0),
      0
    );
  });

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
    meta: normalizedMeta,
    channelColumns,
    channelReplyTotals
  };
}

function buildSlackSavedReportChartRows(runs = []) {
  const byUserId = new Map();

  (Array.isArray(runs) ? runs : []).forEach((run) => {
    const items = Array.isArray(run?.items) ? run.items : [];
    items.forEach((item) => {
      const userId = String(item?.userId || "").trim();
      if (!userId) return;

      const current = byUserId.get(userId) || {
        userId,
        name: String(item?.userName || "").trim() || userId,
        requestCount: 0,
        replyCount: 0
      };

      const nextName = String(item?.userName || "").trim();
      if (nextName) {
        current.name = nextName;
      }
      current.requestCount += toCountInteger(item?.requestCount);
      current.replyCount += toCountInteger(item?.replyCount);
      byUserId.set(userId, current);
    });
  });

  const rows = Array.from(byUserId.values()).sort(
    (a, b) =>
      b.replyCount - a.replyCount || b.requestCount - a.requestCount || a.name.localeCompare(b.name, "tr")
  );
  const maxValue = rows.reduce((max, row) => Math.max(max, row.requestCount, row.replyCount), 0);
  const minVisiblePct = 3;

  return rows.map((row) => {
    const requestPct =
      maxValue > 0 && row.requestCount > 0 ? Math.max((row.requestCount / maxValue) * 100, minVisiblePct) : 0;
    const replyPct =
      maxValue > 0 && row.replyCount > 0 ? Math.max((row.replyCount / maxValue) * 100, minVisiblePct) : 0;

    return {
      userId: row.userId,
      label: row.name,
      requestCount: row.requestCount,
      replyCount: row.replyCount,
      requestText: String(row.requestCount),
      replyText: String(row.replyCount),
      requestPct: Number(requestPct.toFixed(2)),
      replyPct: Number(replyPct.toFixed(2))
    };
  });
}

function buildSlackSqlQueryModel({
  requested = false,
  startDate = "",
  endDate = "",
  rows = [],
  error = null
} = {}) {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  return {
    requested: Boolean(requested),
    filters: {
      startDate: normalizeIsoDateInput(startDate) || getTodayIsoDate(),
      endDate: normalizeIsoDateInput(endDate) || getTodayIsoDate()
    },
    rows: normalizedRows,
    chartRows: buildSlackSavedReportChartRows(normalizedRows),
    error: error ? String(error) : null
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
  const channelReplyByUserId = new Map(SLACK_SELECTED_USERS.map((item) => [item.id, new Map()]));
  const channelRequestCountByName = new Map();

  const addChannelRequestCount = (channelName, count = 1) => {
    const normalizedName = normalizeSlackChannelLabel(channelName);
    if (!normalizedName) return;
    channelRequestCountByName.set(
      normalizedName,
      (channelRequestCountByName.get(normalizedName) || 0) + toCountInteger(count)
    );
  };

  const addChannelReplyCount = (userId, channelName, count = 1) => {
    const normalizedUserId = String(userId || "").trim();
    const normalizedName = normalizeSlackChannelLabel(channelName);
    if (!normalizedUserId || !normalizedName || !selectedIds.has(normalizedUserId)) return;
    const userMap = channelReplyByUserId.get(normalizedUserId) || new Map();
    userMap.set(normalizedName, (userMap.get(normalizedName) || 0) + toCountInteger(count));
    channelReplyByUserId.set(normalizedUserId, userMap);
  };

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

  channels = channels.filter((channel) => shouldAnalyzeSlackChannel(channel?.id, channel?.name));
  if (!channels.length) {
    return buildSlackReplyReportModel({
      requested: true,
      rows: [],
      source: "Slack API",
      notice: `Analiz için uygun kanal bulunamadı. Hedef: ${SLACK_ANALYSIS_TARGET_CHANNELS_RAW || "-"}`
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
  let corpTaggedRequestCount = 0;
  let corpFirstResponderCount = 0;

  const metrics = {
    channelsTotal: channels.length,
    channelsScanned: 0,
    threadsScanned: 0
  };

  const prioritizedChannels = channels
    .slice()
    .sort((a, b) => {
      const matchA = shouldApplyCorpRequestRuleForChannel(a?.id, a?.name) ? 1 : 0;
      const matchB = shouldApplyCorpRequestRuleForChannel(b?.id, b?.name) ? 1 : 0;
      return matchB - matchA;
    });
  const limitedChannels = prioritizedChannels.slice(0, maxChannels);
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
      const channelLabel = normalizeSlackChannelLabel(channelName || channelId);
      const shouldApplyCorpTagRule = shouldApplyCorpRequestRuleForChannel(channelId, channelName);

      metrics.channelsScanned += 1;
      if (channelLabel) {
        scannedChannelNames.add(channelLabel);
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

      if (shouldApplyCorpTagRule && SLACK_CORP_REQUEST_TAG) {
        const timelineMessages = (messagesResult.messages || [])
          .filter((message) => String(message?.ts || "").trim())
          .slice()
          .sort((a, b) => {
            const tsA = Number.parseFloat(String(a?.ts || "0"));
            const tsB = Number.parseFloat(String(b?.ts || "0"));
            return tsA - tsB;
          });
        const pendingCorpTagRequests = [];

        for (const message of timelineMessages) {
          const messageTs = String(message?.ts || "").trim();
          const messageTsNum = Number.parseFloat(messageTs || "0");
          if (!messageTs || !Number.isFinite(messageTsNum)) continue;

          const userId = String(message?.user || "").trim();
          if (pendingCorpTagRequests.length > 0 && shouldCountSlackMessage(message) && selectedIds.has(userId)) {
            const pendingRequest = pendingCorpTagRequests[0];
            if (messageTsNum > pendingRequest.tsNum) {
              pendingCorpTagRequests.shift();
              replyCountByUserId.set(userId, (replyCountByUserId.get(userId) || 0) + 1);
              addChannelReplyCount(userId, channelLabel, 1);
              corpFirstResponderCount += 1;
            }
          }

          if (!messageContainsCorpRequestTag(message)) continue;

          const corpRequestKey = `${channelId}:corp-tag:${messageTs}`;
          const baseRequestKey = `${channelId}:${messageTs}`;
          if (seenRequestMessages.has(corpRequestKey) || seenRequestMessages.has(baseRequestKey)) {
            continue;
          }

          seenRequestMessages.add(corpRequestKey);
          totalRequestCount += 1;
          corpTaggedRequestCount += 1;
          addChannelRequestCount(channelLabel, 1);
          pendingCorpTagRequests.push({
            tsNum: messageTsNum
          });
        }
      }

      const threadRoots = (messagesResult.messages || []).filter((message) => {
        const messageTs = String(message?.ts || "").trim();
        const messageUserId = String(message?.user || "").trim();
        const isEligibleExternalRoot = Boolean(messageTs && messageUserId && !selectedIds.has(messageUserId));

        if (shouldCountSlackMessage(message)) {
          const threadTs = String(message.thread_ts || "").trim();
          const isThreadReply = Boolean(threadTs && threadTs !== messageTs);
          // Talep sayısına sadece seçili 7 kişi dışındaki kullanıcıların başlattığı konuşmaları dahil et.
          if (isEligibleExternalRoot && !isThreadReply) {
            const requestKey = `${channelId}:${messageTs}`;
            if (!seenRequestMessages.has(requestKey)) {
              seenRequestMessages.add(requestKey);
              totalRequestCount += 1;
              addChannelRequestCount(channelLabel, 1);
            }
          }
        }

        const replyCount = Number(message?.reply_count || 0);
        // Yanıt sayısına sadece seçili kullanıcılar dışındaki kişilerin açtığı thread'leri dahil et.
        return isEligibleExternalRoot && Number.isFinite(replyCount) && replyCount > 0;
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
            addChannelReplyCount(userId, channelLabel, 1);
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
  const rows = buildSlackReplyRowsFromCounts(
    replyCountByUserId,
    requestCountByUserId,
    nameByUserId,
    channelReplyByUserId,
    []
  );
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
  if (corpTaggedRequestCount > 0 || corpFirstResponderCount > 0) {
    noticeParts.push(`@corpproduct Talep: ${corpTaggedRequestCount}, İlk Yanıt: ${corpFirstResponderCount}`);
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
    const runIdPlaceholders = runIds.length > 0 ? buildInClausePlaceholders(runIds, 1) : "";
    const itemResult = await pool.query(
      `
        SELECT
          run_id,
          user_id,
          user_name,
          request_count,
          reply_count
        FROM slack_reply_analysis_items
        WHERE run_id IN (${runIdPlaceholders || "NULL"})
        ORDER BY run_id DESC, reply_count DESC, request_count DESC, user_name ASC
      `,
      runIds
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

async function saveSlackReplyReportToDb({
  startDate,
  endDate,
  rows,
  userId,
  totalRequests = null,
  source = "slack-analysis-ui"
}) {
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

    const existingRunResult =
      ownerId === null
        ? await client.query(
            `
              SELECT id, COALESCE(save_count, 1) AS save_count
              FROM slack_reply_analysis_runs
              WHERE start_date = $1
                AND end_date = $2
                AND created_by IS NULL
              ORDER BY id DESC
              OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
            `,
            [startDate, endDate]
          )
        : await client.query(
            `
              SELECT id, COALESCE(save_count, 1) AS save_count
              FROM slack_reply_analysis_runs
              WHERE start_date = $1
                AND end_date = $2
                AND created_by = $3
              ORDER BY id DESC
              OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
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
        [runId, normalizedTotalRequests, totalReplies, normalizedRows.length, source, saveCount]
      );

      await client.query(
        `
          DELETE FROM slack_reply_analysis_items
          WHERE run_id = $1
        `,
        [runId]
      );
    } else {
      await client.query(
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
        `,
        [
          startDate,
          endDate,
          normalizedTotalRequests,
          totalReplies,
          normalizedRows.length,
          1,
          source,
          ownerId
        ]
      );
      const insertedRunResult = await client.query(
        `
          SELECT id, COALESCE(save_count, 1) AS save_count
          FROM slack_reply_analysis_runs
          WHERE start_date = $1
            AND end_date = $2
            AND source = $3
            AND (
              (created_by = $4)
              OR ($4 IS NULL AND created_by IS NULL)
            )
          ORDER BY id DESC
          OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
        `,
        [startDate, endDate, source, ownerId]
      );
      runId = Number(insertedRunResult.rows[0]?.id);
      saveCount = toCountInteger(insertedRunResult.rows[0]?.save_count) || 1;
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

function buildObusJobsPersistencePayload({ report, selectedCompanyMeta, endpointUrl }) {
  const clusterResults = Array.isArray(report?.clusterResults) ? report.clusterResults : [];
  const summary = summarizeObusJobsReport(report);

  return {
    companyCode: String(selectedCompanyMeta?.code || "").trim(),
    companyId: String(selectedCompanyMeta?.id || "").trim(),
    companyCluster: String(selectedCompanyMeta?.cluster || "").trim(),
    endpointUrl: String(endpointUrl || "").trim(),
    summary: {
      requestedClusterCount: summary.requestedClusterCount,
      successClusterCount: summary.successClusterCount,
      errorClusterCount: summary.errorClusterCount,
      jobColumnCount: summary.jobColumnCount,
      jobItemCount: summary.jobItemCount,
      error: String(report?.error || "").trim()
    },
    clusters: clusterResults.map((clusterResult) => ({
      clusterLabel: String(clusterResult?.clusterLabel || "").trim(),
      error: String(clusterResult?.error || "").trim(),
      jobs: (Array.isArray(clusterResult?.jobs) ? clusterResult.jobs : []).map((job) => ({
        id: String(job?.id || "").trim(),
        lastExecution: String(job?.lastExecution || "").trim(),
        lastJobState: String(job?.lastJobState || "").trim(),
        isYesterday: Boolean(job?.isYesterday)
      }))
    }))
  };
}

async function saveObusJobsReportToDb({
  report,
  selectedCompanyMeta,
  endpointUrl,
  userId,
  source = "obus-jobs-ui"
}) {
  const summary = summarizeObusJobsReport(report);
  const payload = buildObusJobsPersistencePayload({
    report,
    selectedCompanyMeta,
    endpointUrl
  });
  const requestKey = `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
  const ownerId = userId || null;
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await client.query(
      `
        INSERT INTO obus_jobs_runs (
          request_key,
          company_code,
          company_id,
          company_cluster,
          endpoint_url,
          requested_cluster_count,
          success_cluster_count,
          error_cluster_count,
          job_column_count,
          job_item_count,
          source,
          created_by,
          summary_error,
          payload_json
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      `,
      [
        requestKey,
        payload.companyCode,
        payload.companyId,
        payload.companyCluster,
        payload.endpointUrl,
        summary.requestedClusterCount,
        summary.successClusterCount,
        summary.errorClusterCount,
        summary.jobColumnCount,
        summary.jobItemCount,
        source,
        ownerId,
        String(report?.error || "").trim(),
        JSON.stringify(payload)
      ]
    );

    const insertedRunResult = await client.query(
      `
        SELECT id
        FROM obus_jobs_runs
        WHERE request_key = $1
        ORDER BY id DESC
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [requestKey]
    );
    const runId = Number(insertedRunResult.rows[0]?.id || 0);
    if (!runId) {
      throw new Error("Obus Joblar SQL kayıt numarası alınamadı.");
    }

    let insertedItemCount = 0;
    for (const clusterResult of payload.clusters) {
      const clusterLabel = String(clusterResult?.clusterLabel || "").trim() || "cluster";
      const clusterError = String(clusterResult?.error || "").trim();
      const jobs = Array.isArray(clusterResult?.jobs) ? clusterResult.jobs : [];

      if (clusterError) {
        await client.query(
          `
            INSERT INTO obus_jobs_items (
              run_id,
              cluster_label,
              job_id,
              last_execution,
              last_job_state,
              is_yesterday,
              cluster_error
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [runId, clusterLabel, null, null, null, false, clusterError]
        );
        insertedItemCount += 1;
        continue;
      }

      for (const job of jobs) {
        await client.query(
          `
            INSERT INTO obus_jobs_items (
              run_id,
              cluster_label,
              job_id,
              last_execution,
              last_job_state,
              is_yesterday,
              cluster_error
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [
            runId,
            clusterLabel,
            String(job?.id || "").trim() || null,
            String(job?.lastExecution || "").trim() || null,
            String(job?.lastJobState || "").trim() || null,
            Boolean(job?.isYesterday),
            null
          ]
        );
        insertedItemCount += 1;
      }
    }

    await client.query("COMMIT");
    return {
      runId,
      requestedClusterCount: summary.requestedClusterCount,
      successClusterCount: summary.successClusterCount,
      errorClusterCount: summary.errorClusterCount,
      jobColumnCount: summary.jobColumnCount,
      jobItemCount: summary.jobItemCount,
      itemRowCount: insertedItemCount
    };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

function collectObusJobsSlackMentionTargets(flag) {
  const values = [];
  const rawValues = [
    ...(Array.isArray(flag?.notifyTargets) ? flag.notifyTargets : []),
    ...(Array.isArray(flag?.mentions) ? flag.mentions : []),
    flag?.notifyTarget,
    flag?.mention,
    flag?.owner,
    flag?.assignee
  ];

  rawValues.forEach((value) => {
    if (Array.isArray(value)) {
      values.push(...value);
      return;
    }
    values.push(...parseSlackMentionTargets(value));
  });

  if (!values.length) {
    values.push(...parseSlackMentionTargets(OBUS_JOBS_DEFAULT_SLACK_MENTION_TARGETS_RAW));
  }

  return dedupeStringList(values);
}

async function buildObusJobsSlackMessage({ report, token }) {
  const tableResult = buildObusJobsSlackTable({ report });
  const flaggedCells = Array.isArray(tableResult.flaggedCells) ? tableResult.flaggedCells : [];
  if (!flaggedCells.length) {
    return {
      summary: "",
      unresolvedMentionTargets: [],
      mentionLookupError: ""
    };
  }

  const rawTargets = dedupeStringList(flaggedCells.flatMap((flag) => collectObusJobsSlackMentionTargets(flag)));
  const mentionResults = await resolveSlackMentions(token, rawTargets);
  const mentionByTarget = new Map(
    mentionResults.map((item) => [String(item.rawTarget || "").trim().toLocaleLowerCase("tr"), item])
  );
  const lines = [];
  flaggedCells.slice(0, 5).forEach((flag) => {
    const mentionText = dedupeStringList(
      collectObusJobsSlackMentionTargets(flag)
        .map((target) => mentionByTarget.get(String(target || "").trim().toLocaleLowerCase("tr")))
        .map((result) => String(result?.mentionText || "").trim())
        .filter(Boolean)
    ).join(" ");
    lines.push(
      `cluster ${flag.cluster} ${flag.jobLabel} job Failed.${mentionText ? ` ${mentionText}` : ""} kontrol edebilir misin?`
    );
  });
  return {
    summary: lines.join("\n"),
    unresolvedMentionTargets: mentionResults.filter((item) => !item.resolved).map((item) => item.rawTarget),
    mentionLookupError:
      mentionResults.find((item) => String(item.error || "").trim())?.error || ""
  };
}

function buildObusJobsSlackTable({ report }) {
  const columnSource = Array.isArray(report?.jobColumns) && report.jobColumns.length > 0
    ? report.jobColumns
    : (Array.isArray(report?.jobIds) ? report.jobIds.map((id) => ({ id, label: id, key: String(id || "").trim().toLowerCase() })) : []);

  const entries = [];
  const flaggedCells = [];
  (Array.isArray(report?.clusterRows) ? report.clusterRows : []).forEach((row) => {
    if (row.error) return;
    columnSource.forEach((column) => {
      const job = row.jobsByLabel?.[column.key];
      if (!job) return;
      entries.push({
        cluster: row.clusterLabel,
        jobLabel: column.label,
        state: job.lastJobState,
        lastExecution: job.lastExecutionText,
        isPast: Boolean(job.isPastExecution),
        isError: String(job.lastJobState || "").trim().toLowerCase() !== "succeeded"
      });
      const stateNorm = String(job.lastJobState || "").trim().toLowerCase();
      if (job.isPastExecution) {
        flaggedCells.push({
          cluster: row.clusterLabel,
          jobLabel: column.label,
          reason: "Tarih geçmiş"
        });
      } else if (stateNorm && stateNorm !== "succeeded" && stateNorm !== "processing") {
        flaggedCells.push({
          cluster: row.clusterLabel,
          jobLabel: column.label,
          reason: `Durum: ${job.lastJobState}`
        });
      }
    });
  });

  if (!entries.length) return { tableText: null, flaggedCells: [] };
  const header = "*Cluster* | *Job* | *State* | *Last Execution*";
  const tableRows = entries
    .map((entry) => `${entry.cluster} | ${entry.jobLabel} | ${entry.state} | ${entry.lastExecution}`)
    .slice(0, 20);
  if (entries.length > tableRows.length) {
    tableRows.push(`... ve ${entries.length - tableRows.length} daha`);
  }

  // Deduplicate flagged cells by cluster+jobLabel
  const seenFlags = new Set();
  const uniqueFlags = [];
  flaggedCells.forEach((flag) => {
    const key = `${flag.cluster}|${flag.jobLabel}|${flag.reason}`;
    if (!seenFlags.has(key)) {
      seenFlags.add(key);
      uniqueFlags.push(flag);
    }
  });

  return {
    tableText: [header, ...tableRows].join("\n"),
    flaggedCells: uniqueFlags
  };
}

async function postObusJobsReportToSlack({ report, selectedCompanyMeta, user, saveResult }) {
  const token = String(process.env.SLACK_BOT_TOKEN || "").trim();
  if (!token) {
    throw new Error("SLACK_BOT_TOKEN gerekli.");
  }

  const resolvedChannel = await resolveSlackConversationId(token, SLACK_CREW_CHANNEL, SLACK_ANALYSIS_CHANNEL_TYPES_RAW);
  const message = await buildObusJobsSlackMessage({
    report,
    token
  });
  const payload = {
    channel: resolvedChannel.channelId,
    text: message.summary,
    unfurl_links: false,
    unfurl_media: false
  };
  if (message.summary) {
    payload.blocks = [
      {
        type: "section",
        text: {
          type: "mrkdwn",
          text: message.summary
        }
      }
    ];
  }
  const response = await slackApiPost("chat.postMessage", token, payload);

  return {
    channelId: resolvedChannel.channelId,
    channelLabel: resolvedChannel.channelLabel,
    ts: String(response?.ts || "").trim(),
    unresolvedMentionTargets: Array.isArray(message.unresolvedMentionTargets) ? message.unresolvedMentionTargets : [],
    mentionLookupError: String(message.mentionLookupError || "").trim()
  };
}

async function runObusJobsScheduledScan({ source = "auto" } = {}) {
  if (source === "auto" && !OBUS_JOBS_AUTO_RUN_ENABLED) return;
  if (obusJobsAutoState.isRunning) {
    console.log(`[ObusJobsScheduler] (${source}) taraması atlandı; önceki çalışma sürüyor.`);
    return;
  }
  obusJobsAutoState.isRunning = true;
  try {
    const { companies, partnerItems } = await loadAuthorizedLinesCompanies();
    const filters = { endpointUrl: OBUS_JOBS_API_URL };
    console.log(`[ObusJobsScheduler] (${source}) taraması başlıyor.`);
    const report = await executeObusJobsScreenAction({
      filters,
      partnerItems
    });
    const selectedCompanyMeta = partnerItems.length > 0 ? partnerItems[0] : null;

    if (Array.isArray(report.clusterResults) && report.clusterResults.length > 0) {
      try {
        const saveResult = await saveObusJobsReportToDb({
          report,
          selectedCompanyMeta,
          endpointUrl: filters.endpointUrl,
          userId: null,
          source: `obus-jobs-${source}`
        });
        console.log(`[ObusJobsScheduler] SQL kaydı oluşturuldu (#${saveResult.runId}).`);
        report.saveResult = saveResult;
      } catch (err) {
        console.error(`[ObusJobsScheduler] SQL kaydı başarısız: ${summarizeErrorMessage(err)}`);
      }

      try {
        const slackResult = await postObusJobsReportToSlack({
          report,
          selectedCompanyMeta,
          user: null,
          saveResult: report.saveResult
        });
        console.log(`[ObusJobsScheduler] Slack bildirimi ${slackResult.channelLabel} kanalına gönderildi.`);
        if (Array.isArray(slackResult.unresolvedMentionTargets) && slackResult.unresolvedMentionTargets.length > 0) {
          console.warn(
            `[ObusJobsScheduler] Mention çözümlenemedi: ${slackResult.unresolvedMentionTargets.join(", ")}${
              slackResult.mentionLookupError ? ` | ${slackResult.mentionLookupError}` : ""
            }`
          );
        }
      } catch (err) {
        console.error(`[ObusJobsScheduler] Slack bildirimi başarısız: ${summarizeErrorMessage(err)}`);
      }
    } else {
      console.log("[ObusJobsScheduler] Kayıt üretilemedi (cluster sonucu yok).");
    }
  } catch (err) {
    console.error(`[ObusJobsScheduler] Hata: ${summarizeErrorMessage(err)}`);
  } finally {
    obusJobsAutoState.isRunning = false;
  }
}

function scheduleNextObusJobsRun() {
  if (!OBUS_JOBS_AUTO_RUN_ENABLED) return;
  const delay = parseTimeToNextDelay(OBUS_JOBS_AUTO_RUN_TIME);
  if (obusJobsAutoState.timerId) {
    clearTimeout(obusJobsAutoState.timerId);
  }
  obusJobsAutoState.timerId = setTimeout(async () => {
    await runObusJobsScheduledScan({ source: "auto" });
    scheduleNextObusJobsRun();
  }, delay);
  console.log(`[ObusJobsScheduler] Bir sonraki tarama ${Math.round(delay / 1000 / 60)} dakika sonra planlandı.`);
}

function parseObusJobsAutoRunTime(value) {
  const raw = String(value || "").trim();
  const matched = raw.match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!matched) {
    return { hour: 10, minute: 0, normalized: "10:00", valid: false };
  }
  const hour = Number.parseInt(matched[1], 10);
  const minute = Number.parseInt(matched[2], 10);
  return {
    hour,
    minute,
    normalized: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
    valid: true
  };
}

async function hasObusJobsAutoRunForDate(targetDate) {
  const normalizedDate = normalizeIsoDateInput(targetDate);
  if (!normalizedDate) return false;

  const result = await pool.query(
    `
      SELECT 1
      FROM obus_jobs_runs
      WHERE source = $1
        AND created_by IS NULL
        AND (created_at AT TIME ZONE $2)::date = $3::date
      ORDER BY id DESC
      OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
    `,
    [OBUS_JOBS_AUTO_RUN_SOURCE, APP_TIME_ZONE, normalizedDate]
  );

  return result.rows.length > 0;
}

async function runDueObusJobsAutoScanOnStartup(parsedTime, now = new Date()) {
  if (!isSlackAutoSaveDueForToday(parsedTime, now)) return;

  const today = formatDateToIsoLocal(now);
  if (!today) return;

  const alreadyCompleted = await hasObusJobsAutoRunForDate(today);
  if (alreadyCompleted) {
    console.log(`[ObusJobsScheduler] ${today} için otomatik tarama zaten kaydedilmiş.`);
    return;
  }

  console.log(`[ObusJobsScheduler] ${today} için kaçan otomatik tarama başlangıçta tetikleniyor.`);
  await runObusJobsScheduledScan({ source: "auto" });
}

function startObusJobsAutoScheduler() {
  if (!OBUS_JOBS_AUTO_RUN_ENABLED) {
    console.log("[ObusJobsScheduler] Otomatik tarama devre dışı.");
    return;
  }

  const parsedTime = parseObusJobsAutoRunTime(OBUS_JOBS_AUTO_RUN_TIME);
  if (!parsedTime.valid) {
    console.warn(
      `[ObusJobsScheduler] OBUS_JOBS_AUTO_RUN_TIME değeri geçersiz (${OBUS_JOBS_AUTO_RUN_TIME}). 10:00 kullanılacak.`
    );
  }

  console.log(`[ObusJobsScheduler] Günlük otomatik tarama aktif. Saat: ${parsedTime.normalized}`);
  scheduleNextObusJobsRun();
  runDueObusJobsAutoScanOnStartup(parsedTime, new Date()).catch((err) => {
    console.error(`[ObusJobsScheduler] başlangıç kontrol hatası: ${summarizeErrorMessage(err)}`);
  });
}

function parseSlackAutoSaveTime(value) {
  const raw = String(value || "").trim();
  const matched = raw.match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!matched) {
    return { hour: 23, minute: 59, normalized: "23:59", valid: false };
  }
  const hour = Number.parseInt(matched[1], 10);
  const minute = Number.parseInt(matched[2], 10);
  return {
    hour,
    minute,
    normalized: `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
    valid: true
  };
}

function formatDateToIsoLocal(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDateToIsoInTimeZone(date = new Date(), timeZone = APP_TIME_ZONE) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: String(timeZone || "").trim() || APP_TIME_ZONE,
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    }).formatToParts(date);
    const year = String(parts.find((part) => part.type === "year")?.value || "").trim();
    const month = String(parts.find((part) => part.type === "month")?.value || "").trim();
    const day = String(parts.find((part) => part.type === "day")?.value || "").trim();
    if (year && month && day) {
      return `${year}-${month}-${day}`;
    }
  } catch (err) {
    // Ignore and fallback to local formatting.
  }
  return formatDateToIsoLocal(date);
}

function shiftIsoDateByDays(isoDate, dayDelta) {
  const normalized = normalizeIsoDateInput(isoDate);
  if (!normalized) return "";
  const [yearRaw, monthRaw, dayRaw] = normalized.split("-");
  const year = Number.parseInt(yearRaw, 10);
  const month = Number.parseInt(monthRaw, 10);
  const day = Number.parseInt(dayRaw, 10);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return "";

  const date = new Date(year, month - 1, day);
  date.setDate(date.getDate() + (Number.parseInt(dayDelta, 10) || 0));
  return formatDateToIsoLocal(date);
}

function isSlackAutoSaveDueForToday(parsedTime, now = new Date()) {
  const hour = Number.parseInt(parsedTime?.hour, 10);
  const minute = Number.parseInt(parsedTime?.minute, 10);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return false;

  if (now.getHours() > hour) return true;
  if (now.getHours() === hour && now.getMinutes() >= minute) return true;
  return false;
}

function getDueSlackAutoSaveLimitDate(parsedTime, now = new Date()) {
  const today = formatDateToIsoLocal(now);
  if (!today) return "";
  if (isSlackAutoSaveDueForToday(parsedTime, now)) return today;
  return shiftIsoDateByDays(today, -1);
}

async function getLatestAutoSlackSavedDate() {
  const result = await pool.query(
    `
      SELECT MAX(start_date) AS latest_date
      FROM slack_reply_analysis_runs
      WHERE source = $1
        AND created_by IS NULL
        AND start_date = end_date
    `,
    ["slack-analysis-auto"]
  );
  const rawDate = normalizeSqlDateValue(result.rows[0]?.latest_date || "");
  return normalizeIsoDateInput(rawDate);
}

async function getPendingSlackAutoSaveDates(parsedTime, now = new Date()) {
  const dueLimitDate = getDueSlackAutoSaveLimitDate(parsedTime, now);
  if (!dueLimitDate) return [];

  const latestSavedDate = await getLatestAutoSlackSavedDate();
  let startDate = dueLimitDate;

  if (latestSavedDate) {
    const nextDate = shiftIsoDateByDays(latestSavedDate, 1);
    if (nextDate) {
      startDate = nextDate;
    }
  }

  if (startDate > dueLimitDate) return [];

  const pendingDates = [];
  let cursor = startDate;
  while (cursor && cursor <= dueLimitDate) {
    pendingDates.push(cursor);
    const nextDate = shiftIsoDateByDays(cursor, 1);
    if (!nextDate || nextDate === cursor) break;
    cursor = nextDate;
  }
  return pendingDates;
}

async function hasAutoSlackSaveRunForDate(targetDate) {
  const normalizedDate = normalizeIsoDateInput(targetDate);
  if (!normalizedDate) return false;
  const result = await pool.query(
    `
      SELECT id
      FROM slack_reply_analysis_runs
      WHERE start_date = $1
        AND end_date = $1
        AND source = $2
        AND created_by IS NULL
      ORDER BY id DESC
      LIMIT 1
    `,
    [normalizedDate, "slack-analysis-auto"]
  );
  return Boolean(result.rows[0]?.id);
}

async function runSlackAutoSaveForDate(targetDate) {
  const normalizedDate = normalizeIsoDateInput(targetDate);
  if (!normalizedDate) {
    throw new Error("Geçersiz tarih.");
  }

  const alreadySaved = await hasAutoSlackSaveRunForDate(normalizedDate);
  if (alreadySaved) {
    return {
      skipped: true,
      reason: "Kayıt zaten mevcut."
    };
  }

  const cacheKey = getSlackReplyReportCacheKey(normalizedDate, normalizedDate);
  slackReplyReportCache.delete(cacheKey);
  const report = await fetchSlackReplyReportForRange(normalizedDate, normalizedDate);
  if (report.error) {
    throw new Error(report.error);
  }
  if (!Array.isArray(report.rows)) {
    throw new Error("Rapor satırları okunamadı.");
  }

  const saveResult = await saveSlackReplyReportToDb({
    startDate: normalizedDate,
    endDate: normalizedDate,
    rows: report.rows,
    totalRequests: report.totalRequests,
    userId: null,
    source: "slack-analysis-auto"
  });

  return {
    skipped: false,
    saveResult
  };
}

function startSlackAutoSaveScheduler() {
  const parsedTime = parseSlackAutoSaveTime(SLACK_ANALYSIS_AUTO_SAVE_TIME);
  if (!parsedTime.valid) {
    console.warn(
      `[SlackAutoSave] SLACK_ANALYSIS_AUTO_SAVE_TIME değeri geçersiz (${SLACK_ANALYSIS_AUTO_SAVE_TIME}). 23:59 kullanılacak.`
    );
  }

  const schedulerLabel = `${String(parsedTime.hour).padStart(2, "0")}:${String(parsedTime.minute).padStart(2, "0")}`;
  console.log(`[SlackAutoSave] Günlük otomatik SQL kaydı aktif. Saat: ${schedulerLabel}`);

  const tick = async () => {
    if (slackAutoSaveState.isRunning) return;

    slackAutoSaveState.isRunning = true;
    try {
      const pendingDates = await getPendingSlackAutoSaveDates(parsedTime, new Date());
      if (!pendingDates.length) return;

      const preview = pendingDates.slice(0, 5).join(", ");
      const extraCount = Math.max(0, pendingDates.length - 5);
      console.log(
        `[SlackAutoSave] Bekleyen ${pendingDates.length} gün bulundu: ${preview}${extraCount > 0 ? ` (+${extraCount})` : ""}`
      );

      for (const targetDate of pendingDates) {
        try {
          const result = await runSlackAutoSaveForDate(targetDate);
          if (result.skipped) {
            console.log(`[SlackAutoSave] ${targetDate} için otomatik kayıt atlandı: ${result.reason}`);
          } else {
            console.log(
              `[SlackAutoSave] ${targetDate} otomatik kaydedildi. Kayıt No: ${result.saveResult.runId} | Talep: ${result.saveResult.totalRequests} | Yanıt: ${result.saveResult.totalReplies}`
            );
          }
        } catch (err) {
          console.error(`[SlackAutoSave] ${targetDate} otomatik kayıt hatası: ${err?.message || "Bilinmeyen hata"}`);
          break;
        }
      }
    } finally {
      slackAutoSaveState.isRunning = false;
    }
  };

  if (slackAutoSaveState.timerId) {
    clearInterval(slackAutoSaveState.timerId);
    slackAutoSaveState.timerId = null;
  }

  slackAutoSaveState.timerId = setInterval(() => {
    tick().catch((err) => {
      console.error(`[SlackAutoSave] scheduler tick hatası: ${err?.message || "Bilinmeyen hata"}`);
    });
  }, 60000);

  tick().catch((err) => {
    console.error(`[SlackAutoSave] başlangıç kontrol hatası: ${err?.message || "Bilinmeyen hata"}`);
  });
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

function buildSalesTotalsFromRows(rows) {
  return (Array.isArray(rows) ? rows : []).reduce(
    (acc, row) => {
      acc.website += toNumber(row?.websiteSaleAmountValue) ?? 0;
      acc.obilet += toNumber(row?.obiletSaleAmountValue) ?? 0;
      return acc;
    },
    { website: 0, obilet: 0 }
  );
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
    return buildSalesTotalsFromRows(listRows);
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
    if (sessionCache) {
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
  const timeout = setTimeout(() => controller.abort(), Math.max(10000, SALES_REPORT_TIMEOUT_MS));

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
    const requestCache = new Map();
    const rangeConcurrency = toBoundedInt(SALES_REPORT_RANGE_CONCURRENCY, 4, 1, 20);
    const targetConcurrency = toBoundedInt(SALES_REPORT_TARGET_CONCURRENCY, 4, 1, 20);
    const sessionConcurrency = toBoundedInt(SALES_REPORT_SESSION_CONCURRENCY, 8, 1, 20);
    const sessionUrls = Array.from(new Set(targets.map((target) => buildSessionUrlForPartnerUrl(target.reportUrl))));

    await runWithConcurrency(
      sessionUrls,
      sessionConcurrency,
      async (sessionUrl) => {
        const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, controller.signal, REPORTING_API_AUTH);
        sessionCache.set(sessionUrl, sessionResult);
        return sessionResult;
      },
      () => controller.signal.aborted
    );

    const dailyRanges = buildDailyRequestRanges(startDate, endDate);
    const monthlyRanges = buildMonthlyRequestRanges(startDate, endDate);
    const normalizedStart = dailyRanges[0]?.startDate || startDate;
    const normalizedEnd = dailyRanges[dailyRanges.length - 1]?.endDate || endDate;
    const fetchSalesReportCached = (target, range) => {
      const requestKey = `${target.clusterLabel}|||${target.partnerId}|||${range.startDate}|||${range.endDate}`;
      if (requestCache.has(requestKey)) {
        return requestCache.get(requestKey);
      }

      const requestPromise = fetchSalesReportFromCluster({
        clusterLabel: target.clusterLabel,
        reportUrl: target.reportUrl,
        startDate: range.startDate,
        endDate: range.endDate,
        partnerId: target.partnerId,
        signal: controller.signal,
        sessionCache
      });
      requestCache.set(requestKey, requestPromise);
      return requestPromise;
    };

    const runRequestsForRanges = async (
      ranges,
      { collectItems = false, collectListRows = false, groupByCode = false } = {}
    ) => {
      const validRanges = (Array.isArray(ranges) ? ranges : []).filter(
        (range) => range && range.startDate && range.endDate
      );
      const rowsByLabel = new Map();
      const items = [];
      const listRows = [];
      const errors = [];

      if (!groupByCode) {
        validRanges.forEach((range) => {
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

      const rangeResults = await runWithConcurrency(
        validRanges,
        rangeConcurrency,
        async (range) => {
          const perRangeItems = [];
          const perRangeListRows = [];
          const perRangeSeriesRows = [];
          const perRangeErrors = [];

          const results = await runWithConcurrency(
            targets,
            targetConcurrency,
            async (target) => fetchSalesReportCached(target, range),
            () => controller.signal.aborted
          );

          results.forEach((result, index) => {
            const target = targets[index];
            if (!result || typeof result !== "object" || !Object.prototype.hasOwnProperty.call(result, "ok")) {
              const detail =
                (typeof result?.error === "string" && result.error) ||
                result?.error?.message ||
                "Bilinmeyen hata";
              const clusterText = target?.clusterLabel || "cluster";
              perRangeErrors.push(`${range.startDate}..${range.endDate} ${clusterText}: ${detail}`);
              return;
            }

            if (collectItems) {
              perRangeItems.push({
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
              perRangeErrors.push(`${range.startDate}..${range.endDate} ${clusterText}: ${detail}`);
              return;
            }

            if (!result.payload || typeof result.payload !== "object") return;
            const extractedRows = extractSalesRowsFromPayload(result.payload);

            if (groupByCode) {
              const groupedRows = groupSalesRowsByCode(extractedRows);
              if (groupedRows.length > 0) {
                groupedRows.forEach((item) => {
                  const code = String(item.code || "").trim();
                  if (!code) return;
                  perRangeSeriesRows.push({
                    label: `${range.label} - ${code}`,
                    code,
                    periodStartDate: String(range.startDate || ""),
                    website: toNumber(item.websiteSaleAmountValue) ?? 0,
                    obilet: toNumber(item.obiletSaleAmountValue) ?? 0
                  });
                });
              } else {
                const totals =
                  extractedRows.length > 0 ? buildSalesTotalsFromRows(extractedRows) : extractSalesTotalsFromPayload(result.payload);
                perRangeSeriesRows.push({
                  label: String(range.label || ""),
                  code: "",
                  periodStartDate: String(range.startDate || ""),
                  website: totals.website,
                  obilet: totals.obilet
                });
              }
            } else {
              const totals =
                extractedRows.length > 0 ? buildSalesTotalsFromRows(extractedRows) : extractSalesTotalsFromPayload(result.payload);
              perRangeSeriesRows.push({
                label: String(range.label || ""),
                code: "",
                periodStartDate: String(range.startDate || ""),
                website: totals.website,
                obilet: totals.obilet
              });
            }

            if (collectListRows) {
              extractedRows.forEach((item) => perRangeListRows.push(item));
            }
          });

          return {
            items: perRangeItems,
            listRows: perRangeListRows,
            seriesRows: perRangeSeriesRows,
            errors: perRangeErrors
          };
        },
        () => controller.signal.aborted
      );

      rangeResults.forEach((result, index) => {
        const range = validRanges[index];
        if (!result || typeof result !== "object" || !Object.prototype.hasOwnProperty.call(result, "seriesRows")) {
          const detail =
            (typeof result?.error === "string" && result.error) ||
            result?.error?.message ||
            "Bilinmeyen hata";
          const rangeText = range ? `${range.startDate}..${range.endDate}` : "range";
          errors.push(`${rangeText}: ${detail}`);
          return;
        }

        items.push(...(Array.isArray(result.items) ? result.items : []));
        listRows.push(...(Array.isArray(result.listRows) ? result.listRows : []));
        errors.push(...(Array.isArray(result.errors) ? result.errors : []));

        (Array.isArray(result.seriesRows) ? result.seriesRows : []).forEach((row) => {
          const label = String(row?.label || "");
          if (!label) return;
          const code = String(row?.code || "").trim();
          const rowKey = code ? `${label}__${code}` : label;
          const current = rowsByLabel.get(rowKey) || {
            label,
            code,
            periodStartDate: String(row?.periodStartDate || ""),
            website: 0,
            obilet: 0
          };
          current.website += toNumber(row?.website) ?? 0;
          current.obilet += toNumber(row?.obilet) ?? 0;
          rowsByLabel.set(rowKey, current);
        });
      });

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
    const [dailyResult, monthlyResult] = await Promise.all([
      runRequestsForRanges(dailyRanges, { groupByCode: shouldGroupSeriesByCode }),
      runRequestsForRanges(monthlyRanges, { groupByCode: shouldGroupSeriesByCode })
    ]);

    const allErrors = Array.from(new Set([...listResult.errors, ...dailyResult.errors, ...monthlyResult.errors]));
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

function buildAuthorizedLinesReportModel() {
  return {
    requested: false,
    status: null,
    error: null,
    errorDetail: "",
    userMessage: "",
    requestUrl: "",
    requestBody: "",
    responseBody: "",
    sessionId: "",
    deviceId: "",
    branchId: "",
    loginToken: "",
    loginUrl: ""
  };
}

function buildJourneyUpdateReportModel() {
  return {
    requested: false,
    status: null,
    error: null,
    errorDetail: "",
    userMessage: "",
    requestUrl: "",
    requestBody: "",
    responseBody: "",
    sessionId: "",
    deviceId: "",
    branchId: "",
    loginToken: "",
    loginUrl: "",
    tableRows: [],
    dayResults: [],
    tableColumns: [],
    updateEndpointUrl: "",
    updateRequestHeaders: "",
    updateRequestBodyPreview: "",
    updateRequestDate: "",
    updateEditorValues: {},
    updateJourneyIdsCsv: "",
    updateDetailState: "",
    tableRowsState: "",
    tableColumnsState: ""
  };
}

function formatJourneyUpdateTableCell(value, fallback = "-") {
  const text = formatPartnerCellValue(value).trim();
  return text || fallback;
}

function buildJourneyUpdateBaseColumns() {
  return [
    { key: "id", label: "Id", filterType: "text", sortType: "number" },
    { key: "departureTime", label: "Departure-Time", filterType: "date", sortType: "date" },
    { key: "origin", label: "Origin", filterType: "text", sortType: "text" },
    { key: "destination", label: "Destination", filterType: "text", sortType: "text" },
    { key: "status", label: "Status", filterType: "select", sortType: "text", options: ["true", "false"] },
    { key: "journeyCode", label: "Journey-Code", filterType: "text", sortType: "text" },
    { key: "description", label: "Description", filterType: "text", sortType: "text" },
    { key: "seatModel", label: "Seat-Model", filterType: "text", sortType: "text" },
    { key: "plate", label: "Plaka", filterType: "text", sortType: "text" },
    { key: "journeyType", label: "Journey-Type", filterType: "select", sortType: "text", options: ["true", "false"] },
    { key: "routeInfo", label: "Route-Info", filterType: "text", sortType: "text" },
    { key: "duration", label: "Duration", filterType: "text", sortType: "duration" },
    { key: "distance", label: "Distance", filterType: "text", sortType: "number" },
    { key: "lastExtendTime", label: "Last-Extend-Time", filterType: "date", sortType: "date" }
  ];
}

function normalizeJourneyUpdateParameterColumnKey(typeLabel = "") {
  const normalized = normalizeTokenName(typeLabel);
  return normalized ? `parameter_${normalized}` : "";
}

const JOURNEY_UPDATE_PARAMETER_LABEL_MAP = Object.freeze({
  WebWarning: "Web Uyarısı",
  RouteFilterBeforeXMinutes: "Web Site Rota Listeleme Süresi",
  ReservationOption: "Rezervasyon Opsiyon Süresi",
  RefundOption: "İade ve Transfer Opsiyon Süresi",
  MaxTotalDiscountCount: "Azami İndirimli Adedi",
  MaxSingleSeatCount: "Azami Tek koltuk Adedi",
  MaxSingleFemaleCount: "Azami Tek Bayan Adedi",
  MaxReservationCount: "Azami Rezervasyon Adedi",
  MaxPointSalesCount: "Azami Puanlı Satış Adedi",
  MaxInfantCount: "Azami Çocuk Adedi",
  MaxGuestCount: "Azami Misafir Adedi",
  MaxDisabledCount: "Azami Engelli Adedi",
  CanStopAtWayStation: "Ara Durakta Durmaz",
  CanSellThroughWeb: "İnternetten Satışa Açık",
  CanApplyDiscount: "İndirim Yapılamaz"
});

function formatJourneyUpdateDynamicFieldLabel(value = "") {
  const rawValue = String(value || "").trim();
  if (!rawValue) return "";

  const tokens = rawValue
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(" ")
    .filter(Boolean);

  return tokens
    .map((token) => {
      if (/^[A-Z0-9]+$/.test(token)) return token;
      const lower = token.toLocaleLowerCase("tr");
      return `${lower.charAt(0).toLocaleUpperCase("tr")}${lower.slice(1)}`;
    })
    .join("-");
}

function getJourneyUpdateParameterDisplayLabel(typeLabel = "") {
  const rawLabel = String(typeLabel || "").trim();
  if (!rawLabel) return "";
  return JOURNEY_UPDATE_PARAMETER_LABEL_MAP[rawLabel] || formatJourneyUpdateDynamicFieldLabel(rawLabel);
}

const JOURNEY_UPDATE_EDITOR_FIELDS = Object.freeze([
  {
    key: "webWarning",
    parameterType: "WebWarning",
    label: getJourneyUpdateParameterDisplayLabel("WebWarning"),
    inputType: "text",
    valueType: "text"
  },
  {
    key: "routeFilterBeforeXMinutes",
    parameterType: "RouteFilterBeforeXMinutes",
    label: getJourneyUpdateParameterDisplayLabel("RouteFilterBeforeXMinutes"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "reservationOption",
    parameterType: "ReservationOption",
    label: getJourneyUpdateParameterDisplayLabel("ReservationOption"),
    inputType: "time",
    valueType: "time",
    step: "60"
  },
  {
    key: "refundOption",
    parameterType: "RefundOption",
    label: getJourneyUpdateParameterDisplayLabel("RefundOption"),
    inputType: "time",
    valueType: "time",
    step: "60"
  },
  {
    key: "maxTotalDiscountCount",
    parameterType: "MaxTotalDiscountCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxTotalDiscountCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxSingleSeatCount",
    parameterType: "MaxSingleSeatCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxSingleSeatCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxSingleFemaleCount",
    parameterType: "MaxSingleFemaleCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxSingleFemaleCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxReservationCount",
    parameterType: "MaxReservationCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxReservationCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxPointSalesCount",
    parameterType: "MaxPointSalesCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxPointSalesCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxInfantCount",
    parameterType: "MaxInfantCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxInfantCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxGuestCount",
    parameterType: "MaxGuestCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxGuestCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "maxDisabledCount",
    parameterType: "MaxDisabledCount",
    label: getJourneyUpdateParameterDisplayLabel("MaxDisabledCount"),
    inputType: "number",
    valueType: "text",
    step: "1",
    min: "0"
  },
  {
    key: "canStopAtWayStation",
    parameterType: "CanStopAtWayStation",
    label: getJourneyUpdateParameterDisplayLabel("CanStopAtWayStation"),
    inputType: "select",
    valueType: "boolean"
  },
  {
    key: "canSellThroughWeb",
    parameterType: "CanSellThroughWeb",
    label: getJourneyUpdateParameterDisplayLabel("CanSellThroughWeb"),
    inputType: "select",
    valueType: "boolean"
  },
  {
    key: "canApplyDiscount",
    parameterType: "CanApplyDiscount",
    label: getJourneyUpdateParameterDisplayLabel("CanApplyDiscount"),
    inputType: "select",
    valueType: "boolean"
  }
]);

function buildJourneyUpdateParameterColumn(columnType = "") {
  const key = normalizeJourneyUpdateParameterColumnKey(columnType);
  if (!key) return null;
  return {
    key,
    label: getJourneyUpdateParameterDisplayLabel(columnType),
    filterType: "text",
    sortType: "text"
  };
}

function buildJourneyUpdateRequestFieldAliases(fieldName = "", extraAliases = []) {
  return Array.from(
    new Set(
      [fieldName, normalizeJourneyUpdateParameterColumnKey(fieldName), ...(Array.isArray(extraAliases) ? extraAliases : [])]
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    )
  );
}

const JOURNEY_UPDATE_REQUEST_DATA_FIELD_SPECS = Object.freeze([
  {
    field: "description",
    aliases: buildJourneyUpdateRequestFieldAliases("description"),
    valueType: "string"
  },
  {
    field: "bus-id",
    aliases: buildJourneyUpdateRequestFieldAliases("bus-id"),
    valueType: "nullable-number"
  },
  {
    field: "original-bus-type-id",
    aliases: buildJourneyUpdateRequestFieldAliases("original-bus-type-id"),
    valueType: "nullable-number"
  },
  {
    field: "destination-display-name",
    aliases: buildJourneyUpdateRequestFieldAliases("destination-display-name"),
    valueType: "string"
  },
  {
    field: "is-active",
    aliases: buildJourneyUpdateRequestFieldAliases("is-active"),
    valueType: "boolean"
  },
  {
    field: "is-additional",
    aliases: buildJourneyUpdateRequestFieldAliases("is-additional"),
    valueType: "boolean"
  },
  {
    field: "duration",
    aliases: buildJourneyUpdateRequestFieldAliases("duration"),
    valueType: "nullable-string"
  },
  {
    field: "type",
    aliases: buildJourneyUpdateRequestFieldAliases("type", ["journeyType", "journey-type"]),
    valueType: "boolean"
  },
  {
    field: "distance",
    aliases: buildJourneyUpdateRequestFieldAliases("distance"),
    valueType: "nullable-number"
  },
  {
    field: "code",
    aliases: buildJourneyUpdateRequestFieldAliases("code", ["journeyCode", "journey-code"]),
    valueType: "string"
  },
  {
    field: "departure-time",
    aliases: buildJourneyUpdateRequestFieldAliases("departure-time", ["departureTime"]),
    valueType: "string"
  },
  {
    field: "id",
    aliases: buildJourneyUpdateRequestFieldAliases("id"),
    valueType: "journey-id"
  },
  {
    field: "route-id",
    aliases: buildJourneyUpdateRequestFieldAliases("route-id"),
    valueType: "nullable-number"
  },
  {
    field: "name",
    aliases: buildJourneyUpdateRequestFieldAliases("name"),
    valueType: "string"
  },
  {
    field: "extend-journey-activation",
    aliases: buildJourneyUpdateRequestFieldAliases("extend-journey-activation"),
    valueType: "boolean"
  }
]);

function cloneJsonCompatibleValue(value) {
  if (value === undefined) return undefined;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (err) {
    return undefined;
  }
}

function normalizeJourneyUpdateCellText(value) {
  const text = String(value ?? "").trim();
  if (!text || text === "-" || /^(null|undefined)$/i.test(text)) return "";
  return text;
}

function hasJourneyUpdateRowValue(value) {
  if (typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  return Boolean(normalizeJourneyUpdateCellText(value));
}

function buildJourneyUpdateRowLookup(row = {}, columns = []) {
  const lookup = new Map();
  const normalizedRow = row && typeof row === "object" && !Array.isArray(row) ? row : {};

  Object.entries(normalizedRow).forEach(([key, value]) => {
    const normalizedKey = normalizeTokenName(key);
    if (!normalizedKey || lookup.has(normalizedKey)) return;
    lookup.set(normalizedKey, value);
  });

  (Array.isArray(columns) ? columns : []).forEach((column) => {
    const columnKey = String(column?.key || "").trim();
    if (!columnKey) return;
    const value = normalizedRow[columnKey];
    const normalizedColumnKey = normalizeTokenName(columnKey);
    const normalizedColumnLabel = normalizeTokenName(column?.label);

    if (normalizedColumnKey && !lookup.has(normalizedColumnKey)) {
      lookup.set(normalizedColumnKey, value);
    }
    if (normalizedColumnLabel && !lookup.has(normalizedColumnLabel)) {
      lookup.set(normalizedColumnLabel, value);
    }
  });

  return lookup;
}

function readJourneyUpdateRowLookupValue(lookup, aliases = []) {
  const aliasList = Array.isArray(aliases) ? aliases : [aliases];
  for (const alias of aliasList) {
    const normalizedAlias = normalizeTokenName(alias);
    if (!normalizedAlias || !lookup.has(normalizedAlias)) continue;
    return lookup.get(normalizedAlias);
  }
  return undefined;
}

function parseJourneyUpdateNullableNumberValue(value) {
  const text = normalizeJourneyUpdateCellText(value);
  if (!text) return null;
  if (/^-?\d+$/.test(text)) {
    const parsedInteger = Number.parseInt(text, 10);
    if (Number.isSafeInteger(parsedInteger)) return parsedInteger;
  }
  const parsedNumber = Number(text.replace(",", "."));
  return Number.isFinite(parsedNumber) ? parsedNumber : null;
}

function parseJourneyUpdateFieldValueByType(value, valueType = "string") {
  switch (String(valueType || "").trim()) {
    case "journey-id":
      return normalizeStationPassengerJourneyIdForRequest(value);
    case "nullable-number":
      return parseJourneyUpdateNullableNumberValue(value);
    case "nullable-string": {
      const text = normalizeJourneyUpdateCellText(value);
      return text || null;
    }
    case "boolean": {
      const parsedBoolean = parseAllCompaniesBooleanValue(value);
      return parsedBoolean === null ? undefined : parsedBoolean;
    }
    case "string":
    default: {
      const text = normalizeJourneyUpdateCellText(value);
      return text || "";
    }
  }
}

function normalizeJourneyUpdateUpdateParameterEntry(item) {
  if (!item || typeof item !== "object" || Array.isArray(item)) return null;
  const type = String(item.type || "").trim();
  if (!type) return null;
  const rawValue = item.value;
  if (typeof rawValue === "boolean") {
    return { type, value: rawValue };
  }
  if (typeof rawValue === "number") {
    return Number.isFinite(rawValue) ? { type, value: rawValue } : null;
  }
  const textValue = String(rawValue ?? "").trim();
  if (!textValue) return null;
  return { type, value: textValue };
}

function normalizeJourneyUpdateUpdateParameters(parameters = []) {
  return (Array.isArray(parameters) ? parameters : []).map(normalizeJourneyUpdateUpdateParameterEntry).filter(Boolean);
}

function mergeJourneyUpdateRequestParameters(existingParameters = [], overrideParameters = []) {
  const normalizedExisting = normalizeJourneyUpdateUpdateParameters(existingParameters);
  const normalizedOverrides = normalizeJourneyUpdateUpdateParameters(overrideParameters);
  const merged = [];
  const indexByType = new Map();

  normalizedExisting.forEach((item) => {
    const normalizedType = normalizeTokenName(item.type);
    if (!normalizedType || indexByType.has(normalizedType)) return;
    indexByType.set(normalizedType, merged.length);
    merged.push(item);
  });

  normalizedOverrides.forEach((item) => {
    const normalizedType = normalizeTokenName(item.type);
    if (!normalizedType) return;
    if (indexByType.has(normalizedType)) {
      merged[indexByType.get(normalizedType)] = item;
      return;
    }
    indexByType.set(normalizedType, merged.length);
    merged.push(item);
  });

  return merged;
}

function buildJourneyUpdateUpdateDataPayload({
  journeyId = "",
  row = {},
  tableColumns = [],
  detailStateById = {},
  overrideParameters = []
} = {}) {
  const normalizedJourneyId = String(journeyId || row?.id || "").trim();
  const detailState =
    detailStateById && typeof detailStateById === "object" && !Array.isArray(detailStateById) ? detailStateById : {};
  const baseDetailData = cloneJsonCompatibleValue(detailState[normalizedJourneyId]);
  const dataPayload =
    baseDetailData && typeof baseDetailData === "object" && !Array.isArray(baseDetailData) ? baseDetailData : {};
  const rowLookup = buildJourneyUpdateRowLookup(row, tableColumns);

  JOURNEY_UPDATE_REQUEST_DATA_FIELD_SPECS.forEach((spec) => {
    const rawRowValue = readJourneyUpdateRowLookupValue(rowLookup, spec.aliases);
    if (!hasJourneyUpdateRowValue(rawRowValue)) return;
    const parsedValue = parseJourneyUpdateFieldValueByType(rawRowValue, spec.valueType);
    if (parsedValue !== undefined) {
      dataPayload[spec.field] = parsedValue;
    }
  });

  dataPayload.id = normalizeStationPassengerJourneyIdForRequest(dataPayload.id || normalizedJourneyId);
  dataPayload.parameters = mergeJourneyUpdateRequestParameters(dataPayload.parameters, overrideParameters);
  if (!Array.isArray(dataPayload.staffs)) {
    dataPayload.staffs = [];
  }

  return dataPayload;
}

function normalizeJourneyUpdateEditorInputState(source = {}) {
  const rawSource = source && typeof source === "object" ? source : {};
  return JOURNEY_UPDATE_EDITOR_FIELDS.reduce((acc, field) => {
    const rawValue = rawSource[`update_${field.key}`];
    acc[field.key] = normalizeJourneyUpdateEditorFieldValue(field, rawValue);
    return acc;
  }, {});
}

function buildJourneyUpdateEditorFieldsForView(values = {}) {
  const inputState = values && typeof values === "object" ? values : {};
  return JOURNEY_UPDATE_EDITOR_FIELDS.map((field) => ({
    ...field,
    value: normalizeJourneyUpdateEditorFieldValue(field, inputState[field.key])
  }));
}

function normalizeJourneyUpdateHourMinuteValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";

  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return text;

  const hour = Number.parseInt(match[1], 10);
  const minute = Number.parseInt(match[2], 10);
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return text;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return text;

  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function normalizeJourneyUpdateEditorFieldValue(field, value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (String(field?.valueType || "").trim() === "time") {
    return normalizeJourneyUpdateHourMinuteValue(text);
  }
  return text;
}

function parseJourneyUpdateBooleanEditorValue(value) {
  const normalized = String(value ?? "").trim().toLocaleLowerCase("tr");
  if (normalized === "true") return true;
  if (normalized === "false") return false;
  return null;
}

function buildJourneyUpdateParametersFromEditorState(values = {}) {
  const inputState = values && typeof values === "object" ? values : {};
  return JOURNEY_UPDATE_EDITOR_FIELDS.map((field) => {
    const rawValue = normalizeJourneyUpdateEditorFieldValue(field, inputState[field.key]);
    if (!rawValue) return null;
    if (field.valueType === "boolean") {
      const parsedBoolean = parseJourneyUpdateBooleanEditorValue(rawValue);
      if (parsedBoolean === null) return null;
      return {
        type: field.parameterType,
        value: parsedBoolean
      };
    }
    return {
      type: field.parameterType,
      value: rawValue
    };
  }).filter(Boolean);
}

function buildJourneyUpdateUpdateRequestDate(date = new Date()) {
  return formatDateTimeToSecondPrecisionInTimeZone(date, STATION_PASSENGER_INFO_TIME_ZONE);
}

function parseJourneyUpdateRowIdsInput(value) {
  const raw = Array.isArray(value) ? value.join(",") : String(value ?? "");
  const seen = new Set();
  return raw
    .split(",")
    .map((item) => String(item || "").trim())
    .filter((item) => {
      if (!item || seen.has(item)) return false;
      seen.add(item);
      return true;
    });
}

function encodeJsonStateToBase64(value) {
  try {
    return Buffer.from(JSON.stringify(value ?? null), "utf8").toString("base64");
  } catch (err) {
    return "";
  }
}

function parseBase64JsonState(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    return parseJsonSafe(Buffer.from(raw, "base64").toString("utf8"));
  } catch (err) {
    return null;
  }
}

function parseJourneyUpdateTableRowsState(value) {
  const parsed = parseBase64JsonState(value);
  return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object" && !Array.isArray(item)) : [];
}

function parseJourneyUpdateTableColumnsState(value) {
  const parsed = parseBase64JsonState(value);
  return Array.isArray(parsed) ? parsed.filter((item) => item && typeof item === "object" && !Array.isArray(item)) : [];
}

function parseJourneyUpdateDetailState(value) {
  const parsed = parseBase64JsonState(value);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};

  return Object.entries(parsed).reduce((acc, [key, item]) => {
    const normalizedKey = String(key || "").trim();
    const clonedItem = cloneJsonCompatibleValue(item);
    if (!normalizedKey || !clonedItem || typeof clonedItem !== "object" || Array.isArray(clonedItem)) {
      return acc;
    }
    acc[normalizedKey] = clonedItem;
    return acc;
  }, {});
}

function buildJourneyUpdateUpdateRequestHeadersText() {
  return JSON.stringify(
    {
      "Content-Type": "application/json",
      Authorization: STATION_PASSENGER_INFO_API_AUTH
    },
    null,
    2
  );
}

function buildJourneyUpdateUpdatePreviewBody({
  endpointUrl = "",
  tableRows = [],
  tableColumns = [],
  detailState = {},
  editorValues = {},
  sampleLimit = 3
} = {}) {
  const normalizedEndpointUrl = String(endpointUrl || "").trim();
  const normalizedRows = (Array.isArray(tableRows) ? tableRows : []).filter(
    (row) => row && typeof row === "object" && !Array.isArray(row)
  );
  const normalizedColumns = Array.isArray(tableColumns) ? tableColumns : [];
  const normalizedDetailState =
    detailState && typeof detailState === "object" && !Array.isArray(detailState) ? detailState : {};
  const overrideParameters = buildJourneyUpdateParametersFromEditorState(editorValues);
  const requestDate = buildJourneyUpdateUpdateRequestDate();
  const sampleBodies = normalizedRows.slice(0, Math.max(1, sampleLimit)).map((row) =>
    buildJourneyUpdateUpdateRequestBody({
      journeyId: row?.id,
      dataPayload: buildJourneyUpdateUpdateDataPayload({
        journeyId: row?.id,
        row,
        tableColumns: normalizedColumns,
        detailStateById: normalizedDetailState,
        overrideParameters
      }),
      usePlaceholders: true,
      dateValue: requestDate
    })
  );
  const preview = {
    requestUrl: normalizedEndpointUrl,
    requestCount: normalizedRows.length,
    parameterTypes: overrideParameters.map((item) => String(item.type || "").trim()).filter(Boolean),
    requests: sampleBodies
  };
  if (normalizedRows.length > sampleBodies.length) {
    preview.moreRequests = normalizedRows.length - sampleBodies.length;
  }
  return {
    requestDate,
    bodyText: JSON.stringify(preview, null, 2)
  };
}

function applyJourneyUpdateEditorStateToReport(
  report,
  { endpointUrl = "", editorValues = {}, tableRows = [], tableColumns = [], detailState = {} } = {}
) {
  const nextReport = report && typeof report === "object" ? report : buildJourneyUpdateReportModel();
  const normalizedRows = Array.isArray(tableRows) ? tableRows : [];
  const normalizedColumns = Array.isArray(tableColumns) ? tableColumns : [];
  const normalizedDetailState =
    detailState && typeof detailState === "object" && !Array.isArray(detailState) ? detailState : {};
  const normalizedEditorValues = normalizeJourneyUpdateEditorInputState(editorValues);
  const journeyIds = normalizedRows
    .map((row) => String(row?.id || "").trim())
    .filter((idValue) => Boolean(idValue) && idValue !== "-");
  const preview = buildJourneyUpdateUpdatePreviewBody({
    endpointUrl,
    tableRows: normalizedRows,
    tableColumns: normalizedColumns,
    detailState: normalizedDetailState,
    editorValues: normalizedEditorValues
  });

  nextReport.updateEndpointUrl = String(endpointUrl || "").trim();
  nextReport.updateRequestHeaders = buildJourneyUpdateUpdateRequestHeadersText();
  nextReport.updateRequestBodyPreview = preview.bodyText;
  nextReport.updateRequestDate = preview.requestDate;
  nextReport.updateEditorValues = normalizedEditorValues;
  nextReport.updateJourneyIdsCsv = journeyIds.join(",");
  nextReport.updateDetailState = encodeJsonStateToBase64(normalizedDetailState);
  nextReport.tableRowsState = encodeJsonStateToBase64(normalizedRows);
  nextReport.tableColumnsState = encodeJsonStateToBase64(normalizedColumns);
  return nextReport;
}

function isJourneyUpdateSummaryNode(node) {
  if (!node || typeof node !== "object" || Array.isArray(node)) return false;
  const idValue = readPartnerRawValueByAliases(node, ["id"]);
  const departureValue = readPartnerRawValueByAliases(node, ["departure-time", "departure_time", "departureTime"]);
  if (idValue === undefined || departureValue === undefined) return false;

  const extraFieldCount = [
    readPartnerRawValueByAliases(node, ["origin"]),
    readPartnerRawValueByAliases(node, ["destination"]),
    readPartnerRawValueByAliases(node, ["status"]),
    readPartnerRawValueByAliases(node, ["journey-code", "journey_code", "journeyCode"]),
    readPartnerRawValueByAliases(node, ["description"]),
    readPartnerRawValueByAliases(node, ["seat-model", "seat_model", "seatModel"]),
    readPartnerRawValueByAliases(node, ["plate"]),
    readPartnerRawValueByAliases(node, ["journey-type", "journey_type", "journeyType"]),
    readPartnerRawValueByAliases(node, ["route-info", "route_info", "routeInfo"]),
    readPartnerRawValueByAliases(node, ["duration"]),
    readPartnerRawValueByAliases(node, ["distance"]),
    readPartnerRawValueByAliases(node, ["last-extend-time", "last_extend_time", "lastExtendTime"])
  ].filter((value) => value !== undefined).length;

  return extraFieldCount >= 2;
}

function buildJourneyUpdateBaseColumnNormalizedKeySet() {
  return new Set(buildJourneyUpdateBaseColumns().map((column) => normalizeTokenName(column?.key)));
}

function extractJourneyUpdateSummaryNodes(payload) {
  const collected = [];
  const visited = new Set();

  const walk = (node, depth = 0) => {
    if (depth > 8 || node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) walk(parsed, depth + 1);
      }
      return;
    }
    if (typeof node !== "object") return;
    if (visited.has(node)) return;
    visited.add(node);

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1));
      return;
    }

    if (isJourneyUpdateSummaryNode(node)) {
      collected.push(node);
      return;
    }

    Object.values(node).forEach((value) => walk(value, depth + 1));
  };

  walk(payload);
  return collected;
}

function resolveJourneyUpdateDetailRecord(node) {
  if (!node || typeof node !== "object" || Array.isArray(node)) return null;

  const directIdValue = readPartnerRawValueByAliases(node, ["id"]);
  const directParametersValue = readPartnerRawValueByAliases(node, ["parameters", "parameter", "params"]);
  if (directIdValue !== undefined && Array.isArray(directParametersValue)) {
    return {
      id: String(directIdValue || "").trim(),
      parameters: directParametersValue,
      fieldSource: node
    };
  }

  const nestedDataValue = readPartnerRawValueByAliases(node, ["data"]);
  if (nestedDataValue && typeof nestedDataValue === "object" && !Array.isArray(nestedDataValue)) {
    const nestedIdValue = readPartnerRawValueByAliases(nestedDataValue, ["id"]);
    const nestedParametersValue = readPartnerRawValueByAliases(nestedDataValue, ["parameters", "parameter", "params"]);
    if (nestedIdValue !== undefined && Array.isArray(nestedParametersValue)) {
      return {
        id: String(nestedIdValue || "").trim(),
        parameters: nestedParametersValue,
        fieldSource: nestedDataValue
      };
    }
  }

  return null;
}

function isJourneyUpdateDetailNode(node) {
  return Boolean(resolveJourneyUpdateDetailRecord(node));
}

function extractJourneyUpdateDetailNodes(payload) {
  const collected = [];
  const visited = new Set();

  const walk = (node, depth = 0) => {
    if (depth > 8 || node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      if (!trimmed) return;
      if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
        const parsed = parseJsonSafe(trimmed);
        if (parsed !== null) walk(parsed, depth + 1);
      }
      return;
    }
    if (typeof node !== "object") return;
    if (visited.has(node)) return;
    visited.add(node);

    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, depth + 1));
      return;
    }

    if (isJourneyUpdateDetailNode(node)) {
      collected.push(node);
      return;
    }

    Object.values(node).forEach((value) => walk(value, depth + 1));
  };

  walk(payload);
  return collected;
}

function extractJourneyUpdateParameterEntries(parameters) {
  if (!Array.isArray(parameters)) return [];

  return parameters
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const typeLabel = String(
        readPartnerRawValueByAliases(item, ["type", "parameter-type", "parameter_type", "parameterType", "name"]) || ""
      ).trim();
      if (!typeLabel) return null;
      const columnKey = normalizeJourneyUpdateParameterColumnKey(typeLabel);
      if (!columnKey) return null;
      const value =
        readPartnerRawValueByAliases(item, ["value", "parameter-value", "parameter_value", "parameterValue", "data"]) ??
        "";
      return {
        type: typeLabel,
        key: columnKey,
        label: getJourneyUpdateParameterDisplayLabel(typeLabel),
        value: formatJourneyUpdateTableCell(value)
      };
    })
    .filter(Boolean);
}

function isJourneyUpdateDetailScalarValue(value) {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}

function extractJourneyUpdateDirectFieldEntries(detailRecord) {
  const fieldSource = detailRecord?.fieldSource;
  if (!fieldSource || typeof fieldSource !== "object" || Array.isArray(fieldSource)) return [];

  const baseColumnKeySet = buildJourneyUpdateBaseColumnNormalizedKeySet();

  return Object.entries(fieldSource)
    .map(([fieldKey, fieldValue]) => {
      const normalizedFieldKey = normalizeTokenName(fieldKey);
      if (!normalizedFieldKey) return null;
      if (["id", "parameters", "parameter", "params"].includes(normalizedFieldKey)) return null;
      if (baseColumnKeySet.has(normalizedFieldKey)) return null;
      if (!isJourneyUpdateDetailScalarValue(fieldValue)) return null;

      const column = buildJourneyUpdateParameterColumn(fieldKey);
      if (!column) return null;

      return {
        type: fieldKey,
        key: column.key,
        label: column.label,
        value: formatJourneyUpdateTableCell(fieldValue)
      };
    })
    .filter(Boolean);
}

function buildJourneyUpdateDetailMap(payload) {
  const detailsById = new Map();
  const detailColumnsByKey = new Map();
  const detailDataById = {};

  extractJourneyUpdateDetailNodes(payload).forEach((node) => {
    const detailRecord = resolveJourneyUpdateDetailRecord(node);
    const idText = String(detailRecord?.id || "").trim();
    if (!idText) return;
    const directFieldEntries = extractJourneyUpdateDirectFieldEntries(detailRecord);
    const parameterEntries = extractJourneyUpdateParameterEntries(detailRecord?.parameters);
    const detailEntries = directFieldEntries.concat(parameterEntries);
    if (detailEntries.length === 0) return;

    const existing = detailsById.get(idText) || {};
    const clonedDetailData = cloneJsonCompatibleValue(detailRecord?.fieldSource);
    if (clonedDetailData && typeof clonedDetailData === "object" && !Array.isArray(clonedDetailData)) {
      if (clonedDetailData.id === undefined || clonedDetailData.id === null || String(clonedDetailData.id).trim() === "") {
        clonedDetailData.id = normalizeStationPassengerJourneyIdForRequest(idText);
      }
      detailDataById[idText] = clonedDetailData;
    }

    detailEntries.forEach((entry) => {
      detailColumnsByKey.set(entry.key, buildJourneyUpdateParameterColumn(entry.type) || {
        key: entry.key,
        label: entry.label,
        filterType: "text",
        sortType: "text"
      });
      const previousValue = String(existing[entry.key] || "").trim();
      if (!previousValue) {
        existing[entry.key] = entry.value;
      } else if (!previousValue.split(" | ").includes(entry.value)) {
        existing[entry.key] = `${previousValue} | ${entry.value}`;
      }
    });
    detailsById.set(idText, existing);
  });

  return {
    detailsById,
    detailDataById,
    detailColumns: Array.from(detailColumnsByKey.values()).sort((a, b) =>
      String(a.label || "").localeCompare(String(b.label || ""), "tr")
    )
  };
}

function buildJourneyUpdateTableRows(payload, { requestDate = "", companyCode = "", partnerId = "", cluster = "" } = {}) {
  return extractJourneyUpdateSummaryNodes(payload).map((raw) => {
    return {
      requestDate: String(requestDate || "").trim(),
      companyCode: String(companyCode || "").trim(),
      partnerId: String(partnerId || "").trim(),
      cluster: extractClusterLabel(cluster),
      id: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["id"])),
      departureTime: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["departure-time", "departure_time", "departureTime"])
      ),
      origin: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["origin"])),
      destination: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["destination"])),
      status: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["status"])),
      journeyCode: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["journey-code", "journey_code", "journeyCode"])
      ),
      description: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["description"])),
      seatModel: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["seat-model", "seat_model", "seatModel"])
      ),
      plate: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["plate"])),
      journeyType: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["journey-type", "journey_type", "journeyType"])
      ),
      routeInfo: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["route-info", "route_info", "routeInfo"])
      ),
      duration: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["duration"])),
      distance: formatJourneyUpdateTableCell(readPartnerRawValueByAliases(raw, ["distance"])),
      lastExtendTime: formatJourneyUpdateTableCell(
        readPartnerRawValueByAliases(raw, ["last-extend-time", "last_extend_time", "lastExtendTime"])
      )
    };
  });
}

function mergeJourneyUpdateRowDetails(rows, detailsById) {
  const detailMap = detailsById instanceof Map ? detailsById : new Map();
  return (Array.isArray(rows) ? rows : []).map((row) => {
    const idText = String(row?.id || "").trim();
    const detailValues = idText ? detailMap.get(idText) || {} : {};
    return {
      ...row,
      ...detailValues
    };
  });
}

function buildJourneyUpdateTableColumns(detailColumns = []) {
  return buildJourneyUpdateBaseColumns().concat(Array.isArray(detailColumns) ? detailColumns : []);
}

function mergeJourneyUpdateColumnsWithParameters(existingColumns, parameters) {
  const baseColumns = buildJourneyUpdateBaseColumns();
  const baseKeys = new Set(baseColumns.map((column) => String(column.key || "").trim()));
  const dynamicColumnsByKey = new Map();

  (Array.isArray(existingColumns) ? existingColumns : []).forEach((column) => {
    const key = String(column?.key || "").trim();
    if (!key || baseKeys.has(key)) return;
    dynamicColumnsByKey.set(key, column);
  });

  (Array.isArray(parameters) ? parameters : []).forEach((parameter) => {
    const column = buildJourneyUpdateParameterColumn(parameter?.type);
    if (!column) return;
    dynamicColumnsByKey.set(column.key, column);
  });

  return baseColumns.concat(
    Array.from(dynamicColumnsByKey.values()).sort((a, b) =>
      String(a?.label || "").localeCompare(String(b?.label || ""), "tr")
    )
  );
}

function applyJourneyUpdateParametersToRows(rows, parameters, successfulIds) {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  const normalizedParameters = Array.isArray(parameters) ? parameters : [];
  const targetIds = new Set(parseJourneyUpdateRowIdsInput(successfulIds));
  if (!targetIds.size || !normalizedParameters.length) {
    return normalizedRows.slice();
  }

  const parameterEntries = normalizedParameters
    .map((parameter) => {
      const column = buildJourneyUpdateParameterColumn(parameter?.type);
      if (!column) return null;
      return {
        columnKey: column.key,
        value: formatJourneyUpdateTableCell(parameter?.value)
      };
    })
    .filter(Boolean);

  return normalizedRows.map((row) => {
    const rowId = String(row?.id || "").trim();
    if (!rowId || !targetIds.has(rowId)) return row;
    const nextRow = { ...row };
    parameterEntries.forEach((entry) => {
      nextRow[entry.columnKey] = entry.value;
    });
    return nextRow;
  });
}

function applyJourneyUpdateParametersToDetailState(detailStateById, parameters, successfulIds) {
  const normalizedDetailState =
    detailStateById && typeof detailStateById === "object" && !Array.isArray(detailStateById) ? detailStateById : {};
  const normalizedParameters = normalizeJourneyUpdateUpdateParameters(parameters);
  const targetIds = new Set(parseJourneyUpdateRowIdsInput(successfulIds));
  if (!targetIds.size || !normalizedParameters.length) {
    return { ...normalizedDetailState };
  }

  return Object.entries(normalizedDetailState).reduce((acc, [id, rawDetailData]) => {
    const normalizedId = String(id || "").trim();
    const clonedDetailData = cloneJsonCompatibleValue(rawDetailData);
    if (!normalizedId || !clonedDetailData || typeof clonedDetailData !== "object" || Array.isArray(clonedDetailData)) {
      return acc;
    }
    if (targetIds.has(normalizedId)) {
      clonedDetailData.parameters = mergeJourneyUpdateRequestParameters(clonedDetailData.parameters, normalizedParameters);
    }
    acc[normalizedId] = clonedDetailData;
    return acc;
  }, {});
}

function dedupeJourneyUpdateTableRows(rows) {
  const uniqueRows = new Map();
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const key = Object.keys(row || {})
      .sort((a, b) => a.localeCompare(b, "tr"))
      .map((fieldKey) => `${fieldKey}:${formatJourneyUpdateTableCell(row?.[fieldKey])}`)
      .join("|||")
      .toLocaleLowerCase("tr");
    if (!uniqueRows.has(key)) {
      uniqueRows.set(key, row);
    }
  });
  return Array.from(uniqueRows.values());
}

function sortJourneyUpdateTableRows(rows) {
  return (Array.isArray(rows) ? rows : []).slice().sort((a, b) => {
    const aDateKey = buildStationPassengerComparableDateTimeKey(a?.departureTime);
    const bDateKey = buildStationPassengerComparableDateTimeKey(b?.departureTime);
    if (aDateKey && bDateKey && aDateKey !== bDateKey) {
      return aDateKey.localeCompare(bDateKey, "tr");
    }
    if (aDateKey && !bDateKey) return -1;
    if (!aDateKey && bDateKey) return 1;

    const byId = String(a?.id || "").localeCompare(String(b?.id || ""), "tr");
    if (byId !== 0) return byId;

    const byRoute = String(a?.routeInfo || "").localeCompare(String(b?.routeInfo || ""), "tr");
    if (byRoute !== 0) return byRoute;

    return String(a?.plate || "").localeCompare(String(b?.plate || ""), "tr");
  });
}

function buildObusJobsReportModel() {
  return {
    requested: false,
    error: null,
    clusterResults: [],
    clusterRows: [],
    jobIds: [],
    jobColumns: [],
    clusterCount: 0,
    totalJobCount: 0,
    successClusterCount: 0,
    errorClusterCount: 0,
    successMessages: [],
    warningMessages: [],
    saveResult: null,
    slackResult: null
  };
}

function summarizeObusJobsReport(report) {
  const clusterResults = Array.isArray(report?.clusterResults) ? report.clusterResults : [];
  const requestedClusterCount = clusterResults.length;
  const errorClusterCount = clusterResults.filter((item) => String(item?.error || "").trim()).length;
  const successClusterCount = Math.max(0, requestedClusterCount - errorClusterCount);
  const jobItemCount = clusterResults.reduce((sum, item) => {
    const jobs = Array.isArray(item?.jobs) ? item.jobs : [];
    return sum + jobs.length;
  }, 0);
  const errorSamples = clusterResults
    .filter((item) => String(item?.error || "").trim())
    .slice(0, 3)
    .map((item) => `${item.clusterLabel}: ${String(item.error || "").trim()}`);

  return {
    requestedClusterCount,
    successClusterCount,
    errorClusterCount,
    jobColumnCount: Number(report?.totalJobCount || 0) || 0,
    jobItemCount,
    errorSamples
  };
}

function buildObusJobsRequestBody({ sessionId = "", deviceId = "", token = "" } = {}) {
  return {
    data: null,
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    token: String(token || "").trim(),
    date: OBUS_JOBS_REQUEST_DATE,
    language: OBUS_JOBS_REQUEST_LANGUAGE
  };
}

function formatObusJobsLastExecution(value) {
  const raw = String(value || "").trim();
  if (!raw) return "-";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  try {
    return new Intl.DateTimeFormat("tr-TR", {
      dateStyle: "short",
      timeStyle: "medium"
    }).format(parsed);
  } catch (err) {
    return raw;
  }
}

function isDateYesterdayFromToday(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return false;

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);

  const target = new Date(parsed);
  target.setHours(0, 0, 0, 0);
  return target.getTime() === yesterday.getTime();
}

function isDateBeforeToday(value) {
  const raw = String(value || "").trim();
  if (!raw) return false;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return false;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const target = new Date(parsed);
  target.setHours(0, 0, 0, 0);
  return target.getTime() < today.getTime();
}

function parseTimeToNextDelay(value) {
  const now = new Date();
  const match = String(value || "").match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  const hour = match ? Number(match[1]) : 10;
  const minute = match ? Number(match[2]) : 0;
  const target = new Date(now);
  target.setHours(hour, minute, 0, 0);
  if (target.getTime() <= now.getTime()) {
    target.setDate(target.getDate() + 1);
  }
  return target.getTime() - now.getTime();
}

function extractObusJobsItems(payload) {
  const rows = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.Data)
      ? payload.Data
      : [];

  return rows
    .map((item) => {
      const id = String(item?.ID ?? item?.Id ?? item?.id ?? "").trim();
      if (!id) return null;
      const lastExecution = String(item?.LastExecution ?? item?.lastExecution ?? item?.lastexecution ?? "").trim();
      const lastJobState = String(item?.LastJobState ?? item?.lastJobState ?? item?.lastjobstate ?? "").trim() || "-";
      const nameValue = String(
        item?.Name ??
          item?.name ??
          item?.Label ??
          item?.label ??
          item?.JobName ??
          item?.jobName ??
          id
      ).trim();
      return {
        id,
        lastExecution,
        lastExecutionText: formatObusJobsLastExecution(lastExecution),
        lastJobState,
        isYesterday: isDateYesterdayFromToday(lastExecution)
        ,
        columnName: nameValue,
        isPastExecution: isDateBeforeToday(lastExecution)
      };
    })
    .filter(Boolean);
}

function extractScheduledTaskNameHint(taskHint = "") {
  const raw = String(taskHint || "").trim();
  if (!raw) return "";

  const match = raw.match(/^(.*)-([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i);
  return String(match?.[1] || raw).trim();
}

function buildUetdsScheduledTaskHintCandidates(taskHint = "") {
  const candidates = [String(taskHint || "").trim(), extractScheduledTaskNameHint(taskHint)];
  const seen = new Set();

  return candidates
    .map((value) => String(value || "").trim())
    .filter((value) => {
      const normalized = normalizeTokenName(value);
      if (!normalized || seen.has(normalized)) return false;
      seen.add(normalized);
      return true;
    });
}

function scoreScheduledTaskMatch(value = "", normalizedHints = []) {
  const normalizedValue = normalizeTokenName(value);
  if (!normalizedValue) return 0;

  let bestScore = 0;
  normalizedHints.forEach((hint, index) => {
    if (!hint) return;
    const indexPenalty = index * 4;
    if (normalizedValue === hint) {
      bestScore = Math.max(bestScore, 120 - indexPenalty);
      return;
    }
    if (normalizedValue.startsWith(hint)) {
      bestScore = Math.max(bestScore, 90 - indexPenalty);
      return;
    }
    if (normalizedValue.includes(hint)) {
      bestScore = Math.max(bestScore, 60 - indexPenalty);
    }
  });

  return bestScore;
}

function buildScheduledTaskSampleText(jobs = [], maxCount = 5) {
  return (Array.isArray(jobs) ? jobs : [])
    .slice(0, maxCount)
    .map((job) => String(job?.columnName || job?.id || "").trim())
    .filter(Boolean)
    .join(", ");
}

function resolveUetdsScheduledTaskItem(jobs = [], taskHint = UETDS_PRICES_TASK_HINT) {
  const items = (Array.isArray(jobs) ? jobs : []).filter((job) => String(job?.id || "").trim());
  if (items.length === 0) {
    return {
      item: null,
      error: "getscheduledtasks boş döndü."
    };
  }

  const hintCandidates = buildUetdsScheduledTaskHintCandidates(taskHint);
  const normalizedHints = hintCandidates.map((item) => normalizeTokenName(item)).filter(Boolean);
  if (normalizedHints.length === 0) {
    return {
      item: null,
      error: "UETDS scheduled task eşleştirme anahtarı boş."
    };
  }

  const scoredItems = items
    .map((job) => {
      const id = String(job?.id || "").trim();
      const label = String(job?.columnName || "").trim();
      return {
        job,
        score: Math.max(scoreScheduledTaskMatch(id, normalizedHints), scoreScheduledTaskMatch(label, normalizedHints)),
        textLength: `${label} ${id}`.trim().length
      };
    })
    .filter((item) => item.score > 0)
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      if (left.textLength !== right.textLength) return left.textLength - right.textLength;
      return String(left.job?.id || "").localeCompare(String(right.job?.id || ""), "tr", {
        numeric: true,
        sensitivity: "base"
      });
    });

  if (scoredItems.length === 0) {
    const hintText = hintCandidates.join(" / ");
    const sampleText = buildScheduledTaskSampleText(items);
    return {
      item: null,
      error: `UETDS scheduled task bulunamadı. Hint=${hintText || "-"}.${sampleText ? ` Örnek tasklar: ${sampleText}` : ""}`
    };
  }

  const [bestMatch, nextMatch] = scoredItems;
  const bestMatchId = String(bestMatch?.job?.id || "").trim();
  const nextMatchId = String(nextMatch?.job?.id || "").trim();
  if (
    nextMatch &&
    nextMatch.score === bestMatch.score &&
    bestMatchId &&
    nextMatchId &&
    bestMatchId !== nextMatchId
  ) {
    const ambiguousItems = scoredItems
      .filter((item) => item.score === bestMatch.score)
      .slice(0, 3)
      .map((item) => String(item.job?.columnName || item.job?.id || "").trim())
      .filter(Boolean)
      .join(", ");
    return {
      item: null,
      error: `Birden fazla scheduled task eşleşti: ${ambiguousItems || `${bestMatchId}, ${nextMatchId}`}`
    };
  }

  return {
    item: bestMatch.job,
    error: null
  };
}

async function fetchObusScheduledTasksReport({ endpointUrl, sessionId, deviceId, token }) {
  const requestUrl = buildObusJobsUrl(endpointUrl || OBUS_JOBS_API_URL);
  const normalizedRequestUrl = normalizeTargetUrl(requestUrl);
  if (!normalizedRequestUrl) {
    return {
      requested: true,
      status: null,
      error: "getscheduledtasks URL oluşturulamadı.",
      requestUrl: requestUrl || "",
      requestBody: "{}",
      responseBody: "",
      jobs: []
    };
  }

  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();

  if (!normalizedSessionId || !normalizedDeviceId) {
    return {
      requested: true,
      status: null,
      error: "getscheduledtasks için session/device bilgisi bulunamadı.",
      requestUrl: normalizedRequestUrl,
      requestBody: "{}",
      responseBody: "",
      jobs: []
    };
  }

  if (!normalizedToken) {
    return {
      requested: true,
      status: null,
      error: "getscheduledtasks için token zorunludur.",
      requestUrl: normalizedRequestUrl,
      requestBody: "{}",
      responseBody: "",
      jobs: []
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_JOBS_TIMEOUT_MS);
  let requestBody = "{}";

  try {
    const body = buildObusJobsRequestBody({
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      token: normalizedToken
    });
    requestBody = JSON.stringify(body, null, 2);

    const response = await fetch(normalizedRequestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: PARTNERS_API_AUTH
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBody =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        requested: true,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        requestUrl: normalizedRequestUrl,
        requestBody,
        responseBody: responseBody || "{}",
        jobs: []
      };
    }

    const hasExplicitStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      const reason =
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
      return {
        requested: true,
        status: response.status,
        error: reason,
        requestUrl: normalizedRequestUrl,
        requestBody,
        responseBody: responseBody || "{}",
        jobs: []
      };
    }

    return {
      requested: true,
      status: response.status,
      error: null,
      requestUrl: normalizedRequestUrl,
      requestBody,
      responseBody: responseBody || "{}",
      jobs: extractObusJobsItems(parsed)
    };
  } catch (err) {
    return {
      requested: true,
      status: null,
      error: err?.message || "getscheduledtasks isteği gönderilemedi.",
      requestUrl: normalizedRequestUrl,
      requestBody,
      responseBody: "",
      jobs: []
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function resolveUetdsPricesTaskData({ endpointUrl, sessionId, deviceId, token }) {
  const scheduledTasksReport = await fetchObusScheduledTasksReport({
    endpointUrl,
    sessionId,
    deviceId,
    token
  });

  if (scheduledTasksReport.error) {
    return {
      ok: false,
      error: scheduledTasksReport.error,
      requestUrl: scheduledTasksReport.requestUrl,
      requestBody: scheduledTasksReport.requestBody,
      responseBody: scheduledTasksReport.responseBody,
      status: scheduledTasksReport.status,
      taskData: ""
    };
  }

  const taskMatch = resolveUetdsScheduledTaskItem(scheduledTasksReport.jobs, UETDS_PRICES_TASK_HINT);
  if (!taskMatch?.item) {
    return {
      ok: false,
      error: taskMatch?.error || "UETDS scheduled task bulunamadı.",
      requestUrl: scheduledTasksReport.requestUrl,
      requestBody: scheduledTasksReport.requestBody,
      responseBody: scheduledTasksReport.responseBody,
      status: scheduledTasksReport.status,
      taskData: ""
    };
  }

  return {
    ok: true,
    error: null,
    requestUrl: scheduledTasksReport.requestUrl,
    requestBody: scheduledTasksReport.requestBody,
    responseBody: scheduledTasksReport.responseBody,
    status: scheduledTasksReport.status,
    taskData: String(taskMatch.item.id || "").trim()
  };
}

function buildObusJobsTableModel(clusterResults) {
  const jobIds = Array.from(
    new Set(
      (Array.isArray(clusterResults) ? clusterResults : []).flatMap((result) =>
        Array.isArray(result?.jobs) ? result.jobs.map((job) => String(job?.id || "").trim()).filter(Boolean) : []
      )
    )
  ).sort((left, right) => left.localeCompare(right, "tr", { numeric: true, sensitivity: "base" }));

  const jobDetails = (Array.isArray(clusterResults) ? clusterResults : [])
    .flatMap((result) => (Array.isArray(result?.jobs) ? result.jobs : []))
    .filter((job) => String(job?.id || "").trim())
    .reduce((map, job) => {
      const id = String(job.id).trim();
      if (!id) return map;
      const label = String(job.columnName || job.Name || job.name || job.label || "-").trim() || id;
      const key = label.toLowerCase();
      if (!key) return map;
      if (!map.has(key)) {
        map.set(key, {
          id,
          label,
          key
        });
      }
      return map;
    }, new Map());

  const jobColumns = Array.from(jobDetails.values()).sort((left, right) =>
    left.label.localeCompare(right.label, "tr", { sensitivity: "base" })
  );

  const clusterRows = (Array.isArray(clusterResults) ? clusterResults : []).map((result) => {
    const jobsById = {};
    const jobsByLabel = {};
    (Array.isArray(result?.jobs) ? result.jobs : []).forEach((job) => {
      const id = String(job?.id || "").trim();
      if (!id) return;
      jobsById[id] = job;
      const label = String(job.columnName || job.Name || job.name || job.label || id).trim();
      if (label) {
        jobsByLabel[label.toLowerCase()] = job;
      }
    });
    return {
      clusterLabel: String(result?.clusterLabel || "").trim() || "cluster",
      error: String(result?.error || "").trim(),
      jobsById,
      jobsByLabel
    };
  });

  return {
    clusterRows,
    jobIds,
    jobColumns,
    clusterCount: clusterRows.length,
    totalJobCount: jobIds.length
  };
}

function shuffleArray(items = []) {
  const list = Array.isArray(items) ? [...items] : [];
  for (let i = list.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [list[i], list[j]] = [list[j], list[i]];
  }
  return list;
}

function buildClusterCompanyCandidates(partnerItems = []) {
  const map = new Map();
  (Array.isArray(partnerItems) ? partnerItems : []).forEach((item) => {
    const cluster = String((item?.cluster || "").trim()).toLowerCase();
    if (!cluster) return;
    const clusterLabel = cluster.startsWith("cluster") ? cluster : `cluster${cluster}`;
    const list = map.get(clusterLabel) || [];
    list.push({
      code: String(item?.code || "").trim(),
      id: String(item?.id || "").trim(),
      cluster: clusterLabel,
      url: String(item?.url || "").trim(),
      branchId: String(item?.branchId || item?.id || "").trim()
    });
    map.set(clusterLabel, list);
  });
  return map;
}

function buildObusUserDeactivateReportModel() {
  return {
    requested: false,
    status: null,
    error: "",
    errorDetail: "",
    userMessage: "",
    requestUrl: "",
    requestBody: "{}",
    responseBody: "",
    rows: [],
    scannedCompanyCount: 0,
    successCompanyCount: 0,
    failureCompanyCount: 0,
    totalUserCount: 0,
    activeUserCount: 0,
    listedUserCount: 0,
    matchedUserCount: 0
  };
}

function buildObusUserDeactivateRequestBody({ sessionId = "", deviceId = "", token = "", usePlaceholders = false } = {}) {
  return {
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    date: OBUS_USER_DEACTIVATE_REQUEST_DATE,
    language: "tr-TR",
    token: usePlaceholders ? "{{token}}" : String(token || "").trim()
  };
}

function buildObusUserDeleteRequestBody({
  userIds = [],
  sessionId = "",
  deviceId = "",
  token = "",
  usePlaceholders = false
} = {}) {
  const normalizedIds = (Array.isArray(userIds) ? userIds : [userIds])
    .map((item) => normalizeObusPartnerIdValue(item))
    .filter((item) => Number.isInteger(item) && item > 0);

  return {
    data: usePlaceholders ? ["{{userId}}"] : normalizedIds,
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    token: usePlaceholders ? "{{token}}" : String(token || "").trim(),
    date: OBUS_USER_DELETE_REQUEST_DATE,
    language: "tr-TR"
  };
}

function normalizeObusUserDeactivateUsername(value) {
  return String(value || "").trim().toLocaleLowerCase("tr");
}

function isObusUserDeactivateCandidateNode(node) {
  if (!node || typeof node !== "object" || Array.isArray(node)) return false;
  const usernameValue = readPartnerRawValueByAliases(node, ["username", "user-name", "user_name", "userName"]);
  const idValue = readPartnerRawValueByAliases(node, ["id", "user-id", "user_id", "userId", "userid"]);
  const activeValue = readPartnerRawValueByAliases(node, ["is-active", "is_active", "isactive", "isActive"]);
  const fullNameValue = readPartnerRawValueByAliases(node, ["full-name", "full_name", "fullName", "fullname", "name"]);
  return [usernameValue, idValue, activeValue, fullNameValue].some((value) => {
    if (value === undefined || value === null) return false;
    return String(value).trim() !== "";
  });
}

function extractObusUserDeactivateRows(payload) {
  const directSource = payload && typeof payload === "object" ? payload.data ?? payload.Data ?? null : null;
  const parsedSource =
    typeof directSource === "string" ? parseJsonSafe(directSource) ?? directSource : directSource;
  const candidates = [];
  const pushCandidate = (dataKey, node) => {
    if (!node || typeof node !== "object" || Array.isArray(node)) return;
    candidates.push({
      dataKey: String(dataKey || "").trim(),
      node
    });
  };

  if (Array.isArray(parsedSource)) {
    parsedSource.forEach((item, index) => {
      pushCandidate(`data[${index}]`, item);
    });
  } else if (parsedSource && typeof parsedSource === "object") {
    Object.entries(parsedSource).forEach(([key, value]) => {
      const normalizedKey = normalizeTokenName(key);
      if (normalizedKey.startsWith("id")) {
        if (Array.isArray(value)) {
          value.forEach((item, index) => {
            pushCandidate(`${key}[${index}]`, item);
          });
        } else {
          pushCandidate(key, value);
        }
        return;
      }

      if (Array.isArray(value)) {
        value.forEach((item, index) => {
          if (isObusUserDeactivateCandidateNode(item)) {
            pushCandidate(`${key}[${index}]`, item);
          }
        });
        return;
      }

      if (isObusUserDeactivateCandidateNode(value)) {
        pushCandidate(key, value);
      }
    });
  }

  if (candidates.length === 0 && parsedSource && typeof parsedSource === "object") {
    const walk = (node, path = "data") => {
      if (!node || typeof node !== "object") return;
      if (Array.isArray(node)) {
        node.forEach((item, index) => {
          walk(item, `${path}[${index}]`);
        });
        return;
      }

      if (isObusUserDeactivateCandidateNode(node)) {
        pushCandidate(path, node);
      }

      Object.entries(node).forEach(([key, value]) => {
        walk(value, path ? `${path}.${key}` : key);
      });
    };

    walk(parsedSource);
  }

  const deduped = new Map();
  candidates.forEach(({ dataKey, node }) => {
    const username = formatPartnerCellValue(
      readPartnerRawValueByAliases(node, ["username", "user-name", "user_name", "userName"])
    ).trim();
    if (!username) return;

    const userIdValue = formatPartnerCellValue(
      readPartnerRawValueByAliases(node, ["id", "user-id", "user_id", "userId", "userid"])
    ).trim();
    const partnerIdValue = formatPartnerCellValue(
      readPartnerRawValueByAliases(node, ["partner-id", "partner_id", "partnerid", "partnerId", "partnerID"])
    ).trim();
    const fullName = formatPartnerCellValue(
      readPartnerRawValueByAliases(node, ["full-name", "full_name", "fullName", "fullname", "name"])
    ).trim();
    const activeRaw = readPartnerRawValueByAliases(node, ["is-active", "is_active", "isactive", "isActive"]);
    const isActive = parseAllCompaniesBooleanValue(activeRaw);
    const userId = userIdValue || dataKey || username;
    const row = {
      dataKey: dataKey || userId,
      userId,
      partnerId: partnerIdValue,
      username,
      fullName,
      isActive,
      isActiveText: isActive === null ? formatPartnerCellValue(activeRaw).trim() || "-" : isActive ? "true" : "false"
    };
    const dedupeKey = [
      normalizeObusUserDeactivateUsername(row.username),
      String(row.userId || "").trim(),
      String(row.partnerId || "").trim(),
      String(row.dataKey || "").trim()
    ].join("|||");
    if (!deduped.has(dedupeKey)) {
      deduped.set(dedupeKey, row);
    }
  });

  return Array.from(deduped.values()).sort((a, b) => {
    const byUsername = String(a.username || "").localeCompare(String(b.username || ""), "tr");
    if (byUsername !== 0) return byUsername;
    const byPartnerId = String(a.partnerId || "").localeCompare(String(b.partnerId || ""), "tr");
    if (byPartnerId !== 0) return byPartnerId;
    return String(a.userId || "").localeCompare(String(b.userId || ""), "tr");
  });
}

function buildObusUserDeactivateCompanyBaseUrl(company = {}, clusterLabel = "") {
  const normalizedCluster =
    normalizeObusClusterLabel(clusterLabel) ||
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_DEACTIVATE_API_URL)) ||
    "cluster4";

  return (
    normalizeTargetUrl(buildUrlForCluster(OBUS_USER_DEACTIVATE_API_URL, normalizedCluster)) ||
    normalizeTargetUrl(OBUS_USER_DEACTIVATE_API_URL)
  );
}

function getObusUserDeactivateSqlPassword() {
  return resolveObusUserDeactivateSecret(OBUS_USER_DEACTIVATE_SQL_PASSWORD_SECRET_NAMES, {
    trim: false,
    fallback: OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.password || ""
  });
}

function getObusUserDeactivateSqlProxyToken() {
  return resolveObusUserDeactivateSecret(OBUS_USER_DEACTIVATE_SQL_PROXY_TOKEN_SECRET_NAMES, { trim: false });
}

function resolveObusUserDeactivateSecret(secretNames = [], { trim = true, fallback = "" } = {}) {
  const names = (Array.isArray(secretNames) ? secretNames : [secretNames])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  if (names.length === 0) return "";

  for (const secretName of names) {
    const keychainValue = readMacOsKeychainSecret(secretName, { trim });
    if (keychainValue && !isObusUserDeactivatePlaceholderConfigValue(keychainValue)) return keychainValue;
  }

  const legacyValue = readLegacyLocalSecret(names, { trim });
  if (legacyValue && !isObusUserDeactivatePlaceholderConfigValue(legacyValue)) {
    writeMacOsKeychainSecret(names[0], legacyValue, { trim });
    return legacyValue;
  }

  const fallbackValue = normalizeMacOsKeychainSecretValue(fallback, trim);
  return fallbackValue && !isObusUserDeactivatePlaceholderConfigValue(fallbackValue) ? fallbackValue : "";
}

function buildObusUserDeactivateSqlConfigurationMessage(missingItems = []) {
  const normalizedMissingItems = (Array.isArray(missingItems) ? missingItems : [])
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  if (normalizedMissingItems.length === 0) {
    return "Obus kullanıcı listeleme SQL bağlantı bilgileri eksik.";
  }
  return `Obus kullanıcı listeleme SQL bağlantı bilgileri eksik: ${normalizedMissingItems.join(", ")}.`;
}

function getObusUserDeactivateSqlCredentials() {
  const password = getObusUserDeactivateSqlPassword();
  const missingItems = [];
  if (!OBUS_USER_DEACTIVATE_SQL_HOST) missingItems.push("OBUS_USER_DEACTIVATE_SQL_HOST");
  if (!OBUS_USER_DEACTIVATE_SQL_DATABASE) missingItems.push("OBUS_USER_DEACTIVATE_SQL_DATABASE");
  if (!OBUS_USER_DEACTIVATE_SQL_USERNAME) missingItems.push("OBUS_USER_DEACTIVATE_SQL_USERNAME");
  if (!password) missingItems.push(OBUS_USER_DEACTIVATE_SQL_PASSWORD_SECRET_NAMES[0]);

  return {
    host: OBUS_USER_DEACTIVATE_SQL_HOST,
    port: OBUS_USER_DEACTIVATE_SQL_PORT,
    database: OBUS_USER_DEACTIVATE_SQL_DATABASE,
    username: OBUS_USER_DEACTIVATE_SQL_USERNAME,
    password,
    error: missingItems.length > 0 ? buildObusUserDeactivateSqlConfigurationMessage(missingItems) : ""
  };
}

function buildObusUserDeactivateSqlRequestUrl() {
  if (OBUS_USER_DEACTIVATE_SQL_PROXY_URL) {
    return OBUS_USER_DEACTIVATE_SQL_PROXY_URL;
  }
  return `mssql://${OBUS_USER_DEACTIVATE_SQL_HOST}:${OBUS_USER_DEACTIVATE_SQL_PORT}/${OBUS_USER_DEACTIVATE_SQL_DATABASE}`;
}

function buildObusUserDeactivateSqlRequestBody(usernameFilter = "") {
  return JSON.stringify(
    {
      query: [
        "select u.ID, u.PartnerId, p.Code, u.Username from b2b.[user] u",
        "left join partner p on p.ID = u.PartnerId",
        "where username like @usernameFilter"
      ].join("\n"),
      usernameFilter: `%${String(usernameFilter || "").trim()}%`
    },
    null,
    2
  );
}

function buildObusUserDeactivateSqlPreview({
  usernameFilter = "",
  status = null,
  responseBody = "",
  error = ""
} = {}) {
  return buildObusRequestPreviewFromTrace(
    buildObusServiceTraceEntry({
      service: "SQL Kullanıcı Listeleme",
      url: buildObusUserDeactivateSqlRequestUrl(),
      status,
      requestBody: buildObusUserDeactivateSqlRequestBody(usernameFilter),
      responseBody: responseBody || error || "",
      error
    })
  );
}

let obusUserDeactivateSqlPoolPromise = null;
let obusUserDeactivateSqlPoolKey = "";

async function getObusUserDeactivateSqlPool() {
  const credentials = getObusUserDeactivateSqlCredentials();
  if (credentials.error) {
    throw new Error(credentials.error);
  }

  const poolKey = [
    credentials.host,
    credentials.port,
    credentials.database,
    credentials.username,
    credentials.password
  ].join("\u0000");
  if (obusUserDeactivateSqlPoolPromise && obusUserDeactivateSqlPoolKey === poolKey) {
    return obusUserDeactivateSqlPoolPromise;
  }

  const encrypt = parseObusUserDeactivateBooleanFlag(
    process.env.OBUS_USER_DEACTIVATE_SQL_ENCRYPT,
    typeof OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.encrypt === "boolean"
      ? OBUS_USER_DEACTIVATE_DATABASE_MSSQL_CONFIG.encrypt
      : !net.isIP(credentials.host)
  );

  const pool = new mssql.ConnectionPool({
    user: credentials.username,
    password: credentials.password,
    server: credentials.host,
    port: Number(credentials.port || 1433),
    database: credentials.database,
    connectionTimeout: OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS,
    requestTimeout: OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS,
    pool: {
      min: 0,
      max: 4,
      idleTimeoutMillis: 30000
    },
    options: {
      encrypt,
      trustServerCertificate: true,
      ...(net.isIP(credentials.host)
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

  obusUserDeactivateSqlPoolKey = poolKey;
  obusUserDeactivateSqlPoolPromise = pool.connect().catch((err) => {
    if (obusUserDeactivateSqlPoolKey === poolKey) {
      obusUserDeactivateSqlPoolPromise = null;
      obusUserDeactivateSqlPoolKey = "";
    }
    throw err;
  });
  return obusUserDeactivateSqlPoolPromise;
}

async function fetchObusUserDeactivateSqlRows({ usernameFilter = "" } = {}) {
  if (OBUS_USER_DEACTIVATE_SQL_PROXY_URL) {
    return fetchObusUserDeactivateSqlRowsViaProxy({ usernameFilter });
  }

  const pool = await getObusUserDeactivateSqlPool();
  const normalizedUsernameFilter = String(usernameFilter || "").trim();
  const request = pool.request();
  request.input("usernameFilter", mssql.NVarChar, `%${normalizedUsernameFilter}%`);
  const result = await request.query(`
    select u.ID, u.PartnerId, p.Code, u.Username from b2b.[user] u
    left join partner p on p.ID = u.PartnerId
    where username like @usernameFilter
  `);
  return Array.isArray(result?.recordset) ? result.recordset : [];
}

function sanitizeObusUserDeactivateSqlProxyUrl(value = "") {
  const raw = String(value || "").trim();
  if (!raw) return "SQL proxy URL";
  try {
    const parsed = new URL(raw);
    parsed.username = "";
    parsed.password = "";
    return parsed.toString();
  } catch (err) {
    return raw;
  }
}

function buildObusUserDeactivateSqlProxyFetchError(err, requestUrl = "") {
  const cause = err?.cause && typeof err.cause === "object" ? err.cause : null;
  const parts = [`SQL proxy erişilemedi: ${sanitizeObusUserDeactivateSqlProxyUrl(requestUrl)}`];
  const code = String(cause?.code || err?.code || "").trim();
  const address = String(cause?.address || "").trim();
  const port = String(cause?.port || "").trim();
  const detail = String(cause?.message || err?.message || "").trim();

  if (code) parts.push(`kod=${code}`);
  if (address || port) parts.push(`adres=${[address, port].filter(Boolean).join(":")}`);
  if (detail) parts.push(`detay=${detail}`);
  return parts.join(" | ");
}

async function fetchObusUserDeactivateSqlRowsViaProxy({
  usernameFilter = "",
  requestUrl = OBUS_USER_DEACTIVATE_SQL_PROXY_URL
} = {}) {
  const normalizedRequestUrl = String(requestUrl || "").trim();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_USER_DEACTIVATE_SQL_TIMEOUT_MS);
  const token = getObusUserDeactivateSqlProxyToken();

  try {
    const headers = {
      Accept: "application/json",
      "Content-Type": "application/json"
    };
    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    const response = await fetch(normalizedRequestUrl, {
      method: "POST",
      headers,
      body: JSON.stringify({
        usernameFilter: String(usernameFilter || "").trim()
      }),
      signal: controller.signal
    });
    const raw = await response.text();
    const payload = parseJsonSafe(raw);
    if (!response.ok || !payload?.ok) {
      const message =
        (payload && typeof payload === "object" && String(payload.error || payload.message || "").trim()) ||
        response.statusText ||
        "SQL proxy yanıtı başarısız.";
      throw new Error(`SQL proxy HTTP ${response.status}: ${message}`);
    }
    return Array.isArray(payload.rows) ? payload.rows : [];
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error(
        `SQL proxy isteği zaman aşımına uğradı: ${sanitizeObusUserDeactivateSqlProxyUrl(normalizedRequestUrl)}`
      );
    }
    if (String(err?.message || "").startsWith("SQL proxy HTTP")) {
      throw err;
    }
    throw new Error(buildObusUserDeactivateSqlProxyFetchError(err, normalizedRequestUrl));
  } finally {
    clearTimeout(timeout);
  }
}

function buildObusUserDeactivateLocalSqlProxyUrls() {
  return Array.from(
    new Set(
      [
        OBUS_USER_DEACTIVATE_SQL_PROXY_URL,
        `http://127.0.0.1:${OBUS_USER_DEACTIVATE_SQL_PROXY_PORT}/obus-user-deactivate/users`,
        `http://localhost:${OBUS_USER_DEACTIVATE_SQL_PROXY_PORT}/obus-user-deactivate/users`
      ]
        .map((item) => String(item || "").trim())
        .filter(Boolean)
    )
  );
}

async function fetchObusUserDeactivateLocalSqlProxyRows({ usernameFilter = "" } = {}) {
  const requestUrls = buildObusUserDeactivateLocalSqlProxyUrls();
  let lastError = null;

  for (const requestUrl of requestUrls) {
    try {
      const rows = await fetchObusUserDeactivateSqlRowsViaProxy({ usernameFilter, requestUrl });
      return {
        rows,
        sourceUrl: requestUrl
      };
    } catch (err) {
      lastError = err;
    }
  }

  const triedUrls = requestUrls.map((item) => sanitizeObusUserDeactivateSqlProxyUrl(item)).join(", ");
  throw new Error(`${lastError?.message || "SQL proxy erişilemedi."} Denenen adresler: ${triedUrls}`);
}

function normalizeObusUserDeactivateSqlRow(row = {}) {
  const userId = formatPartnerCellValue(readPartnerRawValueByAliases(row, ["ID", "id", "user-id", "userId"])).trim();
  const partnerId = formatPartnerCellValue(
    readPartnerRawValueByAliases(row, ["PartnerId", "PartnerID", "partner-id", "partner_id", "partnerId", "partnerid"])
  ).trim();
  const code = formatPartnerCellValue(readPartnerRawValueByAliases(row, ["Code", "code"])).trim();
  const username = formatPartnerCellValue(readPartnerRawValueByAliases(row, ["Username", "username"])).trim();
  if (!userId || !code || !username) return null;
  return {
    userId,
    partnerId,
    code,
    username
  };
}

function normalizeObusUserDeactivateSqlCode(value) {
  return String(value || "").trim().toLocaleLowerCase("tr");
}

function buildObusUserDeactivateSqlCompanyLookup(partnerItems = []) {
  const byCode = new Map();
  const byCodeAndPartnerId = new Map();
  (Array.isArray(partnerItems) ? partnerItems : []).forEach((company) => {
    const code = String(company?.code || "").trim();
    const codeKey = normalizeObusUserDeactivateSqlCode(code);
    const partnerId = String(company?.id || "").trim();
    if (!codeKey) return;

    const codeItems = byCode.get(codeKey) || [];
    codeItems.push(company);
    byCode.set(codeKey, codeItems);

    if (partnerId) {
      const partnerKey = `${codeKey}|||${partnerId}`;
      const partnerItems = byCodeAndPartnerId.get(partnerKey) || [];
      partnerItems.push(company);
      byCodeAndPartnerId.set(partnerKey, partnerItems);
    }
  });
  return {
    byCode,
    byCodeAndPartnerId
  };
}

function findObusUserDeactivateSqlCompany(companyLookup, row = {}) {
  const codeKey = normalizeObusUserDeactivateSqlCode(row?.code);
  const partnerId = String(row?.partnerId || "").trim();
  if (!codeKey || !companyLookup || typeof companyLookup !== "object") return null;

  if (partnerId && companyLookup.byCodeAndPartnerId instanceof Map) {
    const directCandidates = companyLookup.byCodeAndPartnerId.get(`${codeKey}|||${partnerId}`) || [];
    if (directCandidates.length > 0) return directCandidates[0];
  }

  const candidates = companyLookup.byCode instanceof Map ? companyLookup.byCode.get(codeKey) || [] : [];
  if (candidates.length === 1) return candidates[0];
  if (!partnerId && candidates.length > 0) return candidates[0];
  return null;
}

function buildObusUserDeactivateListedRowFromSql(row = {}, company = {}) {
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_DEACTIVATE_API_URL)) ||
    "cluster4";
  const requestUrl = buildMembershipGetUsersWithoutPermissionsUrl(
    buildObusUserDeactivateCompanyBaseUrl(company, clusterLabel) || OBUS_USER_DEACTIVATE_API_URL,
    clusterLabel
  );

  return {
    userId: String(row?.userId || "").trim(),
    partnerId: String(company?.id || row?.partnerId || "").trim(),
    code: String(row?.code || company?.code || "").trim(),
    username: String(row?.username || "").trim(),
    fullName: "",
    clusterUrl: requestUrl,
    clusterLabel,
    isActive: true,
    isActiveText: "true"
  };
}

async function fetchObusUserDeactivateCompanyResult({
  company,
  loginCredentials = {},
  sessionCache = null
}) {
  const companyCode = String(company?.code || "").trim();
  const partnerId = String(company?.id || "").trim();
  const branchId = String(company?.branchId || company?.id || "").trim();
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_DEACTIVATE_API_URL)) ||
    "cluster4";
  const companyBaseUrl = buildObusUserDeactivateCompanyBaseUrl(company, clusterLabel);
  const requestUrl = normalizeTargetUrl(
    buildMembershipGetUsersWithoutPermissionsUrl(companyBaseUrl || OBUS_USER_DEACTIVATE_API_URL, clusterLabel)
  );
  const buildPreview = (trace, fallback = {}) =>
    buildObusRequestPreviewFromTrace(trace, {
      requestUrl,
      ...fallback
    });

  const buildFailure = (error = "", errorDetail = "", status = null, responseBody = "") => ({
    ok: false,
    code: companyCode,
    partnerId,
    clusterLabel,
    requestUrl,
    status: Number.isFinite(Number(status)) ? Number(status) : null,
    error: String(error || "").trim() || "İstek gönderilemedi.",
    errorDetail: String(errorDetail || "").trim(),
    responseBody: String(responseBody || "").trim(),
    firstRequestPreview: null,
    failedRequestPreview: null
  });

  if (!companyCode) {
    return buildFailure("Firma code bulunamadı.");
  }
  if (!partnerId) {
    return buildFailure("Firma partner-id bulunamadı.");
  }
  if (!requestUrl) {
    return buildFailure("GetUsersWithoutPermissions URL oluşturulamadı.");
  }

  const loginResult = await resolveAuthorizedLinesLoginResultWithBranchFallback({
    endpointUrl: requestUrl,
    companyUrl: companyBaseUrl || requestUrl,
    partnerCode: companyCode,
    partnerId,
    username: String(loginCredentials?.username || "").trim(),
    password: typeof loginCredentials?.password === "string" ? loginCredentials.password : "",
    fallbackBranchId: branchId,
    sessionClusterLabel: clusterLabel,
    authorization: OBUS_USER_DEACTIVATE_API_AUTH,
    timeoutMs: OBUS_USER_DEACTIVATE_TIMEOUT_MS,
    sessionCache
  });
  const firstLoginTrace = getFirstObusServiceTrace(loginResult?.serviceLogs);
  const firstRequestPreview = buildPreview(firstLoginTrace, {
    requestUrl: String(loginResult?.loginUrl || "").trim() || requestUrl,
    responseBody: String(loginResult?.rawLoginBody || "").trim()
  });
  const failedLoginPreview = buildPreview(loginResult?.failedServiceLog || getLastObusServiceTrace(loginResult?.serviceLogs), {
    requestUrl: String(loginResult?.loginUrl || "").trim() || requestUrl,
    responseBody: String(loginResult?.rawLoginBody || "").trim(),
    error: String(loginResult?.error || "").trim()
  });

  if (!loginResult?.ok) {
    const failure = buildFailure(
      String(loginResult?.error || "UserLogin başarısız.").trim() || "UserLogin başarısız.",
      String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim(),
      null,
      String(loginResult?.rawLoginBody || "").trim()
    );
    failure.firstRequestPreview = firstRequestPreview;
    failure.failedRequestPreview = failedLoginPreview;
    return failure;
  }

  const token = String(loginResult?.token || "").trim();
  if (!token) {
    const failure = buildFailure(
      "UserLogin token bulunamadı.",
      String(loginResult?.tokenMissingDetail || loginResult?.errorDetail || "").trim(),
      null,
      String(loginResult?.rawLoginBody || "").trim()
    );
    failure.firstRequestPreview = firstRequestPreview;
    failure.failedRequestPreview = failedLoginPreview;
    return failure;
  }

  const requestBodyObject = buildObusUserDeactivateRequestBody({
    sessionId: String(loginResult?.sessionId || "").trim(),
    deviceId: String(loginResult?.deviceId || "").trim(),
    token
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_USER_DEACTIVATE_TIMEOUT_MS);

  try {
    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_USER_DEACTIVATE_API_AUTH
      },
      body: JSON.stringify(requestBodyObject),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBody =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();
    const requestTrace = buildObusServiceTraceEntry({
      service: "Membership GetUsersWithoutPermissions",
      url: requestUrl,
      status: response.status,
      requestBody: requestBodyObject,
      responseBody: parsed ?? raw
    });
    const reason =
      (parsed &&
        typeof parsed === "object" &&
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      const failure = buildFailure(`HTTP ${response.status}: ${reason}`, responseBody, response.status, responseBody);
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, { responseBody, status: response.status, error: reason });
      return failure;
    }

    if (!parsed || typeof parsed !== "object") {
      const failure = buildFailure(
        "Servis yanıtı okunamadı.",
        String(raw || "").trim(),
        response.status,
        String(raw || "").trim()
      );
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, {
        responseBody: String(raw || "").trim(),
        status: response.status
      });
      return failure;
    }

    const hasExplicitStatusField =
      "status" in parsed || "success" in parsed || "status-code" in parsed;
    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      const failure = buildFailure(reason || "İşlem başarısız döndü.", responseBody, response.status, responseBody);
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, { responseBody, status: response.status, error: reason });
      return failure;
    }

    const allRows = extractObusUserDeactivateRows(parsed);
    const activeRows = allRows.filter((row) => row.isActive === true);
    const listedRows = activeRows
      .map((row) => ({
        userId: String(row.userId || "").trim(),
        partnerId: String(row.partnerId || "").trim() || partnerId,
        code: companyCode,
        username: String(row.username || "").trim(),
        fullName: String(row.fullName || "").trim(),
        clusterUrl: requestUrl,
        clusterLabel,
        isActive: row.isActive,
        isActiveText: row.isActiveText
      }));

    return {
      ok: true,
      code: companyCode,
      partnerId,
      clusterLabel,
      requestUrl,
      status: response.status,
      totalUserCount: allRows.length,
      activeUserCount: activeRows.length,
      listedUserCount: listedRows.length,
      listedRows,
      responseBody,
      firstRequestPreview: firstRequestPreview || buildPreview(requestTrace),
      failedRequestPreview: null
    };
  } catch (err) {
    const requestTrace = buildObusServiceTraceEntry({
      service: "Membership GetUsersWithoutPermissions",
      url: requestUrl,
      requestBody: requestBodyObject,
      responseBody: "",
      error: err?.message || "İstek gönderilemedi."
    });
    const failure = buildFailure(
      err?.name === "AbortError" ? "GetUsersWithoutPermissions isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi."
    );
    failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
    failure.failedRequestPreview = buildPreview(requestTrace, {
      error: err?.name === "AbortError" ? "GetUsersWithoutPermissions isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi."
    });
    return failure;
  } finally {
    clearTimeout(timeout);
  }
}

async function deactivateObusUsersForCompany({
  company,
  selectedUsers = [],
  loginCredentials = {},
  sessionCache = null
}) {
  const normalizedSelectedUsers = (Array.isArray(selectedUsers) ? selectedUsers : []).filter(
    (item) => item && Number.isInteger(Number(item.userIdValue)) && Number(item.userIdValue) > 0
  );
  const companyCode = String(company?.code || "").trim();
  const partnerId = String(company?.id || "").trim();
  const branchId = String(company?.branchId || company?.id || "").trim();
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_DEACTIVATE_API_URL)) ||
    "cluster4";
  const companyLabel = buildObusUserDeactivateCompanyEventLabel({
    code: companyCode,
    id: partnerId,
    cluster: clusterLabel
  });
  const companyBaseUrl = buildObusUserDeactivateCompanyBaseUrl(company, clusterLabel);
  const requestUrl = normalizeTargetUrl(
    buildMembershipDeleteUserUrl(companyBaseUrl || OBUS_USER_DEACTIVATE_API_URL, clusterLabel)
  );
  const buildPreview = (trace, fallback = {}) =>
    buildObusRequestPreviewFromTrace(trace, {
      requestUrl,
      ...fallback
    });
  const normalizedUserMeta = normalizedSelectedUsers.map((item) => ({
    key: String(item?.key || "").trim(),
    userIdValue: Number(item?.userIdValue),
    username: String(item?.username || "").trim()
  }));
  const buildFailure = (error = "", errorDetail = "", status = null, responseBody = "") => ({
    ok: false,
    code: companyCode,
    partnerId,
    clusterLabel,
    companyLabel,
    requestUrl,
    status: Number.isFinite(Number(status)) ? Number(status) : null,
    error: String(error || "").trim() || "İstek gönderilemedi.",
    errorDetail: String(errorDetail || "").trim(),
    responseBody: String(responseBody || "").trim(),
    selectedUsers: normalizedUserMeta,
    firstRequestPreview: null,
    failedRequestPreview: null
  });

  if (!companyCode || !partnerId) {
    return buildFailure("Pasife alınacak kullanıcı için firma bilgisi eksik.");
  }
  if (!requestUrl) {
    return buildFailure("DeleteUser URL oluşturulamadı.");
  }
  if (normalizedSelectedUsers.length === 0) {
    return buildFailure("Pasife alınacak geçerli kullanıcı bulunamadı.");
  }

  const loginResult = await resolveAuthorizedLinesLoginResultWithBranchFallback({
    endpointUrl: requestUrl,
    companyUrl: companyBaseUrl || requestUrl,
    partnerCode: companyCode,
    partnerId,
    username: String(loginCredentials?.username || "").trim(),
    password: typeof loginCredentials?.password === "string" ? loginCredentials.password : "",
    fallbackBranchId: branchId,
    sessionClusterLabel: clusterLabel,
    authorization: OBUS_USER_DEACTIVATE_API_AUTH,
    timeoutMs: OBUS_USER_DEACTIVATE_TIMEOUT_MS,
    sessionCache
  });
  const firstLoginTrace = getFirstObusServiceTrace(loginResult?.serviceLogs);
  const firstRequestPreview = buildPreview(firstLoginTrace, {
    requestUrl: String(loginResult?.loginUrl || "").trim() || requestUrl,
    responseBody: String(loginResult?.rawLoginBody || "").trim()
  });
  const failedLoginPreview = buildPreview(loginResult?.failedServiceLog || getLastObusServiceTrace(loginResult?.serviceLogs), {
    requestUrl: String(loginResult?.loginUrl || "").trim() || requestUrl,
    responseBody: String(loginResult?.rawLoginBody || "").trim(),
    error: String(loginResult?.error || "").trim()
  });

  if (!loginResult?.ok) {
    const failure = buildFailure(
      String(loginResult?.error || "UserLogin başarısız.").trim() || "UserLogin başarısız.",
      String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim(),
      null,
      String(loginResult?.rawLoginBody || "").trim()
    );
    failure.firstRequestPreview = firstRequestPreview;
    failure.failedRequestPreview = failedLoginPreview;
    return failure;
  }

  const token = String(loginResult?.token || "").trim();
  if (!token) {
    const failure = buildFailure(
      "UserLogin token bulunamadı.",
      String(loginResult?.tokenMissingDetail || loginResult?.errorDetail || "").trim(),
      null,
      String(loginResult?.rawLoginBody || "").trim()
    );
    failure.firstRequestPreview = firstRequestPreview;
    failure.failedRequestPreview = failedLoginPreview;
    return failure;
  }

  const requestBodyObject = buildObusUserDeleteRequestBody({
    userIds: normalizedSelectedUsers.map((item) => item.userIdValue),
    sessionId: String(loginResult?.sessionId || "").trim(),
    deviceId: String(loginResult?.deviceId || "").trim(),
    token
  });

  if (!Array.isArray(requestBodyObject.data) || requestBodyObject.data.length === 0) {
    const failure = buildFailure("DeleteUser body içindeki kullanıcı id listesi boş.");
    failure.firstRequestPreview = firstRequestPreview;
    failure.failedRequestPreview = buildPreview(null, {
      service: "Membership DeleteUser",
      requestUrl,
      requestBody: JSON.stringify(requestBodyObject, null, 2),
      error: "DeleteUser body içindeki kullanıcı id listesi boş."
    });
    return failure;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_USER_DEACTIVATE_TIMEOUT_MS);

  try {
    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_USER_DEACTIVATE_API_AUTH
      },
      body: JSON.stringify(requestBodyObject),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBody =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();
    const requestTrace = buildObusServiceTraceEntry({
      service: "Membership DeleteUser",
      url: requestUrl,
      status: response.status,
      requestBody: requestBodyObject,
      responseBody: parsed ?? raw
    });
    const reason =
      (parsed &&
        typeof parsed === "object" &&
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      const failure = buildFailure(`HTTP ${response.status}: ${reason}`, responseBody, response.status, responseBody);
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, { responseBody, status: response.status, error: reason });
      return failure;
    }

    if (!parsed || typeof parsed !== "object") {
      const failure = buildFailure(
        "Servis yanıtı okunamadı.",
        String(raw || "").trim(),
        response.status,
        String(raw || "").trim()
      );
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, {
        responseBody: String(raw || "").trim(),
        status: response.status
      });
      return failure;
    }

    const hasExplicitStatusField =
      "status" in parsed || "success" in parsed || "status-code" in parsed;
    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      const failure = buildFailure(reason || "İşlem başarısız döndü.", responseBody, response.status, responseBody);
      failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
      failure.failedRequestPreview = buildPreview(requestTrace, { responseBody, status: response.status, error: reason });
      return failure;
    }

    return {
      ok: true,
      code: companyCode,
      partnerId,
      clusterLabel,
      companyLabel,
      requestUrl,
      status: response.status,
      responseBody,
      selectedUsers: normalizedUserMeta,
      firstRequestPreview: firstRequestPreview || buildPreview(requestTrace),
      failedRequestPreview: null,
      message:
        String(reason || "").trim() ||
        `${normalizedUserMeta.length} kullanıcı pasife alındı.`
    };
  } catch (err) {
    const requestTrace = buildObusServiceTraceEntry({
      service: "Membership DeleteUser",
      url: requestUrl,
      requestBody: requestBodyObject,
      responseBody: "",
      error: err?.message || "İstek gönderilemedi."
    });
    const failure = buildFailure(
      err?.name === "AbortError" ? "DeleteUser isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi."
    );
    failure.firstRequestPreview = firstRequestPreview || buildPreview(requestTrace);
    failure.failedRequestPreview = buildPreview(requestTrace, {
      error: err?.name === "AbortError" ? "DeleteUser isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi."
    });
    return failure;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchObusUserDeactivateSearchReport({ partnerItems = [] }) {
  const report = buildObusUserDeactivateReportModel();
  report.requested = true;

  if (!Array.isArray(partnerItems) || partnerItems.length === 0) {
    report.error = "Sorgulanacak firma listesi bulunamadı.";
    return report;
  }

  const loginCredentials = getObusUserCreateLoginCredentials();
  if (!loginCredentials.username || !loginCredentials.password) {
    report.error = buildObusServiceLoginConfigurationMessage(loginCredentials);
    return report;
  }

  const sessionCache = new Map();
  const companyResults = await runWithConcurrency(
    partnerItems,
    OBUS_USER_DEACTIVATE_COMPANY_CONCURRENCY,
    async (company) =>
      fetchObusUserDeactivateCompanyResult({
        company,
        loginCredentials,
        sessionCache
      })
  );

  const normalizedResults = companyResults.map((result, index) => {
    if (result && typeof result === "object" && (result.ok === true || result.ok === false)) {
      return result;
    }
    const company = partnerItems[index] || {};
    return {
      ok: false,
      code: String(company?.code || "").trim(),
      partnerId: String(company?.id || "").trim(),
      clusterLabel: String(company?.cluster || "").trim(),
      requestUrl: "",
      status: null,
      error: String(result?.error?.message || result?.error || "Firma sonucu alınamadı.").trim() || "Firma sonucu alınamadı.",
      errorDetail: ""
    };
  });

  const successResults = normalizedResults.filter((item) => item.ok === true);
  const failureResults = normalizedResults.filter((item) => item.ok !== true);
  const listedRows = successResults
    .flatMap((item) => (Array.isArray(item.listedRows) ? item.listedRows : []))
    .sort((a, b) => {
      const byCode = String(a.code || "").localeCompare(String(b.code || ""), "tr");
      if (byCode !== 0) return byCode;
      const byUsername = String(a.username || "").localeCompare(String(b.username || ""), "tr");
      if (byUsername !== 0) return byUsername;
      return String(a.userId || "").localeCompare(String(b.userId || ""), "tr");
    });

  report.scannedCompanyCount = normalizedResults.length;
  report.successCompanyCount = successResults.length;
  report.failureCompanyCount = failureResults.length;
  report.totalUserCount = successResults.reduce((sum, item) => sum + Number(item.totalUserCount || 0), 0);
  report.activeUserCount = successResults.reduce((sum, item) => sum + Number(item.activeUserCount || 0), 0);
  report.listedUserCount = listedRows.length;
  report.matchedUserCount = listedRows.length;
  report.rows = listedRows;
  report.requestUrl = normalizeTargetUrl(OBUS_USER_DEACTIVATE_API_URL);
  report.requestBody = JSON.stringify(buildObusUserDeactivateRequestBody({ usePlaceholders: true }), null, 2);
  report.responseBody = JSON.stringify(
    {
      scannedCompanyCount: report.scannedCompanyCount,
      successCompanyCount: report.successCompanyCount,
      failureCompanyCount: report.failureCompanyCount,
      listedUserCount: report.listedUserCount,
      failures: failureResults.slice(0, 20).map((item) => ({
        code: item.code,
        "partner-id": item.partnerId,
        cluster: item.clusterLabel,
        status: item.status,
        error: item.error
      }))
    },
    null,
    2
  );

  if (successResults.length === 0) {
    report.status =
      Number(failureResults.find((item) => Number(item.status) > 0)?.status || 0) || null;
    report.error = failureResults[0]?.error || "Hiçbir firma için sonuç alınamadı.";
    report.errorDetail = failureResults
      .map((item) =>
        [String(item.code || "").trim(), String(item.partnerId || "").trim(), String(item.clusterLabel || "").trim(), String(item.error || "").trim()]
          .filter(Boolean)
          .join(" / ")
      )
      .filter(Boolean)
      .join("\n");
    return report;
  }

  report.status = failureResults.length > 0 ? 207 : 200;
  report.userMessage =
    listedRows.length > 0
      ? `${listedRows.length} kullanıcı listelendi.`
      : "Seçilen firma için kullanıcı bulunamadı.";

  if (failureResults.length > 0) {
    report.error = `${failureResults.length} firma sorgusunda hata oluştu.`;
    report.errorDetail = failureResults
      .slice(0, 10)
      .map((item) =>
        [
          String(item.code || "").trim() || "-",
          String(item.partnerId || "").trim() || "-",
          String(item.clusterLabel || "").trim() || "-",
          String(item.error || "").trim() || "Hata"
        ].join(" / ")
      )
      .join("\n");
  }

  return report;
}

function buildObusUserDeactivateCompanyEventKey(company = {}) {
  const code = String(company?.code || "").trim() || "Firma";
  const partnerId = String(company?.id || "").trim() || "partner";
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    "cluster";
  return [code, partnerId, clusterLabel].join("|||");
}

function buildObusUserDeactivateCompanyEventLabel(company = {}) {
  const code = String(company?.code || "").trim() || "Firma";
  const partnerId = String(company?.id || "").trim() || "-";
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    "cluster";
  return `${code} / ${partnerId} / ${clusterLabel}`;
}

function buildObusUserDeactivateMatchEventKey(row = {}) {
  return [
    "match",
    String(row?.code || "").trim() || "Firma",
    String(row?.partnerId || "").trim() || "partner",
    String(row?.userId || "").trim() || String(row?.username || "").trim() || "user"
  ].join("|||");
}

async function runObusUserDeactivateSearchJob(job, { partnerItems = [], usernameFilter = "" }) {
  const normalizedCompanies = Array.isArray(partnerItems) ? partnerItems : [];
  if (normalizedCompanies.length === 0) {
    finishObusLiveJob(job, "Sorgulanacak firma listesi bulunamadı.");
    return;
  }

  const normalizedUsernameFilter = String(usernameFilter || "").trim();
  let listedUserCount = 0;
  let totalUserCount = 0;
  let activeUserCount = 0;
  const failureSamples = [];
  let firstRequestPreview = null;
  let latestFailedRequestPreview = null;
  job.totalCount = normalizedCompanies.length;
  const requestBodyPreview = buildObusUserDeactivateSqlRequestBody(normalizedUsernameFilter);

  const updateSummary = () => {
    setObusLiveJobSummary(job, {
      scannedCompanyCount: normalizedCompanies.length,
      successCompanyCount: Number(job.successCount || 0),
      failureCompanyCount: Number(job.failureCount || 0),
      listedUserCount,
      matchedUserCount: listedUserCount,
      totalUserCount,
      activeUserCount,
      failureSamples,
      requestBody: requestBodyPreview,
      debugPreview: {
        firstRequest: firstRequestPreview,
        failedRequest: latestFailedRequestPreview
      }
    });
  };

  updateSummary();

  normalizedCompanies.forEach((company) => {
    const eventKey = buildObusUserDeactivateCompanyEventKey(company);
    const eventLabel = buildObusUserDeactivateCompanyEventLabel(company);
    const clusterLabel =
      normalizeObusClusterLabel(company?.cluster || "") ||
      normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
      "cluster";

    pushObusLiveJobEvent(job, {
      key: eventKey,
      label: eventLabel,
      statusKind: "pending",
      message: "Firma SQL sorgusuna hazırlanıyor.",
      detailText: [
        `cluster=${clusterLabel}`,
        `sql=${truncateObusDebugText(buildObusUserDeactivateSqlRequestUrl(), 120)}`
      ]
        .filter(Boolean)
        .join(" | "),
      meta: {
        type: "company",
        code: String(company?.code || "").trim(),
        partnerId: String(company?.id || "").trim(),
        clusterLabel
      }
    });
  });

  try {
    const sqlRows = await fetchObusUserDeactivateSqlRows({ usernameFilter: normalizedUsernameFilter });
    firstRequestPreview = buildObusUserDeactivateSqlPreview({
      usernameFilter: normalizedUsernameFilter,
      status: 200,
      responseBody: `${Array.isArray(sqlRows) ? sqlRows.length : 0} SQL satırı döndü.`
    });

    const companyLookup = buildObusUserDeactivateSqlCompanyLookup(normalizedCompanies);
    const listedRowsByCompanyKey = new Map();
    (Array.isArray(sqlRows) ? sqlRows : []).forEach((rawRow) => {
      const normalizedRow = normalizeObusUserDeactivateSqlRow(rawRow);
      if (!normalizedRow) return;

      const matchedCompany = findObusUserDeactivateSqlCompany(companyLookup, normalizedRow);
      if (!matchedCompany) return;

      const listedRow = buildObusUserDeactivateListedRowFromSql(normalizedRow, matchedCompany);
      if (!listedRow.userId || !listedRow.username || !listedRow.code || !listedRow.partnerId) return;

      const companyKey = buildObusUserDeactivateCompanyEventKey(matchedCompany);
      const rows = listedRowsByCompanyKey.get(companyKey) || [];
      rows.push(listedRow);
      listedRowsByCompanyKey.set(companyKey, rows);
    });

    totalUserCount = Array.from(listedRowsByCompanyKey.values()).reduce(
      (sum, rows) => sum + (Array.isArray(rows) ? rows.length : 0),
      0
    );
    activeUserCount = totalUserCount;

    normalizedCompanies.forEach((company) => {
      const eventKey = buildObusUserDeactivateCompanyEventKey(company);
      const eventLabel = buildObusUserDeactivateCompanyEventLabel(company);
      const clusterLabel =
        normalizeObusClusterLabel(company?.cluster || "") ||
        normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
        "cluster";
      const listedRows = listedRowsByCompanyKey.get(eventKey) || [];

      listedRows.forEach((row) => {
        listedUserCount += 1;
        pushObusLiveJobEvent(job, {
          key: buildObusUserDeactivateMatchEventKey(row),
          label: `${String(row?.code || "").trim() || "Firma"} / ${String(row?.username || "").trim() || "-"}`,
          statusKind: "info",
          message: "Kullanıcı listelendi.",
          detailText: [
            `cluster=${clusterLabel}`,
            String(row?.partnerId || "").trim() ? `partner-id=${String(row.partnerId).trim()}` : "",
            String(row?.isActiveText || "").trim() ? `is-active=${String(row.isActiveText).trim()}` : ""
          ]
            .filter(Boolean)
            .join(" | "),
          meta: {
            type: "user",
            userId: String(row?.userId || "").trim(),
            partnerId: String(row?.partnerId || "").trim(),
            code: String(row?.code || "").trim(),
            username: String(row?.username || "").trim(),
            fullName: String(row?.fullName || "").trim(),
            clusterUrl: String(row?.clusterUrl || "").trim(),
            clusterLabel,
            isActive: true,
            isActiveText: "true"
          }
        });
      });

      pushObusLiveJobEvent(job, {
        key: eventKey,
        label: eventLabel,
        statusKind: "success",
        ok: true,
        message:
          listedRows.length > 0
            ? `${listedRows.length} kullanıcı listelendi.`
            : "Kullanıcı bulunamadı.",
        detailText: [
          `cluster=${clusterLabel}`,
          "status=200",
          `kullanıcı=${listedRows.length}`,
          `aktif=${listedRows.length}`
        ]
          .filter(Boolean)
          .join(" | "),
        meta: {
          type: "company",
          code: String(company?.code || "").trim(),
          partnerId: String(company?.id || "").trim(),
          clusterLabel,
          listedUserCount: listedRows.length,
          totalUserCount: listedRows.length,
          activeUserCount: listedRows.length
        }
      });

      updateSummary();
    });

    updateSummary();
    finishObusLiveJob(job);
  } catch (err) {
    const errorMessage = err?.message || "SQL kullanıcı listeleme tamamlanamadı.";
    latestFailedRequestPreview = buildObusUserDeactivateSqlPreview({
      usernameFilter: normalizedUsernameFilter,
      error: errorMessage
    });

    normalizedCompanies.forEach((company) => {
      const eventKey = buildObusUserDeactivateCompanyEventKey(company);
      const eventLabel = buildObusUserDeactivateCompanyEventLabel(company);
      const clusterLabel =
        normalizeObusClusterLabel(company?.cluster || "") ||
        normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
        "cluster";

      pushObusLiveJobEvent(job, {
        key: eventKey,
        label: eventLabel,
        statusKind: "failure",
        ok: false,
        error: errorMessage,
        detailText: [
          `cluster=${clusterLabel}`,
          `sql=${truncateObusDebugText(buildObusUserDeactivateSqlRequestUrl(), 120)}`
        ]
          .filter(Boolean)
          .join(" | "),
        meta: {
          type: "company",
          code: String(company?.code || "").trim(),
          partnerId: String(company?.id || "").trim(),
          clusterLabel
        }
      });

      pushObusUserCreateSample(failureSamples, {
        company: String(company?.code || "Firma").trim(),
        error: errorMessage
      });
    });

    updateSummary();
    finishObusLiveJob(job, `SQL kullanıcı listeleme tamamlanamadı: ${errorMessage}`);
  }
}

async function startObusUserDeactivateSearchJob({ companies = ["all"], usernameFilter = "", ownerUserId = 0 }) {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  if (partnerError && (!Array.isArray(partnerItems) || partnerItems.length === 0)) {
    return {
      ok: false,
      statusCode: 400,
      error: partnerError
    };
  }

  if (!Array.isArray(partnerItems) || partnerItems.length === 0) {
    return {
      ok: false,
      statusCode: 400,
      error: "Tüm Firmalar SQL kaydı boş. Önce firma listesini güncelleyin."
    };
  }

  const selectedCompaniesResult = resolveObusUserDeactivatePartnerItems(partnerItems, companies);
  if (selectedCompaniesResult.error) {
    return {
      ok: false,
      statusCode: 400,
      error: selectedCompaniesResult.error
    };
  }

  const selectedPartnerItems = Array.isArray(selectedCompaniesResult.items) ? selectedCompaniesResult.items : [];
  if (selectedPartnerItems.length === 0) {
    return {
      ok: false,
      statusCode: 400,
      error: "Sorgulanacak firma listesi bulunamadı."
    };
  }

  const job = createObusLiveJob({
    type: "obus-user-deactivate",
    ownerUserId: Number(ownerUserId || 0),
    totalCount: selectedPartnerItems.length
  });

  setImmediate(() => {
    runObusUserDeactivateSearchJob(job, {
      partnerItems: selectedPartnerItems,
      usernameFilter: String(usernameFilter || "").trim()
    }).catch((err) => {
      finishObusLiveJob(job, `Kullanıcı listeleme tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`);
    });
  });

  return {
    ok: true,
    job,
    companyCount: selectedPartnerItems.length
  };
}

function buildUetdsPricesRequestBody({ taskData = "", sessionId = "", deviceId = "", token = "" } = {}) {
  return {
    data: String(taskData || "").trim(),
    "device-session": {
      "session-id": String(sessionId || "").trim(),
      "device-id": String(deviceId || "").trim()
    },
    token: String(token || "").trim(),
    date: UETDS_PRICES_REQUEST_DATE,
    language: UETDS_PRICES_REQUEST_LANGUAGE
  };
}

function applyAuthorizedLinesLoginFailureReport({
  report,
  loginResult,
  partnerCode = "",
  username = "",
  loginBranchId = ""
}) {
  report.error = loginResult.error || "UserLogin başarısız.";
  report.errorDetail =
    String(loginResult.errorDetail || "").trim() || String(loginResult.tokenMissingDetail || "").trim();
  report.userMessage = "";
  report.requestBody = buildAuthorizedLinesLoginRequestBodyPreview({
    partnerCode,
    username,
    loginBranchId
  });
  report.requestUrl = String(loginResult.loginUrl || "").trim();
  report.responseBody = String(loginResult.rawLoginBody || "").trim() || "{}";
}

function applyAuthorizedLinesServiceReport(report, serviceReport) {
  report.requested = serviceReport.requested;
  report.status = serviceReport.status;
  report.error = serviceReport.error;
  report.userMessage = serviceReport.userMessage || "";
  report.requestUrl = serviceReport.requestUrl || report.requestUrl;
  report.requestBody = serviceReport.requestBody;
  report.responseBody = serviceReport.responseBody;
  report.errorDetail = serviceReport.error
    ? String(serviceReport.responseBody || "").trim() || String(serviceReport.error || "").trim()
    : "";
  report.sessionId = serviceReport.sessionId || report.sessionId;
  report.deviceId = serviceReport.deviceId || report.deviceId;
  report.branchId = serviceReport.branchId || report.branchId;
  report.loginToken = serviceReport.loginToken || report.loginToken;
}

async function fetchAuthorizedLinesLoginInfo({
  endpointUrl,
  companyUrl,
  partnerCode,
  partnerId = "",
  username,
  password,
  fallbackBranchId,
  timeoutMs = 90000,
  authorization = PARTNERS_API_AUTH,
  allowEmptyPartnerCode = false,
  loginBranchId = "",
  sessionClusterLabel = "",
  sessionCache = null
}) {
  const loginBaseUrls = buildUserLoginBaseUrlsWithOverrides({
    companyUrl,
    endpointUrl,
    partnerCode,
    partnerId
  });
  if (loginBaseUrls.length === 0) {
    return {
      ok: false,
      error: "Hedef URL geçersiz.",
      errorDetail: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      token: "",
      obusMerkezBranchKey: "",
      tokenMissingDetail: "",
      rawLoginBody: "",
      serviceLogs: [],
      failedServiceLog: null
    };
  }

  const normalizedPartnerCode = String(partnerCode || "").trim();
  const normalizedUsername = String(username || "").trim();
  const normalizedPassword = String(password || "");
  const normalizedLoginBranchId = String(loginBranchId || "").trim();

  if ((!normalizedPartnerCode && !allowEmptyPartnerCode) || !normalizedUsername || !normalizedPassword) {
    return {
      ok: false,
      error: "Firma (partner-code), kullanıcı adı ve şifre zorunludur.",
      errorDetail: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      token: "",
      obusMerkezBranchKey: "",
      tokenMissingDetail: "",
      rawLoginBody: "",
      serviceLogs: [],
      failedServiceLog: null
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(timeoutMs, 90000, 5000, 180000)
  );
  try {
    const allServiceLogs = [];
    let lastError = "UserLogin çağrısı başarısız.";
    let lastErrorDetail = "";
    let lastSessionId = "";
    let lastDeviceId = "";
    let lastLoginUrl = "";
    let lastRawLoginBody = "";
    let lastFailedServiceLog = null;

    for (const baseUrl of loginBaseUrls) {
      const sessionUrl = buildSessionUrlForPartnerUrl(baseUrl, sessionClusterLabel);
      const loginUrl = buildMembershipUserLoginUrl(baseUrl);
      if (!loginUrl) {
        lastError = "Membership UserLogin URL oluşturulamadı.";
        lastErrorDetail = "";
        continue;
      }
      lastLoginUrl = loginUrl;

      const sessionCacheKey = `${String(sessionUrl || "").trim()}|||${String(authorization || "").trim()}`;
      let sessionResult =
        sessionCache instanceof Map && sessionCache.has(sessionCacheKey) ? sessionCache.get(sessionCacheKey) : null;
      if (!sessionResult) {
        sessionResult = await fetchPartnerSessionCredentials(sessionUrl, controller.signal, authorization);
        if (sessionCache instanceof Map) {
          sessionCache.set(sessionCacheKey, sessionResult);
        }
      }
      if (sessionResult?.debug) {
        allServiceLogs.push(sessionResult.debug);
      }
      if (sessionResult.error) {
        lastError = `${sessionResult.error} (Session URL: ${sessionUrl})`;
        lastErrorDetail = "";
        lastFailedServiceLog = sessionResult?.debug || lastFailedServiceLog;
        continue;
      }

      lastSessionId = sessionResult.sessionId || "";
      lastDeviceId = sessionResult.deviceId || "";

      const payload = {
        data: {
          username: normalizedUsername,
          password: normalizedPassword,
          "remember-me": 0,
          "partner-code": normalizedPartnerCode,
          ...(normalizedLoginBranchId ? { "branch-id": normalizedLoginBranchId } : {})
        },
        "device-session": {
          "session-id": sessionResult.sessionId,
          "device-id": sessionResult.deviceId
        },
        date: "2020-02-24T18:03:00",
        language: "tr-TR"
      };

      try {
        const response = await fetch(loginUrl, {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            Authorization: authorization
          },
          body: JSON.stringify(payload),
          signal: controller.signal
        });

        const raw = await response.text();
        lastRawLoginBody = raw;
        const parsed = parseJsonSafe(raw);
        const loginTrace = buildObusServiceTraceEntry({
          service: "Membership UserLogin",
          url: loginUrl,
          status: response.status,
          requestBody: payload,
          responseBody: parsed ?? raw
        });
        allServiceLogs.push(loginTrace);
        const responseHeaders = {};
        response.headers.forEach((value, key) => {
          responseHeaders[String(key || "").toLowerCase()] = String(value || "");
        });
        const errorMessage =
          (parsed &&
            typeof parsed === "object" &&
            String(parsed.message || parsed.error || "").trim()) ||
          response.statusText ||
          "";

        if (!response.ok) {
          lastError = `Membership UserLogin HTTP ${response.status}: ${errorMessage || "Bilinmeyen hata"} (URL: ${loginUrl})`;
          lastErrorDetail = "";
          lastFailedServiceLog = loginTrace;
          continue;
        }

        const tokenValue =
          String(parsed?.token?.data || "").trim() ||
          String(parsed?.token?.token || "").trim() ||
          String(parsed?.token || "").trim() ||
          String(parsed?.data?.token?.data || "").trim() ||
          extractMembershipTokenDataFromPayload(parsed) ||
          extractTokenFromHeaders(responseHeaders) ||
          extractTokenFromRawText(raw) ||
          findNestedValue(parsed, new Set(["accesstoken"])) ||
          findNestedValue(parsed, new Set(["authorizationtoken"])) ||
          "";
        const obusMerkezBranchKey = String(extractObusMerkezBranchKeyFromUserLoginPayload(parsed) || "").trim();
        const detectedBranchId = buildUniqueLoginBranchCandidates(
          obusMerkezBranchKey,
          extractBranchIdFromUserLoginPayload(parsed),
          findNestedValue(parsed, new Set(["defaultbranchid", "activebranchid", "selectedbranchid", "branchid", "branch"])),
          fallbackBranchId
        )[0] || "";
        if (!String(tokenValue || "").trim()) {
          const tokenMissingDetail = buildUserLoginTokenMissingDetail({
            loginUrl,
            sessionId: sessionResult.sessionId,
            deviceId: sessionResult.deviceId,
            responseStatus: response.status,
            parsedBody: parsed,
            responseHeaders,
            rawBody: raw
          });
          if (detectedBranchId) {
            return {
              ok: true,
              error: null,
              errorDetail: "",
              sessionId: sessionResult.sessionId,
              deviceId: sessionResult.deviceId,
              branchId: detectedBranchId,
              token: "",
              obusMerkezBranchKey,
              loginUrl,
              tokenMissingDetail,
              rawLoginBody: raw,
              serviceLogs: allServiceLogs,
              failedServiceLog: null
            };
          }
          lastError = `Membership UserLogin token bulunamadı. (URL: ${loginUrl})`;
          lastErrorDetail = tokenMissingDetail;
          lastFailedServiceLog = loginTrace;
          continue;
        }
        const branchId =
          detectedBranchId ||
          extractBranchIdFromToken(tokenValue) ||
          extractBranchIdFromHeaders(responseHeaders) ||
          extractBranchIdFromText(raw) ||
          String(fallbackBranchId || "").trim() ||
          "";

        return {
          ok: true,
          error: null,
          errorDetail: "",
          sessionId: sessionResult.sessionId,
          deviceId: sessionResult.deviceId,
          branchId: String(branchId || "").trim(),
          token: String(tokenValue || "").trim(),
          obusMerkezBranchKey,
          loginUrl,
          tokenMissingDetail: "",
          rawLoginBody: raw,
          serviceLogs: allServiceLogs,
          failedServiceLog: null
        };
      } catch (err) {
        const loginTrace = buildObusServiceTraceEntry({
          service: "Membership UserLogin",
          url: loginUrl,
          requestBody: payload,
          responseBody: "",
          error: err?.message || "Membership UserLogin isteği başarısız."
        });
        allServiceLogs.push(loginTrace);
        lastError = `${err?.message || "Membership UserLogin isteği başarısız."} (URL: ${loginUrl})`;
        lastErrorDetail = "";
        lastFailedServiceLog = loginTrace;
      }
    }

    return {
      ok: false,
      error: lastError,
      errorDetail: lastErrorDetail,
      sessionId: lastSessionId,
      deviceId: lastDeviceId,
      branchId: String(fallbackBranchId || "").trim(),
      token: "",
      obusMerkezBranchKey: "",
      loginUrl: lastLoginUrl,
      tokenMissingDetail: "",
      rawLoginBody: lastRawLoginBody,
      serviceLogs: allServiceLogs,
      failedServiceLog: lastFailedServiceLog
    };
  } catch (err) {
    return {
      ok: false,
      error: err?.message || "UserLogin isteği gönderilemedi.",
      errorDetail: "",
      sessionId: "",
      deviceId: "",
      branchId: String(fallbackBranchId || "").trim(),
      token: "",
      obusMerkezBranchKey: "",
      loginUrl: "",
      tokenMissingDetail: "",
      rawLoginBody: "",
      serviceLogs: [],
      failedServiceLog: null
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchAuthorizedLinesUploadReport({ endpointUrl, partnerId, partnerCode, token }) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  if (!normalizedEndpointUrl) {
    return {
      requested: true,
      status: null,
      error: "Hedef URL geçersiz.",
      userMessage: "",
      requestUrl: "",
      requestBody: "{}",
      responseBody: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      loginToken: ""
    };
  }

  const normalizedPartnerId = String(partnerId || "").trim();
  if (!normalizedPartnerId) {
    return {
      requested: true,
      status: null,
      error: "PartnerId zorunludur.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody: "{}",
      responseBody: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      loginToken: ""
    };
  }

  const normalizedToken = String(token || "").trim();
  if (!normalizedToken) {
    return {
      requested: true,
      status: null,
      error: "Token zorunludur.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody: "{}",
      responseBody: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      loginToken: ""
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 90000);
  let requestBody = "{}";
  try {
    const sessionUrl = buildSessionUrlForPartnerUrl(normalizedEndpointUrl);
    const sessionResult = await fetchPartnerSessionCredentials(sessionUrl, controller.signal, PARTNERS_API_AUTH);
    if (sessionResult.error) {
      return {
        requested: true,
        status: null,
        error: sessionResult.error,
        userMessage: "",
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responseBody: "",
        sessionId: "",
        deviceId: "",
        branchId: "",
        loginToken: ""
      };
    }

    const body = {
      data: normalizedPartnerId,
      "device-session": {
        "session-id": sessionResult.sessionId,
        "device-id": sessionResult.deviceId
      },
      token: normalizedToken,
      date: "2016-03-11T11:33:00",
      language: "tr-TR"
    };
    requestBody = JSON.stringify(body, null, 2);

    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: PARTNERS_API_AUTH
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBody =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        requested: true,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        userMessage: "",
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responseBody: responseBody || "{}",
        sessionId: sessionResult.sessionId,
        deviceId: sessionResult.deviceId,
        branchId: "",
        loginToken: ""
      };
    }

    const isSuccess = isSuccessStatusPayload(parsed);
    if (!isSuccess) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        "İşlem başarısız döndü.";
      return {
        requested: true,
        status: response.status,
        error: reason,
        userMessage: "",
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responseBody: responseBody || "{}",
        sessionId: sessionResult.sessionId,
        deviceId: sessionResult.deviceId,
        branchId: "",
        loginToken: ""
      };
    }

    const payloadPartnerCode = extractPartnerCodeFromPayload(parsed);
    const effectivePartnerCode = String(payloadPartnerCode || partnerCode || "").trim();
    const userMessage = effectivePartnerCode
      ? `${effectivePartnerCode} izinli hatlar yüklenmiştir.`
      : "İzinli hatlar yüklenmiştir.";

    return {
      requested: true,
      status: response.status,
      error: null,
      userMessage,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responseBody: responseBody || "{}",
      sessionId: sessionResult.sessionId,
      deviceId: sessionResult.deviceId,
      branchId: "",
      loginToken: ""
    };
  } catch (err) {
    return {
      requested: true,
      status: null,
      error: err?.message || "İstek gönderilemedi.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      loginToken: ""
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchUetdsPricesUpdateReport({ endpointUrl, sessionId, deviceId, token }) {
  const normalizedEndpointUrl = normalizeTargetUrl(endpointUrl);
  if (!normalizedEndpointUrl) {
    return {
      requested: true,
      status: null,
      error: "Hedef URL geçersiz.",
      userMessage: "",
      requestUrl: "",
      requestBody: "{}",
      responseBody: "",
      sessionId: "",
      deviceId: "",
      branchId: "",
      loginToken: ""
    };
  }

  const normalizedSessionId = String(sessionId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();
  const normalizedToken = String(token || "").trim();

  if (!normalizedSessionId || !normalizedDeviceId) {
    return {
      requested: true,
      status: null,
      error: "UserLogin session/device bilgisi bulunamadı.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody: "{}",
      responseBody: "",
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      branchId: "",
      loginToken: normalizedToken
    };
  }

  if (!normalizedToken) {
    return {
      requested: true,
      status: null,
      error: "Token zorunludur.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody: "{}",
      responseBody: "",
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      branchId: "",
      loginToken: normalizedToken
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 90000);
  let requestBody = "{}";

  try {
    const taskResolution = await resolveUetdsPricesTaskData({
      endpointUrl: normalizedEndpointUrl,
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      token: normalizedToken
    });
    if (!taskResolution.ok || !taskResolution.taskData) {
      return {
        requested: true,
        status: taskResolution.status,
        error: taskResolution.error || "UETDS scheduled task bulunamadı.",
        userMessage: "",
        requestUrl: taskResolution.requestUrl || buildObusJobsUrl(normalizedEndpointUrl),
        requestBody: taskResolution.requestBody || "{}",
        responseBody: taskResolution.responseBody || "",
        sessionId: normalizedSessionId,
        deviceId: normalizedDeviceId,
        branchId: "",
        loginToken: normalizedToken
      };
    }

    const body = buildUetdsPricesRequestBody({
      taskData: taskResolution.taskData,
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      token: normalizedToken
    });
    requestBody = JSON.stringify(body, null, 2);

    const response = await fetch(normalizedEndpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: PARTNERS_API_AUTH
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBody =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();

    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        requested: true,
        status: response.status,
        error: `HTTP ${response.status}: ${reason}`,
        userMessage: "",
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responseBody: responseBody || "{}",
        sessionId: normalizedSessionId,
        deviceId: normalizedDeviceId,
        branchId: "",
        loginToken: normalizedToken
      };
    }

    const hasExplicitStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      const reason =
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim() ||
        "İşlem başarısız döndü.";
      return {
        requested: true,
        status: response.status,
        error: reason,
        userMessage: "",
        requestUrl: normalizedEndpointUrl,
        requestBody,
        responseBody: responseBody || "{}",
        sessionId: normalizedSessionId,
        deviceId: normalizedDeviceId,
        branchId: "",
        loginToken: normalizedToken
      };
    }

    const userMessage = "UETDS fiyatları güncellenmiştir. Sisteme 10 dk sonra yansıyacaktır.";

    return {
      requested: true,
      status: response.status,
      error: null,
      userMessage,
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responseBody: responseBody || "{}",
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      branchId: "",
      loginToken: normalizedToken
    };
  } catch (err) {
    return {
      requested: true,
      status: null,
      error: err?.message || "İstek gönderilemedi.",
      userMessage: "",
      requestUrl: normalizedEndpointUrl,
      requestBody,
      responseBody: "",
      sessionId: normalizedSessionId,
      deviceId: normalizedDeviceId,
      branchId: "",
      loginToken: normalizedToken
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function executeAuthorizedLinesScreenAction({
  submitAction,
  filters,
  selectedCompanyMeta,
  companies
}) {
  const report = buildAuthorizedLinesReportModel();
  report.requested = true;

  const normalizedAction = normalizeAuthorizedLinesSubmitAction(submitAction);
  const partnerCode = String(selectedCompanyMeta?.code || "").trim();
  const loginBranchId = String(selectedCompanyMeta?.branchId || selectedCompanyMeta?.id || "").trim();

  if (!selectedCompanyMeta) {
    report.error = "Firma seçimi zorunludur.";
    return report;
  }

  if (!filters.username || !filters.password) {
    report.error = "Kullanıcı adı ve şifre zorunludur.";
    return report;
  }

  const partnerId = resolveSelectedPartnerId({
    selectedCompanyMeta,
    selectedCompanyValue: filters.company,
    companies
  });
  if (normalizedAction === "authorized-lines" && !partnerId) {
    report.error = "Seçilen firma için PartnerId bulunamadı.";
    return report;
  }

  const loginResult = await resolveAuthorizedLinesLoginResultWithBranchFallback({
    endpointUrl: filters.endpointUrl,
    companyUrl: String(selectedCompanyMeta?.url || "").trim(),
    partnerCode,
    partnerId,
    username: filters.username,
    password: filters.password,
    fallbackBranchId: loginBranchId,
    sessionClusterLabel:
      normalizeObusClusterLabel(selectedCompanyMeta?.cluster || "") ||
      normalizeObusClusterLabel(extractClusterLabel(String(selectedCompanyMeta?.url || "").trim()))
  });
  report.sessionId = loginResult.sessionId || "";
  report.deviceId = loginResult.deviceId || "";
  report.branchId = loginResult.branchId || "";
  report.loginToken = loginResult.token || "";
  report.loginUrl = loginResult.loginUrl || "";

  if (!loginResult.ok) {
    applyAuthorizedLinesLoginFailureReport({
      report,
      loginResult,
      partnerCode,
      username: filters.username,
      loginBranchId
    });
    return report;
  }

  const effectiveToken = String(loginResult.token || "").trim();
  if (!effectiveToken) {
    report.error = "UserLogin yanıtında token bulunamadı.";
    report.errorDetail =
      String(loginResult.tokenMissingDetail || "").trim() || String(loginResult.errorDetail || "").trim();
    report.userMessage = "";
    report.requestBody = buildAuthorizedLinesLoginRequestBodyPreview({
      partnerCode,
      username: filters.username,
      loginBranchId
    });
    report.responseBody = String(loginResult.rawLoginBody || "").trim() || "{}";
    return report;
  }

  const serviceReport =
    normalizedAction === "uetds-prices"
      ? await fetchUetdsPricesUpdateReport({
          endpointUrl: buildUetdsPricesUpdateUrl(filters.endpointUrl || AUTHORIZED_LINES_API_URL),
          sessionId: report.sessionId,
          deviceId: report.deviceId,
          token: effectiveToken
        })
      : await fetchAuthorizedLinesUploadReport({
          endpointUrl: filters.endpointUrl,
          partnerId,
          partnerCode,
          token: effectiveToken
        });

  applyAuthorizedLinesServiceReport(report, serviceReport);
  return report;
}

async function fetchObusJobsClusterReport({
  clusterLabel,
  endpointBaseUrl,
  companyCandidates = [],
  username,
  password
}) {
  const normalizedClusterLabel = extractClusterLabel(clusterLabel);
  const endpointUrl = buildObusJobsUrl(endpointBaseUrl, normalizedClusterLabel);
  if (!endpointUrl) {
    return {
      clusterLabel: normalizedClusterLabel,
      jobs: [],
      error: "Servis URL oluşturulamadı."
    };
  }

  const candidates = shuffleArray(companyCandidates).filter(
    (item) => String(item?.code || "").trim()
  );
  if (!candidates.length) {
    return {
      clusterLabel: normalizedClusterLabel,
      jobs: [],
      error: "Bu cluster için kayıtlı firma bulunamadı."
    };
  }

  let lastError = "";
  let loginResult = null;
  for (const candidate of candidates) {
    const partnerCode = String(candidate.code || "").trim();
    const partnerId = String(candidate.id || "").trim();
    const fallbackBranchId = String(candidate.branchId || candidate.id || "").trim();
    const companyBaseUrl = String(candidate.url || endpointBaseUrl).trim() || endpointBaseUrl;
    const companyUrl = buildUrlForCluster(companyBaseUrl, normalizedClusterLabel);

    loginResult = await resolveAuthorizedLinesLoginResultWithBranchFallback({
      endpointUrl,
      companyUrl,
      partnerCode,
      partnerId,
      username,
      password,
      fallbackBranchId,
      sessionClusterLabel: normalizedClusterLabel
    });
    if (loginResult?.ok) {
      break;
    }
    lastError = String(loginResult?.error || lastError).trim() || "UserLogin başarısız.";
  }

  if (!loginResult?.ok) {
    return {
      clusterLabel: normalizedClusterLabel,
      jobs: [],
      error: lastError || "UserLogin başarısız."
    };
  }

  const requestBody = buildObusJobsRequestBody({
    sessionId: loginResult.sessionId,
    deviceId: loginResult.deviceId,
    token: loginResult.token
  });
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), OBUS_JOBS_TIMEOUT_MS);

  try {
    const response = await fetch(endpointUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: PARTNERS_API_AUTH
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    if (!response.ok) {
      const reason =
        (parsed &&
          typeof parsed === "object" &&
          String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
        response.statusText ||
        "Bilinmeyen hata";
      return {
        clusterLabel: normalizedClusterLabel,
        jobs: [],
        error: `HTTP ${response.status}: ${reason}`
      };
    }

    const hasExplicitStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      const reason =
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim() || "İşlem başarısız döndü.";
      return {
        clusterLabel: normalizedClusterLabel,
        jobs: [],
        error: reason
      };
    }

    return {
      clusterLabel: normalizedClusterLabel,
      jobs: extractObusJobsItems(parsed),
      error: null
    };
  } catch (err) {
    return {
      clusterLabel: normalizedClusterLabel,
      jobs: [],
      error: err?.message || "İstek gönderilemedi."
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function executeObusJobsScreenAction({ filters, partnerItems }) {
  const report = buildObusJobsReportModel();
  report.requested = true;
  const obusJobsLogin = getObusJobFixedCredentials();

  if (!obusJobsLogin.username || !obusJobsLogin.password) {
    report.error = buildObusServiceLoginConfigurationMessage(obusJobsLogin);
    return report;
  }

  const endpointBaseUrl = String(filters.endpointUrl || OBUS_JOBS_API_URL).trim() || OBUS_JOBS_API_URL;
  const companyCandidatesByCluster = buildClusterCompanyCandidates(partnerItems);
  const clusterLabels = Array.from(
    { length: PARTNER_CLUSTER_MAX - PARTNER_CLUSTER_MIN + 1 },
    (_, index) => `cluster${PARTNER_CLUSTER_MIN + index}`
  );

  const clusterResults = await runWithConcurrency(
    clusterLabels,
    OBUS_JOBS_CLUSTER_CONCURRENCY,
    async (clusterLabel) =>
      fetchObusJobsClusterReport({
        clusterLabel,
        endpointBaseUrl,
        companyCandidates: companyCandidatesByCluster.get(clusterLabel) || [],
        username: obusJobsLogin.username,
        password: obusJobsLogin.password
      })
  );

  const normalizedResults = clusterResults.map((item, index) => {
    if (item && typeof item === "object" && Object.prototype.hasOwnProperty.call(item, "clusterLabel")) {
      return item;
    }
    return {
      clusterLabel: clusterLabels[index],
      jobs: [],
      error:
        String(item?.error || "").trim() ||
        (typeof item === "string" ? item : "") ||
        "Cluster sonucu alınamadı."
    };
  });

  const tableModel = buildObusJobsTableModel(normalizedResults);
  const summary = summarizeObusJobsReport({
    clusterResults: normalizedResults,
    totalJobCount: tableModel.totalJobCount
  });
  report.clusterResults = normalizedResults;
  report.clusterRows = tableModel.clusterRows;
  report.jobIds = tableModel.jobIds;
  report.jobColumns = tableModel.jobColumns || [];
  report.clusterCount = tableModel.clusterCount;
  report.totalJobCount = tableModel.totalJobCount;
  report.successClusterCount = summary.successClusterCount;
  report.errorClusterCount = summary.errorClusterCount;

  const clusterErrors = normalizedResults
    .map((item) => `${item.clusterLabel}: ${String(item.error || "").trim()}`)
    .filter((text) => !/:\s*$/.test(text));
  if (clusterErrors.length > 0) {
    report.error = `${clusterErrors.length}/${clusterLabels.length} cluster alınamadı: ${clusterErrors
      .slice(0, 2)
      .join(" | ")}${clusterErrors.length > 2 ? ` (+${clusterErrors.length - 2} hata)` : ""}`;
  }

  return report;
}

function normalizeObusPartnerIdValue(rawValue) {
  const value = String(rawValue || "").trim();
  if (!/^-?\d+$/.test(value)) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatObusRequestDate(date = new Date()) {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const min = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${ss}`;
}

function formatObusIsoRequestDate(date = new Date()) {
  return formatObusRequestDate(date).replace(" ", "T");
}

function buildObusRuleDefineCompanyOptions(partnerItems = []) {
  return (Array.isArray(partnerItems) ? partnerItems : []).map((item) => {
    const value = buildCompanyOptionValue(item);
    const idText = String(item?.id || "").trim() || "N/A";
    const clusterText = String(item?.cluster || "").trim() || "cluster";
    const branchId = String(item?.branchId || "").trim();
    const isAbroad = parseAllCompaniesBooleanValue(item?.isAbroad ?? item?.isabroad ?? item?.is_abroad);
    return {
      value,
      label: `${String(item?.code || "").trim()} - ${idText} - ${clusterText} - ObusMerkezSubeID: ${branchId || "-"}`,
      code: String(item?.code || "").trim(),
      id: String(item?.id || "").trim(),
      cluster: String(item?.cluster || "").trim(),
      branchId,
      url: String(item?.url || "").trim(),
      isAbroad: isAbroad === null ? "" : isAbroad ? "true" : "false"
    };
  });
}

function buildObusUserDeactivateCompanyOptions(partnerItems = []) {
  return (Array.isArray(partnerItems) ? partnerItems : []).map((item) => {
    const value = buildCompanyOptionValue(item);
    const idText = String(item?.id || "").trim() || "N/A";
    const clusterText = String(item?.cluster || "").trim() || "cluster";
    const branchIdText = String(item?.branchId || "").trim() || "-";
    return {
      value,
      label: `${String(item?.code || "").trim()} - ${idText} - ${clusterText} - ObusMerkezSubeID: ${branchIdText}`,
      code: String(item?.code || "").trim(),
      id: String(item?.id || "").trim(),
      cluster: String(item?.cluster || "").trim(),
      branchId: String(item?.branchId || "").trim(),
      url: String(item?.url || "").trim()
    };
  });
}

function resolveObusUserDeactivatePartnerItems(partnerItems = [], selectedCompanyValues = ["all"]) {
  const normalizedItems = Array.isArray(partnerItems) ? partnerItems : [];
  const normalizedSelectedValues = (Array.isArray(selectedCompanyValues) ? selectedCompanyValues : [selectedCompanyValues])
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  if (
    normalizedSelectedValues.length === 0 ||
    normalizedSelectedValues.some((item) => item.toLowerCase() === "all")
  ) {
    return {
      items: normalizedItems,
      error: ""
    };
  }

  const matchedItems = [];
  const selectedKeys = new Set();

  for (const selectedValue of normalizedSelectedValues) {
    const parsedCompany = parseCompanyOptionValue(selectedValue);
    if (!parsedCompany) {
      return {
        items: [],
        error: "Seçilen firma bulunamadı. Listeden tekrar seçim yapın."
      };
    }

    const matchedItem = normalizedItems.find(
      (item) =>
        String(item?.code || "").trim() === parsedCompany.code &&
        String(item?.id || "").trim() === String(parsedCompany.id || "").trim() &&
        String(item?.cluster || "").trim().toLowerCase() === String(parsedCompany.cluster || "").trim().toLowerCase()
    );

    if (!matchedItem) {
      return {
        items: [],
        error: "Seçilen firma bulunamadı. Listeden tekrar seçim yapın."
      };
    }

    const itemKey = buildCompanyOptionValue(matchedItem);
    if (selectedKeys.has(itemKey)) continue;
    selectedKeys.add(itemKey);
    matchedItems.push(matchedItem);
  }

  return {
    items: matchedItems,
    error: ""
  };
}

function buildObusUserDeactivateSelectedUserKey({
  code = "",
  partnerId = "",
  clusterLabel = "",
  userId = "",
  username = ""
} = {}) {
  return [
    String(code || "").trim() || "Firma",
    String(partnerId || "").trim() || "partner",
    normalizeObusClusterLabel(clusterLabel) || "cluster",
    String(userId || "").trim() || "user",
    String(username || "").trim() || "username"
  ].join("|||");
}

function resolveObusUserDeactivateDeleteTargets(partnerItems = [], selectedUsers = []) {
  const normalizedItems = Array.isArray(partnerItems) ? partnerItems : [];
  const normalizedSelectedUsers = Array.isArray(selectedUsers) ? selectedUsers : [];
  if (normalizedSelectedUsers.length === 0) {
    return {
      targets: [],
      error: "Pasife alınacak kullanıcı seçmelisiniz."
    };
  }

  const seenKeys = new Set();
  const targets = [];

  for (const item of normalizedSelectedUsers) {
    const code = String(item?.code || "").trim();
    const partnerId = String(item?.partnerId || "").trim();
    const clusterLabel = normalizeObusClusterLabel(item?.clusterLabel || extractClusterLabel(item?.clusterUrl || ""));
    const username = String(item?.username || "").trim();
    const userIdValue = normalizeObusPartnerIdValue(item?.userId);
    const key =
      String(item?.key || "").trim() ||
      buildObusUserDeactivateSelectedUserKey({
        code,
        partnerId,
        clusterLabel,
        userId: userIdValue,
        username
      });

    if (!code || !partnerId || !clusterLabel || !Number.isInteger(userIdValue) || userIdValue <= 0) {
      return {
        targets: [],
        error: "Pasife alınacak kullanıcı bilgisi eksik veya geçersiz."
      };
    }

    const matchedCompany = normalizedItems.find(
      (partnerItem) =>
        String(partnerItem?.code || "").trim() === code &&
        String(partnerItem?.id || "").trim() === partnerId &&
        normalizeObusClusterLabel(partnerItem?.cluster || "") === clusterLabel
    );

    if (!matchedCompany) {
      return {
        targets: [],
        error: `Kullanıcı için firma kaydı bulunamadı: ${code} / ${partnerId} / ${clusterLabel}`
      };
    }

    if (seenKeys.has(key)) continue;
    seenKeys.add(key);
    targets.push({
      key,
      code,
      partnerId,
      clusterLabel,
      username,
      userIdValue,
      company: matchedCompany
    });
  }

  return {
    targets,
    error: ""
  };
}

function groupObusUserDeactivateDeleteTargetsByCompany(targets = []) {
  const map = new Map();
  (Array.isArray(targets) ? targets : []).forEach((target) => {
    if (!target || typeof target !== "object" || !target.company) return;
    const companyKey = buildCompanyOptionValue(target.company);
    if (!companyKey) return;
    if (!map.has(companyKey)) {
      map.set(companyKey, {
        company: target.company,
        users: []
      });
    }
    map.get(companyKey).users.push(target);
  });
  return Array.from(map.values());
}

function buildObusPartnerRuleRequestBody({
  partnerIdValue,
  startDate = "",
  endDate = "",
  rate = "",
  capacityBegin = "",
  capacityEnd = "",
  sessionId = "",
  deviceId = "",
  token = "",
  usePlaceholders = false
}) {
  const normalizedPartnerId =
    Number.isInteger(partnerIdValue) || typeof partnerIdValue === "number"
      ? Number(partnerIdValue)
      : normalizeObusPartnerIdValue(partnerIdValue);
  const normalizedStartDate = normalizeIsoDateInput(startDate);
  const normalizedEndDate = normalizeIsoDateInput(endDate);
  const parsedRate = Number.parseFloat(String(rate || "").trim().replace(",", "."));
  const parsedCapacityBegin = Number.parseInt(String(capacityBegin || "").trim(), 10);
  const parsedCapacityEnd = Number.parseInt(String(capacityEnd || "").trim(), 10);
  const startDateIso = parseIsoDateToUtc(normalizedStartDate, false)?.toISOString() || "";
  const endDateIso = parseIsoDateToUtc(normalizedEndDate, true)?.toISOString() || "";
  const normalizedRate = Number.isFinite(parsedRate) ? parsedRate : 0;
  const normalizedCapacityBegin = Number.isInteger(parsedCapacityBegin) ? parsedCapacityBegin : 1;
  const normalizedCapacityEnd = Number.isInteger(parsedCapacityEnd) ? parsedCapacityEnd : 3;

  return {
    data: {
      "partner-id": Number.isInteger(normalizedPartnerId) ? normalizedPartnerId : 0,
      "rule-id": OBUS_PARTNER_RULE_DEFAULT_RULE_ID,
      data: {
        StartDate: startDateIso,
        StartTime: "00:00",
        EndDate: endDateIso,
        EndTime: "23:59",
        BranchType: 1,
        CapacityBegin: normalizedCapacityBegin,
        CapacityEnd: normalizedCapacityEnd,
        IsActive: true,
        PriceChange: "Decrease",
        Rate: normalizedRate,
        Weekdays: "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        IncludedBranches: "",
        IgnoredBranches: "",
        IncludedRoutes: "",
        IgnoredRoutes: "",
        IncludedUsers: "",
        IgnoredUsers: ""
      },
      "partner-rule-station": []
    },
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    date: formatObusRequestDate(new Date()),
    language: "tr-TR",
    token: usePlaceholders ? "{{token}}" : String(token || "").trim()
  };
}

function buildObusPartnerRuleUpdateRequestBody({
  partnerIdValue,
  partnerRuleIdValue,
  startDate = "",
  endDate = "",
  rate = "",
  capacityBegin = "",
  capacityEnd = "",
  sessionId = "",
  deviceId = "",
  token = "",
  usePlaceholders = false
}) {
  const normalizedPartnerId =
    Number.isInteger(partnerIdValue) || typeof partnerIdValue === "number"
      ? Number(partnerIdValue)
      : normalizeObusPartnerIdValue(partnerIdValue);
  const normalizedPartnerRuleId =
    Number.isInteger(partnerRuleIdValue) || typeof partnerRuleIdValue === "number"
      ? Number(partnerRuleIdValue)
      : normalizeObusPartnerIdValue(partnerRuleIdValue);
  const normalizedStartDate = normalizeIsoDateInput(startDate);
  const normalizedEndDate = normalizeIsoDateInput(endDate);
  const parsedRate = Number.parseFloat(String(rate || "").trim().replace(",", "."));
  const normalizedCapacityBegin = normalizeObusPartnerIdValue(capacityBegin);
  const normalizedCapacityEnd = normalizeObusPartnerIdValue(capacityEnd);
  const startDateIso = parseIsoDateToUtc(normalizedStartDate, false)?.toISOString() || "";
  const endDateIso = parseIsoDateToUtc(normalizedEndDate, true)?.toISOString() || "";

  return {
    data: {
      "partner-id": Number.isInteger(normalizedPartnerId) ? normalizedPartnerId : 0,
      "partner-rule-id": Number.isInteger(normalizedPartnerRuleId) ? normalizedPartnerRuleId : 0,
      data: {
        StartDate: startDateIso,
        StartTime: "00:00",
        EndDate: endDateIso,
        EndTime: "23:59",
        Description: null,
        BranchType: 1,
        CapacityBegin: Number.isInteger(normalizedCapacityBegin) ? normalizedCapacityBegin : null,
        CapacityEnd: Number.isInteger(normalizedCapacityEnd) ? normalizedCapacityEnd : null,
        IsActive: true,
        PriceChange: "Decrease",
        MinutesToDepartureTime: null,
        Rate: Number.isFinite(parsedRate) ? parsedRate : null,
        Weekdays: "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday",
        IncludedBranches: "",
        IgnoredBranches: "",
        IncludedRoutes: "",
        IgnoredRoutes: "",
        IncludedUsers: "",
        IgnoredUsers: ""
      },
      "partner-rule-station": []
    },
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    date: formatObusRequestDate(new Date()),
    language: "tr-TR",
    token: usePlaceholders ? "{{token}}" : String(token || "").trim()
  };
}

async function createObusPartnerRuleForCompany({ company, startDate, endDate, rate, capacityBegin, capacityEnd }) {
  const obusUserCreateLogin = getObusUserCreateLoginCredentials();
  const companyLabel = String(company?.label || "").trim() || String(company?.meta?.code || "").trim() || "Firma";
  const parsedCompanyValue = parseCompanyOptionValue(company?.value);
  const companyCluster = normalizeObusClusterLabel(
    company?.meta?.cluster ||
      company?.cluster ||
      parsedCompanyValue?.cluster ||
      extractClusterLabel(String(companyLabel || "").trim())
  );
  const partnerCode = String(company?.meta?.code || "").trim();
  const partnerIdRaw = String(company?.meta?.id || "").trim();
  const partnerIdValue = normalizeObusPartnerIdValue(partnerIdRaw);
  const branchRaw = String(company?.meta?.branchId || "").trim();

  if (!obusUserCreateLogin.username || !obusUserCreateLogin.password) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: Number.isInteger(partnerIdValue) ? partnerIdValue : null,
      status: null,
      error: buildObusServiceLoginConfigurationMessage(obusUserCreateLogin)
    };
  }

  if (!companyCluster) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: Number.isInteger(partnerIdValue) ? partnerIdValue : null,
      status: null,
      error: "Firma için geçerli cluster bilgisi bulunamadı.",
      errorDetail: `Firma değeri: ${String(company?.value || "").trim() || "-"} | Etiket: ${companyLabel || "-"}`
    };
  }

  const createRuleUrl = normalizeTargetUrl(buildObusPartnerRuleCreateUrl(OBUS_PARTNER_RULE_CREATE_API_URL, companyCluster));
  const requestCluster = normalizeObusClusterLabel(extractClusterLabel(createRuleUrl));

  if (!Number.isInteger(partnerIdValue)) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: null,
      status: null,
      error: "Partner ID bulunamadı."
    };
  }

  if (!createRuleUrl) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: partnerIdValue,
      status: null,
      error: "CreatePartnerRule URL oluşturulamadı."
    };
  }

  if (requestCluster !== companyCluster) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: partnerIdValue,
      status: null,
      error: `CreatePartnerRule URL'i ${companyCluster} için üretilemedi.`
    };
  }

  const loginResult = await fetchAuthorizedLinesLoginInfo({
    endpointUrl: createRuleUrl,
    companyUrl: "",
    partnerCode,
    partnerId: partnerIdRaw,
    username: obusUserCreateLogin.username,
    password: obusUserCreateLogin.password,
    fallbackBranchId: branchRaw,
    loginBranchId: branchRaw,
    timeoutMs: OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS,
    authorization: OBUS_PARTNER_RULE_CREATE_API_AUTH
  });

  if (!loginResult.ok) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: partnerIdValue,
      status: null,
      error: String(loginResult.error || "UserLogin başarısız.").trim() || "UserLogin başarısız.",
      errorDetail: String(loginResult.errorDetail || loginResult.tokenMissingDetail || "").trim()
    };
  }

  const token = String(loginResult.token || "").trim();
  if (!token) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: partnerIdValue,
      status: null,
      error: "UserLogin token bulunamadı.",
      errorDetail:
        String(loginResult.tokenMissingDetail || loginResult.errorDetail || "").trim() ||
        (String(loginResult.rawLoginBody || "").trim()
          ? `UserLogin ham yanıtı: ${truncateObusDebugText(loginResult.rawLoginBody, 260)}`
          : "")
    };
  }

  const requestBodyObject = buildObusPartnerRuleRequestBody({
    partnerIdValue,
    startDate,
    endDate,
    rate,
    capacityBegin,
    capacityEnd,
    sessionId: String(loginResult.sessionId || "").trim(),
    deviceId: String(loginResult.deviceId || "").trim(),
    token
  });

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS, 90000, 5000, 180000)
  );

  try {
    const response = await fetch(createRuleUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_PARTNER_RULE_CREATE_API_AUTH
      },
      body: JSON.stringify(requestBodyObject),
      signal: controller.signal
    });
    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const parsedBody = parsed ?? raw;
    const apiMessage =
      (parsed &&
        typeof parsed === "object" &&
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      return {
        ok: false,
        label: companyLabel,
        requestUrl: createRuleUrl,
        partnerId: partnerIdValue,
        status: response.status,
        responseBody: parsedBody,
        error: `HTTP ${response.status}: ${apiMessage}`
      };
    }

    const hasStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    if (hasStatusField && !isSuccessStatusPayload(parsed)) {
      return {
        ok: false,
        label: companyLabel,
        requestUrl: createRuleUrl,
        partnerId: partnerIdValue,
        status: response.status,
        responseBody: parsedBody,
        error: apiMessage || "CreatePartnerRule başarısız döndü."
      };
    }

    return {
      ok: true,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: partnerIdValue,
      status: response.status,
      responseBody: parsedBody,
      message: String(apiMessage || "Kural oluşturuldu.").trim()
    };
  } catch (err) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: createRuleUrl,
      partnerId: partnerIdValue,
      status: null,
      error: err?.message || "CreatePartnerRule isteği gönderilemedi."
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function createObusPartnerRulesByCompanies({ companies, startDate, endDate, rate, capacityBegin, capacityEnd }) {
  const selectedCompanies = Array.isArray(companies) ? companies : [];
  if (selectedCompanies.length === 0) {
    return {
      successItems: [],
      failureItems: [],
      results: []
    };
  }

  const workerResults = await runWithConcurrency(
    selectedCompanies,
    Math.max(1, OBUS_PARTNER_RULE_CREATE_CONCURRENCY),
    async (company) => createObusPartnerRuleForCompany({ company, startDate, endDate, rate, capacityBegin, capacityEnd })
  );

  const successItems = [];
  const failureItems = [];
  const results = workerResults.map((item, index) => {
    if (item?.ok) {
      successItems.push(item);
      return item;
    }
    const fallback = {
      ok: false,
      label: String(selectedCompanies[index]?.label || "").trim() || "Firma",
      requestUrl: "",
      partnerId: null,
      status: null,
      error: String(item?.error?.message || item?.error || "CreatePartnerRule isteği başarısız.").trim()
    };
    failureItems.push(fallback);
    return fallback;
  });

  return {
    successItems,
    failureItems,
    results
  };
}

async function updateObusPartnerRuleForCompany({
  company,
  partnerRuleId,
  startDate,
  endDate,
  rate,
  capacityBegin,
  capacityEnd
}) {
  const obusUserCreateLogin = getObusUserCreateLoginCredentials();
  const companyLabel = String(company?.label || "").trim() || String(company?.meta?.code || "").trim() || "Firma";
  const parsedCompanyValue = parseCompanyOptionValue(company?.value);
  const companyCluster = normalizeObusClusterLabel(
    company?.meta?.cluster ||
      company?.cluster ||
      parsedCompanyValue?.cluster ||
      extractClusterLabel(String(companyLabel || "").trim())
  );
  const partnerCode = String(company?.meta?.code || "").trim();
  const partnerIdRaw = String(company?.meta?.id || "").trim();
  const partnerIdValue = normalizeObusPartnerIdValue(partnerIdRaw);
  const partnerRuleIdValue = normalizeObusPartnerIdValue(partnerRuleId);
  const branchRaw = String(company?.meta?.branchId || "").trim();

  if (!obusUserCreateLogin.username || !obusUserCreateLogin.password) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: Number.isInteger(partnerIdValue) ? partnerIdValue : null,
      partnerRuleId: Number.isInteger(partnerRuleIdValue) ? partnerRuleIdValue : null,
      status: null,
      error: buildObusServiceLoginConfigurationMessage(obusUserCreateLogin)
    };
  }

  if (!companyCluster) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: Number.isInteger(partnerIdValue) ? partnerIdValue : null,
      partnerRuleId: Number.isInteger(partnerRuleIdValue) ? partnerRuleIdValue : null,
      status: null,
      error: "Firma için geçerli cluster bilgisi bulunamadı.",
      errorDetail: `Firma değeri: ${String(company?.value || "").trim() || "-"} | Etiket: ${companyLabel || "-"}`
    };
  }

  const updateRuleUrl = normalizeTargetUrl(buildObusPartnerRuleUpdateUrl(OBUS_PARTNER_RULE_UPDATE_API_URL, companyCluster));
  const requestCluster = normalizeObusClusterLabel(extractClusterLabel(updateRuleUrl));

  if (!Number.isInteger(partnerIdValue)) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: null,
      partnerRuleId: Number.isInteger(partnerRuleIdValue) ? partnerRuleIdValue : null,
      status: null,
      error: "Partner ID bulunamadı."
    };
  }

  if (!Number.isInteger(partnerRuleIdValue)) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: null,
      status: null,
      error: "PartnerRuleId bulunamadı."
    };
  }

  if (!updateRuleUrl) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: "",
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: null,
      error: "UpdatePartnerRule URL oluşturulamadı."
    };
  }

  if (requestCluster !== companyCluster) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: null,
      error: `UpdatePartnerRule URL'i ${companyCluster} için üretilemedi.`
    };
  }

  const loginResult = await fetchAuthorizedLinesLoginInfo({
    endpointUrl: updateRuleUrl,
    companyUrl: "",
    partnerCode,
    partnerId: partnerIdRaw,
    username: obusUserCreateLogin.username,
    password: obusUserCreateLogin.password,
    fallbackBranchId: branchRaw,
    loginBranchId: branchRaw,
    timeoutMs: OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS,
    authorization: OBUS_PARTNER_RULE_CREATE_API_AUTH
  });

  if (!loginResult.ok) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: null,
      error: String(loginResult.error || "UserLogin başarısız.").trim() || "UserLogin başarısız.",
      errorDetail: String(loginResult.errorDetail || loginResult.tokenMissingDetail || "").trim()
    };
  }

  const token = String(loginResult.token || "").trim();
  if (!token) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: null,
      error: "UserLogin token bulunamadı.",
      errorDetail:
        String(loginResult.tokenMissingDetail || loginResult.errorDetail || "").trim() ||
        (String(loginResult.rawLoginBody || "").trim()
          ? `UserLogin ham yanıtı: ${truncateObusDebugText(loginResult.rawLoginBody, 260)}`
          : "")
    };
  }

  const requestBodyObject = buildObusPartnerRuleUpdateRequestBody({
    partnerIdValue,
    partnerRuleIdValue,
    startDate,
    endDate,
    rate,
    capacityBegin,
    capacityEnd,
    sessionId: String(loginResult.sessionId || "").trim(),
    deviceId: String(loginResult.deviceId || "").trim(),
    token
  });

  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(OBUS_PARTNER_RULE_CREATE_TIMEOUT_MS, 90000, 5000, 180000)
  );

  try {
    const response = await fetch(updateRuleUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_PARTNER_RULE_CREATE_API_AUTH
      },
      body: JSON.stringify(requestBodyObject),
      signal: controller.signal
    });
    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const parsedBody = parsed ?? raw;
    const apiMessage =
      (parsed &&
        typeof parsed === "object" &&
        String(parsed["user-message"] || parsed.message || parsed.error || "").trim()) ||
      response.statusText ||
      "Bilinmeyen hata";

    if (!response.ok) {
      return {
        ok: false,
        label: companyLabel,
        requestUrl: updateRuleUrl,
        partnerId: partnerIdValue,
        partnerRuleId: partnerRuleIdValue,
        status: response.status,
        responseBody: parsedBody,
        error: `HTTP ${response.status}: ${apiMessage}`
      };
    }

    const hasStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);
    if (hasStatusField && !isSuccessStatusPayload(parsed)) {
      return {
        ok: false,
        label: companyLabel,
        requestUrl: updateRuleUrl,
        partnerId: partnerIdValue,
        partnerRuleId: partnerRuleIdValue,
        status: response.status,
        responseBody: parsedBody,
        error: apiMessage || "UpdatePartnerRule başarısız döndü."
      };
    }

    return {
      ok: true,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: response.status,
      responseBody: parsedBody,
      message: String(apiMessage || "Kural güncellendi.").trim()
    };
  } catch (err) {
    return {
      ok: false,
      label: companyLabel,
      requestUrl: updateRuleUrl,
      partnerId: partnerIdValue,
      partnerRuleId: partnerRuleIdValue,
      status: null,
      error: err?.message || "UpdatePartnerRule isteği gönderilemedi."
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function updateObusPartnerRulesByCompanies({
  companies,
  partnerRuleId,
  startDate,
  endDate,
  rate,
  capacityBegin,
  capacityEnd
}) {
  const selectedCompanies = Array.isArray(companies) ? companies : [];
  if (selectedCompanies.length === 0) {
    return {
      successItems: [],
      failureItems: [],
      results: []
    };
  }

  const workerResults = await runWithConcurrency(
    selectedCompanies,
    Math.max(1, OBUS_PARTNER_RULE_CREATE_CONCURRENCY),
    async (company) =>
      updateObusPartnerRuleForCompany({
        company,
        partnerRuleId,
        startDate,
        endDate,
        rate,
        capacityBegin,
        capacityEnd
      })
  );

  const successItems = [];
  const failureItems = [];
  const results = workerResults.map((item, index) => {
    if (item?.ok) {
      successItems.push(item);
      return item;
    }
    const fallback = {
      ok: false,
      label: String(selectedCompanies[index]?.label || "").trim() || "Firma",
      requestUrl: "",
      partnerId: null,
      partnerRuleId: null,
      status: null,
      error: String(item?.error?.message || item?.error || "UpdatePartnerRule isteği başarısız.").trim()
    };
    failureItems.push(fallback);
    return fallback;
  });

  return {
    successItems,
    failureItems,
    results
  };
}

function normalizeObusBulkUserTemplateName(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, OBUS_BULK_USER_TEMPLATE_NAME_MAX_LENGTH);
}

function normalizeObusBulkUserTemplateText(value) {
  return String(value || "").trim().slice(0, OBUS_BULK_USER_TEMPLATE_FIELD_MAX_LENGTH);
}

function normalizeObusBulkUserTemplatePassword(value) {
  const text = typeof value === "string" ? value : String(value || "");
  return text.slice(0, OBUS_BULK_USER_TEMPLATE_FIELD_MAX_LENGTH);
}

function extractObusBulkUserTemplateEntriesSource(input) {
  if (Array.isArray(input)) return input;
  if (input && typeof input === "object") {
    if (Array.isArray(input.entries)) return input.entries;
    if (Array.isArray(input.users)) return input.users;
  }
  return [];
}

function normalizeObusBulkUserTemplateEntry(input = {}) {
  const entry = input && typeof input === "object" ? input : {};
  return {
    fullName: normalizeObusBulkUserTemplateText(
      entry.fullName ??
        entry["full-name"] ??
        entry.full_name ??
        entry.fullname ??
        entry.nameSurname ??
        entry.name_surname ??
        ""
    ),
    username: normalizeObusBulkUserTemplateText(entry.username ?? entry.userName ?? entry.user_name ?? ""),
    password: normalizeObusBulkUserTemplatePassword(entry.password ?? "")
  };
}

function normalizeObusBulkUserTemplateEntries(input) {
  return extractObusBulkUserTemplateEntriesSource(input)
    .slice(0, OBUS_BULK_USER_TEMPLATE_ENTRY_LIMIT)
    .map((entry) => normalizeObusBulkUserTemplateEntry(entry))
    .filter((entry) => entry.fullName || entry.username || entry.password);
}

function parseObusBulkUserTemplateEntries(entriesJson) {
  if (typeof entriesJson !== "string") {
    return normalizeObusBulkUserTemplateEntries(entriesJson);
  }

  const raw = String(entriesJson || "").trim();
  if (!raw) return [];
  return normalizeObusBulkUserTemplateEntries(parseJsonSafe(raw) || []);
}

function serializeObusBulkUserTemplateEntries(entries) {
  return JSON.stringify({
    version: 1,
    entries: normalizeObusBulkUserTemplateEntries(entries)
  });
}

function buildObusBulkUserTemplateResponseItem(row, { includeEntries = false } = {}) {
  const entries = parseObusBulkUserTemplateEntries(row?.entries_json);
  const payload = {
    id: Number.isInteger(Number(row?.id)) ? Number(row.id) : null,
    name: normalizeObusBulkUserTemplateName(row?.name),
    entryCount: entries.length,
    createdAt: row?.created_at ? new Date(row.created_at).toISOString() : "",
    updatedAt: row?.updated_at ? new Date(row.updated_at).toISOString() : ""
  };

  if (includeEntries) {
    payload.entries = entries;
  }

  return payload;
}

async function listObusBulkUserTemplatesForUser(userId) {
  const userIdNum = Number(userId);
  if (!Number.isInteger(userIdNum)) return [];

  const result = await pool.query(
    `
      SELECT id, name, entries_json, created_at, updated_at
      FROM obus_bulk_user_templates
      WHERE created_by = $1 OR updated_by = $1
      ORDER BY lower(name) ASC, updated_at DESC, id DESC
    `,
    [userIdNum]
  );

  return result.rows.map((row) => buildObusBulkUserTemplateResponseItem(row));
}

async function getObusBulkUserTemplateByIdForUser(templateId, userId) {
  const templateIdNum = Number(templateId);
  const userIdNum = Number(userId);
  if (!Number.isInteger(templateIdNum) || !Number.isInteger(userIdNum)) return null;

  const result = await pool.query(
    `
      SELECT id, name, entries_json, created_at, updated_at
      FROM obus_bulk_user_templates
      WHERE id = $1
        AND (created_by = $2 OR updated_by = $2)
      LIMIT 1
    `,
    [templateIdNum, userIdNum]
  );

  return result.rows[0] || null;
}

async function findObusBulkUserTemplateByNameForUser(name, userId, excludeTemplateId = null) {
  const normalizedName = normalizeObusBulkUserTemplateName(name);
  const userIdNum = Number(userId);
  const excludeIdNum = Number(excludeTemplateId);
  if (!normalizedName || !Number.isInteger(userIdNum)) return null;

  if (Number.isInteger(excludeIdNum)) {
    const result = await pool.query(
      `
        SELECT id, name, entries_json, created_at, updated_at
        FROM obus_bulk_user_templates
        WHERE lower(name) = lower($1)
          AND (created_by = $2 OR updated_by = $2)
          AND id <> $3
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
      `,
      [normalizedName, userIdNum, excludeIdNum]
    );

    return result.rows[0] || null;
  }

  const result = await pool.query(
    `
      SELECT id, name, entries_json, created_at, updated_at
      FROM obus_bulk_user_templates
      WHERE lower(name) = lower($1)
        AND (created_by = $2 OR updated_by = $2)
      ORDER BY updated_at DESC, id DESC
      LIMIT 1
    `,
    [normalizedName, userIdNum]
  );

  return result.rows[0] || null;
}

async function saveObusBulkUserTemplateForUser({
  templateId = null,
  name,
  entries,
  userId
}) {
  const normalizedName = normalizeObusBulkUserTemplateName(name);
  const userIdNum = Number(userId);
  const templateIdNum = Number(templateId);
  const normalizedEntries = normalizeObusBulkUserTemplateEntries(entries);

  if (!normalizedName) {
    throw new Error("Şablon adı zorunludur.");
  }
  if (!Number.isInteger(userIdNum)) {
    throw new Error("Geçersiz kullanıcı.");
  }
  if (!normalizedEntries.length) {
    throw new Error("Kaydetmek için en az bir kullanıcı satırı doldurulmalıdır.");
  }

  const serializedEntries = serializeObusBulkUserTemplateEntries(normalizedEntries);
  const selectedTemplate = Number.isInteger(templateIdNum)
    ? await getObusBulkUserTemplateByIdForUser(templateIdNum, userIdNum)
    : null;
  const duplicateByName = await findObusBulkUserTemplateByNameForUser(
    normalizedName,
    userIdNum,
    selectedTemplate?.id || null
  );

  if (selectedTemplate) {
    if (duplicateByName && Number(duplicateByName.id) !== Number(selectedTemplate.id)) {
      const conflictError = new Error("Bu şablon adı zaten kullanılıyor.");
      conflictError.code = "template_name_exists";
      throw conflictError;
    }

    const result = await pool.query(
      `
        UPDATE obus_bulk_user_templates
        SET name = $2,
            entries_json = $3,
            updated_by = $4,
            updated_at = now()
        WHERE id = $1
        RETURNING id, name, entries_json, created_at, updated_at
      `,
      [selectedTemplate.id, normalizedName, serializedEntries, userIdNum]
    );

    return {
      action: "updated",
      item: buildObusBulkUserTemplateResponseItem(result.rows[0], { includeEntries: true })
    };
  }

  if (duplicateByName) {
    const result = await pool.query(
      `
        UPDATE obus_bulk_user_templates
        SET name = $2,
            entries_json = $3,
            updated_by = $4,
            updated_at = now()
        WHERE id = $1
        RETURNING id, name, entries_json, created_at, updated_at
      `,
      [duplicateByName.id, normalizedName, serializedEntries, userIdNum]
    );

    return {
      action: "updated",
      item: buildObusBulkUserTemplateResponseItem(result.rows[0], { includeEntries: true })
    };
  }

  const insertResult = await pool.query(
    `
      INSERT INTO obus_bulk_user_templates (
        name,
        entries_json,
        created_by,
        updated_by,
        updated_at
      )
      VALUES ($1, $2, $3, $3, now())
      RETURNING id, name, entries_json, created_at, updated_at
    `,
    [normalizedName, serializedEntries, userIdNum]
  );

  return {
    action: "created",
    item: buildObusBulkUserTemplateResponseItem(insertResult.rows[0], { includeEntries: true })
  };
}

async function deleteObusBulkUserTemplateForUser(templateId, userId) {
  const templateIdNum = Number(templateId);
  const userIdNum = Number(userId);
  if (!Number.isInteger(templateIdNum) || !Number.isInteger(userIdNum)) return false;

  const result = await pool.query(
    `
      DELETE FROM obus_bulk_user_templates
      WHERE id = $1
        AND (created_by = $2 OR updated_by = $2)
    `,
    [templateIdNum, userIdNum]
  );

  return result.rowCount > 0;
}

function normalizeObusUserCreateEntriesInput(input) {
  return extractObusBulkUserTemplateEntriesSource(input)
    .slice(0, OBUS_BULK_USER_TEMPLATE_ENTRY_LIMIT)
    .map((entry) => normalizeObusBulkUserTemplateEntry(entry));
}

function validateObusUserCreateEntries(input) {
  const normalizedEntries = normalizeObusUserCreateEntriesInput(input);
  const readyEntries = [];
  const incompleteRows = [];

  normalizedEntries.forEach((entry, index) => {
    const hasFullName = Boolean(String(entry?.fullName || "").trim());
    const hasUsername = Boolean(String(entry?.username || "").trim());
    const hasPassword = Boolean(String(entry?.password || ""));
    const filledFieldCount = [hasFullName, hasUsername, hasPassword].filter(Boolean).length;
    if (filledFieldCount === 0) return;
    if (filledFieldCount < 3) {
      incompleteRows.push(index + 1);
      return;
    }
    readyEntries.push({
      fullName: String(entry.fullName || "").trim(),
      username: String(entry.username || "").trim(),
      password: String(entry.password || "")
    });
  });

  if (incompleteRows.length > 0) {
    return {
      ok: false,
      error: `Bazı satırlar eksik. Ad Soyad, Kullanıcı Adı ve Şifre alanlarının tamamı doldurulmalıdır. Satır: ${incompleteRows.join(", ")}`
    };
  }

  if (readyEntries.length === 0) {
    return {
      ok: false,
      error: "En az bir kullanıcı satırı doldurulmalıdır."
    };
  }

  return {
    ok: true,
    entries: readyEntries
  };
}

function buildObusUserCreatePermissions(branchIdValue) {
  return OBUS_USER_CREATE_PERMISSION_TYPES.map((type) => ({
    "branch-id": Number(branchIdValue),
    type,
    "is-deleted": false,
    "user-id": 0
  }));
}

function buildObusMembershipCreateUserRequestBody({
  entry,
  partnerIdValue,
  branchIdValue,
  sessionId = "",
  deviceId = "",
  token = "",
  usePlaceholders = false
}) {
  const normalizedEntry = normalizeObusBulkUserTemplateEntry(entry);
  const normalizedPartnerId =
    Number.isInteger(Number(partnerIdValue)) && Number(partnerIdValue) > 0 ? Number(partnerIdValue) : 0;
  const normalizedBranchId =
    Number.isInteger(Number(branchIdValue)) && Number(branchIdValue) > 0 ? Number(branchIdValue) : 0;

  return {
    data: {
      "full-name": String(normalizedEntry.fullName || "").trim(),
      "is-active": true,
      "day-for-can-view-expired-journey": null,
      email: null,
      notes: null,
      password: String(normalizedEntry.password || ""),
      phone: "9999999999",
      username: String(normalizedEntry.username || "").trim(),
      "ignore-password-check": false,
      id: 0,
      "is-system-user": false,
      "user-modules": [
        {
          "module-id": "Obus",
          "partner-id": normalizedPartnerId,
          "user-id": 0
        }
      ],
      branches: [normalizedBranchId],
      "time-to-change-password": 0,
      "is-mac-address-check": false,
      permissions: buildObusUserCreatePermissions(normalizedBranchId),
      "report-permissions": [],
      "branch-station-permission": [],
      "user-branch-profile": []
    },
    "device-session": {
      "session-id": usePlaceholders ? "{{sessionId}}" : String(sessionId || "").trim(),
      "device-id": usePlaceholders ? "{{deviceId}}" : String(deviceId || "").trim()
    },
    language: "tr-TR",
    token: usePlaceholders ? "{{token}}" : String(token || "").trim()
  };
}

function extractObusUserCreateApiMessage(payload, fallback = "") {
  if (!payload || typeof payload !== "object") {
    return String(fallback || "").trim();
  }
  return (
    String(
      payload["user-message"] ||
        payload.message ||
        payload.error ||
        payload?.data?.message ||
        payload?.data?.error ||
        fallback ||
        ""
    ).trim()
  );
}

function buildObusUserCreateClusterBaseUrl(company, clusterLabel = "") {
  const normalizedCluster =
    normalizeObusClusterLabel(clusterLabel) ||
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_CREATE_API_URL)) ||
    "cluster3";

  return (
    normalizeTargetUrl(buildUrlForCluster(OBUS_USER_CREATE_API_URL, normalizedCluster)) ||
    normalizeTargetUrl(buildUrlForCluster(PARTNERS_API_URL, normalizedCluster)) ||
    normalizeTargetUrl(company?.url || "") ||
    normalizeTargetUrl(OBUS_USER_CREATE_API_URL)
  );
}

async function prepareObusUserCreateCompanyTarget(company, options = {}) {
  const loginCredentials = options && typeof options === "object" ? options.loginCredentials || {} : {};
  const sessionCache = options && typeof options === "object" ? options.sessionCache || null : null;
  const companyCode = String(company?.code || "").trim();
  const companyIdRaw = String(company?.id || "").trim();
  const branchIdRaw = String(company?.branchId || company?.id || "").trim();
  const clusterLabel =
    normalizeObusClusterLabel(company?.cluster || "") ||
    normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
    normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_CREATE_API_URL)) ||
    "cluster3";
  const clusterBaseUrl = buildObusUserCreateClusterBaseUrl(company, clusterLabel);
  const createUserUrl = buildMembershipCreateUserUrl(clusterBaseUrl || OBUS_USER_CREATE_API_URL, clusterLabel);
  const companyLabel = `${companyCode || "Firma"} - ${companyIdRaw || "N/A"} - ${clusterLabel}`;
  const partnerIdValue = normalizeObusPartnerIdValue(companyIdRaw);
  const branchIdValue = normalizeObusPartnerIdValue(branchIdRaw);
  const buildFailureResult = (error, errorDetail = "", logLines = []) => ({
    ok: false,
    companyCode,
    companyLabel,
    clusterLabel,
    requestUrl: createUserUrl,
    error: String(error || "").trim(),
    errorDetail: String(errorDetail || "").trim(),
    logLines: (Array.isArray(logLines) ? logLines : []).map((item) => String(item || "").trim()).filter(Boolean)
  });

  if (!companyCode) {
    return {
      ok: false,
      companyCode: "",
      companyLabel,
      clusterLabel,
      requestUrl: createUserUrl,
      error: "Firma kodu bulunamadı.",
      errorDetail: "",
      logLines: []
    };
  }

  if (!createUserUrl) {
    return {
      ...buildFailureResult("CreateUser URL oluşturulamadı."),
      requestUrl: "",
      logLines: []
    };
  }

  if (!Number.isInteger(partnerIdValue) || partnerIdValue <= 0) {
    return buildFailureResult("Partner ID bulunamadı.", `Firma ID: ${companyIdRaw || "-"}`);
  }

  if (!Number.isInteger(branchIdValue) || branchIdValue <= 0) {
    return buildFailureResult("ObusMerkezSubeID bulunamadı.", `Branch ID: ${branchIdRaw || "-"}`);
  }

  const loginResult = await resolveAuthorizedLinesLoginResultWithBranchFallback({
    endpointUrl: createUserUrl,
    companyUrl: clusterBaseUrl || createUserUrl,
    partnerCode: companyCode,
    partnerId: companyIdRaw,
    username: String(loginCredentials?.username || "").trim(),
    password: typeof loginCredentials?.password === "string" ? loginCredentials.password : "",
    fallbackBranchId: branchIdRaw,
    sessionClusterLabel: clusterLabel,
    authorization: OBUS_USER_CREATE_API_AUTH,
    timeoutMs: OBUS_USER_CREATE_TIMEOUT_MS,
    sessionCache
  });

  if (!loginResult.ok) {
    const loginTraceText = buildObusServiceTraceText(
      loginResult?.failedServiceLog || getLastObusServiceTrace(loginResult?.serviceLogs),
      loginResult?.error || ""
    );
    const rawLoginBodyText = String(loginResult.rawLoginBody || "").trim()
      ? `UserLogin ham yanıtı: ${truncateObusDebugText(loginResult.rawLoginBody, 260)}`
      : "";
    const detailText =
      String(loginResult.errorDetail || loginResult.tokenMissingDetail || "").trim() || rawLoginBodyText || loginTraceText;
    return buildFailureResult(
      String(loginResult.error || "UserLogin başarısız.").trim() || "UserLogin başarısız.",
      detailText,
      [loginTraceText, rawLoginBodyText]
    );
  }

  const token = String(loginResult.token || "").trim();
  if (!token) {
    const loginTraceText = buildObusServiceTraceText(
      loginResult?.failedServiceLog || getLastObusServiceTrace(loginResult?.serviceLogs),
      "UserLogin token bulunamadı."
    );
    const rawLoginBodyText = String(loginResult.rawLoginBody || "").trim()
      ? `UserLogin ham yanıtı: ${truncateObusDebugText(loginResult.rawLoginBody, 260)}`
      : "";
    return buildFailureResult(
      "UserLogin token bulunamadı.",
      String(loginResult.tokenMissingDetail || loginResult.errorDetail || "").trim() || rawLoginBodyText || loginTraceText,
      [loginTraceText, rawLoginBodyText]
    );
  }

  return {
    ok: true,
    companyCode,
    companyLabel,
    clusterLabel,
    requestUrl: createUserUrl,
    partnerIdValue,
    branchIdValue,
    sessionId: String(loginResult.sessionId || "").trim(),
    deviceId: String(loginResult.deviceId || "").trim(),
    token
  };
}

async function createObusUserForCompanyTarget({ companyTarget, entry }) {
  const requestBodyObject = buildObusMembershipCreateUserRequestBody({
    entry,
    partnerIdValue: companyTarget?.partnerIdValue,
    branchIdValue: companyTarget?.branchIdValue,
    sessionId: companyTarget?.sessionId,
    deviceId: companyTarget?.deviceId,
    token: companyTarget?.token
  });
  const requestBodyText = JSON.stringify(requestBodyObject, null, 2);
  const requestUrl = String(companyTarget?.requestUrl || "").trim();
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    toBoundedInt(OBUS_USER_CREATE_TIMEOUT_MS, 90000, 5000, 180000)
  );

  try {
    const response = await fetch(requestUrl, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: OBUS_USER_CREATE_API_AUTH
      },
      body: JSON.stringify(requestBodyObject),
      signal: controller.signal
    });

    const raw = await response.text();
    const parsed = parseJsonSafe(raw);
    const responseBodyText =
      parsed && typeof parsed === "object" ? JSON.stringify(parsed, null, 2) : String(raw || "").trim();
    const apiMessage =
      extractObusUserCreateApiMessage(parsed, response.statusText || "Bilinmeyen hata") || "Bilinmeyen hata";
    const hasExplicitStatusField =
      parsed &&
      typeof parsed === "object" &&
      ("status" in parsed || "success" in parsed || "status-code" in parsed);

    if (!response.ok) {
      return {
        ok: false,
        status: response.status,
        requestUrl,
        requestBody: requestBodyText,
        responseBody: responseBodyText,
        message: "",
        error: `HTTP ${response.status}: ${apiMessage}`,
        errorDetail: ""
      };
    }

    if (hasExplicitStatusField && !isSuccessStatusPayload(parsed)) {
      return {
        ok: false,
        status: response.status,
        requestUrl,
        requestBody: requestBodyText,
        responseBody: responseBodyText,
        message: "",
        error: apiMessage || "CreateUser isteği başarısız döndü.",
        errorDetail: ""
      };
    }

    return {
      ok: true,
      status: response.status,
      requestUrl,
      requestBody: requestBodyText,
      responseBody: responseBodyText,
      message: apiMessage || "Kullanıcı oluşturuldu.",
      error: "",
      errorDetail: ""
    };
  } catch (err) {
    return {
      ok: false,
      status: null,
      requestUrl,
      requestBody: requestBodyText,
      responseBody: "",
      message: "",
      error: err?.name === "AbortError" ? "CreateUser isteği zaman aşımına uğradı." : err?.message || "İstek gönderilemedi.",
      errorDetail: ""
    };
  } finally {
    clearTimeout(timeout);
  }
}

function pushObusUserCreateSample(list, item, limit = 8) {
  const target = Array.isArray(list) ? list : [];
  if (target.length >= limit) return;
  target.push(item);
}

function buildObusUserCreateLiveEventKey(companyCode = "", clusterLabel = "", entry = {}, entryIndex = 0) {
  return [
    String(companyCode || "").trim() || "Firma",
    String(clusterLabel || "").trim() || "cluster",
    String(entry?.username || "").trim() || String(entry?.fullName || "").trim() || `row-${Number(entryIndex) + 1}`,
    `row-${Number(entryIndex) + 1}`
  ].join("|||");
}

function buildObusUserCreateLiveEventLabel(companyCode = "", entry = {}, entryIndex = 0) {
  const username = String(entry?.username || "").trim() || String(entry?.fullName || "").trim() || "-";
  return `Satır ${Number(entryIndex) + 1} / ${String(companyCode || "").trim() || "Firma"} / ${username}`;
}

async function runObusBulkUserCreateJob(job, { entries, partnerItems }) {
  const loginCredentials = getObusUserCreateLoginCredentials();
  if (!loginCredentials.username || !loginCredentials.password) {
    finishObusLiveJob(
      job,
      buildObusServiceLoginConfigurationMessage(loginCredentials)
    );
    return;
  }

  const validatedEntries = validateObusUserCreateEntries(entries);
  if (!validatedEntries.ok) {
    finishObusLiveJob(job, validatedEntries.error);
    return;
  }

  const normalizedCompanies = Array.isArray(partnerItems) ? partnerItems : [];
  if (normalizedCompanies.length === 0) {
    finishObusLiveJob(
      job,
      "Firma listesi SQL'de boş. Önce Tüm Firmalar ekranında firma listesini güncelleyin."
    );
    return;
  }

  const sessionCache = new Map();
  const successSamples = [];
  const failureSamples = [];
  const readyEntries = validatedEntries.entries;
  const totalTargetCount = normalizedCompanies.length * readyEntries.length;
  job.totalCount = totalTargetCount;

  normalizedCompanies.forEach((company) => {
    const companyCode = String(company?.code || "").trim();
    const clusterLabel =
      normalizeObusClusterLabel(company?.cluster || "") ||
      normalizeObusClusterLabel(extractClusterLabel(company?.url || "")) ||
      normalizeObusClusterLabel(extractClusterLabel(OBUS_USER_CREATE_API_URL)) ||
      "cluster3";
    const clusterBaseUrl = buildObusUserCreateClusterBaseUrl(company, clusterLabel);
    const requestUrl = buildMembershipCreateUserUrl(clusterBaseUrl || OBUS_USER_CREATE_API_URL, clusterLabel);

    readyEntries.forEach((entry, entryIndex) => {
      pushObusLiveJobEvent(job, {
        key: buildObusUserCreateLiveEventKey(companyCode, clusterLabel, entry, entryIndex),
        label: buildObusUserCreateLiveEventLabel(companyCode, entry, entryIndex),
        statusKind: "pending",
        message: "Firma oturumu hazırlanıyor.",
        detailText: [
          `cluster=${clusterLabel}`,
          requestUrl ? `url=${truncateObusDebugText(String(requestUrl || "").trim(), 120)}` : ""
        ]
          .filter(Boolean)
          .join(" | ")
      });
    });
  });

  const preparedTargetsRaw = await runWithConcurrency(
    normalizedCompanies,
    OBUS_USER_CREATE_LOGIN_CONCURRENCY,
    async (company) => prepareObusUserCreateCompanyTarget(company, { loginCredentials, sessionCache })
  );

  const preparedTargets = preparedTargetsRaw.map((item, index) => {
    if (item && typeof item === "object" && Object.prototype.hasOwnProperty.call(item, "ok")) {
      return item;
    }

    const fallbackCompany = normalizedCompanies[index] || {};
    const fallbackCode = String(fallbackCompany?.code || "").trim() || "Firma";
    const fallbackCluster =
      normalizeObusClusterLabel(fallbackCompany?.cluster || "") ||
      normalizeObusClusterLabel(extractClusterLabel(fallbackCompany?.url || "")) ||
      "cluster";

    return {
      ok: false,
      companyCode: fallbackCode,
      companyLabel: `${fallbackCode} - ${String(fallbackCompany?.id || "").trim() || "N/A"} - ${fallbackCluster}`,
      clusterLabel: fallbackCluster,
      requestUrl: "",
      error: String(item?.error?.message || item?.error || "Firma oturumu hazırlanamadı.").trim(),
      errorDetail: "",
      logLines: []
    };
  });

  const readyCompanies = preparedTargets.filter((item) => item?.ok === true);
  const failedCompanies = preparedTargets.filter((item) => item?.ok !== true);
  const sampleRequestBody = JSON.stringify(
    buildObusMembershipCreateUserRequestBody({
      entry: readyEntries[0],
      partnerIdValue: readyCompanies[0]?.partnerIdValue || 0,
      branchIdValue: readyCompanies[0]?.branchIdValue || 0,
      usePlaceholders: true
    }),
    null,
    2
  );

  setObusLiveJobSummary(job, {
    companyCount: normalizedCompanies.length,
    userCount: readyEntries.length,
    targetCount: totalTargetCount,
    readyCompanyCount: readyCompanies.length,
    failedCompanyCount: failedCompanies.length,
    sampleRequestBody
  });

  failedCompanies.forEach((companyTarget) => {
    readyEntries.forEach((entry, entryIndex) => {
      pushObusLiveJobEvent(job, {
        key: buildObusUserCreateLiveEventKey(companyTarget?.companyCode, companyTarget?.clusterLabel, entry, entryIndex),
        label: buildObusUserCreateLiveEventLabel(companyTarget?.companyCode, entry, entryIndex),
        statusKind: "failure",
        ok: false,
        error: String(companyTarget?.error || "Firma oturumu hazırlanamadı.").trim(),
        errorDetail: String(companyTarget?.errorDetail || "").trim(),
        detailText: [
          String(companyTarget?.clusterLabel || "").trim() ? `cluster=${String(companyTarget.clusterLabel).trim()}` : "",
          String(companyTarget?.requestUrl || "").trim()
            ? `url=${truncateObusDebugText(String(companyTarget.requestUrl || "").trim(), 120)}`
            : ""
        ]
          .filter(Boolean)
          .join(" | "),
        logLines: Array.isArray(companyTarget?.logLines) ? companyTarget.logLines : []
      });
      pushObusUserCreateSample(failureSamples, {
        company: String(companyTarget?.companyCode || "Firma").trim(),
        username: String(entry?.username || "").trim(),
        error: String(companyTarget?.error || "Firma oturumu hazırlanamadı.").trim()
      });
    });
  });

  const tasks = readyCompanies.flatMap((companyTarget) =>
    readyEntries.map((entry, entryIndex) => ({
      companyTarget,
      entry,
      entryIndex
    }))
  );

  await runWithConcurrency(tasks, OBUS_USER_CREATE_REQUEST_CONCURRENCY, async (task) => {
    const companyTarget = task?.companyTarget || {};
    const entry = task?.entry || {};
    const entryIndex = Number.isFinite(Number(task?.entryIndex)) ? Number(task.entryIndex) : 0;
    const eventKey = buildObusUserCreateLiveEventKey(companyTarget?.companyCode, companyTarget?.clusterLabel, entry, entryIndex);
    const eventLabel = buildObusUserCreateLiveEventLabel(companyTarget?.companyCode, entry, entryIndex);

    pushObusLiveJobEvent(job, {
      key: eventKey,
      label: eventLabel,
      statusKind: "pending",
      message: "CreateUser isteği gönderiliyor.",
      detailText: [
        String(companyTarget?.clusterLabel || "").trim() ? `cluster=${String(companyTarget.clusterLabel).trim()}` : "",
        Number.isInteger(Number(companyTarget?.partnerIdValue)) ? `partnerId=${Number(companyTarget.partnerIdValue)}` : "",
        Number.isInteger(Number(companyTarget?.branchIdValue)) ? `branchId=${Number(companyTarget.branchIdValue)}` : "",
        String(companyTarget?.requestUrl || "").trim()
          ? `url=${truncateObusDebugText(String(companyTarget.requestUrl || "").trim(), 120)}`
          : ""
      ]
        .filter(Boolean)
        .join(" | ")
    });

    const result = await createObusUserForCompanyTarget({
      companyTarget,
      entry
    });
    pushObusLiveJobEvent(job, {
      key: eventKey,
      label: eventLabel,
      statusKind: result.ok === true ? "success" : "failure",
      ok: result.ok === true,
      message: result.ok ? String(result.message || "Kullanıcı oluşturuldu.").trim() : "",
      error: result.ok ? "" : String(result.error || "CreateUser başarısız.").trim(),
      errorDetail: result.ok ? "" : String(result.errorDetail || "").trim(),
      detailText: [
        String(companyTarget?.clusterLabel || "").trim() ? `cluster=${String(companyTarget.clusterLabel).trim()}` : "",
        Number.isInteger(Number(companyTarget?.partnerIdValue)) ? `partnerId=${Number(companyTarget.partnerIdValue)}` : "",
        Number.isInteger(Number(companyTarget?.branchIdValue)) ? `branchId=${Number(companyTarget.branchIdValue)}` : "",
        Number.isFinite(Number(result?.status)) ? `status=${Number(result.status)}` : "",
        String(result?.requestUrl || "").trim()
          ? `url=${truncateObusDebugText(String(result.requestUrl || "").trim(), 120)}`
          : ""
      ]
        .filter(Boolean)
        .join(" | "),
      logLines:
        result.ok === true
          ? []
          : [
              String(result?.errorDetail || "").trim(),
              String(result?.responseBody || "").trim()
                ? truncateObusDebugText(String(result.responseBody || "").trim(), 260)
                : ""
            ].filter(Boolean)
    });

    if (result.ok === true) {
      pushObusUserCreateSample(successSamples, {
        company: String(companyTarget?.companyCode || "Firma").trim(),
        username: String(entry?.username || "").trim(),
        message: String(result.message || "Kullanıcı oluşturuldu.").trim()
      });
    } else {
      pushObusUserCreateSample(failureSamples, {
        company: String(companyTarget?.companyCode || "Firma").trim(),
        username: String(entry?.username || "").trim(),
        error: String(result.error || "CreateUser başarısız.").trim()
      });
    }
  });

  setObusLiveJobSummary(job, {
    companyCount: normalizedCompanies.length,
    userCount: readyEntries.length,
    targetCount: totalTargetCount,
    readyCompanyCount: readyCompanies.length,
    failedCompanyCount: failedCompanies.length,
    processedCount: Number(job.processedCount || 0),
    successCount: Number(job.successCount || 0),
    failureCount: Number(job.failureCount || 0),
    successSamples,
    failureSamples,
    sampleRequestBody
  });

  finishObusLiveJob(job);
}

app.get("/", async (req, res) => {
  if (req.session.user) {
    const targetRoute = await resolveInitialRouteForUser(req.session.user);
    return res.redirect(targetRoute);
  }
  return res.redirect("/login");
});

function requestWantsJson(req) {
  const accept = String(req?.get?.("accept") || "").toLocaleLowerCase("tr");
  return accept.includes("application/json");
}

function parseCheckboxBooleanValue(value, { truthy = "1" } = {}) {
  if (Array.isArray(value)) {
    return value.some((item) => String(item || "").trim() === truthy);
  }
  return String(value || "").trim() === truthy;
}

function renderLoginFailure(req, res, statusCode, errorMessage) {
  const normalizedStatusCode = Number.isInteger(statusCode) ? statusCode : 500;
  const normalizedMessage = String(errorMessage || "Hatalı giriş.").trim() || "Hatalı giriş.";
  if (requestWantsJson(req)) {
    return res.status(normalizedStatusCode).json({
      ok: false,
      error: normalizedMessage
    });
  }
  return res.status(normalizedStatusCode).render("login", { error: normalizedMessage });
}

const USER_LOGIN_DEVICE_RESULT_LABELS = {
  success: "Başarılı giriş",
  blocked: "Cihaz izni bekleniyor",
  pending: "Beklemede"
};
const USER_LOGIN_DEVICE_MAC_CACHE_TTL_MS = 30000;
const userLoginDeviceMacCache = new Map();
let localMachineMacAddressCache = null;

function normalizeLoginRequestIp(value = "") {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let normalized = raw;
  const forwardedMatch = normalized.match(/for=(?:"?\[?([^;\],"]+)\]?"?)/i);
  if (forwardedMatch && forwardedMatch[1]) {
    normalized = String(forwardedMatch[1] || "").trim();
  }

  if (normalized.includes(",")) {
    normalized = normalized
      .split(",")
      .map((item) => String(item || "").trim())
      .find(Boolean) || "";
  }

  normalized = normalized.replace(/^\[|\]$/g, "");
  if (normalized.startsWith("::ffff:")) {
    normalized = normalized.slice(7);
  }

  const zoneIndex = normalized.indexOf("%");
  if (zoneIndex >= 0) {
    normalized = normalized.slice(0, zoneIndex);
  }

  if (normalized === "::1") return "127.0.0.1";
  return normalized;
}

function normalizeLoginRequestMacAddress(value = "") {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw || raw.includes("incomplete")) return "";

  const normalizedHex = raw.replace(/[^0-9a-f]/g, "");
  if (normalizedHex.length !== 12 || /^0{12}$/.test(normalizedHex)) return "";

  return normalizedHex.match(/.{1,2}/g)?.join(":") || "";
}

function extractMacAddressFromSystemText(value = "") {
  const text = String(value || "");
  if (!text.trim()) return "";

  const directMatch = text.match(/(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}/i);
  if (directMatch) {
    return normalizeLoginRequestMacAddress(directMatch[0]);
  }

  const dottedMatch = text.match(/(?:[0-9a-f]{4}\.){2}[0-9a-f]{4}/i);
  if (dottedMatch) {
    return normalizeLoginRequestMacAddress(dottedMatch[0]);
  }

  return "";
}

function isPrivateOrLoopbackIpAddress(value = "") {
  const normalized = normalizeLoginRequestIp(value);
  const ipVersion = net.isIP(normalized);
  if (ipVersion === 4) {
    const [firstOctetRaw, secondOctetRaw] = normalized.split(".");
    const firstOctet = Number.parseInt(firstOctetRaw, 10);
    const secondOctet = Number.parseInt(secondOctetRaw, 10);
    if (firstOctet === 10 || firstOctet === 127) return true;
    if (firstOctet === 192 && secondOctet === 168) return true;
    if (firstOctet === 172 && secondOctet >= 16 && secondOctet <= 31) return true;
    if (firstOctet === 169 && secondOctet === 254) return true;
    return false;
  }

  if (ipVersion === 6) {
    const lowered = normalized.toLowerCase();
    return lowered === "::1" || lowered.startsWith("fc") || lowered.startsWith("fd") || lowered.startsWith("fe80:");
  }

  return false;
}

function getRequestClientIp(req) {
  const candidates = [
    req?.get?.("forwarded"),
    req?.get?.("cf-connecting-ip"),
    req?.get?.("x-real-ip"),
    req?.get?.("x-forwarded-for"),
    req?.ip,
    req?.socket?.remoteAddress,
    req?.connection?.remoteAddress
  ];

  for (const candidate of candidates) {
    const normalized = normalizeLoginRequestIp(candidate);
    if (normalized) return normalized;
  }

  return "";
}

function getPrimaryLocalMachineMacAddress() {
  if (localMachineMacAddressCache !== null) {
    return localMachineMacAddressCache;
  }

  const networkMap = os.networkInterfaces();
  const candidates = [];
  Object.values(networkMap || {}).forEach((items) => {
    (Array.isArray(items) ? items : []).forEach((item) => {
      if (!item || item.internal) return;
      const macAddress = normalizeLoginRequestMacAddress(item.mac);
      if (macAddress) candidates.push(macAddress);
    });
  });

  localMachineMacAddressCache = candidates[0] || "";
  return localMachineMacAddressCache;
}

function resolveMacAddressFromSystem(ipAddress = "") {
  const normalizedIpAddress = normalizeLoginRequestIp(ipAddress);
  if (!normalizedIpAddress || !isPrivateOrLoopbackIpAddress(normalizedIpAddress)) return "";

  if (normalizedIpAddress === "127.0.0.1") {
    return getPrimaryLocalMachineMacAddress();
  }

  const commands = [];
  if (process.platform === "darwin") {
    commands.push(["/usr/sbin/arp", ["-n", normalizedIpAddress]]);
  } else if (process.platform === "linux") {
    commands.push(["/sbin/ip", ["neigh", "show", normalizedIpAddress]]);
    commands.push(["ip", ["neigh", "show", normalizedIpAddress]]);
    commands.push(["/usr/sbin/arp", ["-n", normalizedIpAddress]]);
    commands.push(["arp", ["-n", normalizedIpAddress]]);
  } else if (process.platform === "win32") {
    commands.push(["arp", ["-a", normalizedIpAddress]]);
  } else {
    commands.push(["arp", ["-n", normalizedIpAddress]]);
  }

  for (const [command, args] of commands) {
    try {
      const rawOutput = execFileSync(command, args, {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
        timeout: 1500
      });
      const macAddress = extractMacAddressFromSystemText(rawOutput);
      if (macAddress) return macAddress;
    } catch (err) {
      continue;
    }
  }

  return "";
}

function resolveRequestMacAddress(ipAddress = "") {
  const normalizedIpAddress = normalizeLoginRequestIp(ipAddress);
  if (!normalizedIpAddress) return "";

  const cachedEntry = userLoginDeviceMacCache.get(normalizedIpAddress);
  if (cachedEntry && Number(cachedEntry.expiresAt || 0) > Date.now()) {
    return String(cachedEntry.macAddress || "").trim();
  }

  const macAddress = resolveMacAddressFromSystem(normalizedIpAddress);
  userLoginDeviceMacCache.set(normalizedIpAddress, {
    macAddress,
    expiresAt: Date.now() + USER_LOGIN_DEVICE_MAC_CACHE_TTL_MS
  });
  return macAddress;
}

function resolveRequestLoginDeviceInfo(req) {
  const ipAddress = getRequestClientIp(req);
  const macAddress = resolveRequestMacAddress(ipAddress);
  return {
    ipAddress,
    macAddress,
    userAgent: String(req?.get?.("user-agent") || "").trim()
  };
}

function normalizeUserLoginDeviceResult(value = "") {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "success") return "success";
  if (normalized === "blocked") return "blocked";
  return "pending";
}

function formatUserLoginDeviceTimestamp(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "-";
  return parsed.toLocaleString("tr-TR");
}

function buildUserLoginDeviceResultLabel(value = "") {
  const normalized = normalizeUserLoginDeviceResult(value);
  return USER_LOGIN_DEVICE_RESULT_LABELS[normalized] || USER_LOGIN_DEVICE_RESULT_LABELS.pending;
}

function normalizeUserLoginDeviceRow(row = {}) {
  const id = Number(row?.id);
  const userId = Number(row?.user_id ?? row?.userId);
  const lastAttemptAt = row?.last_attempt_at ?? row?.lastAttemptAt ?? null;
  const normalizedResult = normalizeUserLoginDeviceResult(row?.last_login_result ?? row?.lastLoginResult);
  const approved =
    row?.approved ?? row?.isApproved ?? row?.deviceApproved ?? row?.ip_enabled ?? row?.ipEnabled ?? row?.mac_enabled ?? row?.macEnabled;
  return {
    id: Number.isInteger(id) ? id : null,
    userId: Number.isInteger(userId) ? userId : null,
    ipAddress: normalizeLoginRequestIp(row?.ip_address ?? row?.ipAddress),
    macAddress: normalizeLoginRequestMacAddress(row?.mac_address ?? row?.macAddress),
    approved: Boolean(approved),
    ipEnabled: Boolean(row?.ip_enabled ?? row?.ipEnabled),
    macEnabled: Boolean(row?.mac_enabled ?? row?.macEnabled),
    lastAttemptAt,
    lastAttemptAtText: formatUserLoginDeviceTimestamp(lastAttemptAt),
    lastLoginResult: normalizedResult,
    lastLoginResultText: buildUserLoginDeviceResultLabel(normalizedResult),
    lastUserAgent: String(row?.last_user_agent ?? row?.lastUserAgent ?? "").trim()
  };
}

function getUserLoginDeviceRowRecencyValue(row = {}) {
  const timestamp = row?.lastAttemptAt ? new Date(row.lastAttemptAt).getTime() : 0;
  if (Number.isFinite(timestamp) && timestamp > 0) return timestamp;
  const id = Number(row?.id);
  return Number.isInteger(id) ? id : 0;
}

function buildUserLoginDeviceMatchCandidate(row, deviceInfo = {}) {
  const normalizedRow = normalizeUserLoginDeviceRow(row);
  const ipAddress = normalizeLoginRequestIp(deviceInfo?.ipAddress);
  const macAddress = normalizeLoginRequestMacAddress(deviceInfo?.macAddress);
  const rowHasIp = Boolean(normalizedRow.ipAddress);
  const rowHasMac = Boolean(normalizedRow.macAddress);
  const requestHasIp = Boolean(ipAddress);
  const requestHasMac = Boolean(macAddress);
  const ipMatches = Boolean(requestHasIp && rowHasIp && normalizedRow.ipAddress === ipAddress);
  const macMatches = Boolean(requestHasMac && rowHasMac && normalizedRow.macAddress === macAddress);

  let score = 0;
  if (requestHasIp && requestHasMac) {
    if (rowHasIp && rowHasMac) {
      score = ipMatches && macMatches ? 100 : 0;
    } else if (rowHasIp && !rowHasMac) {
      score = ipMatches ? 70 : 0;
    } else if (!rowHasIp && rowHasMac) {
      score = macMatches ? 70 : 0;
    }
  } else if (requestHasIp) {
    score = ipMatches ? 60 : 0;
  } else if (requestHasMac) {
    score = macMatches ? 60 : 0;
  }

  return {
    row: normalizedRow,
    score,
    ipMatches,
    macMatches,
    exactMatch: score >= 100
  };
}

function findBestUserLoginDeviceRow(rows, deviceInfo = {}) {
  const ipAddress = normalizeLoginRequestIp(deviceInfo?.ipAddress);
  const macAddress = normalizeLoginRequestMacAddress(deviceInfo?.macAddress);
  if (!ipAddress && !macAddress) {
    return null;
  }

  let bestCandidate = null;

  (Array.isArray(rows) ? rows : []).forEach((row) => {
    const candidate = buildUserLoginDeviceMatchCandidate(row, { ipAddress, macAddress });
    if (!candidate || candidate.score <= 0) return;

    if (!bestCandidate) {
      bestCandidate = candidate;
      return;
    }

    if (candidate.score !== bestCandidate.score) {
      if (candidate.score > bestCandidate.score) {
        bestCandidate = candidate;
      }
      return;
    }

    if (candidate.row.approved !== bestCandidate.row.approved) {
      if (candidate.row.approved) {
        bestCandidate = candidate;
      }
      return;
    }

    if (getUserLoginDeviceRowRecencyValue(candidate.row) > getUserLoginDeviceRowRecencyValue(bestCandidate.row)) {
      bestCandidate = candidate;
    }
  });

  return bestCandidate;
}

async function isUserLoginDeviceAllowed(userId, deviceInfo = {}) {
  const normalizedUserId = Number(userId);
  const ipAddress = normalizeLoginRequestIp(deviceInfo?.ipAddress);
  const macAddress = normalizeLoginRequestMacAddress(deviceInfo?.macAddress);

  if (!Number.isInteger(normalizedUserId) || (!ipAddress && !macAddress)) {
    return {
      allowed: false,
      matchedDevice: null
    };
  }

  const result = await pool.query(
    `
      SELECT id, user_id, ip_address, mac_address, approved, ip_enabled, mac_enabled, last_attempt_at, last_login_result, last_user_agent
      FROM user_login_devices
      WHERE user_id = $1
      ORDER BY last_attempt_at DESC, id DESC
    `,
    [normalizedUserId]
  );

  const matchedDeviceCandidate = findBestUserLoginDeviceRow(result.rows || [], {
    ipAddress,
    macAddress
  });
  const matchedDevice = matchedDeviceCandidate?.row || null;

  return {
    allowed: Boolean(matchedDevice?.approved),
    matchedDevice
  };
}

async function upsertUserLoginDeviceAttempt({ userId, deviceInfo = {}, loginResult = "pending" }) {
  const normalizedUserId = Number(userId);
  if (!Number.isInteger(normalizedUserId)) return null;

  const normalizedIpAddress = normalizeLoginRequestIp(deviceInfo?.ipAddress);
  const normalizedMacAddress = normalizeLoginRequestMacAddress(deviceInfo?.macAddress);
  const normalizedUserAgent = String(deviceInfo?.userAgent || "").trim() || null;
  const normalizedLoginResult = normalizeUserLoginDeviceResult(loginResult);
  if (!normalizedIpAddress && !normalizedMacAddress) return null;

  const existingResult = await pool.query(
    `
      SELECT id, user_id, ip_address, mac_address, approved, ip_enabled, mac_enabled, last_attempt_at, last_login_result, last_user_agent
      FROM user_login_devices
      WHERE user_id = $1
      ORDER BY last_attempt_at DESC, id DESC
    `,
    [normalizedUserId]
  );

  const matchedRowCandidate = findBestUserLoginDeviceRow(existingResult.rows || [], {
    ipAddress: normalizedIpAddress,
    macAddress: normalizedMacAddress
  });
  const matchedRow = matchedRowCandidate?.row || null;

  if (matchedRow && Number.isInteger(Number(matchedRow.id))) {
    const nextIpAddress = normalizedIpAddress || matchedRow.ipAddress || null;
    const nextMacAddress = normalizedMacAddress || matchedRow.macAddress || null;
    await pool.query(
      `
        UPDATE user_login_devices
        SET ip_address = $2,
            mac_address = $3,
            last_attempt_at = now(),
            last_login_result = $4,
            last_user_agent = $5,
            updated_at = now()
        WHERE id = $1
      `,
      [matchedRow.id, nextIpAddress, nextMacAddress, normalizedLoginResult, normalizedUserAgent]
    );
    return {
      ...matchedRow,
      ipAddress: nextIpAddress || "",
      macAddress: nextMacAddress || "",
      lastLoginResult: normalizedLoginResult,
      lastLoginResultText: buildUserLoginDeviceResultLabel(normalizedLoginResult),
      lastUserAgent: normalizedUserAgent || ""
    };
  }

  await pool.query(
    `
      INSERT INTO user_login_devices (
        user_id,
        ip_address,
        mac_address,
        approved,
        ip_enabled,
        mac_enabled,
        last_attempt_at,
        last_login_result,
        last_user_agent,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, false, false, false, now(), $4, $5, now(), now())
    `,
    [normalizedUserId, normalizedIpAddress || null, normalizedMacAddress || null, normalizedLoginResult, normalizedUserAgent]
  );

  const createdResult = await pool.query(
    `
      SELECT id, user_id, ip_address, mac_address, approved, ip_enabled, mac_enabled, last_attempt_at, last_login_result, last_user_agent
      FROM user_login_devices
      WHERE user_id = $1
      ORDER BY id DESC
      OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
    `,
    [normalizedUserId]
  );
  return normalizeUserLoginDeviceRow(createdResult.rows?.[0] || {});
}

async function listUserLoginDevicesGroupedByUserId() {
  const result = await pool.query(
    `
      SELECT id, user_id, ip_address, mac_address, approved, ip_enabled, mac_enabled, last_attempt_at, last_login_result, last_user_agent
      FROM user_login_devices
      ORDER BY user_id DESC, last_attempt_at DESC, id DESC
    `
  );

  const grouped = new Map();
  (result.rows || []).forEach((row) => {
    const normalizedRow = normalizeUserLoginDeviceRow(row);
    if (!Number.isInteger(normalizedRow.userId)) return;
    if (!grouped.has(normalizedRow.userId)) {
      grouped.set(normalizedRow.userId, []);
    }
    grouped.get(normalizedRow.userId).push(normalizedRow);
  });

  return grouped;
}

app.get("/login", async (req, res) => {
  if (req.session.user) {
    const targetRoute = await resolveInitialRouteForUser(req.session.user);
    return res.redirect(targetRoute);
  }
  return res.render("login", { error: null });
});

app.get("/api/login-lock-status", async (req, res) => {
  const username = String(req.query.username || "").trim();
  if (!username) {
    return res.json({
      ok: true,
      enabled: false,
      allowedComputerEnabled: false,
      version: null,
      username: ""
    });
  }

  try {
    const result = await pool.query(
      `
        SELECT username, login_input_lock_enabled, login_input_lock_version, allowed_computer_enabled
        FROM users
        WHERE username = $1
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [username]
    );
    const user = result.rows?.[0] || null;
    return res.json({
      ok: true,
      enabled: Boolean(user?.login_input_lock_enabled),
      allowedComputerEnabled: Boolean(user?.allowed_computer_enabled),
      deviceApprovalRequired: Boolean(user?.allowed_computer_enabled),
      version: Number.isInteger(Number(user?.login_input_lock_version))
        ? Number(user.login_input_lock_version)
        : null,
      username: String(user?.username || "").trim()
    });
  } catch (err) {
    console.error("Login lock status error:", err);
    return res.status(500).json({
      ok: false,
      error: `Login sabitleme durumu alınamadı: ${classifyDbErrorForUser(err)}`
    });
  }
});

app.post("/login", async (req, res) => {
  const username = String(req.body?.username || "").trim();
  const password = typeof req.body?.password === "string" ? req.body.password : "";

  if (!username || !password) {
    return renderLoginFailure(req, res, 400, "Kullanıcı adı ve şifre gerekli.");
  }

  try {
    const result = await pool.query(
      `
        SELECT id, username, password_hash, display_name, allowed_computer_enabled, login_input_lock_enabled, login_input_lock_version
        FROM users
        WHERE username = $1
      `,
      [username]
    );
    const user = result.rows[0];
    if (!user) {
      return renderLoginFailure(req, res, 401, "Hatalı giriş.");
    }

    const storedHash = typeof user.password_hash === "string" ? user.password_hash.trim() : "";
    if (!storedHash) {
      return renderLoginFailure(
        req,
        res,
        500,
        "Kullanici sifre kaydi eksik. scripts/reset-user-password.js komutunu calistirin."
      );
    }

    const ok = await bcrypt.compare(password, storedHash);
    if (!ok) {
      return renderLoginFailure(req, res, 401, "Hatalı giriş.");
    }

    const isAdminUser = String(user.username || "").trim().toLowerCase() === "admin";
    const deviceInfo = resolveRequestLoginDeviceInfo(req);

    const devicePermission = await isUserLoginDeviceAllowed(user.id, deviceInfo);
    const deviceApprovalRequired = Boolean(user.allowed_computer_enabled);
    const hasKnownDeviceRecord = Boolean(devicePermission.matchedDevice);
    const shouldBlockLogin =
      isAdminUser ? false : hasKnownDeviceRecord ? !devicePermission.allowed : deviceApprovalRequired;

    await upsertUserLoginDeviceAttempt({
      userId: user.id,
      deviceInfo,
      loginResult: shouldBlockLogin ? "blocked" : "success"
    });

    if (shouldBlockLogin) {
      const errorMessage = hasKnownDeviceRecord
        ? "Bu cihaz kaydi icin Cihaza Izin Ver pasif. Admin onayi olmadan giris basarili olmaz."
        : "Bu kullanici icin cihaz onayi zorunlu. Bu IP ve MAC adresi admin tarafinda onaylanmadan giris basarili olmaz.";
      return renderLoginFailure(req, res, 403, errorMessage);
    }

    req.session.user = {
      id: user.id,
      username: user.username,
      displayName: user.display_name
    };
    const targetRoute = await resolveInitialRouteForUser(req.session.user);
    if (requestWantsJson(req)) {
      return res.json({
        ok: true,
        redirectTo: targetRoute,
        loginLock: {
          enabled: Boolean(user.login_input_lock_enabled),
          version: Number.isInteger(Number(user.login_input_lock_version))
            ? Number(user.login_input_lock_version)
            : 1,
          username: String(user.username || "").trim()
        }
      });
    }
    return res.redirect(targetRoute);
  } catch (err) {
    console.error("Login error:", err);
    return renderLoginFailure(req, res, 500, `Sunucu hatasi: ${classifyDbErrorForUser(err)}`);
  }
});

app.post("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

app.get("/no-permission-home", requireAuth, (req, res) => {
  res.render("no-permission-home", {
    user: req.session.user,
    active: ""
  });
});

app.get("/dashboard", requireAuth, requireMenuAccess("dashboard"), (req, res) => {
  res.render("dashboard", {
    user: req.session.user,
    active: "dashboard",
    loginSuccess: req.query.login === "1",
    defaultEndpointBody: buildObusSessionRequestBodyText()
  });
});

app.get("/general/journey-update", requireAuth, requireMenuAccess("journey-update"), async (req, res) => {
  const today = getTodayIsoDate();
  const requestedCompany = typeof req.query.company === "string" ? req.query.company.trim() : "";
  const startDate = normalizeIsoDateInput(String(req.query.startDate || "").trim()) || today;
  const endDate = normalizeIsoDateInput(String(req.query.endDate || "").trim()) || today;
  const { companies, partnerItems, partnerError } = await loadAuthorizedLinesCompanies();

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
  const selectedCompanyCluster = normalizeObusClusterLabel(selectedCompanyMeta?.cluster || parsedCompany?.cluster || "");
  const resolvedEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateDailySummariesUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";
  const resolvedDetailEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateDetailUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";
  const resolvedUpdateEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateUpdateUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";

  const filters = {
    endpointUrl: resolvedEndpointUrl,
    company: selectedCompanyMeta ? buildCompanyOptionValue(selectedCompanyMeta) : requestedCompany,
    username: String(req.query.username || "").trim(),
    password: String(req.query.password || ""),
    startDate,
    endDate
  };
  const report = buildJourneyUpdateReportModel();

  if (requestedCompany && !selectedCompanyMeta) {
    report.requested = true;
    report.error = "Seçilen firma bulunamadı. Listeden tekrar seçim yapın.";
  }
  applyJourneyUpdateEditorStateToReport(report, {
    endpointUrl: resolvedUpdateEndpointUrl,
    editorValues: report.updateEditorValues,
    detailState: {},
    tableRows: report.tableRows,
    tableColumns: report.tableColumns
  });

  res.render("general-journey-update", {
    user: req.session.user,
    active: "journey-update",
    filters,
    companies,
    companySourceBaseUrl: STATION_PASSENGER_INFO_API_URL,
    journeyUpdateEditorFields: buildJourneyUpdateEditorFieldsForView(report.updateEditorValues),
    report,
    partnerError
  });
});

app.post("/general/journey-update", requireAuth, requireMenuAccess("journey-update"), async (req, res) => {
  const today = getTodayIsoDate();
  const submitAction = String(req.body.submitAction || "journey-update").trim();
  const isUpdateAction = submitAction === "journey-update-apply";
  const requestedCompany = typeof req.body.company === "string" ? req.body.company.trim() : "";
  const startDate = normalizeIsoDateInput(String(req.body.startDate || "").trim());
  const endDate = normalizeIsoDateInput(String(req.body.endDate || "").trim());
  const { companies, partnerItems, partnerError } = await loadAuthorizedLinesCompanies();

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
  const selectedCompanyCluster = normalizeObusClusterLabel(selectedCompanyMeta?.cluster || parsedCompany?.cluster || "");
  const resolvedEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateDailySummariesUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";
  const resolvedDetailEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateDetailUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";
  const resolvedUpdateEndpointUrl = selectedCompanyCluster
    ? buildJourneyUpdateUpdateUrl(STATION_PASSENGER_INFO_API_URL, selectedCompanyCluster)
    : "";

  const filters = {
    endpointUrl: resolvedEndpointUrl,
    company: selectedCompanyMeta ? buildCompanyOptionValue(selectedCompanyMeta) : requestedCompany,
    username: String(req.body.username || "").trim(),
    password: String(req.body.password || ""),
    startDate: startDate || today,
    endDate: endDate || today
  };
  const report = buildJourneyUpdateReportModel();
  report.requested = true;
  let detailStateForRender = {};

  report.updateEditorValues = normalizeJourneyUpdateEditorInputState(req.body);

  if (isUpdateAction) {
    const submittedTableRows = parseJourneyUpdateTableRowsState(req.body.tableRowsState);
    const submittedTableColumns = parseJourneyUpdateTableColumnsState(req.body.tableColumnsState);
    const submittedDetailState = parseJourneyUpdateDetailState(req.body.detailState);
    const requestedJourneyIds = parseJourneyUpdateRowIdsInput(req.body.journeyIds);
    const journeyIds =
      requestedJourneyIds.length > 0
        ? requestedJourneyIds
        : submittedTableRows.map((row) => String(row?.id || "").trim()).filter(Boolean);
    const parameters = buildJourneyUpdateParametersFromEditorState(report.updateEditorValues);
    const rowsById = new Map(
      submittedTableRows
        .map((row) => [String(row?.id || "").trim(), row])
        .filter(([rowId]) => Boolean(rowId))
    );
    detailStateForRender = submittedDetailState;

    report.tableRows = submittedTableRows;
    report.tableColumns = submittedTableColumns.length ? submittedTableColumns : buildJourneyUpdateTableColumns([]);
    report.requestUrl = resolvedUpdateEndpointUrl;

    if (requestedCompany && !selectedCompanyMeta) {
      report.error = "Seçilen firma bulunamadı. Listeden tekrar seçim yapın.";
    } else if (!selectedCompanyCluster) {
      report.error = "Seçilen firma için geçerli cluster bilgisi bulunamadı.";
    } else if (!resolvedUpdateEndpointUrl) {
      report.error = "UpdateJourney URL oluşturulamadı.";
    } else if (!journeyIds.length) {
      report.error = "Güncellenecek sefer id listesi bulunamadı.";
    } else if (!parameters.length) {
      report.error = "En az bir güncelleme alanı doldurmalısınız.";
    } else {
      const loginResult = await resolveStationPassengerLoginResult({
        endpointUrl: resolvedEndpointUrl || resolvedUpdateEndpointUrl,
        companyUrl: resolvedEndpointUrl || resolvedUpdateEndpointUrl,
        partnerCode: String(selectedCompanyMeta?.code || "").trim(),
        partnerId: String(selectedCompanyMeta?.id || "").trim(),
        username: filters.username,
        password: filters.password,
        fallbackBranchId: String(selectedCompanyMeta?.branchId || selectedCompanyMeta?.id || "").trim(),
        allowEmptyPartnerCode: false,
        authorization: STATION_PASSENGER_INFO_API_AUTH,
        timeoutMs: STATION_PASSENGER_INFO_TIMEOUT_MS
      });

      report.sessionId = String(loginResult?.sessionId || "").trim();
      report.deviceId = String(loginResult?.deviceId || "").trim();
      report.branchId = String(loginResult?.branchId || "").trim();
      report.loginToken = String(loginResult?.token || "").trim();
      report.loginUrl = String(loginResult?.loginUrl || "").trim();

      if (!(loginResult?.ok === true && String(loginResult?.token || "").trim())) {
        report.error = String(loginResult?.error || "").trim() || "UserLogin başarısız.";
        report.errorDetail =
          String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim() ||
          String(partnerError || "").trim();
      } else {
        const updateResults = await runWithConcurrency(journeyIds, 4, async (journeyId) => {
          const row = rowsById.get(String(journeyId || "").trim()) || {};
          const dataPayload = buildJourneyUpdateUpdateDataPayload({
            journeyId,
            row,
            tableColumns: report.tableColumns,
            detailStateById: submittedDetailState,
            overrideParameters: parameters
          });
          const result = await fetchJourneyUpdateUpdatePayload({
            endpointUrl: resolvedUpdateEndpointUrl,
            sessionId: loginResult.sessionId,
            deviceId: loginResult.deviceId,
            token: loginResult.token,
            journeyId,
            dataPayload,
            authorization: STATION_PASSENGER_INFO_API_AUTH
          });
          return {
            journeyId,
            dataPayload,
            ...result
          };
        });

        const successItems = updateResults.filter((item) => item?.ok === true);
        const failureItems = updateResults.filter((item) => item?.ok !== true);
        const successfulIds = successItems.map((item) => String(item?.journeyId || "").trim()).filter(Boolean);
        const nextDetailState =
          successfulIds.length > 0
            ? applyJourneyUpdateParametersToDetailState(submittedDetailState, parameters, successfulIds)
            : submittedDetailState;
        detailStateForRender = nextDetailState;
        const requestPreview = buildJourneyUpdateUpdatePreviewBody({
          endpointUrl: resolvedUpdateEndpointUrl,
          tableRows: report.tableRows,
          tableColumns: report.tableColumns,
          detailState: nextDetailState,
          editorValues: report.updateEditorValues,
          sampleLimit: 5
        });

        if (successfulIds.length > 0) {
          report.tableRows = applyJourneyUpdateParametersToRows(report.tableRows, parameters, successfulIds);
          report.tableColumns = mergeJourneyUpdateColumnsWithParameters(report.tableColumns, parameters);
        }

        report.requestBody = requestPreview.bodyText;
        report.responseBody = JSON.stringify(
          {
            ok: failureItems.length === 0,
            company: {
              code: String(selectedCompanyMeta?.code || "").trim(),
              id: String(selectedCompanyMeta?.id || "").trim(),
              cluster: selectedCompanyCluster
            },
            requestUrl: resolvedUpdateEndpointUrl,
            requestCount: journeyIds.length,
            successCount: successItems.length,
            failureCount: failureItems.length,
            updatedIds: successfulIds,
            parameterTypes: parameters.map((item) => String(item.type || "").trim()).filter(Boolean),
            failures: failureItems.map((item) => ({
              id: String(item?.journeyId || "").trim(),
              status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
              error: String(item?.error || "").trim(),
              detail: String(item?.detail || "").trim()
            }))
          },
          null,
          2
        );
        report.status =
          failureItems.length === 0
            ? 200
            : successItems.length > 0
              ? 207
              : Number(failureItems.find((item) => Number(item?.status) > 0)?.status || 0) || 0;

        if (failureItems.length === 0) {
          report.userMessage = `${successItems.length} sefer için UpdateJourney isteği başarıyla gönderildi.`;
        } else if (successItems.length > 0) {
          report.userMessage =
            `${successItems.length} sefer güncellendi. ${failureItems.length} sefer için hata alındı.`;
        } else {
          report.error = String(failureItems[0]?.error || "").trim() || "UpdateJourney başarısız.";
          report.errorDetail = failureItems
            .map((item) =>
              [String(item?.journeyId || "").trim(), String(item?.error || "").trim(), String(item?.detail || "").trim()]
                .filter(Boolean)
                .join(" | ")
            )
            .filter(Boolean)
            .join("\n\n");
        }
      }
    }
  } else if (requestedCompany && !selectedCompanyMeta) {
    report.error = "Seçilen firma bulunamadı. Listeden tekrar seçim yapın.";
  } else if (!startDate || !endDate) {
    report.error = "Baş Tar ve Bit Tar alanları zorunludur.";
  } else if (!selectedCompanyCluster) {
    report.error = "Seçilen firma için geçerli cluster bilgisi bulunamadı.";
  } else if (!resolvedEndpointUrl) {
    report.error = "GetDailyJourneySummaries URL oluşturulamadı.";
  } else if (!resolvedDetailEndpointUrl) {
    report.error = "GetJourneyDetail URL oluşturulamadı.";
  } else {
    report.requestUrl = resolvedEndpointUrl;
    const dailyRanges = buildDailyRequestRanges(startDate, endDate);
    if (!dailyRanges.length) {
      report.error = "Tarih aralığı geçersiz.";
    } else {
      const loginResult = await resolveStationPassengerLoginResult({
        endpointUrl: resolvedEndpointUrl,
        companyUrl: resolvedEndpointUrl,
        partnerCode: String(selectedCompanyMeta?.code || "").trim(),
        partnerId: String(selectedCompanyMeta?.id || "").trim(),
        username: filters.username,
        password: filters.password,
        fallbackBranchId: String(selectedCompanyMeta?.branchId || selectedCompanyMeta?.id || "").trim(),
        allowEmptyPartnerCode: false,
        authorization: STATION_PASSENGER_INFO_API_AUTH,
        timeoutMs: STATION_PASSENGER_INFO_TIMEOUT_MS
      });

      report.sessionId = String(loginResult?.sessionId || "").trim();
      report.deviceId = String(loginResult?.deviceId || "").trim();
      report.branchId = String(loginResult?.branchId || "").trim();
      report.loginToken = String(loginResult?.token || "").trim();
      report.loginUrl = String(loginResult?.loginUrl || "").trim();

      if (!(loginResult?.ok === true && String(loginResult?.token || "").trim())) {
        report.error = String(loginResult?.error || "").trim() || "UserLogin başarısız.";
        report.errorDetail =
          String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim() ||
          String(partnerError || "").trim();
      } else {
        const requestBodies = [];
        const dayResults = [];
        const tableRows = [];
        const detailColumnsByKey = new Map();
        const detailDataById = {};

        for (const range of dailyRanges) {
          const requestDate = String(range.startDate || "").trim();
          const requestBody = buildStationPassengerDailySummariesRequestBody({
            sessionId: loginResult.sessionId,
            deviceId: loginResult.deviceId,
            token: loginResult.token,
            dateValue: requestDate,
            includeDateField: true
          });
          requestBodies.push(requestBody);

          const fetchResult = await fetchStationPassengerDailyJourneySummaries({
            endpointUrl: resolvedEndpointUrl,
            sessionId: loginResult.sessionId,
            deviceId: loginResult.deviceId,
            token: loginResult.token,
            dateValue: requestDate,
            includeDateField: true,
            companyCode: String(selectedCompanyMeta?.code || "").trim(),
            cluster: selectedCompanyCluster,
            authorization: STATION_PASSENGER_INFO_API_AUTH
          });

          const summaryRows = buildJourneyUpdateTableRows(fetchResult.responsePayload, {
            requestDate,
            companyCode: String(selectedCompanyMeta?.code || "").trim(),
            partnerId: String(selectedCompanyMeta?.id || "").trim(),
            cluster: selectedCompanyCluster
          });
          const detailResults = fetchResult.ok
            ? await runWithConcurrency(summaryRows, 4, async (row) => {
                const journeyId = String(row?.id || "").trim();
                if (!journeyId || journeyId === "-") {
                  return {
                    row,
                    ok: false,
                    error: "GetJourneyDetail için satır id değeri bulunamadı.",
                    detail: "",
                    detailValues: {},
                    detailColumns: [],
                    detailDataById: {}
                  };
                }

                const detailResult = await fetchJourneyUpdateDetailPayload({
                  endpointUrl: resolvedDetailEndpointUrl,
                  sessionId: loginResult.sessionId,
                  deviceId: loginResult.deviceId,
                  token: loginResult.token,
                  journeyId,
                  dateValue: requestDate,
                  authorization: STATION_PASSENGER_INFO_API_AUTH
                });
                if (!detailResult.ok) {
                  return {
                    row,
                    ok: false,
                    error: String(detailResult.error || "").trim(),
                    detail: String(detailResult.detail || "").trim(),
                    detailValues: {},
                    detailColumns: [],
                    detailDataById: {}
                  };
                }

                const { detailsById, detailColumns, detailDataById } = buildJourneyUpdateDetailMap(detailResult.responsePayload);
                return {
                  row,
                  ok: true,
                  error: "",
                  detail: "",
                  detailValues: detailsById.get(journeyId) || {},
                  detailColumns,
                  detailDataById
                };
              })
            : [];

          const currentTableRows = summaryRows.map((row, index) => {
            const detailItem = Array.isArray(detailResults) ? detailResults[index] : null;
            if (detailItem && Array.isArray(detailItem.detailColumns)) {
              detailItem.detailColumns.forEach((column) => {
                detailColumnsByKey.set(column.key, column);
              });
            }
            if (detailItem?.detailDataById && typeof detailItem.detailDataById === "object") {
              Object.entries(detailItem.detailDataById).forEach(([detailId, detailData]) => {
                const normalizedDetailId = String(detailId || "").trim();
                const clonedDetailData = cloneJsonCompatibleValue(detailData);
                if (!normalizedDetailId || !clonedDetailData) return;
                detailDataById[normalizedDetailId] = clonedDetailData;
              });
            }
            return {
              ...row,
              ...((detailItem?.detailValues && typeof detailItem.detailValues === "object") ? detailItem.detailValues : {})
            };
          });
          tableRows.push(...currentTableRows);

          const dayErrorParts = [];
          if (!fetchResult.ok) {
            dayErrorParts.push(String(fetchResult.error || "").trim());
          }
          (Array.isArray(detailResults) ? detailResults : [])
            .filter((item) => item && item.ok === false && String(item.error || "").trim())
            .forEach((item) => {
              dayErrorParts.push(String(item.error || "").trim());
            });
          const dayDetailParts = [];
          if (!fetchResult.ok && fetchResult.detail) {
            dayDetailParts.push(String(fetchResult.detail || "").trim());
          }
          (Array.isArray(detailResults) ? detailResults : [])
            .filter((item) => item && item.ok === false && String(item.detail || "").trim())
            .forEach((item) => {
              dayDetailParts.push(String(item.detail || "").trim());
            });
          const detailSuccess = (Array.isArray(detailResults) ? detailResults : []).every((item) => item?.ok !== false);

          dayResults.push({
            date: requestDate,
            ok: fetchResult.ok === true && detailSuccess,
            status: Number.isFinite(Number(fetchResult.status)) ? Number(fetchResult.status) : null,
            rowCount: currentTableRows.length,
            error: dayErrorParts.join(" | "),
            detail: dayDetailParts.join(" | ")
          });
        }

        const uniqueTableRows = sortJourneyUpdateTableRows(dedupeJourneyUpdateTableRows(tableRows));
        const tableColumns = buildJourneyUpdateTableColumns(
          Array.from(detailColumnsByKey.values()).sort((a, b) =>
            String(a.label || "").localeCompare(String(b.label || ""), "tr")
          )
        );

        const successfulDayCount = dayResults.filter((item) => item.ok).length;
        const dataDayCount = dayResults.filter((item) => item.ok && item.rowCount > 0).length;
        const failedDayCount = Math.max(0, dayResults.length - successfulDayCount);
        const visibleDayResults = dayResults.filter((item) => item.rowCount > 0 || String(item.error || "").trim());

        report.requestBody = JSON.stringify(requestBodies, null, 2);
        report.responseBody = JSON.stringify(
          {
            ok: failedDayCount === 0,
            company: {
              code: String(selectedCompanyMeta?.code || "").trim(),
              id: String(selectedCompanyMeta?.id || "").trim(),
              cluster: selectedCompanyCluster
            },
            requestUrl: resolvedEndpointUrl,
            detailRequestUrl: resolvedDetailEndpointUrl,
            requestedRange: {
              startDate: dailyRanges[0]?.startDate || startDate,
              endDate: dailyRanges[dailyRanges.length - 1]?.endDate || endDate
            },
            requestedDays: dailyRanges.length,
            dataDays: dataDayCount,
            successfulDays: successfulDayCount,
            failedDays: failedDayCount,
            listedRows: uniqueTableRows.length,
            detailParameterColumns: tableColumns
              .filter((column) => String(column.key || "").startsWith("parameter_"))
              .map((column) => column.label),
            days: visibleDayResults.map((item) => ({
              date: item.date,
              ok: item.ok,
              status: item.status,
              rowCount: item.rowCount,
              error: item.error
            }))
          },
          null,
          2
        );
        report.tableRows = uniqueTableRows;
        report.tableColumns = tableColumns;
        report.dayResults = dayResults;
        detailStateForRender = detailDataById;
        report.status =
          failedDayCount === 0
            ? 200
            : successfulDayCount > 0
              ? 207
              : Number(dayResults.find((item) => Number(item.status) > 0)?.status || 0) || 0;

        if (failedDayCount === 0) {
          report.userMessage =
            uniqueTableRows.length > 0
              ? `${dailyRanges.length} gün tarandı. Veri bulunan ${dataDayCount} gün için ${uniqueTableRows.length} tekil sefer satırı listelendi.`
              : "Seçilen tarih aralığında veri bulunamadı.";
        } else if (successfulDayCount > 0) {
          report.userMessage =
            `${dailyRanges.length} günün ${successfulDayCount} tanesi başarılı tamamlandı. ` +
            `${failedDayCount} gün hata verdi, veri bulunan ${dataDayCount} gün için ${uniqueTableRows.length} tekil satır listelendi.`;
        } else {
          report.error = String(dayResults[0]?.error || "").trim() || "GetDailyJourneySummaries başarısız.";
          report.errorDetail = dayResults
            .map((item) => {
              const parts = [String(item.date || "").trim(), String(item.error || "").trim(), String(item.detail || "").trim()]
                .filter(Boolean);
              return parts.join(" | ");
            })
            .filter(Boolean)
            .join("\n\n");
        }
      }
    }
  }

  applyJourneyUpdateEditorStateToReport(report, {
    endpointUrl: resolvedUpdateEndpointUrl,
    editorValues: report.updateEditorValues,
    detailState: detailStateForRender,
    tableRows: report.tableRows,
    tableColumns: report.tableColumns
  });

  res.render("general-journey-update", {
    user: req.session.user,
    active: "journey-update",
    filters,
    companies,
    companySourceBaseUrl: STATION_PASSENGER_INFO_API_URL,
    journeyUpdateEditorFields: buildJourneyUpdateEditorFieldsForView(report.updateEditorValues),
    report,
    partnerError
  });
});

app.get("/api/obus-live/:jobId", requireAuth, async (req, res) => {
  const jobId = String(req.params.jobId || "").trim();
  if (!jobId) {
    return res.status(400).json({ ok: false, error: "jobId zorunludur." });
  }

  const cursorRaw = String(req.query.cursor || "0").trim();
  const parsedCursor = Number.parseInt(cursorRaw, 10);
  const cursor = Number.isFinite(parsedCursor) ? Math.max(0, parsedCursor) : 0;
  const job = readObusLiveJob(jobId, Number(req.session?.user?.id || 0));
  if (!job) {
    return res.status(404).json({ ok: false, error: "İşlem bulunamadı veya süresi doldu." });
  }
  return res.json(readObusLiveJobSnapshot(job, cursor));
});

app.post("/api/obus-user-deactivate/run", requireAuth, requireMenuAccess("obus-user-deactivate"), async (req, res) => {
  try {
    const companies = Array.isArray(req.body?.companies) ? req.body.companies : [];
    const usernameFilter = String(req.body?.usernameFilter || "").trim();
    const startResult = await startObusUserDeactivateSearchJob({
      companies,
      usernameFilter,
      ownerUserId: req.session?.user?.id || 0
    });

    if (!startResult.ok || !startResult.job) {
      return res.status(startResult.statusCode || 400).json({
        ok: false,
        error: String(startResult.error || "Sorgu başlatılamadı.").trim()
      });
    }

    return res.json({
      ok: true,
      jobId: String(startResult.job.id || "").trim(),
      totalCount: Number(startResult.job.totalCount || 0),
      createdAt: Number(startResult.job.createdAt || 0),
      companyCount: Number(startResult.companyCount || 0)
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `Sorgu başlatılamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.post(
  "/api/obus-user-deactivate/local-sql-users",
  requireAuth,
  requireMenuAccess("obus-user-deactivate"),
  async (req, res) => {
    const usernameFilter = String(req.body?.usernameFilter || "").trim();
    try {
      const result = await fetchObusUserDeactivateLocalSqlProxyRows({ usernameFilter });
      const rows = Array.isArray(result.rows) ? result.rows : [];
      return res.json({
        ok: true,
        count: rows.length,
        sourceUrl: result.sourceUrl || buildObusUserDeactivateSqlRequestUrl(),
        rows
      });
    } catch (err) {
      return res.status(502).json({
        ok: false,
        error: err?.message || "Yerel SQL proxy ile kullanıcı listeleme yapılamadı.",
        sourceUrl: buildObusUserDeactivateLocalSqlProxyUrls()[0] || buildObusUserDeactivateSqlRequestUrl()
      });
    }
  }
);

app.post("/api/obus-user-deactivate/deactivate", requireAuth, requireMenuAccess("obus-user-deactivate"), async (req, res) => {
  try {
    const usernameFilter = String(req.body?.usernameFilter || "").trim();
    const selectedUsers = Array.isArray(req.body?.users) ? req.body.users : [];

    if (!usernameFilter) {
      return res.status(400).json({
        ok: false,
        error: "Pasife alma için önce kullanıcı adı filtresi uygulayın."
      });
    }

    if (selectedUsers.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Pasife alınacak en az bir kullanıcı seçmelisiniz."
      });
    }

    const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
    if (partnerError && (!Array.isArray(partnerItems) || partnerItems.length === 0)) {
      return res.status(400).json({
        ok: false,
        error: String(partnerError || "Firma listesi alınamadı.").trim()
      });
    }

    const targetResult = resolveObusUserDeactivateDeleteTargets(partnerItems, selectedUsers);
    if (targetResult.error) {
      return res.status(400).json({
        ok: false,
        error: String(targetResult.error || "Pasife alınacak kullanıcılar çözümlenemedi.").trim()
      });
    }

    const groupedTargets = groupObusUserDeactivateDeleteTargetsByCompany(targetResult.targets);
    if (!groupedTargets.length) {
      return res.status(400).json({
        ok: false,
        error: "Pasife alınacak kullanıcı grubu oluşturulamadı."
      });
    }

    const loginCredentials = getObusUserCreateLoginCredentials();
    if (!loginCredentials.username || !loginCredentials.password) {
      return res.status(400).json({
        ok: false,
        error: buildObusServiceLoginConfigurationMessage(loginCredentials)
      });
    }

    const sessionCache = new Map();
    const companyResults = await runWithConcurrency(
      groupedTargets,
      OBUS_USER_DELETE_COMPANY_CONCURRENCY,
      async (group) =>
        deactivateObusUsersForCompany({
          company: group.company,
          selectedUsers: group.users,
          loginCredentials,
          sessionCache
        })
    );

    const successResults = companyResults.filter((item) => item?.ok === true);
    const failureResults = companyResults.filter((item) => item?.ok !== true);
    const updatedRows = successResults.flatMap((item) =>
      (Array.isArray(item?.selectedUsers) ? item.selectedUsers : []).map((selectedUser) => ({
        key: String(selectedUser?.key || "").trim(),
        userId: String(selectedUser?.userIdValue || "").trim(),
        username: String(selectedUser?.username || "").trim(),
        code: String(item?.code || "").trim(),
        partnerId: String(item?.partnerId || "").trim(),
        clusterLabel: String(item?.clusterLabel || "").trim(),
        isActive: false,
        isActiveText: "false"
      }))
    );
    const failures = failureResults.flatMap((item) =>
      (Array.isArray(item?.selectedUsers) ? item.selectedUsers : []).map((selectedUser) => ({
        key: String(selectedUser?.key || "").trim(),
        userId: String(selectedUser?.userIdValue || "").trim(),
        username: String(selectedUser?.username || "").trim(),
        code: String(item?.code || "").trim(),
        partnerId: String(item?.partnerId || "").trim(),
        clusterLabel: String(item?.clusterLabel || "").trim(),
        status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
        error: String(item?.error || "DeleteUser başarısız.").trim(),
        detail: String(item?.errorDetail || "").trim()
      }))
    );
    const firstFailedResult = failureResults.find((item) => item?.failedRequestPreview);
    const failedRequestPreview =
      firstFailedResult?.failedRequestPreview && typeof firstFailedResult.failedRequestPreview === "object"
        ? {
            ...firstFailedResult.failedRequestPreview,
            companyCode: String(firstFailedResult?.code || "").trim(),
            partnerId: String(firstFailedResult?.partnerId || "").trim(),
            clusterLabel: String(firstFailedResult?.clusterLabel || "").trim(),
            companyLabel: String(firstFailedResult?.companyLabel || "").trim()
          }
        : null;

    const successCount = updatedRows.length;
    const failureCount = failures.length;
    const hasSuccess = successCount > 0;
    const hasFailure = failureCount > 0;

    if (!hasSuccess) {
      return res.status(502).json({
        ok: false,
        error: String(failureResults[0]?.error || "Seçilen kullanıcılar pasife alınamadı.").trim(),
        failures,
        failedRequestPreview
      });
    }

    return res.status(hasFailure ? 207 : 200).json({
      ok: true,
      successCount,
      failureCount,
      updatedRows,
      failures,
      failedRequestPreview,
      userMessage:
        hasFailure
          ? `${successCount} kullanıcı pasife alındı. ${failureCount} kullanıcı için hata oluştu.`
          : `${successCount} kullanıcı pasife alındı.`
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `Pasife alma işlemi tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.post("/api/journey-search/stations", requireAuth, requireMenuAccess("journey-search"), async (req, res) => {
  try {
    const selectedCompanyValue = String(req.body?.company || "").trim();
    if (!selectedCompanyValue) {
      return res.status(400).json({ ok: false, error: "Firma seçmelisiniz." });
    }

    const { companies } = await loadJourneySearchCompanies();
    const selectedCompany = companies.find(
      (item) => !item?.disabled && String(item?.value || "") === selectedCompanyValue && item?.meta
    );

    if (!selectedCompany?.meta) {
      return res.status(400).json({ ok: false, error: "Seçilen firma bulunamadı. Listeden tekrar seçim yapın." });
    }

    const result = await fetchJourneySearchStations({
      company: selectedCompany.meta
    });

    if (result.error) {
      return res.status(Number.isFinite(Number(result.status)) ? Number(result.status) : 502).json({
        ok: false,
        error: result.error,
        step: String(result.step || "").trim(),
        details: String(result.detail || "").trim(),
        requestUrl: result.requestUrl || "",
        requestBody: String(result.requestBody || "").trim(),
        responseBody: String(result.responseBody || "").trim(),
        status: Number.isFinite(Number(result.status)) ? Number(result.status) : null,
        totalCount: 0
      });
    }

    return res.json({
      ok: true,
      items: Array.isArray(result.items) ? result.items : [],
      totalCount: Array.isArray(result.items) ? result.items.length : 0,
      requestUrl: result.requestUrl || "",
      details: String(result.detail || "").trim(),
      requestBody: String(result.requestBody || "").trim(),
      responseBody: String(result.responseBody || "").trim(),
      status: Number.isFinite(Number(result.status)) ? Number(result.status) : null
    });
  } catch (err) {
    console.error("Journey search stations error:", err);
    return res.status(500).json({
      ok: false,
      error: `İstasyon listesi alınamadı: ${err?.message || "Bilinmeyen hata"}`,
      step: "exception",
      details: buildObusServiceTraceText(
        buildObusServiceTraceEntry({
          service: "JourneySearchStationsApi",
          url: "/api/journey-search/stations",
          requestBody: req.body || {},
          responseBody: "",
          error: err?.message || "Bilinmeyen hata"
        }),
        err?.message || "Bilinmeyen hata",
        {
          bodyMaxLen: 120,
          responseMaxLen: 180
        }
      )
    });
  }
});

app.post("/api/journey-search/journeys", requireAuth, requireMenuAccess("journey-search"), async (req, res) => {
  try {
    const selectedCompanyValue = String(req.body?.company || "").trim();
    if (!selectedCompanyValue) {
      return res.status(400).json({ ok: false, error: "Firma seçmelisiniz." });
    }

    const originId = String(req.body?.origin || "").trim();
    const destinationId = String(req.body?.destination || "").trim();
    const dateValue = String(req.body?.date || "").trim();

    if (!originId || !destinationId) {
      return res.status(400).json({ ok: false, error: "Kalkış ve varış seçmelisiniz." });
    }
    if (!dateValue) {
      return res.status(400).json({ ok: false, error: "Tarih girilmesi zorunludur." });
    }

    const dateRange = buildJourneySearchDateRange(dateValue);
    if (!dateRange) {
      return res.status(400).json({ ok: false, error: "Tarih yyyy-aa-gg formatında olmalıdır." });
    }

    const { companies } = await loadJourneySearchCompanies();
    const selectedCompany = companies.find(
      (item) => !item?.disabled && String(item?.value || "") === selectedCompanyValue && item?.meta
    );

    if (!selectedCompany?.meta) {
      return res.status(400).json({ ok: false, error: "Seçilen firma bulunamadı. Listeden tekrar seçim yapın." });
    }

    const result = await fetchJourneySearchJourneys({
      company: selectedCompany.meta,
      originId,
      destinationId,
      dateRange
    });

    if (result.error) {
      return res.status(Number.isFinite(Number(result.status)) ? Number(result.status) : 502).json({
        ok: false,
        error: result.error,
        step: String(result.step || "").trim(),
        details: String(result.detail || "").trim(),
        requestUrl: result.requestUrl || "",
        requestBody: String(result.requestBody || "").trim(),
        responseBody: String(result.responseBody || "").trim(),
        status: Number.isFinite(Number(result.status)) ? Number(result.status) : null
      });
    }

    return res.json({
      ok: true,
      requestUrl: result.requestUrl || "",
      requestBody: String(result.requestBody || "").trim(),
      responseBody: String(result.responseBody || "").trim(),
      status: Number.isFinite(Number(result.status)) ? Number(result.status) : null,
      details: String(result.detail || "").trim()
    });
  } catch (err) {
    console.error("Journey search journeys error:", err);
    return res.status(500).json({
      ok: false,
      error: `GetJourneys isteği tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`,
      step: "exception",
      details: buildObusServiceTraceText(
        buildObusServiceTraceEntry({
          service: "JourneySearchJourneysApi",
          url: "/api/journey-search/journeys",
          requestBody: req.body || {},
          responseBody: "",
          error: err?.message || "Bilinmeyen hata"
        }),
        err?.message || "Bilinmeyen hata",
        {
          bodyMaxLen: 120,
          responseMaxLen: 180
        }
      )
    });
  }
});

app.get(
  "/general/authorized-lines-upload",
  requireAuth,
  requireMenuAccess("authorized-lines-upload"),
  async (req, res) => {
    const submitAction = normalizeAuthorizedLinesSubmitAction(req.query.submitAction);
    const requestedCompany = typeof req.query.company === "string" ? req.query.company.trim() : "";
    const { companies, partnerItems } = await loadAuthorizedLinesCompanies();

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

    const rawEndpointUrl = String(req.query.endpointUrl || AUTHORIZED_LINES_API_URL || "").trim();
    const resolvedEndpointUrl = buildAuthorizedLinesUploadUrl(
      rawEndpointUrl || AUTHORIZED_LINES_API_URL,
      selectedCompanyMeta?.cluster || ""
    );
    const filters = {
      endpointUrl: resolvedEndpointUrl,
      company: selectedCompanyMeta ? buildCompanyOptionValue(selectedCompanyMeta) : requestedCompany,
      username: String(req.query.username || "").trim(),
      password: String(req.query.password || "")
    };
    const invalidCompanySelection = requestedCompany && !selectedCompanyMeta;
    const shouldRun = req.query.run === "1";
    const report = buildAuthorizedLinesReportModel();

    if (invalidCompanySelection) {
      report.requested = true;
      report.error = "Seçilen firma bulunamadı. Listeden tekrar seçim yapın.";
    } else if (shouldRun) {
      Object.assign(
        report,
        await executeAuthorizedLinesScreenAction({
          submitAction,
          filters,
          selectedCompanyMeta,
          companies
        })
      );
    }

    res.render("general-authorized-lines-upload", {
      user: req.session.user,
      active: "authorized-lines-upload",
      filters,
      companies,
      companySourceBaseUrl: AUTHORIZED_LINES_API_URL,
      report
    });
  }
);

app.post(
  "/general/authorized-lines-upload",
  requireAuth,
  requireMenuAccess("authorized-lines-upload"),
  async (req, res) => {
    const submitAction = normalizeAuthorizedLinesSubmitAction(req.body.submitAction);
    const requestedCompany = typeof req.body.company === "string" ? req.body.company.trim() : "";
    const { companies, partnerItems } = await loadAuthorizedLinesCompanies();

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

    const rawEndpointUrl = String(req.body.endpointUrl || AUTHORIZED_LINES_API_URL || "").trim();
    const resolvedEndpointUrl = buildAuthorizedLinesUploadUrl(
      rawEndpointUrl || AUTHORIZED_LINES_API_URL,
      selectedCompanyMeta?.cluster || ""
    );
    const filters = {
      endpointUrl: resolvedEndpointUrl,
      company: selectedCompanyMeta ? buildCompanyOptionValue(selectedCompanyMeta) : requestedCompany,
      username: String(req.body.username || "").trim(),
      password: String(req.body.password || "")
    };
    const report = await executeAuthorizedLinesScreenAction({
      submitAction,
      filters,
      selectedCompanyMeta,
      companies
    });

    res.render("general-authorized-lines-upload", {
      user: req.session.user,
      active: "authorized-lines-upload",
      filters,
      companies,
      companySourceBaseUrl: AUTHORIZED_LINES_API_URL,
      report
    });
  }
);

app.get("/general/obus-jobs", requireAuth, requireMenuAccess("obus-jobs"), async (req, res) => {
  const { companies, partnerItems } = await loadAuthorizedLinesCompanies();
  const filters = {
    endpointUrl: String(req.query.endpointUrl || OBUS_JOBS_API_URL).trim() || OBUS_JOBS_API_URL
  };
  const report = buildObusJobsReportModel();

  res.render("general-obus-jobs", {
    user: req.session.user,
    active: "obus-jobs",
    filters,
    companies,
    report
  });
});

app.post("/general/obus-jobs", requireAuth, requireMenuAccess("obus-jobs"), async (req, res) => {
  const { companies, partnerItems } = await loadAuthorizedLinesCompanies();
  const filters = {
    endpointUrl: String(req.body.endpointUrl || OBUS_JOBS_API_URL).trim() || OBUS_JOBS_API_URL
  };
  const selectedCompanyMeta = partnerItems.length > 0 ? partnerItems[0] : null;
  const report = await executeObusJobsScreenAction({
    filters,
    partnerItems
  });

  if (Array.isArray(report.clusterResults) && report.clusterResults.length > 0) {
    try {
      const saveResult = await saveObusJobsReportToDb({
        report,
        selectedCompanyMeta,
        endpointUrl: filters.endpointUrl,
        userId: req.session.user?.id || null
      });
      report.saveResult = saveResult;
      report.successMessages.push(`SQL kaydı oluşturuldu. Kayıt No: ${saveResult.runId}`);
    } catch (err) {
      report.warningMessages.push(`SQL kaydı başarısız: ${summarizeErrorMessage(err)}`);
    }

    try {
      const slackResult = await postObusJobsReportToSlack({
        report,
        selectedCompanyMeta,
        user: req.session.user,
        saveResult: report.saveResult
      });
      report.slackResult = slackResult;
      report.successMessages.push(`Slack bildirimi ${slackResult.channelLabel} kanalına gönderildi.`);
      if (Array.isArray(slackResult.unresolvedMentionTargets) && slackResult.unresolvedMentionTargets.length > 0) {
        report.warningMessages.push(
          `Slack mention çözümlenemedi: ${slackResult.unresolvedMentionTargets.join(", ")}${
            slackResult.mentionLookupError ? ` | ${slackResult.mentionLookupError}` : ""
          }`
        );
      }
    } catch (err) {
      report.warningMessages.push(`Slack bildirimi başarısız: ${summarizeErrorMessage(err)}`);
    }
  }

  res.render("general-obus-jobs", {
    user: req.session.user,
    active: "obus-jobs",
    filters,
    companies,
    report
  });
});

app.get("/general/obus-user-create", requireAuth, requireMenuAccess("obus-user-create"), async (req, res) => {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  const sampleCompany = Array.isArray(partnerItems) && partnerItems.length > 0 ? partnerItems[0] : null;
  res.render("general-obus-user-create", {
    user: req.session.user,
    active: "obus-user-create",
    partnerError,
    companyCount: Array.isArray(partnerItems) ? partnerItems.length : 0,
    samplePartnerId: sampleCompany?.id || "",
    sampleBranchId: sampleCompany?.branchId || ""
  });
});

app.get("/general/obus-user-deactivate", requireAuth, requireMenuAccess("obus-user-deactivate"), async (req, res) => {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  const companyOptions = buildObusUserDeactivateCompanyOptions(partnerItems);
  const jobId = String(req.query.jobId || "").trim();
  const liveJob = jobId ? readObusLiveJob(jobId, Number(req.session?.user?.id || 0)) : null;
  res.render("general-obus-user-deactivate", {
    user: req.session.user,
    active: "obus-user-deactivate",
    partnerError,
    companyOptions,
    liveJob: liveJob
      ? {
          id: String(liveJob.id || "").trim(),
          done: Boolean(liveJob.done),
          createdAt: Number(liveJob.createdAt || 0),
          updatedAt: Number(liveJob.updatedAt || 0),
          finishedAt: Number(liveJob.finishedAt || 0),
          totalCount: Number(liveJob.totalCount || 0),
          processedCount: Number(liveJob.processedCount || 0),
          successCount: Number(liveJob.successCount || 0),
          failureCount: Number(liveJob.failureCount || 0)
        }
      : null
  });
});

app.get("/api/obus-user-create/templates", requireAuth, requireMenuAccess("obus-user-create"), async (req, res) => {
  try {
    const items = await listObusBulkUserTemplatesForUser(req.session.user?.id || null);
    return res.json({
      ok: true,
      items
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `Şablon listesi okunamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.get(
  "/api/obus-user-create/templates/:id",
  requireAuth,
  requireMenuAccess("obus-user-create"),
  async (req, res) => {
    try {
      const template = await getObusBulkUserTemplateByIdForUser(req.params.id, req.session.user?.id || null);
      if (!template) {
        return res.status(404).json({
          ok: false,
          error: "Şablon bulunamadı."
        });
      }

      return res.json({
        ok: true,
        item: buildObusBulkUserTemplateResponseItem(template, { includeEntries: true })
      });
    } catch (err) {
      return res.status(500).json({
        ok: false,
        error: `Şablon okunamadı: ${err?.message || "Bilinmeyen hata"}`
      });
    }
  }
);

app.post("/api/obus-user-create/templates", requireAuth, requireMenuAccess("obus-user-create"), async (req, res) => {
  try {
    const rawEntries = extractObusBulkUserTemplateEntriesSource(req.body?.entries);
    if (rawEntries.length > OBUS_BULK_USER_TEMPLATE_ENTRY_LIMIT) {
      return res.status(400).json({
        ok: false,
        error: `En fazla ${OBUS_BULK_USER_TEMPLATE_ENTRY_LIMIT} kullanıcı satırı kaydedebilirsiniz.`
      });
    }

    const saveResult = await saveObusBulkUserTemplateForUser({
      templateId: req.body?.templateId,
      name: req.body?.name,
      entries: rawEntries,
      userId: req.session.user?.id || null
    });

    return res.json({
      ok: true,
      action: saveResult.action,
      item: saveResult.item
    });
  } catch (err) {
    const normalizedCode = String(err?.code || "").trim().toLowerCase();
    const normalizedMessage = String(err?.message || "").trim();
    const statusCode =
      normalizedCode === "template_name_exists"
        ? 409
        : [
              "Şablon adı zorunludur.",
              "Geçersiz kullanıcı.",
              "Kaydetmek için en az bir kullanıcı satırı doldurulmalıdır."
            ].includes(normalizedMessage)
          ? 400
          : 500;
    return res.status(statusCode).json({
      ok: false,
      error: normalizedMessage || "Şablon kaydedilemedi."
    });
  }
});

app.post("/api/obus-user-create/run", requireAuth, requireMenuAccess("obus-user-create"), async (req, res) => {
  try {
    const entryValidation = validateObusUserCreateEntries(req.body?.entries);
    if (!entryValidation.ok) {
      return res.status(400).json({
        ok: false,
        error: entryValidation.error
      });
    }

    const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
    if (partnerError && (!Array.isArray(partnerItems) || partnerItems.length === 0)) {
      return res.status(400).json({
        ok: false,
        error: partnerError
      });
    }

    if (!Array.isArray(partnerItems) || partnerItems.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Tüm Firmalar SQL kaydı boş. Önce firma listesini güncelleyin."
      });
    }

    const loginCredentials = getObusUserCreateLoginCredentials();
    if (!loginCredentials.username || !loginCredentials.password) {
      return res.status(400).json({
        ok: false,
        error: buildObusServiceLoginConfigurationMessage(loginCredentials)
      });
    }

    const totalCount = entryValidation.entries.length * partnerItems.length;
    const ownerUserId = Number(req.session?.user?.id || 0);
    const job = createObusLiveJob({
      type: "obus-user-create",
      ownerUserId,
      totalCount
    });

    setImmediate(() => {
      runObusBulkUserCreateJob(job, {
        entries: entryValidation.entries,
        partnerItems
      }).catch((err) => {
        finishObusLiveJob(job, `Toplu kullanıcı oluşturma tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`);
      });
    });

    return res.json({
      ok: true,
      jobId: job.id,
      companyCount: partnerItems.length,
      userCount: entryValidation.entries.length,
      totalCount
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `Toplu kullanıcı oluşturma başlatılamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.delete(
  "/api/obus-user-create/templates/:id",
  requireAuth,
  requireMenuAccess("obus-user-create"),
  async (req, res) => {
    try {
      const deleted = await deleteObusBulkUserTemplateForUser(req.params.id, req.session.user?.id || null);
      if (!deleted) {
        return res.status(404).json({
          ok: false,
          error: "Şablon bulunamadı."
        });
      }

      return res.json({
        ok: true
      });
    } catch (err) {
      return res.status(500).json({
        ok: false,
        error: `Şablon silinemedi: ${err?.message || "Bilinmeyen hata"}`
      });
    }
  }
);

app.get("/general/obus-rule-define", requireAuth, requireMenuAccess("obus-rule-define"), async (req, res) => {
  const { partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
  const today = getTodayIsoDate();
  const modeRaw = String(req.query.mode || "").trim().toLowerCase();
  const mode = modeRaw === "update" ? "update" : "create";
  const startDate = normalizeIsoDateInput(req.query.startDate) || today;
  const endDate = normalizeIsoDateInput(req.query.endDate) || today;
  const isAbroadFilterRaw = String(req.query.isabroad || "").trim().toLowerCase();
  const isAbroad = ["true", "false"].includes(isAbroadFilterRaw) ? isAbroadFilterRaw : "all";
  const partnerRuleIdInput = String(req.query.partnerRuleId || "").trim();
  const parsedPartnerRuleId = Number.parseInt(partnerRuleIdInput, 10);
  const partnerRuleId =
    Number.isInteger(parsedPartnerRuleId) && parsedPartnerRuleId > 0 ? String(parsedPartnerRuleId) : "";
  const rateInput = String(req.query.rate || "1").trim().replace(",", ".");
  const parsedRate = Number.parseFloat(rateInput);
  const rate = Number.isFinite(parsedRate) ? String(parsedRate) : "1";
  const capacityBeginInput = String(req.query.capacityBegin || "1").trim();
  const capacityEndInput = String(req.query.capacityEnd || "3").trim();
  const parsedCapacityBegin = Number.parseInt(capacityBeginInput, 10);
  const parsedCapacityEnd = Number.parseInt(capacityEndInput, 10);
  const capacityBegin = Number.isInteger(parsedCapacityBegin) && parsedCapacityBegin >= 0 ? String(parsedCapacityBegin) : "1";
  const capacityEnd = Number.isInteger(parsedCapacityEnd) && parsedCapacityEnd >= 0 ? String(parsedCapacityEnd) : "3";
  const companyOptions = buildObusRuleDefineCompanyOptions(partnerItems);

  res.render("general-obus-rule-define", {
    user: req.session.user,
    active: "obus-rule-define",
    partnerError,
    companyOptions,
    selectedCompaniesJson: JSON.stringify([]),
    ruleCreateBaseUrl: OBUS_PARTNER_RULE_CREATE_API_URL,
    ruleUpdateBaseUrl: OBUS_PARTNER_RULE_UPDATE_API_URL,
    filters: {
      mode,
      isAbroad,
      partnerRuleId,
      startDate,
      endDate,
      rate,
      capacityBegin,
      capacityEnd
    }
  });
});

app.post("/api/obus-rule-define/create", requireAuth, requireMenuAccess("obus-rule-define"), async (req, res) => {
  try {
    const obusUserCreateLogin = getObusUserCreateLoginCredentials();
    const selectedCompanyValues = Array.from(
      new Set(parseSelectedCompanyValuesFromInput(req.body?.selectedCompanies))
    );
    const startDate = normalizeIsoDateInput(String(req.body?.startDate || "").trim());
    const endDate = normalizeIsoDateInput(String(req.body?.endDate || "").trim());
    const rateText = String(req.body?.rate || "").trim().replace(",", ".");
    const parsedRate = Number.parseFloat(rateText);
    const capacityBeginText = String(req.body?.capacityBegin || "").trim();
    const capacityEndText = String(req.body?.capacityEnd || "").trim();
    const parsedCapacityBegin = Number.parseInt(capacityBeginText, 10);
    const parsedCapacityEnd = Number.parseInt(capacityEndText, 10);

    if (selectedCompanyValues.length === 0) {
      return res.status(400).json({ ok: false, error: "En az bir firma seçmelisiniz." });
    }
    if (!startDate || !endDate) {
      return res.status(400).json({ ok: false, error: "StartDate ve EndDate zorunludur." });
    }
    if (String(startDate) > String(endDate)) {
      return res.status(400).json({ ok: false, error: "StartDate, EndDate'ten büyük olamaz." });
    }
    if (!Number.isFinite(parsedRate)) {
      return res.status(400).json({ ok: false, error: "Geçerli bir Rate girilmelidir." });
    }
    if (!Number.isInteger(parsedCapacityBegin) || parsedCapacityBegin < 0) {
      return res.status(400).json({ ok: false, error: "Geçerli bir CapacityBegin girilmelidir." });
    }
    if (!Number.isInteger(parsedCapacityEnd) || parsedCapacityEnd < 0) {
      return res.status(400).json({ ok: false, error: "Geçerli bir CapacityEnd girilmelidir." });
    }
    if (parsedCapacityBegin > parsedCapacityEnd) {
      return res.status(400).json({ ok: false, error: "CapacityBegin, CapacityEnd'ten büyük olamaz." });
    }

    if (!obusUserCreateLogin.username || !obusUserCreateLogin.password) {
      return res.status(400).json({
        ok: false,
        error: buildObusServiceLoginConfigurationMessage(obusUserCreateLogin)
      });
    }

    const { companies, partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
    if (partnerError && (!Array.isArray(partnerItems) || partnerItems.length === 0)) {
      return res.status(400).json({ ok: false, error: partnerError });
    }

    const optionByValue = new Map(
      (Array.isArray(companies) ? companies : [])
        .filter((item) => !item?.disabled && item?.meta)
        .map((item) => [String(item.value || "").trim(), item])
    );
    const selectedCompanies = selectedCompanyValues.map((value) => optionByValue.get(value)).filter(Boolean);

    if (selectedCompanies.length === 0) {
      return res.status(400).json({ ok: false, error: "Seçilen firmalar Tüm Firmalar listesinde bulunamadı." });
    }

    const createResult = await createObusPartnerRulesByCompanies({
      companies: selectedCompanies,
      startDate,
      endDate,
      rate: parsedRate,
      capacityBegin: parsedCapacityBegin,
      capacityEnd: parsedCapacityEnd
    });

    return res.json({
      ok: createResult.failureItems.length === 0,
      totalCount: createResult.results.length,
      successCount: createResult.successItems.length,
      failureCount: createResult.failureItems.length,
      filters: {
        startDate,
        endDate,
        rate: parsedRate,
        capacityBegin: parsedCapacityBegin,
        capacityEnd: parsedCapacityEnd
      },
      results: createResult.results.map((item) => ({
        company: String(item?.label || "").trim(),
        partnerId: item?.partnerId ?? null,
        requestUrl: String(item?.requestUrl || "").trim(),
        status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
        ok: item?.ok === true,
        message: String(item?.message || "").trim(),
        error: String(item?.error || "").trim(),
        errorDetail: String(item?.errorDetail || "").trim(),
        responseBody: item?.responseBody ?? null
      }))
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `CreatePartnerRule isteği tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.post("/api/obus-rule-define/update", requireAuth, requireMenuAccess("obus-rule-define"), async (req, res) => {
  try {
    const obusUserCreateLogin = getObusUserCreateLoginCredentials();
    const selectedCompanyValues = Array.from(
      new Set(parseSelectedCompanyValuesFromInput(req.body?.selectedCompanies))
    );
    const partnerRuleIdText = String(req.body?.partnerRuleId || "").trim();
    const parsedPartnerRuleId = Number.parseInt(partnerRuleIdText, 10);
    const startDate = normalizeIsoDateInput(String(req.body?.startDate || "").trim());
    const endDate = normalizeIsoDateInput(String(req.body?.endDate || "").trim());
    const rateText = String(req.body?.rate || "").trim().replace(",", ".");
    const parsedRate = Number.parseFloat(rateText);
    const capacityBeginText = String(req.body?.capacityBegin || "").trim();
    const capacityEndText = String(req.body?.capacityEnd || "").trim();
    const parsedCapacityBegin = capacityBeginText ? Number.parseInt(capacityBeginText, 10) : null;
    const parsedCapacityEnd = capacityEndText ? Number.parseInt(capacityEndText, 10) : null;

    if (selectedCompanyValues.length === 0) {
      return res.status(400).json({ ok: false, error: "En az bir firma seçmelisiniz." });
    }
    if (!Number.isInteger(parsedPartnerRuleId) || parsedPartnerRuleId <= 0) {
      return res.status(400).json({ ok: false, error: "Geçerli bir PartnerRuleId girilmelidir." });
    }
    if (!startDate || !endDate) {
      return res.status(400).json({ ok: false, error: "StartDate ve EndDate zorunludur." });
    }
    if (String(startDate) > String(endDate)) {
      return res.status(400).json({ ok: false, error: "StartDate, EndDate'ten büyük olamaz." });
    }
    if (!Number.isFinite(parsedRate)) {
      return res.status(400).json({ ok: false, error: "Geçerli bir Rate girilmelidir." });
    }
    if (capacityBeginText && (!Number.isInteger(parsedCapacityBegin) || parsedCapacityBegin < 0)) {
      return res.status(400).json({ ok: false, error: "CapacityBegin girilmişse geçerli bir sayı olmalıdır." });
    }
    if (capacityEndText && (!Number.isInteger(parsedCapacityEnd) || parsedCapacityEnd < 0)) {
      return res.status(400).json({ ok: false, error: "CapacityEnd girilmişse geçerli bir sayı olmalıdır." });
    }
    if (
      Number.isInteger(parsedCapacityBegin) &&
      Number.isInteger(parsedCapacityEnd) &&
      parsedCapacityBegin > parsedCapacityEnd
    ) {
      return res.status(400).json({ ok: false, error: "CapacityBegin, CapacityEnd'ten büyük olamaz." });
    }

    if (!obusUserCreateLogin.username || !obusUserCreateLogin.password) {
      return res.status(400).json({
        ok: false,
        error: buildObusServiceLoginConfigurationMessage(obusUserCreateLogin)
      });
    }

    const { companies, partnerItems, partnerError } = await loadAuthorizedLinesCompanies();
    if (partnerError && (!Array.isArray(partnerItems) || partnerItems.length === 0)) {
      return res.status(400).json({ ok: false, error: partnerError });
    }

    const optionByValue = new Map(
      (Array.isArray(companies) ? companies : [])
        .filter((item) => !item?.disabled && item?.meta)
        .map((item) => [String(item.value || "").trim(), item])
    );
    const selectedCompanies = selectedCompanyValues.map((value) => optionByValue.get(value)).filter(Boolean);

    if (selectedCompanies.length === 0) {
      return res.status(400).json({ ok: false, error: "Seçilen firmalar Tüm Firmalar listesinde bulunamadı." });
    }

    const updateResult = await updateObusPartnerRulesByCompanies({
      companies: selectedCompanies,
      partnerRuleId: parsedPartnerRuleId,
      startDate,
      endDate,
      rate: parsedRate,
      capacityBegin: Number.isInteger(parsedCapacityBegin) ? parsedCapacityBegin : null,
      capacityEnd: Number.isInteger(parsedCapacityEnd) ? parsedCapacityEnd : null
    });

    return res.json({
      ok: updateResult.failureItems.length === 0,
      totalCount: updateResult.results.length,
      successCount: updateResult.successItems.length,
      failureCount: updateResult.failureItems.length,
      filters: {
        partnerRuleId: parsedPartnerRuleId,
        startDate,
        endDate,
        rate: parsedRate,
        capacityBegin: Number.isInteger(parsedCapacityBegin) ? parsedCapacityBegin : null,
        capacityEnd: Number.isInteger(parsedCapacityEnd) ? parsedCapacityEnd : null
      },
      results: updateResult.results.map((item) => ({
        company: String(item?.label || "").trim(),
        partnerId: item?.partnerId ?? null,
        partnerRuleId: item?.partnerRuleId ?? null,
        requestUrl: String(item?.requestUrl || "").trim(),
        status: Number.isFinite(Number(item?.status)) ? Number(item.status) : null,
        ok: item?.ok === true,
        message: String(item?.message || "").trim(),
        error: String(item?.error || "").trim(),
        errorDetail: String(item?.errorDetail || "").trim(),
        responseBody: item?.responseBody ?? null
      }))
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: `UpdatePartnerRule isteği tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`
    });
  }
});

app.get("/general/journey-search", requireAuth, requireMenuAccess("journey-search"), async (req, res) => {
  const { companies, partnerError } = await loadJourneySearchCompanies();
  const filters = {
    company: typeof req.query.company === "string" ? req.query.company.trim() : "",
    origin: typeof req.query.origin === "string" ? req.query.origin.trim() : "",
    destination: typeof req.query.destination === "string" ? req.query.destination.trim() : ""
  };

  res.render("general-journey-search", {
    user: req.session.user,
    active: "journey-search",
    filters,
    companies,
    partnerError,
    requestBodyTemplate: JSON.stringify(buildJourneySearchStationsRequestBody({ usePlaceholders: true }), null, 2)
  });
});

app.post("/api/station-passenger-info/search", requireAuth, requireMenuAccess("station-passenger-info"), async (req, res) => {
  try {
    const plate = String(req.body?.plate || "").trim();
    if (!plate) {
      return res.status(400).json({ ok: false, error: "Plaka girilmesi zorunludur." });
    }

    const result = await searchStationPassengerJourneysByPlate(plate);
    if (!result.ok) {
      clearStationPassengerAuthContextFromSession(req);
      return res.status(502).json({
        ok: false,
        error: String(result.error || "").trim() || "Plaka araması tamamlanamadı.",
        details: String(result.detail || "").trim(),
        requestUrl: result.requestUrl || "",
        requestDate: result.requestDate || "",
        requestDateTime: result.requestDateTime || "",
        searchedPlate: result.searchedPlate || plate,
        sourceCompany: result.sourceCompany || "",
        sourceMode: result.sourceMode || ""
      });
    }

    saveStationPassengerAuthContextToSession(req, result.authContext);

    return res.json({
      ok: true,
      items: Array.isArray(result.items) ? result.items : [],
      totalCount: Array.isArray(result.items) ? result.items.length : 0,
      requestUrl: result.requestUrl || "",
      requestDate: result.requestDate || "",
      requestDateTime: result.requestDateTime || "",
      searchedPlate: result.searchedPlate || plate,
      sourceCompany: result.sourceCompany || "",
      sourceMode: result.sourceMode || "",
      details: String(result.detail || "").trim()
    });
  } catch (err) {
    console.error("Station passenger info search error:", err);
    return res.status(500).json({
      ok: false,
      error: `Plaka araması tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`,
      details: buildObusServiceTraceText(
        buildObusServiceTraceEntry({
          service: "StationPassengerInfoApi",
          url: "/api/station-passenger-info/search",
          requestBody: req.body || {},
          responseBody: "",
          error: err?.message || "Bilinmeyen hata"
        }),
        err?.message || "Bilinmeyen hata",
        {
          bodyMaxLen: 120,
          responseMaxLen: 180
        }
      )
    });
  }
});

app.post(
  "/api/station-passenger-info/journey-stations",
  requireAuth,
  requireMenuAccess("station-passenger-info"),
  async (req, res) => {
    try {
      const inventoryLogin = getInventoryBranchesLoginCredentials();
      const tripId = String(req.body?.tripId || req.body?.journeyId || req.body?.seferId || "").trim();
      if (!tripId) {
        return res.status(400).json({ ok: false, error: "journey-id zorunludur." });
      }
      if (!inventoryLogin.username || !inventoryLogin.password) {
        return res.status(400).json({
          ok: false,
          error: buildObusServiceLoginConfigurationMessage(inventoryLogin)
        });
      }

      const endpointUrl = normalizeTargetUrl(STATION_PASSENGER_INFO_API_URL);
      const clusterLabel = extractClusterLabel(endpointUrl || STATION_PASSENGER_INFO_API_URL) || "cluster3";
      let authContext = getStationPassengerAuthContextFromSession(req);
      let usedCachedAuth = Boolean(authContext);
      let authRefreshed = false;

      const ensureFreshAuthContext = async () => {
        const targetResolution = await resolveStationPassengerTargetCandidate({
          endpointUrl,
          clusterLabel
        });
        const targetCandidate = targetResolution.candidate;
        const loginResult = await resolveStationPassengerLoginResult({
          endpointUrl,
          companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
          partnerCode: String(targetCandidate.code || "").trim(),
          partnerId: String(targetCandidate.id || "").trim(),
          username: inventoryLogin.username,
          password: inventoryLogin.password,
          fallbackBranchId: String(targetCandidate.branchId || targetCandidate.id || "").trim(),
          allowEmptyPartnerCode: false,
          authorization: STATION_PASSENGER_INFO_API_AUTH,
          timeoutMs: STATION_PASSENGER_INFO_TIMEOUT_MS
        });

        if (!(loginResult?.ok && String(loginResult.token || "").trim())) {
          return {
            ok: false,
            error: String(loginResult?.error || "").trim() || "UserLogin başarısız.",
            detail:
              String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim() ||
              String(targetResolution.partnerError || "").trim()
          };
        }

        return {
          ok: true,
          authContext: buildStationPassengerAuthContext({
            endpointUrl,
            companyCode: String(targetCandidate.code || "").trim(),
            companyId: String(targetCandidate.id || "").trim(),
            companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
            cluster: clusterLabel,
            loginResult
          })
        };
      };

      if (!authContext) {
        const authResolution = await ensureFreshAuthContext();
        if (!authResolution.ok) {
          clearStationPassengerAuthContextFromSession(req);
          return res.status(502).json({
            ok: false,
            error: authResolution.error,
            details: authResolution.detail,
            tripId,
            requestUrl: STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL,
            usedCachedAuth: false,
            authRefreshed: false
          });
        }
        authContext = authResolution.authContext;
        saveStationPassengerAuthContextToSession(req, authContext);
      }

      let fetchResult = await fetchStationPassengerJourneyStations({
        endpointUrl: STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL,
        sessionId: authContext.sessionId,
        deviceId: authContext.deviceId,
        token: authContext.token,
        journeyId: tripId,
        authorization: STATION_PASSENGER_INFO_API_AUTH
      });

      if (!fetchResult.ok && usedCachedAuth) {
        const authResolution = await ensureFreshAuthContext();
        if (authResolution.ok) {
          authContext = authResolution.authContext;
          authRefreshed = true;
          saveStationPassengerAuthContextToSession(req, authContext);
          fetchResult = await fetchStationPassengerJourneyStations({
            endpointUrl: STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL,
            sessionId: authContext.sessionId,
            deviceId: authContext.deviceId,
            token: authContext.token,
            journeyId: tripId,
            authorization: STATION_PASSENGER_INFO_API_AUTH
          });
        }
      }

      if (!fetchResult.ok) {
        return res.status(502).json({
          ok: false,
          error: String(fetchResult.error || "").trim() || "GetJourneyStations başarısız.",
          details: String(fetchResult.detail || "").trim(),
          tripId,
          requestUrl: fetchResult.requestUrl || STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL,
          usedCachedAuth,
          authRefreshed
        });
      }

      let resolvedItems = Array.isArray(fetchResult.items) ? fetchResult.items : [];
      let nextStation = fetchResult.nextStation || null;
      const nextStationId = String(nextStation?.stationId || nextStation?.["station-id"] || "").trim();
      if (resolvedItems.length > 0 || (nextStation && nextStationId)) {
        const stationsResult = await fetchStationPassengerWebStations({
          endpointUrl: STATION_PASSENGER_INFO_WEB_STATIONS_API_URL,
          sessionId: authContext.sessionId,
          deviceId: authContext.deviceId,
          authorization: STATION_PASSENGER_INFO_WEB_STATIONS_API_AUTH,
          partnerCode: authContext.companyCode || STATION_PASSENGER_INFO_TARGET_COMPANY_CODE
        });
        if (stationsResult.ok) {
          resolvedItems = applyStationPassengerJourneyStationNames(resolvedItems, stationsResult.items);
          const resolvedNextStation = findStationPassengerJourneyStationByIdentity(resolvedItems, nextStation);
          if (resolvedNextStation) {
            nextStation = resolvedNextStation;
          } else if (nextStation && nextStationId) {
            const matchedStation = findStationPassengerStationCatalogItem(stationsResult.items, nextStationId);
            const resolvedStationName = String(matchedStation?.name || matchedStation?.label || "").trim();
            if (resolvedStationName) {
              nextStation = {
                ...nextStation,
                stationName: resolvedStationName,
                "station-name": resolvedStationName
              };
            }
          }
        }
      }

      return res.json({
        ok: true,
        tripId,
        requestUrl: fetchResult.requestUrl || STATION_PASSENGER_INFO_JOURNEY_STATIONS_API_URL,
        items: resolvedItems,
        totalCount: resolvedItems.length,
        requestDate: String(fetchResult.requestDate || "").trim(),
        nextStation,
        usedCachedAuth,
        authRefreshed
      });
    } catch (err) {
      console.error("Station passenger journey stations error:", err);
      return res.status(500).json({
        ok: false,
        error: `GetJourneyStations tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`,
        details: buildObusServiceTraceText(
          buildObusServiceTraceEntry({
            service: "StationPassengerJourneyStationsApi",
            url: "/api/station-passenger-info/journey-stations",
            requestBody: req.body || {},
            responseBody: "",
            error: err?.message || "Bilinmeyen hata"
          }),
          err?.message || "Bilinmeyen hata",
          {
            bodyMaxLen: 120,
            responseMaxLen: 180
          }
        )
      });
    }
  }
);

app.post(
  "/api/station-passenger-info/passenger-state-history",
  requireAuth,
  requireMenuAccess("station-passenger-info"),
  async (req, res) => {
    try {
      const inventoryLogin = getInventoryBranchesLoginCredentials();
      const tripId = String(req.body?.tripId || req.body?.journeyId || req.body?.seferId || "").trim();
      const stationId = String(req.body?.stationId || req.body?.["station-id"] || "").trim();
      if (!tripId) {
        return res.status(400).json({ ok: false, error: "journey-id zorunludur." });
      }
      if (!stationId) {
        return res.status(400).json({ ok: false, error: "station-id zorunludur." });
      }
      if (!inventoryLogin.username || !inventoryLogin.password) {
        return res.status(400).json({
          ok: false,
          error: buildObusServiceLoginConfigurationMessage(inventoryLogin)
        });
      }

      const endpointUrl = normalizeTargetUrl(STATION_PASSENGER_INFO_API_URL);
      const clusterLabel = extractClusterLabel(endpointUrl || STATION_PASSENGER_INFO_API_URL) || "cluster3";
      let authContext = getStationPassengerAuthContextFromSession(req);
      let usedCachedAuth = Boolean(authContext);
      let authRefreshed = false;

      const ensureFreshAuthContext = async () => {
        const targetResolution = await resolveStationPassengerTargetCandidate({
          endpointUrl,
          clusterLabel
        });
        const targetCandidate = targetResolution.candidate;
        const loginResult = await resolveStationPassengerLoginResult({
          endpointUrl,
          companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
          partnerCode: String(targetCandidate.code || "").trim(),
          partnerId: String(targetCandidate.id || "").trim(),
          username: inventoryLogin.username,
          password: inventoryLogin.password,
          fallbackBranchId: String(targetCandidate.branchId || targetCandidate.id || "").trim(),
          allowEmptyPartnerCode: false,
          authorization: STATION_PASSENGER_INFO_API_AUTH,
          timeoutMs: STATION_PASSENGER_INFO_TIMEOUT_MS
        });

        if (!(loginResult?.ok && String(loginResult.token || "").trim())) {
          return {
            ok: false,
            error: String(loginResult?.error || "").trim() || "UserLogin başarısız.",
            detail:
              String(loginResult?.errorDetail || loginResult?.tokenMissingDetail || "").trim() ||
              String(targetResolution.partnerError || "").trim()
          };
        }

        return {
          ok: true,
          authContext: buildStationPassengerAuthContext({
            endpointUrl,
            companyCode: String(targetCandidate.code || "").trim(),
            companyId: String(targetCandidate.id || "").trim(),
            companyUrl: String(targetCandidate.url || endpointUrl).trim() || endpointUrl,
            cluster: clusterLabel,
            loginResult
          })
        };
      };

      if (!authContext) {
        const authResolution = await ensureFreshAuthContext();
        if (!authResolution.ok) {
          clearStationPassengerAuthContextFromSession(req);
          return res.status(502).json({
            ok: false,
            error: authResolution.error,
            details: authResolution.detail,
            tripId,
            stationId,
            requestUrl: STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL,
            usedCachedAuth: false,
            authRefreshed: false
          });
        }
        authContext = authResolution.authContext;
        saveStationPassengerAuthContextToSession(req, authContext);
      }

      let fetchResult = await fetchStationPassengerPassengerStateHistory({
        endpointUrl: STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL,
        sessionId: authContext.sessionId,
        deviceId: authContext.deviceId,
        token: authContext.token,
        journeyId: tripId,
        stationId,
        authorization: STATION_PASSENGER_INFO_API_AUTH
      });

      if (!fetchResult.ok && usedCachedAuth) {
        const authResolution = await ensureFreshAuthContext();
        if (authResolution.ok) {
          authContext = authResolution.authContext;
          authRefreshed = true;
          saveStationPassengerAuthContextToSession(req, authContext);
          fetchResult = await fetchStationPassengerPassengerStateHistory({
            endpointUrl: STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL,
            sessionId: authContext.sessionId,
            deviceId: authContext.deviceId,
            token: authContext.token,
            journeyId: tripId,
            stationId,
            authorization: STATION_PASSENGER_INFO_API_AUTH
          });
        }
      }

      if (!fetchResult.ok) {
        return res.status(502).json({
          ok: false,
          error: String(fetchResult.error || "").trim() || "GetPassengerStateHistory başarısız.",
          details: String(fetchResult.detail || "").trim(),
          tripId,
          stationId,
          requestUrl: fetchResult.requestUrl || STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL,
          usedCachedAuth,
          authRefreshed
        });
      }

      return res.json({
        ok: true,
        tripId,
        stationId,
        requestUrl: fetchResult.requestUrl || STATION_PASSENGER_INFO_PASSENGER_STATE_HISTORY_API_URL,
        requestDate: String(fetchResult.requestDate || "").trim(),
        items: Array.isArray(fetchResult.items) ? fetchResult.items : [],
        boardingPassengers: Array.isArray(fetchResult.boardingPassengers) ? fetchResult.boardingPassengers : [],
        dropoffPassengers: Array.isArray(fetchResult.dropoffPassengers) ? fetchResult.dropoffPassengers : [],
        usedCachedAuth,
        authRefreshed
      });
    } catch (err) {
      console.error("Station passenger passenger state history error:", err);
      return res.status(500).json({
        ok: false,
        error: `GetPassengerStateHistory tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`,
        details: buildObusServiceTraceText(
          buildObusServiceTraceEntry({
            service: "StationPassengerStateHistoryApi",
            url: "/api/station-passenger-info/passenger-state-history",
            requestBody: req.body || {},
            responseBody: "",
            error: err?.message || "Bilinmeyen hata"
          }),
          err?.message || "Bilinmeyen hata",
          {
            bodyMaxLen: 120,
            responseMaxLen: 180
          }
        )
      });
    }
  }
);

app.get("/obus/station-passenger-info", requireAuth, requireMenuAccess("station-passenger-info"), (req, res) => {
  res.render("general-station-passenger-info", {
    user: req.session.user,
    active: "station-passenger-info"
  });
});

app.get("/general/station-passenger-info", requireAuth, (req, res) => {
  return res.redirect("/obus/station-passenger-info");
});

app.get("/change-password", requireAuth, requireMenuAccess("password"), (req, res) => {
  res.render("change-password", {
    user: req.session.user,
    error: null,
    ok: req.query.ok === "1",
    active: "password"
  });
});

app.get("/menti", requireAuth, requireMenuAccess("menti"), (req, res) => {
  res.render("menti", {
    user: req.session.user,
    active: "menti",
    mentiUrl: "https://www.menti.com/",
    chatgptUrl: "https://chatgpt.com/"
  });
});

app.get("/obus/journey-passengers", requireAuth, (req, res) => {
  return res.redirect("/obus/station-passenger-info");
});

module.exports = {
  runObusJobsScheduledScan
};

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
  const { map: obusMerkezSubeIdByCompany, error: obusCacheError } = await fetchAllCompaniesObusMerkezSubeIdMap();
  const companies = [{ value: "all", label: "Tümü" }].concat(
    partnerItems.map((item) => {
      const idText = item.id || "N/A";
      const clusterText = item.cluster || "cluster";
      const value = buildCompanyOptionValue(item);
      const obusMerkezSubeId = String(obusMerkezSubeIdByCompany.get(value) || "").trim();
      const label = `${item.code} - ${idText} - ${clusterText} - ObusMerkezSubeID: ${obusMerkezSubeId || "-"}`;
      return {
        value,
        label,
        meta: item
      };
    })
  );
  if (obusCacheError) {
    console.error("Sales report ObusMerkezSubeID cache read error:", obusCacheError);
  }
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
  const shouldSync = String(req.query.sync || "").trim() === "1";
  const syncJobId = String(req.query.syncJobId || "").trim();
  const shouldViewCache = String(req.query.cache || "").trim() === "1";
  const saveSucceeded = String(req.query.saved || "").trim() === "1";
  const obusUpdateSucceeded = String(req.query.obusUpdated || "").trim() === "1";
  const obusJobId = String(req.query.obusJobId || "").trim();
  const saveErrorCode = String(req.query.saveErr || "").trim();
  const savedCountRaw = Number.parseInt(String(req.query.savedCount || "0"), 10);
  const savedCount = Number.isFinite(savedCountRaw) ? Math.max(0, savedCountRaw) : 0;
  const deletedCountRaw = Number.parseInt(String(req.query.deletedCount || "0"), 10);
  const deletedCount = Number.isFinite(deletedCountRaw) ? Math.max(0, deletedCountRaw) : 0;
  const obusScannedRaw = Number.parseInt(String(req.query.obusScanned || "0"), 10);
  const obusScanned = Number.isFinite(obusScannedRaw) ? Math.max(0, obusScannedRaw) : 0;
  const obusFilledRaw = Number.parseInt(String(req.query.obusFilled || "0"), 10);
  const obusFilled = Number.isFinite(obusFilledRaw) ? Math.max(0, obusFilledRaw) : 0;
  const obusRemainingRaw = Number.parseInt(String(req.query.obusRemaining || "0"), 10);
  const obusRemaining = Number.isFinite(obusRemainingRaw) ? Math.max(0, obusRemainingRaw) : 0;
  const obusUpdatePartial = String(req.query.obusPartial || "").trim() === "1";
  const currentUserId = Number(req.session?.user?.id);
  const syncJob = syncJobId ? readObusLiveJob(syncJobId, currentUserId) : null;
  const obusJob = obusJobId ? readObusLiveJob(obusJobId, currentUserId) : null;
  const obusJobSummary = obusJob ? buildAllCompaniesObusUpdateJobSummary(obusJob) : null;

  if (shouldSync && !syncJobId) {
    const job = createObusLiveJob({
      type: "all-companies-service-sync",
      ownerUserId: currentUserId,
      totalCount: 1
    });
    setImmediate(() => {
      runAllCompaniesServiceSyncJob(job, currentUserId).catch((err) => {
        finishObusLiveJob(job, `Servisten güncelleme tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`);
      });
    });
    return res.redirect(`/reports/all-companies?cache=1&syncJobId=${encodeURIComponent(job.id)}`);
  }

  let result = buildEmptyAllCompaniesReport(0);
  const errorParts = [];
  if (shouldSync) {
    const syncStartedAt = Date.now();
    console.log(
      `[AllCompanies] Service refresh started user=${Number.isInteger(currentUserId) ? currentUserId : "unknown"}`
    );

    const fetchedResult = await fetchAllPartnerRows({ includeObusMerkezSubeId: false });
    const { report: liveResult, obusCacheError } = await attachKnownObusMerkezSubeIdsToAllCompaniesReport(
      fetchedResult
    );
    result = {
      columns: liveResult.columns || [],
      rows: liveResult.rows || [],
      error: liveResult.error || null,
      clusterCount: liveResult.clusterCount || 0
    };

    const syncDurationMs = Date.now() - syncStartedAt;
    const metrics = liveResult.metrics || {};
    console.log(
      `[AllCompanies] Service refresh completed in ${syncDurationMs}ms | clusters=${
        liveResult.clusterCount || 0
      } | rawRows=${metrics.rawRowCount || 0} | rows=${(liveResult.rows || []).length} | clusterFetch=${
        metrics.clusterFetchMs || 0
      }ms | normalize=${metrics.normalizeMs || 0}ms | enrich=${metrics.enrichMs || 0}ms${
        liveResult.error ? " | status=partial" : " | status=ok"
      }`
    );
    if (obusCacheError) {
      console.error("All companies page ObusMerkezSubeID cache read error:", obusCacheError);
    }

    if (liveResult.error) {
      errorParts.push(liveResult.error);
    }
    setAllCompaniesServicePreviewForUser(currentUserId, liveResult);
  } else {
    result = await fetchAllCompaniesRowsFromCache();
    if (result.error) {
      errorParts.push(result.error);
    }
  }

  const previewSnapshot = getAllCompaniesServicePreviewForUser(currentUserId);
  const hasServicePreview = Array.isArray(previewSnapshot?.rows) && previewSnapshot.rows.length > 0;
  if (syncJob && syncJob.done && !syncJob.error && hasServicePreview) {
    result = {
      columns: Array.isArray(previewSnapshot?.columns) ? previewSnapshot.columns : [],
      rows: Array.isArray(previewSnapshot?.rows) ? previewSnapshot.rows : [],
      error: null,
      clusterCount: Number(previewSnapshot?.clusterCount || 0)
    };
  }
  if (obusJob?.done && !obusJob?.error && Array.isArray(obusJobSummary?.missingBranchDetails)) {
    result = {
      ...result,
      rows: attachAllCompaniesObusUpdateMissingBranchDetails(
        Array.isArray(result?.rows) ? result.rows : [],
        obusJobSummary.missingBranchDetails
      )
    };
  }

  let syncMessage = shouldSync
    ? `Servisten ${result.rows?.length || 0} kayıt getirildi. Boş ObusMerkezSubeID kayıtları için 'ObusMerkezSubeID Güncelle' butonunu kullanın. SQL'e kaydetmek için 'SQL'e Kaydet' butonunu kullanın.`
    : hasServicePreview
      ? `Servisten alınan son veri hazır (${previewSnapshot.rows.length} kayıt).`
      : "";
  let syncMessageKind = "success";

  if (syncJobId) {
    if (!syncJob) {
      errorParts.push("Servisten güncelleme işi bulunamadı veya süresi doldu.");
    } else if (syncJob.done) {
      if (syncJob.error) {
        errorParts.push(syncJob.error);
      } else {
        const syncRowCount = Number(syncJob?.summary?.rowCount || previewSnapshot?.rows?.length || 0);
        syncMessage = `Servisten ${syncRowCount} kayıt getirildi. Boş ObusMerkezSubeID kayıtları için 'ObusMerkezSubeID Güncelle' butonunu kullanın. SQL'e kaydetmek için 'SQL'e Kaydet' butonunu kullanın.`;
        const syncError = String(syncJob?.summary?.error || "").trim();
        if (syncError) {
          syncMessageKind = "warning";
          errorParts.push(syncError);
        }
      }
    } else {
      syncMessage = "Servisten güncelleme sürüyor. Sayfa otomatik yenilenecek...";
      syncMessageKind = "progress";
    }
  }
  const cacheMessage = shouldViewCache
    ? `SQL'den gösteriliyor. Toplam ${result.rows?.length || 0} kayıt.`
    : "";
  const saveMessage = saveSucceeded
    ? `SQL kayıt tamamlandı. ${savedCount} kayıt işlendi, serviste olmayan ${deletedCount} kayıt silindi.`
    : "";
  let obusMessage = obusUpdateSucceeded
    ? `ObusMerkezSubeID güncelleme tamamlandı. Kontrol: ${obusScanned} | Doluya dönen: ${obusFilled} | Hâlâ boş: ${obusRemaining}`
    : "";
  let obusMessageKind = "success";

  if (obusJobId) {
    if (!obusJob) {
      errorParts.push("ObusMerkezSubeID güncelleme işi bulunamadı veya süresi doldu.");
    } else if (obusJob.done) {
      if (obusJob.error) {
        errorParts.push(obusJob.error);
      } else {
        obusMessage = `ObusMerkezSubeID güncelleme tamamlandı. Kontrol: ${obusJobSummary?.scanned || 0} | Doluya dönen: ${
          obusJobSummary?.filled || 0
        } | Hâlâ boş: ${obusJobSummary?.remaining || 0}`;
        if (obusJobSummary?.partial) {
          obusMessageKind = "warning";
          if (obusJobSummary.notice) {
            errorParts.push(obusJobSummary.notice);
          } else {
            errorParts.push("ObusMerkezSubeID güncellemesi kısmi tamamlandı.");
          }
        }
      }
    } else {
      obusMessage = "ObusMerkezSubeID güncellemesi sürüyor. Sayfa otomatik yenilenecek...";
      obusMessageKind = "progress";
    }
  }

  if (saveErrorCode === "no_service_data") {
    errorParts.push("Kaydedilecek kayıt bulunamadı.");
  } else if (saveErrorCode === "save_failed") {
    errorParts.push("SQL kayıt işlemi başarısız oldu.");
  } else if (saveErrorCode === "obus_update_failed") {
    errorParts.push("ObusMerkezSubeID güncellemesi başarısız oldu.");
  }

  if (obusUpdatePartial) {
    errorParts.push("ObusMerkezSubeID güncellemesi kısmi tamamlandı.");
  }

  const emptyCacheMessage =
    (result.rows || []).length === 0
      ? shouldSync
        ? "Servisten gösterilecek kayıt bulunamadı."
        : "Önbellek boş. 'Servisten Güncelle' butonuna basarak firmaları yükleyin."
      : "";

  res.render("reports-all-companies", {
    user: req.session.user,
    active: "all-companies",
    report: {
      columns: result.columns || [],
      rows: result.rows || [],
      error: errorParts.length > 0 ? errorParts.join(" | ") : null,
      clusterCount: result.clusterCount || 0,
      requested: true,
      sync: {
        requested: shouldSync || Boolean(syncJobId),
        message: syncMessage,
        hasPreview: hasServicePreview,
        kind: syncMessageKind,
        job: syncJob
          ? {
              id: syncJobId,
              done: Boolean(syncJob.done),
              createdAt: Number(syncJob.createdAt || 0),
              refreshUrl: `/reports/all-companies?syncJobId=${encodeURIComponent(syncJobId)}`,
              runningMessage: "Servisten güncelleme sürüyor. Sayfa otomatik yenilenecek...",
              showCounts: false
            }
          : null
      },
      cache: {
        requested: shouldViewCache,
        message: cacheMessage
      },
      save: {
        requested: saveSucceeded,
        message: saveMessage
      },
      obus: {
        requested: obusUpdateSucceeded || Boolean(obusJobId),
        message: obusMessage,
        kind: obusMessageKind,
        job: obusJob
          ? {
              id: obusJobId,
              done: Boolean(obusJob.done),
              createdAt: Number(obusJob.createdAt || 0),
              totalCount: Number(obusJob.totalCount || 0),
              processedCount: Number(obusJob.processedCount || 0),
              successCount: Number(obusJob.successCount || 0),
              failureCount: Number(obusJob.failureCount || 0),
              refreshUrl: `/reports/all-companies?cache=1&obusJobId=${encodeURIComponent(obusJobId)}`
            }
          : null
      },
      exclusions: buildAllCompaniesExclusionSummary(),
      notice: emptyCacheMessage
    }
  });
});

app.post("/reports/all-companies/save-sql", requireAuth, requireMenuAccess("all-companies"), async (req, res) => {
  const currentUserId = Number(req.session?.user?.id);
  const saveSource = String(req.body?.saveSource || "").trim().toLowerCase();
  const previewSnapshot = getAllCompaniesServicePreviewForUser(currentUserId);
  const previewRows = normalizeAllCompaniesCacheRows(Array.isArray(previewSnapshot?.rows) ? previewSnapshot.rows : []);
  let rowsToSave = [];

  if (saveSource === "service") {
    rowsToSave = previewRows;
  } else {
    const cacheResult = await fetchAllCompaniesRowsFromCache();
    if (cacheResult.error) {
      console.error("All companies SQL save cache read error:", cacheResult.error);
      return res.redirect("/reports/all-companies?saveErr=save_failed");
    }
    rowsToSave = normalizeAllCompaniesCacheRows(cacheResult.rows || []);
  }

  if (rowsToSave.length === 0 && previewRows.length > 0) {
    rowsToSave = previewRows;
  }

  if (rowsToSave.length === 0) {
    return res.redirect("/reports/all-companies?saveErr=no_service_data");
  }

  const saveResult = await upsertAllCompaniesCacheRows(rowsToSave, { pruneMissing: true });
  if (saveResult.error) {
    console.error("All companies SQL save error:", saveResult.error);
    return res.redirect("/reports/all-companies?saveErr=save_failed");
  }

  return res.redirect(
    `/reports/all-companies?cache=1&saved=1&savedCount=${saveResult.savedCount}&deletedCount=${
      saveResult.deletedCount || 0
    }`
  );
});

app.post(
  "/reports/all-companies/update-obus-merkez-sube-id",
  requireAuth,
  requireMenuAccess("all-companies"),
  async (req, res) => {
    try {
      const currentUserId = Number(req.session?.user?.id || 0);
      const cacheResult = await fetchAllCompaniesRowsFromCache();
      if (cacheResult.error) {
        return res.redirect("/reports/all-companies?saveErr=obus_update_failed");
      }

      const cacheRows = normalizeAllCompaniesCacheRows(cacheResult.rows || []);
      const clusterSetByPartnerId = new Map();
      cacheRows.forEach((row) => {
        const partnerId = String(row?.id || "").trim();
        const clusterLabel = extractClusterLabel(row?.source);
        if (!partnerId || !clusterLabel) return;
        if (!clusterSetByPartnerId.has(partnerId)) {
          clusterSetByPartnerId.set(partnerId, new Set());
        }
        clusterSetByPartnerId.get(partnerId).add(clusterLabel);
      });
      const forceRefreshPartnerClusterKeys = new Set();
      clusterSetByPartnerId.forEach((clusterSet, partnerId) => {
        if (!(clusterSet instanceof Set) || clusterSet.size <= 1) return;
        clusterSet.forEach((clusterLabel) => {
          const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, clusterLabel);
          if (partnerClusterKey) forceRefreshPartnerClusterKeys.add(partnerClusterKey);
        });
      });
      const knownBranchIdByPartnerClusterKey = new Map();
      cacheRows.forEach((row) => {
        const partnerId = String(row?.id || "").trim();
        const branchId = String(row?.ObusMerkezSubeID || "").trim();
        const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.source);
        if (!partnerClusterKey || !branchId || knownBranchIdByPartnerClusterKey.has(partnerClusterKey)) return;
        knownBranchIdByPartnerClusterKey.set(partnerClusterKey, branchId);
      });
      const targetRows = cacheRows
        .filter((row) => {
          const partnerClusterKey = buildObusMerkezPartnerClusterKey(String(row?.id || "").trim(), row?.source);
          if (partnerClusterKey && forceRefreshPartnerClusterKeys.has(partnerClusterKey)) return true;
          return !String(row?.ObusMerkezSubeID || "").trim();
        })
        .map((row) => {
          const partnerId = String(row?.id || "").trim();
          const partnerClusterKey = buildObusMerkezPartnerClusterKey(partnerId, row?.source);
          if (partnerClusterKey && forceRefreshPartnerClusterKeys.has(partnerClusterKey)) {
            return {
              ...row,
              ObusMerkezSubeID: "",
              ObusMerkezSubeIDDebug: ""
            };
          }
          const cachedBranchId = String(knownBranchIdByPartnerClusterKey.get(partnerClusterKey) || "").trim();
          if (!partnerClusterKey || !cachedBranchId) return row;
          return {
            ...row,
            ObusMerkezSubeID: cachedBranchId
          };
        });
      if (targetRows.length === 0) {
        return res.redirect("/reports/all-companies?cache=1&obusUpdated=1&obusScanned=0&obusFilled=0&obusRemaining=0");
      }
      const job = createObusLiveJob({
        type: "all-companies-obus-update",
        ownerUserId: currentUserId,
        totalCount: targetRows.length
      });

      setImmediate(() => {
        runAllCompaniesObusMerkezUpdateJob(job, targetRows).catch((err) => {
          finishObusLiveJob(
            job,
            `ObusMerkezSubeID güncellemesi tamamlanamadı: ${err?.message || "Bilinmeyen hata"}`
          );
        });
      });

      return res.redirect(`/reports/all-companies?cache=1&obusJobId=${encodeURIComponent(job.id)}`);
    } catch (err) {
      console.error("All companies ObusMerkezSubeID update error:", err);
      return res.redirect("/reports/all-companies?saveErr=obus_update_failed");
    }
  }
);

app.get("/reports/slack-analysis", requireAuth, requireMenuAccess("slack-analysis"), async (req, res) => {
  const shouldFetchReport = req.query.run === "1";
  const shouldQuerySql = req.query.dbRun === "1";
  const today = getTodayIsoDate();
  const startDate = normalizeIsoDateInput(req.query.startDate) || today;
  const endDate = normalizeIsoDateInput(req.query.endDate) || today;
  const dbStartDate = normalizeIsoDateInput(req.query.dbStartDate) || startDate;
  const dbEndDate = normalizeIsoDateInput(req.query.dbEndDate) || endDate;

  let report = buildSlackReplyReportModel({ requested: false });
  let sqlQuery = buildSlackSqlQueryModel({
    requested: false,
    startDate: dbStartDate,
    endDate: dbEndDate
  });

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
      sqlQuery = buildSlackSqlQueryModel({
        requested: true,
        startDate: dbStartDate,
        endDate: dbEndDate,
        rows: [],
        error: dbValidationError
      });
    } else {
      const sqlResult = await fetchSlackSavedReports({
        startDate: dbStartDate,
        endDate: dbEndDate,
        limit: 25
      });
      sqlQuery = buildSlackSqlQueryModel({
        requested: true,
        startDate: sqlResult.filters?.startDate || dbStartDate,
        endDate: sqlResult.filters?.endDate || dbEndDate,
        rows: sqlResult.rows || [],
        error: sqlResult.error || null
      });
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

app.get("/reports/jira-analysis", requireAuth, requireMenuAccess("jira-analysis"), async (req, res) => {
  const shouldFetchReport = req.query.run === "1";
  const today = getTodayIsoDate();
  const startDate = normalizeIsoDateInput(req.query.startDate) || getIsoDateDaysAgo(30);
  const endDate = normalizeIsoDateInput(req.query.endDate) || today;
  const rawProjectKey = typeof req.query.projectKey === "string" ? req.query.projectKey : "";
  const normalizedProjectKey = normalizeJiraProjectKey(rawProjectKey);
  const jql = typeof req.query.jql === "string" ? req.query.jql.trim() : "";
  const maxResults = toBoundedInt(req.query.maxResults, JIRA_MAX_RESULTS, 1, 200);
  const jiraBaseUrl = normalizeJiraBaseUrl(JIRA_BASE_URL);

  const filters = {
    startDate,
    endDate,
    projectKey: rawProjectKey ? String(rawProjectKey).trim().toUpperCase() : "",
    jql,
    maxResults
  };

  let report = {
    requested: shouldFetchReport,
    jql: "",
    issues: [],
    total: 0,
    startAt: 0,
    maxResults,
    source: "",
    warning: null,
    error: null
  };

  if (shouldFetchReport) {
    const dateValidationError = validateSlackDateRange(startDate, endDate);
    const missingConfig = [];
    if (!jiraBaseUrl) missingConfig.push("JIRA_BASE_URL");
    if (!String(JIRA_EMAIL || "").trim()) missingConfig.push("JIRA_EMAIL");
    if (!String(JIRA_API_TOKEN || "").trim()) missingConfig.push("JIRA_API_TOKEN");

    if (rawProjectKey && !normalizedProjectKey) {
      report.error = "Proje anahtarı geçersiz. Örnek: PROJ veya WEB_APP.";
    } else if (dateValidationError) {
      report.error = dateValidationError;
    } else if (missingConfig.length > 0) {
      report.error = `Jira yapılandırması eksik: ${missingConfig.join(", ")}.`;
    } else {
      const effectiveJql = buildJiraJql({
        projectKey: normalizedProjectKey,
        startDate,
        endDate,
        customJql: jql
      });
      const jiraResult = await fetchJiraIssues({
        baseUrl: jiraBaseUrl,
        email: JIRA_EMAIL,
        apiToken: JIRA_API_TOKEN,
        jql: effectiveJql,
        maxResults,
        startAt: 0
      });

      report = {
        requested: true,
        jql: effectiveJql,
        issues: jiraResult.issues || [],
        total: Number.isFinite(Number(jiraResult.total)) ? Number(jiraResult.total) : 0,
        startAt: Number.isFinite(Number(jiraResult.startAt)) ? Number(jiraResult.startAt) : 0,
        maxResults: Number.isFinite(Number(jiraResult.maxResults)) ? Number(jiraResult.maxResults) : maxResults,
        source: String(jiraResult.source || "Jira API"),
        warning: jql
          ? "Özel JQL kullanıldığı için proje ve tarih filtreleri JQL içeriğine göre değerlendirilir."
          : null,
        error: jiraResult.error || null
      };
    }
  }

  res.render("reports-jira-analysis", {
    user: req.session.user,
    active: "jira-analysis",
    filters,
    report,
    jiraConfig: {
      baseUrl: jiraBaseUrl,
      hasConfig: Boolean(jiraBaseUrl && JIRA_EMAIL && JIRA_API_TOKEN)
    }
  });
});

app.get("/reports/jira-board", requireAuth, requireMenuAccess("jira-board"), async (req, res) => {
  const jiraBaseUrl = normalizeJiraBaseUrl(JIRA_BASE_URL);
  const jiraConfigOk = Boolean(jiraBaseUrl && JIRA_EMAIL && JIRA_API_TOKEN);
  let boardReport = {
    error: null
  };

  const boardColumns = [
    {
      key: "backlog",
      title: "Backlog",
      subtitle: "Henüz ele alınmamış işler",
      tone: "backlog",
      cards: [],
      emptyMessage: "Bu kriterlerde task bulunamadı."
    },
    {
      key: "hotfix",
      title: "Hotfix",
      subtitle: "Öncelikli acil düzeltmeler",
      tone: "hotfix",
      cards: [],
      emptyMessage: "Bu kolona gelecek kart verisini paylaştığınızda burada göstereceğim."
    },
    {
      key: "test-pending",
      title: "Test Edilecekler",
      subtitle: "Kontrole çıkacak işler",
      tone: "review",
      cards: [],
      emptyMessage: "Bu kolona gelecek kart verisini paylaştığınızda burada göstereceğim."
    },
    {
      key: "done",
      title: "Test Edildi",
      subtitle: "Testten geçen işler",
      tone: "done",
      cards: [],
      emptyMessage: "Bu kolona gelecek kart verisini paylaştığınızda burada göstereceğim."
    }
  ];

  if (!jiraConfigOk) {
    boardReport.error = "Jira bağlantısı için JIRA_BASE_URL, JIRA_EMAIL ve JIRA_API_TOKEN tanımlı olmalıdır.";
  } else {
    const columnResults = await Promise.all([
      fetchJiraBoardCards({
        baseUrl: jiraBaseUrl,
        email: JIRA_EMAIL,
        apiToken: JIRA_API_TOKEN,
        projectKey: "OBUSDEV",
        statuses: [
          "Backlog",
          "Analysis Done",
          "Analysis Revision",
          "Developmnet",
          "Development",
          "Planlama",
          "Selected for Development",
          "selected for development",
          "Development Done",
          "development done",
          "Devam Ediyor"
        ],
        issueType: "Task",
        maxResults: JIRA_MAX_RESULTS,
        sortOptions: {
          epicFirst: true
        }
      }),
      fetchJiraBoardCards({
        baseUrl: jiraBaseUrl,
        email: JIRA_EMAIL,
        apiToken: JIRA_API_TOKEN,
        projectKey: "OBUSDEV",
        statuses: ["HOTFİX", "HOTFIX", "Hotfix"],
        issueType: "Task",
        maxResults: JIRA_MAX_RESULTS,
        sortOptions: {
          epicFirst: true
        }
      }),
      fetchJiraBoardCards({
        baseUrl: jiraBaseUrl,
        email: JIRA_EMAIL,
        apiToken: JIRA_API_TOKEN,
        projectKey: "OBUSDEV",
        statuses: ["TEST EDİLECEK"],
        issueType: "Task",
        maxResults: JIRA_MAX_RESULTS,
        sortOptions: {
          epicFirst: true
        }
      }),
      fetchJiraBoardCards({
        baseUrl: jiraBaseUrl,
        email: JIRA_EMAIL,
        apiToken: JIRA_API_TOKEN,
        projectKey: "OBUSDEV",
        statuses: ["Test Ok", "TEST OK", "In Deployment"],
        issueType: "Task",
        maxResults: JIRA_MAX_RESULTS,
        sortOptions: {
          epicFirst: true
        }
      })
    ]);

    const resultMessages = [];
    boardColumns.forEach((column, index) => {
      const result = columnResults[index] || { cards: [], error: null };
      column.cards = Array.isArray(result.cards) ? result.cards : [];
      column.emptyMessage = result.error
        ? `${column.title} verisi Jira'dan alınamadı.`
        : "Seçilen Jira kriterlerinde task bulunamadı.";
      if (result.error) {
        resultMessages.push(`${column.title}: ${result.error}`);
      }
    });

    boardReport = {
      error: resultMessages.length > 0 ? resultMessages.join(" | ") : null
    };
  }

  res.render("reports-jira-board", {
    user: req.session.user,
    active: "jira-board",
    boardColumns,
    boardReport,
    jiraConfig: {
      baseUrl: jiraBaseUrl,
      hasConfig: jiraConfigOk
    }
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

  let sqlQuery = buildSlackSqlQueryModel({
    requested: false,
    startDate: dbStartDate,
    endDate: dbEndDate
  });

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
  sqlQuery = buildSlackSqlQueryModel({
    requested: true,
    startDate: sqlResult.filters?.startDate || dbStartDate,
    endDate: sqlResult.filters?.endDate || dbEndDate,
    rows: sqlResult.rows || [],
    error: sqlResult.error || null
  });

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
    create_failed: "Kullanıcı oluşturulamadı.",
    login_lock_failed: "Login sabitleme ayarı güncellenemedi.",
    allowed_computer_failed: "Cihaz onayı ayarı güncellenemedi.",
    device_not_found: "Cihaz kaydı bulunamadı.",
    device_update_failed: "Cihaz onayı güncellenemedi."
  };

  const okValue = String(req.query.ok || "").trim();
  const errValue = String(req.query.err || "").trim();
  const notice =
    okValue === "1"
        ? "Kullanıcı oluşturuldu."
      : okValue === "2"
        ? "Kullanıcı güncellendi."
        : okValue === "3"
          ? "Login sabitleme ayarı güncellendi."
          : okValue === "4"
            ? "Cihaz onayı ayarı güncellendi."
            : okValue === "5"
              ? "Cihaz onayı güncellendi."
          : null;
  const error = errorMessages[errValue] || null;
  const editUserId = Number(req.query.edit);
  const editingUserId = Number.isInteger(editUserId) ? editUserId : null;
  const openDevicesUserId = Number(req.query.devices);
  const expandedDeviceUserId = Number.isInteger(openDevicesUserId) ? openDevicesUserId : null;

  try {
    const [result, userLoginDevicesByUserId] = await Promise.all([
      pool.query(
        `
          SELECT
            id,
            username,
            display_name,
            created_at,
            allowed_computer_enabled,
            login_input_lock_enabled,
            login_input_lock_version
          FROM users
          ORDER BY id DESC
        `
      ),
      listUserLoginDevicesGroupedByUserId()
    ]);
    res.render("users", {
      user: req.session.user,
      users: result.rows,
      userLoginDevicesByUserId,
      notice,
      error,
      editingUserId,
      expandedDeviceUserId,
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
    const normalizedUsername = username.trim();
    await pool.query("INSERT INTO users (username, password_hash, display_name) VALUES ($1, $2, $3)", [
      normalizedUsername,
      passwordHash,
      displayName.trim()
    ]);
    const createdUserResult = await pool.query(
      `
        SELECT id
        FROM users
        WHERE username = $1
        ORDER BY id DESC
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [normalizedUsername]
    );
    const newUserId = Number(createdUserResult.rows?.[0]?.id);
    if (Number.isInteger(newUserId)) {
      await ensureSidebarPermissionsForUser(newUserId);
    }
    res.redirect("/users?ok=1");
  } catch (err) {
    console.error(err);
    if (String(err?.code || "") === "23505" || Number(err?.number) === 2627 || Number(err?.number) === 2601) {
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
    const existingUserResult = await pool.query(
      `
        SELECT username
        FROM users
        WHERE id = $1
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [userId]
    );
    const existingUser = existingUserResult.rows?.[0] || null;
    if (!existingUser) {
      return res.redirect("/users?err=user_not_found");
    }

    const shouldBumpLoginLockVersion = Boolean(password) || String(existingUser.username || "").trim() !== username;
    let queryResult;
    if (password) {
      const passwordHash = await bcrypt.hash(password, 10);
      queryResult = await pool.query(
        `
          UPDATE users
          SET username = $1,
              display_name = $2,
              password_hash = $3,
              login_input_lock_version = CASE
                WHEN $5 THEN login_input_lock_version + 1
                ELSE login_input_lock_version
              END
          WHERE id = $4
        `,
        [username, displayName, passwordHash, userId, shouldBumpLoginLockVersion]
      );
    } else {
      queryResult = await pool.query(
        `
          UPDATE users
          SET username = $1,
              display_name = $2,
              login_input_lock_version = CASE
                WHEN $4 THEN login_input_lock_version + 1
                ELSE login_input_lock_version
              END
          WHERE id = $3
        `,
        [username, displayName, userId, shouldBumpLoginLockVersion]
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
    if (String(err?.code || "") === "23505" || Number(err?.number) === 2627 || Number(err?.number) === 2601) {
      return res.redirect(`/users?edit=${userId}&err=username_exists`);
    }
    return res.redirect(`/users?edit=${userId}&err=update_failed`);
  }
});

app.post("/users/:userId/login-lock", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) {
    return res.redirect("/users?err=invalid_user");
  }

  const enabled = parseCheckboxBooleanValue(req.body?.enabled);

  try {
    const result = await pool.query(
      `
        UPDATE users
        SET login_input_lock_enabled = $1,
            login_input_lock_version = login_input_lock_version + 1
        WHERE id = $2
      `,
      [enabled, userId]
    );

    if ((result?.rowCount || 0) === 0) {
      return res.redirect("/users?err=user_not_found");
    }

    return res.redirect("/users?ok=3");
  } catch (err) {
    console.error(err);
    return res.redirect("/users?err=login_lock_failed");
  }
});

app.post("/users/:userId/allowed-computer", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId)) {
    if (requestWantsJson(req)) {
      return res.status(400).json({ ok: false, error: "Geçersiz kullanıcı seçimi." });
    }
    return res.redirect("/users?err=invalid_user");
  }

  const enabled = parseCheckboxBooleanValue(req.body?.enabled);

  try {
    const result = await pool.query(
      `
        UPDATE users
        SET allowed_computer_enabled = $1
        WHERE id = $2
      `,
      [enabled, userId]
    );

    if ((result?.rowCount || 0) === 0) {
      if (requestWantsJson(req)) {
        return res.status(404).json({ ok: false, error: "Kullanıcı bulunamadı." });
      }
      return res.redirect("/users?err=user_not_found");
    }

    if (requestWantsJson(req)) {
      return res.json({
        ok: true,
        enabled,
        message: enabled
          ? "Cihaz onayi zorunlu aktif edildi. Bu kullanici artik sadece Cihazlar bolumunden onay verilen IP ve MAC kayitlariyla giris yapabilir."
          : "Cihaz onayi zorunlu kapatildi. Bu kullanici yeniden cihaz onayi olmadan giris yapabilir."
      });
    }

    return res.redirect(`/users?devices=${userId}&ok=4`);
  } catch (err) {
    console.error(err);
    if (requestWantsJson(req)) {
      return res.status(500).json({ ok: false, error: "Izinli bilgisayar ayari guncellenemedi." });
    }
    return res.redirect(`/users?devices=${userId}&err=allowed_computer_failed`);
  }
});

app.post("/users/:userId/login-devices/:deviceId/update", requireAuth, requireMenuAccess("users"), async (req, res) => {
  const userId = Number(req.params.userId);
  const deviceId = Number(req.params.deviceId);
  if (!Number.isInteger(userId) || !Number.isInteger(deviceId)) {
    if (requestWantsJson(req)) {
      return res.status(400).json({ ok: false, error: "Geçersiz kullanıcı seçimi." });
    }
    return res.redirect("/users?err=invalid_user");
  }

  try {
    const deviceResult = await pool.query(
      `
        SELECT id, user_id, ip_address, mac_address, approved
        FROM user_login_devices
        WHERE id = $1 AND user_id = $2
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [deviceId, userId]
    );
    const deviceRow = normalizeUserLoginDeviceRow(deviceResult.rows?.[0] || {});
    if (!Number.isInteger(deviceRow.id) || deviceRow.userId !== userId) {
      if (requestWantsJson(req)) {
        return res.status(404).json({ ok: false, error: "Cihaz kaydı bulunamadı." });
      }
      return res.redirect(`/users?devices=${userId}&err=device_not_found`);
    }

    const approved = parseCheckboxBooleanValue(req.body?.approved);
    const matchConditions = [];
    const matchParams = [userId];

    if (deviceRow.ipAddress) {
      matchParams.push(deviceRow.ipAddress);
      matchConditions.push(`ip_address = $${matchParams.length}`);
    }
    if (deviceRow.macAddress) {
      matchParams.push(deviceRow.macAddress);
      matchConditions.push(`mac_address = $${matchParams.length}`);
    }

    if (matchConditions.length === 0) {
      matchParams.push(deviceId);
      matchConditions.push(`id = $${matchParams.length}`);
    }

    const matchWhereSql = `user_id = $1 AND (${matchConditions.join(" OR ")})`;
    const updateParams = [approved, ...matchParams];
    const updateWhereSql = matchWhereSql.replace(/\$([0-9]+)/g, (_, value) => `$${Number(value) + 1}`);

    const updateResult = await pool.query(
      `
        UPDATE user_login_devices
        SET approved = $1,
            ip_enabled = $1,
            mac_enabled = $1,
            updated_at = now()
        WHERE ${updateWhereSql}
      `,
      updateParams
    );

    if ((updateResult?.rowCount || 0) === 0) {
      if (requestWantsJson(req)) {
        return res.status(404).json({ ok: false, error: "Cihaz kaydı bulunamadı." });
      }
      return res.redirect(`/users?devices=${userId}&err=device_not_found`);
    }

    const updatedDevicesResult = await pool.query(
      `
        SELECT id
        FROM user_login_devices
        WHERE ${matchWhereSql}
      `,
      matchParams
    );

    const approvedCountResult = await pool.query(
      `
        SELECT COUNT(*)::int AS approved_count
        FROM user_login_devices
        WHERE user_id = $1
          AND approved = true
      `,
      [userId]
    );
    const approvedCount = Number(approvedCountResult.rows?.[0]?.approved_count || 0);
    const updatedDeviceIds = (updatedDevicesResult.rows || [])
      .map((row) => Number(row?.id))
      .filter((id) => Number.isInteger(id));

    if (requestWantsJson(req)) {
      return res.json({
        ok: true,
        approved,
        approvedCount,
        updatedDeviceIds,
        message: approved ? "Cihaz onayi verildi." : "Cihaz onayi kaldirildi."
      });
    }

    return res.redirect(`/users?devices=${userId}&ok=5`);
  } catch (err) {
    console.error(err);
    if (requestWantsJson(req)) {
      return res.status(500).json({ ok: false, error: "Cihaz izinleri guncellenemedi." });
    }
    return res.redirect(`/users?devices=${userId}&err=device_update_failed`);
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
    const isTargetAdmin = String(targetUser.username || "").toLowerCase() === "admin";
    const menuSections = isTargetAdmin
      ? buildPermissionSections(
          SIDEBAR_MENU_REGISTRY.map((item) => ({
            key: item.key,
            label: item.label,
            type: item.type,
            parent_key: item.parentKey || null,
            route: item.route || null,
            route_key: item.routeKey || null,
            sort_order: item.sortOrder,
            icon_key: item.iconKey || "folder",
            can_view: true,
            can_view_logs: true
          }))
        )
      : await loadSidebarPermissionSectionsForUser(userId);
    res.render("permissions", {
      user: req.session.user,
      targetUser,
      menuSections,
      isTargetAdmin,
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
  try {
    const targetUserResult = await pool.query("SELECT username FROM users WHERE id = $1", [userId]);
    if (!targetUserResult.rows.length) {
      return res.status(404).send("Kullanıcı bulunamadı");
    }

    const isTargetAdmin = String(targetUserResult.rows[0].username || "").toLowerCase() === "admin";
    if (isTargetAdmin) {
      return res.redirect(`/permissions/${userId}?ok=1`);
    }

    await ensureSidebarPermissionsForUser(userId);

    const activeMenusResult = await pool.query(
      `
        SELECT key, type, parent_key
        FROM sidebar_menu_items
        WHERE is_active = true
          AND type IN ('section', 'item')
      `
    );
    const activeSections = new Set();
    const activeItems = [];
    const sectionItemsMap = new Map();

    activeMenusResult.rows.forEach((row) => {
      const key = String(row.key || "").trim();
      const type = String(row.type || "").trim();
      const parentKey = String(row.parent_key || "").trim();
      if (!key) return;
      if (type === "section") {
        activeSections.add(key);
        return;
      }
      if (type !== "item") return;
      activeItems.push(key);
      const current = sectionItemsMap.get(parentKey) || [];
      current.push(key);
      sectionItemsMap.set(parentKey, current);
    });
    const validItemKeys = new Set(activeItems);

    const selectedRaw = req.body.menuKeys;
    const selectedList = Array.isArray(selectedRaw)
      ? selectedRaw
      : typeof selectedRaw === "string" && selectedRaw.trim()
        ? [selectedRaw]
        : [];
    const selectedItemKeys = Array.from(
      new Set(
        selectedList
          .map((value) => String(value || "").trim())
          .filter((value) => value && validItemKeys.has(value))
      )
    );

    const selectedLogRaw = req.body.menuLogKeys;
    const selectedLogList = Array.isArray(selectedLogRaw)
      ? selectedLogRaw
      : typeof selectedLogRaw === "string" && selectedLogRaw.trim()
        ? [selectedLogRaw]
        : [];
    const selectedLogKeys = Array.from(
      new Set(
        selectedLogList
          .map((value) => String(value || "").trim())
          .filter((value) => value && validItemKeys.has(value))
      )
    );

    const selectedSectionsRaw = req.body.sectionKeys;
    const selectedSectionList = Array.isArray(selectedSectionsRaw)
      ? selectedSectionsRaw
      : typeof selectedSectionsRaw === "string" && selectedSectionsRaw.trim()
        ? [selectedSectionsRaw]
        : [];
    const selectedSectionKeys = Array.from(
      new Set(
        selectedSectionList
          .map((value) => String(value || "").trim())
          .filter((value) => value && activeSections.has(value))
      )
    );
    const selectedSectionSet = new Set(selectedSectionKeys);
    const currentSectionPermissionResult = await pool.query(
      `
        SELECT usp.menu_key, COALESCE(usp.can_view, false) AS can_view
        FROM user_sidebar_permissions usp
        JOIN sidebar_menu_items m
          ON m.key = usp.menu_key
        WHERE usp.user_id = $1
          AND m.is_active = true
          AND m.type = 'section'
      `,
      [userId]
    );
    const currentSectionSet = new Set(
      currentSectionPermissionResult.rows
        .filter((row) => toSidebarBool(row.can_view, false))
        .map((row) => String(row.menu_key || "").trim())
        .filter((value) => activeSections.has(value))
    );

    const touchedSectionsInput = parseJsonSafe(String(req.body.sectionTouchedJson || ""));
    const touchedSectionKeys = Array.isArray(touchedSectionsInput)
      ? touchedSectionsInput.map((value) => String(value || "").trim()).filter((value) => activeSections.has(value))
      : [];
    const changedSectionKeys = new Set(touchedSectionKeys);

    const finalItemSet = new Set(selectedItemKeys);
    changedSectionKeys.forEach((sectionKey) => {
      const children = sectionItemsMap.get(sectionKey) || [];
      if (selectedSectionSet.has(sectionKey)) {
        children.forEach((itemKey) => finalItemSet.add(itemKey));
      } else {
        children.forEach((itemKey) => finalItemSet.delete(itemKey));
      }
    });
    const finalItemKeys = Array.from(finalItemSet);
    const finalLogKeys = selectedLogKeys.filter((key) => finalItemSet.has(key));
    const effectiveSectionSet = new Set(currentSectionSet);
    changedSectionKeys.forEach((sectionKey) => {
      if (selectedSectionSet.has(sectionKey)) {
        effectiveSectionSet.add(sectionKey);
      } else {
        effectiveSectionSet.delete(sectionKey);
      }
    });
    sectionItemsMap.forEach((children, sectionKey) => {
      if (!activeSections.has(sectionKey)) return;
      if (children.some((itemKey) => finalItemSet.has(itemKey))) {
        effectiveSectionSet.add(sectionKey);
      }
    });
    const effectiveSectionKeys = Array.from(effectiveSectionSet);

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      await client.query(
        `
          UPDATE user_sidebar_permissions usp
          SET can_view = false, updated_at = now()
          FROM sidebar_menu_items m
          WHERE usp.user_id = $1
            AND usp.menu_key = m.key
            AND m.is_active = true
            AND m.type = 'item'
        `,
        [userId]
      );

      if (finalItemKeys.length > 0) {
        const finalItemPlaceholders = buildInClausePlaceholders(finalItemKeys, 2);
        await client.query(
          `
            UPDATE user_sidebar_permissions
            SET can_view = true, updated_at = now()
            WHERE user_id = $1
              AND menu_key IN (${finalItemPlaceholders})
          `,
          [userId].concat(finalItemKeys)
        );
      }

      await client.query(
        `
          UPDATE user_sidebar_permissions usp
          SET can_view_logs = false, updated_at = now()
          FROM sidebar_menu_items m
          WHERE usp.user_id = $1
            AND usp.menu_key = m.key
            AND m.is_active = true
            AND m.type = 'item'
        `,
        [userId]
      );

      if (finalLogKeys.length > 0) {
        const selectedLogPlaceholders = buildInClausePlaceholders(finalLogKeys, 2);
        await client.query(
          `
            UPDATE user_sidebar_permissions
            SET can_view_logs = true, updated_at = now()
            WHERE user_id = $1
              AND menu_key IN (${selectedLogPlaceholders})
          `,
          [userId].concat(finalLogKeys)
        );
      }

      await client.query(
        `
          UPDATE user_sidebar_permissions usp
          SET can_view = false, updated_at = now()
          FROM sidebar_menu_items m
          WHERE usp.user_id = $1
            AND usp.menu_key = m.key
            AND m.is_active = true
            AND m.type = 'section'
        `,
        [userId]
      );

      if (effectiveSectionKeys.length > 0) {
        const effectiveSectionPlaceholders = buildInClausePlaceholders(effectiveSectionKeys, 2);
        await client.query(
          `
            UPDATE user_sidebar_permissions
            SET can_view = true, updated_at = now()
            WHERE user_id = $1
              AND menu_key IN (${effectiveSectionPlaceholders})
          `,
          [userId].concat(effectiveSectionKeys)
        );
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    res.redirect(`/permissions/${userId}?ok=1`);
  } catch (err) {
    console.error(err);
    res.status(500).send("Güncelleme hatası");
  }
});

app.get("/api/screen-logs/:menuKey", requireAuth, async (req, res) => {
  const menuKey = String(req.params.menuKey || "").trim();
  const registryItem = getScreenActionRegistryItem(menuKey);
  if (!registryItem) {
    return res.status(404).json({ ok: false, error: "Ekran log kaynağı bulunamadı." });
  }

  const allowed = await canUserViewScreenLogs(req.session?.user, registryItem.key);
  if (!allowed) {
    return res.status(403).json({ ok: false, error: "Bu ekran loglarını görüntüleme yetkiniz yok." });
  }

  const panel = await loadScreenLogPanelForUser(req.session?.user, registryItem.key, 20);
  return res.json({
    ok: true,
    menuKey: panel.menuKey,
    title: panel.title,
    items: Array.isArray(panel.items) ? panel.items : []
  });
});

app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      ok: true,
      dbInitOk: dbRuntimeState.initOk,
      dbInitCompletedAt: dbRuntimeState.initCompletedAt,
      dbInitError: dbRuntimeState.initError,
      dbInitErrorCode: dbRuntimeState.initErrorCode,
      dbInitErrorRaw: dbRuntimeState.initErrorRaw
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: classifyDbErrorForUser(err),
      errorCode: String(err?.code || err?.originalError?.code || ""),
      errorRaw: summarizeErrorMessage(err),
      dbInitOk: dbRuntimeState.initOk,
      dbInitError: dbRuntimeState.initError,
      dbInitErrorCode: dbRuntimeState.initErrorCode,
      dbInitErrorRaw: dbRuntimeState.initErrorRaw,
      dbInitCompletedAt: dbRuntimeState.initCompletedAt
    });
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
    const updateResult = await pool.query("UPDATE api_targets SET updated_at = now() WHERE url = $1", [normalized]);
    if (!updateResult.rowCount) {
      await pool.query("INSERT INTO api_targets (url) VALUES ($1)", [normalized]);
    }
    const result = await pool.query(
      `
        SELECT id, url, created_at, updated_at
        FROM api_targets
        WHERE url = $1
        ORDER BY id DESC
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
      `,
      [normalized]
    );
    res.json({ ok: true, item: result.rows[0] || null });
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
    const normalizedTitle = title.trim();
    const normalizedMethod = method.trim().toUpperCase();
    const normalizedPath = path.trim();
    const normalizedTargetUrl = targetUrl ? targetUrl.trim() : null;
    const normalizedDescription = description ? description.trim() : null;
    const normalizedBody = body || "{}";
    const normalizedHeaders = headers || "{\n  \"Content-Type\": \"application/json\"\n}";
    const normalizedParams = params || "{}";

    await pool.query(
      `INSERT INTO api_endpoints (title, method, path, target_url, description, body, headers, params, sort_order)
       VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8,
         COALESCE((SELECT MIN(sort_order) - 1 FROM api_endpoints), 1)
       )`,
      [
        normalizedTitle,
        normalizedMethod,
        normalizedPath,
        normalizedTargetUrl,
        normalizedDescription,
        normalizedBody,
        normalizedHeaders,
        normalizedParams
      ]
    );
    const result = await pool.query(
      `SELECT id, title, method, path, target_url, description, body, headers, params, sort_order
       FROM api_endpoints
       WHERE title = $1
         AND method = $2
         AND path = $3
         AND (
           (target_url = $4)
           OR (target_url IS NULL AND $4 IS NULL)
         )
       ORDER BY id DESC
       OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY`,
      [normalizedTitle, normalizedMethod, normalizedPath, normalizedTargetUrl]
    );
    res.json({ ok: true, item: result.rows[0] || null });
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

    const totalResult = await pool.query("SELECT CAST(COUNT(*) AS INT) AS count FROM api_endpoints");
    const total = totalResult.rows[0]?.count || 0;
    if (uniqueIds.length !== total) {
      return res.status(400).json({ ok: false, error: "Tüm endpointler sıralama listesinde olmalı." });
    }

    const checkPlaceholders = buildInClausePlaceholders(uniqueIds, 1);
    const checkResult = await pool.query(
      `SELECT CAST(COUNT(*) AS INT) AS count FROM api_endpoints WHERE id IN (${checkPlaceholders})`,
      uniqueIds
    );
    if (checkResult.rows[0]?.count !== total) {
      return res.status(400).json({ ok: false, error: "Geçersiz endpoint id listesi." });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (let i = 0; i < uniqueIds.length; i += 1) {
        await client.query(
          `
            UPDATE api_endpoints
            SET sort_order = $2,
                updated_at = now()
            WHERE id = $1
          `,
          [uniqueIds[i], i + 1]
        );
      }
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

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

    await pool.query(
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
       WHERE id = $10`,
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
    const result = await pool.query(
      `SELECT id, title, method, path, target_url, description, body, headers, params, sort_order
       FROM api_endpoints
       WHERE id = $1`,
      [id]
    );
    res.json({ ok: true, item: result.rows[0] || null });
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

  const requestBody = hasBody ? buildExecuteRequestBody(body) : undefined;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);
  const startedAt = Date.now();

  try {
    const response = await fetch(url.toString(), {
      method: httpMethod,
      headers: finalHeaders,
      body: requestBody,
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
          requestBody || "",
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
          requestBody || "",
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

const handleMentiChatGptRequest = async (req, res) => {
  const prompt = String(req.body?.prompt || "").trim();

  if (!prompt) {
    return res.status(400).json({ ok: false, error: "Mesaj bos olamaz." });
  }
  if (prompt.length > 12000) {
    return res.status(400).json({ ok: false, error: "Mesaj cok uzun." });
  }

  return res.json({
    ok: true,
    reply: "Bu ekran artik API kullanmiyor. Mesaji kopyalayip ChatGPT web ekraninda yapistirin.",
    model: "chatgpt-web-helper"
  });
};

app.post("/api/menti/chatgpt-chat", requireAuth, requireMenuAccess("menti"), handleMentiChatGptRequest);
app.post("/api/menti/gemini-chat", requireAuth, requireMenuAccess("menti"), handleMentiChatGptRequest);

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

function wantsJsonErrorResponse(req) {
  const pathText = String(req.path || req.originalUrl || "").trim();
  if (pathText.startsWith("/api/")) return true;

  const accept = String(req.headers?.accept || "").toLowerCase();
  const contentType = String(req.headers?.["content-type"] || "").toLowerCase();
  return accept.includes("application/json") || contentType.includes("application/json");
}

function buildRequestBodyTooLargeMessage(req) {
  const pathText = String(req.path || req.originalUrl || "").trim();
  const limitText = String(REQUEST_BODY_LIMIT || "5mb").trim();

  return `Gönderilen veri sunucu limitini (${limitText}) aştı.`;
}

app.use((err, req, res, next) => {
  if (err?.type !== "entity.too.large" && Number(err?.status || 0) !== 413) {
    return next(err);
  }

  const errorMessage = buildRequestBodyTooLargeMessage(req);
  if (wantsJsonErrorResponse(req)) {
    return res.status(413).json({ ok: false, error: errorMessage });
  }
  return res.status(413).send(errorMessage);
});

if (require.main === module && !initDbOnly) {
  app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}
