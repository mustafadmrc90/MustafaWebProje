#!/usr/bin/env node

const fs = require("fs");
const path = require("path");

const SEARCH_TEXT = String(process.argv[2] || "strela").trim().toLowerCase();
const REQUEST_TIMEOUT_MS = Number.parseInt(process.env.DEBUG_OBUS_TIMEOUT_MS || "10000", 10) || 10000;
const RESPONSE_LIMIT = Number.parseInt(process.env.DEBUG_OBUS_RESPONSE_LIMIT || "12000", 10) || 12000;
const CLUSTER_MIN = 0;
const CLUSTER_MAX = 15;

function loadEnv(filePath = path.join(__dirname, "..", ".env")) {
  if (!fs.existsSync(filePath)) return;
  const raw = fs.readFileSync(filePath, "utf8");
  raw.split(/\r?\n/).forEach((line) => {
    const trimmed = String(line || "").trim();
    if (!trimmed || trimmed.startsWith("#")) return;
    const normalizedLine = trimmed.startsWith("export ") ? trimmed.slice(7).trim() : trimmed;
    const eqIndex = normalizedLine.indexOf("=");
    if (eqIndex <= 0) return;

    const key = normalizedLine.slice(0, eqIndex).trim();
    if (!key || process.env[key]) return;

    let value = normalizedLine.slice(eqIndex + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    process.env[key] = value;
  });
}

function truncate(value, maxLen = RESPONSE_LIMIT) {
  const text = String(value || "").trim();
  if (!text || maxLen === 0) return text;
  return text.length > maxLen ? `${text.slice(0, maxLen)}... [truncated ${text.length - maxLen} chars]` : text;
}

function mask(value) {
  return String(value || "")
    .replace(/("password"\s*:\s*)"[^"]*"/gi, '$1"***"')
    .replace(/("session-id"\s*:\s*)"[^"]*"/gi, '$1"***"')
    .replace(/("device-id"\s*:\s*)"[^"]*"/gi, '$1"***"')
    .replace(/("token"\s*:\s*)"[^"]*"/gi, '$1"***"')
    .replace(/("data"\s*:\s*)"([A-Za-z0-9+/=]{20,})"/g, '$1"***"');
}

function normalizeTokenName(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function normalizeSearchTokenName(value) {
  return String(value || "")
    .replace(/[Çç]/g, "c")
    .replace(/[Ğğ]/g, "g")
    .replace(/[İIı]/g, "i")
    .replace(/[Öö]/g, "o")
    .replace(/[Şş]/g, "s")
    .replace(/[Üü]/g, "u")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function isObusMerkezBranchName(value) {
  return normalizeSearchTokenName(value).startsWith("obusmerkez");
}

function parseJsonSafe(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function readByAliases(row, aliases = []) {
  if (!row || typeof row !== "object") return undefined;
  const aliasSet = new Set((Array.isArray(aliases) ? aliases : []).map(normalizeTokenName).filter(Boolean));
  if (aliasSet.size === 0) return undefined;
  for (const [key, value] of Object.entries(row)) {
    if (aliasSet.has(normalizeTokenName(key))) return value;
  }
  return undefined;
}

function findNestedValue(node, aliases = []) {
  if (node === null || node === undefined) return "";
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findNestedValue(item, aliases);
      if (found) return found;
    }
    return "";
  }
  if (typeof node !== "object") return "";

  const direct = readByAliases(node, aliases);
  if (direct !== undefined && direct !== null && String(direct).trim()) return String(direct).trim();
  for (const value of Object.values(node)) {
    const found = findNestedValue(value, aliases);
    if (found) return found;
  }
  return "";
}

function extractClusterLabel(value) {
  const match = String(value || "").match(/cluster\d+/i);
  return match ? match[0].toLowerCase() : "";
}

function buildUrlForCluster(baseUrl, clusterLabel) {
  const raw = String(baseUrl || "").trim();
  if (!raw) return "";
  return /cluster\d+/i.test(raw) ? raw.replace(/cluster\d+/i, clusterLabel) : raw;
}

function buildSessionBody() {
  return {
    type: 1,
    connection: {
      "ip-address": String(
        process.env.OBUS_SESSION_CONNECTION_IP_ADDRESS || process.env.OBUS_CONNECTION_IP_ADDRESS || "212.156.219.182"
      ).trim(),
      port: String(process.env.OBUS_SESSION_CONNECTION_PORT || "5117").trim() || "5117"
    },
    browser: {
      name: "Chrome"
    }
  };
}

async function postJson(url, body, authorization) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Authorization: authorization
      },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    const raw = await response.text();
    return {
      ok: response.ok,
      status: response.status,
      statusText: response.statusText,
      raw,
      parsed: parseJsonSafe(raw)
    };
  } catch (err) {
    return {
      ok: false,
      status: null,
      statusText: "",
      raw: "",
      parsed: null,
      error: err?.name === "AbortError" ? `${REQUEST_TIMEOUT_MS}ms timeout` : err?.message || String(err)
    };
  } finally {
    clearTimeout(timeout);
  }
}

function collectPartnerMatches(payload, clusterLabel) {
  const matches = [];
  const seen = new Set();
  const aliases = [
    "code",
    "name",
    "title",
    "display-name",
    "display_name",
    "displayName",
    "commercial-title",
    "commercial_title",
    "url"
  ];
  const idAliases = ["id", "partner-id", "partner_id", "partnerId", "provider-id", "provider_id", "providerId"];

  const walk = (node) => {
    if (node === null || node === undefined || matches.length >= 30) return;
    if (Array.isArray(node)) {
      node.forEach(walk);
      return;
    }
    if (typeof node !== "object") return;

    const searchable = aliases
      .map((alias) => readByAliases(node, [alias]))
      .filter((value) => value !== undefined && value !== null)
      .map((value) => String(value).toLowerCase())
      .join(" ");
    const id = String(readByAliases(node, idAliases) ?? "").trim();
    const code = String(readByAliases(node, ["code"]) ?? "").trim();

    if (searchable.includes(SEARCH_TEXT) && (id || code)) {
      const key = `${clusterLabel}|||${id}|||${code}`;
      if (!seen.has(key)) {
        seen.add(key);
        matches.push({
          cluster: clusterLabel,
          id,
          code,
          status: String(readByAliases(node, ["status"]) ?? "").trim(),
          name: String(readByAliases(node, ["name", "title", "display-name", "display_name", "displayName"]) ?? "").trim(),
          url: String(readByAliases(node, ["url"]) ?? "").trim()
        });
      }
    }

    Object.values(node).forEach(walk);
  };

  walk(payload);
  return matches;
}

function extractObusMerkezRows(payload, fallbackPartnerId = "", clusterLabel = "") {
  const rows = [];
  const normalizedFallbackPartnerId = String(fallbackPartnerId || "").trim();
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
  const branchIdAliases = ["id", "key", "branch-id", "branch_id", "branchid", "branch-key", "branch_key", "branchkey"];
  const branchNameAliases = [
    "name",
    "branch-name",
    "branch_name",
    "branchname",
    "label",
    "title",
    "text",
    "display-name",
    "display_name",
    "displayName",
    "value"
  ];
  const nestedPartnerAliases = ["partner", "company", "provider", "firm", "operator"];
  const readExplicitPartnerId = (node) => {
    const direct = String(readByAliases(node, partnerIdAliases) ?? "").trim();
    if (direct) return direct;
    const nested = readByAliases(node, nestedPartnerAliases);
    if (nested && typeof nested === "object" && !Array.isArray(nested)) {
      return String(readByAliases(nested, ["id", ...partnerIdAliases]) ?? "").trim();
    }
    return "";
  };
  const hasNestedBranchCollection = (node) =>
    Object.entries(node || {}).some(([key, value]) => {
      if (!value || typeof value !== "object") return false;
      const normalizedKey = normalizeSearchTokenName(key);
      return normalizedKey.includes("branch") || normalizedKey.includes("sube");
    });
  const readContainerPartnerId = (node, branchName = "") => {
    const explicit = readExplicitPartnerId(node);
    if (explicit) return explicit;
    if (isObusMerkezBranchName(branchName) || !hasNestedBranchCollection(node)) return "";
    return String(readByAliases(node, ["id", "partner-id", "partner_id", "partnerId"]) ?? "").trim();
  };
  const walk = (node, inheritedPartnerId = "") => {
    if (node === null || node === undefined) return;
    if (typeof node === "string") {
      const trimmed = node.trim();
      const parsed =
        (trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))
          ? parseJsonSafe(trimmed)
          : null;
      if (parsed !== null) walk(parsed, inheritedPartnerId);
      return;
    }
    if (Array.isArray(node)) {
      node.forEach((item) => walk(item, inheritedPartnerId));
      return;
    }
    if (typeof node !== "object") return;

    const branchName = String(readByAliases(node, branchNameAliases) ?? "").trim();
    const directPartnerId = readExplicitPartnerId(node);
    const containerPartnerId = readContainerPartnerId(node, branchName);
    const nextPartnerId = directPartnerId || containerPartnerId || String(inheritedPartnerId || "").trim();
    if (isObusMerkezBranchName(branchName)) {
      const partnerId = directPartnerId || String(inheritedPartnerId || "").trim() || normalizedFallbackPartnerId;
      const branchId = String(readByAliases(node, branchIdAliases) ?? "").trim();
      if (partnerId && branchId) rows.push({ partnerId, name: branchName, branchId, cluster: clusterLabel });
    }
    Object.entries(node).forEach(([key, value]) => {
      const keyPartnerId = !nextPartnerId && /^\d+$/.test(String(key || "").trim()) ? String(key || "").trim() : nextPartnerId;
      walk(value, keyPartnerId);
    });
  };

  walk(payload, normalizedFallbackPartnerId);
  return rows;
}

async function findPartnerMatches() {
  const authorization = process.env.PARTNERS_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
  const partnerBaseUrl =
    process.env.PARTNERS_API_URL || "https://api-coreprod-cluster0.obus.com.tr/api/partner/getpartners";
  const sessionBaseUrl =
    process.env.PARTNERS_SESSION_API_URL || "https://api-coreprod-cluster0.obus.com.tr/api/client/getsession";

  const clusterNumbers = Array.from({ length: CLUSTER_MAX - CLUSTER_MIN + 1 }, (_, index) => CLUSTER_MIN + index);
  const results = await Promise.all(
    clusterNumbers.map(async (clusterNumber) => {
      const cluster = `cluster${clusterNumber}`;
      const sessionUrl = buildUrlForCluster(sessionBaseUrl, cluster);
      const partnerUrl = buildUrlForCluster(partnerBaseUrl, cluster);
      const sessionResponse = await postJson(sessionUrl, buildSessionBody(), authorization);
      if (!sessionResponse.ok || !sessionResponse.parsed) {
        return {
          cluster,
          error: `GetSession failed: ${sessionResponse.error || `HTTP ${sessionResponse.status}`}`,
          response: truncate(mask(sessionResponse.raw), 2000)
        };
      }

      const sessionId = findNestedValue(sessionResponse.parsed, ["session-id", "session_id", "sessionId"]);
      const deviceId = findNestedValue(sessionResponse.parsed, ["device-id", "device_id", "deviceId"]);
      const partnerResponse = await postJson(
        partnerUrl,
        {
          data: "BusTicketProvider",
          "device-session": {
            "session-id": sessionId,
            "device-id": deviceId
          },
          date: "2016-03-11T11:33:00",
          language: "tr-TR"
        },
        authorization
      );
      if (!partnerResponse.ok || !partnerResponse.parsed) {
        return {
          cluster,
          error: `GetPartners failed: ${partnerResponse.error || `HTTP ${partnerResponse.status}`}`,
          response: truncate(mask(partnerResponse.raw), 2000)
        };
      }
      return {
        cluster,
        matches: collectPartnerMatches(partnerResponse.parsed, cluster)
      };
    })
  );

  return results;
}

async function inspectPartner(match) {
  const authorization = process.env.INVENTORY_BRANCHES_API_AUTH || "Basic MTIzNDU2MHg2NTUwR21STG5QYXJ5bnVt";
  const username = String(process.env.INVENTORY_BRANCHES_LOGIN_USERNAME || "busproductapp").trim();
  const password = String(process.env.INVENTORY_BRANCHES_LOGIN_PASSWORD || "").trim();
  const cluster = match.cluster;
  const sessionUrl = `https://api-coreprod-${cluster}.obus.com.tr/api/client/getsession`;
  const loginUrl = `https://api-coreprod-${cluster}.obus.com.tr/api/membership/userlogin`;
  const branchesUrl = `https://api-coreprod-${cluster}.obus.com.tr/api/inventory/getbranches`;

  console.log(`\\n=== ${cluster} / ${match.code || "-"} / ${match.id || "-"} ===`);
  console.log(JSON.stringify(match, null, 2));

  const sessionResponse = await postJson(sessionUrl, buildSessionBody(), authorization);
  if (!sessionResponse.ok || !sessionResponse.parsed) {
    console.log(`STUCK: GetSession ${sessionResponse.error || `HTTP ${sessionResponse.status}`}`);
    console.log(truncate(mask(sessionResponse.raw)));
    return;
  }
  const sessionId = findNestedValue(sessionResponse.parsed, ["session-id", "session_id", "sessionId"]);
  const deviceId = findNestedValue(sessionResponse.parsed, ["device-id", "device_id", "deviceId"]);

  const loginBody = {
    data: {
      username,
      password,
      "remember-me": 0,
      "partner-code": match.code
    },
    "device-session": {
      "session-id": sessionId,
      "device-id": deviceId
    },
    date: "2020-02-24T18:03:00",
    language: "tr-TR"
  };
  const loginResponse = await postJson(loginUrl, loginBody, authorization);
  if (!loginResponse.ok || !loginResponse.parsed) {
    console.log(`STUCK: UserLogin ${loginResponse.error || `HTTP ${loginResponse.status}`}`);
    console.log(truncate(mask(loginResponse.raw)));
    return;
  }
  const token =
    String(loginResponse.parsed?.token?.data || "").trim() ||
    String(loginResponse.parsed?.token?.token || "").trim() ||
    String(loginResponse.parsed?.token || "").trim() ||
    (typeof loginResponse.parsed?.data === "string" ? String(loginResponse.parsed.data || "").trim() : "") ||
    String(loginResponse.parsed?.data?.token?.data || "").trim();
  if (!token) {
    console.log("STUCK: UserLogin token not found");
    console.log(truncate(mask(loginResponse.raw)));
    return;
  }

  const branchesResponse = await postJson(
    branchesUrl,
    {
      data: {},
      "device-session": {
        "session-id": sessionId,
        "device-id": deviceId
      },
      token,
      date: "2016-03-11T11:33:00",
      language: "tr-TR"
    },
    authorization
  );
  if (!branchesResponse.ok || !branchesResponse.parsed) {
    console.log(`STUCK: GetBranches ${branchesResponse.error || `HTTP ${branchesResponse.status}`}`);
    console.log(truncate(mask(branchesResponse.raw)));
    return;
  }

  const obusRows = extractObusMerkezRows(branchesResponse.parsed, match.id, cluster);
  const targetRows = obusRows.filter((row) => String(row.partnerId || "").trim() === String(match.id || "").trim());
  if (targetRows.length > 0) {
    console.log("FOUND_OBUSMERKEZ:");
    console.log(JSON.stringify(targetRows, null, 2));
    return;
  }

  console.log("NOT_FOUND_IN_GETBRANCHES_RESPONSE");
  console.log("Parsed OBUSMERKEZ rows:");
  console.log(JSON.stringify(obusRows, null, 2));
  console.log("GetBranches response:");
  console.log(truncate(mask(branchesResponse.raw)));
}

async function main() {
  loadEnv();
  if (!SEARCH_TEXT) throw new Error("Usage: node scripts/debug-obus-branch-row.js <search-text>");

  console.log(`Searching partner list for: ${SEARCH_TEXT}`);
  const clusterResults = await findPartnerMatches();
  const matches = clusterResults.flatMap((result) => result.matches || []);
  const errors = clusterResults.filter((result) => result.error);

  if (errors.length > 0) {
    console.log("\nCluster errors:");
    errors.forEach((item) => console.log(`${item.cluster}: ${item.error}`));
  }
  if (matches.length === 0) {
    console.log("\nNo partner row matched.");
    return;
  }

  console.log("\nPartner matches:");
  console.log(JSON.stringify(matches, null, 2));

  for (const match of matches) {
    await inspectPartner(match);
  }
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
