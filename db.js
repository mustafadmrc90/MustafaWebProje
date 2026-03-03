const sql = require("mssql");
const net = require("net");
const { Pool: PgDriverPool } = require("pg");

function parseBooleanFlag(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  return fallback;
}

function parseOptionalInt(value, fallback) {
  if (value === undefined || value === null || String(value).trim() === "") return fallback;
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function isMssqlStyleConnectionString(raw) {
  return /^[a-z0-9 _-]+=/i.test(raw) && raw.includes(";");
}

function detectDatabaseEngine(connectionString) {
  const raw = String(connectionString || "").trim();
  if (!raw) {
    throw new Error("DATABASE_URL is required");
  }

  if (isMssqlStyleConnectionString(raw)) {
    return "mssql";
  }

  let parsed;
  try {
    parsed = new URL(raw);
  } catch (err) {
    throw new Error(`DATABASE_URL parse failed: ${err?.message || "unknown error"}`);
  }

  const protocol = String(parsed.protocol || "").toLowerCase();
  if (protocol === "postgres:" || protocol === "postgresql:") {
    return "postgres";
  }
  if (protocol === "mssql:" || protocol === "sqlserver:") {
    return "mssql";
  }

  throw new Error(`DATABASE_URL parse failed: Unsupported protocol '${protocol || "unknown"}'`);
}

function buildTlsOptionsForServer(serverName) {
  const host = String(serverName || "").trim();
  // Node.js does not allow SNI to be an IP literal. Disable explicit servername in that case.
  if (host && net.isIP(host)) {
    return {
      servername: "",
      serverName: "",
      cryptoCredentialsDetails: {
        servername: ""
      }
    };
  }
  return {};
}

function parseMssqlConnectionString(connectionString) {
  const raw = String(connectionString || "").trim();
  if (!raw) {
    throw new Error("DATABASE_URL is required");
  }

  // SQL Server style: Server=...;Database=...;User Id=...;Password=...;
  if (isMssqlStyleConnectionString(raw)) {
    const kv = {};
    String(raw)
      .split(";")
      .map((part) => String(part || "").trim())
      .filter(Boolean)
      .forEach((part) => {
        const idx = part.indexOf("=");
        if (idx <= 0) return;
        const key = part
          .slice(0, idx)
          .replace(/\s+/g, " ")
          .trim()
          .toLowerCase();
        const value = part.slice(idx + 1).trim();
        kv[key] = value;
      });

    const serverRaw = kv.server || kv["data source"] || kv.address || "";
    let server = "";
    let portRaw = kv.port || "";
    if (serverRaw.includes(",")) {
      [server, portRaw] = serverRaw.split(",");
    } else if (/^[^:]+:\d+$/.test(serverRaw)) {
      [server, portRaw] = serverRaw.split(":");
    } else {
      server = serverRaw;
    }
    const host = String(server || "")
      .trim()
      .replace(/^tcp:/i, "");
    if (!host) {
      throw new Error("DATABASE_URL parse failed: Server/host is empty");
    }
    const requestedEncrypt = parseBooleanFlag(kv.encrypt, true);
    const encrypt = host && net.isIP(host) ? false : requestedEncrypt;
    return {
      user: kv["user id"] || kv.uid || kv.user || "",
      password: kv.password || kv.pwd || "",
      server: host,
      port: Number.parseInt(portRaw || kv.port || "1433", 10) || 1433,
      database: kv.database || kv["initial catalog"] || "master",
      options: {
        encrypt,
        trustServerCertificate: parseBooleanFlag(
          kv.trustservercertificate,
          parseBooleanFlag(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED, false) === false
        ),
        ...buildTlsOptionsForServer(host)
      },
      pool: {
        min: 0,
        max: 10,
        idleTimeoutMillis: 30000
      }
    };
  }

  // URL style: mssql://user:pass@host:1433/db or sqlserver://...
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (err) {
    throw new Error(`DATABASE_URL parse failed: ${err?.message || "unknown error"}`);
  }

  const sslMode = String(parsed.searchParams.get("sslmode") || "").toLowerCase();
  const useSsl =
    parseBooleanFlag(process.env.DATABASE_SSL, true) ||
    sslMode === "require" ||
    sslMode === "verify-ca" ||
    sslMode === "verify-full";

  const rejectUnauthorized = parseBooleanFlag(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED, false);

  const host = String(parsed.hostname || "").trim();
  if (!host) {
    throw new Error("DATABASE_URL parse failed: Host is empty");
  }
  const encrypt = host && net.isIP(host) ? false : useSsl;
  return {
    user: decodeURIComponent(parsed.username || ""),
    password: decodeURIComponent(parsed.password || ""),
    server: host,
    port: Number.parseInt(String(parsed.port || "1433"), 10) || 1433,
    database: String(parsed.pathname || "/master").replace(/^\//, "") || "master",
    options: {
      encrypt,
      trustServerCertificate: !rejectUnauthorized,
      ...buildTlsOptionsForServer(host)
    },
    pool: {
      min: 0,
      max: 10,
      idleTimeoutMillis: 30000
    }
  };
}

function normalizeSqlTypes(value) {
  return String(value || "")
    .replace(/\bSERIAL\b/gi, "INT IDENTITY(1,1)")
    .replace(/\bTIMESTAMPTZ\b/gi, "DATETIME2")
    .replace(/\bBOOLEAN\b/gi, "BIT")
    .replace(/\bINTEGER\b/gi, "INT")
    .replace(/\bTEXT\b/gi, "NVARCHAR(MAX)")
    .replace(/\bnow\(\)/gi, "SYSUTCDATETIME()");
}

function normalizeCreateTableBodyByTable(tableName, body) {
  let text = String(body || "");
  const table = String(tableName || "").trim().toLowerCase();

  if (table === "users") {
    text = text.replace(/\busername\s+NVARCHAR\(MAX\)\s+UNIQUE\b/i, "username NVARCHAR(255) UNIQUE");
  }
  if (table === "screens") {
    text = text.replace(/\bkey\s+NVARCHAR\(MAX\)\s+UNIQUE\b/i, "key NVARCHAR(255) UNIQUE");
  }
  if (table === "api_targets") {
    text = text.replace(/\burl\s+NVARCHAR\(MAX\)\s+UNIQUE\b/i, "url NVARCHAR(450) UNIQUE");
  }
  if (table === "sidebar_menu_items") {
    text = text
      .replace(/\bkey\s+NVARCHAR\(MAX\)\s+PRIMARY\s+KEY\b/i, "key NVARCHAR(255) PRIMARY KEY")
      .replace(
        /\bparent_key\s+NVARCHAR\(MAX\)\s+REFERENCES\s+sidebar_menu_items\(key\)\s+ON\s+DELETE\s+CASCADE\b/i,
        "parent_key NVARCHAR(255) REFERENCES sidebar_menu_items(key) ON DELETE CASCADE"
      )
      .replace(/\broute_key\s+NVARCHAR\(MAX\)\b/i, "route_key NVARCHAR(255)");
  }
  if (table === "user_sidebar_permissions") {
    text = text.replace(
      /\bmenu_key\s+NVARCHAR\(MAX\)\s+NOT\s+NULL\s+REFERENCES\s+sidebar_menu_items\(key\)\s+ON\s+DELETE\s+CASCADE\b/i,
      "menu_key NVARCHAR(255) NOT NULL REFERENCES sidebar_menu_items(key) ON DELETE CASCADE"
    );
  }
  if (table === "obus_merkez_branches") {
    text = text
      .replace(/\bpartner_id\s+NVARCHAR\(MAX\)\s+PRIMARY\s+KEY\b/i, "partner_id NVARCHAR(255) PRIMARY KEY")
      .replace(/\bbranch_id\s+NVARCHAR\(MAX\)\s+NOT\s+NULL\b/i, "branch_id NVARCHAR(255) NOT NULL")
      .replace(/\bsource_cluster\s+NVARCHAR\(MAX\)\b/i, "source_cluster NVARCHAR(255)");
  }
  if (table === "all_companies_cache") {
    text = text
      .replace(/\bid\s+NVARCHAR\(MAX\)\s+NOT\s+NULL\b/i, "id NVARCHAR(255) NOT NULL")
      .replace(/\bcode\s+NVARCHAR\(MAX\)\s+NOT\s+NULL\b/i, "code NVARCHAR(255) NOT NULL")
      .replace(/\bsource\s+NVARCHAR\(MAX\)\s+NOT\s+NULL\b/i, "source NVARCHAR(255) NOT NULL")
      .replace(/\bobilet_partner_id\s+NVARCHAR\(MAX\)\b/i, "obilet_partner_id NVARCHAR(255)")
      .replace(/\bbiletall_partner_id\s+NVARCHAR\(MAX\)\b/i, "biletall_partner_id NVARCHAR(255)")
      .replace(/\burl\s+NVARCHAR\(MAX\)\b/i, "url NVARCHAR(2048)")
      .replace(/\bobus_merkez_sube_id\s+NVARCHAR\(MAX\)\b/i, "obus_merkez_sube_id NVARCHAR(255)");
  }

  return text;
}

function transformCreateTableIfNotExists(sqlText) {
  const trimmed = String(sqlText || "").trim().replace(/;$/, "");
  const match = trimmed.match(/^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][\w]*)\s*\(([\s\S]+)\)$/i);
  if (!match) return sqlText;
  const table = match[1];
  const body = normalizeCreateTableBodyByTable(table, normalizeSqlTypes(match[2]));
  return `IF OBJECT_ID(N'${table}', N'U') IS NULL BEGIN CREATE TABLE ${table} (${body}) END`;
}

function transformCreateIndexIfNotExists(sqlText) {
  const trimmed = String(sqlText || "").trim().replace(/;$/, "");
  const match = trimmed.match(
    /^CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][\w]*)\s+ON\s+([a-zA-Z_][\w]*)\s*\(([\s\S]+)\)$/i
  );
  if (!match) return sqlText;
  const indexName = match[1];
  const tableName = match[2];
  const columns = match[3].trim();
  return [
    `IF NOT EXISTS (`,
    `  SELECT 1`,
    `  FROM sys.indexes`,
    `  WHERE name = '${indexName}'`,
    `    AND object_id = OBJECT_ID('${tableName}')`,
    `)`,
    `BEGIN`,
    `  CREATE INDEX ${indexName} ON ${tableName} (${columns})`,
    `END`
  ].join("\n");
}

function transformAlterAddColumnIfNotExists(sqlText) {
  const trimmed = String(sqlText || "").trim().replace(/;$/, "");
  if (!/^ALTER\s+TABLE\s+/i.test(trimmed) || !/ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS/i.test(trimmed)) {
    return sqlText;
  }

  const headerMatch = trimmed.match(/^ALTER\s+TABLE\s+([a-zA-Z_][\w]*)\s+([\s\S]+)$/i);
  if (!headerMatch) return sqlText;
  const tableName = headerMatch[1];
  const body = headerMatch[2];
  const parts = body
    .split(/,\s*(?=ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS)/i)
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  const statements = [];
  for (const part of parts) {
    const colMatch = part.match(/^ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+([a-zA-Z_][\w]*)\s+([\s\S]+)$/i);
    if (!colMatch) continue;
    const column = colMatch[1];
    const definition = normalizeSqlTypes(colMatch[2]);
    statements.push(
      `IF COL_LENGTH('${tableName}', '${column}') IS NULL BEGIN ALTER TABLE ${tableName} ADD ${column} ${definition} END`
    );
  }
  return statements.length > 0 ? statements.join("\n") : sqlText;
}

function transformSqlForMssql(rawText) {
  let sqlText = String(rawText || "");

  sqlText = transformCreateTableIfNotExists(sqlText);
  sqlText = transformCreateIndexIfNotExists(sqlText);
  sqlText = transformAlterAddColumnIfNotExists(sqlText);
  sqlText = normalizeSqlTypes(sqlText);

  sqlText = sqlText
    .replace(/::int\b/gi, "")
    .replace(/::text\[\]/gi, "")
    .replace(/::int\[\]/gi, "")
    .replace(/COUNT\(\*\)\s*::\s*int/gi, "CAST(COUNT(*) AS INT)")
    .replace(/\bTRUE\b/gi, "1")
    .replace(/\bFALSE\b/gi, "0");

  sqlText = sqlText.replace(/\$([0-9]+)/g, "@p$1");
  sqlText = sqlText.replace(/\bLIMIT\s+@p([0-9]+)\b/gi, "OFFSET 0 ROWS FETCH NEXT @p$1 ROWS ONLY");
  sqlText = sqlText.replace(/\bLIMIT\s+([0-9]+)\b/gi, "OFFSET 0 ROWS FETCH NEXT $1 ROWS ONLY");

  return sqlText;
}

function normalizeResult(result) {
  const rows = Array.isArray(result?.recordset) ? result.recordset : [];
  const rowsAffectedRaw = Array.isArray(result?.rowsAffected) ? result.rowsAffected : [];
  const rowCount = rows.length > 0 ? rows.length : rowsAffectedRaw.reduce((sum, item) => sum + Number(item || 0), 0);
  return { rows, rowCount };
}

function buildRequest(executor, params = []) {
  const request = new sql.Request(executor);
  if (!Array.isArray(params)) return request;
  params.forEach((value, index) => {
    request.input(`p${index + 1}`, value === undefined ? null : value);
  });
  return request;
}

class MssqlClient {
  constructor(rootPool) {
    this.rootPool = rootPool;
    this.transaction = null;
  }

  async query(text, params = []) {
    const normalized = String(text || "").trim().toUpperCase();
    if (normalized === "BEGIN" || normalized === "BEGIN TRANSACTION") {
      if (!this.transaction) {
        this.transaction = new sql.Transaction(this.rootPool);
        await this.transaction.begin();
      }
      return { rows: [], rowCount: 0 };
    }
    if (normalized === "COMMIT" || normalized === "COMMIT TRANSACTION") {
      if (this.transaction) {
        await this.transaction.commit();
        this.transaction = null;
      }
      return { rows: [], rowCount: 0 };
    }
    if (normalized === "ROLLBACK" || normalized === "ROLLBACK TRANSACTION") {
      if (this.transaction) {
        await this.transaction.rollback();
        this.transaction = null;
      }
      return { rows: [], rowCount: 0 };
    }

    const sqlText = transformSqlForMssql(text);
    const executor = this.transaction || this.rootPool;
    const request = buildRequest(executor, params);
    const result = await request.query(sqlText);
    return normalizeResult(result);
  }

  release() {
    // No-op. Pool/transaction lifecycle is handled by query commands.
  }
}

class MssqlPool {
  constructor(config) {
    this.config = config;
    this.pool = new sql.ConnectionPool(config);
    this.poolPromise = null;
  }

  async ensureConnected() {
    if (!this.poolPromise) {
      this.poolPromise = this.pool.connect();
    }
    await this.poolPromise;
  }

  async query(text, params = []) {
    await this.ensureConnected();
    const client = new MssqlClient(this.pool);
    return client.query(text, params);
  }

  async connect() {
    await this.ensureConnected();
    return new MssqlClient(this.pool);
  }

  async end() {
    await this.pool.close();
  }
}

function normalizePgResult(result) {
  const rows = Array.isArray(result?.rows) ? result.rows : [];
  const rowCount = Number.isFinite(Number(result?.rowCount)) ? Number(result.rowCount) : rows.length;
  return { rows, rowCount };
}

function buildPgPoolConfig(connectionString) {
  const config = {
    connectionString: String(connectionString || "").trim(),
    max: parseOptionalInt(process.env.DATABASE_POOL_MAX, 10),
    idleTimeoutMillis: parseOptionalInt(process.env.DATABASE_POOL_IDLE_TIMEOUT_MS, 30000)
  };

  const hasDatabaseSslFlag = process.env.DATABASE_SSL !== undefined;
  const hasRejectUnauthorizedFlag = process.env.DATABASE_SSL_REJECT_UNAUTHORIZED !== undefined;
  if (!hasDatabaseSslFlag && !hasRejectUnauthorizedFlag) {
    return config;
  }

  const sslEnabled = parseBooleanFlag(process.env.DATABASE_SSL, true);
  if (!sslEnabled) {
    config.ssl = false;
    return config;
  }

  config.ssl = {
    rejectUnauthorized: parseBooleanFlag(process.env.DATABASE_SSL_REJECT_UNAUTHORIZED, false)
  };
  return config;
}

class PostgresClient {
  constructor(client) {
    this.client = client;
  }

  async query(text, params = []) {
    const result = await this.client.query(String(text || ""), Array.isArray(params) ? params : []);
    return normalizePgResult(result);
  }

  release() {
    if (this.client) {
      this.client.release();
    }
  }
}

class PostgresPool {
  constructor(connectionString) {
    this.pool = new PgDriverPool(buildPgPoolConfig(connectionString));
  }

  async query(text, params = []) {
    const result = await this.pool.query(String(text || ""), Array.isArray(params) ? params : []);
    return normalizePgResult(result);
  }

  async connect() {
    const client = await this.pool.connect();
    return new PostgresClient(client);
  }

  async end() {
    await this.pool.end();
  }
}

function createDatabasePool(connectionString) {
  const engine = detectDatabaseEngine(connectionString);
  if (engine === "postgres") {
    return new PostgresPool(connectionString);
  }

  const config = parseMssqlConnectionString(connectionString);
  return new MssqlPool(config);
}

module.exports = {
  createDatabasePool
};
