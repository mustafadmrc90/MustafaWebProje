#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const { createDatabasePool } = require("../db");

const TARGET_ID = "691";
const TARGET_SOURCE = "cluster3";
const WRONG_CODE = "cagdashatayseyehat";
const CORRECT_CODE = "cagdasbingolseyahat";

function loadLocalEnvFile(filePath = path.join(__dirname, "..", ".env")) {
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
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
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
}

async function fetchTargetRows(pool) {
  const result = await pool.query(
    `
      SELECT
        id,
        code,
        source,
        obilet_partner_id,
        biletall_partner_id,
        url,
        obus_merkez_sube_id,
        obus_merkez_sube_id_debug,
        updated_at
      FROM all_companies_cache
      WHERE id = $1
        AND source = $2
        AND code IN ($3, $4)
      ORDER BY code ASC
    `,
    [TARGET_ID, TARGET_SOURCE, WRONG_CODE, CORRECT_CODE]
  );
  return result.rows || [];
}

async function fixCode(pool) {
  const beforeRows = await fetchTargetRows(pool);
  const wrongRow = beforeRows.find((row) => String(row.code || "").trim() === WRONG_CODE);
  const correctRow = beforeRows.find((row) => String(row.code || "").trim() === CORRECT_CODE);

  if (!wrongRow && correctRow) {
    return { changed: false, message: "Kayıt zaten doğru.", beforeRows, afterRows: beforeRows };
  }
  if (!wrongRow) {
    return { changed: false, message: "Düzeltilecek yanlış kayıt bulunamadı.", beforeRows, afterRows: beforeRows };
  }

  if (!correctRow) {
    await pool.query(
      `
        UPDATE all_companies_cache
        SET code = $1,
            updated_at = now()
        WHERE id = $2
          AND source = $3
          AND code = $4
      `,
      [CORRECT_CODE, TARGET_ID, TARGET_SOURCE, WRONG_CODE]
    );
  } else {
    const mergedBranchId =
      String(correctRow.obus_merkez_sube_id || "").trim() ||
      String(wrongRow.obus_merkez_sube_id || "").trim();
    const mergedDebug =
      String(correctRow.obus_merkez_sube_id_debug || "").trim() ||
      String(wrongRow.obus_merkez_sube_id_debug || "").trim();

    await pool.query(
      `
        UPDATE all_companies_cache
        SET obus_merkez_sube_id = $1,
            obus_merkez_sube_id_debug = $2,
            updated_at = now()
        WHERE id = $3
          AND source = $4
          AND code = $5
      `,
      [mergedBranchId || null, mergedDebug || null, TARGET_ID, TARGET_SOURCE, CORRECT_CODE]
    );
    await pool.query(
      `
        DELETE FROM all_companies_cache
        WHERE id = $1
          AND source = $2
          AND code = $3
      `,
      [TARGET_ID, TARGET_SOURCE, WRONG_CODE]
    );
  }

  return {
    changed: true,
    message: "Kayıt düzeltildi.",
    beforeRows,
    afterRows: await fetchTargetRows(pool)
  };
}

async function main() {
  loadLocalEnvFile();
  if (!String(process.env.DATABASE_URL || "").trim()) {
    throw new Error(".env içinde DATABASE_URL yok.");
  }

  const pool = createDatabasePool(process.env.DATABASE_URL);
  try {
    const result = await fixCode(pool);
    console.log(result.message);
    console.log(JSON.stringify({ changed: result.changed, before: result.beforeRows, after: result.afterRows }, null, 2));
  } finally {
    if (pool && typeof pool.end === "function") {
      await pool.end();
    }
  }
}

main().catch((err) => {
  console.error(`Hata: ${err?.message || err}`);
  process.exit(1);
});
