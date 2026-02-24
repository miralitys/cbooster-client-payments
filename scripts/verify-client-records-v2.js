#!/usr/bin/env node

const { Pool } = require("pg");
const {
  DEFAULT_SOURCE_STATE_ROW_ID,
  buildRecordsV2TableRefsFromEnv,
  computeRecordHash,
  computeRowsChecksum,
  ensureClientRecordsV2Schema,
  normalizeLegacyRecordsSnapshot,
  sanitizeTextValue,
} = require("../client-records-v2-utils");
const { createPgSslConfig } = require("../server/shared/db/pool");

function parseArgs(argv) {
  const options = {
    sourceRowId: DEFAULT_SOURCE_STATE_ROW_ID,
    maxDiffItems: 25,
    failOnMismatch: true,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--source-row-id") {
      const parsed = Number.parseInt(argv[index + 1] || "", 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error("Invalid --source-row-id value. Use a positive integer.");
      }
      options.sourceRowId = parsed;
      index += 1;
      continue;
    }

    if (arg === "--max-diff-items") {
      const parsed = Number.parseInt(argv[index + 1] || "", 10);
      if (!Number.isFinite(parsed) || parsed <= 0 || parsed > 500) {
        throw new Error("Invalid --max-diff-items value. Use integer in range 1..500.");
      }
      options.maxDiffItems = parsed;
      index += 1;
      continue;
    }

    if (arg === "--no-fail") {
      options.failOnMismatch = false;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function buildLimitedList(items, maxItems) {
  const list = Array.isArray(items) ? items : [];
  if (list.length <= maxItems) {
    return list;
  }

  return [...list.slice(0, maxItems), `... +${list.length - maxItems} more`];
}

function normalizeV2Rows(rawRows) {
  const list = Array.isArray(rawRows) ? rawRows : [];
  const rows = [];
  for (const item of list) {
    const id = sanitizeTextValue(item?.id, 180);
    if (!id) {
      continue;
    }

    const record = item?.record && typeof item.record === "object" && !Array.isArray(item.record)
      ? item.record
      : {};
    const computedHash = computeRecordHash(record);
    const storedHash = sanitizeTextValue(item?.record_hash, 128).toLowerCase();
    rows.push({
      id,
      recordHash: computedHash,
      storedHash,
      storedHashMatches: storedHash ? storedHash === computedHash : false,
    });
  }

  rows.sort((left, right) => left.id.localeCompare(right.id));
  return rows;
}

function compareSnapshots(legacyRows, v2Rows, maxDiffItems) {
  const legacyMap = new Map(legacyRows.map((row) => [row.id, row]));
  const v2Map = new Map(v2Rows.map((row) => [row.id, row]));

  const missingInV2 = [];
  const extraInV2 = [];
  const hashMismatch = [];
  const storedHashMismatch = [];

  for (const [id, legacyRow] of legacyMap.entries()) {
    const v2Row = v2Map.get(id);
    if (!v2Row) {
      missingInV2.push(id);
      continue;
    }

    if (legacyRow.recordHash !== v2Row.recordHash) {
      hashMismatch.push(id);
    }
  }

  for (const [id, v2Row] of v2Map.entries()) {
    if (!legacyMap.has(id)) {
      extraInV2.push(id);
    }

    if (!v2Row.storedHashMatches) {
      storedHashMismatch.push(id);
    }
  }

  const legacyChecksum = computeRowsChecksum(legacyRows);
  const v2Checksum = computeRowsChecksum(v2Rows);

  const countsMatch = legacyRows.length === v2Rows.length;
  const checksumsMatch = legacyChecksum === v2Checksum;
  const ok =
    countsMatch &&
    checksumsMatch &&
    missingInV2.length === 0 &&
    extraInV2.length === 0 &&
    hashMismatch.length === 0 &&
    storedHashMismatch.length === 0;

  return {
    ok,
    countsMatch,
    checksumsMatch,
    legacyCount: legacyRows.length,
    v2Count: v2Rows.length,
    legacyChecksum,
    v2Checksum,
    missingInV2: buildLimitedList(missingInV2, maxDiffItems),
    extraInV2: buildLimitedList(extraInV2, maxDiffItems),
    hashMismatch: buildLimitedList(hashMismatch, maxDiffItems),
    storedHashMismatch: buildLimitedList(storedHashMismatch, maxDiffItems),
  };
}

(async function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const databaseUrl = sanitizeTextValue(process.env.DATABASE_URL, 2000);
    if (!databaseUrl) {
      throw new Error("DATABASE_URL is required.");
    }

    const refs = buildRecordsV2TableRefsFromEnv(process.env);

    const pool = new Pool({
      connectionString: databaseUrl,
      ssl: createPgSslConfig({}, process.env),
    });

    try {
      await ensureClientRecordsV2Schema(pool, refs);

      const stateResult = await pool.query(
        `SELECT records, updated_at FROM ${refs.stateTable} WHERE id = $1 LIMIT 1`,
        [options.sourceRowId],
      );
      const stateRow = stateResult.rows[0] || null;
      const legacyRecords = Array.isArray(stateRow?.records) ? stateRow.records : [];
      const legacySnapshot = normalizeLegacyRecordsSnapshot(legacyRecords, {
        sourceStateUpdatedAt: stateRow?.updated_at,
        sourceStateRowId: options.sourceRowId,
      });

      const v2Result = await pool.query(
        `
          SELECT id, record, record_hash
          FROM ${refs.recordsV2Table}
          WHERE source_state_row_id = $1
          ORDER BY id ASC
        `,
        [options.sourceRowId],
      );

      const v2Rows = normalizeV2Rows(v2Result.rows);
      const compare = compareSnapshots(legacySnapshot.rows, v2Rows, options.maxDiffItems);

      const report = {
        ok: compare.ok,
        sourceRowId: options.sourceRowId,
        table: refs.recordsV2Table,
        sourceStateUpdatedAt: stateRow?.updated_at ? new Date(stateRow.updated_at).toISOString() : null,
        skippedInvalidRecordCount: legacySnapshot.skippedInvalidRecordCount,
        skippedMissingIdCount: legacySnapshot.skippedMissingIdCount,
        duplicateIdCount: legacySnapshot.duplicateIdCount,
        ...compare,
      };

      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);

      if (!compare.ok && options.failOnMismatch) {
        process.exit(2);
      }
    } finally {
      await pool.end();
    }
  } catch (error) {
    process.stderr.write(`Error: ${sanitizeTextValue(error?.message, 1000) || "Unknown error"}\n`);
    process.exit(1);
  }
})();
