#!/usr/bin/env node

"use strict";

const { Pool } = require("pg");
const {
  DEFAULT_SOURCE_STATE_ROW_ID,
  buildRecordsV2TableRefsFromEnv,
  computeRowsChecksum,
  ensureClientRecordsV2Schema,
  normalizeIsoTimestamp,
  normalizeLegacyRecordsSnapshot,
  sanitizeTextValue,
} = require("../client-records-v2-utils");

const DEFAULT_BATCH_SIZE = 300;

function parseArgs(argv) {
  const options = {
    dryRun: false,
    sourceRowId: DEFAULT_SOURCE_STATE_ROW_ID,
    deleteMissing: true,
    batchSize: DEFAULT_BATCH_SIZE,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (arg === "--keep-missing") {
      options.deleteMissing = false;
      continue;
    }

    if (arg === "--source-row-id") {
      const rawValue = argv[index + 1] || "";
      const parsed = Number.parseInt(rawValue, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error("Invalid --source-row-id value. Use a positive integer.");
      }
      options.sourceRowId = parsed;
      index += 1;
      continue;
    }

    if (arg === "--batch-size") {
      const rawValue = argv[index + 1] || "";
      const parsed = Number.parseInt(rawValue, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error("Invalid --batch-size value. Use a positive integer.");
      }
      options.batchSize = parsed;
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return options;
}

function buildBatchInsertQuery(tableName, rows) {
  const columns = [
    "id",
    "record",
    "record_hash",
    "client_name",
    "company_name",
    "closed_by",
    "created_at",
    "source_state_updated_at",
    "source_state_row_id",
    "inserted_at",
    "updated_at",
  ];

  const values = [];
  const placeholders = [];
  let paramIndex = 1;

  for (const row of rows) {
    placeholders.push(
      `($${paramIndex}, $${paramIndex + 1}::jsonb, $${paramIndex + 2}, $${paramIndex + 3}, ` +
        `$${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}::timestamptz, ` +
        `$${paramIndex + 7}::timestamptz, $${paramIndex + 8}, NOW(), NOW())`,
    );
    values.push(
      row.id,
      JSON.stringify(row.record),
      row.recordHash,
      row.clientName,
      row.companyName,
      row.closedBy,
      row.createdAt,
      row.sourceStateUpdatedAt,
      row.sourceStateRowId,
    );
    paramIndex += 9;
  }

  const sql = `
    INSERT INTO ${tableName} AS target
      (${columns.join(", ")})
    VALUES
      ${placeholders.join(", ")}
    ON CONFLICT (id)
    DO UPDATE SET
      record = EXCLUDED.record,
      record_hash = EXCLUDED.record_hash,
      client_name = EXCLUDED.client_name,
      company_name = EXCLUDED.company_name,
      closed_by = EXCLUDED.closed_by,
      created_at = EXCLUDED.created_at,
      source_state_updated_at = EXCLUDED.source_state_updated_at,
      source_state_row_id = EXCLUDED.source_state_row_id,
      updated_at = NOW()
    WHERE
      (target.record_hash, target.client_name, target.company_name, target.closed_by, target.created_at, target.source_state_updated_at, target.source_state_row_id)
      IS DISTINCT FROM
      (EXCLUDED.record_hash, EXCLUDED.client_name, EXCLUDED.company_name, EXCLUDED.closed_by, EXCLUDED.created_at, EXCLUDED.source_state_updated_at, EXCLUDED.source_state_row_id)
    RETURNING (xmax = 0) AS inserted
  `;

  return { sql, values };
}

async function backfillClientRecordsV2(pool, options) {
  const refs = buildRecordsV2TableRefsFromEnv(process.env);
  const client = await pool.connect();
  const sourceRowId = options.sourceRowId;

  try {
    await client.query("BEGIN");
    await ensureClientRecordsV2Schema(client, refs);

    const stateResult = await client.query(
      `SELECT records, updated_at FROM ${refs.stateTable} WHERE id = $1 LIMIT 1`,
      [sourceRowId],
    );

    const stateRow = stateResult.rows[0] || null;
    const legacyRecords = Array.isArray(stateRow?.records) ? stateRow.records : [];
    const sourceStateUpdatedAt = normalizeIsoTimestamp(stateRow?.updated_at);
    const snapshot = normalizeLegacyRecordsSnapshot(legacyRecords, {
      sourceStateUpdatedAt,
      sourceStateRowId: sourceRowId,
    });

    let insertedCount = 0;
    let updatedCount = 0;
    let unchangedCount = 0;

    for (let index = 0; index < snapshot.rows.length; index += options.batchSize) {
      const batch = snapshot.rows.slice(index, index + options.batchSize);
      if (!batch.length) {
        continue;
      }
      const { sql, values } = buildBatchInsertQuery(refs.recordsV2Table, batch);
      const result = await client.query(sql, values);
      let inserted = 0;
      let updated = 0;
      for (const row of result.rows) {
        if (row?.inserted) {
          inserted += 1;
        } else {
          updated += 1;
        }
      }
      insertedCount += inserted;
      updatedCount += updated;
      unchangedCount += batch.length - inserted - updated;
    }

    let deletedCount = 0;
    if (options.deleteMissing) {
      if (snapshot.rows.length) {
        const activeIds = snapshot.rows.map((row) => row.id);
        const deleteResult = await client.query(
          `
            DELETE FROM ${refs.recordsV2Table}
            WHERE source_state_row_id = $1
              AND NOT (id = ANY($2::text[]))
          `,
          [sourceRowId, activeIds],
        );
        deletedCount = deleteResult.rowCount || 0;
      } else {
        const deleteResult = await client.query(
          `
            DELETE FROM ${refs.recordsV2Table}
            WHERE source_state_row_id = $1
          `,
          [sourceRowId],
        );
        deletedCount = deleteResult.rowCount || 0;
      }
    }

    const v2CountResult = await client.query(
      `SELECT COUNT(*)::bigint AS total FROM ${refs.recordsV2Table} WHERE source_state_row_id = $1`,
      [sourceRowId],
    );
    const v2Count = Number.parseInt(v2CountResult.rows[0]?.total, 10) || 0;

    if (options.dryRun) {
      await client.query("ROLLBACK");
    } else {
      await client.query("COMMIT");
    }

    return {
      ok: true,
      dryRun: options.dryRun,
      deleteMissing: options.deleteMissing,
      sourceRowId,
      sourceStateUpdatedAt,
      legacyRecordCountRaw: legacyRecords.length,
      legacyRecordCountNormalized: snapshot.rows.length,
      v2RecordCount: v2Count,
      insertedCount,
      updatedCount,
      unchangedCount,
      deletedCount,
      skippedInvalidRecordCount: snapshot.skippedInvalidRecordCount,
      skippedMissingIdCount: snapshot.skippedMissingIdCount,
      duplicateIdCount: snapshot.duplicateIdCount,
      legacyChecksum: computeRowsChecksum(snapshot.rows),
      table: refs.recordsV2Table,
      batchSize: options.batchSize,
    };
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // Best-effort rollback.
    }
    throw error;
  } finally {
    client.release();
  }
}

(async function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const databaseUrl = sanitizeTextValue(process.env.DATABASE_URL, 2000);
    if (!databaseUrl) {
      throw new Error("DATABASE_URL is required.");
    }

    const pool = new Pool({
      connectionString: databaseUrl,
      ssl: { rejectUnauthorized: false },
    });

    try {
      const report = await backfillClientRecordsV2(pool, options);
      process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    } finally {
      await pool.end();
    }
  } catch (error) {
    process.stderr.write(`Error: ${sanitizeTextValue(error?.message, 1000) || "Unknown error"}\n`);
    process.exit(1);
  }
})();
