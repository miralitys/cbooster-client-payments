"use strict";

function createRecordsRepo(dependencies = {}) {
  const {
    db,
    ensureDatabaseReady,
    tables,
    flags,
    helpers,
    metrics,
    performanceObservability,
    logger,
  } = dependencies;

  const pool = db?.pool || null;
  const createClientQuery =
    typeof db?.createClientQuery === "function"
      ? db.createClientQuery
      : (client) => {
          if (client && typeof client.query === "function") {
            return client.query.bind(client);
          }
          return null;
        };

  const dbUnavailableErrorFactory = () => {
    const error = new Error("Database is not configured. Set DATABASE_URL.");
    error.code = "db_not_configured";
    return error;
  };
  const query =
    typeof db?.query === "function"
      ? db.query
      : async () => {
          throw dbUnavailableErrorFactory();
        };
  const runInTransaction =
    typeof db?.tx === "function"
      ? db.tx
      : async () => {
          throw dbUnavailableErrorFactory();
        };

  const STATE_TABLE = tables?.stateTable;
  const STATE_ROW_ID = tables?.stateRowId;
  const CLIENT_RECORDS_V2_TABLE = tables?.clientRecordsV2Table;

  const DUAL_WRITE_V2_ENABLED = flags?.dualWriteV2Enabled === true;
  const DUAL_READ_COMPARE_ENABLED = flags?.dualReadCompareEnabled === true;
  const WRITE_V2_ENABLED = flags?.writeV2Enabled === true;
  const LEGACY_MIRROR_ENABLED = flags?.legacyMirrorEnabled === true;

  const {
    normalizeRecordStateTimestamp,
    normalizeLegacyRecordsSnapshot,
    computeRecordHash,
    computeRowsChecksum,
    sanitizeTextValue,
    createHttpError,
    isRecordStateRevisionMatch,
    applyRecordsPatchOperations,
    normalizeDualWriteSummaryValue,
    buildDualReadCompareSummaryPayload,
    buildDualWriteSummaryPayload,
  } = helpers || {};

  const {
    recordDualReadCompareAttempt,
    recordDualReadCompareMismatch,
    recordDualReadCompareSuccess,
    recordDualWriteAttempt,
    recordDualWriteSuccess,
    recordDualWriteFailure,
  } = metrics || {};

  const log = logger || console;
  let legacyStateUpdatedAtModeCache = "";

  if (typeof ensureDatabaseReady !== "function") {
    throw new Error("createRecordsRepo requires ensureDatabaseReady()");
  }

  function resolveRecordsV2UpdatedAt(candidateValues) {
    const values = Array.isArray(candidateValues) ? candidateValues : [];
    let maxTimestamp = null;
    for (const value of values) {
      const normalized = normalizeRecordStateTimestamp(value);
      if (normalized === null) {
        continue;
      }
      if (maxTimestamp === null || normalized > maxTimestamp) {
        maxTimestamp = normalized;
      }
    }
    if (maxTimestamp === null) {
      return null;
    }
    return new Date(maxTimestamp).toISOString();
  }

  function normalizeUpdatedAtForApi(rawValue) {
    const timestamp = normalizeRecordStateTimestamp(rawValue);
    if (timestamp === null) {
      return null;
    }
    return new Date(timestamp).toISOString();
  }

  function parseQualifiedTableName(rawValue) {
    const value = sanitizeTextValue(rawValue, 240);
    if (!value) {
      return null;
    }

    const quotedSchemaMatch = value.match(/^"([^"]+)"\."([^"]+)"$/);
    if (quotedSchemaMatch) {
      return {
        schemaName: sanitizeTextValue(quotedSchemaMatch[1], 120),
        tableName: sanitizeTextValue(quotedSchemaMatch[2], 120),
      };
    }

    const plainSchemaMatch = value.match(/^([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)$/);
    if (plainSchemaMatch) {
      return {
        schemaName: sanitizeTextValue(plainSchemaMatch[1], 120),
        tableName: sanitizeTextValue(plainSchemaMatch[2], 120),
      };
    }

    const quotedTableMatch = value.match(/^"([^"]+)"$/);
    if (quotedTableMatch) {
      return {
        schemaName: "public",
        tableName: sanitizeTextValue(quotedTableMatch[1], 120),
      };
    }

    const plainTableMatch = value.match(/^([a-zA-Z_][a-zA-Z0-9_]*)$/);
    if (plainTableMatch) {
      return {
        schemaName: "public",
        tableName: sanitizeTextValue(plainTableMatch[1], 120),
      };
    }

    return null;
  }

  async function resolveLegacyStateUpdatedAtMode(queryClient) {
    if (legacyStateUpdatedAtModeCache) {
      return legacyStateUpdatedAtModeCache;
    }

    const parsedStateTable = parseQualifiedTableName(STATE_TABLE);
    if (!parsedStateTable?.schemaName || !parsedStateTable?.tableName) {
      legacyStateUpdatedAtModeCache = "timestamp";
      return legacyStateUpdatedAtModeCache;
    }

    try {
      const result = await queryClient(
        `
          SELECT udt_name
          FROM information_schema.columns
          WHERE table_schema = $1
            AND table_name = $2
            AND column_name = 'updated_at'
          LIMIT 1
        `,
        [parsedStateTable.schemaName, parsedStateTable.tableName],
      );
      const udtName = sanitizeTextValue(result.rows[0]?.udt_name, 40).toLowerCase();
      legacyStateUpdatedAtModeCache = udtName === "int8" ? "bigint" : "timestamp";
    } catch (error) {
      log.warn?.(
        `[records] Failed to resolve ${STATE_TABLE}.updated_at type, falling back to timestamp: ${sanitizeTextValue(error?.message, 320) || "unknown error"}`,
      );
      legacyStateUpdatedAtModeCache = "timestamp";
    }

    return legacyStateUpdatedAtModeCache;
  }

  async function upsertLegacyStateRecords(queryClient, records, writeTimestamp) {
    const updatedAtMode = await resolveLegacyStateUpdatedAtMode(queryClient);
    const normalizedWriteTimestamp = normalizeSourceStateUpdatedAtForV2(writeTimestamp) || new Date().toISOString();
    const parsedWriteTimestamp = Date.parse(normalizedWriteTimestamp);
    const writeTimestampMs = Number.isFinite(parsedWriteTimestamp) ? Math.floor(parsedWriteTimestamp) : Date.now();

    if (updatedAtMode === "bigint") {
      const result = await queryClient(
        `
          INSERT INTO ${STATE_TABLE} (id, records, updated_at)
          VALUES ($1, $2::jsonb, $3::bigint)
          ON CONFLICT (id)
          DO UPDATE SET records = EXCLUDED.records, updated_at = EXCLUDED.updated_at
          RETURNING updated_at
        `,
        [STATE_ROW_ID, JSON.stringify(records), writeTimestampMs],
      );
      return normalizeUpdatedAtForApi(result.rows[0]?.updated_at) || normalizedWriteTimestamp;
    }

    const result = await queryClient(
      `
        INSERT INTO ${STATE_TABLE} (id, records, updated_at)
        VALUES ($1, $2::jsonb, $3::timestamptz)
        ON CONFLICT (id)
        DO UPDATE SET records = EXCLUDED.records, updated_at = EXCLUDED.updated_at
        RETURNING updated_at
      `,
      [STATE_ROW_ID, JSON.stringify(records), normalizedWriteTimestamp],
    );
    return normalizeUpdatedAtForApi(result.rows[0]?.updated_at) || normalizedWriteTimestamp;
  }

  function normalizeRecordFromV2Row(rawValue) {
    if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
      return null;
    }
    return rawValue;
  }

  async function getStoredRecords() {
    await ensureDatabaseReady();
    const result = await query(`SELECT records, updated_at FROM ${STATE_TABLE} WHERE id = $1`, [STATE_ROW_ID]);

    if (!result.rows.length) {
      return { records: [], updatedAt: null };
    }

    const row = result.rows[0];
    return {
      records: Array.isArray(row.records) ? row.records : [],
      updatedAt: normalizeUpdatedAtForApi(row.updated_at),
    };
  }

  async function getStoredRecordsFromV2() {
    await ensureDatabaseReady();
    const stateResult = await query(`SELECT updated_at FROM ${STATE_TABLE} WHERE id = $1`, [STATE_ROW_ID]);
    const stateUpdatedAt = stateResult.rows[0]?.updated_at || null;
    const result = await query(
      `
        SELECT id, record, source_state_updated_at, updated_at
        FROM ${CLIENT_RECORDS_V2_TABLE}
        WHERE source_state_row_id = $1
        ORDER BY id ASC
      `,
      [STATE_ROW_ID],
    );

    const records = [];
    const updatedAtCandidates = [];

    for (const row of result.rows) {
      const record = normalizeRecordFromV2Row(row?.record);
      if (record) {
        records.push(record);
      }
      updatedAtCandidates.push(row?.source_state_updated_at, row?.updated_at);
    }
    updatedAtCandidates.push(stateUpdatedAt);

    return {
      records,
      updatedAt: resolveRecordsV2UpdatedAt(updatedAtCandidates),
    };
  }

  async function getStoredRecordsHeadRevision() {
    await ensureDatabaseReady();
    const revisionResult = await query(`SELECT updated_at FROM ${STATE_TABLE} WHERE id = $1`, [STATE_ROW_ID]);
    return normalizeUpdatedAtForApi(revisionResult.rows[0]?.updated_at);
  }

  function normalizeV2RowForDualReadCompare(rawRow) {
    const id = sanitizeTextValue(rawRow?.id, 180);
    if (!id) {
      return null;
    }

    const record = rawRow?.record && typeof rawRow.record === "object" && !Array.isArray(rawRow.record) ? rawRow.record : {};
    const recordHash = computeRecordHash(record);
    const storedHash = sanitizeTextValue(rawRow?.record_hash, 128).toLowerCase();

    return {
      id,
      recordHash,
      storedHashMatches: storedHash ? storedHash === recordHash : false,
    };
  }

  function compareLegacyAndV2RecordSnapshots(legacyRows, v2Rows, options = {}) {
    const source = sanitizeTextValue(options.source, 80) || "GET /api/records";
    const maxSampleIds = Math.min(Math.max(Number.parseInt(options.maxSampleIds, 10) || 20, 1), 50);

    const normalizedLegacyRows = Array.isArray(legacyRows) ? legacyRows : [];
    const normalizedV2Rows = Array.isArray(v2Rows) ? v2Rows : [];
    const legacyMap = new Map(normalizedLegacyRows.map((row) => [row.id, row.recordHash]));
    const v2Map = new Map(normalizedV2Rows.map((row) => [row.id, row.recordHash]));

    const missingInV2Ids = [];
    const extraInV2Ids = [];
    const hashMismatchIds = [];
    let v2StoredHashMismatchCount = 0;

    for (const [id, legacyHash] of legacyMap.entries()) {
      if (!v2Map.has(id)) {
        missingInV2Ids.push(id);
        continue;
      }
      const v2Hash = v2Map.get(id);
      if (legacyHash !== v2Hash) {
        hashMismatchIds.push(id);
      }
    }

    for (const row of normalizedV2Rows) {
      if (!legacyMap.has(row.id)) {
        extraInV2Ids.push(row.id);
      }
      if (row.storedHashMatches === false) {
        v2StoredHashMismatchCount += 1;
      }
    }

    const summary = {
      source,
      legacyCount: normalizedLegacyRows.length,
      v2Count: normalizedV2Rows.length,
      legacyChecksum: computeRowsChecksum(normalizedLegacyRows),
      v2Checksum: computeRowsChecksum(normalizedV2Rows),
      missingInV2Count: missingInV2Ids.length,
      extraInV2Count: extraInV2Ids.length,
      hashMismatchCount: hashMismatchIds.length,
      v2StoredHashMismatchCount,
      missingInV2SampleIds: missingInV2Ids.slice(0, maxSampleIds),
      extraInV2SampleIds: extraInV2Ids.slice(0, maxSampleIds),
      hashMismatchSampleIds: hashMismatchIds.slice(0, maxSampleIds),
    };

    const mismatchDetected =
      summary.legacyCount !== summary.v2Count ||
      summary.legacyChecksum !== summary.v2Checksum ||
      summary.missingInV2Count > 0 ||
      summary.extraInV2Count > 0 ||
      summary.hashMismatchCount > 0 ||
      summary.v2StoredHashMismatchCount > 0;

    return {
      mismatchDetected,
      summary,
    };
  }

  async function runDualReadCompareForLegacyRecords(records, options = {}) {
    if (!DUAL_READ_COMPARE_ENABLED || !pool) {
      return;
    }

    recordDualReadCompareAttempt?.(performanceObservability);
    const source = sanitizeTextValue(options.source, 80) || "GET /api/records";
    const requestedBy = sanitizeTextValue(options.requestedBy, 160);

    try {
      await ensureDatabaseReady();

      const legacySnapshot = normalizeLegacyRecordsSnapshot(records, {
        sourceStateRowId: STATE_ROW_ID,
      });

      const v2Result = await query(
        `
          SELECT id, record, record_hash
          FROM ${CLIENT_RECORDS_V2_TABLE}
          WHERE source_state_row_id = $1
          ORDER BY id ASC
        `,
        [STATE_ROW_ID],
      );

      const v2Rows = [];
      for (const row of v2Result.rows) {
        const normalizedRow = normalizeV2RowForDualReadCompare(row);
        if (!normalizedRow) {
          continue;
        }
        v2Rows.push(normalizedRow);
      }
      v2Rows.sort((left, right) => left.id.localeCompare(right.id));

      const compareResult = compareLegacyAndV2RecordSnapshots(legacySnapshot.rows, v2Rows, {
        source,
        maxSampleIds: 20,
      });

      if (compareResult.mismatchDetected) {
        const summary = {
          requestedBy,
          ...compareResult.summary,
        };
        recordDualReadCompareMismatch?.(performanceObservability, summary);
        log.warn?.("[records dual-read compare] mismatch detected:", buildDualReadCompareSummaryPayload(summary));
        return;
      }

      recordDualReadCompareSuccess?.(performanceObservability, {
        requestedBy,
        ...compareResult.summary,
      });
    } catch (error) {
      const message = sanitizeTextValue(error?.message, 600) || "unknown error";
      const code = sanitizeTextValue(error?.code, 80) || "no_code";
      log.warn?.(`[records dual-read compare] compare failed: ${code}: ${message}`);
    }
  }

  function normalizeSourceStateUpdatedAtForV2(rawValue) {
    if (!rawValue) {
      return null;
    }
    const parsed = Date.parse(rawValue);
    if (!Number.isFinite(parsed)) {
      return null;
    }
    return new Date(parsed).toISOString();
  }

  async function syncLegacyRecordsSnapshotToV2(queryClient, records, options = {}) {
    const sourceStateRowId = STATE_ROW_ID;
    const writeTimestamp = normalizeSourceStateUpdatedAtForV2(options.writeTimestamp) || new Date().toISOString();
    const sourceStateUpdatedAt = normalizeSourceStateUpdatedAtForV2(options.sourceStateUpdatedAt) || writeTimestamp;
    const snapshot = normalizeLegacyRecordsSnapshot(records, {
      sourceStateRowId,
      sourceStateUpdatedAt,
      writeTimestamp,
    });
    const rows = snapshot.rows;

    if (rows.length) {
      const values = [];
      const placeholders = rows.map((row, index) => {
        const base = index * 8;
        values.push(
          row.id,
          row.sourceStateRowId,
          row.sourceStateUpdatedAt,
          JSON.stringify(row.record),
          row.recordHash,
          row.createdAt,
          row.updatedAt,
          row.writeTimestamp,
        );
        return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}::jsonb, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8})`;
      });

      const result = await queryClient(
        `
          INSERT INTO ${CLIENT_RECORDS_V2_TABLE}
            (id, source_state_row_id, source_state_updated_at, record, record_hash, created_at, updated_at, write_timestamp)
          VALUES ${placeholders.join(", ")}
          ON CONFLICT (id)
          DO UPDATE
          SET
            source_state_row_id = EXCLUDED.source_state_row_id,
            source_state_updated_at = EXCLUDED.source_state_updated_at,
            record = EXCLUDED.record,
            record_hash = EXCLUDED.record_hash,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            write_timestamp = EXCLUDED.write_timestamp
          RETURNING id
        `,
        values,
      );
      snapshot.upsertedCount = normalizeDualWriteSummaryValue(result.rowCount);
    } else {
      snapshot.upsertedCount = 0;
    }

    const ids = snapshot.rows.map((row) => row.id);
    if (ids.length) {
      const deleteResult = await queryClient(
        `
          DELETE FROM ${CLIENT_RECORDS_V2_TABLE}
          WHERE source_state_row_id = $1
            AND id <> ALL($2::text[])
        `,
        [sourceStateRowId, ids],
      );
      snapshot.deletedCount = normalizeDualWriteSummaryValue(deleteResult.rowCount);
    } else {
      const deleteResult = await queryClient(
        `
          DELETE FROM ${CLIENT_RECORDS_V2_TABLE}
          WHERE source_state_row_id = $1
        `,
        [sourceStateRowId],
      );
      snapshot.deletedCount = normalizeDualWriteSummaryValue(deleteResult.rowCount);
    }

    const countResult = await queryClient(
      `
        SELECT COUNT(*)::bigint AS total
        FROM ${CLIENT_RECORDS_V2_TABLE}
        WHERE source_state_row_id = $1
      `,
      [sourceStateRowId],
    );
    const v2Count = normalizeDualWriteSummaryValue(countResult.rows[0]?.total);
    snapshot.v2Count = v2Count;
    snapshot.inSync = snapshot.expectedCount === v2Count;
    return snapshot;
  }

  async function applyRecordsDualWriteV2(queryClient, records, options = {}) {
    if (!DUAL_WRITE_V2_ENABLED) {
      return null;
    }

    recordDualWriteAttempt?.(performanceObservability);
    const mode = sanitizeTextValue(options.mode, 32) || "unknown";
    const recordsCount = Array.isArray(records) ? records.length : 0;

    try {
      const writeTimestamp = normalizeSourceStateUpdatedAtForV2(options.sourceStateUpdatedAt) || new Date().toISOString();
      const syncSummary = await syncLegacyRecordsSnapshotToV2(queryClient, records, {
        writeTimestamp,
        sourceStateUpdatedAt: writeTimestamp,
      });
      const metricSummary = {
        mode,
        ...syncSummary,
      };

      if (!syncSummary.inSync) {
        recordDualWriteFailure?.(performanceObservability, metricSummary);
        const desyncError = createHttpError(
          "Dual-write synchronization failed. client_records_v2 row count mismatch.",
          500,
          "records_dual_write_desync",
        );
        desyncError.summary = metricSummary;
        throw desyncError;
      }

      recordDualWriteSuccess?.(performanceObservability, metricSummary);
      return metricSummary;
    } catch (error) {
      if (error && error.code === "records_dual_write_desync") {
        throw error;
      }

      const wrappedError = createHttpError(
        "Dual-write synchronization to client_records_v2 failed.",
        500,
        "records_dual_write_failed",
      );
      wrappedError.summary = buildDualWriteSummaryPayload({
        mode,
        recordsCount,
        errorCode: sanitizeTextValue(error?.code, 80),
        errorMessage: sanitizeTextValue(error?.message, 320),
      });
      throw wrappedError;
    }
  }

  async function upsertLegacyStateRevisionPointer(queryClient, updatedAt) {
    const writeTimestamp = normalizeSourceStateUpdatedAtForV2(updatedAt) || new Date().toISOString();
    const updatedAtMode = await resolveLegacyStateUpdatedAtMode(queryClient);
    const parsedWriteTimestamp = Date.parse(writeTimestamp);
    const writeTimestampMs = Number.isFinite(parsedWriteTimestamp) ? Math.floor(parsedWriteTimestamp) : Date.now();

    if (updatedAtMode === "bigint") {
      await queryClient(
        `
          INSERT INTO ${STATE_TABLE} (id, records, updated_at)
          VALUES ($1, '[]'::jsonb, $2::bigint)
          ON CONFLICT (id)
          DO UPDATE SET updated_at = EXCLUDED.updated_at
        `,
        [STATE_ROW_ID, writeTimestampMs],
      );
      return new Date(writeTimestampMs).toISOString();
    }

    await queryClient(
      `
        INSERT INTO ${STATE_TABLE} (id, records, updated_at)
        VALUES ($1, '[]'::jsonb, $2::timestamptz)
        ON CONFLICT (id)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
      `,
      [STATE_ROW_ID, writeTimestamp],
    );
    return writeTimestamp;
  }

  async function mirrorLegacyStateRecordsBestEffort(queryClient, records, updatedAt, options = {}) {
    const mode = sanitizeTextValue(options.mode, 32) || "unknown";
    await queryClient("SAVEPOINT legacy_mirror_write", []);
    try {
      await upsertLegacyStateRecords(queryClient, records, updatedAt);
      await queryClient("RELEASE SAVEPOINT legacy_mirror_write", []);
      return {
        mirrored: true,
      };
    } catch (error) {
      await queryClient("ROLLBACK TO SAVEPOINT legacy_mirror_write", []);
      await queryClient("RELEASE SAVEPOINT legacy_mirror_write", []);
      log.warn?.(
        `[records] LEGACY_MIRROR write skipped after v2 ${mode} (code=${sanitizeTextValue(error?.code, 80) || "no_code"}): ${sanitizeTextValue(error?.message, 320) || "unknown error"}`,
      );
      return {
        mirrored: false,
        errorCode: sanitizeTextValue(error?.code, 80),
      };
    }
  }

  async function prependSingleRecordToLegacyState(queryClient, record, options = {}) {
    const writeTimestamp = normalizeSourceStateUpdatedAtForV2(options.writeTimestamp) || new Date().toISOString();
    const updatedAtMode = await resolveLegacyStateUpdatedAtMode(queryClient);
    const parsedWriteTimestamp = Date.parse(writeTimestamp);
    const writeTimestampMs = Number.isFinite(parsedWriteTimestamp) ? Math.floor(parsedWriteTimestamp) : Date.now();

    if (updatedAtMode === "bigint") {
      await queryClient(
        `
          INSERT INTO ${STATE_TABLE} (id, records, updated_at)
          VALUES ($1, jsonb_build_array($2::jsonb), $3::bigint)
          ON CONFLICT (id)
          DO UPDATE SET
            records = jsonb_build_array($2::jsonb) || ${STATE_TABLE}.records,
            updated_at = EXCLUDED.updated_at
        `,
        [STATE_ROW_ID, JSON.stringify(record), writeTimestampMs],
      );
      return;
    }

    await queryClient(
      `
        INSERT INTO ${STATE_TABLE} (id, records, updated_at)
        VALUES ($1, jsonb_build_array($2::jsonb), $3::timestamptz)
        ON CONFLICT (id)
        DO UPDATE SET
          records = jsonb_build_array($2::jsonb) || ${STATE_TABLE}.records,
          updated_at = EXCLUDED.updated_at
      `,
      [STATE_ROW_ID, JSON.stringify(record), writeTimestamp],
    );
  }

  async function upsertSingleRecordToV2(queryClient, record, options = {}) {
    const writeTimestamp = normalizeSourceStateUpdatedAtForV2(options.writeTimestamp) || new Date().toISOString();
    const snapshot = normalizeLegacyRecordsSnapshot([record], {
      sourceStateUpdatedAt: writeTimestamp,
      sourceStateRowId: STATE_ROW_ID,
    });
    const row = snapshot.rows[0];
    if (!row) {
      throw createHttpError(
        "Cannot approve submission. Record payload is invalid.",
        400,
        "invalid_submission_record",
      );
    }
    await queryClient(
      `
        INSERT INTO ${CLIENT_RECORDS_V2_TABLE}
          (id, record, record_hash, client_name, company_name, closed_by, created_at, source_state_updated_at, source_state_row_id, inserted_at, updated_at)
        VALUES
          ($1, $2::jsonb, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz, $9, $10::timestamptz, $10::timestamptz)
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
          updated_at = EXCLUDED.updated_at
        WHERE
          (record_hash, client_name, company_name, closed_by, created_at, source_state_updated_at, source_state_row_id)
          IS DISTINCT FROM
          (EXCLUDED.record_hash, EXCLUDED.client_name, EXCLUDED.company_name, EXCLUDED.closed_by, EXCLUDED.created_at, EXCLUDED.source_state_updated_at, EXCLUDED.source_state_row_id)
      `,
      [
        row.id,
        JSON.stringify(row.record),
        row.recordHash,
        row.clientName,
        row.companyName,
        row.closedBy,
        row.createdAt,
        row.sourceStateUpdatedAt,
        row.sourceStateRowId,
        writeTimestamp,
      ],
    );
    return {
      writeTimestamp,
    };
  }

  async function listCurrentRecordsFromV2ForWrite(queryClient) {
    const result = await queryClient(
      `
        SELECT id, record
        FROM ${CLIENT_RECORDS_V2_TABLE}
        WHERE source_state_row_id = $1
        ORDER BY id ASC
      `,
      [STATE_ROW_ID],
    );
    const records = [];
    for (const row of result.rows) {
      const record = normalizeRecordFromV2Row(row?.record);
      if (record) {
        records.push(record);
      }
    }
    return records;
  }

  async function saveStoredRecordsUsingV2(records, options = {}) {
    return runInTransaction(async ({ query: txQuery }) => {
      const stateResult = await txQuery(
        `
          SELECT updated_at
          FROM ${STATE_TABLE}
          WHERE id = $1
          FOR UPDATE
        `,
        [STATE_ROW_ID],
      );

      const currentUpdatedAt = stateResult.rows[0]?.updated_at || null;
      if (!isRecordStateRevisionMatch(options.expectedUpdatedAt, currentUpdatedAt)) {
        const conflictError = createHttpError(
          "Records were updated by another operation. Refresh records and try again.",
          409,
          "records_conflict",
        );
        conflictError.currentUpdatedAt = normalizeUpdatedAtForApi(currentUpdatedAt);
        throw conflictError;
      }

      const writeTimestamp = new Date().toISOString();
      const syncSummary = await syncLegacyRecordsSnapshotToV2(txQuery, records, {
        writeTimestamp,
        sourceStateUpdatedAt: writeTimestamp,
      });
      if (!syncSummary.inSync) {
        const desyncError = createHttpError(
          "Dual-write synchronization failed. client_records_v2 row count mismatch.",
          500,
          "records_dual_write_desync",
        );
        desyncError.summary = {
          mode: "put",
          ...syncSummary,
        };
        throw desyncError;
      }

      const updatedAt = await upsertLegacyStateRevisionPointer(txQuery, writeTimestamp);
      if (LEGACY_MIRROR_ENABLED) {
        await mirrorLegacyStateRecordsBestEffort(txQuery, records, updatedAt, {
          mode: "put",
        });
      }

      return updatedAt;
    });
  }

  async function saveStoredRecordsPatchUsingV2(operations, options = {}) {
    return runInTransaction(async ({ query: txQuery }) => {
      const stateResult = await txQuery(
        `
          SELECT updated_at
          FROM ${STATE_TABLE}
          WHERE id = $1
          FOR UPDATE
        `,
        [STATE_ROW_ID],
      );

      const currentUpdatedAt = stateResult.rows[0]?.updated_at || null;
      if (!isRecordStateRevisionMatch(options.expectedUpdatedAt, currentUpdatedAt)) {
        const conflictError = createHttpError(
          "Records were updated by another operation. Refresh records and try again.",
          409,
          "records_conflict",
        );
        conflictError.currentUpdatedAt = normalizeUpdatedAtForApi(currentUpdatedAt);
        throw conflictError;
      }

      const normalizedOperations = Array.isArray(operations) ? operations : [];
      if (!normalizedOperations.length) {
        return {
          updatedAt: normalizeUpdatedAtForApi(currentUpdatedAt),
        };
      }

      const currentRecords = await listCurrentRecordsFromV2ForWrite(txQuery);
      const nextRecords = applyRecordsPatchOperations(currentRecords, normalizedOperations);
      const writeTimestamp = new Date().toISOString();
      const syncSummary = await syncLegacyRecordsSnapshotToV2(txQuery, nextRecords, {
        writeTimestamp,
        sourceStateUpdatedAt: writeTimestamp,
      });
      if (!syncSummary.inSync) {
        const desyncError = createHttpError(
          "Dual-write synchronization failed. client_records_v2 row count mismatch.",
          500,
          "records_dual_write_desync",
        );
        desyncError.summary = {
          mode: "patch",
          ...syncSummary,
        };
        throw desyncError;
      }

      const updatedAt = await upsertLegacyStateRevisionPointer(txQuery, writeTimestamp);
      if (LEGACY_MIRROR_ENABLED) {
        await mirrorLegacyStateRecordsBestEffort(txQuery, nextRecords, updatedAt, {
          mode: "patch",
        });
      }

      return {
        updatedAt,
      };
    });
  }

  async function saveStoredRecords(records, options = {}) {
    await ensureDatabaseReady();

    const hasExpectedUpdatedAt = Object.prototype.hasOwnProperty.call(options, "expectedUpdatedAt");
    if (!hasExpectedUpdatedAt) {
      throw createHttpError(
        "Payload must include `expectedUpdatedAt` (latest revision from GET /api/records).",
        428,
        "records_precondition_required",
      );
    }

    const expectedUpdatedAt = options.expectedUpdatedAt;
    const expectedUpdatedAtMs = normalizeRecordStateTimestamp(expectedUpdatedAt);
    if (expectedUpdatedAt !== null && expectedUpdatedAt !== "" && expectedUpdatedAtMs === null) {
      throw createHttpError("`expectedUpdatedAt` must be a valid ISO datetime or null.", 400, "invalid_expected_updated_at");
    }

    if (WRITE_V2_ENABLED) {
      return saveStoredRecordsUsingV2(records, {
        expectedUpdatedAt,
      });
    }

    return runInTransaction(async ({ query: txQuery }) => {
      const stateResult = await txQuery(
        `
          SELECT records, updated_at
          FROM ${STATE_TABLE}
          WHERE id = $1
          FOR UPDATE
        `,
        [STATE_ROW_ID],
      );

      const currentUpdatedAt = stateResult.rows[0]?.updated_at || null;
      if (!isRecordStateRevisionMatch(expectedUpdatedAt, currentUpdatedAt)) {
        const conflictError = createHttpError(
          "Records were updated by another operation. Refresh records and try again.",
          409,
          "records_conflict",
        );
        conflictError.currentUpdatedAt = normalizeUpdatedAtForApi(currentUpdatedAt);
        throw conflictError;
      }
      const updatedAt = await upsertLegacyStateRecords(txQuery, records, new Date().toISOString());
      await applyRecordsDualWriteV2(txQuery, records, {
        mode: "put",
        sourceStateUpdatedAt: updatedAt,
      });
      return updatedAt;
    });
  }

  async function saveStoredRecordsPatch(operations, options = {}) {
    await ensureDatabaseReady();

    const hasExpectedUpdatedAt = Object.prototype.hasOwnProperty.call(options, "expectedUpdatedAt");
    if (!hasExpectedUpdatedAt) {
      throw createHttpError(
        "Payload must include `expectedUpdatedAt` (latest revision from GET /api/records).",
        428,
        "records_precondition_required",
      );
    }

    const expectedUpdatedAt = options.expectedUpdatedAt;
    const expectedUpdatedAtMs = normalizeRecordStateTimestamp(expectedUpdatedAt);
    if (expectedUpdatedAt !== null && expectedUpdatedAt !== "" && expectedUpdatedAtMs === null) {
      throw createHttpError("`expectedUpdatedAt` must be a valid ISO datetime or null.", 400, "invalid_expected_updated_at");
    }

    if (WRITE_V2_ENABLED) {
      return saveStoredRecordsPatchUsingV2(operations, {
        expectedUpdatedAt,
      });
    }

    return runInTransaction(async ({ query: txQuery }) => {
      const stateResult = await txQuery(
        `
          SELECT records, updated_at
          FROM ${STATE_TABLE}
          WHERE id = $1
          FOR UPDATE
        `,
        [STATE_ROW_ID],
      );

      const currentUpdatedAt = stateResult.rows[0]?.updated_at || null;
      if (!isRecordStateRevisionMatch(expectedUpdatedAt, currentUpdatedAt)) {
        const conflictError = createHttpError(
          "Records were updated by another operation. Refresh records and try again.",
          409,
          "records_conflict",
        );
        conflictError.currentUpdatedAt = normalizeUpdatedAtForApi(currentUpdatedAt);
        throw conflictError;
      }

      const currentRecords = Array.isArray(stateResult.rows[0]?.records) ? stateResult.rows[0].records : [];
      const normalizedOperations = Array.isArray(operations) ? operations : [];

      if (!normalizedOperations.length) {
        return {
          updatedAt: normalizeUpdatedAtForApi(currentUpdatedAt),
        };
      }

      const nextRecords = applyRecordsPatchOperations(currentRecords, normalizedOperations);
      const updatedAt = await upsertLegacyStateRecords(txQuery, nextRecords, new Date().toISOString());
      await applyRecordsDualWriteV2(txQuery, nextRecords, {
        mode: "patch",
        sourceStateUpdatedAt: updatedAt,
      });

      return {
        updatedAt,
      };
    });
  }

  async function upsertSingleRecordToV2ByClient(client, record, options = {}) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return upsertSingleRecordToV2(txQuery, record, options);
  }

  async function prependSingleRecordToLegacyStateByClient(client, record, options = {}) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return prependSingleRecordToLegacyState(txQuery, record, options);
  }

  async function upsertLegacyStateRevisionPointerByClient(client, updatedAt) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return upsertLegacyStateRevisionPointer(txQuery, updatedAt);
  }

  async function syncLegacyRecordsSnapshotToV2ByClient(client, records, options = {}) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return syncLegacyRecordsSnapshotToV2(txQuery, records, options);
  }

  async function mirrorLegacyStateRecordsBestEffortByClient(client, records, updatedAt, options = {}) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return mirrorLegacyStateRecordsBestEffort(txQuery, records, updatedAt, options);
  }

  async function listCurrentRecordsFromV2ForWriteByClient(client) {
    const txQuery = createClientQuery(client);
    if (!txQuery) {
      throw dbUnavailableErrorFactory();
    }
    return listCurrentRecordsFromV2ForWrite(txQuery);
  }

  return {
    getStoredRecords,
    getStoredRecordsFromV2,
    getStoredRecordsHeadRevision,
    runDualReadCompareForLegacyRecords,
    syncLegacyRecordsSnapshotToV2: syncLegacyRecordsSnapshotToV2ByClient,
    upsertLegacyStateRevisionPointer: upsertLegacyStateRevisionPointerByClient,
    mirrorLegacyStateRecordsBestEffort: mirrorLegacyStateRecordsBestEffortByClient,
    prependSingleRecordToLegacyState: prependSingleRecordToLegacyStateByClient,
    upsertSingleRecordToV2: upsertSingleRecordToV2ByClient,
    listCurrentRecordsFromV2ForWrite: listCurrentRecordsFromV2ForWriteByClient,
    saveStoredRecordsUsingV2,
    saveStoredRecordsPatchUsingV2,
    saveStoredRecords,
    saveStoredRecordsPatch,
    upsertLegacyStateRevisionPointerByClient,
    prependSingleRecordToLegacyStateByClient,
    upsertSingleRecordToV2ByClient,
  };
}

module.exports = {
  createRecordsRepo,
};
