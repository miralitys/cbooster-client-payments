const crypto = require("crypto");

const DEFAULT_DB_SCHEMA = "public";
const DEFAULT_STATE_TABLE_NAME = "client_records_state";
const DEFAULT_CLIENT_RECORDS_V2_TABLE_NAME = "client_records_v2";
const DEFAULT_SOURCE_STATE_ROW_ID = 1;
const MAX_ID_LENGTH = 180;
const MAX_CLIENT_NAME_LENGTH = 300;
const MAX_COMPANY_NAME_LENGTH = 300;
const MAX_CLOSED_BY_LENGTH = 220;

function sanitizeTextValue(value, maxLength = 4000) {
  return (value ?? "").toString().trim().slice(0, maxLength);
}

function resolveTableName(rawTableName, fallbackTableName) {
  const normalized = (rawTableName || fallbackTableName || "").trim();
  if (!normalized) {
    throw new Error("DB table name cannot be empty.");
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(normalized)) {
    throw new Error(`Unsafe DB table name: "${normalized}"`);
  }

  return normalized;
}

function resolveSchemaName(rawSchemaName, fallbackSchemaName) {
  const normalized = (rawSchemaName || fallbackSchemaName || "").trim();
  if (!normalized) {
    throw new Error("DB schema name cannot be empty.");
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(normalized)) {
    throw new Error(`Unsafe DB schema name: "${normalized}"`);
  }

  return normalized;
}

function qualifyTableName(schemaName, tableName) {
  return `"${schemaName}"."${tableName}"`;
}

function buildRecordsV2TableRefsFromEnv(env = process.env) {
  const schemaName = resolveSchemaName(env.DB_SCHEMA, DEFAULT_DB_SCHEMA);
  const stateTableName = resolveTableName(env.DB_TABLE_NAME, DEFAULT_STATE_TABLE_NAME);
  const recordsV2TableName = resolveTableName(
    env.DB_CLIENT_RECORDS_V2_TABLE_NAME,
    DEFAULT_CLIENT_RECORDS_V2_TABLE_NAME,
  );

  return {
    schemaName,
    stateTableName,
    recordsV2TableName,
    stateTable: qualifyTableName(schemaName, stateTableName),
    recordsV2Table: qualifyTableName(schemaName, recordsV2TableName),
  };
}

function buildSafeIndexName(baseName) {
  const normalized = sanitizeTextValue(baseName, 300).toLowerCase().replace(/[^a-z0-9_]+/g, "_");
  const candidate = normalized || "idx";
  if (candidate.length <= 63) {
    return candidate;
  }

  const hash = crypto.createHash("sha1").update(candidate).digest("hex").slice(0, 8);
  return `${candidate.slice(0, 63 - hash.length - 1)}_${hash}`;
}

async function ensureClientRecordsV2Schema(queryable, refs) {
  const table = refs.recordsV2Table;
  const tableName = refs.recordsV2TableName;

  await queryable.query(`
    CREATE TABLE IF NOT EXISTS ${table} (
      id TEXT PRIMARY KEY,
      record JSONB NOT NULL,
      record_hash TEXT NOT NULL,
      client_name TEXT NOT NULL DEFAULT '',
      company_name TEXT NOT NULL DEFAULT '',
      closed_by TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ,
      source_state_updated_at TIMESTAMPTZ,
      source_state_row_id BIGINT NOT NULL DEFAULT ${DEFAULT_SOURCE_STATE_ROW_ID},
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS record_hash TEXT NOT NULL DEFAULT ''
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS client_name TEXT NOT NULL DEFAULT ''
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS company_name TEXT NOT NULL DEFAULT ''
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS closed_by TEXT NOT NULL DEFAULT ''
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS source_state_updated_at TIMESTAMPTZ
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS source_state_row_id BIGINT NOT NULL DEFAULT ${DEFAULT_SOURCE_STATE_ROW_ID}
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `);

  await queryable.query(`
    ALTER TABLE ${table}
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  `);

  const indexQueries = [
    {
      name: buildSafeIndexName(`${tableName}_client_name_idx`),
      expression: `(client_name)`,
    },
    {
      name: buildSafeIndexName(`${tableName}_created_at_idx`),
      expression: `(created_at DESC NULLS LAST)`,
    },
    {
      name: buildSafeIndexName(`${tableName}_updated_at_idx`),
      expression: `(updated_at DESC)`,
    },
    {
      name: buildSafeIndexName(`${tableName}_source_state_updated_at_idx`),
      expression: `(source_state_updated_at DESC NULLS LAST)`,
    },
    {
      name: buildSafeIndexName(`${tableName}_record_gin_idx`),
      expression: `USING GIN (record)`,
    },
  ];

  for (const index of indexQueries) {
    await queryable.query(`
      CREATE INDEX IF NOT EXISTS ${index.name}
      ON ${table} ${index.expression}
    `);
  }
}

function normalizeRecordFieldValue(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return "";
  }

  if (typeof rawValue === "string") {
    return rawValue.trim();
  }

  if (typeof rawValue === "number") {
    return Number.isFinite(rawValue) ? String(rawValue) : "";
  }

  if (typeof rawValue === "boolean") {
    return rawValue ? "Yes" : "";
  }

  if (Array.isArray(rawValue)) {
    return rawValue.map((item) => normalizeRecordFieldValue(item));
  }

  if (typeof rawValue === "object") {
    const normalizedObject = {};
    for (const [key, value] of Object.entries(rawValue)) {
      const normalizedKey = sanitizeTextValue(key, 120);
      if (!normalizedKey) {
        continue;
      }
      normalizedObject[normalizedKey] = normalizeRecordFieldValue(value);
    }
    return normalizedObject;
  }

  return "";
}

function stableStringify(value) {
  if (value === undefined) {
    return "null";
  }

  if (value === null) {
    return "null";
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }

  if (typeof value === "object") {
    const keys = Object.keys(value).sort();
    const pairs = keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`);
    return `{${pairs.join(",")}}`;
  }

  return JSON.stringify(value);
}

function computeRecordHash(record) {
  const canonical = stableStringify(record);
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

function normalizeIsoTimestamp(value) {
  const raw = sanitizeTextValue(value, 80);
  if (!raw) {
    return null;
  }

  const timestamp = Date.parse(raw);
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  return new Date(timestamp).toISOString();
}

function normalizeSourceStateRowId(rawValue) {
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return DEFAULT_SOURCE_STATE_ROW_ID;
  }

  return parsed;
}

function normalizeLegacyRecordToV2Row(rawRecord, options = {}) {
  if (!rawRecord || typeof rawRecord !== "object" || Array.isArray(rawRecord)) {
    return null;
  }

  const normalizedRecord = {};
  for (const [key, rawValue] of Object.entries(rawRecord)) {
    const normalizedKey = sanitizeTextValue(key, 120);
    if (!normalizedKey) {
      continue;
    }
    normalizedRecord[normalizedKey] = normalizeRecordFieldValue(rawValue);
  }

  const id = sanitizeTextValue(normalizedRecord.id, MAX_ID_LENGTH);
  if (!id) {
    return null;
  }

  normalizedRecord.id = id;

  return {
    id,
    record: normalizedRecord,
    recordHash: computeRecordHash(normalizedRecord),
    clientName: sanitizeTextValue(normalizedRecord.clientName, MAX_CLIENT_NAME_LENGTH),
    companyName: sanitizeTextValue(normalizedRecord.companyName, MAX_COMPANY_NAME_LENGTH),
    closedBy: sanitizeTextValue(normalizedRecord.closedBy, MAX_CLOSED_BY_LENGTH),
    createdAt: normalizeIsoTimestamp(normalizedRecord.createdAt),
    sourceStateUpdatedAt: normalizeIsoTimestamp(options.sourceStateUpdatedAt),
    sourceStateRowId: normalizeSourceStateRowId(options.sourceStateRowId),
  };
}

function normalizeLegacyRecordsSnapshot(rawRecords, options = {}) {
  const list = Array.isArray(rawRecords) ? rawRecords : [];
  const rowsById = new Map();
  let skippedInvalidRecordCount = 0;
  let skippedMissingIdCount = 0;
  let duplicateIdCount = 0;

  for (const rawRecord of list) {
    if (!rawRecord || typeof rawRecord !== "object" || Array.isArray(rawRecord)) {
      skippedInvalidRecordCount += 1;
      continue;
    }

    const normalizedRow = normalizeLegacyRecordToV2Row(rawRecord, options);
    if (!normalizedRow) {
      const possibleId = sanitizeTextValue(rawRecord.id, MAX_ID_LENGTH);
      if (!possibleId) {
        skippedMissingIdCount += 1;
      } else {
        skippedInvalidRecordCount += 1;
      }
      continue;
    }

    if (rowsById.has(normalizedRow.id)) {
      duplicateIdCount += 1;
    }
    rowsById.set(normalizedRow.id, normalizedRow);
  }

  const rows = [...rowsById.values()].sort((left, right) => left.id.localeCompare(right.id));

  return {
    rows,
    skippedInvalidRecordCount,
    skippedMissingIdCount,
    duplicateIdCount,
    checksum: computeRowsChecksum(rows),
  };
}

function computeRowsChecksum(rows) {
  const hash = crypto.createHash("sha256");
  const list = Array.isArray(rows) ? [...rows] : [];
  list.sort((left, right) => sanitizeTextValue(left?.id, MAX_ID_LENGTH).localeCompare(sanitizeTextValue(right?.id, MAX_ID_LENGTH)));

  for (const row of list) {
    const id = sanitizeTextValue(row?.id, MAX_ID_LENGTH);
    if (!id) {
      continue;
    }
    const recordHash = sanitizeTextValue(row?.recordHash, 64) || computeRecordHash(row?.record || {});
    hash.update(id);
    hash.update(":");
    hash.update(recordHash);
    hash.update("\n");
  }

  return hash.digest("hex");
}

module.exports = {
  DEFAULT_CLIENT_RECORDS_V2_TABLE_NAME,
  DEFAULT_DB_SCHEMA,
  DEFAULT_SOURCE_STATE_ROW_ID,
  DEFAULT_STATE_TABLE_NAME,
  buildRecordsV2TableRefsFromEnv,
  buildSafeIndexName,
  computeRecordHash,
  computeRowsChecksum,
  ensureClientRecordsV2Schema,
  normalizeIsoTimestamp,
  normalizeLegacyRecordToV2Row,
  normalizeLegacyRecordsSnapshot,
  qualifyTableName,
  resolveSchemaName,
  resolveTableName,
  sanitizeTextValue,
  stableStringify,
};
