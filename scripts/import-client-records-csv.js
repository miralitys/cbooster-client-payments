#!/usr/bin/env node

"use strict";

const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");
const {
  DEFAULT_SOURCE_STATE_ROW_ID,
  buildRecordsV2TableRefsFromEnv,
  ensureClientRecordsV2Schema,
  normalizeLegacyRecordsSnapshot,
  sanitizeTextValue,
} = require("../client-records-v2-utils");
const { createPgSslConfig } = require("../server/shared/db/pool");

const MAX_NOTES_LENGTH = 8000;
const MAX_BULK_ROWS = 500;
const DEFAULT_OWNER_COMPANY = "Credit Booster";
const OWNER_COMPANY_COLUMN_KEYS = [
  "owner_company",
  "portfolio_company",
  "booster_company",
  "agency_company",
  "company_brand",
  "base_company",
];

function parseArgs(argv) {
  const options = {
    csvPath: "",
    dryRun: false,
    sourceRowId: DEFAULT_SOURCE_STATE_ROW_ID,
    backupDir: "backups/system-backups",
    skipBackup: false,
    recordsJsonPath: "",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = sanitizeTextValue(argv[index], 400);

    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (arg === "--skip-backup") {
      options.skipBackup = true;
      continue;
    }

    if (arg === "--csv") {
      options.csvPath = sanitizeTextValue(argv[index + 1], 2000);
      index += 1;
      continue;
    }

    if (arg === "--backup-dir") {
      options.backupDir = sanitizeTextValue(argv[index + 1], 2000) || options.backupDir;
      index += 1;
      continue;
    }

    if (arg === "--records-json") {
      options.recordsJsonPath = sanitizeTextValue(argv[index + 1], 2000);
      index += 1;
      continue;
    }

    if (arg === "--source-row-id") {
      const rawValue = sanitizeTextValue(argv[index + 1], 40);
      const parsed = Number.parseInt(rawValue, 10);
      if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error("Invalid --source-row-id value. Use a positive integer.");
      }
      options.sourceRowId = parsed;
      index += 1;
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  if (!options.csvPath) {
    throw new Error("Missing required --csv argument.");
  }

  return options;
}

function parseCsvRows(rawInput) {
  const input = rawInput.replace(/^\uFEFF/, "");
  const rows = [];
  let currentRow = [];
  let currentCell = "";
  let inQuotes = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const nextChar = input[index + 1];

    if (char === "\"") {
      if (inQuotes && nextChar === "\"") {
        currentCell += "\"";
        index += 1;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      currentRow.push(currentCell);
      currentCell = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && nextChar === "\n") {
        index += 1;
      }
      currentRow.push(currentCell);
      currentCell = "";
      if (!isCsvRowEmpty(currentRow)) {
        rows.push(currentRow);
      }
      currentRow = [];
      continue;
    }

    currentCell += char;
  }

  if (currentCell.length > 0 || currentRow.length > 0) {
    currentRow.push(currentCell);
    if (!isCsvRowEmpty(currentRow)) {
      rows.push(currentRow);
    }
  }

  return rows;
}

function isCsvRowEmpty(row) {
  if (!Array.isArray(row) || !row.length) {
    return true;
  }
  return row.every((cell) => !sanitizeTextValue(cell, 20000));
}

function buildCsvObjects(rows) {
  if (!Array.isArray(rows) || rows.length < 2) {
    return [];
  }

  const rawHeader = rows[0];
  const header = rawHeader.map((cell, index) => {
    const normalized = sanitizeTextValue(cell, 160).toLowerCase();
    return normalized || `column_${index + 1}`;
  });

  const objects = [];
  for (let rowIndex = 1; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const item = {};
    for (let columnIndex = 0; columnIndex < header.length; columnIndex += 1) {
      item[header[columnIndex]] = sanitizeTextValue(row[columnIndex], 20000);
    }
    objects.push(item);
  }

  return objects;
}

function parseMoneyLike(rawValue) {
  const value = sanitizeTextValue(rawValue, 400);
  if (!value) {
    return null;
  }
  const normalized = value
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");
  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return null;
  }
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatDateUs(year, month, day) {
  return `${String(month).padStart(2, "0")}/${String(day).padStart(2, "0")}/${String(year).padStart(4, "0")}`;
}

function normalizeDateForRecord(rawValue) {
  const value = sanitizeTextValue(rawValue, 120);
  if (!value) {
    return "";
  }

  const usMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    const year = Number(usMatch[3]);
    if (isValidDateParts(year, month, day)) {
      return formatDateUs(year, month, day);
    }
    return "";
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T].*)?$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    if (isValidDateParts(year, month, day)) {
      return formatDateUs(year, month, day);
    }
    return "";
  }

  const parsedTimestamp = Date.parse(value);
  if (!Number.isFinite(parsedTimestamp)) {
    return "";
  }
  const date = new Date(parsedTimestamp);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  if (!isValidDateParts(year, month, day)) {
    return "";
  }
  return formatDateUs(year, month, day);
}

function normalizeDateForIso(rawValue) {
  const value = sanitizeTextValue(rawValue, 120);
  if (!value) {
    return null;
  }

  const usMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    const year = Number(usMatch[3]);
    if (!isValidDateParts(year, month, day)) {
      return null;
    }
    return new Date(Date.UTC(year, month - 1, day)).toISOString();
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T].*)?$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    if (!isValidDateParts(year, month, day)) {
      return null;
    }
    return new Date(Date.UTC(year, month - 1, day)).toISOString();
  }

  const parsedTimestamp = Date.parse(value);
  if (!Number.isFinite(parsedTimestamp)) {
    return null;
  }
  const date = new Date(parsedTimestamp);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  if (!isValidDateParts(year, month, day)) {
    return null;
  }
  return date.toISOString();
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }
  if (year < 1900 || year > 2100) {
    return false;
  }
  if (month < 1 || month > 12) {
    return false;
  }
  if (day < 1 || day > 31) {
    return false;
  }

  const timestamp = Date.UTC(year, month - 1, day);
  const date = new Date(timestamp);
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function normalizeMoneyForRecord(rawValue) {
  const value = sanitizeTextValue(rawValue, 120);
  if (!value) {
    return "";
  }
  const parsed = parseMoneyLike(value);
  if (parsed === null) {
    return value;
  }
  return String(parsed);
}

function resolveCsvText(rawRow, keys, maxLength = 120) {
  for (const key of keys) {
    const value = sanitizeTextValue(rawRow?.[key], maxLength);
    if (value) {
      return value;
    }
  }
  return "";
}

function normalizeOwnerCompanyValue(rawValue) {
  const value = sanitizeTextValue(rawValue, 160);
  if (!value) {
    return "";
  }

  const normalized = value.toLowerCase().replace(/\s+/g, " ");
  if (normalized === "credit booster") {
    return "Credit Booster";
  }
  if (normalized === "ramis booster") {
    return "Ramis Booster";
  }
  if (normalized === "brobas") {
    return "Brobas";
  }
  if (normalized === "wolfowich" || normalized === "wolfovich") {
    return "Wolfowich";
  }

  return value;
}

function resolveOwnerCompany(rawRow) {
  const explicit = resolveCsvText(rawRow, OWNER_COMPANY_COLUMN_KEYS, 160);
  const normalized = normalizeOwnerCompanyValue(explicit);
  return normalized || DEFAULT_OWNER_COMPANY;
}

function resolveContractTotalRaw(rawRow) {
  return resolveCsvText(rawRow, ["xontract_total", "сontract_total", "contract_total"], 120);
}

function getPaymentIndexesFromRow(rawRow) {
  const indexes = new Set();
  for (const key of Object.keys(rawRow || {})) {
    const match = key.match(/^payment_(\d+)$/);
    if (match) {
      indexes.add(Number(match[1]));
      continue;
    }
    const dateMatch = key.match(/^payment_(\d+)_date$/);
    if (dateMatch) {
      indexes.add(Number(dateMatch[1]));
    }
  }
  return [...indexes].filter((value) => Number.isInteger(value) && value > 0).sort((left, right) => left - right);
}

function buildEmptyRecord() {
  return {
    id: "",
    createdAt: "",
    clientName: "",
    closedBy: "",
    companyName: "",
    ownerCompany: DEFAULT_OWNER_COMPANY,
    serviceType: "",
    purchasedService: "",
    address: "",
    dateOfBirth: "",
    ssn: "",
    creditMonitoringLogin: "",
    creditMonitoringPassword: "",
    leadSource: "",
    clientPhoneNumber: "",
    clientEmailAddress: "",
    futurePayment: "",
    identityIq: "",
    contractTotals: "",
    totalPayments: "",
    payment1: "",
    payment1Date: "",
    payment2: "",
    payment2Date: "",
    payment3: "",
    payment3Date: "",
    payment4: "",
    payment4Date: "",
    payment5: "",
    payment5Date: "",
    payment6: "",
    payment6Date: "",
    payment7: "",
    payment7Date: "",
    futurePayments: "",
    afterResult: "",
    writtenOff: "",
    contractSigned: "",
    startedInWork: "",
    notes: "",
    collection: "",
    dateOfCollection: "",
    dateWhenWrittenOff: "",
    dateWhenFullyPaid: "",
  };
}

function buildLegacyPaymentTail(rawRow) {
  const parts = [];
  const indexes = getPaymentIndexesFromRow(rawRow).filter((value) => value > 7);
  for (const paymentIndex of indexes) {
    const amount = sanitizeTextValue(rawRow[`payment_${paymentIndex}`], 120);
    const date = normalizeDateForRecord(rawRow[`payment_${paymentIndex}_date`]);
    if (!amount && !date) {
      continue;
    }
    if (amount && date) {
      parts.push(`P${paymentIndex}: ${amount} (${date})`);
      continue;
    }
    if (amount) {
      parts.push(`P${paymentIndex}: ${amount}`);
      continue;
    }
    parts.push(`P${paymentIndex}: (${date})`);
  }

  if (!parts.length) {
    return "";
  }

  return `Legacy payments 8+: ${parts.join("; ")}`;
}

function buildNotes(rawRow) {
  const parts = [];
  const baseNotes = resolveCsvText(rawRow, ["notes_or_result_of_conversation", "notes"], MAX_NOTES_LENGTH);
  if (baseNotes) {
    parts.push(baseNotes);
  }

  const whoCalled = sanitizeTextValue(rawRow.who_called, 220);
  if (whoCalled) {
    parts.push(`Who called: ${whoCalled}`);
  }

  const checkingDate = normalizeDateForRecord(rawRow.checking_date);
  if (checkingDate) {
    parts.push(`Checking date: ${checkingDate}`);
  }

  const status = sanitizeTextValue(rawRow.client_status, 220);
  if (status) {
    parts.push(`Legacy status: ${status}`);
  }

  const badDebtBalance = sanitizeTextValue(rawRow.bad_debt_balance, 80);
  if (badDebtBalance) {
    parts.push(`Bad debt balance: ${badDebtBalance}`);
  }

  const afterResultBalance = sanitizeTextValue(rawRow.after_result_balance, 80);
  if (afterResultBalance) {
    parts.push(`After result balance: ${afterResultBalance}`);
  }

  const paymentTail = buildLegacyPaymentTail(rawRow);
  if (paymentTail) {
    parts.push(paymentTail);
  }

  return sanitizeTextValue(parts.join("\n"), MAX_NOTES_LENGTH);
}

function hasAnyPaymentSignal(rawRow) {
  const indexes = getPaymentIndexesFromRow(rawRow);
  for (const paymentIndex of indexes) {
    if (sanitizeTextValue(rawRow[`payment_${paymentIndex}`], 120)) {
      return true;
    }
    if (sanitizeTextValue(rawRow[`payment_${paymentIndex}_date`], 120)) {
      return true;
    }
  }
  return false;
}

function resolveContractSigned(rawRow) {
  const contractTotal = parseMoneyLike(resolveContractTotalRaw(rawRow));
  if (contractTotal !== null && contractTotal > 0) {
    return "Yes";
  }

  if (hasAnyPaymentSignal(rawRow)) {
    return "Yes";
  }

  const futurePayments = parseMoneyLike(rawRow.future_payments);
  if (futurePayments !== null && futurePayments > 0) {
    return "Yes";
  }

  const collection = parseMoneyLike(rawRow.collection);
  if (collection !== null && collection !== 0) {
    return "Yes";
  }

  return "No";
}

function resolveStartedInWork(rawRow) {
  if (hasAnyPaymentSignal(rawRow)) {
    return "Yes";
  }

  const checkingDate = normalizeDateForRecord(rawRow.checking_date);
  if (checkingDate) {
    return "Yes";
  }

  const status = sanitizeTextValue(rawRow.client_status, 220).toLowerCase();
  if (status && status !== "new") {
    return "Yes";
  }

  return "No";
}

function resolveAfterResult(rawRow) {
  const afterResultBalance = parseMoneyLike(rawRow.after_result_balance);
  if (afterResultBalance !== null && afterResultBalance > 0) {
    return "Yes";
  }
  return "";
}

function resolveWrittenOff(rawRow) {
  const badDebtBalance = parseMoneyLike(rawRow.bad_debt_balance);
  if (badDebtBalance !== null && badDebtBalance > 0) {
    return "Yes";
  }

  if (normalizeDateForRecord(rawRow.date_when_written_of)) {
    return "Yes";
  }

  const status = sanitizeTextValue(rawRow.client_status, 220).toLowerCase();
  if (/\bviolet\b|\bwrite[-\s]?off\b/.test(status)) {
    return "Yes";
  }

  return "";
}

function resolveCreatedAtIso(rawRow) {
  const candidates = [rawRow.payment_1_date, rawRow.checking_date, rawRow.date_of_collection, rawRow.date_when_written_of];
  const paymentIndexes = getPaymentIndexesFromRow(rawRow);
  for (const paymentIndex of paymentIndexes) {
    candidates.push(rawRow[`payment_${paymentIndex}_date`]);
  }
  for (const candidate of candidates) {
    const normalized = normalizeDateForIso(candidate);
    if (normalized) {
      return normalized;
    }
  }
  return new Date().toISOString();
}

function mapCsvRowToRecord(rawRow) {
  const record = buildEmptyRecord();

  record.id = sanitizeTextValue(rawRow.id, 180);
  if (!record.id) {
    return null;
  }

  record.createdAt = resolveCreatedAtIso(rawRow);
  record.clientName = sanitizeTextValue(rawRow.client_name, 300);
  record.closedBy = sanitizeTextValue(rawRow.closed_by, 220);
  record.companyName = sanitizeTextValue(rawRow.company_name_or_payer, 300);
  record.ownerCompany = resolveOwnerCompany(rawRow);
  record.serviceType = resolveCsvText(rawRow, ["trucks_or_short_useful_info", "service"], 300);
  record.contractTotals = normalizeMoneyForRecord(resolveContractTotalRaw(rawRow));
  record.payment1 = normalizeMoneyForRecord(rawRow.payment_1);
  record.payment1Date = normalizeDateForRecord(rawRow.payment_1_date);
  record.payment2 = normalizeMoneyForRecord(rawRow.payment_2);
  record.payment2Date = normalizeDateForRecord(rawRow.payment_2_date);
  record.payment3 = normalizeMoneyForRecord(rawRow.payment_3);
  record.payment3Date = normalizeDateForRecord(rawRow.payment_3_date);
  record.payment4 = normalizeMoneyForRecord(rawRow.payment_4);
  record.payment4Date = normalizeDateForRecord(rawRow.payment_4_date);
  record.payment5 = normalizeMoneyForRecord(rawRow.payment_5);
  record.payment5Date = normalizeDateForRecord(rawRow.payment_5_date);
  record.payment6 = normalizeMoneyForRecord(rawRow.payment_6);
  record.payment6Date = normalizeDateForRecord(rawRow.payment_6_date);
  record.payment7 = normalizeMoneyForRecord(rawRow.payment_7);
  record.payment7Date = normalizeDateForRecord(rawRow.payment_7_date);
  record.futurePayments = normalizeMoneyForRecord(rawRow.future_payments);
  record.collection = normalizeMoneyForRecord(rawRow.collection);
  record.dateOfCollection = normalizeDateForRecord(rawRow.date_of_collection);
  record.dateWhenWrittenOff = normalizeDateForRecord(rawRow.date_when_written_of);
  record.notes = buildNotes(rawRow);
  record.afterResult = resolveAfterResult(rawRow);
  record.writtenOff = resolveWrittenOff(rawRow);
  record.contractSigned = resolveContractSigned(rawRow);
  record.startedInWork = resolveStartedInWork(rawRow);

  return record;
}

function dedupeRecordsById(records) {
  const map = new Map();
  let duplicates = 0;

  for (const record of records) {
    if (!record || typeof record !== "object") {
      continue;
    }
    if (!record.id) {
      continue;
    }
    if (map.has(record.id)) {
      duplicates += 1;
    }
    map.set(record.id, record);
  }

  return {
    records: [...map.values()],
    duplicates,
  };
}

function buildBackupPayload(stateRow, refs, sourceRowId) {
  return {
    createdAt: new Date().toISOString(),
    sourceRowId,
    table: refs.stateTable,
    updatedAt: stateRow?.updated_at || null,
    recordsCount: Array.isArray(stateRow?.records) ? stateRow.records.length : 0,
    records: Array.isArray(stateRow?.records) ? stateRow.records : [],
  };
}

function ensureDirectory(directoryPath) {
  fs.mkdirSync(directoryPath, { recursive: true });
}

function buildBackupFilePath(baseDir) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return path.resolve(baseDir, `client-records-backup-${timestamp}.json`);
}

function chunkRows(rows, chunkSize) {
  const chunks = [];
  for (let index = 0; index < rows.length; index += chunkSize) {
    chunks.push(rows.slice(index, index + chunkSize));
  }
  return chunks;
}

async function upsertV2Rows(client, refs, rows) {
  let insertedOrUpdated = 0;
  let unchanged = 0;
  const chunks = chunkRows(rows, MAX_BULK_ROWS);

  for (const chunk of chunks) {
    if (!chunk.length) {
      continue;
    }

    const values = [];
    const placeholders = chunk.map((row, index) => {
      const base = index * 10;
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
      return `($${base + 1}, $${base + 2}::jsonb, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}::timestamptz, $${base + 8}::timestamptz, $${base + 9}, NOW(), NOW())`;
    });

    const queryText = `
      INSERT INTO ${refs.recordsV2Table}
        (id, record, record_hash, client_name, company_name, closed_by, created_at, source_state_updated_at, source_state_row_id, inserted_at, updated_at)
      VALUES ${placeholders.join(", ")}
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
        (record_hash, client_name, company_name, closed_by, created_at, source_state_updated_at, source_state_row_id)
        IS DISTINCT FROM
        (EXCLUDED.record_hash, EXCLUDED.client_name, EXCLUDED.company_name, EXCLUDED.closed_by, EXCLUDED.created_at, EXCLUDED.source_state_updated_at, EXCLUDED.source_state_row_id)
      RETURNING id
    `;

    const result = await client.query(queryText, values);
    const changedCount = result.rowCount || 0;
    insertedOrUpdated += changedCount;
    unchanged += Math.max(chunk.length - changedCount, 0);
  }

  return {
    insertedOrUpdated,
    unchanged,
  };
}

async function deleteMissingV2Rows(client, refs, sourceRowId, ids) {
  if (ids.length) {
    const result = await client.query(
      `
        DELETE FROM ${refs.recordsV2Table}
        WHERE source_state_row_id = $1
          AND NOT (id = ANY($2::text[]))
      `,
      [sourceRowId, ids],
    );
    return result.rowCount || 0;
  }

  const result = await client.query(
    `
      DELETE FROM ${refs.recordsV2Table}
      WHERE source_state_row_id = $1
    `,
    [sourceRowId],
  );
  return result.rowCount || 0;
}

function mapCsvToRecords(csvPath) {
  const absoluteCsvPath = path.resolve(csvPath);
  const content = fs.readFileSync(absoluteCsvPath, "utf8");
  const rows = parseCsvRows(content);
  const items = buildCsvObjects(rows);
  const mapped = [];
  let skippedWithoutId = 0;

  for (const item of items) {
    const record = mapCsvRowToRecord(item);
    if (!record) {
      skippedWithoutId += 1;
      continue;
    }
    mapped.push(record);
  }

  const deduped = dedupeRecordsById(mapped);
  return {
    csvPath: absoluteCsvPath,
    rawRowCount: Math.max(rows.length - 1, 0),
    mappedRowCount: mapped.length,
    finalRowCount: deduped.records.length,
    skippedWithoutId,
    duplicateIdCount: deduped.duplicates,
    records: deduped.records,
  };
}

function writeRecordsJsonIfRequested(records, outputPath) {
  const targetPath = sanitizeTextValue(outputPath, 2000);
  if (!targetPath) {
    return "";
  }

  const absolutePath = path.resolve(targetPath);
  const directory = path.dirname(absolutePath);
  ensureDirectory(directory);
  fs.writeFileSync(absolutePath, JSON.stringify(records), "utf8");
  return absolutePath;
}

async function importRecords(options) {
  const mapResult = mapCsvToRecords(options.csvPath);
  const records = mapResult.records;
  const recordsJsonPath = writeRecordsJsonIfRequested(records, options.recordsJsonPath);

  if (options.dryRun) {
    return {
      ok: true,
      dryRun: true,
      recordsJsonPath: recordsJsonPath || null,
      map: {
        csvPath: mapResult.csvPath,
        rawRowCount: mapResult.rawRowCount,
        mappedRowCount: mapResult.mappedRowCount,
        finalRowCount: mapResult.finalRowCount,
        skippedWithoutId: mapResult.skippedWithoutId,
        duplicateIdCount: mapResult.duplicateIdCount,
      },
    };
  }

  const databaseUrl = sanitizeTextValue(process.env.DATABASE_URL, 2000);
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for import.");
  }

  const refs = buildRecordsV2TableRefsFromEnv(process.env);
  const pool = new Pool({
    connectionString: databaseUrl,
    ssl: createPgSslConfig({}, process.env),
  });
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await ensureClientRecordsV2Schema(client, refs);

    const stateResult = await client.query(
      `SELECT records, updated_at FROM ${refs.stateTable} WHERE id = $1 LIMIT 1`,
      [options.sourceRowId],
    );
    const stateRow = stateResult.rows[0] || null;

    let backupFilePath = "";
    if (!options.skipBackup) {
      ensureDirectory(options.backupDir);
      backupFilePath = buildBackupFilePath(options.backupDir);
      const backupPayload = buildBackupPayload(stateRow, refs, options.sourceRowId);
      fs.writeFileSync(backupFilePath, JSON.stringify(backupPayload, null, 2), "utf8");
    }

    const writeTimestamp = new Date().toISOString();
    await client.query(
      `
        INSERT INTO ${refs.stateTable} (id, records, updated_at)
        VALUES ($1, $2::jsonb, $3::timestamptz)
        ON CONFLICT (id)
        DO UPDATE SET records = EXCLUDED.records, updated_at = EXCLUDED.updated_at
      `,
      [options.sourceRowId, JSON.stringify(records), writeTimestamp],
    );

    const snapshot = normalizeLegacyRecordsSnapshot(records, {
      sourceStateUpdatedAt: writeTimestamp,
      sourceStateRowId: options.sourceRowId,
    });

    const v2UpsertSummary = await upsertV2Rows(client, refs, snapshot.rows);
    const v2DeletedCount = await deleteMissingV2Rows(
      client,
      refs,
      options.sourceRowId,
      snapshot.rows.map((row) => row.id),
    );

    const v2CountResult = await client.query(
      `SELECT COUNT(*)::bigint AS total FROM ${refs.recordsV2Table} WHERE source_state_row_id = $1`,
      [options.sourceRowId],
    );
    const v2Count = Number.parseInt(v2CountResult.rows[0]?.total, 10) || 0;

    await client.query("COMMIT");

    return {
      ok: true,
      dryRun: false,
      writeTimestamp,
      backupFilePath: backupFilePath || null,
      recordsJsonPath: recordsJsonPath || null,
      map: {
        csvPath: mapResult.csvPath,
        rawRowCount: mapResult.rawRowCount,
        mappedRowCount: mapResult.mappedRowCount,
        finalRowCount: mapResult.finalRowCount,
        skippedWithoutId: mapResult.skippedWithoutId,
        duplicateIdCount: mapResult.duplicateIdCount,
      },
      v2: {
        upsertChangedCount: v2UpsertSummary.insertedOrUpdated,
        upsertUnchangedCount: v2UpsertSummary.unchanged,
        deletedCount: v2DeletedCount,
        finalCount: v2Count,
      },
      tables: {
        stateTable: refs.stateTable,
        recordsV2Table: refs.recordsV2Table,
      },
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
    await pool.end();
  }
}

(async function main() {
  try {
    const options = parseArgs(process.argv.slice(2));
    const result = await importRecords(options);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) {
    process.stderr.write(`Error: ${sanitizeTextValue(error?.message, 1000) || "Unknown error"}\n`);
    process.exit(1);
  }
})();
