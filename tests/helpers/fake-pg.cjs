"use strict";

const fs = require("node:fs");
const Module = require("node:module");

if (process.env.TEST_USE_FAKE_PG !== "1") {
  return;
}

const originalLoad = Module._load;
const state = {
  submissions: new Map(),
  submissionFiles: [],
  legacyState: {
    exists: false,
    records: [],
    updated_at: null,
  },
  v2Records: new Map(),
};
const captureFile = String(process.env.TEST_PG_CAPTURE_FILE || "").trim();
const STATE_ROW_ID = "1";

function normalizeSql(rawSql) {
  return String(rawSql || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function safeJsonParse(rawValue, fallbackValue) {
  if (rawValue && typeof rawValue === "object") {
    return rawValue;
  }

  if (typeof rawValue !== "string") {
    return fallbackValue;
  }

  try {
    return JSON.parse(rawValue);
  } catch {
    return fallbackValue;
  }
}

function queryResult(rows = [], rowCount = rows.length) {
  return {
    rows,
    rowCount,
  };
}

function cloneJson(value) {
  if (value === null || value === undefined) {
    return value;
  }

  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function normalizeIsoTimestamp(rawValue) {
  const candidate = String(rawValue || "").trim();
  if (!candidate) {
    return new Date().toISOString();
  }

  const parsed = Date.parse(candidate);
  if (!Number.isFinite(parsed)) {
    return new Date().toISOString();
  }
  return new Date(parsed).toISOString();
}

function normalizeSubmissionForResponse(submission) {
  if (!submission || typeof submission !== "object") {
    return null;
  }

  return {
    id: String(submission.id || "").trim(),
    record: cloneJson(submission.record && typeof submission.record === "object" ? submission.record : {}),
    mini_data: cloneJson(submission.mini_data && typeof submission.mini_data === "object" ? submission.mini_data : {}),
    submitted_by: cloneJson(
      submission.submitted_by && typeof submission.submitted_by === "object" ? submission.submitted_by : {},
    ),
    status: String(submission.status || "pending"),
    submitted_at: submission.submitted_at || null,
    reviewed_at: submission.reviewed_at || null,
    reviewed_by: submission.reviewed_by || "",
    review_note: submission.review_note || null,
    purged_at: submission.purged_at || null,
  };
}

function getSubmissionIdsFromParam(rawValue) {
  if (Array.isArray(rawValue)) {
    return rawValue
      .map((value) => String(value || "").trim())
      .filter(Boolean);
  }
  const single = String(rawValue || "").trim();
  return single ? [single] : [];
}

function sortSubmissionsNewestFirst(rows) {
  return [...rows].sort((left, right) => {
    const leftTs = Number.isFinite(Date.parse(left?.submitted_at)) ? Date.parse(left.submitted_at) : 0;
    const rightTs = Number.isFinite(Date.parse(right?.submitted_at)) ? Date.parse(right.submitted_at) : 0;
    if (rightTs !== leftTs) {
      return rightTs - leftTs;
    }
    const leftId = String(left?.id || "");
    const rightId = String(right?.id || "");
    return rightId.localeCompare(leftId);
  });
}

function ensureLegacyStateRow() {
  if (!state.legacyState.exists) {
    state.legacyState.exists = true;
    state.legacyState.records = [];
    state.legacyState.updated_at = null;
  }
}

function captureEvent(payload) {
  if (!captureFile) {
    return;
  }

  try {
    fs.appendFileSync(`${captureFile}`, `${JSON.stringify(payload)}\n`, "utf8");
  } catch {
    // Best-effort capture for tests.
  }
}

function executeFakeQuery(rawSql, rawParams) {
  const sql = normalizeSql(rawSql);
  const params = Array.isArray(rawParams) ? rawParams : [];

  if (!sql) {
    return queryResult();
  }

  if (sql === "begin" || sql === "commit" || sql === "rollback") {
    return queryResult([], null);
  }

  if (sql.includes("insert into") && sql.includes("mini_client_submissions")) {
    const id = String(params[0] || "").trim() || "sub-fake";
    const record = safeJsonParse(params[1], {});
    const miniData = safeJsonParse(params[2], {});
    const submittedBy = safeJsonParse(params[3], {});
    const submittedAt = new Date().toISOString();
    const row = {
      id,
      record: record && typeof record === "object" ? record : {},
      mini_data: miniData && typeof miniData === "object" ? miniData : {},
      submitted_by: submittedBy && typeof submittedBy === "object" ? submittedBy : {},
      status: "pending",
      submitted_at: submittedAt,
      reviewed_at: null,
      reviewed_by: "",
      review_note: null,
      purged_at: null,
    };
    state.submissions.set(id, row);
    captureEvent({
      type: "submission_insert",
      id,
      record,
      miniData: row.mini_data,
      submittedBy: row.submitted_by,
    });
    return queryResult(
      [
        {
          id,
          status: row.status,
          submitted_at: submittedAt,
          mini_data: cloneJson(row.mini_data),
        },
      ],
      1,
    );
  }

  if (sql.includes("insert into") && sql.includes("mini_submission_files")) {
    const storageKey = String(params[7] || "").trim();
    const fileEvent = {
      type: "file_insert",
      id: String(params[0] || "").trim(),
      submissionId: String(params[1] || "").trim(),
      fileName: String(params[2] || "").trim(),
      mimeType: String(params[3] || "").trim(),
      sizeBytes: Number.parseInt(params[4], 10) || 0,
      storageKey,
    };
    state.submissionFiles.push({
      id: fileEvent.id,
      submission_id: fileEvent.submissionId,
      file_name: fileEvent.fileName,
      mime_type: fileEvent.mimeType,
      size_bytes: fileEvent.sizeBytes,
      storage_key: storageKey,
    });
    captureEvent(fileEvent);
    return queryResult([], 1);
  }

  if (
    sql.includes("select id, record, mini_data, submitted_by, status, submitted_at, purged_at") &&
    sql.includes("mini_client_submissions") &&
    sql.includes("where id = $1") &&
    sql.includes("for update")
  ) {
    const submissionId = String(params[0] || "").trim();
    const submission = normalizeSubmissionForResponse(state.submissions.get(submissionId));
    return queryResult(submission ? [submission] : [], submission ? 1 : 0);
  }

  if (
    sql.includes("select id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note, purged_at") &&
    sql.includes("mini_client_submissions") &&
    sql.includes("order by submitted_at desc, id desc")
  ) {
    const hasStatusFilter = sql.includes("where status = $1");
    const statusFilter = hasStatusFilter ? String(params[0] || "").trim().toLowerCase() : "";
    const limitParam = hasStatusFilter ? params[1] : params[0];
    const limit = Number.parseInt(limitParam, 10);
    const normalizedRows = sortSubmissionsNewestFirst(
      [...state.submissions.values()].map((submission) => normalizeSubmissionForResponse(submission)).filter(Boolean),
    );
    const filteredRows = hasStatusFilter
      ? normalizedRows.filter((row) => String(row.status || "").toLowerCase() === statusFilter)
      : normalizedRows;
    if (!Number.isFinite(limit) || limit <= 0) {
      return queryResult(filteredRows, filteredRows.length);
    }
    return queryResult(filteredRows.slice(0, limit), Math.min(filteredRows.length, limit));
  }

  if (
    sql.includes("update") &&
    sql.includes("mini_client_submissions") &&
    sql.includes("set status = $2") &&
    sql.includes("where id = $1")
  ) {
    const submissionId = String(params[0] || "").trim();
    const nextStatus = String(params[1] || "").trim().toLowerCase();
    const reviewedBy = String(params[2] || "").trim();
    const reviewNote = params[3] === null || params[3] === undefined ? null : String(params[3]);
    const row = state.submissions.get(submissionId);
    if (!row) {
      return queryResult([], 0);
    }

    row.status = nextStatus || row.status;
    row.reviewed_at = new Date().toISOString();
    row.reviewed_by = reviewedBy;
    row.review_note = reviewNote;

    state.submissions.set(submissionId, row);
    captureEvent({
      type: "submission_reviewed",
      id: submissionId,
      status: row.status,
      reviewedBy,
      reviewNote,
    });
    return queryResult([normalizeSubmissionForResponse(row)], 1);
  }

  if (
    sql.includes("update") &&
    sql.includes("mini_client_submissions") &&
    sql.includes("set mini_data = '{}'::jsonb") &&
    sql.includes("submitted_by = '{}'::jsonb")
  ) {
    const submissionIds = getSubmissionIdsFromParam(params[0]);
    let rowCount = 0;
    for (const submissionId of submissionIds) {
      const row = state.submissions.get(submissionId);
      if (!row || row.status === "pending") {
        continue;
      }
      row.mini_data = {};
      row.submitted_by = {};
      row.purged_at = new Date().toISOString();
      state.submissions.set(submissionId, row);
      rowCount += 1;
    }
    return queryResult([], rowCount);
  }

  if (
    sql.includes("update") &&
    sql.includes("mini_client_submissions") &&
    sql.includes("set purged_at = coalesce(purged_at, now())")
  ) {
    const submissionIds = getSubmissionIdsFromParam(params[0]);
    let rowCount = 0;
    for (const submissionId of submissionIds) {
      const row = state.submissions.get(submissionId);
      if (!row || row.status === "pending") {
        continue;
      }
      if (!row.purged_at) {
        row.purged_at = new Date().toISOString();
      }
      state.submissions.set(submissionId, row);
      rowCount += 1;
    }
    return queryResult([], rowCount);
  }

  if (sql.includes("select storage_key") && sql.includes("mini_submission_files") && sql.includes("submission_id = any")) {
    const submissionIds = new Set(getSubmissionIdsFromParam(params[0]));
    const rows = state.submissionFiles
      .filter((file) => submissionIds.has(String(file.submission_id || "").trim()))
      .map((file) => ({
        storage_key: file.storage_key || "",
      }));
    return queryResult(rows, rows.length);
  }

  if (sql.includes("delete from") && sql.includes("mini_submission_files") && sql.includes("submission_id = any")) {
    const submissionIds = new Set(getSubmissionIdsFromParam(params[0]));
    const beforeCount = state.submissionFiles.length;
    state.submissionFiles = state.submissionFiles.filter(
      (file) => !submissionIds.has(String(file.submission_id || "").trim()),
    );
    const deletedCount = Math.max(0, beforeCount - state.submissionFiles.length);
    return queryResult([], deletedCount);
  }

  if (
    sql.includes("insert into") &&
    sql.includes("(id, records, updated_at)") &&
    sql.includes("jsonb_build_array($2::jsonb)")
  ) {
    ensureLegacyStateRow();
    const record = safeJsonParse(params[1], {});
    const writeTimestamp = normalizeIsoTimestamp(params[2]);
    state.legacyState.records = [
      record && typeof record === "object" ? cloneJson(record) : {},
      ...state.legacyState.records,
    ];
    state.legacyState.updated_at = writeTimestamp;
    captureEvent({
      type: "legacy_state_prepend",
      updatedAt: writeTimestamp,
      recordsCount: state.legacyState.records.length,
    });
    return queryResult([], 1);
  }

  if (
    sql.includes("insert into") &&
    sql.includes("(id, records, updated_at)") &&
    sql.includes("'[]'::jsonb") &&
    params.length >= 2
  ) {
    ensureLegacyStateRow();
    state.legacyState.updated_at = normalizeIsoTimestamp(params[1]);
    return queryResult([], 1);
  }

  if (sql.includes("insert into") && sql.includes("(id, records, updated_at)")) {
    ensureLegacyStateRow();
    const records = safeJsonParse(params[1], []);
    const updatedAt = params.length >= 3 ? normalizeIsoTimestamp(params[2]) : new Date().toISOString();
    state.legacyState.records = Array.isArray(records) ? cloneJson(records) : [];
    state.legacyState.updated_at = updatedAt;
    if (sql.includes("returning updated_at")) {
      return queryResult([{ updated_at: updatedAt }], 1);
    }
    return queryResult([], 1);
  }

  if (sql.includes("select records, updated_at from") && sql.includes("where id = $1")) {
    const requestedRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    if (!state.legacyState.exists || requestedRowId !== STATE_ROW_ID) {
      return queryResult([], 0);
    }
    return queryResult(
      [
        {
          records: cloneJson(state.legacyState.records),
          updated_at: state.legacyState.updated_at,
        },
      ],
      1,
    );
  }

  if (sql.includes("select updated_at from") && sql.includes("where id = $1")) {
    const requestedRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    if (!state.legacyState.exists || requestedRowId !== STATE_ROW_ID) {
      return queryResult([], 0);
    }
    return queryResult(
      [
        {
          updated_at: state.legacyState.updated_at,
        },
      ],
      1,
    );
  }

  if (sql.includes("insert into") && sql.includes("client_records_v2")) {
    const id = String(params[0] || "").trim();
    if (!id) {
      return queryResult([], 0);
    }

    const existing = state.v2Records.get(id);
    const record = safeJsonParse(params[1], {});
    const row = {
      id,
      record: record && typeof record === "object" ? cloneJson(record) : {},
      record_hash: String(params[2] || "").trim(),
      source_state_updated_at: normalizeIsoTimestamp(params[7]),
      source_state_row_id: String(params[8] || "").trim() || STATE_ROW_ID,
      updated_at: normalizeIsoTimestamp(params[9]),
    };
    state.v2Records.set(id, row);
    captureEvent({
      type: "v2_record_upsert",
      id,
      sourceStateRowId: row.source_state_row_id,
    });
    if (sql.includes("returning (xmax = 0) as inserted")) {
      return queryResult([{ inserted: !existing }], 1);
    }
    return queryResult([], 1);
  }

  if (
    sql.includes("select id, record, source_state_updated_at, updated_at") &&
    sql.includes("client_records_v2") &&
    sql.includes("where source_state_row_id = $1")
  ) {
    const sourceStateRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    const rows = [...state.v2Records.values()]
      .filter((row) => row.source_state_row_id === sourceStateRowId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((row) => ({
        id: row.id,
        record: cloneJson(row.record),
        source_state_updated_at: row.source_state_updated_at,
        updated_at: row.updated_at,
      }));
    return queryResult(rows, rows.length);
  }

  if (sql.includes("select id, record from") && sql.includes("client_records_v2") && sql.includes("where source_state_row_id = $1")) {
    const sourceStateRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    const rows = [...state.v2Records.values()]
      .filter((row) => row.source_state_row_id === sourceStateRowId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((row) => ({
        id: row.id,
        record: cloneJson(row.record),
      }));
    return queryResult(rows, rows.length);
  }

  if (
    sql.includes("select id, record, record_hash") &&
    sql.includes("client_records_v2") &&
    sql.includes("where source_state_row_id = $1")
  ) {
    const sourceStateRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    const rows = [...state.v2Records.values()]
      .filter((row) => row.source_state_row_id === sourceStateRowId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((row) => ({
        id: row.id,
        record: cloneJson(row.record),
        record_hash: row.record_hash,
      }));
    return queryResult(rows, rows.length);
  }

  if (sql.includes("delete from") && sql.includes("client_records_v2") && sql.includes("where source_state_row_id = $1")) {
    const sourceStateRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    const safeIds = params.length > 1 ? new Set(getSubmissionIdsFromParam(params[1])) : null;
    let deletedCount = 0;
    for (const [id, row] of state.v2Records.entries()) {
      if (row.source_state_row_id !== sourceStateRowId) {
        continue;
      }
      if (safeIds && safeIds.has(id)) {
        continue;
      }
      state.v2Records.delete(id);
      deletedCount += 1;
    }
    return queryResult([], deletedCount);
  }

  if (
    sql.includes("select count(*)::bigint as total from") &&
    sql.includes("client_records_v2") &&
    sql.includes("where source_state_row_id = $1")
  ) {
    const sourceStateRowId = String(params[0] || "").trim() || STATE_ROW_ID;
    const total = [...state.v2Records.values()].filter((row) => row.source_state_row_id === sourceStateRowId).length;
    return queryResult([{ total: String(total) }], 1);
  }

  if (sql.includes("select refresh_token") && sql.includes("quickbooks_auth_state")) {
    return queryResult([], 0);
  }

  if (sql.includes("select 1")) {
    return queryResult([{ "?column?": 1 }], 1);
  }

  return queryResult([], 0);
}

class FakePgClient {
  async query(sql, params) {
    return executeFakeQuery(sql, params);
  }

  release() {
    // no-op
  }
}

class FakePgPool {
  constructor() {}

  async query(sql, params) {
    return executeFakeQuery(sql, params);
  }

  async connect() {
    return new FakePgClient();
  }

  async end() {
    // no-op
  }
}

Module._load = function patchedLoad(request, parent, isMain) {
  if (request === "pg") {
    return {
      Pool: FakePgPool,
    };
  }

  return originalLoad(request, parent, isMain);
};
