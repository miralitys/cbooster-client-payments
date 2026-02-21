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
};
const captureFile = String(process.env.TEST_PG_CAPTURE_FILE || "").trim();

function normalizeSql(rawSql) {
  return String(rawSql || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function safeJsonParse(rawValue, fallbackValue) {
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
    const row = {
      id,
      status: "pending",
      submitted_at: new Date().toISOString(),
      mini_data: miniData && typeof miniData === "object" ? miniData : {},
    };
    state.submissions.set(id, row);
    captureEvent({
      type: "submission_insert",
      id,
      record,
      miniData,
    });
    return queryResult([row], 1);
  }

  if (sql.includes("insert into") && sql.includes("mini_submission_files")) {
    const fileEvent = {
      type: "file_insert",
      id: String(params[0] || "").trim(),
      submissionId: String(params[1] || "").trim(),
      fileName: String(params[2] || "").trim(),
      mimeType: String(params[3] || "").trim(),
      sizeBytes: Number.parseInt(params[4], 10) || 0,
    };
    state.submissionFiles.push({
      id: fileEvent.id,
      submission_id: fileEvent.submissionId,
    });
    captureEvent(fileEvent);
    return queryResult([], 1);
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
