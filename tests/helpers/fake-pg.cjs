"use strict";

const Module = require("node:module");

if (process.env.TEST_USE_FAKE_PG !== "1") {
  return;
}

const originalLoad = Module._load;
const state = {
  submissions: new Map(),
  submissionFiles: [],
};

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
    const miniData = safeJsonParse(params[2], {});
    const row = {
      id,
      status: "pending",
      submitted_at: new Date().toISOString(),
      mini_data: miniData && typeof miniData === "object" ? miniData : {},
    };
    state.submissions.set(id, row);
    return queryResult([row], 1);
  }

  if (sql.includes("insert into") && sql.includes("mini_submission_files")) {
    state.submissionFiles.push({
      id: String(params[0] || "").trim(),
      submission_id: String(params[1] || "").trim(),
    });
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

