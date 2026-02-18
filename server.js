const path = require("path");
const crypto = require("crypto");
const express = require("express");
const { Pool } = require("pg");

const PORT = Number.parseInt(process.env.PORT || "10000", 10);
const DATABASE_URL = (process.env.DATABASE_URL || "").trim();
const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || "").trim();
const TELEGRAM_ALLOWED_USER_IDS = parseTelegramAllowedUserIds(process.env.TELEGRAM_ALLOWED_USER_IDS);
const TELEGRAM_INIT_DATA_TTL_SEC = parsePositiveInteger(process.env.TELEGRAM_INIT_DATA_TTL_SEC, 86400);
const STATE_ROW_ID = 1;
const DEFAULT_TABLE_NAME = "client_records_state";
const TABLE_NAME = resolveTableName(process.env.DB_TABLE_NAME, DEFAULT_TABLE_NAME);
const DEFAULT_MODERATION_TABLE_NAME = "mini_client_submissions";
const MODERATION_TABLE_NAME = resolveTableName(process.env.DB_MODERATION_TABLE_NAME, DEFAULT_MODERATION_TABLE_NAME);
const DB_SCHEMA = resolveSchemaName(process.env.DB_SCHEMA, "public");
const STATE_TABLE = qualifyTableName(DB_SCHEMA, TABLE_NAME);
const MODERATION_TABLE = qualifyTableName(DB_SCHEMA, MODERATION_TABLE_NAME);
const MODERATION_STATUSES = new Set(["pending", "approved", "rejected"]);
const DEFAULT_MODERATION_LIST_LIMIT = 200;

const RECORD_TEXT_FIELDS = [
  "clientName",
  "closedBy",
  "companyName",
  "serviceType",
  "contractTotals",
  "totalPayments",
  "payment1",
  "payment2",
  "payment3",
  "payment4",
  "payment5",
  "payment6",
  "payment7",
  "futurePayments",
  "notes",
  "collection",
  "dateWhenFullyPaid",
];
const RECORD_DATE_FIELDS = [
  "payment1Date",
  "payment2Date",
  "payment3Date",
  "payment4Date",
  "payment5Date",
  "payment6Date",
  "payment7Date",
  "dateOfCollection",
  "dateWhenWrittenOff",
];
const RECORD_CHECKBOX_FIELDS = ["afterResult", "writtenOff"];
const MINI_ALLOWED_FIELDS = new Set([
  ...RECORD_TEXT_FIELDS,
  ...RECORD_DATE_FIELDS,
  ...RECORD_CHECKBOX_FIELDS,
]);

const app = express();
app.use(express.json({ limit: "10mb" }));

const staticRoot = __dirname;
app.use(express.static(staticRoot));

const pool = DATABASE_URL
  ? new Pool({
      connectionString: DATABASE_URL,
      ssl: shouldUseSsl() ? { rejectUnauthorized: false } : false,
    })
  : null;

let dbReadyPromise = null;

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

function shouldUseSsl() {
  const mode = (process.env.PGSSLMODE || "").toLowerCase();
  return mode !== "disable";
}

function parseTelegramAllowedUserIds(rawValue) {
  if (!rawValue) {
    return new Set();
  }

  return new Set(
    rawValue
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean),
  );
}

function parsePositiveInteger(rawValue, fallbackValue) {
  const parsed = Number.parseInt(rawValue || "", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallbackValue;
  }
  return parsed;
}

function safeEqual(leftValue, rightValue) {
  const left = Buffer.from(leftValue, "utf8");
  const right = Buffer.from(rightValue, "utf8");

  if (left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

function verifyTelegramInitData(rawInitData) {
  if (!TELEGRAM_BOT_TOKEN) {
    return {
      ok: false,
      status: 503,
      error: "Telegram auth is not configured on server.",
    };
  }

  const initData = (rawInitData || "").toString().trim();
  if (!initData) {
    return {
      ok: false,
      status: 401,
      error: "Missing Telegram initData.",
    };
  }

  const params = new URLSearchParams(initData);
  const receivedHash = (params.get("hash") || "").trim().toLowerCase();
  if (!receivedHash) {
    return {
      ok: false,
      status: 401,
      error: "Invalid Telegram initData hash.",
    };
  }

  const authDateRaw = (params.get("auth_date") || "").trim();
  const authDate = Number.parseInt(authDateRaw, 10);
  if (!Number.isFinite(authDate) || authDate <= 0) {
    return {
      ok: false,
      status: 401,
      error: "Invalid Telegram auth_date.",
    };
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  if (Math.abs(nowSeconds - authDate) > TELEGRAM_INIT_DATA_TTL_SEC) {
    return {
      ok: false,
      status: 401,
      error: "Telegram session expired. Reopen Mini App from Telegram chat.",
    };
  }

  const dataCheckString = [...params.entries()]
    .filter(([key]) => key !== "hash")
    .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");

  const secretKey = crypto.createHmac("sha256", "WebAppData").update(TELEGRAM_BOT_TOKEN).digest();
  const expectedHash = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex").toLowerCase();

  if (!safeEqual(receivedHash, expectedHash)) {
    return {
      ok: false,
      status: 401,
      error: "Telegram signature check failed.",
    };
  }

  const user = parseTelegramUser(params.get("user"));
  if (!isTelegramUserAllowed(user)) {
    return {
      ok: false,
      status: 403,
      error: "Telegram user is not allowed.",
    };
  }

  return {
    ok: true,
    user,
    authDate,
  };
}

function parseTelegramUser(rawUser) {
  const userJson = (rawUser || "").toString().trim();
  if (!userJson) {
    return null;
  }

  try {
    const parsed = JSON.parse(userJson);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return null;
  }
}

function isTelegramUserAllowed(user) {
  if (!TELEGRAM_ALLOWED_USER_IDS.size) {
    return true;
  }

  const userId = user?.id;
  if (userId === null || userId === undefined) {
    return false;
  }

  return TELEGRAM_ALLOWED_USER_IDS.has(String(userId));
}

async function ensureDatabaseReady() {
  if (!pool) {
    throw new Error("DATABASE_URL is not configured.");
  }

  if (!dbReadyPromise) {
    dbReadyPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${STATE_TABLE} (
          id BIGINT PRIMARY KEY,
          records JSONB NOT NULL DEFAULT '[]'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(
        `
          INSERT INTO ${STATE_TABLE} (id, records)
          VALUES ($1, '[]'::jsonb)
          ON CONFLICT (id) DO NOTHING
        `,
        [STATE_ROW_ID],
      );

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${MODERATION_TABLE} (
          id TEXT PRIMARY KEY,
          record JSONB NOT NULL,
          submitted_by JSONB,
          status TEXT NOT NULL DEFAULT 'pending',
          submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          reviewed_at TIMESTAMPTZ,
          reviewed_by TEXT,
          review_note TEXT
        )
      `);
    })().catch((error) => {
      dbReadyPromise = null;
      throw error;
    });
  }

  return dbReadyPromise;
}

async function getStoredRecords() {
  await ensureDatabaseReady();
  const result = await pool.query(`SELECT records, updated_at FROM ${STATE_TABLE} WHERE id = $1`, [STATE_ROW_ID]);

  if (!result.rows.length) {
    return { records: [], updatedAt: null };
  }

  const row = result.rows[0];
  return {
    records: Array.isArray(row.records) ? row.records : [],
    updatedAt: row.updated_at || null,
  };
}

async function saveStoredRecords(records) {
  await ensureDatabaseReady();
  const result = await pool.query(
    `
      INSERT INTO ${STATE_TABLE} (id, records, updated_at)
      VALUES ($1, $2::jsonb, NOW())
      ON CONFLICT (id)
      DO UPDATE SET records = EXCLUDED.records, updated_at = NOW()
      RETURNING updated_at
    `,
    [STATE_ROW_ID, JSON.stringify(records)],
  );

  return result.rows[0]?.updated_at || null;
}

function isValidRecordsPayload(value) {
  return Array.isArray(value);
}

function mapModerationRow(row) {
  return {
    id: (row.id || "").toString(),
    status: (row.status || "").toString(),
    client: row.record && typeof row.record === "object" ? row.record : null,
    submittedBy: row.submitted_by && typeof row.submitted_by === "object" ? row.submitted_by : null,
    submittedAt: row.submitted_at ? new Date(row.submitted_at).toISOString() : null,
    reviewedAt: row.reviewed_at ? new Date(row.reviewed_at).toISOString() : null,
    reviewedBy: (row.reviewed_by || "").toString(),
    reviewNote: (row.review_note || "").toString(),
  };
}

function normalizeModerationStatus(rawStatus, options = {}) {
  const { allowAll = false, fallback = "pending" } = options;
  const normalized = (rawStatus || "").toString().trim().toLowerCase();

  if (!normalized) {
    return fallback;
  }

  if (allowAll && normalized === "all") {
    return "all";
  }

  if (MODERATION_STATUSES.has(normalized)) {
    return normalized;
  }

  return null;
}

function getReviewerIdentity(req) {
  const candidates = [
    req.headers["x-user-email"],
    req.headers["x-user"],
    req.headers["x-auth-request-email"],
    req.headers["x-auth-request-user"],
  ];

  for (const candidate of candidates) {
    const reviewer = sanitizeTextValue(candidate, 200);
    if (reviewer) {
      return reviewer;
    }
  }

  return "moderator";
}

async function queueClientSubmission(record, submittedBy) {
  await ensureDatabaseReady();

  const submissionId = `sub-${generateId()}`;
  const submittedByPayload = submittedBy && typeof submittedBy === "object" ? submittedBy : null;
  const result = await pool.query(
    `
      INSERT INTO ${MODERATION_TABLE} (id, record, submitted_by, status)
      VALUES ($1, $2::jsonb, $3::jsonb, 'pending')
      RETURNING id, status, submitted_at
    `,
    [submissionId, JSON.stringify(record), JSON.stringify(submittedByPayload)],
  );

  return {
    id: result.rows[0]?.id || submissionId,
    status: result.rows[0]?.status || "pending",
    submittedAt: result.rows[0]?.submitted_at ? new Date(result.rows[0].submitted_at).toISOString() : null,
  };
}

async function listModerationSubmissions(options = {}) {
  await ensureDatabaseReady();

  const status = normalizeModerationStatus(options.status, {
    allowAll: true,
    fallback: "pending",
  });
  if (!status) {
    return {
      error: "Invalid moderation status filter.",
      items: [],
      status: null,
    };
  }

  const limit = Math.min(
    Math.max(parsePositiveInteger(options.limit, DEFAULT_MODERATION_LIST_LIMIT), 1),
    500,
  );

  let result;
  if (status === "all") {
    result = await pool.query(
      `
        SELECT id, record, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
        FROM ${MODERATION_TABLE}
        ORDER BY submitted_at DESC
        LIMIT $1
      `,
      [limit],
    );
  } else {
    result = await pool.query(
      `
        SELECT id, record, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
        FROM ${MODERATION_TABLE}
        WHERE status = $1
        ORDER BY submitted_at DESC
        LIMIT $2
      `,
      [status, limit],
    );
  }

  return {
    status,
    items: result.rows.map(mapModerationRow),
  };
}

async function reviewClientSubmission(submissionId, decision, reviewedBy, reviewNote) {
  await ensureDatabaseReady();

  const normalizedDecision = normalizeModerationStatus(decision, {
    allowAll: false,
    fallback: null,
  });
  if (!normalizedDecision || normalizedDecision === "pending") {
    return {
      ok: false,
      status: 400,
      error: "Invalid moderation action.",
    };
  }

  const normalizedSubmissionId = sanitizeTextValue(submissionId, 160);
  if (!normalizedSubmissionId) {
    return {
      ok: false,
      status: 400,
      error: "Submission id is required.",
    };
  }

  const normalizedReviewer = sanitizeTextValue(reviewedBy, 200) || "moderator";
  const normalizedReviewNote = sanitizeTextValue(reviewNote, 2000) || null;
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const submissionResult = await client.query(
      `
        SELECT id, record, submitted_by, status, submitted_at
        FROM ${MODERATION_TABLE}
        WHERE id = $1
        FOR UPDATE
      `,
      [normalizedSubmissionId],
    );

    if (!submissionResult.rows.length) {
      await client.query("ROLLBACK");
      return {
        ok: false,
        status: 404,
        error: "Submission not found.",
      };
    }

    const submission = submissionResult.rows[0];
    if (submission.status !== "pending") {
      await client.query("ROLLBACK");
      return {
        ok: false,
        status: 409,
        error: `Submission already reviewed (${submission.status}).`,
      };
    }

    if (normalizedDecision === "approved") {
      const stateResult = await client.query(
        `SELECT records FROM ${STATE_TABLE} WHERE id = $1 FOR UPDATE`,
        [STATE_ROW_ID],
      );

      const currentRecords = Array.isArray(stateResult.rows[0]?.records) ? stateResult.rows[0].records : [];
      currentRecords.unshift(submission.record);

      await client.query(
        `
          INSERT INTO ${STATE_TABLE} (id, records, updated_at)
          VALUES ($1, $2::jsonb, NOW())
          ON CONFLICT (id)
          DO UPDATE SET records = EXCLUDED.records, updated_at = NOW()
        `,
        [STATE_ROW_ID, JSON.stringify(currentRecords)],
      );
    }

    const updateResult = await client.query(
      `
        UPDATE ${MODERATION_TABLE}
        SET status = $2, reviewed_at = NOW(), reviewed_by = $3, review_note = $4
        WHERE id = $1
        RETURNING id, record, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
      `,
      [normalizedSubmissionId, normalizedDecision, normalizedReviewer, normalizedReviewNote],
    );

    await client.query("COMMIT");

    return {
      ok: true,
      status: 200,
      item: mapModerationRow(updateResult.rows[0]),
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

function sanitizeTextValue(value, maxLength = 4000) {
  return (value ?? "").toString().trim().slice(0, maxLength);
}

function toCheckboxValue(value) {
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return normalized === "yes" || normalized === "true" || normalized === "1" ? "Yes" : "";
  }

  return value ? "Yes" : "";
}

function createEmptyRecord() {
  const record = {};

  for (const field of RECORD_TEXT_FIELDS) {
    record[field] = "";
  }

  for (const field of RECORD_DATE_FIELDS) {
    record[field] = "";
  }

  for (const field of RECORD_CHECKBOX_FIELDS) {
    record[field] = "";
  }

  return record;
}

function createRecordFromMiniPayload(rawClient, telegramUser) {
  if (!rawClient || typeof rawClient !== "object") {
    return {
      error: "Payload must include `client` object.",
    };
  }

  const client = {};
  for (const [key, value] of Object.entries(rawClient)) {
    if (MINI_ALLOWED_FIELDS.has(key)) {
      client[key] = value;
    }
  }

  const clientName = sanitizeTextValue(client.clientName, 200);
  if (!clientName) {
    return {
      error: "`clientName` is required.",
    };
  }

  const record = createEmptyRecord();
  record.clientName = clientName;

  for (const field of RECORD_TEXT_FIELDS) {
    if (field === "clientName") {
      continue;
    }

    record[field] = sanitizeTextValue(client[field]);
  }

  for (const field of RECORD_DATE_FIELDS) {
    const rawDate = client[field] ?? "";
    const normalizedDate = normalizeDateForStorage(rawDate);
    if (sanitizeTextValue(rawDate, 100) && normalizedDate === null) {
      return {
        error: `Invalid date in field "${field}". Use MM/DD/YYYY.`,
      };
    }

    record[field] = normalizedDate || "";
  }

  for (const field of RECORD_CHECKBOX_FIELDS) {
    record[field] = toCheckboxValue(client[field]);
  }

  if (!record.closedBy && telegramUser) {
    record.closedBy = buildDefaultClosedBy(telegramUser);
  }

  if (record.writtenOff === "Yes" && !record.dateWhenWrittenOff) {
    record.dateWhenWrittenOff = getTodayDateUs();
  }

  return {
    record: {
      id: generateId(),
      createdAt: new Date().toISOString(),
      ...record,
    },
  };
}

function buildDefaultClosedBy(telegramUser) {
  const username = sanitizeTextValue(telegramUser?.username, 120);
  if (username) {
    return `@${username}`;
  }

  const firstName = sanitizeTextValue(telegramUser?.first_name, 120);
  const lastName = sanitizeTextValue(telegramUser?.last_name, 120);
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    return fullName;
  }

  const userId = sanitizeTextValue(telegramUser?.id, 50);
  return userId ? `tg:${userId}` : "";
}

function generateId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`;
}

function getTodayDateUs() {
  const today = new Date();
  const month = String(today.getMonth() + 1).padStart(2, "0");
  const day = String(today.getDate()).padStart(2, "0");
  const year = String(today.getFullYear());
  return `${month}/${day}/${year}`;
}

function parseDateValue(rawValue) {
  const value = sanitizeTextValue(rawValue, 100);
  if (!value) {
    return null;
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }
    return null;
  }

  const usMatch = value.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2}|\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    let year = Number(usMatch[3]);
    if (usMatch[3].length === 2) {
      year += 2000;
    }

    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }
    return null;
  }

  return null;
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function formatDateTimestampUs(timestamp) {
  const date = new Date(timestamp);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const year = String(date.getUTCFullYear());
  return `${month}/${day}/${year}`;
}

function normalizeDateForStorage(rawValue) {
  const value = sanitizeTextValue(rawValue, 100);
  if (!value) {
    return "";
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return null;
  }

  return formatDateTimestampUs(timestamp);
}

function buildPublicErrorPayload(error, fallbackMessage) {
  const payload = {
    error: fallbackMessage,
  };

  const code = sanitizeTextValue(error?.code, 40);
  const message = sanitizeTextValue(error?.message, 600);
  const detail = sanitizeTextValue(error?.detail, 600);
  const hint = sanitizeTextValue(error?.hint, 600);

  if (code) {
    payload.code = code;
  }

  if (message) {
    payload.details = message;
  }

  if (detail) {
    payload.dbDetail = detail;
  }

  if (hint) {
    payload.dbHint = hint;
  }

  return payload;
}

function resolveDbHttpStatus(error, fallbackStatus = 500) {
  const code = sanitizeTextValue(error?.code, 40).toUpperCase();
  const unavailableCodes = new Set([
    "28P01",
    "3D000",
    "08001",
    "08003",
    "08004",
    "08006",
    "57P01",
    "57P02",
    "57P03",
    "ENOTFOUND",
    "ECONNREFUSED",
    "ECONNRESET",
    "ETIMEDOUT",
    "EHOSTUNREACH",
  ]);

  if (unavailableCodes.has(code)) {
    return 503;
  }

  return fallbackStatus;
}

app.get("/api/health", async (_req, res) => {
  if (!pool) {
    res.status(503).json({
      ok: false,
      error: "DATABASE_URL is not configured",
    });
    return;
  }

  try {
    await ensureDatabaseReady();
    await pool.query("SELECT 1");
    res.json({
      ok: true,
    });
  } catch (error) {
    console.error("GET /api/health failed:", error);
    res.status(resolveDbHttpStatus(error, 503)).json({
      ok: false,
      ...buildPublicErrorPayload(error, "Database connection failed"),
    });
  }
});

app.get("/api/records", async (_req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    const state = await getStoredRecords();
    res.json({
      records: state.records,
      updatedAt: state.updatedAt,
    });
  } catch (error) {
    console.error("GET /api/records failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load records"));
  }
});

app.put("/api/records", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  const nextRecords = req.body?.records;
  if (!isValidRecordsPayload(nextRecords)) {
    res.status(400).json({
      error: "Payload must include `records` as an array.",
    });
    return;
  }

  try {
    const updatedAt = await saveStoredRecords(nextRecords);
    res.json({
      ok: true,
      updatedAt,
    });
  } catch (error) {
    console.error("PUT /api/records failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to save records"));
  }
});

app.post("/api/mini/clients", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  const authResult = verifyTelegramInitData(req.body?.initData);
  if (!authResult.ok) {
    res.status(authResult.status).json({
      error: authResult.error,
    });
    return;
  }

  const creationResult = createRecordFromMiniPayload(req.body?.client, authResult.user);
  if (!creationResult.record) {
    res.status(400).json({
      error: creationResult.error || "Invalid client payload.",
    });
    return;
  }

  try {
    const submission = await queueClientSubmission(creationResult.record, authResult.user);
    res.status(201).json({
      ok: true,
      status: submission.status,
      submissionId: submission.id,
      submittedAt: submission.submittedAt,
    });
  } catch (error) {
    console.error("POST /api/mini/clients failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to submit client"));
  }
});

app.get("/api/moderation/submissions", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    const result = await listModerationSubmissions({
      status: req.query.status,
      limit: req.query.limit,
    });

    if (result.error) {
      res.status(400).json({
        error: result.error,
      });
      return;
    }

    res.json({
      status: result.status,
      items: result.items,
    });
  } catch (error) {
    console.error("GET /api/moderation/submissions failed:", error);
    res
      .status(resolveDbHttpStatus(error))
      .json(buildPublicErrorPayload(error, "Failed to load moderation submissions"));
  }
});

app.post("/api/moderation/submissions/:id/approve", async (req, res) => {
  try {
    const result = await reviewClientSubmission(
      req.params.id,
      "approved",
      getReviewerIdentity(req),
      req.body?.reviewNote,
    );

    if (!result.ok) {
      res.status(result.status).json({
        error: result.error,
      });
      return;
    }

    res.json({
      ok: true,
      item: result.item,
    });
  } catch (error) {
    console.error("POST /api/moderation/submissions/:id/approve failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to approve submission"));
  }
});

app.post("/api/moderation/submissions/:id/reject", async (req, res) => {
  try {
    const result = await reviewClientSubmission(
      req.params.id,
      "rejected",
      getReviewerIdentity(req),
      req.body?.reviewNote,
    );

    if (!result.ok) {
      res.status(result.status).json({
        error: result.error,
      });
      return;
    }

    res.json({
      ok: true,
      item: result.item,
    });
  } catch (error) {
    console.error("POST /api/moderation/submissions/:id/reject failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to reject submission"));
  }
});

app.get("/mini", (_req, res) => {
  res.sendFile(path.join(staticRoot, "mini.html"));
});

app.get("/Client_Payments", (_req, res) => {
  res.sendFile(path.join(staticRoot, "client-payments.html"));
});

app.get("/moderation", (_req, res) => {
  res.redirect(302, "/");
});

app.use("/api", (_req, res) => {
  res.status(404).json({
    error: "API route not found",
  });
});

app.get("*", (_req, res) => {
  res.sendFile(path.join(staticRoot, "index.html"));
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
  if (!pool) {
    console.warn("DATABASE_URL is missing. API routes will return 503 until configured.");
  }
  if (!TELEGRAM_BOT_TOKEN) {
    console.warn("Mini App write API is disabled. Set TELEGRAM_BOT_TOKEN to enable Telegram auth.");
  }
});
