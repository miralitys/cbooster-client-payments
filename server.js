const path = require("path");
const crypto = require("crypto");
const express = require("express");
const multer = require("multer");
const { Pool } = require("pg");

const PORT = Number.parseInt(process.env.PORT || "10000", 10);
const DATABASE_URL = (process.env.DATABASE_URL || "").trim();
const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || "").trim();
const TELEGRAM_ALLOWED_USER_IDS = parseTelegramAllowedUserIds(process.env.TELEGRAM_ALLOWED_USER_IDS);
const TELEGRAM_INIT_DATA_TTL_SEC = parsePositiveInteger(process.env.TELEGRAM_INIT_DATA_TTL_SEC, 86400);
const TELEGRAM_REQUIRED_CHAT_ID = parseOptionalTelegramChatId(process.env.TELEGRAM_REQUIRED_CHAT_ID);
const TELEGRAM_NOTIFY_CHAT_ID = (process.env.TELEGRAM_NOTIFY_CHAT_ID || "").toString().trim();
const TELEGRAM_NOTIFY_THREAD_ID = parseOptionalPositiveInteger(process.env.TELEGRAM_NOTIFY_THREAD_ID);
const TELEGRAM_MEMBER_ALLOWED_STATUSES = new Set(["member", "administrator", "creator", "restricted"]);
const STATE_ROW_ID = 1;
const DEFAULT_TABLE_NAME = "client_records_state";
const TABLE_NAME = resolveTableName(process.env.DB_TABLE_NAME, DEFAULT_TABLE_NAME);
const DEFAULT_MODERATION_TABLE_NAME = "mini_client_submissions";
const MODERATION_TABLE_NAME = resolveTableName(process.env.DB_MODERATION_TABLE_NAME, DEFAULT_MODERATION_TABLE_NAME);
const DEFAULT_MODERATION_FILES_TABLE_NAME = "mini_submission_files";
const MODERATION_FILES_TABLE_NAME = resolveTableName(
  process.env.DB_MODERATION_FILES_TABLE_NAME,
  DEFAULT_MODERATION_FILES_TABLE_NAME,
);
const DB_SCHEMA = resolveSchemaName(process.env.DB_SCHEMA, "public");
const STATE_TABLE = qualifyTableName(DB_SCHEMA, TABLE_NAME);
const MODERATION_TABLE = qualifyTableName(DB_SCHEMA, MODERATION_TABLE_NAME);
const MODERATION_FILES_TABLE = qualifyTableName(DB_SCHEMA, MODERATION_FILES_TABLE_NAME);
const MODERATION_STATUSES = new Set(["pending", "approved", "rejected"]);
const DEFAULT_MODERATION_LIST_LIMIT = 200;
const MINI_MAX_ATTACHMENTS_COUNT = 10;
const MINI_MAX_ATTACHMENT_SIZE_BYTES = 10 * 1024 * 1024;
const MINI_MAX_ATTACHMENTS_TOTAL_SIZE_BYTES = 40 * 1024 * 1024;
const MINI_BLOCKED_FILE_EXTENSIONS = new Set([
  ".html",
  ".htm",
  ".xhtml",
  ".shtml",
  ".js",
  ".mjs",
  ".cjs",
  ".jsx",
  ".ts",
  ".tsx",
  ".sh",
  ".bash",
  ".zsh",
  ".bat",
  ".cmd",
  ".ps1",
  ".psm1",
  ".py",
  ".rb",
  ".php",
  ".pl",
  ".cgi",
]);
const MINI_BLOCKED_MIME_TYPES = new Set([
  "text/html",
  "application/xhtml+xml",
  "application/javascript",
  "text/javascript",
  "application/ecmascript",
  "text/ecmascript",
  "application/x-javascript",
  "application/x-httpd-php",
]);
const MINI_BLOCKED_MIME_PATTERNS = [/javascript/i, /ecmascript/i, /x-sh/i, /shellscript/i, /python/i, /xhtml/i];

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
const MINI_EXTRA_TEXT_FIELDS = ["leadSource", "ssn", "clientPhoneNumber", "futurePayment", "identityIq", "clientEmailAddress"];
const MINI_EXTRA_FIELD_SET = new Set(MINI_EXTRA_TEXT_FIELDS);
const MINI_EXTRA_MAX_LENGTH = {
  leadSource: 200,
  ssn: 64,
  clientPhoneNumber: 64,
  futurePayment: 120,
  identityIq: 2000,
  clientEmailAddress: 320,
};
const MINI_ALLOWED_FIELDS = new Set([
  ...RECORD_TEXT_FIELDS,
  ...RECORD_DATE_FIELDS,
  ...RECORD_CHECKBOX_FIELDS,
  ...MINI_EXTRA_TEXT_FIELDS,
]);
const TELEGRAM_NOTIFICATION_FIELD_ORDER = [
  ...RECORD_TEXT_FIELDS,
  ...RECORD_DATE_FIELDS,
  ...RECORD_CHECKBOX_FIELDS,
  ...MINI_EXTRA_TEXT_FIELDS,
];
const TELEGRAM_NOTIFICATION_FIELD_LABELS = {
  clientName: "Client name",
  closedBy: "Closed by",
  companyName: "Company name",
  serviceType: "Service type",
  contractTotals: "Contract totals",
  totalPayments: "Total payments",
  payment1: "Payment 1",
  payment2: "Payment 2",
  payment3: "Payment 3",
  payment4: "Payment 4",
  payment5: "Payment 5",
  payment6: "Payment 6",
  payment7: "Payment 7",
  futurePayments: "Future payments",
  notes: "Notes",
  collection: "Collection",
  dateWhenFullyPaid: "Date when fully paid",
  payment1Date: "Payment 1 date",
  payment2Date: "Payment 2 date",
  payment3Date: "Payment 3 date",
  payment4Date: "Payment 4 date",
  payment5Date: "Payment 5 date",
  payment6Date: "Payment 6 date",
  payment7Date: "Payment 7 date",
  dateOfCollection: "Date of collection",
  dateWhenWrittenOff: "Date when written off",
  afterResult: "After result",
  writtenOff: "Written off",
  leadSource: "Lead source",
  ssn: "SSN",
  clientPhoneNumber: "Client phone number",
  futurePayment: "Future payment",
  identityIq: "IdentityIQ",
  clientEmailAddress: "Client email address",
};

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
const miniAttachmentsUploadMiddleware = multer({
  storage: multer.memoryStorage(),
  limits: {
    files: MINI_MAX_ATTACHMENTS_COUNT,
    fileSize: MINI_MAX_ATTACHMENT_SIZE_BYTES,
  },
}).array("attachments", MINI_MAX_ATTACHMENTS_COUNT);

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

function parseOptionalPositiveInteger(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return null;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function parseOptionalTelegramChatId(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "";
  }

  // Telegram chat id may be negative for groups/supergroups.
  if (!/^-?\d+$/.test(value)) {
    return "";
  }

  return value;
}

function createHttpError(message, status = 400) {
  const error = new Error(message);
  error.httpStatus = status;
  return error;
}

function isMultipartRequest(req) {
  const contentType = (req.headers["content-type"] || "").toString().toLowerCase();
  return contentType.includes("multipart/form-data");
}

function parseMiniMultipartRequest(req, res) {
  return new Promise((resolve, reject) => {
    miniAttachmentsUploadMiddleware(req, res, (error) => {
      if (!error) {
        resolve();
        return;
      }

      if (error instanceof multer.MulterError) {
        if (error.code === "LIMIT_FILE_COUNT" || error.code === "LIMIT_UNEXPECTED_FILE") {
          reject(createHttpError(`You can upload up to ${MINI_MAX_ATTACHMENTS_COUNT} files.`, 400));
          return;
        }

        if (error.code === "LIMIT_FILE_SIZE") {
          reject(
            createHttpError(
              `Each file must be up to ${Math.floor(MINI_MAX_ATTACHMENT_SIZE_BYTES / (1024 * 1024))} MB.`,
              400,
            ),
          );
          return;
        }
      }

      reject(error);
    });
  });
}

function parseMiniClientPayload(req) {
  const initData = sanitizeTextValue(req.body?.initData, 12000);
  const client = parseMiniClientObject(req.body?.client);

  if (!initData) {
    return {
      error: "Missing Telegram initData.",
      status: 400,
    };
  }

  if (!client) {
    return {
      error: "Payload must include `client` object.",
      status: 400,
    };
  }

  return {
    initData,
    client,
  };
}

function parseMiniClientObject(rawClient) {
  if (rawClient && typeof rawClient === "object" && !Array.isArray(rawClient)) {
    return rawClient;
  }

  const json = (rawClient || "").toString().trim();
  if (!json) {
    return null;
  }

  try {
    const parsed = JSON.parse(json);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed;
    }
  } catch {
    return null;
  }

  return null;
}

function sanitizeAttachmentFileName(rawFileName) {
  const baseName = path.basename((rawFileName || "").toString());
  const normalized = baseName.replace(/[^\w.\- ()[\]]+/g, "_").replace(/\s+/g, " ").trim();
  const safeName = normalized || "attachment";
  return safeName.slice(0, 180);
}

function normalizeAttachmentMimeType(rawMimeType) {
  const normalized = sanitizeTextValue(rawMimeType, 120).toLowerCase();
  return normalized || "application/octet-stream";
}

function getMiniAttachmentBlockReason(fileName, mimeType) {
  const extension = path.extname(fileName).toLowerCase();
  if (MINI_BLOCKED_FILE_EXTENSIONS.has(extension)) {
    return `File "${fileName}" is not allowed. Script and HTML files are blocked.`;
  }

  if (MINI_BLOCKED_MIME_TYPES.has(mimeType)) {
    return `File "${fileName}" is not allowed. Script and HTML files are blocked.`;
  }

  for (const pattern of MINI_BLOCKED_MIME_PATTERNS) {
    if (pattern.test(mimeType)) {
      return `File "${fileName}" is not allowed. Script and HTML files are blocked.`;
    }
  }

  return "";
}

function buildMiniSubmissionAttachments(rawFiles) {
  const files = Array.isArray(rawFiles) ? rawFiles : [];
  if (!files.length) {
    return {
      attachments: [],
    };
  }

  if (files.length > MINI_MAX_ATTACHMENTS_COUNT) {
    return {
      error: `You can upload up to ${MINI_MAX_ATTACHMENTS_COUNT} files.`,
      status: 400,
    };
  }

  let totalSizeBytes = 0;
  const attachments = [];

  for (const file of files) {
    const fileName = sanitizeAttachmentFileName(file?.originalname);
    const mimeType = normalizeAttachmentMimeType(file?.mimetype);
    const buffer = Buffer.isBuffer(file?.buffer) ? file.buffer : null;

    if (!buffer || !buffer.length) {
      return {
        error: `Failed to read "${fileName}". Please try uploading the file again.`,
        status: 400,
      };
    }

    const sizeBytes = Number.parseInt(file?.size, 10);
    const normalizedSize = Number.isFinite(sizeBytes) && sizeBytes > 0 ? sizeBytes : buffer.length;
    if (normalizedSize > MINI_MAX_ATTACHMENT_SIZE_BYTES) {
      return {
        error: `File "${fileName}" exceeds ${Math.floor(MINI_MAX_ATTACHMENT_SIZE_BYTES / (1024 * 1024))} MB limit.`,
        status: 400,
      };
    }

    totalSizeBytes += normalizedSize;
    if (totalSizeBytes > MINI_MAX_ATTACHMENTS_TOTAL_SIZE_BYTES) {
      return {
        error: `Total attachment size must not exceed ${Math.floor(MINI_MAX_ATTACHMENTS_TOTAL_SIZE_BYTES / (1024 * 1024))} MB.`,
        status: 400,
      };
    }

    const blockedReason = getMiniAttachmentBlockReason(fileName, mimeType);
    if (blockedReason) {
      return {
        error: blockedReason,
        status: 400,
      };
    }

    attachments.push({
      id: `file-${generateId()}`,
      fileName,
      mimeType,
      sizeBytes: normalizedSize,
      content: buffer,
    });
  }

  return {
    attachments,
  };
}

function isPreviewableAttachmentMimeType(mimeType) {
  const normalized = normalizeAttachmentMimeType(mimeType);
  if (normalized === "image/svg+xml") {
    return false;
  }

  return normalized.startsWith("image/") || normalized === "application/pdf";
}

function buildContentDisposition(dispositionType, fileName) {
  const safeName = sanitizeAttachmentFileName(fileName);
  const asciiFallback = safeName.replace(/[^\x20-\x7E]/g, "_").replace(/[\\"]/g, "_");
  const encodedName = encodeURIComponent(safeName).replace(/['()*]/g, (char) =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`,
  );
  return `${dispositionType}; filename="${asciiFallback}"; filename*=UTF-8''${encodedName}`;
}

function byteaToBuffer(value) {
  if (Buffer.isBuffer(value)) {
    return value;
  }

  if (typeof value === "string" && value.startsWith("\\x")) {
    return Buffer.from(value.slice(2), "hex");
  }

  return Buffer.from([]);
}

function safeEqual(leftValue, rightValue) {
  const left = Buffer.from(leftValue, "utf8");
  const right = Buffer.from(rightValue, "utf8");

  if (left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

async function verifyTelegramInitData(rawInitData) {
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
  const accessResult = await verifyTelegramUserAccess(user);
  if (!accessResult.ok) {
    return accessResult;
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

async function verifyTelegramUserAccess(user) {
  if (!isTelegramUserAllowed(user)) {
    return {
      ok: false,
      status: 403,
      error: "Telegram user is not allowed.",
    };
  }

  if (!TELEGRAM_REQUIRED_CHAT_ID) {
    return {
      ok: true,
    };
  }

  const membershipResult = await verifyTelegramGroupMembership(user);
  if (!membershipResult.ok) {
    return membershipResult;
  }

  return {
    ok: true,
  };
}

async function verifyTelegramGroupMembership(user) {
  const userId = sanitizeTextValue(user?.id, 50);
  if (!userId) {
    return {
      ok: false,
      status: 403,
      error: "Only members of the allowed Telegram group can use Mini App.",
    };
  }

  let response;
  try {
    const query = new URLSearchParams({
      chat_id: TELEGRAM_REQUIRED_CHAT_ID,
      user_id: userId,
    });
    response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getChatMember?${query.toString()}`);
  } catch (error) {
    console.error("Telegram getChatMember network failed:", error);
    return {
      ok: false,
      status: 503,
      error: "Telegram membership check failed. Try again in a moment.",
    };
  }

  const responseText = await response.text();
  let body = null;
  try {
    body = responseText ? JSON.parse(responseText) : null;
  } catch {
    body = null;
  }

  if (!response.ok || !body?.ok) {
    const description = sanitizeTextValue(body?.description || responseText, 400);
    if (response.status === 400 || response.status === 403) {
      console.warn("Telegram getChatMember denied:", description || response.statusText);
      return {
        ok: false,
        status: 403,
        error: "Only members of the allowed Telegram group can use Mini App.",
      };
    }

    console.error("Telegram getChatMember failed:", response.status, description || response.statusText);
    return {
      ok: false,
      status: 503,
      error: "Telegram membership check failed. Try again in a moment.",
    };
  }

  const memberStatus = sanitizeTextValue(body?.result?.status, 40).toLowerCase();
  if (!TELEGRAM_MEMBER_ALLOWED_STATUSES.has(memberStatus)) {
    return {
      ok: false,
      status: 403,
      error: "Only members of the allowed Telegram group can use Mini App.",
    };
  }

  return {
    ok: true,
  };
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
          mini_data JSONB NOT NULL DEFAULT '{}'::jsonb,
          submitted_by JSONB,
          status TEXT NOT NULL DEFAULT 'pending',
          submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          reviewed_at TIMESTAMPTZ,
          reviewed_by TEXT,
          review_note TEXT
        )
      `);

      await pool.query(`
        ALTER TABLE ${MODERATION_TABLE}
        ADD COLUMN IF NOT EXISTS mini_data JSONB NOT NULL DEFAULT '{}'::jsonb
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${MODERATION_FILES_TABLE} (
          id TEXT PRIMARY KEY,
          submission_id TEXT NOT NULL REFERENCES ${MODERATION_TABLE}(id) ON DELETE CASCADE,
          file_name TEXT NOT NULL,
          mime_type TEXT NOT NULL,
          size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
          content BYTEA NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS ${MODERATION_FILES_TABLE_NAME}_submission_idx
        ON ${MODERATION_FILES_TABLE} (submission_id)
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
    miniData: row.mini_data && typeof row.mini_data === "object" ? row.mini_data : {},
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

async function queueClientSubmission(record, submittedBy, miniData = {}, attachments = []) {
  await ensureDatabaseReady();

  const submissionId = `sub-${generateId()}`;
  const submittedByPayload = submittedBy && typeof submittedBy === "object" ? submittedBy : null;
  const miniDataPayload = miniData && typeof miniData === "object" ? miniData : {};
  const normalizedAttachments = Array.isArray(attachments) ? attachments : [];
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `
        INSERT INTO ${MODERATION_TABLE} (id, record, mini_data, submitted_by, status)
        VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, 'pending')
        RETURNING id, status, submitted_at, mini_data
      `,
      [submissionId, JSON.stringify(record), JSON.stringify(miniDataPayload), JSON.stringify(submittedByPayload)],
    );

    for (const attachment of normalizedAttachments) {
      await client.query(
        `
          INSERT INTO ${MODERATION_FILES_TABLE} (id, submission_id, file_name, mime_type, size_bytes, content)
          VALUES ($1, $2, $3, $4, $5, $6)
        `,
        [
          sanitizeTextValue(attachment.id, 180),
          submissionId,
          sanitizeAttachmentFileName(attachment.fileName),
          normalizeAttachmentMimeType(attachment.mimeType),
          Number.parseInt(attachment.sizeBytes, 10) || 0,
          attachment.content,
        ],
      );
    }

    await client.query("COMMIT");

    return {
      id: result.rows[0]?.id || submissionId,
      status: result.rows[0]?.status || "pending",
      submittedAt: result.rows[0]?.submitted_at ? new Date(result.rows[0].submitted_at).toISOString() : null,
      miniData: result.rows[0]?.mini_data && typeof result.rows[0].mini_data === "object" ? result.rows[0].mini_data : {},
      attachmentsCount: normalizedAttachments.length,
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

async function listPendingSubmissionFiles(submissionId) {
  await ensureDatabaseReady();

  const normalizedSubmissionId = sanitizeTextValue(submissionId, 180);
  if (!normalizedSubmissionId) {
    return {
      ok: false,
      status: 400,
      error: "Submission id is required.",
    };
  }

  const submissionResult = await pool.query(
    `SELECT id, status FROM ${MODERATION_TABLE} WHERE id = $1`,
    [normalizedSubmissionId],
  );
  if (!submissionResult.rows.length) {
    return {
      ok: false,
      status: 404,
      error: "Submission not found.",
    };
  }

  const status = sanitizeTextValue(submissionResult.rows[0]?.status, 40).toLowerCase();
  if (status !== "pending") {
    return {
      ok: false,
      status: 409,
      error: "Files are available only while submission is pending moderation.",
    };
  }

  const filesResult = await pool.query(
    `
      SELECT id, file_name, mime_type, size_bytes, created_at
      FROM ${MODERATION_FILES_TABLE}
      WHERE submission_id = $1
      ORDER BY created_at ASC, id ASC
    `,
    [normalizedSubmissionId],
  );

  const items = filesResult.rows.map((row) => ({
    id: sanitizeTextValue(row.id, 180),
    fileName: sanitizeAttachmentFileName(row.file_name),
    mimeType: normalizeAttachmentMimeType(row.mime_type),
    sizeBytes: Number.parseInt(row.size_bytes, 10) || 0,
    createdAt: row.created_at ? new Date(row.created_at).toISOString() : null,
  }));

  return {
    ok: true,
    submissionId: normalizedSubmissionId,
    items,
  };
}

async function getPendingSubmissionFile(submissionId, fileId) {
  await ensureDatabaseReady();

  const normalizedSubmissionId = sanitizeTextValue(submissionId, 180);
  const normalizedFileId = sanitizeTextValue(fileId, 180);
  if (!normalizedSubmissionId || !normalizedFileId) {
    return {
      ok: false,
      status: 400,
      error: "Submission id and file id are required.",
    };
  }

  const filesResult = await pool.query(
    `
      SELECT f.id, f.file_name, f.mime_type, f.size_bytes, f.content, s.status AS submission_status
      FROM ${MODERATION_FILES_TABLE} f
      JOIN ${MODERATION_TABLE} s ON s.id = f.submission_id
      WHERE f.submission_id = $1 AND f.id = $2
      LIMIT 1
    `,
    [normalizedSubmissionId, normalizedFileId],
  );

  if (!filesResult.rows.length) {
    return {
      ok: false,
      status: 404,
      error: "File not found.",
    };
  }

  const row = filesResult.rows[0];
  const status = sanitizeTextValue(row.submission_status, 40).toLowerCase();
  if (status !== "pending") {
    return {
      ok: false,
      status: 409,
      error: "Files are available only while submission is pending moderation.",
    };
  }

  return {
    ok: true,
    file: {
      id: sanitizeTextValue(row.id, 180),
      fileName: sanitizeAttachmentFileName(row.file_name),
      mimeType: normalizeAttachmentMimeType(row.mime_type),
      sizeBytes: Number.parseInt(row.size_bytes, 10) || 0,
      content: byteaToBuffer(row.content),
    },
  };
}

function buildTelegramUserLabel(user) {
  if (!user || typeof user !== "object") {
    return "";
  }

  const username = sanitizeTextValue(user.username, 120);
  if (username) {
    return `@${username}`;
  }

  const firstName = sanitizeTextValue(user.first_name, 120);
  const lastName = sanitizeTextValue(user.last_name, 120);
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    return fullName;
  }

  return "";
}

function normalizeTelegramMessageFieldValue(value, maxLength = 600) {
  return sanitizeTextValue(value, maxLength).replace(/\s+/g, " ").trim();
}

function buildTelegramSubmissionMessage(record, miniData, _submission, telegramUser, attachments = []) {
  const lines = ["New client submission from Mini App"];

  const submittedBy = buildTelegramUserLabel(telegramUser);
  if (submittedBy) {
    lines.push(`Submitted by: ${submittedBy}`);
  }

  lines.push("");
  lines.push("Client data:");

  for (const field of TELEGRAM_NOTIFICATION_FIELD_ORDER) {
    const label = TELEGRAM_NOTIFICATION_FIELD_LABELS[field] || field;
    if (field === "afterResult" || field === "writtenOff") {
      if (toCheckboxValue(record?.[field]) !== "Yes") {
        continue;
      }
      lines.push(`- ${label}: Yes`);
      continue;
    }

    const source = MINI_EXTRA_FIELD_SET.has(field) ? miniData : record;
    const value = normalizeTelegramMessageFieldValue(source?.[field]);
    if (!value) {
      continue;
    }

    lines.push(`- ${label}: ${value}`);
  }

  const attachmentsCount = Array.isArray(attachments) ? attachments.length : 0;
  if (attachmentsCount > 0) {
    lines.push(`- Attachments: ${attachmentsCount}`);
  }

  const message = lines.join("\n").trim();
  const TELEGRAM_MAX_MESSAGE_LENGTH = 3900;
  if (message.length <= TELEGRAM_MAX_MESSAGE_LENGTH) {
    return message;
  }

  return `${message.slice(0, TELEGRAM_MAX_MESSAGE_LENGTH - 3)}...`;
}

async function sendMiniSubmissionTelegramAttachments(submission, attachments = []) {
  const normalizedAttachments = Array.isArray(attachments)
    ? attachments
        .map((attachment) => ({
          fileName: sanitizeAttachmentFileName(attachment?.fileName),
          mimeType: normalizeAttachmentMimeType(attachment?.mimeType),
          content: Buffer.isBuffer(attachment?.content) ? attachment.content : null,
        }))
        .filter((attachment) => attachment.content && attachment.content.length)
    : [];
  if (!normalizedAttachments.length) {
    return;
  }

  const submissionId = sanitizeTextValue(submission?.id, 140);
  if (normalizedAttachments.length === 1) {
    const attachment = normalizedAttachments[0];
    const payload = new FormData();
    payload.append("chat_id", TELEGRAM_NOTIFY_CHAT_ID);
    if (TELEGRAM_NOTIFY_THREAD_ID) {
      payload.append("message_thread_id", String(TELEGRAM_NOTIFY_THREAD_ID));
    }
    if (submissionId) {
      const caption = `Submission ${submissionId} · file 1/1`;
      payload.append("caption", sanitizeTextValue(caption, 900));
    }
    payload.append("document", new Blob([attachment.content], { type: attachment.mimeType }), attachment.fileName);

    const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendDocument`, {
      method: "POST",
      body: payload,
    });

    const responseText = await response.text();
    if (!response.ok) {
      throw new Error(`Telegram sendDocument HTTP ${response.status}: ${sanitizeTextValue(responseText, 700)}`);
    }

    let body;
    try {
      body = JSON.parse(responseText);
    } catch {
      body = null;
    }

    if (!body?.ok) {
      const description = sanitizeTextValue(body?.description || responseText, 700) || "Unknown Telegram API error.";
      throw new Error(`Telegram sendDocument failed: ${description}`);
    }
    return;
  }

  const mediaPayload = new FormData();
  mediaPayload.append("chat_id", TELEGRAM_NOTIFY_CHAT_ID);
  if (TELEGRAM_NOTIFY_THREAD_ID) {
    mediaPayload.append("message_thread_id", String(TELEGRAM_NOTIFY_THREAD_ID));
  }

  const media = normalizedAttachments.map((attachment, index) => {
    const item = {
      type: "document",
      media: `attach://file_${index}`,
    };

    if (submissionId && index === 0) {
      item.caption = sanitizeTextValue(
        `Submission ${submissionId} · ${normalizedAttachments.length} files`,
        900,
      );
    }

    return item;
  });

  mediaPayload.append("media", JSON.stringify(media));
  for (let index = 0; index < normalizedAttachments.length; index += 1) {
    const attachment = normalizedAttachments[index];
    mediaPayload.append(
      `file_${index}`,
      new Blob([attachment.content], { type: attachment.mimeType }),
      attachment.fileName,
    );
  }

  const mediaResponse = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMediaGroup`, {
    method: "POST",
    body: mediaPayload,
  });

  const mediaResponseText = await mediaResponse.text();
  if (!mediaResponse.ok) {
    throw new Error(`Telegram sendMediaGroup HTTP ${mediaResponse.status}: ${sanitizeTextValue(mediaResponseText, 700)}`);
  }

  let mediaBody;
  try {
    mediaBody = JSON.parse(mediaResponseText);
  } catch {
    mediaBody = null;
  }

  if (!mediaBody?.ok) {
    const description = sanitizeTextValue(mediaBody?.description || mediaResponseText, 700) || "Unknown Telegram API error.";
    throw new Error(`Telegram sendMediaGroup failed: ${description}`);
  }
}

async function sendMiniSubmissionTelegramNotification(record, miniData, submission, telegramUser, attachments = []) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_NOTIFY_CHAT_ID) {
    return;
  }

  const payload = {
    chat_id: TELEGRAM_NOTIFY_CHAT_ID,
    text: buildTelegramSubmissionMessage(record, miniData, submission, telegramUser, attachments),
    disable_web_page_preview: true,
  };

  if (TELEGRAM_NOTIFY_THREAD_ID) {
    payload.message_thread_id = TELEGRAM_NOTIFY_THREAD_ID;
  }

  const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`Telegram sendMessage HTTP ${response.status}: ${sanitizeTextValue(responseText, 700)}`);
  }

  let body;
  try {
    body = JSON.parse(responseText);
  } catch {
    body = null;
  }

  if (!body?.ok) {
    const description = sanitizeTextValue(body?.description || responseText, 700) || "Unknown Telegram API error.";
    throw new Error(`Telegram sendMessage failed: ${description}`);
  }

  await sendMiniSubmissionTelegramAttachments(submission, attachments);
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
        SELECT id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
        FROM ${MODERATION_TABLE}
        ORDER BY submitted_at DESC
        LIMIT $1
      `,
      [limit],
    );
  } else {
    result = await pool.query(
      `
        SELECT id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
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
        SELECT id, record, mini_data, submitted_by, status, submitted_at
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
        RETURNING id, record, mini_data, submitted_by, status, submitted_at, reviewed_at, reviewed_by, review_note
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

function normalizeSsnForStorage(rawValue) {
  const value = sanitizeTextValue(rawValue, MINI_EXTRA_MAX_LENGTH.ssn || 64);
  if (!value) {
    return "";
  }

  const digits = value.replace(/\D/g, "");
  if (digits.length !== 9) {
    return null;
  }

  return `${digits.slice(0, 3)}-${digits.slice(3, 5)}-${digits.slice(5)}`;
}

function normalizeUsPhoneForStorage(rawValue) {
  const value = sanitizeTextValue(rawValue, MINI_EXTRA_MAX_LENGTH.clientPhoneNumber || 64);
  if (!value) {
    return "";
  }

  let digits = value.replace(/\D/g, "");
  if (digits.startsWith("1") && digits.length > 10) {
    digits = digits.slice(1);
  }

  if (digits.length !== 10) {
    return null;
  }

  return `+1(${digits.slice(0, 3)})${digits.slice(3, 6)}-${digits.slice(6)}`;
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

function createEmptyMiniData() {
  const miniData = {};

  for (const field of MINI_EXTRA_TEXT_FIELDS) {
    miniData[field] = "";
  }

  return miniData;
}

function createRecordFromMiniPayload(rawClient) {
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
  const miniData = createEmptyMiniData();
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

  for (const field of MINI_EXTRA_TEXT_FIELDS) {
    if (field === "ssn" || field === "clientPhoneNumber") {
      continue;
    }

    miniData[field] = sanitizeTextValue(client[field], MINI_EXTRA_MAX_LENGTH[field] || 4000);
  }

  const normalizedSsn = normalizeSsnForStorage(client.ssn);
  if (sanitizeTextValue(client.ssn, MINI_EXTRA_MAX_LENGTH.ssn || 64) && normalizedSsn === null) {
    return {
      error: "Invalid SSN format. Use XXX-XX-XXXX.",
    };
  }
  miniData.ssn = normalizedSsn || "";

  const normalizedPhone = normalizeUsPhoneForStorage(client.clientPhoneNumber);
  if (
    sanitizeTextValue(client.clientPhoneNumber, MINI_EXTRA_MAX_LENGTH.clientPhoneNumber || 64) &&
    normalizedPhone === null
  ) {
    return {
      error: "Invalid client phone format. Use +1(XXX)XXX-XXXX.",
    };
  }
  miniData.clientPhoneNumber = normalizedPhone || "";

  if (record.writtenOff === "Yes" && !record.dateWhenWrittenOff) {
    record.dateWhenWrittenOff = getTodayDateUs();
  }

  return {
    record: {
      id: generateId(),
      createdAt: new Date().toISOString(),
      ...record,
    },
    miniData,
  };
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

app.post("/api/mini/access", async (req, res) => {
  const authResult = await verifyTelegramInitData(req.body?.initData);
  if (!authResult.ok) {
    res.status(authResult.status).json({
      error: authResult.error,
    });
    return;
  }

  res.json({
    ok: true,
    user: {
      id: sanitizeTextValue(authResult.user?.id, 50),
      username: sanitizeTextValue(authResult.user?.username, 120),
    },
  });
});

app.post("/api/mini/clients", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    if (isMultipartRequest(req)) {
      await parseMiniMultipartRequest(req, res);
    }
  } catch (error) {
    res.status(error.httpStatus || 400).json({
      error: sanitizeTextValue(error?.message, 500) || "Failed to process file uploads.",
    });
    return;
  }

  const parsedPayload = parseMiniClientPayload(req);
  if (parsedPayload.error) {
    res.status(parsedPayload.status || 400).json({
      error: parsedPayload.error,
    });
    return;
  }

  const authResult = await verifyTelegramInitData(parsedPayload.initData);
  if (!authResult.ok) {
    res.status(authResult.status).json({
      error: authResult.error,
    });
    return;
  }

  const creationResult = createRecordFromMiniPayload(parsedPayload.client);
  if (!creationResult.record) {
    res.status(400).json({
      error: creationResult.error || "Invalid client payload.",
    });
    return;
  }

  const attachmentsResult = buildMiniSubmissionAttachments(req.files);
  if (attachmentsResult.error) {
    res.status(attachmentsResult.status || 400).json({
      error: attachmentsResult.error,
    });
    return;
  }

  try {
    const submission = await queueClientSubmission(
      creationResult.record,
      authResult.user,
      creationResult.miniData,
      attachmentsResult.attachments,
    );
    try {
      await sendMiniSubmissionTelegramNotification(
        creationResult.record,
        creationResult.miniData,
        submission,
        authResult.user,
        attachmentsResult.attachments,
      );
    } catch (notificationError) {
      console.error("Mini App Telegram notification failed:", notificationError);
    }

    res.status(201).json({
      ok: true,
      status: submission.status,
      submissionId: submission.id,
      submittedAt: submission.submittedAt,
      attachmentsCount: submission.attachmentsCount || 0,
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

app.get("/api/moderation/submissions/:id/files", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    const filesResult = await listPendingSubmissionFiles(req.params.id);
    if (!filesResult.ok) {
      res.status(filesResult.status).json({
        error: filesResult.error,
      });
      return;
    }

    const basePath = `/api/moderation/submissions/${encodeURIComponent(filesResult.submissionId)}/files`;
    const items = filesResult.items.map((file) => {
      const canPreview = isPreviewableAttachmentMimeType(file.mimeType);
      return {
        ...file,
        canPreview,
        previewUrl: canPreview ? `${basePath}/${encodeURIComponent(file.id)}?inline=1` : "",
        downloadUrl: `${basePath}/${encodeURIComponent(file.id)}`,
      };
    });

    res.json({
      ok: true,
      items,
    });
  } catch (error) {
    console.error("GET /api/moderation/submissions/:id/files failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load submission files"));
  }
});

app.get("/api/moderation/submissions/:id/files/:fileId", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  try {
    const fileResult = await getPendingSubmissionFile(req.params.id, req.params.fileId);
    if (!fileResult.ok) {
      res.status(fileResult.status).json({
        error: fileResult.error,
      });
      return;
    }

    const file = fileResult.file;
    const mimeType = normalizeAttachmentMimeType(file.mimeType);
    const inlineRequested = sanitizeTextValue(req.query.inline, 10) === "1";
    const isInline = inlineRequested && isPreviewableAttachmentMimeType(mimeType);
    res.setHeader("Content-Type", mimeType);
    res.setHeader("Content-Length", String(file.content.length));
    res.setHeader(
      "Content-Disposition",
      buildContentDisposition(isInline ? "inline" : "attachment", file.fileName),
    );
    res.setHeader("Cache-Control", "private, max-age=0, no-cache");
    res.send(file.content);
  } catch (error) {
    console.error("GET /api/moderation/submissions/:id/files/:fileId failed:", error);
    res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load file"));
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
  if (TELEGRAM_REQUIRED_CHAT_ID && !TELEGRAM_BOT_TOKEN) {
    console.warn("TELEGRAM_REQUIRED_CHAT_ID is ignored because TELEGRAM_BOT_TOKEN is missing.");
  }
  if (TELEGRAM_NOTIFY_CHAT_ID && !TELEGRAM_BOT_TOKEN) {
    console.warn("Telegram submission notifications are disabled: TELEGRAM_BOT_TOKEN is missing.");
  }
  if (TELEGRAM_NOTIFY_THREAD_ID && !TELEGRAM_NOTIFY_CHAT_ID) {
    console.warn("TELEGRAM_NOTIFY_THREAD_ID is ignored because TELEGRAM_NOTIFY_CHAT_ID is not set.");
  }
});
