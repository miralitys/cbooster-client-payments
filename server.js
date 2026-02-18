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
const DEFAULT_WEB_AUTH_USERNAME = "ramisi@creditbooster.com";
const DEFAULT_WEB_AUTH_PASSWORD = "Ringo@123Qwerty";
const WEB_AUTH_USERNAME = normalizeWebAuthConfigValue(process.env.WEB_AUTH_USERNAME) || DEFAULT_WEB_AUTH_USERNAME;
const WEB_AUTH_PASSWORD = normalizeWebAuthConfigValue(process.env.WEB_AUTH_PASSWORD) || DEFAULT_WEB_AUTH_PASSWORD;
const WEB_AUTH_SESSION_COOKIE_NAME = "cbooster_auth_session";
const WEB_AUTH_MOBILE_SESSION_HEADER = "x-cbooster-session";
const WEB_AUTH_SESSION_TTL_SEC = parsePositiveInteger(process.env.WEB_AUTH_SESSION_TTL_SEC, 12 * 60 * 60);
const WEB_AUTH_COOKIE_SECURE = resolveOptionalBoolean(process.env.WEB_AUTH_COOKIE_SECURE);
const WEB_AUTH_SESSION_SECRET = resolveWebAuthSessionSecret(process.env.WEB_AUTH_SESSION_SECRET);
const QUICKBOOKS_CLIENT_ID = (process.env.QUICKBOOKS_CLIENT_ID || "").toString().trim();
const QUICKBOOKS_CLIENT_SECRET = (process.env.QUICKBOOKS_CLIENT_SECRET || "").toString().trim();
const QUICKBOOKS_REFRESH_TOKEN = (process.env.QUICKBOOKS_REFRESH_TOKEN || "").toString().trim();
const QUICKBOOKS_REALM_ID = (process.env.QUICKBOOKS_REALM_ID || "").toString().trim();
const QUICKBOOKS_REDIRECT_URI = (process.env.QUICKBOOKS_REDIRECT_URI || "").toString().trim();
const QUICKBOOKS_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer";
const QUICKBOOKS_API_BASE_URL = ((process.env.QUICKBOOKS_API_BASE_URL || "https://quickbooks.api.intuit.com").toString().trim() || "https://quickbooks.api.intuit.com").replace(/\/+$/, "");
const QUICKBOOKS_QUERY_PAGE_SIZE = 200;
const QUICKBOOKS_MAX_QUERY_ROWS = 5000;
const QUICKBOOKS_PAYMENT_DETAILS_CONCURRENCY = 2;
const QUICKBOOKS_PAYMENT_DETAILS_MAX_RETRIES = 5;
const QUICKBOOKS_PAYMENT_DETAILS_RETRY_BASE_MS = 250;
const QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE = 250;
const QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT = 0.000001;
const QUICKBOOKS_ZERO_RECONCILE_MAX_ROWS = 200;
const QUICKBOOKS_DEFAULT_FROM_DATE = "2026-01-01";
const GHL_API_KEY = (process.env.GHL_API_KEY || process.env.GOHIGHLEVEL_API_KEY || "").toString().trim();
const GHL_LOCATION_ID = (process.env.GHL_LOCATION_ID || "").toString().trim();
const GHL_API_BASE_URL = (
  (process.env.GHL_API_BASE_URL || process.env.GOHIGHLEVEL_API_BASE_URL || "https://services.leadconnectorhq.com")
    .toString()
    .trim() || "https://services.leadconnectorhq.com"
).replace(/\/+$/, "");
const GHL_API_VERSION = (process.env.GHL_API_VERSION || "2021-07-28").toString().trim() || "2021-07-28";
const GHL_REQUEST_TIMEOUT_MS = Math.min(Math.max(parsePositiveInteger(process.env.GHL_REQUEST_TIMEOUT_MS, 15000), 2000), 60000);
const GHL_CONTACT_SEARCH_LIMIT = Math.min(Math.max(parsePositiveInteger(process.env.GHL_CONTACT_SEARCH_LIMIT, 20), 1), 100);
const GHL_CLIENT_MANAGER_LOOKUP_CONCURRENCY = Math.min(
  Math.max(parsePositiveInteger(process.env.GHL_CLIENT_MANAGER_LOOKUP_CONCURRENCY, 4), 1),
  12,
);
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
const DEFAULT_QUICKBOOKS_TRANSACTIONS_TABLE_NAME = "quickbooks_transactions";
const QUICKBOOKS_TRANSACTIONS_TABLE_NAME = resolveTableName(
  process.env.DB_QUICKBOOKS_TRANSACTIONS_TABLE_NAME,
  DEFAULT_QUICKBOOKS_TRANSACTIONS_TABLE_NAME,
);
const DEFAULT_QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME = "quickbooks_customers_cache";
const QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME = resolveTableName(
  process.env.DB_QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME,
  DEFAULT_QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME,
);
const DEFAULT_GHL_CLIENT_MANAGER_CACHE_TABLE_NAME = "ghl_client_manager_cache";
const GHL_CLIENT_MANAGER_CACHE_TABLE_NAME = resolveTableName(
  process.env.DB_GHL_CLIENT_MANAGER_CACHE_TABLE_NAME,
  DEFAULT_GHL_CLIENT_MANAGER_CACHE_TABLE_NAME,
);
const DB_SCHEMA = resolveSchemaName(process.env.DB_SCHEMA, "public");
const STATE_TABLE = qualifyTableName(DB_SCHEMA, TABLE_NAME);
const MODERATION_TABLE = qualifyTableName(DB_SCHEMA, MODERATION_TABLE_NAME);
const MODERATION_FILES_TABLE = qualifyTableName(DB_SCHEMA, MODERATION_FILES_TABLE_NAME);
const QUICKBOOKS_TRANSACTIONS_TABLE = qualifyTableName(DB_SCHEMA, QUICKBOOKS_TRANSACTIONS_TABLE_NAME);
const QUICKBOOKS_CUSTOMERS_CACHE_TABLE = qualifyTableName(DB_SCHEMA, QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME);
const GHL_CLIENT_MANAGER_CACHE_TABLE = qualifyTableName(DB_SCHEMA, GHL_CLIENT_MANAGER_CACHE_TABLE_NAME);
const MODERATION_STATUSES = new Set(["pending", "approved", "rejected"]);
const GHL_CLIENT_MANAGER_STATUSES = new Set(["assigned", "unassigned", "error"]);
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
const MINI_REQUIRED_FIELDS = ["clientName"];
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
app.set("trust proxy", 1);
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: false }));

const staticRoot = __dirname;

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

function normalizeWebAuthConfigValue(value) {
  return (value || "").toString().normalize("NFKC").trim();
}

function resolveOptionalBoolean(rawValue) {
  const normalized = (rawValue || "").toString().trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") {
    return true;
  }

  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") {
    return false;
  }

  return null;
}

function resolveWebAuthSessionSecret(rawSecret) {
  const explicit = normalizeWebAuthConfigValue(rawSecret);
  if (explicit.length >= 16) {
    return explicit;
  }

  return crypto
    .createHash("sha256")
    .update(`cbooster-web-auth:${WEB_AUTH_USERNAME}:${WEB_AUTH_PASSWORD}`)
    .digest("hex");
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

function encodeBase64Url(rawValue) {
  return Buffer.from(rawValue, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function decodeBase64Url(rawValue) {
  if (!rawValue) {
    return "";
  }

  const normalized = rawValue.replace(/-/g, "+").replace(/_/g, "/");
  const padding = normalized.length % 4 ? "=".repeat(4 - (normalized.length % 4)) : "";
  return Buffer.from(normalized + padding, "base64").toString("utf8");
}

function signWebAuthPayload(payload) {
  return crypto.createHmac("sha256", WEB_AUTH_SESSION_SECRET).update(payload).digest("hex");
}

function createWebAuthSessionToken(username) {
  const expiresAt = Date.now() + WEB_AUTH_SESSION_TTL_SEC * 1000;
  const payload = JSON.stringify({
    u: sanitizeTextValue(username, 200),
    e: expiresAt,
  });
  const encodedPayload = encodeBase64Url(payload);
  const signature = signWebAuthPayload(encodedPayload);
  return `${encodedPayload}.${signature}`;
}

function parseWebAuthSessionToken(rawToken) {
  const token = sanitizeTextValue(rawToken, 1200);
  if (!token) {
    return "";
  }

  const separatorIndex = token.lastIndexOf(".");
  if (separatorIndex <= 0) {
    return "";
  }

  const encodedPayload = token.slice(0, separatorIndex);
  const receivedSignature = token.slice(separatorIndex + 1);
  const expectedSignature = signWebAuthPayload(encodedPayload);
  if (!safeEqual(receivedSignature, expectedSignature)) {
    return "";
  }

  let parsedPayload = null;
  try {
    parsedPayload = JSON.parse(decodeBase64Url(encodedPayload));
  } catch {
    return "";
  }

  const username = sanitizeTextValue(parsedPayload?.u, 200);
  const expiresAt = Number.parseInt(parsedPayload?.e, 10);
  if (!username || !Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    return "";
  }

  return username;
}

function getRequestCookie(req, cookieName) {
  const normalizedName = sanitizeTextValue(cookieName, 200);
  if (!normalizedName) {
    return "";
  }

  const rawCookieHeader = (req.headers.cookie || "").toString();
  if (!rawCookieHeader) {
    return "";
  }

  const chunks = rawCookieHeader.split(";");
  for (const chunk of chunks) {
    const [rawKey, ...rawValueParts] = chunk.split("=");
    const key = (rawKey || "").trim();
    if (!key || key !== normalizedName) {
      continue;
    }

    const rawValue = rawValueParts.join("=").trim();
    if (!rawValue) {
      return "";
    }

    try {
      return decodeURIComponent(rawValue);
    } catch {
      return rawValue;
    }
  }

  return "";
}

function isSecureCookieRequired(req) {
  if (WEB_AUTH_COOKIE_SECURE !== null) {
    return WEB_AUTH_COOKIE_SECURE;
  }

  if (req?.secure) {
    return true;
  }

  const forwardedProto = sanitizeTextValue(req?.headers?.["x-forwarded-proto"], 40).toLowerCase();
  if (forwardedProto === "https") {
    return true;
  }

  return false;
}

function setWebAuthSessionCookie(req, res, username, sessionToken = "") {
  const token = sanitizeTextValue(sessionToken, 1200) || createWebAuthSessionToken(username);
  res.cookie(WEB_AUTH_SESSION_COOKIE_NAME, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: isSecureCookieRequired(req),
    maxAge: WEB_AUTH_SESSION_TTL_SEC * 1000,
    path: "/",
  });
}

function clearWebAuthSessionCookie(req, res) {
  res.clearCookie(WEB_AUTH_SESSION_COOKIE_NAME, {
    httpOnly: true,
    sameSite: "lax",
    secure: isSecureCookieRequired(req),
    path: "/",
  });
}

function getRequestWebAuthUser(req) {
  const cookieToken = getRequestCookie(req, WEB_AUTH_SESSION_COOKIE_NAME);
  const cookieUsername = parseWebAuthSessionToken(cookieToken);
  if (cookieUsername) {
    return cookieUsername;
  }

  const headerToken = sanitizeTextValue(req?.headers?.[WEB_AUTH_MOBILE_SESSION_HEADER], 1200);
  const headerUsername = parseWebAuthSessionToken(headerToken);
  if (headerUsername) {
    return headerUsername;
  }

  const authorizationHeader = sanitizeTextValue(req?.headers?.authorization, 1400);
  if (authorizationHeader.toLowerCase().startsWith("bearer ")) {
    const bearerToken = authorizationHeader.slice("bearer ".length).trim();
    const bearerUsername = parseWebAuthSessionToken(bearerToken);
    if (bearerUsername) {
      return bearerUsername;
    }
  }

  return "";
}

function resolveSafeNextPath(rawValue) {
  const candidate = sanitizeTextValue(rawValue, 2000);
  if (!candidate || !candidate.startsWith("/") || candidate.startsWith("//")) {
    return "/";
  }

  if (candidate.startsWith("/login") || candidate.startsWith("/logout")) {
    return "/";
  }

  return candidate;
}

function isValidWebAuthCredentials(rawUsername, rawPassword) {
  const username = normalizeWebAuthConfigValue(rawUsername).toLowerCase();
  const password = normalizeWebAuthConfigValue(rawPassword);
  const expectedUsername = normalizeWebAuthConfigValue(WEB_AUTH_USERNAME).toLowerCase();
  const expectedPassword = normalizeWebAuthConfigValue(WEB_AUTH_PASSWORD);
  return safeEqual(username, expectedUsername) && safeEqual(password, expectedPassword);
}

function isPublicWebAuthPath(pathname) {
  if (!pathname) {
    return false;
  }

  if (
    pathname === "/login" ||
    pathname === "/logout" ||
    pathname === "/favicon.ico" ||
    pathname === "/mini" ||
    pathname === "/mini.html" ||
    pathname === "/mini.js" ||
    pathname === "/api/auth/login" ||
    pathname === "/api/auth/logout" ||
    pathname === "/api/mobile/auth/login" ||
    pathname === "/api/mobile/auth/logout" ||
    pathname === "/api/health"
  ) {
    return true;
  }

  if (pathname.startsWith("/api/mini/")) {
    return true;
  }

  return false;
}

function buildWebLoginPageHtml({ nextPath = "/", errorMessage = "" } = {}) {
  const safeNextPath = resolveSafeNextPath(nextPath);
  const safeError = sanitizeTextValue(errorMessage, 200);
  const errorBlock = safeError
    ? `<p class="auth-error" role="alert">${escapeHtml(safeError)}</p>`
    : `<p class="auth-help">Use your account credentials to access the dashboard.</p>`;

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Sign In | Credit Booster</title>
    <style>
      :root {
        color-scheme: light;
        --color-bg: #f3f4f6;
        --color-surface: #ffffff;
        --color-border: #d6dde6;
        --color-border-strong: #c1ccd8;
        --color-text: #0f172a;
        --color-text-muted: #475569;
        --color-primary: #0f766e;
        --color-primary-hover: #115e59;
        --color-primary-contrast: #ffffff;
        --color-danger: #991b1b;
        --font-family-base: "Avenir Next", "Avenir", "Segoe UI", Helvetica, sans-serif;
        --font-family-heading: "Avenir Next Demi Bold", "Avenir Next", "Avenir", "Segoe UI", Helvetica, sans-serif;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 24px;
        font-family: var(--font-family-base);
        color: var(--color-text);
        background: var(--color-bg);
      }

      .auth-shell {
        width: min(450px, 100%);
        border: 1px solid var(--color-border);
        border-radius: 18px;
        background: var(--color-surface);
        box-shadow: 0 20px 48px -28px rgba(15, 23, 42, 0.42);
        padding: 24px;
        display: grid;
        gap: 12px;
      }

      .auth-eyebrow {
        margin: 0;
        font-size: 0.72rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--color-text-muted);
        font-weight: 600;
      }

      h1 {
        margin: 0;
        font-family: var(--font-family-heading);
        font-size: 1.62rem;
        line-height: 1.2;
        font-weight: 700;
      }

      .auth-subtitle {
        margin: 0;
        color: var(--color-text-muted);
        font-size: 0.78rem;
      }

      form {
        display: grid;
        gap: 12px;
        margin-top: 4px;
      }

      label {
        display: grid;
        gap: 6px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--color-text-muted);
      }

      input {
        width: 100%;
        border: 1px solid var(--color-border);
        border-radius: 10px;
        min-height: 38px;
        padding: 10px 12px;
        font-size: 0.88rem;
        background: #ffffff;
        color: var(--color-text);
      }

      input:focus {
        outline: none;
        border-color: var(--color-primary);
        box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.18);
      }

      button {
        border: 1px solid var(--color-primary);
        border-radius: 10px;
        min-height: 38px;
        padding: 10px 12px;
        font-size: 0.78rem;
        font-weight: 600;
        color: var(--color-primary-contrast);
        background: var(--color-primary);
        cursor: pointer;
        transition: background-color 0.16s ease, border-color 0.16s ease;
      }

      button:hover {
        background: var(--color-primary-hover);
        border-color: var(--color-primary-hover);
      }

      .auth-help,
      .auth-error {
        margin: 0;
        font-size: 0.78rem;
      }

      .auth-help {
        color: var(--color-text-muted);
      }

      .auth-error {
        color: var(--color-danger);
        background: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 10px;
        padding: 8px 10px;
      }
    </style>
  </head>
  <body>
    <main class="auth-shell">
      <p class="auth-eyebrow">Credit Booster</p>
      <h1>Sign In</h1>
      <p class="auth-subtitle">Client Payments Dashboard</p>
      ${errorBlock}
      <form method="post" action="/login" novalidate>
        <input type="hidden" name="next" value="${escapeHtml(safeNextPath)}" />
        <label>
          Username
          <input type="text" name="username" autocomplete="username" required />
        </label>
        <label>
          Password
          <input type="password" name="password" autocomplete="current-password" required />
        </label>
        <button type="submit">Log In</button>
      </form>
    </main>
  </body>
</html>`;
}

function escapeHtml(value) {
  return (value || "")
    .toString()
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function requireWebAuth(req, res, next) {
  const pathname = req.path || "/";
  if (isPublicWebAuthPath(pathname)) {
    next();
    return;
  }

  const username = getRequestWebAuthUser(req);
  if (username) {
    req.webAuthUser = username;
    next();
    return;
  }

  clearWebAuthSessionCookie(req, res);

  if (pathname.startsWith("/api/")) {
    res.status(401).json({
      error: "Authentication required.",
    });
    return;
  }

  const nextPath = resolveSafeNextPath(req.originalUrl || pathname);
  res.redirect(302, `/login?next=${encodeURIComponent(nextPath)}`);
}

function isQuickBooksConfigured() {
  return Boolean(
    QUICKBOOKS_CLIENT_ID &&
      QUICKBOOKS_CLIENT_SECRET &&
      QUICKBOOKS_REFRESH_TOKEN &&
      QUICKBOOKS_REALM_ID,
  );
}

function isGhlConfigured() {
  return Boolean(GHL_API_KEY && GHL_LOCATION_ID);
}

function buildGhlRequestHeaders(includeJsonBody = false) {
  const headers = {
    Authorization: `Bearer ${GHL_API_KEY}`,
    Version: GHL_API_VERSION,
    Accept: "application/json",
  };

  if (includeJsonBody) {
    headers["Content-Type"] = "application/json";
  }

  return headers;
}

function buildGhlUrl(pathname, query = {}) {
  const normalizedPath = `/${(pathname || "").toString().replace(/^\/+/, "")}`;
  const url = new URL(`${GHL_API_BASE_URL}${normalizedPath}`);

  const entries = Object.entries(query || {});
  for (const [key, rawValue] of entries) {
    const value = sanitizeTextValue(rawValue, 1000);
    if (!value) {
      continue;
    }
    url.searchParams.set(key, value);
  }

  return url;
}

async function requestGhlApi(pathname, options = {}) {
  const method = (options.method || "GET").toString().toUpperCase();
  const includeJsonBody = method !== "GET" && method !== "HEAD";
  const headers = buildGhlRequestHeaders(includeJsonBody);
  const query = options.query && typeof options.query === "object" ? options.query : {};
  const tolerateNotFound = Boolean(options.tolerateNotFound);
  const url = buildGhlUrl(pathname, query);
  const controller = new AbortController();
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, GHL_REQUEST_TIMEOUT_MS);

  let response;
  try {
    response = await fetch(url, {
      method,
      headers,
      body: includeJsonBody && options.body ? JSON.stringify(options.body) : undefined,
      signal: controller.signal,
    });
  } catch (error) {
    clearTimeout(timeoutId);
    const errorMessage = sanitizeTextValue(error?.message, 300) || "Unknown network error.";
    if (error?.name === "AbortError") {
      throw createHttpError(`GHL request timed out after ${GHL_REQUEST_TIMEOUT_MS}ms (${pathname}).`, 504);
    }
    throw createHttpError(`GHL request failed (${pathname}): ${errorMessage}`, 503);
  }

  clearTimeout(timeoutId);

  const responseText = await response.text();
  let body = null;
  try {
    body = responseText ? JSON.parse(responseText) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    if (tolerateNotFound && response.status === 404) {
      return {
        ok: false,
        status: 404,
        body: null,
      };
    }

    const details = sanitizeTextValue(
      body?.message ||
        body?.error ||
        body?.detail ||
        body?.details ||
        body?.meta?.message ||
        responseText,
      500,
    );
    throw createHttpError(
      `GHL API request failed (${pathname}, HTTP ${response.status}). ${details || "No details provided."}`,
      response.status >= 500 ? 502 : response.status,
    );
  }

  return {
    ok: true,
    status: response.status,
    body,
  };
}

function extractGhlContactsFromPayload(payload) {
  const candidates = [
    payload?.contacts,
    payload?.data?.contacts,
    payload?.data?.items,
    payload?.items,
    payload?.data,
    payload?.result?.contacts,
  ];

  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    return candidate.filter((item) => item && typeof item === "object");
  }

  if (payload?.contact && typeof payload.contact === "object") {
    return [payload.contact];
  }

  return [];
}

function extractGhlUsersFromPayload(payload) {
  const candidates = [
    payload?.users,
    payload?.data?.users,
    payload?.data?.items,
    payload?.items,
    payload?.data,
  ];

  for (const candidate of candidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    return candidate.filter((item) => item && typeof item === "object");
  }

  if (payload?.user && typeof payload.user === "object") {
    return [payload.user];
  }

  return [];
}

function normalizeNameForLookup(rawValue) {
  const value = sanitizeTextValue(rawValue, 300).toLowerCase();
  if (!value) {
    return "";
  }

  return value
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function buildContactCandidateName(contact) {
  const variants = [
    contact?.name,
    [contact?.firstName, contact?.lastName].filter(Boolean).join(" "),
    [contact?.first_name, contact?.last_name].filter(Boolean).join(" "),
    [contact?.contactNameFirst, contact?.contactNameLast].filter(Boolean).join(" "),
  ]
    .map((value) => sanitizeTextValue(value, 300))
    .filter(Boolean);

  return variants[0] || "";
}

function areNamesEquivalent(expectedName, candidateName) {
  const expected = normalizeNameForLookup(expectedName);
  const candidate = normalizeNameForLookup(candidateName);
  if (!expected || !candidate) {
    return false;
  }

  if (expected === candidate) {
    return true;
  }

  const expectedParts = expected.split(" ").filter(Boolean);
  const candidateParts = candidate.split(" ").filter(Boolean);

  if (expectedParts.length >= 2 && candidateParts.length >= 2) {
    const expectedFirst = expectedParts[0];
    const expectedLast = expectedParts[expectedParts.length - 1];
    const candidateFirst = candidateParts[0];
    const candidateLast = candidateParts[candidateParts.length - 1];
    if (expectedFirst === candidateFirst && expectedLast === candidateLast) {
      return true;
    }
  }

  return false;
}

function pushManagerIdToSet(rawValue, targetSet) {
  if (!targetSet || !(targetSet instanceof Set) || rawValue === null || rawValue === undefined) {
    return;
  }

  if (Array.isArray(rawValue)) {
    for (const item of rawValue) {
      pushManagerIdToSet(item, targetSet);
    }
    return;
  }

  if (typeof rawValue === "object") {
    const nestedCandidates = [rawValue.id, rawValue.userId, rawValue.user_id, rawValue.value];
    for (const candidate of nestedCandidates) {
      pushManagerIdToSet(candidate, targetSet);
    }
    return;
  }

  const id = sanitizeTextValue(rawValue, 160);
  if (!id) {
    return;
  }

  targetSet.add(id);
}

function extractManagerIdsFromContact(contact) {
  const managerIds = new Set();
  const candidates = [
    contact?.assignedTo,
    contact?.assigned_to,
    contact?.assignedUserId,
    contact?.assigned_user_id,
    contact?.ownerId,
    contact?.owner_id,
  ];

  for (const candidate of candidates) {
    pushManagerIdToSet(candidate, managerIds);
  }

  return [...managerIds];
}

function formatManagerNameFromUser(user, fallbackId = "") {
  if (!user || typeof user !== "object") {
    return sanitizeTextValue(fallbackId, 160);
  }

  const firstName = sanitizeTextValue(user.firstName || user.first_name, 120);
  const lastName = sanitizeTextValue(user.lastName || user.last_name, 120);
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    return fullName;
  }

  const directName = sanitizeTextValue(user.name || user.displayName, 240);
  if (directName) {
    return directName;
  }

  const email = sanitizeTextValue(user.email, 240);
  if (email) {
    return email;
  }

  return sanitizeTextValue(fallbackId, 160);
}

async function listGhlUsersIndex() {
  const query = {
    locationId: GHL_LOCATION_ID,
    limit: 200,
    page: 1,
  };

  const attempts = [
    () => requestGhlApi("/users/", { method: "GET", query, tolerateNotFound: true }),
    () => requestGhlApi("/users", { method: "GET", query, tolerateNotFound: true }),
  ];

  for (const attempt of attempts) {
    let response;
    try {
      response = await attempt();
    } catch {
      continue;
    }

    if (!response.ok) {
      continue;
    }

    const users = extractGhlUsersFromPayload(response.body);
    if (!users.length) {
      continue;
    }

    const index = new Map();
    for (const user of users) {
      const userId = sanitizeTextValue(user?.id || user?._id || user?.userId || user?.user_id, 160);
      if (!userId) {
        continue;
      }
      const managerName = formatManagerNameFromUser(user, userId);
      if (!managerName) {
        continue;
      }
      index.set(userId, managerName);
    }

    if (index.size) {
      return index;
    }
  }

  return new Map();
}

async function resolveGhlManagerName(managerId, usersIndex, managerNameCache) {
  const normalizedManagerId = sanitizeTextValue(managerId, 160);
  if (!normalizedManagerId) {
    return "";
  }

  if (managerNameCache.has(normalizedManagerId)) {
    return managerNameCache.get(normalizedManagerId);
  }

  const indexedName = usersIndex.get(normalizedManagerId);
  if (indexedName) {
    managerNameCache.set(normalizedManagerId, indexedName);
    return indexedName;
  }

  const response = await requestGhlApi(`/users/${encodeURIComponent(normalizedManagerId)}`, {
    method: "GET",
    query: {
      locationId: GHL_LOCATION_ID,
    },
    tolerateNotFound: true,
  });

  if (!response.ok) {
    managerNameCache.set(normalizedManagerId, normalizedManagerId);
    return normalizedManagerId;
  }

  const user = response.body?.user && typeof response.body.user === "object"
    ? response.body.user
    : response.body?.data && typeof response.body.data === "object"
      ? response.body.data
      : response.body;
  const managerName = formatManagerNameFromUser(user, normalizedManagerId) || normalizedManagerId;
  managerNameCache.set(normalizedManagerId, managerName);
  return managerName;
}

async function searchGhlContactsByClientName(clientName) {
  const normalizedClientName = sanitizeTextValue(clientName, 300);
  if (!normalizedClientName) {
    return [];
  }

  const attempts = [
    () =>
      requestGhlApi("/contacts/search", {
        method: "POST",
        body: {
          locationId: GHL_LOCATION_ID,
          page: 1,
          pageLimit: GHL_CONTACT_SEARCH_LIMIT,
          query: normalizedClientName,
        },
        tolerateNotFound: true,
      }),
    () =>
      requestGhlApi("/contacts/search", {
        method: "POST",
        body: {
          locationId: GHL_LOCATION_ID,
          page: 1,
          limit: GHL_CONTACT_SEARCH_LIMIT,
          query: normalizedClientName,
        },
        tolerateNotFound: true,
      }),
    () =>
      requestGhlApi("/contacts/", {
        method: "GET",
        query: {
          locationId: GHL_LOCATION_ID,
          query: normalizedClientName,
          page: 1,
          limit: GHL_CONTACT_SEARCH_LIMIT,
        },
        tolerateNotFound: true,
      }),
    () =>
      requestGhlApi("/contacts", {
        method: "GET",
        query: {
          locationId: GHL_LOCATION_ID,
          query: normalizedClientName,
          page: 1,
          limit: GHL_CONTACT_SEARCH_LIMIT,
        },
        tolerateNotFound: true,
      }),
  ];

  const contactsById = new Map();
  let successfulRequestCount = 0;
  let lastError = null;

  for (const attempt of attempts) {
    let response;
    try {
      response = await attempt();
    } catch (error) {
      lastError = error;
      continue;
    }

    if (!response.ok) {
      continue;
    }

    successfulRequestCount += 1;

    const contacts = extractGhlContactsFromPayload(response.body);
    for (const contact of contacts) {
      const candidateName = buildContactCandidateName(contact);
      if (!areNamesEquivalent(normalizedClientName, candidateName)) {
        continue;
      }

      const contactId = sanitizeTextValue(contact?.id || contact?._id || contact?.contactId, 160);
      if (contactId) {
        contactsById.set(contactId, contact);
        continue;
      }

      const fallbackKey = `${normalizeNameForLookup(candidateName)}::${contactsById.size}`;
      contactsById.set(fallbackKey, contact);
    }

    if (contactsById.size) {
      break;
    }
  }

  if (!successfulRequestCount && lastError) {
    throw lastError;
  }

  return [...contactsById.values()];
}

function getUniqueClientNamesFromRecords(records) {
  const normalizedRecords = Array.isArray(records) ? records : [];
  const names = new Set();

  for (const record of normalizedRecords) {
    const clientName = sanitizeTextValue(record?.clientName, 300);
    if (!clientName) {
      continue;
    }
    names.add(clientName);
  }

  return [...names].sort((left, right) => left.localeCompare(right, "en", { sensitivity: "base" }));
}

async function buildGhlClientManagerLookupRows(clientNames) {
  const names = Array.isArray(clientNames) ? clientNames : [];
  if (!names.length) {
    return [];
  }

  const managerNameCache = new Map();
  const usersIndex = await listGhlUsersIndex();
  const rows = new Array(names.length);
  let cursor = 0;
  const workerCount = Math.min(GHL_CLIENT_MANAGER_LOOKUP_CONCURRENCY, names.length);

  async function worker() {
    while (cursor < names.length) {
      const currentIndex = cursor;
      cursor += 1;
      const clientName = sanitizeTextValue(names[currentIndex], 300);
      if (!clientName) {
        rows[currentIndex] = {
          clientName: "",
          managers: [],
          managersLabel: "-",
          matchedContacts: 0,
          status: "unassigned",
        };
        continue;
      }

      try {
        const contacts = await searchGhlContactsByClientName(clientName);
        const managerIds = new Set();

        for (const contact of contacts) {
          for (const managerId of extractManagerIdsFromContact(contact)) {
            managerIds.add(managerId);
          }
        }

        const managerNames = [];
        for (const managerId of managerIds) {
          const managerName = await resolveGhlManagerName(managerId, usersIndex, managerNameCache);
          if (!managerName) {
            continue;
          }
          managerNames.push(managerName);
        }

        const uniqueManagerNames = [...new Set(managerNames)];
        rows[currentIndex] = {
          clientName,
          managers: uniqueManagerNames,
          managersLabel: uniqueManagerNames.join(", ") || "-",
          matchedContacts: contacts.length,
          status: uniqueManagerNames.length ? "assigned" : "unassigned",
        };
      } catch (error) {
        rows[currentIndex] = {
          clientName,
          managers: [],
          managersLabel: "-",
          matchedContacts: 0,
          status: "error",
          error: sanitizeTextValue(error?.message, 300) || "GHL lookup failed.",
        };
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return rows.filter(Boolean);
}

function normalizeGhlClientManagerStatus(rawStatus, fallback = "unassigned") {
  const normalized = sanitizeTextValue(rawStatus, 40).toLowerCase();
  if (GHL_CLIENT_MANAGER_STATUSES.has(normalized)) {
    return normalized;
  }
  return fallback;
}

function normalizeGhlRefreshMode(rawRefreshMode) {
  const value = sanitizeTextValue(rawRefreshMode, 40).toLowerCase();
  if (value === "full") {
    return "full";
  }
  if (value === "incremental") {
    return "incremental";
  }
  return "none";
}

function mapGhlClientManagerCacheRow(row) {
  const managers = Array.isArray(row?.managers)
    ? row.managers
        .map((value) => sanitizeTextValue(value, 240))
        .filter(Boolean)
    : [];
  const managersLabel = sanitizeTextValue(row?.managers_label, 2000) || managers.join(", ") || "-";
  const matchedContacts = Number.parseInt(row?.matched_contacts, 10);
  const status = normalizeGhlClientManagerStatus(row?.status);
  const error = sanitizeTextValue(row?.error, 500);

  return {
    clientName: sanitizeTextValue(row?.client_name, 300),
    managers,
    managersLabel,
    matchedContacts: Number.isFinite(matchedContacts) && matchedContacts >= 0 ? matchedContacts : 0,
    status,
    error,
    updatedAt: row?.updated_at ? new Date(row.updated_at).toISOString() : null,
  };
}

function normalizeGhlLookupRowForCache(row) {
  const clientName = sanitizeTextValue(row?.clientName, 300);
  if (!clientName) {
    return null;
  }

  const managers = Array.isArray(row?.managers)
    ? row.managers
        .map((value) => sanitizeTextValue(value, 240))
        .filter(Boolean)
    : [];
  const uniqueManagers = [...new Set(managers)];
  const managersLabel = sanitizeTextValue(row?.managersLabel, 2000) || uniqueManagers.join(", ") || "-";
  const matchedContacts = Number.parseInt(row?.matchedContacts, 10);
  const status = normalizeGhlClientManagerStatus(
    row?.status,
    uniqueManagers.length ? "assigned" : "unassigned",
  );
  const error = sanitizeTextValue(row?.error, 500);

  return {
    clientName,
    managers: uniqueManagers,
    managersLabel,
    matchedContacts: Number.isFinite(matchedContacts) && matchedContacts >= 0 ? matchedContacts : 0,
    status,
    error,
  };
}

async function listCachedGhlClientManagerRowsByClientNames(clientNames) {
  await ensureDatabaseReady();

  const names = (Array.isArray(clientNames) ? clientNames : [])
    .map((value) => sanitizeTextValue(value, 300))
    .filter(Boolean);
  if (!names.length) {
    return [];
  }

  const result = await pool.query(
    `
      SELECT client_name, managers, managers_label, matched_contacts, status, error, updated_at
      FROM ${GHL_CLIENT_MANAGER_CACHE_TABLE}
      WHERE client_name = ANY($1::text[])
      ORDER BY client_name ASC
    `,
    [names],
  );

  return result.rows.map(mapGhlClientManagerCacheRow).filter((row) => row.clientName);
}

async function upsertGhlClientManagerCacheRows(rows) {
  await ensureDatabaseReady();

  const normalizedRows = (Array.isArray(rows) ? rows : [])
    .map(normalizeGhlLookupRowForCache)
    .filter(Boolean);
  if (!normalizedRows.length) {
    return 0;
  }

  let writtenCount = 0;
  for (let offset = 0; offset < normalizedRows.length; offset += 150) {
    const batch = normalizedRows.slice(offset, offset + 150);
    const placeholders = [];
    const values = [];

    for (let index = 0; index < batch.length; index += 1) {
      const row = batch[index];
      const base = index * 6;
      placeholders.push(`($${base + 1}, $${base + 2}::jsonb, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6})`);
      values.push(
        row.clientName,
        JSON.stringify(row.managers),
        row.managersLabel,
        row.matchedContacts,
        row.status,
        row.error,
      );
    }

    const result = await pool.query(
      `
        INSERT INTO ${GHL_CLIENT_MANAGER_CACHE_TABLE}
          (client_name, managers, managers_label, matched_contacts, status, error)
        VALUES ${placeholders.join(", ")}
        ON CONFLICT (client_name)
        DO UPDATE SET
          managers = EXCLUDED.managers,
          managers_label = EXCLUDED.managers_label,
          matched_contacts = EXCLUDED.matched_contacts,
          status = EXCLUDED.status,
          error = EXCLUDED.error,
          updated_at = NOW()
      `,
      values,
    );

    writtenCount += result.rowCount || 0;
  }

  return writtenCount;
}

async function deleteStaleGhlClientManagerCacheRows(clientNames) {
  await ensureDatabaseReady();

  const names = (Array.isArray(clientNames) ? clientNames : [])
    .map((value) => sanitizeTextValue(value, 300))
    .filter(Boolean);

  if (!names.length) {
    const result = await pool.query(`DELETE FROM ${GHL_CLIENT_MANAGER_CACHE_TABLE}`);
    return result.rowCount || 0;
  }

  const result = await pool.query(
    `
      DELETE FROM ${GHL_CLIENT_MANAGER_CACHE_TABLE}
      WHERE NOT (client_name = ANY($1::text[]))
    `,
    [names],
  );

  return result.rowCount || 0;
}

function buildClientManagerItemsFromCache(clientNames, cachedRows) {
  const rowsByClientName = new Map();
  for (const row of Array.isArray(cachedRows) ? cachedRows : []) {
    if (!row?.clientName) {
      continue;
    }
    rowsByClientName.set(row.clientName, row);
  }

  const items = [];
  for (const clientName of Array.isArray(clientNames) ? clientNames : []) {
    const normalizedClientName = sanitizeTextValue(clientName, 300);
    if (!normalizedClientName) {
      continue;
    }

    const cachedRow = rowsByClientName.get(normalizedClientName);
    if (cachedRow) {
      items.push(cachedRow);
      continue;
    }

    items.push({
      clientName: normalizedClientName,
      managers: [],
      managersLabel: "-",
      matchedContacts: 0,
      status: "unassigned",
      error: "",
      updatedAt: null,
    });
  }

  return items;
}

function formatQuickBooksDateUtc(value) {
  const date = value instanceof Date ? value : new Date(value);
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeQuickBooksDateInput(rawValue) {
  return sanitizeTextValue(rawValue, 20);
}

function isValidIsoDateString(value) {
  const match = (value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return false;
  }

  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  return isValidDateParts(year, month, day);
}

function getQuickBooksDateRange(rawFromDate, rawToDate) {
  const todayIso = formatQuickBooksDateUtc(new Date());
  const from = normalizeQuickBooksDateInput(rawFromDate) || QUICKBOOKS_DEFAULT_FROM_DATE;
  const to = normalizeQuickBooksDateInput(rawToDate) || todayIso;

  if (!isValidIsoDateString(from)) {
    throw createHttpError("Invalid `from` date. Use YYYY-MM-DD format.", 400);
  }

  if (!isValidIsoDateString(to)) {
    throw createHttpError("Invalid `to` date. Use YYYY-MM-DD format.", 400);
  }

  if (from > to) {
    throw createHttpError("Invalid date range. `from` must be less than or equal to `to`.", 400);
  }

  return {
    from,
    to,
  };
}

function parseQuickBooksSyncFlag(rawValue) {
  const normalized = sanitizeTextValue(rawValue, 20).toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function parseQuickBooksTotalRefreshFlag(rawValue) {
  const normalized = sanitizeTextValue(rawValue, 40).toLowerCase();
  return (
    parseQuickBooksSyncFlag(rawValue) ||
    normalized === "full" ||
    normalized === "all" ||
    normalized === "total"
  );
}

function normalizeQuickBooksCustomerId(rawValue) {
  return sanitizeTextValue(rawValue, 120);
}

function normalizeQuickBooksCustomerPhone(rawValue) {
  return sanitizeTextValue(rawValue, 80);
}

function normalizeQuickBooksCustomerEmail(rawValue) {
  return sanitizeTextValue(rawValue, 320).toLowerCase();
}

function mapQuickBooksCustomerContactRow(row) {
  const customerId = normalizeQuickBooksCustomerId(row?.customer_id);
  if (!customerId) {
    return null;
  }

  return {
    customerId,
    clientName: sanitizeTextValue(row?.client_name, 300) || "",
    clientPhone: normalizeQuickBooksCustomerPhone(row?.client_phone),
    clientEmail: normalizeQuickBooksCustomerEmail(row?.client_email),
  };
}

function normalizeQuickBooksTransaction(item) {
  const transactionType = sanitizeTextValue(item?.transactionType, 40).toLowerCase();
  if (transactionType !== "payment" && transactionType !== "refund") {
    return null;
  }

  const transactionId = sanitizeTextValue(item?.transactionId, 160);
  if (!transactionId) {
    return null;
  }

  const clientName = sanitizeTextValue(item?.clientName, 300) || "Unknown client";
  const customerId = normalizeQuickBooksCustomerId(item?.customerId);
  const clientPhone = normalizeQuickBooksCustomerPhone(item?.clientPhone);
  const clientEmail = normalizeQuickBooksCustomerEmail(item?.clientEmail);
  const parsedAmount = Number.parseFloat(item?.paymentAmount);
  const paymentAmount = Number.isFinite(parsedAmount) ? parsedAmount : 0;
  const paymentDate = sanitizeTextValue(item?.paymentDate, 20);
  if (!isValidIsoDateString(paymentDate)) {
    return null;
  }

  return {
    transactionType,
    transactionId,
    customerId,
    clientName,
    clientPhone,
    clientEmail,
    paymentAmount,
    paymentDate,
  };
}

function mapQuickBooksTransactionRow(row) {
  const normalized = normalizeQuickBooksTransaction({
    transactionType: row?.transaction_type,
    transactionId: row?.transaction_id,
    customerId: row?.customer_id,
    clientName: row?.client_name,
    clientPhone: row?.client_phone,
    clientEmail: row?.client_email,
    paymentAmount: row?.payment_amount,
    paymentDate: row?.payment_date,
  });

  if (!normalized) {
    return null;
  }

  return {
    clientName: normalized.clientName,
    clientPhone: normalized.clientPhone,
    clientEmail: normalized.clientEmail,
    paymentAmount: normalized.paymentAmount,
    paymentDate: normalized.paymentDate,
    transactionType: normalized.transactionType,
  };
}

function sleepMilliseconds(milliseconds) {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function fetchQuickBooksAccessToken() {
  const basicCredentials = Buffer.from(`${QUICKBOOKS_CLIENT_ID}:${QUICKBOOKS_CLIENT_SECRET}`, "utf8").toString(
    "base64",
  );
  const payload = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: QUICKBOOKS_REFRESH_TOKEN,
  });

  if (QUICKBOOKS_REDIRECT_URI) {
    payload.set("redirect_uri", QUICKBOOKS_REDIRECT_URI);
  }

  let response;
  try {
    response = await fetch(QUICKBOOKS_TOKEN_URL, {
      method: "POST",
      headers: {
        Authorization: `Basic ${basicCredentials}`,
        Accept: "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: payload.toString(),
    });
  } catch (error) {
    throw createHttpError(`QuickBooks token request failed: ${sanitizeTextValue(error?.message, 300)}`, 503);
  }

  const responseText = await response.text();
  let body = null;
  try {
    body = responseText ? JSON.parse(responseText) : null;
  } catch {
    body = null;
  }

  if (!response.ok) {
    const details = sanitizeTextValue(body?.error_description || body?.error || responseText, 400);
    throw createHttpError(`QuickBooks auth failed. ${details || "Unable to refresh access token."}`, 502);
  }

  const accessToken = sanitizeTextValue(body?.access_token, 5000);
  if (!accessToken) {
    throw createHttpError("QuickBooks auth failed. Empty access token.", 502);
  }

  return accessToken;
}

async function fetchQuickBooksEntityInRange(accessToken, entityName, fromDate, toDate, requestLabel) {
  const normalizedEntityName = sanitizeTextValue(entityName, 80);
  if (!normalizedEntityName) {
    throw createHttpError("QuickBooks query entity is missing.", 500);
  }

  const items = [];
  let startPosition = 1;

  while (items.length < QUICKBOOKS_MAX_QUERY_ROWS) {
    const query = [
      "SELECT Id, TotalAmt, TxnDate, CustomerRef",
      `FROM ${normalizedEntityName}`,
      `WHERE TxnDate >= '${fromDate}' AND TxnDate <= '${toDate}'`,
      "ORDER BY TxnDate DESC",
      `STARTPOSITION ${startPosition}`,
      `MAXRESULTS ${QUICKBOOKS_QUERY_PAGE_SIZE}`,
    ].join(" ");
    const endpoint = `${QUICKBOOKS_API_BASE_URL}/v3/company/${encodeURIComponent(QUICKBOOKS_REALM_ID)}/query`;
    const queryParams = new URLSearchParams({
      query,
      minorversion: "75",
    });

    let response;
    try {
      response = await fetch(`${endpoint}?${queryParams.toString()}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: "application/json",
        },
      });
    } catch (error) {
      throw createHttpError(
        `QuickBooks ${requestLabel} request failed: ${sanitizeTextValue(error?.message, 300)}`,
        503,
      );
    }

    const responseText = await response.text();
    let body = null;
    try {
      body = responseText ? JSON.parse(responseText) : null;
    } catch {
      body = null;
    }

    if (!response.ok) {
      const faultError = body?.Fault?.Error?.[0];
      const message = sanitizeTextValue(
        faultError?.Detail || faultError?.Message || responseText || "Unknown QuickBooks API error.",
        500,
      );
      throw createHttpError(`QuickBooks ${requestLabel} query failed. ${message}`, 502);
    }

    const responseItems = body?.QueryResponse?.[normalizedEntityName];
    const pageItems = Array.isArray(responseItems)
      ? responseItems
      : responseItems
        ? [responseItems]
        : [];
    if (!pageItems.length) {
      break;
    }

    items.push(...pageItems);

    if (pageItems.length < QUICKBOOKS_QUERY_PAGE_SIZE) {
      break;
    }

    startPosition += QUICKBOOKS_QUERY_PAGE_SIZE;
  }

  return items.slice(0, QUICKBOOKS_MAX_QUERY_ROWS);
}

async function fetchQuickBooksPaymentsInRange(accessToken, fromDate, toDate) {
  return fetchQuickBooksEntityInRange(accessToken, "Payment", fromDate, toDate, "payments");
}

async function fetchQuickBooksRefundsInRange(accessToken, fromDate, toDate) {
  return fetchQuickBooksEntityInRange(accessToken, "RefundReceipt", fromDate, toDate, "refunds");
}

async function fetchQuickBooksPaymentDetails(accessToken, paymentId) {
  const normalizedPaymentId = sanitizeTextValue(paymentId, 120);
  if (!normalizedPaymentId) {
    return null;
  }

  const endpoint = `${QUICKBOOKS_API_BASE_URL}/v3/company/${encodeURIComponent(QUICKBOOKS_REALM_ID)}/payment/${encodeURIComponent(normalizedPaymentId)}`;
  const queryParams = new URLSearchParams({
    minorversion: "75",
  });

  for (let attempt = 1; attempt <= QUICKBOOKS_PAYMENT_DETAILS_MAX_RETRIES; attempt += 1) {
    let response;
    try {
      response = await fetch(`${endpoint}?${queryParams.toString()}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: "application/json",
        },
      });
    } catch (error) {
      console.warn(
        "QuickBooks payment detail request failed:",
        normalizedPaymentId,
        sanitizeTextValue(error?.message, 300),
      );
      return null;
    }

    const responseText = await response.text();
    let body = null;
    try {
      body = responseText ? JSON.parse(responseText) : null;
    } catch {
      body = null;
    }

    if (response.ok) {
      const payment = body?.Payment;
      if (!payment || typeof payment !== "object") {
        return null;
      }
      return payment;
    }

    const shouldRetry = response.status === 429 && attempt < QUICKBOOKS_PAYMENT_DETAILS_MAX_RETRIES;
    if (shouldRetry) {
      const retryDelay = QUICKBOOKS_PAYMENT_DETAILS_RETRY_BASE_MS * attempt;
      await sleepMilliseconds(retryDelay);
      continue;
    }

    const faultError = body?.Fault?.Error?.[0];
    console.warn(
      "QuickBooks payment detail query failed:",
      normalizedPaymentId,
      sanitizeTextValue(faultError?.Detail || faultError?.Message || responseText, 400),
    );
    return null;
  }

  return null;
}

function extractQuickBooksCustomerContact(customerRecord) {
  const customerId = normalizeQuickBooksCustomerId(customerRecord?.Id);
  if (!customerId) {
    return null;
  }

  const displayName = sanitizeTextValue(customerRecord?.DisplayName, 300);
  const fullyQualifiedName = sanitizeTextValue(customerRecord?.FullyQualifiedName, 300);
  const fallbackName = sanitizeTextValue(customerRecord?.CompanyName, 300);
  const clientName = displayName || fullyQualifiedName || fallbackName || "";

  const primaryPhone = normalizeQuickBooksCustomerPhone(customerRecord?.PrimaryPhone?.FreeFormNumber);
  const mobilePhone = normalizeQuickBooksCustomerPhone(customerRecord?.Mobile?.FreeFormNumber);
  const altPhone = normalizeQuickBooksCustomerPhone(customerRecord?.AlternatePhone?.FreeFormNumber);
  const resPhone = normalizeQuickBooksCustomerPhone(customerRecord?.PrimaryPhone?.FreeFormNumber);
  const clientPhone = primaryPhone || mobilePhone || altPhone || resPhone || "";

  const clientEmail = normalizeQuickBooksCustomerEmail(customerRecord?.PrimaryEmailAddr?.Address);

  return {
    customerId,
    clientName,
    clientPhone,
    clientEmail,
  };
}

async function fetchQuickBooksCustomerById(accessToken, customerId) {
  const normalizedCustomerId = normalizeQuickBooksCustomerId(customerId);
  if (!normalizedCustomerId) {
    return null;
  }

  const endpoint = `${QUICKBOOKS_API_BASE_URL}/v3/company/${encodeURIComponent(QUICKBOOKS_REALM_ID)}/customer/${encodeURIComponent(normalizedCustomerId)}`;
  const queryParams = new URLSearchParams({
    minorversion: "75",
  });

  for (let attempt = 1; attempt <= QUICKBOOKS_PAYMENT_DETAILS_MAX_RETRIES; attempt += 1) {
    let response;
    try {
      response = await fetch(`${endpoint}?${queryParams.toString()}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: "application/json",
        },
      });
    } catch (error) {
      console.warn(
        "QuickBooks customer request failed:",
        normalizedCustomerId,
        sanitizeTextValue(error?.message, 300),
      );
      return null;
    }

    const responseText = await response.text();
    let body = null;
    try {
      body = responseText ? JSON.parse(responseText) : null;
    } catch {
      body = null;
    }

    if (response.ok) {
      return extractQuickBooksCustomerContact(body?.Customer);
    }

    const shouldRetry = response.status === 429 && attempt < QUICKBOOKS_PAYMENT_DETAILS_MAX_RETRIES;
    if (shouldRetry) {
      const retryDelay = QUICKBOOKS_PAYMENT_DETAILS_RETRY_BASE_MS * attempt;
      await sleepMilliseconds(retryDelay);
      continue;
    }

    const faultError = body?.Fault?.Error?.[0];
    console.warn(
      "QuickBooks customer query failed:",
      normalizedCustomerId,
      sanitizeTextValue(faultError?.Detail || faultError?.Message || responseText, 400),
    );
    return null;
  }

  return null;
}

function deriveQuickBooksDepositLinkedAmount(payment) {
  const lines = Array.isArray(payment?.Line) ? payment.Line : [];
  let total = 0;

  for (const line of lines) {
    const parsedAmount = Number.parseFloat(line?.Amount);
    if (!Number.isFinite(parsedAmount) || parsedAmount === 0) {
      continue;
    }

    const linkedTransactions = Array.isArray(line?.LinkedTxn) ? line.LinkedTxn : [];
    const hasLinkedDeposit = linkedTransactions.some(
      (transaction) => sanitizeTextValue(transaction?.TxnType, 40).toLowerCase() === "deposit",
    );
    if (!hasLinkedDeposit) {
      continue;
    }

    // In QuickBooks these linked deposit amounts may appear with opposite sign in bank/deposit views.
    // For dashboard reporting we treat the linked deposit movement as received money.
    total += Math.abs(parsedAmount);
  }

  return total;
}

function deriveQuickBooksCreditMemoLinkedAmount(payment) {
  const lines = Array.isArray(payment?.Line) ? payment.Line : [];
  let total = 0;

  for (const line of lines) {
    const parsedAmount = Number.parseFloat(line?.Amount);
    if (!Number.isFinite(parsedAmount) || parsedAmount === 0) {
      continue;
    }

    const linkedTransactions = Array.isArray(line?.LinkedTxn) ? line.LinkedTxn : [];
    const hasLinkedCreditMemo = linkedTransactions.some(
      (transaction) => sanitizeTextValue(transaction?.TxnType, 40).toLowerCase() === "creditmemo",
    );
    if (!hasLinkedCreditMemo) {
      continue;
    }

    // Credit memo linked payment lines indicate a receivable write-off/credit application.
    total += Math.abs(parsedAmount);
  }

  return total;
}

async function enrichQuickBooksPaymentsWithEffectiveAmount(accessToken, paymentRecords) {
  const records = Array.isArray(paymentRecords) ? paymentRecords : [];
  if (!records.length) {
    return [];
  }

  const enrichedRecords = records.map((record) => {
    const parsedAmount = Number.parseFloat(record?.TotalAmt);
    return {
      ...record,
      _effectiveAmount: Number.isFinite(parsedAmount) ? parsedAmount : 0,
    };
  });

  const zeroAmountIndexes = [];
  for (let index = 0; index < enrichedRecords.length; index += 1) {
    if (Math.abs(enrichedRecords[index]._effectiveAmount) < 0.000001) {
      zeroAmountIndexes.push(index);
    }
  }

  if (!zeroAmountIndexes.length) {
    return enrichedRecords;
  }

  const workerCount = Math.min(QUICKBOOKS_PAYMENT_DETAILS_CONCURRENCY, zeroAmountIndexes.length);
  let cursor = 0;

  async function worker() {
    while (cursor < zeroAmountIndexes.length) {
      const currentIndex = zeroAmountIndexes[cursor];
      cursor += 1;
      const paymentRecord = enrichedRecords[currentIndex];
      const paymentDetails = await fetchQuickBooksPaymentDetails(accessToken, paymentRecord?.Id);
      const derivedDepositAmount = deriveQuickBooksDepositLinkedAmount(paymentDetails);
      if (Number.isFinite(derivedDepositAmount) && derivedDepositAmount > 0) {
        enrichedRecords[currentIndex]._effectiveAmount = derivedDepositAmount;
        continue;
      }

      const derivedCreditMemoAmount = deriveQuickBooksCreditMemoLinkedAmount(paymentDetails);
      if (Number.isFinite(derivedCreditMemoAmount) && derivedCreditMemoAmount > 0) {
        enrichedRecords[currentIndex]._effectiveAmount = -Math.abs(derivedCreditMemoAmount);
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return enrichedRecords;
}

function mapQuickBooksPayment(record) {
  const customerName = sanitizeTextValue(record?.CustomerRef?.name, 300);
  const customerId = normalizeQuickBooksCustomerId(record?.CustomerRef?.value);
  const transactionId = sanitizeTextValue(record?.Id, 160);
  const parsedAmount = Number.parseFloat(record?._effectiveAmount ?? record?.TotalAmt);
  const paymentAmount = Number.isFinite(parsedAmount) ? parsedAmount : 0;
  const paymentDate = sanitizeTextValue(record?.TxnDate, 20);

  return {
    transactionId,
    customerId,
    clientName: customerName || (customerId ? `Customer ${customerId}` : "Unknown client"),
    clientPhone: "",
    clientEmail: "",
    paymentAmount,
    paymentDate: paymentDate || "",
    transactionType: "payment",
  };
}

function mapQuickBooksRefund(record) {
  const customerName = sanitizeTextValue(record?.CustomerRef?.name, 300);
  const customerId = normalizeQuickBooksCustomerId(record?.CustomerRef?.value);
  const transactionId = sanitizeTextValue(record?.Id, 160);
  const parsedAmount = Number.parseFloat(record?.TotalAmt);
  const refundAmount = Number.isFinite(parsedAmount) ? -Math.abs(parsedAmount) : 0;
  const paymentDate = sanitizeTextValue(record?.TxnDate, 20);

  return {
    transactionId,
    customerId,
    clientName: customerName || (customerId ? `Customer ${customerId}` : "Unknown client"),
    clientPhone: "",
    clientEmail: "",
    paymentAmount: refundAmount,
    paymentDate: paymentDate || "",
    transactionType: "refund",
  };
}

function sortQuickBooksTransactionsByDateDesc(items) {
  return [...items].sort((left, right) => {
    const leftDate = sanitizeTextValue(left?.paymentDate, 20);
    const rightDate = sanitizeTextValue(right?.paymentDate, 20);
    if (leftDate === rightDate) {
      return 0;
    }
    if (!leftDate) {
      return 1;
    }
    if (!rightDate) {
      return -1;
    }
    return leftDate < rightDate ? 1 : -1;
  });
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

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${QUICKBOOKS_TRANSACTIONS_TABLE} (
          transaction_type TEXT NOT NULL,
          transaction_id TEXT NOT NULL,
          customer_id TEXT NOT NULL DEFAULT '',
          client_name TEXT NOT NULL,
          client_phone TEXT NOT NULL DEFAULT '',
          client_email TEXT NOT NULL DEFAULT '',
          payment_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
          payment_date DATE NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          PRIMARY KEY (transaction_type, transaction_id)
        )
      `);

      await pool.query(`
        ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
        ADD COLUMN IF NOT EXISTS customer_id TEXT NOT NULL DEFAULT ''
      `);

      await pool.query(`
        ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
        ADD COLUMN IF NOT EXISTS client_phone TEXT NOT NULL DEFAULT ''
      `);

      await pool.query(`
        ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
        ADD COLUMN IF NOT EXISTS client_email TEXT NOT NULL DEFAULT ''
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS ${QUICKBOOKS_TRANSACTIONS_TABLE_NAME}_payment_date_idx
        ON ${QUICKBOOKS_TRANSACTIONS_TABLE} (payment_date DESC)
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE} (
          customer_id TEXT PRIMARY KEY,
          client_name TEXT NOT NULL DEFAULT '',
          client_phone TEXT NOT NULL DEFAULT '',
          client_email TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME}_updated_at_idx
        ON ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE} (updated_at DESC)
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS ${GHL_CLIENT_MANAGER_CACHE_TABLE} (
          client_name TEXT PRIMARY KEY,
          managers JSONB NOT NULL DEFAULT '[]'::jsonb,
          managers_label TEXT NOT NULL DEFAULT '-',
          matched_contacts INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'unassigned',
          error TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await pool.query(`
        CREATE INDEX IF NOT EXISTS ${GHL_CLIENT_MANAGER_CACHE_TABLE_NAME}_updated_at_idx
        ON ${GHL_CLIENT_MANAGER_CACHE_TABLE} (updated_at DESC)
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

async function listCachedQuickBooksCustomerContacts(customerIds) {
  await ensureDatabaseReady();

  const normalizedIds = [...new Set((Array.isArray(customerIds) ? customerIds : []).map(normalizeQuickBooksCustomerId))]
    .filter(Boolean);
  if (!normalizedIds.length) {
    return new Map();
  }

  const result = await pool.query(
    `
      SELECT customer_id, client_name, client_phone, client_email
      FROM ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE}
      WHERE customer_id = ANY($1::text[])
    `,
    [normalizedIds],
  );

  const cache = new Map();
  for (const row of result.rows) {
    const mapped = mapQuickBooksCustomerContactRow(row);
    if (!mapped) {
      continue;
    }
    cache.set(mapped.customerId, mapped);
  }

  return cache;
}

async function upsertQuickBooksCustomerContacts(customerContacts) {
  await ensureDatabaseReady();

  const items = (Array.isArray(customerContacts) ? customerContacts : [])
    .map((item) => {
      const customerId = normalizeQuickBooksCustomerId(item?.customerId);
      if (!customerId) {
        return null;
      }
      return {
        customerId,
        clientName: sanitizeTextValue(item?.clientName, 300) || "",
        clientPhone: normalizeQuickBooksCustomerPhone(item?.clientPhone),
        clientEmail: normalizeQuickBooksCustomerEmail(item?.clientEmail),
      };
    })
    .filter((item) => item !== null);
  if (!items.length) {
    return {
      writtenCount: 0,
    };
  }

  const client = await pool.connect();
  let writtenCount = 0;

  try {
    await client.query("BEGIN");

    for (let offset = 0; offset < items.length; offset += QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE) {
      const batch = items.slice(offset, offset + QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE);
      const placeholders = [];
      const values = [];

      for (let index = 0; index < batch.length; index += 1) {
        const item = batch[index];
        const base = index * 4;
        placeholders.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`);
        values.push(item.customerId, item.clientName, item.clientPhone, item.clientEmail);
      }

      const result = await client.query(
        `
          INSERT INTO ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE}
            (customer_id, client_name, client_phone, client_email)
          VALUES ${placeholders.join(", ")}
          ON CONFLICT (customer_id)
          DO UPDATE
          SET
            client_name = EXCLUDED.client_name,
            client_phone = EXCLUDED.client_phone,
            client_email = EXCLUDED.client_email,
            updated_at = NOW()
        `,
        values,
      );

      writtenCount += result.rowCount || 0;
    }

    await client.query("COMMIT");

    return {
      writtenCount,
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

async function enrichQuickBooksTransactionsWithCustomerContacts(accessToken, transactionItems, options = {}) {
  const records = Array.isArray(transactionItems) ? transactionItems : [];
  if (!records.length) {
    return [];
  }

  const forceRefresh = Boolean(options?.forceRefresh);
  const customerIds = [
    ...new Set(
      records
        .map((item) => normalizeQuickBooksCustomerId(item?.customerId))
        .filter(Boolean),
    ),
  ];
  if (!customerIds.length) {
    return records.map((item) => ({
      ...item,
      customerId: normalizeQuickBooksCustomerId(item?.customerId),
      clientPhone: normalizeQuickBooksCustomerPhone(item?.clientPhone),
      clientEmail: normalizeQuickBooksCustomerEmail(item?.clientEmail),
    }));
  }

  const contactsById = await listCachedQuickBooksCustomerContacts(customerIds);
  const missingIds = forceRefresh ? customerIds : customerIds.filter((customerId) => !contactsById.has(customerId));

  if (missingIds.length) {
    const fetchedContacts = [];
    const workerCount = Math.min(QUICKBOOKS_PAYMENT_DETAILS_CONCURRENCY, missingIds.length);
    let cursor = 0;

    async function worker() {
      while (cursor < missingIds.length) {
        const currentIndex = cursor;
        cursor += 1;
        const customerId = missingIds[currentIndex];
        const contact = await fetchQuickBooksCustomerById(accessToken, customerId);
        if (!contact) {
          continue;
        }
        fetchedContacts.push(contact);
      }
    }

    await Promise.all(Array.from({ length: workerCount }, () => worker()));
    if (fetchedContacts.length) {
      await upsertQuickBooksCustomerContacts(fetchedContacts);
      for (const contact of fetchedContacts) {
        const customerId = normalizeQuickBooksCustomerId(contact.customerId);
        if (!customerId) {
          continue;
        }
        contactsById.set(customerId, {
          customerId,
          clientName: sanitizeTextValue(contact.clientName, 300) || "",
          clientPhone: normalizeQuickBooksCustomerPhone(contact.clientPhone),
          clientEmail: normalizeQuickBooksCustomerEmail(contact.clientEmail),
        });
      }
    }
  }

  return records.map((item) => {
    const customerId = normalizeQuickBooksCustomerId(item?.customerId);
    const cachedContact = customerId ? contactsById.get(customerId) : null;

    return {
      ...item,
      customerId: customerId || "",
      clientName: sanitizeTextValue(cachedContact?.clientName, 300) || sanitizeTextValue(item?.clientName, 300) || "Unknown client",
      clientPhone: normalizeQuickBooksCustomerPhone(cachedContact?.clientPhone || item?.clientPhone),
      clientEmail: normalizeQuickBooksCustomerEmail(cachedContact?.clientEmail || item?.clientEmail),
    };
  });
}

async function listCachedQuickBooksTransactionsInRange(fromDate, toDate) {
  await ensureDatabaseReady();

  const result = await pool.query(
    `
      SELECT
        transaction_type,
        transaction_id,
        customer_id,
        client_name,
        client_phone,
        client_email,
        payment_amount,
        payment_date::text AS payment_date
      FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
      WHERE payment_date >= $1::date
        AND payment_date <= $2::date
      ORDER BY payment_date DESC, updated_at DESC, transaction_type ASC, transaction_id ASC
    `,
    [fromDate, toDate],
  );

  const items = [];
  for (const row of result.rows) {
    const mapped = mapQuickBooksTransactionRow(row);
    if (!mapped) {
      continue;
    }
    if (Math.abs(mapped.paymentAmount) < QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT) {
      continue;
    }
    items.push(mapped);
  }

  return items;
}

async function getLatestCachedQuickBooksPaymentDate(fromDate, toDate) {
  await ensureDatabaseReady();

  const result = await pool.query(
    `
      SELECT MAX(payment_date)::text AS max_date
      FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
      WHERE payment_date >= $1::date
        AND payment_date <= $2::date
    `,
    [fromDate, toDate],
  );

  const maxDate = sanitizeTextValue(result.rows[0]?.max_date, 20);
  return isValidIsoDateString(maxDate) ? maxDate : "";
}

async function listCachedQuickBooksZeroPaymentsInRange(fromDate, toDate) {
  await ensureDatabaseReady();

  const result = await pool.query(
    `
      SELECT
        transaction_id,
        customer_id,
        client_name,
        client_phone,
        client_email,
        payment_date::text AS payment_date
      FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
      WHERE transaction_type = 'payment'
        AND payment_date >= $1::date
        AND payment_date <= $2::date
        AND ABS(payment_amount) < $3
      ORDER BY payment_date DESC, updated_at ASC, transaction_id ASC
      LIMIT $4
    `,
    [fromDate, toDate, QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT, QUICKBOOKS_ZERO_RECONCILE_MAX_ROWS],
  );

  return result.rows
    .map((row) => ({
      transactionId: sanitizeTextValue(row?.transaction_id, 160),
      customerId: normalizeQuickBooksCustomerId(row?.customer_id),
      clientName: sanitizeTextValue(row?.client_name, 300) || "Unknown client",
      clientPhone: normalizeQuickBooksCustomerPhone(row?.client_phone),
      clientEmail: normalizeQuickBooksCustomerEmail(row?.client_email),
      paymentDate: sanitizeTextValue(row?.payment_date, 20),
    }))
    .filter((row) => row.transactionId && isValidIsoDateString(row.paymentDate));
}

function buildQuickBooksIncrementalSyncFromDate(rangeFromDate, rangeToDate, latestCachedDate) {
  if (!isValidIsoDateString(rangeFromDate) || !isValidIsoDateString(rangeToDate)) {
    return "";
  }

  let syncFromDate = rangeFromDate;
  if (isValidIsoDateString(latestCachedDate) && latestCachedDate > syncFromDate) {
    syncFromDate = latestCachedDate;
  }

  if (syncFromDate > rangeToDate) {
    return "";
  }

  return syncFromDate;
}

async function reconcileCachedQuickBooksZeroPayments(accessToken, fromDate, toDate) {
  const zeroRows = await listCachedQuickBooksZeroPaymentsInRange(fromDate, toDate);
  if (!zeroRows.length) {
    return {
      scannedCount: 0,
      reconciledCount: 0,
      writtenCount: 0,
    };
  }

  const updates = [];
  const workerCount = Math.min(QUICKBOOKS_PAYMENT_DETAILS_CONCURRENCY, zeroRows.length);
  let cursor = 0;

  async function worker() {
    while (cursor < zeroRows.length) {
      const currentIndex = cursor;
      cursor += 1;
      const row = zeroRows[currentIndex];
      const paymentDetails = await fetchQuickBooksPaymentDetails(accessToken, row.transactionId);
      const derivedDepositAmount = deriveQuickBooksDepositLinkedAmount(paymentDetails);
      if (Number.isFinite(derivedDepositAmount) && derivedDepositAmount > 0) {
        updates.push({
          transactionType: "payment",
          transactionId: row.transactionId,
          customerId: row.customerId,
          clientName: row.clientName,
          clientPhone: row.clientPhone,
          clientEmail: row.clientEmail,
          paymentAmount: derivedDepositAmount,
          paymentDate: row.paymentDate,
        });
        continue;
      }

      const derivedCreditMemoAmount = deriveQuickBooksCreditMemoLinkedAmount(paymentDetails);
      if (Number.isFinite(derivedCreditMemoAmount) && derivedCreditMemoAmount > 0) {
        updates.push({
          transactionType: "payment",
          transactionId: row.transactionId,
          customerId: row.customerId,
          clientName: row.clientName,
          clientPhone: row.clientPhone,
          clientEmail: row.clientEmail,
          paymentAmount: -Math.abs(derivedCreditMemoAmount),
          paymentDate: row.paymentDate,
        });
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  if (!updates.length) {
    return {
      scannedCount: zeroRows.length,
      reconciledCount: 0,
      writtenCount: 0,
    };
  }

  const upsertResult = await upsertQuickBooksTransactions(updates);
  return {
    scannedCount: zeroRows.length,
    reconciledCount: updates.length,
    writtenCount: upsertResult.writtenCount,
  };
}

async function upsertQuickBooksTransactions(items) {
  await ensureDatabaseReady();

  const normalizedItems = (Array.isArray(items) ? items : [])
    .map(normalizeQuickBooksTransaction)
    .filter((item) => item !== null);
  if (!normalizedItems.length) {
    return {
      insertedCount: 0,
      writtenCount: 0,
    };
  }

  const client = await pool.connect();
  let insertedCount = 0;
  let writtenCount = 0;

  try {
    await client.query("BEGIN");

    for (let offset = 0; offset < normalizedItems.length; offset += QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE) {
      const batch = normalizedItems.slice(offset, offset + QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE);
      const placeholders = [];
      const values = [];

      for (let index = 0; index < batch.length; index += 1) {
        const item = batch[index];
        const base = index * 8;
        placeholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}::date)`,
        );
        values.push(
          item.transactionType,
          item.transactionId,
          item.customerId,
          item.clientName,
          item.clientPhone,
          item.clientEmail,
          item.paymentAmount,
          item.paymentDate,
        );
      }

      const result = await client.query(
        `
          INSERT INTO ${QUICKBOOKS_TRANSACTIONS_TABLE}
            (transaction_type, transaction_id, customer_id, client_name, client_phone, client_email, payment_amount, payment_date)
          VALUES ${placeholders.join(", ")}
          ON CONFLICT (transaction_type, transaction_id)
          DO UPDATE
          SET
            customer_id = EXCLUDED.customer_id,
            client_name = EXCLUDED.client_name,
            client_phone = EXCLUDED.client_phone,
            client_email = EXCLUDED.client_email,
            payment_amount = EXCLUDED.payment_amount,
            payment_date = EXCLUDED.payment_date,
            updated_at = NOW()
          RETURNING (xmax = 0) AS inserted
        `,
        values,
      );

      writtenCount += result.rowCount || 0;
      for (const row of result.rows) {
        if (row?.inserted) {
          insertedCount += 1;
        }
      }
    }

    await client.query("COMMIT");

    return {
      insertedCount,
      writtenCount,
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
    req.webAuthUser,
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

function normalizeEmailForStorage(rawValue) {
  const value = sanitizeTextValue(rawValue, MINI_EXTRA_MAX_LENGTH.clientEmailAddress || 320).trim();
  if (!value) {
    return "";
  }

  if (!value.includes("@")) {
    return null;
  }

  return value;
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

  for (const field of MINI_REQUIRED_FIELDS) {
    const maxLength =
      Object.prototype.hasOwnProperty.call(MINI_EXTRA_MAX_LENGTH, field) ? MINI_EXTRA_MAX_LENGTH[field] : 4000;
    const value = sanitizeTextValue(client[field], maxLength);
    if (!value) {
      return {
        error: `\`${field}\` is required.`,
      };
    }
  }

  const clientName = sanitizeTextValue(client.clientName, 200);

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
    if (field === "ssn" || field === "clientPhoneNumber" || field === "clientEmailAddress") {
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

  const normalizedEmail = normalizeEmailForStorage(client.clientEmailAddress);
  if (
    sanitizeTextValue(client.clientEmailAddress, MINI_EXTRA_MAX_LENGTH.clientEmailAddress || 320) &&
    normalizedEmail === null
  ) {
    return {
      error: "Invalid client email. Email must include @.",
    };
  }
  miniData.clientEmailAddress = normalizedEmail || "";

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

app.get("/login", (req, res) => {
  const nextPath = resolveSafeNextPath(req.query.next);
  const currentSessionToken = getRequestCookie(req, WEB_AUTH_SESSION_COOKIE_NAME);
  const currentUser = parseWebAuthSessionToken(currentSessionToken);
  if (currentUser) {
    res.redirect(302, nextPath);
    return;
  }

  const hasError = Boolean(sanitizeTextValue(req.query.error, 20));
  res.setHeader("Cache-Control", "no-store, private");
  res
    .status(200)
    .type("html")
    .send(
      buildWebLoginPageHtml({
        nextPath,
        errorMessage: hasError ? "Invalid login or password." : "",
      }),
    );
});

app.post("/login", (req, res) => {
  const username = req.body?.username;
  const password = req.body?.password;
  const nextPath = resolveSafeNextPath(req.body?.next || req.query.next);

  if (!isValidWebAuthCredentials(username, password)) {
    clearWebAuthSessionCookie(req, res);
    res.redirect(302, `/login?error=1&next=${encodeURIComponent(nextPath)}`);
    return;
  }

  setWebAuthSessionCookie(req, res, WEB_AUTH_USERNAME);
  res.redirect(302, nextPath);
});

function handleApiAuthLogin(req, res) {
  const username = req.body?.username;
  const password = req.body?.password;

  if (!isValidWebAuthCredentials(username, password)) {
    clearWebAuthSessionCookie(req, res);
    res.status(401).json({
      error: "Invalid login or password.",
    });
    return;
  }

  const authUsername = normalizeWebAuthConfigValue(WEB_AUTH_USERNAME);
  const sessionToken = createWebAuthSessionToken(authUsername);
  setWebAuthSessionCookie(req, res, authUsername, sessionToken);
  res.setHeader("Cache-Control", "no-store, private");
  res.json({
    ok: true,
    sessionToken,
    user: {
      username: authUsername,
    },
  });
}

function handleApiAuthLogout(req, res) {
  clearWebAuthSessionCookie(req, res);
  res.setHeader("Cache-Control", "no-store, private");
  res.json({
    ok: true,
  });
}

app.post("/api/auth/login", handleApiAuthLogin);
app.post("/api/auth/logout", handleApiAuthLogout);
app.post("/api/mobile/auth/login", handleApiAuthLogin);
app.post("/api/mobile/auth/logout", handleApiAuthLogout);

function handleWebLogout(req, res) {
  clearWebAuthSessionCookie(req, res);
  res.redirect(302, "/login");
}

app.get("/logout", handleWebLogout);
app.post("/logout", handleWebLogout);

app.use(requireWebAuth);
app.use(express.static(staticRoot));

app.get("/api/auth/session", (req, res) => {
  res.json({
    ok: true,
    user: {
      username: sanitizeTextValue(req.webAuthUser, 200),
    },
  });
});

app.all("/api/quickbooks/*", (req, res, next) => {
  if (req.method === "GET") {
    next();
    return;
  }

  res.status(405).json({
    error: "QuickBooks integration is read-only. Write operations are disabled.",
  });
});

app.get("/api/quickbooks/payments/recent", async (req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  let range;
  try {
    range = getQuickBooksDateRange(req.query.from, req.query.to);
  } catch (error) {
    res.status(error.httpStatus || 400).json({
      error: sanitizeTextValue(error?.message, 300) || "Invalid date range.",
    });
    return;
  }

  const shouldTotalRefresh = parseQuickBooksTotalRefreshFlag(req.query.fullSync || req.query.totalRefresh);
  const shouldSync = parseQuickBooksSyncFlag(req.query.sync) || shouldTotalRefresh;
  if (shouldSync && !isQuickBooksConfigured()) {
    res.status(503).json({
      error:
        "QuickBooks is not configured. Set QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET, QUICKBOOKS_REFRESH_TOKEN, and QUICKBOOKS_REALM_ID.",
    });
    return;
  }

  try {
    let syncMeta = {
      requested: shouldSync,
      syncMode: shouldTotalRefresh ? "full" : "incremental",
      performed: false,
      syncFrom: "",
      fetchedCount: 0,
      insertedCount: 0,
      writtenCount: 0,
      reconciledScannedCount: 0,
      reconciledCount: 0,
      reconciledWrittenCount: 0,
    };

    if (shouldSync) {
      const latestCachedDate = await getLatestCachedQuickBooksPaymentDate(range.from, range.to);
      const syncFromDate = shouldTotalRefresh
        ? range.from
        : buildQuickBooksIncrementalSyncFromDate(range.from, range.to, latestCachedDate);

      syncMeta.syncFrom = syncFromDate;
      const accessToken = await fetchQuickBooksAccessToken();

      if (syncFromDate) {
        const [paymentRecords, refundRecords] = await Promise.all([
          fetchQuickBooksPaymentsInRange(accessToken, syncFromDate, range.to),
          fetchQuickBooksRefundsInRange(accessToken, syncFromDate, range.to),
        ]);
        const normalizedPaymentRecords = await enrichQuickBooksPaymentsWithEffectiveAmount(accessToken, paymentRecords);
        const paymentItems = normalizedPaymentRecords.map(mapQuickBooksPayment);
        const refundItems = refundRecords.map(mapQuickBooksRefund);
        const fetchedItems = sortQuickBooksTransactionsByDateDesc([...paymentItems, ...refundItems]);
        const enrichedItems = await enrichQuickBooksTransactionsWithCustomerContacts(accessToken, fetchedItems, {
          forceRefresh: shouldTotalRefresh,
        });
        const upsertResult = await upsertQuickBooksTransactions(enrichedItems);

        syncMeta = {
          ...syncMeta,
          performed: true,
          fetchedCount: enrichedItems.length,
          insertedCount: upsertResult.insertedCount,
          writtenCount: upsertResult.writtenCount,
        };
      }

      const reconcileResult = await reconcileCachedQuickBooksZeroPayments(accessToken, range.from, range.to);
      syncMeta = {
        ...syncMeta,
        reconciledScannedCount: reconcileResult.scannedCount,
        reconciledCount: reconcileResult.reconciledCount,
        reconciledWrittenCount: reconcileResult.writtenCount,
      };
    }

    const items = await listCachedQuickBooksTransactionsInRange(range.from, range.to);

    res.json({
      ok: true,
      range: {
        from: range.from,
        to: range.to,
      },
      count: items.length,
      items,
      source: shouldSync ? "cache+sync" : "cache",
      sync: syncMeta,
    });
  } catch (error) {
    console.error("GET /api/quickbooks/payments/recent failed:", error);
    res.status(error.httpStatus || 502).json({
      error: sanitizeTextValue(error?.message, 600) || "Failed to load QuickBooks payments.",
    });
  }
});

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

app.get("/api/ghl/client-managers", async (_req, res) => {
  if (!pool) {
    res.status(503).json({
      error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
    });
    return;
  }

  const refreshMode = normalizeGhlRefreshMode(_req.query.refresh);

  try {
    const state = await getStoredRecords();
    const clientNames = getUniqueClientNamesFromRecords(state.records);
    let cachedRows = await listCachedGhlClientManagerRowsByClientNames(clientNames);
    let refreshedClientsCount = 0;
    let refreshedRowsWritten = 0;
    let deletedStaleRowsCount = 0;
    let refreshed = false;

    if (refreshMode !== "none") {
      if (!isGhlConfigured()) {
        res.status(503).json({
          error: "GHL integration is not configured. Set GHL_API_KEY and GHL_LOCATION_ID.",
        });
        return;
      }

      const cachedClientNameSet = new Set(cachedRows.map((row) => row.clientName));
      const namesToLookup =
        refreshMode === "full"
          ? clientNames
          : clientNames.filter((clientName) => !cachedClientNameSet.has(clientName));

      if (refreshMode === "full") {
        deletedStaleRowsCount = await deleteStaleGhlClientManagerCacheRows(clientNames);
      }

      if (namesToLookup.length) {
        const lookedUpRows = await buildGhlClientManagerLookupRows(namesToLookup);
        refreshedRowsWritten = await upsertGhlClientManagerCacheRows(lookedUpRows);
        refreshedClientsCount = lookedUpRows.length;
      }

      refreshed = true;
      cachedRows = await listCachedGhlClientManagerRowsByClientNames(clientNames);
    }

    const items = buildClientManagerItemsFromCache(clientNames, cachedRows);

    res.json({
      ok: true,
      count: items.length,
      items,
      source: "gohighlevel",
      updatedAt: state.updatedAt,
      refresh: {
        mode: refreshMode,
        performed: refreshed,
        refreshedClientsCount,
        refreshedRowsWritten,
        deletedStaleRowsCount,
      },
    });
  } catch (error) {
    console.error("GET /api/ghl/client-managers failed:", error);
    res.status(error.httpStatus || 502).json({
      error: sanitizeTextValue(error?.message, 500) || "Failed to load client-manager data from GHL.",
    });
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

app.get("/quickbooks-payments", (_req, res) => {
  res.sendFile(path.join(staticRoot, "quickbooks-payments.html"));
});

app.get("/client-managers", (_req, res) => {
  res.sendFile(path.join(staticRoot, "client-managers.html"));
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
  console.log("Web auth is enabled. Sign in at /login.");
  if (WEB_AUTH_USERNAME === DEFAULT_WEB_AUTH_USERNAME && WEB_AUTH_PASSWORD === DEFAULT_WEB_AUTH_PASSWORD) {
    console.warn("Using default web auth credentials. Set WEB_AUTH_USERNAME and WEB_AUTH_PASSWORD in environment.");
  }
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
  if (!isQuickBooksConfigured()) {
    console.warn("QuickBooks test API is disabled. Set QUICKBOOKS_CLIENT_ID/SECRET/REFRESH_TOKEN/REALM_ID.");
  }
  if (!isGhlConfigured()) {
    console.warn("GHL client-manager lookup is disabled. Set GHL_API_KEY and GHL_LOCATION_ID.");
  }
});
