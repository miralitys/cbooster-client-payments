const path = require("path");
const multer = require("multer");

const CUSTOM_DASHBOARD_TABLE_DEFAULT = "app_data";
const CUSTOM_DASHBOARD_DB_SCHEMA_DEFAULT = "public";
const CUSTOM_DASHBOARD_UPLOAD_MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024;
const CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD = 50000;
const CUSTOM_DASHBOARD_ALLOWED_TEXT_UPLOAD_EXTENSIONS = new Set([".csv", ".tsv", ".txt"]);
const CUSTOM_DASHBOARD_WIDGET_KEYS = ["managerTasks", "specialistTasks", "salesReport", "callsByManager"];

const CUSTOM_DASHBOARD_UPLOAD_TYPES = new Set(["tasks", "contacts", "calls"]);
const CUSTOM_DASHBOARD_DIRECTION_INCOMING = "incoming";
const CUSTOM_DASHBOARD_DIRECTION_OUTGOING = "outgoing";

const CUSTOM_DASHBOARD_USERS_KEY = "custom_dashboard/users.json";
const CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS = {
  tasks: "custom_dashboard/latest/tasks",
  contacts: "custom_dashboard/latest/contacts",
  calls: "custom_dashboard/latest/calls",
};
const CUSTOM_DASHBOARD_TASKS_SOURCE_KEY = "custom_dashboard/settings/tasks_source";
const CUSTOM_DASHBOARD_GHL_TASKS_LATEST_KEY = "custom_dashboard/latest/tasks_ghl";
const CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY = "custom_dashboard/sync/tasks_ghl_state";
const CUSTOM_DASHBOARD_TASK_MOVEMENTS_CACHE_KEY = "custom_dashboard/cache/tasks_movements_24h";
const CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY = "custom_dashboard/sync/calls_ghl_state";

const CUSTOM_DASHBOARD_TASKS_SOURCE_UPLOAD = "upload";
const CUSTOM_DASHBOARD_TASKS_SOURCE_GHL = "ghl";
const CUSTOM_DASHBOARD_TASKS_SOURCES = new Set([CUSTOM_DASHBOARD_TASKS_SOURCE_UPLOAD, CUSTOM_DASHBOARD_TASKS_SOURCE_GHL]);

const CUSTOM_DASHBOARD_GHL_API_KEY = (
  process.env.CUSTOM_DASHBOARD_GHL_API_KEY ||
  process.env.GHL_API_KEY ||
  process.env.GOHIGHLEVEL_API_KEY ||
  ""
)
  .toString()
  .trim();
const CUSTOM_DASHBOARD_GHL_LOCATION_ID = (process.env.CUSTOM_DASHBOARD_GHL_LOCATION_ID || process.env.GHL_LOCATION_ID || "")
  .toString()
  .trim();
const CUSTOM_DASHBOARD_GHL_API_BASE_URL = (
  process.env.CUSTOM_DASHBOARD_GHL_API_BASE_URL ||
  process.env.GHL_API_BASE_URL ||
  process.env.GOHIGHLEVEL_API_BASE_URL ||
  "https://services.leadconnectorhq.com"
)
  .toString()
  .trim()
  .replace(/\/+$/, "");
const CUSTOM_DASHBOARD_GHL_API_VERSION = (
  process.env.CUSTOM_DASHBOARD_GHL_API_VERSION ||
  process.env.GHL_API_VERSION ||
  "2021-07-28"
)
  .toString()
  .trim();
const CUSTOM_DASHBOARD_GHL_REQUEST_TIMEOUT_MS = Math.min(
  Math.max(
    parsePositiveInteger(
      process.env.CUSTOM_DASHBOARD_GHL_REQUEST_TIMEOUT_MS || process.env.GHL_REQUEST_TIMEOUT_MS,
      15000,
    ),
    2000,
  ),
  60000,
);
const CUSTOM_DASHBOARD_GHL_CONTACT_PAGE_LIMIT = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CONTACT_PAGE_LIMIT, 100), 10),
  200,
);
const CUSTOM_DASHBOARD_GHL_USERS_PAGE_LIMIT = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_USERS_PAGE_LIMIT, 200), 10),
  500,
);
const CUSTOM_DASHBOARD_GHL_USERS_MAX_PAGES = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_USERS_MAX_PAGES, 200), 1),
  2000,
);
const CUSTOM_DASHBOARD_GHL_CONTACT_MAX_PAGES = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CONTACT_MAX_PAGES, 500), 1),
  2000,
);
const CUSTOM_DASHBOARD_GHL_CONTACT_MAX_STALE_PAGES = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CONTACT_MAX_STALE_PAGES, 2), 1),
  10,
);
const CUSTOM_DASHBOARD_GHL_TASKS_SYNC_CONCURRENCY = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASKS_SYNC_CONCURRENCY, 6), 1),
  20,
);
const CUSTOM_DASHBOARD_GHL_TASKS_MAX_ITEMS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASKS_MAX_ITEMS, 100000), 1000),
  250000,
);
const CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED = resolveOptionalBoolean(
  process.env.CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED,
) === true;
const CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_INTERVAL_MS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_INTERVAL_MS, 15 * 60 * 1000), 60 * 1000),
  24 * 60 * 60 * 1000,
);
const CUSTOM_DASHBOARD_GHL_TASKS_CURSOR_SKEW_MS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASKS_CURSOR_SKEW_MS, 5 * 60 * 1000), 0),
  24 * 60 * 60 * 1000,
);
const CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED = resolveOptionalBoolean(
  process.env.CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED,
) !== false;
const CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE = (
  process.env.CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE ||
  "America/Chicago"
)
  .toString()
  .trim();
const CUSTOM_DASHBOARD_REPORT_TIMEZONE = (
  process.env.CUSTOM_DASHBOARD_REPORT_TIMEZONE ||
  process.env.CUSTOM_DASHBOARD_CALLS_REPORT_TIMEZONE ||
  CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE ||
  "America/Chicago"
)
  .toString()
  .trim();
const CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_HOUR = Math.min(
  Math.max(toSafeInteger(process.env.CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_HOUR, 22), 0),
  23,
);
const CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_MINUTE = Math.min(
  Math.max(toSafeInteger(process.env.CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_MINUTE, 0), 0),
  59,
);
const CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS, 24), 1),
  24 * 7,
);
const CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_PAGE_LIMIT = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_PAGE_LIMIT, 1000), 50),
  2000,
);
const CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_PAGES = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_PAGES, 200), 1),
  5000,
);
const CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_ROWS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_ROWS, 3000), 200),
  20000,
);
const CUSTOM_DASHBOARD_GHL_CALLS_CHANNEL = "Call";
const CUSTOM_DASHBOARD_GHL_CALLS_PAGE_LIMIT = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_PAGE_LIMIT, 100), 10),
  500,
);
const CUSTOM_DASHBOARD_GHL_CALLS_MAX_PAGES = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_MAX_PAGES, 500), 1),
  5000,
);
const CUSTOM_DASHBOARD_GHL_CALLS_MAX_ITEMS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_MAX_ITEMS, 100000), 1000),
  250000,
);
const CUSTOM_DASHBOARD_GHL_CALLS_INITIAL_LOOKBACK_DAYS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_INITIAL_LOOKBACK_DAYS, 30), 1),
  365,
);
const CUSTOM_DASHBOARD_GHL_CALLS_RETENTION_DAYS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_RETENTION_DAYS, 120), 1),
  730,
);
const CUSTOM_DASHBOARD_GHL_CALLS_CURSOR_SKEW_MS = Math.min(
  Math.max(parsePositiveInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_CURSOR_SKEW_MS, 10 * 60 * 1000), 0),
  24 * 60 * 60 * 1000,
);
const CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED = resolveOptionalBoolean(
  process.env.CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED,
) !== false;
const CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_TIMEZONE = (
  process.env.CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_TIMEZONE ||
  CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE ||
  "America/Chicago"
)
  .toString()
  .trim();
const CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_HOUR = Math.min(
  Math.max(toSafeInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_HOUR, 22), 0),
  23,
);
const CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_MINUTE = Math.min(
  Math.max(toSafeInteger(process.env.CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_MINUTE, 0), 0),
  59,
);

const DAY_IN_MS = 24 * 60 * 60 * 1000;

const TASK_MANAGER_ALIASES = ["manager", "closedby", "owner", "salesmanager", "менеджер", "менеджерпо продажам"];
const TASK_SPECIALIST_ALIASES = ["specialist", "assignee", "assignedto", "employee", "user", "специалист", "исполнитель"];
const TASK_TITLE_ALIASES = ["task", "taskname", "title", "subject", "description", "задача", "название"];
const TASK_CLIENT_ALIASES = ["client", "clientname", "lead", "contact", "клиент", "имяклиента"];
const TASK_STATUS_ALIASES = ["status", "state", "result", "статус"];
const TASK_DUE_DATE_ALIASES = ["duedate", "deadline", "date", "дедлайн", "срок"];
const TASK_CREATED_AT_ALIASES = ["createdat", "createddate", "datecreated", "дата создания"];
const TASK_COMPLETED_AT_ALIASES = ["completedat", "doneat", "closedat", "дата закрытия", "дата выполнения"];

const CONTACT_MANAGER_ALIASES = ["manager", "owner", "closedby", "assignedto", "менеджер"];
const CONTACT_CLIENT_ALIASES = ["client", "clientname", "lead", "contact", "клиент"];
const CONTACT_STATUS_ALIASES = ["status", "stage", "result", "pipeline", "статус"];
const CONTACT_DATE_ALIASES = ["date", "createdat", "activitydate", "updatedat", "contactdate", "дата"];
const CONTACT_CALLS_ALIASES = ["calls", "callcount", "totalcalls", "звонки"];
const CONTACT_ANSWERS_ALIASES = ["answered", "answers", "accepted", "connections", "ответы"];
const CONTACT_TALKS_ALIASES = ["talks", "conversations", "speaks", "talkover30", "разговоры"];
const CONTACT_INTERESTED_ALIASES = ["interested", "interestedcount", "hot", "warm", "заинтересованные"];
const CONTACT_CLOSED_ALIASES = ["closed", "closeddeals", "deals", "won", "sales", "закрытые сделки"];
const CONTACT_AMOUNT_ALIASES = ["amount", "sum", "revenue", "dealamount", "closedamount", "сумма"];

const CALL_MANAGER_ALIASES = ["manager", "owner", "user", "agent", "менеджер"];
const CALL_CLIENT_ALIASES = ["client", "clientname", "contact", "lead", "клиент"];
const CALL_PHONE_ALIASES = ["phone", "number", "clientphone", "fromnumber", "tonumber", "телефон"];
const CALL_DIRECTION_ALIASES = ["direction", "calltype", "type", "направление"];
const CALL_STATUS_ALIASES = ["status", "result", "disposition", "callstatus", "статус"];
const CALL_DURATION_ALIASES = ["duration", "durationsec", "talktime", "calllength", "длительность"];
const CALL_DATE_TIME_ALIASES = ["datetime", "calltime", "timestamp", "date", "дата", "время"];
const CALL_DATE_ALIASES = ["date", "calldate", "дата"];
const CALL_TIME_ALIASES = ["time", "calltime", "время"];

const MISSED_STATUS_MATCHERS = [
  "missed",
  "noanswer",
  "notanswered",
  "unanswered",
  "busy",
  "voicemail",
  "notpick",
  "неответ",
  "пропущ",
  "занят",
];
const ANSWERED_STATUS_MATCHERS = [
  "answered",
  "completed",
  "connected",
  "accepted",
  "answeredcall",
  "ответ",
  "соедин",
  "принят",
  "успеш",
];
const CLOSED_STATUS_MATCHERS = ["closed", "won", "sale", "sold", "dealwon", "успеш", "закрыт", "продан"];
const INTERESTED_STATUS_MATCHERS = ["interested", "warm", "hot", "qualified", "заинтерес", "тепл", "горяч"];
const COMPLETED_TASK_STATUS_MATCHERS = ["done", "completed", "closed", "resolved", "выполн", "закрыт", "готов"];

const GHL_CONTACT_ID_FIELDS = ["id", "_id", "contactId", "contact_id"];
const GHL_CONTACT_UPDATED_FIELDS = ["dateUpdated", "updatedAt", "lastUpdated", "date_updated"];
const GHL_CONTACT_FULL_NAME_FIELDS = ["name", "fullName", "contactName", "full_name"];
const GHL_CONTACT_FIRST_NAME_FIELDS = ["firstName", "firstname", "first_name"];
const GHL_CONTACT_LAST_NAME_FIELDS = ["lastName", "lastname", "last_name"];
const GHL_TASK_ID_FIELDS = ["id", "_id", "taskId", "task_id"];
const GHL_TASK_TITLE_FIELDS = ["title", "name", "task", "subject", "body", "description"];
const GHL_TASK_STATUS_FIELDS = ["status", "state", "result"];
const GHL_TASK_DUE_FIELDS = ["dueDate", "dueAt", "due", "date", "due_date"];
const GHL_TASK_CREATED_FIELDS = ["createdAt", "dateAdded", "createdOn", "created_date"];
const GHL_TASK_UPDATED_FIELDS = ["updatedAt", "dateUpdated", "lastUpdated", "updatedOn", "updated_date"];
const GHL_TASK_COMPLETED_FIELDS = ["completedAt", "dateCompleted", "completedOn", "doneAt", "closedAt"];
const GHL_TASK_OWNER_FIELDS = [
  "assignedTo",
  "assignedUserId",
  "assignedUser",
  "userId",
  "ownerId",
  "assigned_to",
  "assigned_user_id",
];
const GHL_TASK_OWNER_NAME_FIELDS = ["assignedToName", "assignedUserName", "ownerName", "assigned_to_name"];
const GHL_TASK_CONTACT_ID_FIELDS = ["contactId", "contactID", "contact_id"];
const GHL_CALL_ID_FIELDS = ["id", "_id", "messageId", "message_id"];
const GHL_CALL_STATUS_FIELDS = ["status", "callStatus", "meta.call.status", "meta.call.disposition", "disposition"];
const GHL_CALL_DIRECTION_FIELDS = ["direction", "meta.call.direction", "meta.direction", "call.direction"];
const GHL_CALL_UPDATED_FIELDS = ["dateUpdated", "updatedAt", "meta.call.updatedAt", "lastUpdated"];
const GHL_CALL_CREATED_FIELDS = ["dateAdded", "createdAt", "meta.call.createdAt"];
const GHL_CALL_AT_FIELDS = ["dateAdded", "timestamp", "dateCreated", "meta.call.startedAt", "meta.call.startTime"];
const GHL_CALL_DURATION_FIELDS = ["meta.call.duration", "duration", "call.duration", "meta.duration"];
const GHL_CALL_USER_ID_FIELDS = ["userId", "assignedTo", "ownerId", "assignedUserId", "user.id"];
const GHL_CALL_USER_NAME_FIELDS = [
  "userName",
  "assignedToName",
  "assignedUserName",
  "user.name",
  "user.fullName",
  "meta.call.userName",
];
const GHL_CALL_CONTACT_ID_FIELDS = ["contactId", "contactID", "contact.id"];
const GHL_CALL_CONTACT_NAME_FIELDS = [
  "contactName",
  "fullName",
  "name",
  "contact.fullName",
  "contact.name",
  "meta.call.contactName",
];
const GHL_CALL_PHONE_FIELDS = ["phone", "contactPhone", "contact.phone", "meta.call.phone", "number"];
const GHL_CALL_FROM_PHONE_FIELDS = ["from", "fromNumber", "meta.call.from", "meta.call.fromNumber"];
const GHL_CALL_TO_PHONE_FIELDS = ["to", "toNumber", "meta.call.to", "meta.call.toNumber"];

function registerCustomDashboardModule(config) {
  const {
    app,
    pool,
    requireWebPermission,
    hasWebAuthPermission,
    listWebAuthUsers,
    WEB_AUTH_PERMISSION_VIEW_DASHBOARD,
    WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL,
  } = config || {};

  if (!app || typeof app.get !== "function" || typeof app.post !== "function" || typeof app.put !== "function") {
    throw new Error("registerCustomDashboardModule requires Express app instance.");
  }

  if (typeof requireWebPermission !== "function") {
    throw new Error("registerCustomDashboardModule requires requireWebPermission().");
  }

  const dbSchema = resolveSafeSqlIdentifier(process.env.DB_SCHEMA, CUSTOM_DASHBOARD_DB_SCHEMA_DEFAULT);
  const tableName = resolveSafeSqlIdentifier(process.env.DB_CUSTOM_DASHBOARD_APP_DATA_TABLE_NAME, CUSTOM_DASHBOARD_TABLE_DEFAULT);
  const appDataTable = `"${dbSchema}"."${tableName}"`;

  const uploadMiddleware = multer({
    storage: multer.memoryStorage(),
    limits: {
      files: 1,
      fileSize: CUSTOM_DASHBOARD_UPLOAD_MAX_FILE_SIZE_BYTES,
    },
  }).single("file");

  let appDataReadyPromise = null;

  async function ensureAppDataTableReady() {
    if (!pool) {
      throw createHttpError("Database is not configured. Add DATABASE_URL in Render environment variables.", 503);
    }

    if (!appDataReadyPromise) {
      appDataReadyPromise = pool
        .query(
          `
            CREATE TABLE IF NOT EXISTS ${appDataTable} (
              key TEXT PRIMARY KEY,
              value JSONB NOT NULL DEFAULT '{}'::jsonb,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
          `,
        )
        .catch((error) => {
          appDataReadyPromise = null;
          throw error;
        });
    }

    return appDataReadyPromise;
  }

  async function readAppDataValue(key, fallbackValue) {
    await ensureAppDataTableReady();
    const normalizedKey = sanitizeTextValue(key, 240);
    if (!normalizedKey) {
      return fallbackValue;
    }

    const result = await pool.query(`SELECT value FROM ${appDataTable} WHERE key = $1 LIMIT 1`, [normalizedKey]);
    if (!result.rows.length) {
      return fallbackValue;
    }

    return result.rows[0]?.value ?? fallbackValue;
  }

  async function upsertAppDataValue(key, value) {
    await ensureAppDataTableReady();
    const normalizedKey = sanitizeTextValue(key, 240);
    if (!normalizedKey) {
      throw createHttpError("Invalid app_data key.", 400);
    }

    const serialized = JSON.stringify(value ?? null);
    await pool.query(
      `
        INSERT INTO ${appDataTable} (key, value, updated_at)
        VALUES ($1, $2::jsonb, NOW())
        ON CONFLICT (key)
        DO UPDATE
        SET value = EXCLUDED.value,
            updated_at = NOW()
      `,
      [normalizedKey, serialized],
    );
  }

  let ghlTasksSyncInFlight = null;
  let ghlTasksAutoSyncIntervalId = null;
  let ghlTasksSyncRuntimeState = {
    inFlight: false,
    lastStartedAt: "",
    lastFinishedAt: "",
    lastTrigger: "",
    lastError: "",
  };
  let taskMovementsAutoSyncTimerId = null;
  let taskMovementsAutoSyncRuntimeState = {
    inFlight: false,
    lastStartedAt: "",
    lastFinishedAt: "",
    lastSuccessAt: "",
    nextRunAt: "",
    lastError: "",
  };
  let ghlCallsSyncInFlight = null;
  let ghlCallsAutoSyncTimerId = null;
  let ghlCallsSyncRuntimeState = {
    inFlight: false,
    lastStartedAt: "",
    lastFinishedAt: "",
    lastSuccessAt: "",
    nextRunAt: "",
    lastTrigger: "",
    lastError: "",
  };

  function isGhlTasksSyncConfigured() {
    return Boolean(CUSTOM_DASHBOARD_GHL_API_KEY && CUSTOM_DASHBOARD_GHL_LOCATION_ID);
  }

  function normalizeTasksSource(value) {
    const normalized = sanitizeTextValue(value, 40).toLowerCase();
    if (CUSTOM_DASHBOARD_TASKS_SOURCES.has(normalized)) {
      return normalized;
    }
    return CUSTOM_DASHBOARD_TASKS_SOURCE_UPLOAD;
  }

  async function readTasksSourceSetting() {
    const sourceRaw = await readAppDataValue(CUSTOM_DASHBOARD_TASKS_SOURCE_KEY, null);
    if (sourceRaw && typeof sourceRaw === "object") {
      return normalizeTasksSource(sourceRaw.source);
    }
    return normalizeTasksSource(sourceRaw);
  }

  async function saveTasksSourceSetting(source, actorUsername) {
    const normalizedSource = normalizeTasksSource(source);
    if (normalizedSource === CUSTOM_DASHBOARD_TASKS_SOURCE_GHL && !isGhlTasksSyncConfigured()) {
      throw createHttpError("GHL integration is not configured. Set GHL_API_KEY and GHL_LOCATION_ID.", 400);
    }

    const payload = {
      source: normalizedSource,
      updatedAt: new Date().toISOString(),
      updatedBy: sanitizeTextValue(actorUsername, 220),
    };
    await upsertAppDataValue(CUSTOM_DASHBOARD_TASKS_SOURCE_KEY, payload);
    return payload;
  }

  function buildGhlRequestHeaders(includeJsonBody = false) {
    const headers = {
      Authorization: `Bearer ${CUSTOM_DASHBOARD_GHL_API_KEY}`,
      Version: CUSTOM_DASHBOARD_GHL_API_VERSION || "2021-07-28",
      Accept: "application/json",
    };
    if (includeJsonBody) {
      headers["Content-Type"] = "application/json";
    }
    return headers;
  }

  function buildGhlUrl(pathname, query = {}) {
    const normalizedPath = `/${sanitizeTextValue(pathname, 600).replace(/^\/+/, "")}`;
    const url = new URL(`${CUSTOM_DASHBOARD_GHL_API_BASE_URL}${normalizedPath}`);

    for (const [key, rawValue] of Object.entries(query || {})) {
      if (rawValue === null || rawValue === undefined) {
        continue;
      }

      if (Array.isArray(rawValue)) {
        for (const item of rawValue) {
          const arrayValue = sanitizeTextValue(item, 1000);
          if (!arrayValue) {
            continue;
          }
          url.searchParams.append(key, arrayValue);
        }
        continue;
      }

      const value = sanitizeTextValue(rawValue, 1000);
      if (!value) {
        continue;
      }
      url.searchParams.set(key, value);
    }

    return url;
  }

  async function requestGhlApi(pathname, options = {}) {
    const method = sanitizeTextValue(options.method || "GET", 12).toUpperCase() || "GET";
    const includeJsonBody = method !== "GET" && method !== "HEAD";
    const tolerateNotFound = Boolean(options.tolerateNotFound);
    const headers = buildGhlRequestHeaders(includeJsonBody);
    const query = options.query && typeof options.query === "object" ? options.query : {};
    const url = buildGhlUrl(pathname, query);
    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
    }, CUSTOM_DASHBOARD_GHL_REQUEST_TIMEOUT_MS);

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
      if (error?.name === "AbortError") {
        throw createHttpError(`GHL request timed out after ${CUSTOM_DASHBOARD_GHL_REQUEST_TIMEOUT_MS}ms (${pathname}).`, 504);
      }
      const details = sanitizeTextValue(error?.message, 300) || "Unknown network error.";
      throw createHttpError(`GHL request failed (${pathname}): ${details}`, 503);
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
        body?.message || body?.error || body?.detail || body?.details || body?.meta?.message || responseText,
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

  function extractArrayCandidate(payload, candidates) {
    const paths = Array.isArray(candidates) ? candidates : [];
    for (const candidatePath of paths) {
      const value = readObjectPath(payload, candidatePath);
      if (Array.isArray(value)) {
        return value;
      }
    }
    return [];
  }

  function extractGhlContactsFromPayload(payload) {
    return extractArrayCandidate(payload, [
      "contacts",
      "data.contacts",
      "data.items",
      "items",
      "data",
      "",
    ]).filter((item) => item && typeof item === "object");
  }

  function extractGhlUsersFromPayload(payload) {
    return extractArrayCandidate(payload, [
      "users",
      "data.users",
      "data.items",
      "items",
      "data",
      "",
    ]).filter((item) => item && typeof item === "object");
  }

  function extractGhlTasksFromPayload(payload) {
    return extractArrayCandidate(payload, [
      "tasks",
      "data.tasks",
      "data.items",
      "items",
      "data",
      "",
    ]).filter((item) => item && typeof item === "object");
  }

  function extractGhlMessagesFromPayload(payload) {
    return extractArrayCandidate(payload, [
      "messages",
      "data.messages",
      "data.items",
      "items",
      "data",
      "",
    ]).filter((item) => item && typeof item === "object");
  }

  function extractGhlMessagesNextCursor(payload) {
    return sanitizeTextValue(
      pickValueFromObject(payload, ["nextCursor", "meta.nextCursor", "pagination.nextCursor", "cursor.next"]),
      500,
    );
  }

  function normalizeGhlTasksSearchAfterCursor(rawValue) {
    if (Array.isArray(rawValue)) {
      const cursor = [];
      for (const rawItem of rawValue) {
        if (rawItem === null || rawItem === undefined || rawItem === "") {
          continue;
        }

        if (typeof rawItem === "number" && Number.isFinite(rawItem)) {
          cursor.push(rawItem);
          continue;
        }

        const value = sanitizeTextValue(rawItem, 1200);
        if (!value) {
          continue;
        }
        cursor.push(value);
      }
      return cursor;
    }

    const singleValue = sanitizeTextValue(rawValue, 1200);
    return singleValue ? [singleValue] : [];
  }

  function isSameGhlTasksSearchAfterCursor(leftCursor, rightCursor) {
    const left = Array.isArray(leftCursor) ? leftCursor : [];
    const right = Array.isArray(rightCursor) ? rightCursor : [];
    if (left.length !== right.length) {
      return false;
    }

    for (let index = 0; index < left.length; index += 1) {
      if (String(left[index]) !== String(right[index])) {
        return false;
      }
    }

    return true;
  }

  function extractGhlTasksSearchAfterFromPayload(payload, tasks) {
    const direct = normalizeGhlTasksSearchAfterCursor(
      pickValueFromObject(payload, ["meta.searchAfter", "searchAfter", "meta.nextSearchAfter", "nextSearchAfter"]),
    );
    if (direct.length) {
      return direct;
    }

    const list = Array.isArray(tasks) ? tasks : [];
    for (let index = list.length - 1; index >= 0; index -= 1) {
      const item = list[index];
      if (!item || typeof item !== "object") {
        continue;
      }

      const value = normalizeGhlTasksSearchAfterCursor(pickValueFromObject(item, ["searchAfter", "search_after"]));
      if (value.length) {
        return value;
      }
    }

    return [];
  }

  async function listGhlUsersIndex() {
    const endpointPaths = ["/users/", "/users"];

    for (const endpointPath of endpointPaths) {
      const index = new Map();
      let page = 1;
      let stalePages = 0;

      while (page <= CUSTOM_DASHBOARD_GHL_USERS_MAX_PAGES) {
        let response;
        try {
          response = await requestGhlApi(endpointPath, {
            method: "GET",
            query: {
              locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
              limit: CUSTOM_DASHBOARD_GHL_USERS_PAGE_LIMIT,
              page,
            },
            tolerateNotFound: true,
          });
        } catch {
          break;
        }

        if (!response.ok) {
          break;
        }

        const users = extractGhlUsersFromPayload(response.body);
        if (!users.length) {
          break;
        }

        let addedOnPage = 0;
        for (const user of users) {
          const userId = sanitizeTextValue(
            pickValueFromObject(user, ["id", "_id", "userId", "user_id"]),
            160,
          );
          if (!userId) {
            continue;
          }

          const userName = buildFullName(
            sanitizeTextValue(pickValueFromObject(user, ["firstName", "firstname", "first_name"]), 120),
            sanitizeTextValue(pickValueFromObject(user, ["lastName", "lastname", "last_name"]), 120),
          );
          const fallbackName = sanitizeTextValue(
            pickValueFromObject(user, ["name", "fullName", "email", "username"]),
            220,
          );
          const resolved = userName || fallbackName || userId;
          if (!index.has(userId)) {
            addedOnPage += 1;
          }
          index.set(userId, resolved);
        }

        if (addedOnPage === 0) {
          stalePages += 1;
        } else {
          stalePages = 0;
        }
        if (stalePages >= CUSTOM_DASHBOARD_GHL_CONTACT_MAX_STALE_PAGES) {
          break;
        }

        const hasMoreHint = resolveGhlContactsHasMore(response.body, page, CUSTOM_DASHBOARD_GHL_USERS_PAGE_LIMIT);
        const likelyLastPageBySize = users.length < CUSTOM_DASHBOARD_GHL_USERS_PAGE_LIMIT;

        if (hasMoreHint === false) {
          break;
        }
        if (likelyLastPageBySize && hasMoreHint !== true) {
          break;
        }

        page += 1;
      }

      if (index.size) {
        return index;
      }
    }

    return new Map();
  }

  async function listAllGhlContacts() {
    const contactsByKey = new Map();
    let page = 1;
    let stalePages = 0;
    let fallbackIndex = 0;

    while (page <= CUSTOM_DASHBOARD_GHL_CONTACT_MAX_PAGES) {
      const response = await requestGhlApi("/contacts/search", {
        method: "POST",
        body: {
          locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
          page,
          pageLimit: CUSTOM_DASHBOARD_GHL_CONTACT_PAGE_LIMIT,
          query: "",
        },
      });

      const contacts = extractGhlContactsFromPayload(response.body);
      if (!contacts.length) {
        break;
      }

      let addedOnPage = 0;
      for (const contact of contacts) {
        const contactId = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_ID_FIELDS), 160);
        const key = contactId || `fallback:${page}:${fallbackIndex++}`;
        if (!contactsByKey.has(key)) {
          addedOnPage += 1;
        }
        contactsByKey.set(key, contact);
      }

      if (addedOnPage === 0) {
        stalePages += 1;
      } else {
        stalePages = 0;
      }

      const hasMoreHint = resolveGhlContactsHasMore(response.body, page, CUSTOM_DASHBOARD_GHL_CONTACT_PAGE_LIMIT);
      const likelyLastPageBySize = contacts.length < CUSTOM_DASHBOARD_GHL_CONTACT_PAGE_LIMIT;
      const shouldStopByStalePages = stalePages >= CUSTOM_DASHBOARD_GHL_CONTACT_MAX_STALE_PAGES;

      if (shouldStopByStalePages) {
        console.warn(
          `[custom-dashboard] GHL contacts pagination produced ${stalePages} stale page(s). Stopping at page ${page}.`,
        );
        break;
      }

      if (hasMoreHint === false) {
        break;
      }
      if (likelyLastPageBySize && hasMoreHint !== true) {
        break;
      }

      page += 1;
    }

    return [...contactsByKey.values()];
  }

  async function listGhlContactTasks(contactId) {
    const normalizedContactId = sanitizeTextValue(contactId, 160);
    if (!normalizedContactId) {
      return [];
    }

    const attempts = [
      () =>
        requestGhlApi(`/contacts/${encodeURIComponent(normalizedContactId)}/tasks`, {
          method: "GET",
          query: {
            locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
          },
          tolerateNotFound: true,
        }),
      () =>
        requestGhlApi(`/contacts/${encodeURIComponent(normalizedContactId)}/tasks/`, {
          method: "GET",
          query: {
            locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
          },
          tolerateNotFound: true,
        }),
      () =>
        requestGhlApi("/tasks", {
          method: "GET",
          query: {
            locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
            contactId: normalizedContactId,
          },
          tolerateNotFound: true,
        }),
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

      const tasks = extractGhlTasksFromPayload(response.body);
      return tasks;
    }

    return [];
  }

  function readSyncContactIndex(rawIndex) {
    const source = rawIndex && typeof rawIndex === "object" ? rawIndex : {};
    const next = {};

    for (const [rawContactId, rawEntry] of Object.entries(source)) {
      const contactId = sanitizeTextValue(rawContactId, 160);
      if (!contactId) {
        continue;
      }

      const entry = rawEntry && typeof rawEntry === "object" ? rawEntry : {};
      const taskIdsRaw = Array.isArray(entry.taskIds) ? entry.taskIds : [];
      next[contactId] = {
        updatedAt: normalizeIsoDateOrEmpty(entry.updatedAt),
        taskIds: taskIdsRaw.map((item) => sanitizeTextValue(item, 260)).filter(Boolean),
      };
    }

    return next;
  }

  function normalizeGhlTasksSyncState(rawValue) {
    const source = rawValue && typeof rawValue === "object" ? rawValue : {};
    const mode = sanitizeTextValue(source.lastMode, 20).toLowerCase();

    return {
      version: 1,
      lastAttemptedAt: normalizeIsoDateOrEmpty(source.lastAttemptedAt),
      lastSuccessfulSyncAt: normalizeIsoDateOrEmpty(source.lastSuccessfulSyncAt),
      lastMode: mode === "full" ? "full" : mode === "delta" ? "delta" : "",
      lastError: sanitizeTextValue(source.lastError, 600),
      cursorUpdatedAt: normalizeIsoDateOrEmpty(source.cursorUpdatedAt),
      contactTaskIndex: readSyncContactIndex(source.contactTaskIndex),
      stats: {
        contactsTotal: Math.max(0, toSafeInteger(source.stats?.contactsTotal, 0)),
        contactsProcessed: Math.max(0, toSafeInteger(source.stats?.contactsProcessed, 0)),
        contactsDeleted: Math.max(0, toSafeInteger(source.stats?.contactsDeleted, 0)),
        tasksTotal: Math.max(0, toSafeInteger(source.stats?.tasksTotal, 0)),
      },
    };
  }

  function normalizeGhlCallsSyncState(rawValue) {
    const source = rawValue && typeof rawValue === "object" ? rawValue : {};
    const mode = sanitizeTextValue(source.lastMode, 20).toLowerCase();

    return {
      version: 1,
      lastAttemptedAt: normalizeIsoDateOrEmpty(source.lastAttemptedAt),
      lastSuccessfulSyncAt: normalizeIsoDateOrEmpty(source.lastSuccessfulSyncAt),
      lastMode: mode === "full" ? "full" : mode === "delta" ? "delta" : "",
      lastError: sanitizeTextValue(source.lastError, 600),
      cursorUpdatedAt: normalizeIsoDateOrEmpty(source.cursorUpdatedAt),
      stats: {
        pagesFetched: Math.max(0, toSafeInteger(source.stats?.pagesFetched, 0)),
        scannedMessages: Math.max(0, toSafeInteger(source.stats?.scannedMessages, 0)),
        importedCalls: Math.max(0, toSafeInteger(source.stats?.importedCalls, 0)),
        storedCalls: Math.max(0, toSafeInteger(source.stats?.storedCalls, 0)),
      },
    };
  }

  function buildGhlCallsAutoSyncInfo(syncState) {
    const state = normalizeGhlCallsSyncState(syncState);
    return {
      enabled: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED,
      timeZone: resolveSafeTimeZone(CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_TIMEZONE, "America/Chicago"),
      hour: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_HOUR,
      minute: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_MINUTE,
      inFlight: Boolean(ghlCallsSyncRuntimeState.inFlight),
      nextRunAt: ghlCallsSyncRuntimeState.nextRunAt || "",
      lastStartedAt: ghlCallsSyncRuntimeState.lastStartedAt || "",
      lastFinishedAt: ghlCallsSyncRuntimeState.lastFinishedAt || "",
      lastSuccessAt: ghlCallsSyncRuntimeState.lastSuccessAt || state.lastSuccessfulSyncAt || "",
      lastError: ghlCallsSyncRuntimeState.lastError || state.lastError || "",
    };
  }

  function buildCallsSyncPayload(syncState) {
    const state = normalizeGhlCallsSyncState(syncState);
    return {
      configured: isGhlTasksSyncConfigured(),
      syncInFlight: Boolean(ghlCallsSyncRuntimeState.inFlight),
      lastAttemptedAt: state.lastAttemptedAt || ghlCallsSyncRuntimeState.lastStartedAt || "",
      lastSyncedAt: state.lastSuccessfulSyncAt || "",
      lastMode: state.lastMode || "",
      lastError: ghlCallsSyncRuntimeState.lastError || state.lastError || "",
      cursorUpdatedAt: state.cursorUpdatedAt || "",
      stats: state.stats,
      autoSync: buildGhlCallsAutoSyncInfo(state),
    };
  }

  function buildTasksSourcePayload(selectedSource, syncState) {
    const state = normalizeGhlTasksSyncState(syncState);
    return {
      selected: normalizeTasksSource(selectedSource),
      options: [CUSTOM_DASHBOARD_TASKS_SOURCE_UPLOAD, CUSTOM_DASHBOARD_TASKS_SOURCE_GHL],
      ghlConfigured: isGhlTasksSyncConfigured(),
      autoSyncEnabled: CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED,
      syncInFlight: Boolean(ghlTasksSyncRuntimeState.inFlight),
      lastAttemptedAt: state.lastAttemptedAt || ghlTasksSyncRuntimeState.lastStartedAt || "",
      lastSyncedAt: state.lastSuccessfulSyncAt || "",
      lastMode: state.lastMode || "",
      lastError: ghlTasksSyncRuntimeState.lastError || state.lastError || "",
      cursorUpdatedAt: state.cursorUpdatedAt || "",
      stats: state.stats,
    };
  }

  function pickTaskCacheKey(task) {
    const sourceTaskId = sanitizeTextValue(task?.sourceTaskId, 200);
    if (sourceTaskId) {
      return `task:${sourceTaskId}`;
    }

    const id = sanitizeTextValue(task?.id, 260);
    if (id) {
      return `id:${id}`;
    }

    return "";
  }

  function normalizeGhlContactRecord(contact) {
    const contactId = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_ID_FIELDS), 160);
    if (!contactId) {
      return null;
    }

    const fullName = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_FULL_NAME_FIELDS), 260);
    const firstName = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_FIRST_NAME_FIELDS), 120);
    const lastName = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_LAST_NAME_FIELDS), 120);
    const email = sanitizeTextValue(pickValueFromObject(contact, ["email"]), 240);
    const phone = sanitizeTextValue(pickValueFromObject(contact, ["phone", "mobilePhone"]), 80);
    const clientName = fullName || buildFullName(firstName, lastName) || email || phone || contactId;
    const updatedRaw = sanitizeTextValue(pickValueFromObject(contact, GHL_CONTACT_UPDATED_FIELDS), 120);
    const updatedTimestamp = parseDateTimeValue(updatedRaw);

    return {
      contactId,
      clientName,
      updatedAtIso: updatedTimestamp !== null ? new Date(updatedTimestamp).toISOString() : "",
      updatedTimestamp,
      raw: contact,
    };
  }

  function looksLikeOpaqueUserId(value) {
    const candidate = sanitizeTextValue(value, 240);
    if (!candidate) {
      return false;
    }
    if (candidate.includes(" ") || candidate.includes("@")) {
      return false;
    }
    return /^[A-Za-z0-9_-]{14,}$/.test(candidate);
  }

  function resolveGhlTaskOwnerName(task, usersIndex) {
    const ownerNameFromDetails = sanitizeTextValue(
      pickValueFromObject(task, [
        "assignedToUserDetails.name",
        "assignedToUserDetails.fullName",
        "assignedToUserDetails.displayName",
        "assignedToUserDetails.userName",
        "assignedUserDetails.name",
        "assignedUserDetails.fullName",
        "assignedUserDetails.displayName",
        "assignedUserDetails.userName",
      ]),
      220,
    );
    if (ownerNameFromDetails && !looksLikeOpaqueUserId(ownerNameFromDetails)) {
      return ownerNameFromDetails;
    }

    const ownerFirstName = sanitizeTextValue(
      pickValueFromObject(task, [
        "assignedToUserDetails.firstName",
        "assignedToUserDetails.firstname",
        "assignedUserDetails.firstName",
        "assignedUserDetails.firstname",
      ]),
      120,
    );
    const ownerLastName = sanitizeTextValue(
      pickValueFromObject(task, [
        "assignedToUserDetails.lastName",
        "assignedToUserDetails.lastname",
        "assignedUserDetails.lastName",
        "assignedUserDetails.lastname",
      ]),
      120,
    );
    const ownerFullName = buildFullName(ownerFirstName, ownerLastName);
    if (ownerFullName && !looksLikeOpaqueUserId(ownerFullName)) {
      return ownerFullName;
    }

    const ownerNameDirect = sanitizeTextValue(
      pickValueFromObject(task, [...GHL_TASK_OWNER_NAME_FIELDS, "assignedTo.name", "assignedUser.name", "owner.name"]),
      220,
    );
    if (ownerNameDirect && !looksLikeOpaqueUserId(ownerNameDirect)) {
      return ownerNameDirect;
    }

    const ownerValue = pickValueFromObject(task, GHL_TASK_OWNER_FIELDS);
    const ownerObject = ownerValue && typeof ownerValue === "object" ? ownerValue : null;

    const ownerNameFromObject = ownerObject
      ? sanitizeTextValue(
          pickValueFromObject(ownerObject, ["name", "fullName", "email", "username"]),
          220,
        )
      : "";
    if (ownerNameFromObject && !looksLikeOpaqueUserId(ownerNameFromObject)) {
      return ownerNameFromObject;
    }

    const ownerId = ownerObject
      ? sanitizeTextValue(pickValueFromObject(ownerObject, ["id", "_id", "userId", "user_id"]), 160)
      : sanitizeTextValue(ownerValue, 160);
    if (!ownerId) {
      return "";
    }
    const resolvedFromIndex = sanitizeTextValue(usersIndex.get(ownerId), 220);
    if (resolvedFromIndex && !looksLikeOpaqueUserId(resolvedFromIndex)) {
      return resolvedFromIndex;
    }
    return ownerId;
  }

  function resolveGhlTaskClientName(task) {
    const direct = sanitizeTextValue(
      pickValueFromObject(task, [
        "contactDetails.name",
        "contact.name",
        "contact.fullName",
        "contactName",
        "clientName",
        "leadName",
      ]),
      260,
    );
    if (direct) {
      return direct;
    }

    const firstName = sanitizeTextValue(
      pickValueFromObject(task, ["contactDetails.firstName", "contact.firstName", "contact.firstname"]),
      120,
    );
    const lastName = sanitizeTextValue(
      pickValueFromObject(task, ["contactDetails.lastName", "contact.lastName", "contact.lastname"]),
      120,
    );
    const fullName = buildFullName(firstName, lastName);
    if (fullName) {
      return fullName;
    }

    const email = sanitizeTextValue(pickValueFromObject(task, ["contactDetails.email", "contact.email"]), 240);
    if (email) {
      return email;
    }
    return sanitizeTextValue(pickValueFromObject(task, ["contactDetails.phone", "contact.phone"]), 80);
  }

  function resolveGhlTaskMovementType(movementCandidate) {
    const candidate = sanitizeTextValue(movementCandidate, 20).toLowerCase();
    if (candidate === "created" || candidate === "completed") {
      return candidate;
    }
    return "updated";
  }

  function normalizeGhlTaskMovementRow(rawTask, usersIndex, sinceTimestamp) {
    const taskIdRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_ID_FIELDS), 160);
    const taskTitle = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_TITLE_FIELDS), 500) || "Task";
    const status = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_STATUS_FIELDS), 220);
    const createdRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_CREATED_FIELDS), 160);
    const updatedRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_UPDATED_FIELDS), 160);
    const completedRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_COMPLETED_FIELDS), 160);
    const completedFlag = resolveOptionalBoolean(pickValueFromObject(rawTask, ["completed", "isCompleted", "done"]));
    const statusComparable = normalizeComparableText(status, 220);
    const completedAtTimestamp = parseDateTimeValue(completedRaw);
    const createdAtTimestamp = parseDateTimeValue(createdRaw);
    const updatedAtTimestamp = parseDateTimeValue(updatedRaw) || completedAtTimestamp || createdAtTimestamp;
    if (!Number.isFinite(updatedAtTimestamp) || updatedAtTimestamp < sinceTimestamp) {
      return null;
    }

    const isCompleted =
      completedFlag === true ||
      Boolean(completedAtTimestamp) ||
      COMPLETED_TASK_STATUS_MATCHERS.some((token) => statusComparable.includes(token));

    const contactId = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_CONTACT_ID_FIELDS), 160);
    const managerName = resolveGhlTaskOwnerName(rawTask, usersIndex) || "Unassigned";
    const clientName = resolveGhlTaskClientName(rawTask);
    const movementTypeRaw = Number.isFinite(createdAtTimestamp) && createdAtTimestamp >= sinceTimestamp
      ? "created"
      : isCompleted && Number.isFinite(completedAtTimestamp) && completedAtTimestamp >= sinceTimestamp
        ? "completed"
        : isCompleted && !Number.isFinite(completedAtTimestamp)
          ? "completed"
          : "updated";
    const movementType = resolveGhlTaskMovementType(movementTypeRaw);
    const safeTaskId = taskIdRaw || `${contactId || "unknown"}:${taskTitle}:${updatedAtTimestamp}`;

    return {
      taskId: safeTaskId,
      title: taskTitle,
      managerName,
      clientName: clientName || "",
      contactId,
      status: status || (isCompleted ? "completed" : "open"),
      isCompleted,
      changeType: movementType,
      createdAt: Number.isFinite(createdAtTimestamp) ? new Date(createdAtTimestamp).toISOString() : "",
      updatedAt: new Date(updatedAtTimestamp).toISOString(),
      completedAt: Number.isFinite(completedAtTimestamp) ? new Date(completedAtTimestamp).toISOString() : "",
      updatedAtTimestamp,
    };
  }

  async function buildGhlTaskMovementsPayload(options = {}) {
    if (!isGhlTasksSyncConfigured()) {
      throw createHttpError("GHL integration is not configured. Set GHL_API_KEY and GHL_LOCATION_ID.", 400);
    }

    const requestedHours = Math.max(1, toSafeInteger(options.hours, 24));
    const periodHours = Math.min(requestedHours, CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS);
    const generatedAtTimestamp = Date.now();
    const generatedAt = new Date(generatedAtTimestamp).toISOString();
    const sinceTimestamp = generatedAtTimestamp - periodHours * 60 * 60 * 1000;
    const since = new Date(sinceTimestamp).toISOString();
    const usersIndex = await listGhlUsersIndex();

    let page = 0;
    let scannedTasks = 0;
    let changedTasks = 0;
    let createdTasks = 0;
    let completedTasks = 0;
    let searchAfter = [];
    let truncatedRows = false;

    const rows = [];
    const managerSummaryMap = new Map();
    const contactIds = new Set();
    const seenTaskKeys = new Set();

    while (page < CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_PAGES) {
      const requestBody = {
        limit: CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_PAGE_LIMIT,
        query: "",
      };
      if (searchAfter.length) {
        requestBody.searchAfter = searchAfter;
      }

      const response = await requestGhlApi(`/locations/${encodeURIComponent(CUSTOM_DASHBOARD_GHL_LOCATION_ID)}/tasks/search`, {
        method: "POST",
        body: requestBody,
      });
      const tasks = extractGhlTasksFromPayload(response.body);
      if (!tasks.length) {
        break;
      }
      page += 1;

      for (const task of tasks) {
        scannedTasks += 1;
        const movementRow = normalizeGhlTaskMovementRow(task, usersIndex, sinceTimestamp);
        if (!movementRow) {
          continue;
        }

        const dedupeKeyRaw = movementRow.taskId || `${movementRow.contactId}:${movementRow.title}:${movementRow.updatedAt}`;
        const dedupeKey = normalizeComparableText(dedupeKeyRaw, 500);
        if (dedupeKey && seenTaskKeys.has(dedupeKey)) {
          continue;
        }
        if (dedupeKey) {
          seenTaskKeys.add(dedupeKey);
        }

        changedTasks += 1;
        if (movementRow.changeType === "created") {
          createdTasks += 1;
        } else if (movementRow.changeType === "completed") {
          completedTasks += 1;
        }

        const managerComparable = normalizeComparableText(movementRow.managerName, 220) || "unassigned";
        const existingManager = managerSummaryMap.get(managerComparable) || {
          managerName: movementRow.managerName || "Unassigned",
          changed: 0,
          created: 0,
          completed: 0,
        };
        existingManager.changed += 1;
        if (movementRow.changeType === "created") {
          existingManager.created += 1;
        }
        if (movementRow.changeType === "completed") {
          existingManager.completed += 1;
        }
        managerSummaryMap.set(managerComparable, existingManager);

        if (movementRow.contactId) {
          contactIds.add(movementRow.contactId);
        }

        if (rows.length < CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_ROWS) {
          rows.push(movementRow);
        } else {
          truncatedRows = true;
        }
      }

      const nextSearchAfter = extractGhlTasksSearchAfterFromPayload(response.body, tasks);
      if (!nextSearchAfter.length || isSameGhlTasksSearchAfterCursor(nextSearchAfter, searchAfter)) {
        break;
      }
      searchAfter = nextSearchAfter;
    }

    rows.sort((left, right) => {
      const leftUpdated = Number.isFinite(left.updatedAtTimestamp) ? left.updatedAtTimestamp : 0;
      const rightUpdated = Number.isFinite(right.updatedAtTimestamp) ? right.updatedAtTimestamp : 0;
      if (leftUpdated !== rightUpdated) {
        return rightUpdated - leftUpdated;
      }
      return sanitizeTextValue(left.title, 300).localeCompare(sanitizeTextValue(right.title, 300), "en-US", {
        sensitivity: "base",
      });
    });

    const managerSummary = [...managerSummaryMap.values()]
      .map((row) => ({
        managerName: row.managerName,
        changed: row.changed,
        created: row.created,
        completed: row.completed,
        updated: Math.max(0, row.changed - row.created - row.completed),
      }))
      .sort((left, right) => {
        if (right.changed !== left.changed) {
          return right.changed - left.changed;
        }
        return left.managerName.localeCompare(right.managerName, "en-US", { sensitivity: "base" });
      });

    const serializedRows = rows.map((item) => ({
      taskId: item.taskId,
      title: item.title,
      managerName: item.managerName,
      clientName: item.clientName,
      contactId: item.contactId,
      status: item.status,
      isCompleted: item.isCompleted,
      changeType: item.changeType,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      completedAt: item.completedAt,
    }));

    return {
      ok: true,
      generatedAt,
      periodHours,
      since,
      scannedTasks,
      changedTasks,
      createdTasks,
      completedTasks,
      updatedTasks: Math.max(0, changedTasks - createdTasks - completedTasks),
      totalPages: page,
      managers: managerSummary.length,
      contacts: contactIds.size,
      rowsReturned: serializedRows.length,
      rowLimit: CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_ROWS,
      truncatedRows,
      rows: serializedRows,
      managerSummary,
    };
  }

  function buildTaskMovementsAutoSyncInfo() {
    return {
      enabled: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED,
      timeZone: resolveSafeTimeZone(CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE, "America/Chicago"),
      hour: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_HOUR,
      minute: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_MINUTE,
      inFlight: Boolean(taskMovementsAutoSyncRuntimeState.inFlight),
      nextRunAt: taskMovementsAutoSyncRuntimeState.nextRunAt || "",
      lastStartedAt: taskMovementsAutoSyncRuntimeState.lastStartedAt || "",
      lastFinishedAt: taskMovementsAutoSyncRuntimeState.lastFinishedAt || "",
      lastSuccessAt: taskMovementsAutoSyncRuntimeState.lastSuccessAt || "",
      lastError: taskMovementsAutoSyncRuntimeState.lastError || "",
    };
  }

  async function readTaskMovementsCache() {
    const raw = await readAppDataValue(CUSTOM_DASHBOARD_TASK_MOVEMENTS_CACHE_KEY, null);
    if (!raw || typeof raw !== "object") {
      return null;
    }
    if (!Array.isArray(raw.rows) || !Array.isArray(raw.managerSummary)) {
      return null;
    }
    return raw;
  }

  async function refreshTaskMovementsCache(options = {}) {
    const requestedHours = Math.max(1, toSafeInteger(options.hours, 24));
    const periodHours = Math.min(requestedHours, CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS);
    const trigger = sanitizeTextValue(options.trigger, 80) || "manual";
    const payload = await buildGhlTaskMovementsPayload({ hours: periodHours });
    const cachedAt = new Date().toISOString();
    const record = {
      ...payload,
      cachedAt,
      cacheTrigger: trigger,
    };

    if (periodHours === 24) {
      await upsertAppDataValue(CUSTOM_DASHBOARD_TASK_MOVEMENTS_CACHE_KEY, record);
    }
    return record;
  }

  async function loadTaskMovementsPayload(options = {}) {
    const requestedHours = Math.max(1, toSafeInteger(options.hours, 24));
    const periodHours = Math.min(requestedHours, CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS);
    const shouldRefresh = Boolean(options.refresh);
    const trigger = sanitizeTextValue(options.trigger, 80) || "api";

    if (!shouldRefresh && periodHours === 24) {
      const cached = await readTaskMovementsCache();
      if (cached) {
        return {
          ...cached,
          fromCache: true,
          autoSync: buildTaskMovementsAutoSyncInfo(),
        };
      }
    }

    const refreshed = await refreshTaskMovementsCache({
      hours: periodHours,
      trigger,
    });
    return {
      ...refreshed,
      fromCache: false,
      autoSync: buildTaskMovementsAutoSyncInfo(),
    };
  }

  async function runTaskMovementsAutoSync(options = {}) {
    if (taskMovementsAutoSyncRuntimeState.inFlight) {
      return false;
    }

    const trigger = sanitizeTextValue(options.trigger, 80) || "auto";
    const startedAt = new Date().toISOString();
    taskMovementsAutoSyncRuntimeState = {
      ...taskMovementsAutoSyncRuntimeState,
      inFlight: true,
      lastStartedAt: startedAt,
      lastError: "",
    };

    try {
      await refreshTaskMovementsCache({
        hours: 24,
        trigger,
      });
      const finishedAt = new Date().toISOString();
      taskMovementsAutoSyncRuntimeState = {
        ...taskMovementsAutoSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastSuccessAt: finishedAt,
        lastError: "",
      };
      return true;
    } catch (error) {
      const finishedAt = new Date().toISOString();
      taskMovementsAutoSyncRuntimeState = {
        ...taskMovementsAutoSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastError: sanitizeTextValue(error?.message, 600) || "Task movements auto sync failed.",
      };
      console.error("[custom-dashboard] task movements auto sync failed:", error);
      return false;
    }
  }

  function scheduleTaskMovementsAutoSyncNextRun() {
    if (taskMovementsAutoSyncTimerId) {
      clearTimeout(taskMovementsAutoSyncTimerId);
      taskMovementsAutoSyncTimerId = null;
    }

    const nextRunTimestamp = getNextZonedDailyRunTimestamp({
      timeZone: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_TIMEZONE,
      hour: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_HOUR,
      minute: CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_MINUTE,
      nowTimestamp: Date.now(),
    });
    if (!Number.isFinite(nextRunTimestamp)) {
      return false;
    }

    const delayMs = Math.max(1000, Math.round(nextRunTimestamp - Date.now()));
    taskMovementsAutoSyncRuntimeState = {
      ...taskMovementsAutoSyncRuntimeState,
      nextRunAt: new Date(nextRunTimestamp).toISOString(),
    };

    taskMovementsAutoSyncTimerId = setTimeout(() => {
      taskMovementsAutoSyncTimerId = null;
      void runTaskMovementsAutoSync({
        trigger: "scheduled-daily",
      }).finally(() => {
        scheduleTaskMovementsAutoSyncNextRun();
      });
    }, delayMs);

    return true;
  }

  function startTaskMovementsAutoSyncScheduler() {
    if (taskMovementsAutoSyncTimerId) {
      return false;
    }
    if (!pool || !isGhlTasksSyncConfigured() || !CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED) {
      return false;
    }

    const started = scheduleTaskMovementsAutoSyncNextRun();
    if (!started) {
      return false;
    }

    void readTaskMovementsCache()
      .then((cached) => {
        if (cached) {
          return;
        }
        return runTaskMovementsAutoSync({
          trigger: "startup-initial",
        });
      })
      .catch((error) => {
        console.error("[custom-dashboard] task movements cache bootstrap failed:", error);
      });

    return true;
  }

  function normalizeGhlTaskForDashboard(rawTask, context) {
    const syncedAtIso = context.syncedAtIso;
    const todayStart = context.todayStart;

    const taskIdRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_ID_FIELDS), 160);
    const taskTitle = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_TITLE_FIELDS), 500) || "Task";
    const status = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_STATUS_FIELDS), 220);
    const dueRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_DUE_FIELDS), 160);
    const createdRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_CREATED_FIELDS), 160);
    const completedRaw = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_COMPLETED_FIELDS), 160);
    const completedFlag = resolveOptionalBoolean(pickValueFromObject(rawTask, ["completed", "isCompleted", "done"]));

    const dueDateTimestamp = parseDateTimeValue(dueRaw);
    const createdAtTimestamp = parseDateTimeValue(createdRaw) || parseDateTimeValue(syncedAtIso);
    const completedAtTimestamp = parseDateTimeValue(completedRaw);
    const statusComparable = normalizeComparableText(status, 220);
    const isCompleted =
      completedFlag === true ||
      Boolean(completedAtTimestamp) ||
      COMPLETED_TASK_STATUS_MATCHERS.some((token) => statusComparable.includes(token));
    const dueDateDayStart = getUtcDayStart(dueDateTimestamp);
    const isOverdue = !isCompleted && dueDateDayStart !== null && dueDateDayStart < todayStart;
    const isDueToday = !isCompleted && dueDateDayStart !== null && dueDateDayStart === todayStart;
    const managerName = resolveGhlTaskOwnerName(rawTask, context.usersIndex) || "Unassigned";
    const contactTaskContactId = sanitizeTextValue(pickValueFromObject(rawTask, GHL_TASK_CONTACT_ID_FIELDS), 160);
    const sourceTaskId = taskIdRaw || `${context.contact.contactId}:${taskTitle}:${dueRaw}`;
    const id = taskIdRaw ? `ghl-${taskIdRaw}` : `ghl-${context.contact.contactId}-${context.rowIndex + 1}`;

    return {
      id,
      title: taskTitle,
      managerName,
      specialistName: managerName,
      clientName: context.contact.clientName || "",
      status: status || (isCompleted ? "completed" : "open"),
      dueDate: dueDateTimestamp !== null ? new Date(dueDateTimestamp).toISOString() : "",
      dueDateTimestamp,
      createdAt: createdAtTimestamp !== null ? new Date(createdAtTimestamp).toISOString() : syncedAtIso,
      createdAtTimestamp,
      completedAt: completedAtTimestamp !== null ? new Date(completedAtTimestamp).toISOString() : "",
      completedAtTimestamp,
      isCompleted,
      isOverdue,
      isDueToday,
      source: "ghl",
      sourceTaskId,
      sourceContactId: contactTaskContactId || context.contact.contactId,
    };
  }

  function pickCallCacheKey(call) {
    const sourceCallId = sanitizeTextValue(call?.sourceCallId, 220);
    if (sourceCallId) {
      return `source:${sourceCallId}`;
    }

    const id = sanitizeTextValue(call?.id, 260);
    if (id) {
      return `id:${id}`;
    }

    return "";
  }

  function parseDurationSeconds(rawValue) {
    if (rawValue === null || rawValue === undefined || rawValue === "") {
      return 0;
    }

    if (typeof rawValue === "number" && Number.isFinite(rawValue)) {
      return Math.max(0, Math.round(rawValue));
    }

    const value = sanitizeTextValue(rawValue, 80);
    if (!value) {
      return 0;
    }

    const hhmmssMatch = value.match(/^(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/);
    if (hhmmssMatch) {
      const first = toSafeInteger(hhmmssMatch[1], 0);
      const second = toSafeInteger(hhmmssMatch[2], 0);
      const third = toSafeInteger(hhmmssMatch[3], 0);
      if (hhmmssMatch[3] !== undefined) {
        return Math.max(0, first * 3600 + second * 60 + third);
      }
      return Math.max(0, first * 60 + second);
    }

    const asInteger = toSafeInteger(value, 0);
    return Math.max(0, asInteger);
  }

  function resolveGhlCallManagerName(rawMessage, usersIndex) {
    const direct = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_USER_NAME_FIELDS), 220);
    if (direct && !looksLikeOpaqueUserId(direct)) {
      return direct;
    }

    const userId = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_USER_ID_FIELDS), 160);
    if (!userId) {
      return "";
    }

    const resolvedFromIndex = sanitizeTextValue(usersIndex.get(userId), 220);
    if (resolvedFromIndex && !looksLikeOpaqueUserId(resolvedFromIndex)) {
      return resolvedFromIndex;
    }

    return userId;
  }

  function resolveGhlCallClientName(rawMessage) {
    const direct = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_CONTACT_NAME_FIELDS), 260);
    if (direct) {
      return direct;
    }

    const firstName = sanitizeTextValue(
      pickValueFromObject(rawMessage, ["contact.firstName", "contact.firstname", "meta.call.contactFirstName"]),
      120,
    );
    const lastName = sanitizeTextValue(
      pickValueFromObject(rawMessage, ["contact.lastName", "contact.lastname", "meta.call.contactLastName"]),
      120,
    );
    const fullName = buildFullName(firstName, lastName);
    if (fullName) {
      return fullName;
    }

    return sanitizeTextValue(pickValueFromObject(rawMessage, ["contact.email", "meta.call.contactEmail"]), 260);
  }

  function resolveGhlCallPhone(rawMessage, direction) {
    const direct = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_PHONE_FIELDS), 80);
    if (direct) {
      return direct;
    }

    const fromNumber = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_FROM_PHONE_FIELDS), 80);
    const toNumber = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_TO_PHONE_FIELDS), 80);
    if (direction === CUSTOM_DASHBOARD_DIRECTION_INCOMING) {
      return fromNumber || toNumber;
    }
    if (direction === CUSTOM_DASHBOARD_DIRECTION_OUTGOING) {
      return toNumber || fromNumber;
    }

    return fromNumber || toNumber;
  }

  function normalizeGhlCallMessage(rawMessage, context) {
    const callIdRaw = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_ID_FIELDS), 220);
    const status = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_STATUS_FIELDS), 220) || "unknown";
    const directionRaw = sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_DIRECTION_FIELDS), 120);
    const direction = normalizeCallDirection(directionRaw, status);
    const durationSec = parseDurationSeconds(pickValueFromObject(rawMessage, GHL_CALL_DURATION_FIELDS));
    const updatedTimestamp =
      parseDateTimeValue(pickValueFromObject(rawMessage, GHL_CALL_UPDATED_FIELDS)) ||
      parseDateTimeValue(pickValueFromObject(rawMessage, GHL_CALL_CREATED_FIELDS));
    const callAtTimestamp =
      parseDateTimeValue(pickValueFromObject(rawMessage, GHL_CALL_AT_FIELDS)) ||
      updatedTimestamp ||
      parseDateTimeValue(context.syncedAtIso);

    if (!Number.isFinite(callAtTimestamp)) {
      return null;
    }

    const managerName = resolveGhlCallManagerName(rawMessage, context.usersIndex) || "Unassigned";
    const clientName = resolveGhlCallClientName(rawMessage);
    const phone = resolveGhlCallPhone(rawMessage, direction);
    const phoneNormalized = normalizePhone(phone);
    const statusComparable = normalizeComparableText(status, 220);
    const isAnswered = ANSWERED_STATUS_MATCHERS.some((token) => statusComparable.includes(token)) || durationSec > 0;
    const isMissedIncoming =
      direction === CUSTOM_DASHBOARD_DIRECTION_INCOMING &&
      (MISSED_STATUS_MATCHERS.some((token) => statusComparable.includes(token)) || !isAnswered);
    const sourceCallId =
      callIdRaw ||
      sanitizeTextValue(
        `${sanitizeTextValue(pickValueFromObject(rawMessage, GHL_CALL_CONTACT_ID_FIELDS), 160)}:${callAtTimestamp}:${phoneNormalized}:${direction}`,
        220,
      );

    if (!sourceCallId) {
      return null;
    }

    return {
      id: `ghl-call-${sourceCallId}`,
      managerName,
      clientName: clientName || "",
      phone: phone || "",
      phoneNormalized,
      direction,
      status,
      durationSec,
      callAtIso: new Date(callAtTimestamp).toISOString(),
      callAtTimestamp,
      isMissedIncoming,
      isAnswered,
      isOver30Sec: durationSec > 30,
      source: "ghl",
      sourceCallId,
      sourceUpdatedAt: Number.isFinite(updatedTimestamp) ? new Date(updatedTimestamp).toISOString() : "",
    };
  }

  async function runGhlCallsSync(options = {}) {
    if (!isGhlTasksSyncConfigured()) {
      throw createHttpError("GHL integration is not configured. Set GHL_API_KEY and GHL_LOCATION_ID.", 400);
    }

    if (ghlCallsSyncInFlight) {
      throw createHttpError("Calls sync is already running.", 409);
    }

    const requestedModeRaw = sanitizeTextValue(options.mode, 20).toLowerCase();
    const requestedMode = requestedModeRaw === "full" ? "full" : "delta";
    const actorUsername = sanitizeTextValue(options.actorUsername, 220) || "system:ghl-calls-sync";
    const trigger = sanitizeTextValue(options.trigger, 80) || "manual";

    const job = (async () => {
      const startedAt = new Date().toISOString();
      ghlCallsSyncRuntimeState = {
        ...ghlCallsSyncRuntimeState,
        inFlight: true,
        lastStartedAt: startedAt,
        lastTrigger: trigger,
        lastError: "",
      };

      const [previousSyncStateRaw, previousCallsRaw, usersIndex] = await Promise.all([
        readAppDataValue(CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY, null),
        readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null),
        listGhlUsersIndex(),
      ]);

      const previousSyncState = normalizeGhlCallsSyncState(previousSyncStateRaw);
      const previousCallsData = normalizeUploadData(previousCallsRaw, "calls");
      const previousItems = Array.isArray(previousCallsData.items) ? previousCallsData.items : [];
      const previousSource = normalizeComparableText(previousCallsData.source || previousCallsData.fileName, 80);
      const canRunDeltaFromPrevious =
        requestedMode === "delta" &&
        (previousSource.includes("ghl") || previousItems.some((item) => sanitizeTextValue(item?.sourceCallId, 220)));
      const syncMode = canRunDeltaFromPrevious ? "delta" : "full";

      const callsMap = new Map();
      if (syncMode === "delta") {
        for (const previousItem of previousItems) {
          const key = pickCallCacheKey(previousItem);
          if (!key) {
            continue;
          }
          callsMap.set(key, previousItem);
        }
      }

      const previousCursorTimestamp = parseDateTimeValue(previousSyncState.cursorUpdatedAt);
      const defaultStartTimestamp = Date.now() - CUSTOM_DASHBOARD_GHL_CALLS_INITIAL_LOOKBACK_DAYS * DAY_IN_MS;
      const incrementalStartTimestamp = Number.isFinite(previousCursorTimestamp)
        ? Math.max(0, previousCursorTimestamp - CUSTOM_DASHBOARD_GHL_CALLS_CURSOR_SKEW_MS)
        : defaultStartTimestamp;
      const startDateIso = new Date(syncMode === "delta" ? incrementalStartTimestamp : defaultStartTimestamp).toISOString();

      let page = 0;
      let cursor = "";
      let scannedMessages = 0;
      let importedCalls = 0;
      let maxCursorTimestamp = Number.isFinite(previousCursorTimestamp) ? previousCursorTimestamp : null;

      while (page < CUSTOM_DASHBOARD_GHL_CALLS_MAX_PAGES) {
        const query = {
          locationId: CUSTOM_DASHBOARD_GHL_LOCATION_ID,
          channel: CUSTOM_DASHBOARD_GHL_CALLS_CHANNEL,
          limit: CUSTOM_DASHBOARD_GHL_CALLS_PAGE_LIMIT,
          startDate: startDateIso,
        };
        if (cursor) {
          query.cursor = cursor;
        }

        const response = await requestGhlApi("/conversations/messages/export", {
          method: "GET",
          query,
        });
        const messages = extractGhlMessagesFromPayload(response.body);
        if (!messages.length) {
          break;
        }

        page += 1;
        for (let index = 0; index < messages.length; index += 1) {
          const rawMessage = messages[index];
          scannedMessages += 1;
          const normalizedCall = normalizeGhlCallMessage(rawMessage, {
            usersIndex,
            syncedAtIso: startedAt,
          });
          if (!normalizedCall) {
            continue;
          }

          importedCalls += 1;
          const key = pickCallCacheKey(normalizedCall);
          if (!key) {
            continue;
          }
          callsMap.set(key, normalizedCall);

          const updatedTimestamp = parseDateTimeValue(normalizedCall.sourceUpdatedAt) || normalizedCall.callAtTimestamp;
          if (
            Number.isFinite(updatedTimestamp) &&
            (!Number.isFinite(maxCursorTimestamp) || updatedTimestamp > maxCursorTimestamp)
          ) {
            maxCursorTimestamp = updatedTimestamp;
          }
        }

        const nextCursor = extractGhlMessagesNextCursor(response.body);
        if (!nextCursor || nextCursor === cursor) {
          break;
        }
        cursor = nextCursor;
      }

      const retentionCutoffTimestamp = Date.now() - CUSTOM_DASHBOARD_GHL_CALLS_RETENTION_DAYS * DAY_IN_MS;
      const calls = [...callsMap.values()]
        .filter((item) => {
          const callTimestamp = Number.isFinite(item?.callAtTimestamp) ? item.callAtTimestamp : parseDateTimeValue(item?.callAtIso);
          return !Number.isFinite(callTimestamp) || callTimestamp >= retentionCutoffTimestamp;
        })
        .sort((left, right) => {
          const leftTimestamp = Number.isFinite(left?.callAtTimestamp) ? left.callAtTimestamp : parseDateTimeValue(left?.callAtIso) || 0;
          const rightTimestamp = Number.isFinite(right?.callAtTimestamp)
            ? right.callAtTimestamp
            : parseDateTimeValue(right?.callAtIso) || 0;
          return rightTimestamp - leftTimestamp;
        })
        .slice(0, CUSTOM_DASHBOARD_GHL_CALLS_MAX_ITEMS);

      const finishedAt = new Date().toISOString();
      const archiveKey = `custom_dashboard/archive_calls_${buildArchiveTimestamp(finishedAt)}`;
      const payload = {
        type: "calls",
        uploadedAt: finishedAt,
        uploadedBy: actorUsername,
        fileName: "ghl/calls-sync",
        count: calls.length,
        archiveKey,
        source: "ghl",
        items: calls,
      };

      const nextSyncState = {
        version: 1,
        lastAttemptedAt: finishedAt,
        lastSuccessfulSyncAt: finishedAt,
        lastMode: syncMode,
        lastError: "",
        cursorUpdatedAt: Number.isFinite(maxCursorTimestamp) ? new Date(maxCursorTimestamp).toISOString() : previousSyncState.cursorUpdatedAt,
        stats: {
          pagesFetched: page,
          scannedMessages,
          importedCalls,
          storedCalls: calls.length,
        },
      };

      await Promise.all([
        upsertAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, payload),
        upsertAppDataValue(archiveKey, payload),
        upsertAppDataValue(CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY, nextSyncState),
      ]);

      ghlCallsSyncRuntimeState = {
        ...ghlCallsSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastSuccessAt: finishedAt,
        lastError: "",
      };

      return {
        ok: true,
        mode: syncMode,
        requestedMode,
        uploadedAt: finishedAt,
        count: calls.length,
        archiveKey,
        stats: nextSyncState.stats,
        callsSync: buildCallsSyncPayload(nextSyncState),
      };
    })().catch(async (error) => {
      const finishedAt = new Date().toISOString();
      const previousSyncStateRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY, null);
      const previousSyncState = normalizeGhlCallsSyncState(previousSyncStateRaw);
      const nextSyncState = {
        ...previousSyncState,
        lastAttemptedAt: finishedAt,
        lastMode: requestedMode,
        lastError: sanitizeTextValue(error?.message, 600) || "Calls sync failed.",
      };
      await upsertAppDataValue(CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY, nextSyncState);

      ghlCallsSyncRuntimeState = {
        ...ghlCallsSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastError: sanitizeTextValue(error?.message, 600) || "Calls sync failed.",
      };

      throw error;
    });

    ghlCallsSyncInFlight = job;
    try {
      return await job;
    } finally {
      ghlCallsSyncInFlight = null;
    }
  }

  async function runGhlCallsAutoSync(options = {}) {
    if (ghlCallsSyncRuntimeState.inFlight) {
      return false;
    }

    const trigger = sanitizeTextValue(options.trigger, 80) || "auto";
    const mode = sanitizeTextValue(options.mode, 20).toLowerCase() === "full" ? "full" : "delta";
    try {
      await runGhlCallsSync({
        mode,
        trigger,
        actorUsername: "system:ghl-calls-auto-sync",
      });
      return true;
    } catch (error) {
      console.error("[custom-dashboard] GHL calls auto sync failed:", error);
      return false;
    }
  }

  function scheduleGhlCallsAutoSyncNextRun() {
    if (ghlCallsAutoSyncTimerId) {
      clearTimeout(ghlCallsAutoSyncTimerId);
      ghlCallsAutoSyncTimerId = null;
    }

    const nextRunTimestamp = getNextZonedDailyRunTimestamp({
      timeZone: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_TIMEZONE,
      hour: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_HOUR,
      minute: CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_MINUTE,
      nowTimestamp: Date.now(),
    });
    if (!Number.isFinite(nextRunTimestamp)) {
      return false;
    }

    ghlCallsSyncRuntimeState = {
      ...ghlCallsSyncRuntimeState,
      nextRunAt: new Date(nextRunTimestamp).toISOString(),
    };

    const delayMs = Math.max(1000, Math.round(nextRunTimestamp - Date.now()));
    ghlCallsAutoSyncTimerId = setTimeout(() => {
      ghlCallsAutoSyncTimerId = null;
      void runGhlCallsAutoSync({
        trigger: "scheduled-daily",
        mode: "delta",
      }).finally(() => {
        scheduleGhlCallsAutoSyncNextRun();
      });
    }, delayMs);

    return true;
  }

  function startGhlCallsAutoSyncScheduler() {
    if (ghlCallsAutoSyncTimerId) {
      return false;
    }
    if (!pool || !isGhlTasksSyncConfigured() || !CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED) {
      return false;
    }

    const started = scheduleGhlCallsAutoSyncNextRun();
    if (!started) {
      return false;
    }

    void readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null)
      .then((rawValue) => {
        const current = normalizeUploadData(rawValue, "calls");
        const source = normalizeComparableText(current.source || current.fileName, 120);
        if (current.count > 0 && source.includes("ghl")) {
          return;
        }

        return runGhlCallsAutoSync({
          trigger: "startup-initial",
          mode: "delta",
        });
      })
      .catch((error) => {
        console.error("[custom-dashboard] GHL calls startup sync failed:", error);
      });

    return true;
  }

  async function runGhlTasksSync(options = {}) {
    if (!isGhlTasksSyncConfigured()) {
      throw createHttpError("GHL integration is not configured. Set GHL_API_KEY and GHL_LOCATION_ID.", 400);
    }

    const requestedMode = sanitizeTextValue(options.mode, 20).toLowerCase();
    const syncMode = requestedMode === "full" ? "full" : "delta";
    const actorUsername = sanitizeTextValue(options.actorUsername, 220) || "system:ghl-sync";
    const trigger = sanitizeTextValue(options.trigger, 60) || "manual";

    if (ghlTasksSyncInFlight) {
      throw createHttpError("Tasks sync is already running.", 409);
    }

    const job = (async () => {
      const startedAt = new Date().toISOString();
      ghlTasksSyncRuntimeState = {
        ...ghlTasksSyncRuntimeState,
        inFlight: true,
        lastStartedAt: startedAt,
        lastTrigger: trigger,
        lastError: "",
      };

      const previousSyncStateRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null);
      const previousSyncState = normalizeGhlTasksSyncState(previousSyncStateRaw);
      const previousDataRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_LATEST_KEY, null);
      const previousData = normalizeUploadData(previousDataRaw, "tasks");
      const previousItems = Array.isArray(previousData.items) ? previousData.items : [];
      const usersIndex = await listGhlUsersIndex();

      const taskMap = new Map();
      for (const task of previousItems) {
        const key = pickTaskCacheKey(task);
        if (!key) {
          continue;
        }
        taskMap.set(key, task);
      }

      const contactTaskIndex = new Map(Object.entries(previousSyncState.contactTaskIndex || {}));
      const contactsRaw = await listAllGhlContacts();
      const contacts = contactsRaw.map((item) => normalizeGhlContactRecord(item)).filter(Boolean);
      const contactsById = new Map(contacts.map((item) => [item.contactId, item]));
      const knownContactIds = new Set(contacts.map((item) => item.contactId));
      const cursorTimestamp = parseDateTimeValue(previousSyncState.cursorUpdatedAt);
      const cursorWithSkew = Number.isFinite(cursorTimestamp)
        ? Math.max(0, cursorTimestamp - CUSTOM_DASHBOARD_GHL_TASKS_CURSOR_SKEW_MS)
        : null;

      let deletedContacts = 0;
      for (const [contactId, previousEntry] of contactTaskIndex.entries()) {
        if (knownContactIds.has(contactId)) {
          continue;
        }

        const taskIds = Array.isArray(previousEntry?.taskIds) ? previousEntry.taskIds : [];
        for (const taskId of taskIds) {
          taskMap.delete(sanitizeTextValue(taskId, 260));
        }
        contactTaskIndex.delete(contactId);
        deletedContacts += 1;
      }

      const contactsToProcess = [];
      for (const contact of contacts) {
        if (syncMode === "full") {
          contactsToProcess.push(contact);
          continue;
        }

        const existing = contactTaskIndex.get(contact.contactId);
        if (!existing) {
          contactsToProcess.push(contact);
          continue;
        }

        if (!Number.isFinite(cursorWithSkew) || !Number.isFinite(contact.updatedTimestamp)) {
          continue;
        }

        if (contact.updatedTimestamp >= cursorWithSkew) {
          contactsToProcess.push(contact);
        }
      }

      const todayStart = getCurrentUtcDayStart();
      const contactResults = await mapWithConcurrency(
        contactsToProcess,
        CUSTOM_DASHBOARD_GHL_TASKS_SYNC_CONCURRENCY,
        async (contact) => {
          const rawTasks = await listGhlContactTasks(contact.contactId);
          const tasks = [];

          for (let index = 0; index < rawTasks.length; index += 1) {
            const normalized = normalizeGhlTaskForDashboard(rawTasks[index], {
              usersIndex,
              contact,
              rowIndex: index,
              syncedAtIso: startedAt,
              todayStart,
            });

            tasks.push(normalized);
            if (tasks.length >= CUSTOM_DASHBOARD_GHL_TASKS_MAX_ITEMS) {
              break;
            }
          }

          return {
            contactId: contact.contactId,
            updatedAt: contact.updatedAtIso || startedAt,
            tasks,
          };
        },
      );

      let processedContacts = 0;
      let maxCursorTimestamp = Number.isFinite(cursorTimestamp) ? cursorTimestamp : null;
      for (const result of contactResults) {
        const previousEntry = contactTaskIndex.get(result.contactId);
        const previousTaskIds = Array.isArray(previousEntry?.taskIds) ? previousEntry.taskIds : [];
        for (const taskId of previousTaskIds) {
          taskMap.delete(sanitizeTextValue(taskId, 260));
        }

        const nextTaskIds = [];
        for (const task of result.tasks) {
          const key = pickTaskCacheKey(task);
          if (!key) {
            continue;
          }
          taskMap.set(key, task);
          nextTaskIds.push(key);
          if (taskMap.size >= CUSTOM_DASHBOARD_GHL_TASKS_MAX_ITEMS) {
            break;
          }
        }

        contactTaskIndex.set(result.contactId, {
          updatedAt: normalizeIsoDateOrNow(result.updatedAt),
          taskIds: nextTaskIds,
        });

        const updatedTimestamp = parseDateTimeValue(result.updatedAt);
        if (Number.isFinite(updatedTimestamp) && (!Number.isFinite(maxCursorTimestamp) || updatedTimestamp > maxCursorTimestamp)) {
          maxCursorTimestamp = updatedTimestamp;
        }
        processedContacts += 1;
      }

      const tasks = [...taskMap.values()]
        .slice(0, CUSTOM_DASHBOARD_GHL_TASKS_MAX_ITEMS)
        .sort((left, right) => {
          if (left.isCompleted !== right.isCompleted) {
            return left.isCompleted ? 1 : -1;
          }
          const leftDue = Number.isFinite(left.dueDateTimestamp) ? left.dueDateTimestamp : Number.MAX_SAFE_INTEGER;
          const rightDue = Number.isFinite(right.dueDateTimestamp) ? right.dueDateTimestamp : Number.MAX_SAFE_INTEGER;
          if (leftDue !== rightDue) {
            return leftDue - rightDue;
          }
          return sanitizeTextValue(left.title, 220).localeCompare(sanitizeTextValue(right.title, 220), "en-US", {
            sensitivity: "base",
          });
        });

      const finishedAt = new Date().toISOString();
      const archiveKey = `custom_dashboard/archive_tasks_${buildArchiveTimestamp(finishedAt)}`;
      const payload = {
        type: "tasks",
        uploadedAt: finishedAt,
        uploadedBy: actorUsername,
        fileName: "ghl/tasks-sync",
        count: tasks.length,
        archiveKey,
        source: "ghl",
        items: tasks,
      };

      const nextSyncState = {
        version: 1,
        lastAttemptedAt: finishedAt,
        lastSuccessfulSyncAt: finishedAt,
        lastMode: syncMode,
        lastError: "",
        cursorUpdatedAt: Number.isFinite(maxCursorTimestamp) ? new Date(maxCursorTimestamp).toISOString() : previousSyncState.cursorUpdatedAt,
        contactTaskIndex: Object.fromEntries(contactTaskIndex.entries()),
        stats: {
          contactsTotal: contactsById.size,
          contactsProcessed: processedContacts,
          contactsDeleted: deletedContacts,
          tasksTotal: tasks.length,
        },
      };

      await Promise.all([
        upsertAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_LATEST_KEY, payload),
        upsertAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, nextSyncState),
        upsertAppDataValue(archiveKey, payload),
      ]);

      ghlTasksSyncRuntimeState = {
        ...ghlTasksSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastError: "",
      };

      return {
        ok: true,
        mode: syncMode,
        uploadedAt: finishedAt,
        count: tasks.length,
        archiveKey,
        stats: nextSyncState.stats,
      };
    })().catch(async (error) => {
      const finishedAt = new Date().toISOString();
      const previousSyncStateRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null);
      const previousSyncState = normalizeGhlTasksSyncState(previousSyncStateRaw);

      const nextSyncState = {
        ...previousSyncState,
        lastAttemptedAt: finishedAt,
        lastMode: syncMode,
        lastError: sanitizeTextValue(error?.message, 600) || "Tasks sync failed.",
      };
      await upsertAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, nextSyncState);

      ghlTasksSyncRuntimeState = {
        ...ghlTasksSyncRuntimeState,
        inFlight: false,
        lastFinishedAt: finishedAt,
        lastError: sanitizeTextValue(error?.message, 600) || "Tasks sync failed.",
      };

      throw error;
    });

    ghlTasksSyncInFlight = job;
    try {
      return await job;
    } finally {
      ghlTasksSyncInFlight = null;
    }
  }

  function startGhlTasksAutoSync() {
    if (ghlTasksAutoSyncIntervalId) {
      return false;
    }
    if (!CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED || !pool || !isGhlTasksSyncConfigured()) {
      return false;
    }

    ghlTasksAutoSyncIntervalId = setInterval(() => {
      if (ghlTasksSyncInFlight) {
        return;
      }

      void runGhlTasksSync({
        mode: "delta",
        actorUsername: "system:auto-sync",
        trigger: "auto",
      }).catch((error) => {
        console.error("[custom-dashboard] auto sync failed:", error);
      });
    }, CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_INTERVAL_MS);

    return true;
  }

  function resolveModuleRole(userProfile) {
    const canManage = hasWebAuthPermissionSafe(hasWebAuthPermission, userProfile, WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL);
    return canManage ? "admin" : "user";
  }

  function buildDefaultWidgetSettings() {
    return {
      managerTasks: {
        enabled: true,
        visibleNames: [],
      },
      specialistTasks: {
        enabled: true,
        visibleNames: [],
      },
      salesReport: {
        enabled: true,
        visibleNames: [],
      },
      callsByManager: {
        enabled: true,
        visibleNames: [],
      },
    };
  }

  function normalizeWidgetConfig(rawWidgetConfig) {
    const base = buildDefaultWidgetSettings();
    const source = rawWidgetConfig && typeof rawWidgetConfig === "object" ? rawWidgetConfig : {};

    for (const widgetKey of CUSTOM_DASHBOARD_WIDGET_KEYS) {
      const widgetRaw = source[widgetKey] && typeof source[widgetKey] === "object" ? source[widgetKey] : {};
      base[widgetKey] = {
        enabled: resolveOptionalBoolean(widgetRaw.enabled) !== false,
        visibleNames: normalizeVisibleNames(widgetRaw.visibleNames),
      };
    }

    return base;
  }

  function normalizeUsersConfig(rawUsersConfig) {
    const source = rawUsersConfig && typeof rawUsersConfig === "object" ? rawUsersConfig : {};
    const sourceUsers = source.users && typeof source.users === "object" ? source.users : {};
    const users = {};

    for (const [rawUsername, rawUserValue] of Object.entries(sourceUsers)) {
      const username = normalizeUsername(rawUsername);
      if (!username) {
        continue;
      }

      const profile = rawUserValue && typeof rawUserValue === "object" ? rawUserValue : {};
      users[username] = {
        widgets: normalizeWidgetConfig(profile.widgets),
      };
    }

    return {
      version: 1,
      updatedAt: normalizeIsoDateOrNow(source.updatedAt),
      users,
    };
  }

  function buildUserSettingsRecord(userProfile, usersConfig) {
    const username = normalizeUsername(userProfile?.username);
    const displayName = sanitizeTextValue(userProfile?.displayName, 220) || username;
    const moduleRole = resolveModuleRole(userProfile);
    const rawEntry = username ? usersConfig.users?.[username] : null;
    const widgets = normalizeWidgetConfig(rawEntry?.widgets);

    return {
      username,
      displayName,
      isOwner: Boolean(userProfile?.isOwner),
      moduleRole,
      widgets,
    };
  }

  function buildWidgetOptions(tasksData, contactsData, callsData) {
    const taskItems = Array.isArray(tasksData?.items) ? tasksData.items : [];
    const contactItems = Array.isArray(contactsData?.items) ? contactsData.items : [];
    const callItems = Array.isArray(callsData?.items) ? callsData.items : [];

    return {
      managerTasks: uniqueSortedNames(taskItems.map((item) => item.managerName)),
      specialistTasks: uniqueSortedNames(taskItems.map((item) => item.specialistName)),
      salesReport: uniqueSortedNames([...contactItems.map((item) => item.managerName), ...callItems.map((item) => item.managerName)]),
      callsByManager: uniqueSortedNames(callItems.map((item) => item.managerName)),
    };
  }

  function filterByVisibility(items, getter, visibleNames) {
    const sourceItems = Array.isArray(items) ? items : [];
    const normalizedVisible = normalizeVisibleNames(visibleNames);
    if (!normalizedVisible.length) {
      return sourceItems;
    }

    const allowedSet = new Set(normalizedVisible.map((item) => normalizeComparableText(item, 220)));
    return sourceItems.filter((item) => {
      const comparable = normalizeComparableText(getter(item), 220);
      return comparable && allowedSet.has(comparable);
    });
  }

  function buildManagerTasksWidget(tasks, widgetSettings) {
    const normalizedWidgetSettings = normalizeWidgetConfig({ managerTasks: widgetSettings }).managerTasks;
    if (!normalizedWidgetSettings.enabled) {
      return {
        enabled: false,
        visibleNames: normalizedWidgetSettings.visibleNames,
        totals: {
          managers: 0,
          tasks: 0,
          open: 0,
          overdue: 0,
          dueToday: 0,
          completedYesterday: 0,
        },
        rows: [],
      };
    }

    const visibleTasks = filterByVisibility(tasks, (item) => item.managerName, normalizedWidgetSettings.visibleNames);
    const todayStart = getCurrentUtcDayStart();
    const yesterdayStart = todayStart - DAY_IN_MS;
    const grouped = new Map();

    for (const task of visibleTasks) {
      const managerName = sanitizeTextValue(task.managerName, 220) || "Unassigned";
      const key = normalizeComparableText(managerName, 220) || "unassigned";
      const current = grouped.get(key) || {
        managerName,
        open: 0,
        overdue: 0,
        dueToday: 0,
        oldestOverdueDays: 0,
        completedYesterday: 0,
      };

      if (!task.isCompleted) {
        current.open += 1;
      }

      if (task.isOverdue) {
        current.overdue += 1;
        const overdueDays = getDaysBetweenUtc(task.dueDateTimestamp, todayStart);
        if (overdueDays > current.oldestOverdueDays) {
          current.oldestOverdueDays = overdueDays;
        }
      }

      if (task.isDueToday) {
        current.dueToday += 1;
      }

      if (task.completedAtTimestamp !== null) {
        const completedDayStart = getUtcDayStart(task.completedAtTimestamp);
        if (completedDayStart === yesterdayStart) {
          current.completedYesterday += 1;
        }
      }

      grouped.set(key, current);
    }

    const rows = [...grouped.values()].sort((left, right) => {
      if (right.overdue !== left.overdue) {
        return right.overdue - left.overdue;
      }
      if (right.open !== left.open) {
        return right.open - left.open;
      }
      return left.managerName.localeCompare(right.managerName, "en-US", { sensitivity: "base" });
    });

    const totals = rows.reduce(
      (acc, row) => {
        acc.managers += 1;
        acc.open += row.open;
        acc.overdue += row.overdue;
        acc.dueToday += row.dueToday;
        acc.completedYesterday += row.completedYesterday;
        return acc;
      },
      {
        managers: 0,
        tasks: visibleTasks.length,
        open: 0,
        overdue: 0,
        dueToday: 0,
        completedYesterday: 0,
      },
    );

    return {
      enabled: true,
      visibleNames: normalizedWidgetSettings.visibleNames,
      totals,
      rows,
    };
  }

  function buildSpecialistTasksWidget(tasks, widgetSettings, currentUserProfile) {
    const normalizedWidgetSettings = normalizeWidgetConfig({ specialistTasks: widgetSettings }).specialistTasks;
    const emptyResult = {
      enabled: false,
      visibleNames: normalizedWidgetSettings.visibleNames,
      specialistOptions: [],
      selectedSpecialist: "",
      totals: {
        all: 0,
        open: 0,
        overdue: 0,
        dueToday: 0,
      },
      allTasks: [],
      overdueTasks: [],
      dueTodayTasks: [],
    };

    if (!normalizedWidgetSettings.enabled) {
      return emptyResult;
    }

    const visibleTasks = filterByVisibility(tasks, (item) => item.specialistName, normalizedWidgetSettings.visibleNames);
    const specialistOptions = uniqueSortedNames(visibleTasks.map((item) => item.specialistName));

    const preferredNames = [
      sanitizeTextValue(currentUserProfile?.displayName, 220),
      sanitizeTextValue(currentUserProfile?.username, 220),
      sanitizeTextValue(currentUserProfile?.username, 220).split("@")[0] || "",
    ]
      .map((value) => sanitizeTextValue(value, 220))
      .filter(Boolean);

    let selectedSpecialist = "";
    for (const candidate of preferredNames) {
      const match = specialistOptions.find(
        (option) => normalizeComparableText(option, 220) === normalizeComparableText(candidate, 220),
      );
      if (match) {
        selectedSpecialist = match;
        break;
      }
    }

    if (!selectedSpecialist && specialistOptions.length) {
      selectedSpecialist = specialistOptions[0];
    }

    const scopedTasks = selectedSpecialist
      ? visibleTasks.filter(
          (item) => normalizeComparableText(item.specialistName, 220) === normalizeComparableText(selectedSpecialist, 220),
        )
      : visibleTasks;

    const sortedTasks = [...scopedTasks].sort((left, right) => {
      if (left.isCompleted !== right.isCompleted) {
        return left.isCompleted ? 1 : -1;
      }
      if (left.isOverdue !== right.isOverdue) {
        return left.isOverdue ? -1 : 1;
      }
      const leftDue = Number.isFinite(left.dueDateTimestamp) ? left.dueDateTimestamp : Number.MAX_SAFE_INTEGER;
      const rightDue = Number.isFinite(right.dueDateTimestamp) ? right.dueDateTimestamp : Number.MAX_SAFE_INTEGER;
      if (leftDue !== rightDue) {
        return leftDue - rightDue;
      }
      return left.title.localeCompare(right.title, "en-US", { sensitivity: "base" });
    });

    return {
      enabled: true,
      visibleNames: normalizedWidgetSettings.visibleNames,
      specialistOptions,
      selectedSpecialist,
      totals: {
        all: sortedTasks.length,
        open: sortedTasks.filter((item) => !item.isCompleted).length,
        overdue: sortedTasks.filter((item) => item.isOverdue).length,
        dueToday: sortedTasks.filter((item) => item.isDueToday).length,
      },
      allTasks: sortedTasks,
      overdueTasks: sortedTasks.filter((item) => item.isOverdue),
      dueTodayTasks: sortedTasks.filter((item) => item.isDueToday),
    };
  }

  function buildSalesWidget(contacts, calls, widgetSettings) {
    const normalizedWidgetSettings = normalizeWidgetConfig({ salesReport: widgetSettings }).salesReport;
    const emptyResult = {
      enabled: false,
      visibleNames: normalizedWidgetSettings.visibleNames,
      periods: {
        today: buildEmptySalesMetrics(),
        yesterday: buildEmptySalesMetrics(),
        currentWeek: buildEmptySalesMetrics(),
        currentMonth: buildEmptySalesMetrics(),
      },
      managerBreakdown: [],
    };

    if (!normalizedWidgetSettings.enabled) {
      return emptyResult;
    }

    const visibleContacts = filterByVisibility(contacts, (item) => item.managerName, normalizedWidgetSettings.visibleNames);
    const visibleCalls = filterByVisibility(calls, (item) => item.managerName, normalizedWidgetSettings.visibleNames);

    const todayStart = getCurrentUtcDayStart();
    const yesterdayStart = todayStart - DAY_IN_MS;
    const weekStart = getCurrentWeekStartUtc(todayStart);
    const monthStart = getCurrentMonthStartUtc(todayStart);

    const periods = {
      today: summarizeSalesPeriod(visibleContacts, visibleCalls, todayStart, todayStart),
      yesterday: summarizeSalesPeriod(visibleContacts, visibleCalls, yesterdayStart, yesterdayStart),
      currentWeek: summarizeSalesPeriod(visibleContacts, visibleCalls, weekStart, todayStart),
      currentMonth: summarizeSalesPeriod(visibleContacts, visibleCalls, monthStart, todayStart),
    };

    const managerMap = new Map();

    for (const call of visibleCalls) {
      const dayStart = getUtcDayStart(call.callAtTimestamp);
      if (dayStart === null || dayStart < monthStart || dayStart > todayStart) {
        continue;
      }

      const managerName = sanitizeTextValue(call.managerName, 220) || "Unassigned";
      const key = normalizeComparableText(managerName, 220) || "unassigned";
      const current = managerMap.get(key) || {
        managerName,
        calls: 0,
        answers: 0,
        talks: 0,
        interested: 0,
        closedDeals: 0,
        closedAmount: 0,
      };

      current.calls += 1;
      current.answers += call.isAnswered ? 1 : 0;
      current.talks += call.isOver30Sec ? 1 : 0;
      managerMap.set(key, current);
    }

    for (const contact of visibleContacts) {
      const dayStart = getUtcDayStart(contact.eventTimestamp);
      if (dayStart === null || dayStart < monthStart || dayStart > todayStart) {
        continue;
      }

      const managerName = sanitizeTextValue(contact.managerName, 220) || "Unassigned";
      const key = normalizeComparableText(managerName, 220) || "unassigned";
      const current = managerMap.get(key) || {
        managerName,
        calls: 0,
        answers: 0,
        talks: 0,
        interested: 0,
        closedDeals: 0,
        closedAmount: 0,
      };

      current.calls += Math.max(0, contact.callsCount);
      current.answers += Math.max(0, contact.answersCount);
      current.talks += Math.max(0, contact.talksCount);
      current.interested += Math.max(0, contact.interestedCount);
      current.closedDeals += Math.max(0, contact.closedDealsCount);
      current.closedAmount += Number.isFinite(contact.closedAmount) ? contact.closedAmount : 0;
      managerMap.set(key, current);
    }

    const managerBreakdown = [...managerMap.values()].sort((left, right) => {
      if (right.closedAmount !== left.closedAmount) {
        return right.closedAmount - left.closedAmount;
      }
      if (right.closedDeals !== left.closedDeals) {
        return right.closedDeals - left.closedDeals;
      }
      if (right.calls !== left.calls) {
        return right.calls - left.calls;
      }
      return left.managerName.localeCompare(right.managerName, "en-US", { sensitivity: "base" });
    });

    return {
      enabled: true,
      visibleNames: normalizedWidgetSettings.visibleNames,
      periods,
      managerBreakdown,
    };
  }

  function buildCallsWidget(calls, widgetSettings) {
    const normalizedWidgetSettings = normalizeWidgetConfig({ callsByManager: widgetSettings }).callsByManager;
    const emptyResult = {
      enabled: false,
      visibleNames: normalizedWidgetSettings.visibleNames,
      managerOptions: [],
      todaySummary: {
        outgoing: 0,
        incoming: 0,
        missed: 0,
      },
      todayByManager: [],
      stats: [],
      missedCalls: [],
    };

    if (!normalizedWidgetSettings.enabled) {
      return emptyResult;
    }

    const visibleCalls = filterByVisibility(calls, (item) => item.managerName, normalizedWidgetSettings.visibleNames);
    const managerOptions = uniqueSortedNames(visibleCalls.map((item) => item.managerName));

    const statsMap = new Map();
    for (const call of visibleCalls) {
      const managerName = sanitizeTextValue(call.managerName, 220) || "Unassigned";
      const key = normalizeComparableText(managerName, 220) || "unassigned";
      const current = statsMap.get(key) || {
        managerName,
        totalCalls: 0,
        acceptedCalls: 0,
        over30Sec: 0,
      };

      current.totalCalls += 1;
      current.acceptedCalls += call.isAnswered ? 1 : 0;
      current.over30Sec += call.isOver30Sec ? 1 : 0;
      statsMap.set(key, current);
    }

    const stats = [...statsMap.values()].sort((left, right) => {
      if (right.totalCalls !== left.totalCalls) {
        return right.totalCalls - left.totalCalls;
      }
      if (right.acceptedCalls !== left.acceptedCalls) {
        return right.acceptedCalls - left.acceptedCalls;
      }
      return left.managerName.localeCompare(right.managerName, "en-US", { sensitivity: "base" });
    });

    const reportTimeZone = resolveSafeTimeZone(CUSTOM_DASHBOARD_REPORT_TIMEZONE, "America/Chicago");
    const todayStart = getCurrentDayStartInTimeZone(reportTimeZone);
    const todaySummary = {
      outgoing: 0,
      incoming: 0,
      missed: 0,
    };
    const todayMap = new Map();

    for (const call of visibleCalls) {
      const callDayStart = getDayStartInTimeZone(call.callAtTimestamp, reportTimeZone);
      if (callDayStart === null || callDayStart !== todayStart) {
        continue;
      }

      const managerName = sanitizeTextValue(call.managerName, 220) || "Unassigned";
      const key = normalizeComparableText(managerName, 220) || "unassigned";
      const current = todayMap.get(key) || {
        managerName,
        outgoing: 0,
        incoming: 0,
        missed: 0,
      };

      if (call.direction === CUSTOM_DASHBOARD_DIRECTION_OUTGOING) {
        current.outgoing += 1;
        todaySummary.outgoing += 1;
      }

      if (call.direction === CUSTOM_DASHBOARD_DIRECTION_INCOMING) {
        current.incoming += 1;
        todaySummary.incoming += 1;
      }

      if (call.isMissedIncoming) {
        current.missed += 1;
        todaySummary.missed += 1;
      }

      todayMap.set(key, current);
    }

    const todayByManager = [...todayMap.values()].sort((left, right) => {
      if (right.outgoing !== left.outgoing) {
        return right.outgoing - left.outgoing;
      }
      if (right.incoming !== left.incoming) {
        return right.incoming - left.incoming;
      }
      if (right.missed !== left.missed) {
        return right.missed - left.missed;
      }
      return left.managerName.localeCompare(right.managerName, "en-US", { sensitivity: "base" });
    });

    const phoneCallsByManager = new Map();
    for (const call of visibleCalls) {
      if (!call.phoneNormalized || call.callAtTimestamp === null) {
        continue;
      }

      const managerKey = normalizeComparableText(call.managerName, 220) || "unassigned";
      const key = `${managerKey}::${call.phoneNormalized}`;
      if (!phoneCallsByManager.has(key)) {
        phoneCallsByManager.set(key, []);
      }
      phoneCallsByManager.get(key).push(call);
    }

    for (const value of phoneCallsByManager.values()) {
      value.sort((left, right) => (left.callAtTimestamp || 0) - (right.callAtTimestamp || 0));
    }

    const missedCalls = visibleCalls
      .filter((call) => call.isMissedIncoming)
      .map((call) => {
        const managerKey = normalizeComparableText(call.managerName, 220) || "unassigned";
        const listKey = `${managerKey}::${call.phoneNormalized}`;
        const samePhoneCalls = phoneCallsByManager.get(listKey) || [];
        const calledBack = samePhoneCalls.some((candidate) => {
          if (candidate.direction !== CUSTOM_DASHBOARD_DIRECTION_OUTGOING) {
            return false;
          }
          if (candidate.callAtTimestamp === null || call.callAtTimestamp === null) {
            return false;
          }
          return candidate.callAtTimestamp > call.callAtTimestamp;
        });

        return {
          id: call.id,
          managerName: call.managerName || "Unassigned",
          clientName: call.clientName || "-",
          phone: call.phone || "-",
          callAt: call.callAtIso || "",
          status: call.status || "missed",
          calledBack,
        };
      })
      .sort((left, right) => {
        const leftTs = parseDateTimeValue(left.callAt);
        const rightTs = parseDateTimeValue(right.callAt);
        return (rightTs || 0) - (leftTs || 0);
      });

    return {
      enabled: true,
      visibleNames: normalizedWidgetSettings.visibleNames,
      managerOptions,
      todaySummary,
      todayByManager,
      stats,
      missedCalls,
    };
  }

  async function buildDashboardPayload(req) {
    const usersConfigRaw = await readAppDataValue(CUSTOM_DASHBOARD_USERS_KEY, null);
    const usersConfig = normalizeUsersConfig(usersConfigRaw);

    const [tasksSourceSelected, tasksUploadDataRaw, tasksGhlDataRaw, tasksSyncStateRaw, contactsDataRaw, callsDataRaw, callsSyncStateRaw] = await Promise.all([
      readTasksSourceSetting(),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.tasks, null),
      readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_LATEST_KEY, null),
      readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.contacts, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null),
      readAppDataValue(CUSTOM_DASHBOARD_GHL_CALLS_SYNC_STATE_KEY, null),
    ]);

    const tasksUploadData = normalizeUploadData(tasksUploadDataRaw, "tasks");
    const tasksGhlData = normalizeUploadData(tasksGhlDataRaw, "tasks");
    const tasksSyncState = normalizeGhlTasksSyncState(tasksSyncStateRaw);
    const tasksData = tasksSourceSelected === CUSTOM_DASHBOARD_TASKS_SOURCE_GHL ? tasksGhlData : tasksUploadData;
    const contactsData = normalizeUploadData(contactsDataRaw, "contacts");
    const callsData = normalizeUploadData(callsDataRaw, "calls");
    const callsSyncState = normalizeGhlCallsSyncState(callsSyncStateRaw);

    const options = buildWidgetOptions(tasksData, contactsData, callsData);
    const activeUserProfile = req.webAuthProfile && typeof req.webAuthProfile === "object" ? req.webAuthProfile : null;
    const activeUserSettings = buildUserSettingsRecord(activeUserProfile, usersConfig);

    const managerTasks = buildManagerTasksWidget(tasksData.items, activeUserSettings.widgets.managerTasks);
    const specialistTasks = buildSpecialistTasksWidget(tasksData.items, activeUserSettings.widgets.specialistTasks, activeUserProfile);
    const salesReport = buildSalesWidget(contactsData.items, callsData.items, activeUserSettings.widgets.salesReport);
    const callsByManager = buildCallsWidget(callsData.items, activeUserSettings.widgets.callsByManager);

    return {
      ok: true,
      moduleRole: activeUserSettings.moduleRole,
      canManage: hasWebAuthPermissionSafe(hasWebAuthPermission, req.webAuthProfile, WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
      activeUser: {
        username: activeUserSettings.username,
        displayName: activeUserSettings.displayName,
        isOwner: activeUserSettings.isOwner,
      },
      widgets: activeUserSettings.widgets,
      uploads: {
        tasks: buildUploadMeta(tasksUploadData),
        tasksGhl: buildUploadMeta(tasksGhlData),
        contacts: buildUploadMeta(contactsData),
        calls: buildUploadMeta(callsData),
      },
      tasksSource: buildTasksSourcePayload(tasksSourceSelected, tasksSyncState),
      callsSync: buildCallsSyncPayload(callsSyncState),
      options,
      managerTasks,
      specialistTasks,
      salesReport,
      callsByManager,
    };
  }

  async function buildUsersSettingsPayload() {
    const usersConfigRaw = await readAppDataValue(CUSTOM_DASHBOARD_USERS_KEY, null);
    const usersConfig = normalizeUsersConfig(usersConfigRaw);

    const [tasksSourceSelected, tasksUploadDataRaw, tasksGhlDataRaw, tasksSyncStateRaw, contactsDataRaw, callsDataRaw] = await Promise.all([
      readTasksSourceSetting(),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.tasks, null),
      readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_LATEST_KEY, null),
      readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.contacts, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null),
    ]);

    const tasksUploadData = normalizeUploadData(tasksUploadDataRaw, "tasks");
    const tasksGhlData = normalizeUploadData(tasksGhlDataRaw, "tasks");
    const tasksSyncState = normalizeGhlTasksSyncState(tasksSyncStateRaw);
    const tasksData = tasksSourceSelected === CUSTOM_DASHBOARD_TASKS_SOURCE_GHL ? tasksGhlData : tasksUploadData;
    const contactsData = normalizeUploadData(contactsDataRaw, "contacts");
    const callsData = normalizeUploadData(callsDataRaw, "calls");

    const options = buildWidgetOptions(tasksData, contactsData, callsData);
    const users = (typeof listWebAuthUsers === "function" ? listWebAuthUsers() : [])
      .map((userProfile) => {
        const user = buildUserSettingsRecord(userProfile, usersConfig);
        return {
          username: user.username,
          displayName: user.displayName,
          isOwner: user.isOwner,
          moduleRole: user.moduleRole,
          widgets: user.widgets,
        };
      })
      .filter((item) => item.username)
      .sort((left, right) => {
        if (left.isOwner !== right.isOwner) {
          return left.isOwner ? -1 : 1;
        }
        return left.displayName.localeCompare(right.displayName, "en-US", { sensitivity: "base" });
      });

    return {
      ok: true,
      users,
      options,
      updatedAt: usersConfig.updatedAt,
      tasksSource: buildTasksSourcePayload(tasksSourceSelected, tasksSyncState),
    };
  }

  async function saveUsersSettingsFromPayload(payload, actorUsername) {
    const source = payload && typeof payload === "object" ? payload : {};
    const rawUsers = Array.isArray(source.users) ? source.users : [];

    const usersConfigRaw = await readAppDataValue(CUSTOM_DASHBOARD_USERS_KEY, null);
    const previous = normalizeUsersConfig(usersConfigRaw);
    const nextUsers = { ...previous.users };

    for (const rawEntry of rawUsers) {
      const entry = rawEntry && typeof rawEntry === "object" ? rawEntry : {};
      const username = normalizeUsername(entry.username);
      if (!username) {
        continue;
      }

      nextUsers[username] = {
        widgets: normalizeWidgetConfig(entry.widgets),
      };
    }

    const nextPayload = {
      version: 1,
      updatedAt: new Date().toISOString(),
      updatedBy: sanitizeTextValue(actorUsername, 220),
      users: nextUsers,
    };

    await upsertAppDataValue(CUSTOM_DASHBOARD_USERS_KEY, nextPayload);
    return normalizeUsersConfig(nextPayload);
  }

  async function parseUploadRows(file) {
    const fileName = sanitizeTextValue(file?.originalname, 260);
    const extension = path.extname(fileName).toLowerCase();
    const mimeType = sanitizeTextValue(file?.mimetype, 140).toLowerCase();
    const content = Buffer.isBuffer(file?.buffer) ? file.buffer : null;

    if (!content || !content.length) {
      throw createHttpError("Uploaded file is empty.", 400);
    }

    if (extension === ".xlsx" || extension === ".xls" || mimeType.includes("spreadsheetml") || mimeType.includes("ms-excel")) {
      throw createHttpError(
        "Excel uploads are temporarily disabled for security. Upload CSV/TSV/TXT export instead.",
        400,
        "spreadsheet_upload_disabled",
      );
    }

    const isTextUpload =
      CUSTOM_DASHBOARD_ALLOWED_TEXT_UPLOAD_EXTENSIONS.has(extension) ||
      mimeType.includes("text/csv") ||
      mimeType.includes("text/tab-separated-values") ||
      mimeType.includes("text/plain");

    if (!isTextUpload) {
      throw createHttpError(
        "Unsupported file format. Upload CSV/TSV/TXT file.",
        400,
        "unsupported_upload_format",
      );
    }

    const text = decodeTextBuffer(content);
    if (!text.trim()) {
      throw createHttpError("Uploaded text file is empty.", 400);
    }

    return parseCsvTextToRows(text);
  }

  function normalizeRawRows(rows) {
    const source = Array.isArray(rows) ? rows : [];
    const normalized = [];

    for (const rawRow of source) {
      if (!rawRow || typeof rawRow !== "object" || Array.isArray(rawRow)) {
        continue;
      }

      const next = {};
      let hasAnyValue = false;
      for (const [key, rawValue] of Object.entries(rawRow)) {
        const safeKey = sanitizeTextValue(key, 220);
        if (!safeKey) {
          continue;
        }

        const value = sanitizeTextValue(rawValue, 6000);
        if (value) {
          hasAnyValue = true;
        }
        next[safeKey] = value;
      }

      if (hasAnyValue) {
        normalized.push(next);
      }

      if (normalized.length >= CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD) {
        break;
      }
    }

    return normalized;
  }

function parseCsvTextToRows(csvText) {
  const text = String(csvText || "").replace(/^\uFEFF/, "");
  const delimiter = detectCsvDelimiter(text);
  const rawRows = [];
  let currentRow = [];
  let currentField = "";
  let index = 0;
  let inQuotes = false;

    while (index < text.length) {
      const char = text[index];

      if (char === '"') {
        const nextChar = text[index + 1];
        if (inQuotes && nextChar === '"') {
          currentField += '"';
          index += 2;
          continue;
        }

        inQuotes = !inQuotes;
        index += 1;
        continue;
      }

      if (!inQuotes && char === delimiter) {
        currentRow.push(currentField);
        currentField = "";
        index += 1;
        continue;
      }

      if (!inQuotes && (char === "\n" || char === "\r")) {
        if (char === "\r" && text[index + 1] === "\n") {
          index += 1;
        }

        currentRow.push(currentField);
        rawRows.push(currentRow);
        currentRow = [];
        currentField = "";
        index += 1;
        continue;
      }

      currentField += char;
      index += 1;
    }

    if (currentField.length > 0 || currentRow.length > 0) {
      currentRow.push(currentField);
      rawRows.push(currentRow);
    }

    const cleanedRows = rawRows
      .map((row) => row.map((cell) => sanitizeTextValue(cell, 6000)))
      .filter((row) => row.some((cell) => Boolean(cell)));

    if (!cleanedRows.length) {
      return [];
    }

    const headers = cleanedRows[0].map((header, headerIndex) => sanitizeTextValue(header, 220) || `Column ${headerIndex + 1}`);
    const records = [];

    for (let rowIndex = 1; rowIndex < cleanedRows.length; rowIndex += 1) {
      const row = cleanedRows[rowIndex];
      if (!row.some((cell) => Boolean(cell))) {
        continue;
      }

      const next = {};
      let hasAnyValue = false;
      for (let columnIndex = 0; columnIndex < headers.length; columnIndex += 1) {
        const header = headers[columnIndex];
        const value = sanitizeTextValue(row[columnIndex], 6000);
        next[header] = value;
        if (value) {
          hasAnyValue = true;
        }
      }

      if (!hasAnyValue) {
        continue;
      }

      records.push(next);
      if (records.length >= CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD) {
        break;
      }
    }

  return records;
}

function detectCsvDelimiter(csvText) {
  const text = String(csvText || "");
  const lines = text.split(/\r?\n/);
  const probeLine = lines.find((line) => sanitizeTextValue(line, 2000)) || "";
  if (!probeLine) {
    return ",";
  }

  const candidates = [",", ";", "\t"];
  let bestDelimiter = ",";
  let bestScore = -1;

  for (const candidate of candidates) {
    let score = 0;
    let inQuotes = false;

    for (let index = 0; index < probeLine.length; index += 1) {
      const char = probeLine[index];
      if (char === "\"") {
        const nextChar = probeLine[index + 1];
        if (inQuotes && nextChar === "\"") {
          index += 1;
          continue;
        }
        inQuotes = !inQuotes;
        continue;
      }

      if (!inQuotes && char === candidate) {
        score += 1;
      }
    }

    if (score > bestScore) {
      bestScore = score;
      bestDelimiter = candidate;
    }
  }

  return bestDelimiter;
}

  function buildRowLookup(row) {
    const entries = [];

    for (const [rawKey, rawValue] of Object.entries(row || {})) {
      const key = sanitizeTextValue(rawKey, 220);
      if (!key) {
        continue;
      }

      entries.push({
        key,
        normalizedKey: normalizeColumnKey(key),
        value: sanitizeTextValue(rawValue, 6000),
      });
    }

    return entries;
  }

  function resolveRowValue(rowLookup, aliases) {
    const normalizedAliases = [...new Set((Array.isArray(aliases) ? aliases : []).map((alias) => normalizeColumnKey(alias)).filter(Boolean))];
    if (!normalizedAliases.length) {
      return "";
    }

    for (const entry of rowLookup) {
      if (!entry.normalizedKey) {
        continue;
      }

      if (normalizedAliases.includes(entry.normalizedKey)) {
        return entry.value;
      }
    }

    for (const alias of normalizedAliases) {
      for (const entry of rowLookup) {
        if (!entry.normalizedKey) {
          continue;
        }

        if (entry.normalizedKey.includes(alias) || alias.includes(entry.normalizedKey)) {
          return entry.value;
        }
      }
    }

    return "";
  }

  function normalizeTaskItems(rows, uploadedAtIso) {
    const todayStart = getCurrentUtcDayStart();
    const items = [];

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const lookup = buildRowLookup(row);
      const managerName = sanitizeTextValue(resolveRowValue(lookup, TASK_MANAGER_ALIASES), 220);
      const specialistName = sanitizeTextValue(resolveRowValue(lookup, TASK_SPECIALIST_ALIASES), 220);
      const title = sanitizeTextValue(resolveRowValue(lookup, TASK_TITLE_ALIASES), 500);
      const clientName = sanitizeTextValue(resolveRowValue(lookup, TASK_CLIENT_ALIASES), 260);
      const status = sanitizeTextValue(resolveRowValue(lookup, TASK_STATUS_ALIASES), 220);
      const dueDateRaw = sanitizeTextValue(resolveRowValue(lookup, TASK_DUE_DATE_ALIASES), 140);
      const createdAtRaw = sanitizeTextValue(resolveRowValue(lookup, TASK_CREATED_AT_ALIASES), 140);
      const completedAtRaw = sanitizeTextValue(resolveRowValue(lookup, TASK_COMPLETED_AT_ALIASES), 140);

      if (!managerName && !specialistName && !title && !clientName) {
        continue;
      }

      const dueDateTimestamp = parseDateTimeValue(dueDateRaw);
      const createdAtTimestamp = parseDateTimeValue(createdAtRaw) || parseDateTimeValue(uploadedAtIso);
      const completedAtTimestamp = parseDateTimeValue(completedAtRaw);
      const statusComparable = normalizeComparableText(status, 220);
      const isCompleted = Boolean(completedAtTimestamp) || COMPLETED_TASK_STATUS_MATCHERS.some((token) => statusComparable.includes(token));

      const dueDateDayStart = getUtcDayStart(dueDateTimestamp);
      const isOverdue = !isCompleted && dueDateDayStart !== null && dueDateDayStart < todayStart;
      const isDueToday = !isCompleted && dueDateDayStart !== null && dueDateDayStart === todayStart;

      items.push({
        id: `task-${index + 1}`,
        title: title || "Task",
        managerName: managerName || "Unassigned",
        specialistName: specialistName || managerName || "Unassigned",
        clientName: clientName || "",
        status: status || (isCompleted ? "completed" : "open"),
        dueDate: dueDateTimestamp !== null ? new Date(dueDateTimestamp).toISOString() : "",
        dueDateTimestamp,
        createdAt: createdAtTimestamp !== null ? new Date(createdAtTimestamp).toISOString() : uploadedAtIso,
        createdAtTimestamp,
        completedAt: completedAtTimestamp !== null ? new Date(completedAtTimestamp).toISOString() : "",
        completedAtTimestamp,
        isCompleted,
        isOverdue,
        isDueToday,
      });

      if (items.length >= CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD) {
        break;
      }
    }

    return items;
  }

  function normalizeContactItems(rows, uploadedAtIso) {
    const items = [];

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const lookup = buildRowLookup(row);
      const managerName = sanitizeTextValue(resolveRowValue(lookup, CONTACT_MANAGER_ALIASES), 220);
      const clientName = sanitizeTextValue(resolveRowValue(lookup, CONTACT_CLIENT_ALIASES), 260);
      const status = sanitizeTextValue(resolveRowValue(lookup, CONTACT_STATUS_ALIASES), 260);
      const eventDateRaw = sanitizeTextValue(resolveRowValue(lookup, CONTACT_DATE_ALIASES), 160);

      if (!managerName && !clientName && !status) {
        continue;
      }

      const statusComparable = normalizeComparableText(status, 260);
      const callsCount = toSafeInteger(resolveRowValue(lookup, CONTACT_CALLS_ALIASES), 0);
      const answersCountRaw = toSafeInteger(resolveRowValue(lookup, CONTACT_ANSWERS_ALIASES), -1);
      const talksCountRaw = toSafeInteger(resolveRowValue(lookup, CONTACT_TALKS_ALIASES), -1);
      const interestedCountRaw = toSafeInteger(resolveRowValue(lookup, CONTACT_INTERESTED_ALIASES), -1);
      const closedCountRaw = toSafeInteger(resolveRowValue(lookup, CONTACT_CLOSED_ALIASES), -1);
      const amountRaw = parseCurrencyValue(resolveRowValue(lookup, CONTACT_AMOUNT_ALIASES));

      const answersCount = answersCountRaw >= 0 ? answersCountRaw : guessBooleanCountFromStatus(statusComparable, ANSWERED_STATUS_MATCHERS);
      const talksCount = talksCountRaw >= 0 ? talksCountRaw : 0;
      const interestedCount = interestedCountRaw >= 0 ? interestedCountRaw : guessBooleanCountFromStatus(statusComparable, INTERESTED_STATUS_MATCHERS);
      const closedDealsCount = closedCountRaw >= 0 ? closedCountRaw : guessBooleanCountFromStatus(statusComparable, CLOSED_STATUS_MATCHERS);
      const closedAmount = closedDealsCount > 0 && Number.isFinite(amountRaw) ? amountRaw : 0;

      const eventTimestamp = parseDateTimeValue(eventDateRaw) || parseDateTimeValue(uploadedAtIso);

      items.push({
        id: `contact-${index + 1}`,
        managerName: managerName || "Unassigned",
        clientName: clientName || "",
        status,
        eventAt: eventTimestamp !== null ? new Date(eventTimestamp).toISOString() : uploadedAtIso,
        eventTimestamp,
        callsCount: Math.max(0, callsCount),
        answersCount: Math.max(0, answersCount),
        talksCount: Math.max(0, talksCount),
        interestedCount: Math.max(0, interestedCount),
        closedDealsCount: Math.max(0, closedDealsCount),
        closedAmount: Number.isFinite(closedAmount) ? closedAmount : 0,
      });

      if (items.length >= CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD) {
        break;
      }
    }

    return items;
  }

  function normalizeCallItems(rows, uploadedAtIso) {
    const items = [];

    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const lookup = buildRowLookup(row);

      const managerName = sanitizeTextValue(resolveRowValue(lookup, CALL_MANAGER_ALIASES), 220);
      const clientName = sanitizeTextValue(resolveRowValue(lookup, CALL_CLIENT_ALIASES), 260);
      const phone = sanitizeTextValue(resolveRowValue(lookup, CALL_PHONE_ALIASES), 80);
      const directionRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_DIRECTION_ALIASES), 120);
      const statusRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_STATUS_ALIASES), 220);
      const durationRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_DURATION_ALIASES), 80);
      const dateTimeRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_DATE_TIME_ALIASES), 180);
      const dateRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_DATE_ALIASES), 120);
      const timeRaw = sanitizeTextValue(resolveRowValue(lookup, CALL_TIME_ALIASES), 120);

      if (!managerName && !clientName && !phone && !statusRaw) {
        continue;
      }

      const direction = normalizeCallDirection(directionRaw, statusRaw);
      const status = statusRaw || "unknown";
      const durationSec = Math.max(0, toSafeInteger(durationRaw, 0));
      const combinedDateTime = [dateRaw, timeRaw].filter(Boolean).join(" ").trim();
      const callAtTimestamp =
        parseDateTimeValue(dateTimeRaw) || parseDateTimeValue(combinedDateTime) || parseDateTimeValue(uploadedAtIso);

      const statusComparable = normalizeComparableText(status, 220);
      const isMissedIncoming =
        direction === CUSTOM_DASHBOARD_DIRECTION_INCOMING && MISSED_STATUS_MATCHERS.some((token) => statusComparable.includes(token));
      const isAnswered = ANSWERED_STATUS_MATCHERS.some((token) => statusComparable.includes(token)) || durationSec > 0;
      const isOver30Sec = durationSec > 30;

      items.push({
        id: `call-${index + 1}`,
        managerName: managerName || "Unassigned",
        clientName: clientName || "",
        phone,
        phoneNormalized: normalizePhone(phone),
        direction,
        status,
        durationSec,
        callAtIso: callAtTimestamp !== null ? new Date(callAtTimestamp).toISOString() : uploadedAtIso,
        callAtTimestamp,
        isMissedIncoming,
        isAnswered,
        isOver30Sec,
      });

      if (items.length >= CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD) {
        break;
      }
    }

    return items;
  }

  function normalizeUploadData(rawValue, type) {
    const source = rawValue && typeof rawValue === "object" ? rawValue : {};
    const normalizedType = CUSTOM_DASHBOARD_UPLOAD_TYPES.has(type) ? type : "tasks";
    const itemsRaw = Array.isArray(source.items) ? source.items : [];

    return {
      type: normalizedType,
      uploadedAt: normalizeIsoDateOrEmpty(source.uploadedAt),
      uploadedBy: sanitizeTextValue(source.uploadedBy, 220),
      fileName: sanitizeTextValue(source.fileName, 260),
      count: toSafeInteger(source.count, itemsRaw.length),
      archiveKey: sanitizeTextValue(source.archiveKey, 260),
      source: sanitizeTextValue(source.source, 80),
      items: itemsRaw,
    };
  }

  function buildUploadMeta(uploadData) {
    return {
      type: uploadData.type,
      uploadedAt: uploadData.uploadedAt || "",
      uploadedBy: uploadData.uploadedBy || "",
      fileName: uploadData.fileName || "",
      count: Math.max(0, toSafeInteger(uploadData.count, 0)),
      archiveKey: uploadData.archiveKey || "",
    };
  }

  app.get(
    "/api/custom-dashboard",
    requireWebPermission(WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const payload = await buildDashboardPayload(req);
        res.json(payload);
      } catch (error) {
        console.error("GET /api/custom-dashboard failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to load custom dashboard.",
        });
      }
    },
  );

  app.get(
    "/api/custom-dashboard/users",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const payload = await buildUsersSettingsPayload();
        res.json(payload);
      } catch (error) {
        console.error("GET /api/custom-dashboard/users failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to load custom dashboard user settings.",
        });
      }
    },
  );

  app.put(
    "/api/custom-dashboard/users",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const updatedConfig = await saveUsersSettingsFromPayload(req.body, req.webAuthUser || req.webAuthProfile?.username || "");
        res.json({
          ok: true,
          updatedAt: updatedConfig.updatedAt,
        });
      } catch (error) {
        console.error("PUT /api/custom-dashboard/users failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to save custom dashboard user settings.",
        });
      }
    },
  );

  app.put(
    "/api/custom-dashboard/tasks-source",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const source = normalizeTasksSource(req.body?.source);
        const saved = await saveTasksSourceSetting(source, req.webAuthUser || req.webAuthProfile?.username || "");
        const syncStateRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null);
        const syncState = normalizeGhlTasksSyncState(syncStateRaw);
        res.json({
          ok: true,
          tasksSource: buildTasksSourcePayload(saved.source, syncState),
        });
      } catch (error) {
        console.error("PUT /api/custom-dashboard/tasks-source failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to update tasks source.",
        });
      }
    },
  );

  app.post(
    "/api/custom-dashboard/tasks-sync",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const modeRaw = sanitizeTextValue(req.body?.mode, 20).toLowerCase();
        const mode = modeRaw === "full" ? "full" : "delta";
        const result = await runGhlTasksSync({
          mode,
          actorUsername: req.webAuthUser || req.webAuthProfile?.username || "",
          trigger: "manual",
        });
        const selectedSource = await readTasksSourceSetting();
        const syncStateRaw = await readAppDataValue(CUSTOM_DASHBOARD_GHL_TASKS_SYNC_STATE_KEY, null);
        const syncState = normalizeGhlTasksSyncState(syncStateRaw);

        res.status(201).json({
          ...result,
          tasksSource: buildTasksSourcePayload(selectedSource, syncState),
        });
      } catch (error) {
        console.error("POST /api/custom-dashboard/tasks-sync failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to sync tasks from GoHighLevel.",
        });
      }
    },
  );

  app.post(
    "/api/custom-dashboard/calls-sync",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const modeRaw = sanitizeTextValue(req.body?.mode, 20).toLowerCase();
        const mode = modeRaw === "full" ? "full" : "delta";
        const result = await runGhlCallsSync({
          mode,
          actorUsername: req.webAuthUser || req.webAuthProfile?.username || "",
          trigger: "manual",
        });
        res.status(201).json(result);
      } catch (error) {
        console.error("POST /api/custom-dashboard/calls-sync failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to sync calls from GoHighLevel.",
        });
      }
    },
  );

  app.get(
    "/api/custom-dashboard/tasks-movements",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        const requestedHours = toSafeInteger(req.query?.hours, 24);
        const hours = Math.min(Math.max(requestedHours, 1), CUSTOM_DASHBOARD_GHL_TASK_MOVEMENTS_MAX_HOURS);
        const refresh = resolveOptionalBoolean(req.query?.refresh) === true;
        const payload = await loadTaskMovementsPayload({
          hours,
          refresh,
          trigger: refresh ? "api-refresh" : "api-read",
        });
        res.json(payload);
      } catch (error) {
        console.error("GET /api/custom-dashboard/tasks-movements failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to load task movements from GoHighLevel.",
        });
      }
    },
  );

  app.post(
    "/api/custom-dashboard/upload",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      try {
        await parseMultipartUpload(req, res, uploadMiddleware);
      } catch (error) {
        res.status(error.httpStatus || 400).json({
          error: sanitizeTextValue(error?.message, 500) || "Failed to parse upload payload.",
        });
        return;
      }

      const type = sanitizeTextValue(req.body?.type, 80).toLowerCase();
      if (!CUSTOM_DASHBOARD_UPLOAD_TYPES.has(type)) {
        res.status(400).json({
          error: "Upload type must be one of: tasks, contacts, calls.",
        });
        return;
      }

      const file = req.file;
      if (!file || !Buffer.isBuffer(file.buffer) || !file.buffer.length) {
        res.status(400).json({
          error: "Please attach a file in `file` field.",
        });
        return;
      }

      try {
        const uploadedAt = new Date().toISOString();
        const rows = await parseUploadRows(file);
        if (!rows.length) {
          res.status(400).json({
            error: "Uploaded file does not contain rows.",
          });
          return;
        }

        let items = [];
        if (type === "tasks") {
          items = normalizeTaskItems(rows, uploadedAt);
        } else if (type === "contacts") {
          items = normalizeContactItems(rows, uploadedAt);
        } else {
          items = normalizeCallItems(rows, uploadedAt);
        }

        const archiveKey = `custom_dashboard/archive_${type}_${buildArchiveTimestamp(uploadedAt)}`;
        const payload = {
          type,
          uploadedAt,
          uploadedBy: sanitizeTextValue(req.webAuthUser || req.webAuthProfile?.username, 220),
          fileName: sanitizeTextValue(file.originalname, 260),
          count: items.length,
          archiveKey,
          items,
        };

        await Promise.all([
          upsertAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS[type], payload),
          upsertAppDataValue(archiveKey, payload),
        ]);

        res.status(201).json({
          ok: true,
          type,
          count: items.length,
          archiveKey,
          uploadedAt,
        });
      } catch (error) {
        console.error("POST /api/custom-dashboard/upload failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to upload custom dashboard data.",
        });
      }
    },
  );

  app.get(
    "/api/custom-dashboard/archive/:type/:archiveKey",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (req, res) => {
      if (!pool) {
        res.status(503).json({
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        });
        return;
      }

      const type = sanitizeTextValue(req.params.type, 80).toLowerCase();
      const archiveKey = sanitizeTextValue(req.params.archiveKey, 300);
      if (!CUSTOM_DASHBOARD_UPLOAD_TYPES.has(type) || !archiveKey) {
        res.status(400).json({
          error: "Invalid archive route parameters.",
        });
        return;
      }

      const fullKey = archiveKey.startsWith("custom_dashboard/") ? archiveKey : `custom_dashboard/${archiveKey}`;
      try {
        const raw = await readAppDataValue(fullKey, null);
        if (!raw || typeof raw !== "object") {
          res.status(404).json({
            error: "Archive item not found.",
          });
          return;
        }

        const normalized = normalizeUploadData(raw, type);
        res.json({
          ok: true,
          item: normalized,
        });
      } catch (error) {
        console.error("GET /api/custom-dashboard/archive/:type/:archiveKey failed:", error);
        res.status(resolveDbErrorStatus(error)).json({
          error: sanitizeTextValue(error?.message, 600) || "Failed to load archive item.",
        });
      }
    },
  );

  app.get(
    "/api/custom-dashboard/_health",
    requireWebPermission(WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    async (_req, res) => {
      if (!pool) {
        res.status(503).json({
          ok: false,
          error: "Database is not configured.",
        });
        return;
      }

      try {
        await ensureAppDataTableReady();
        res.json({
          ok: true,
          table: `${dbSchema}.${tableName}`,
          ghlTasksConfigured: isGhlTasksSyncConfigured(),
          ghlTasksAutoSyncEnabled: CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED,
          taskMovementsAutoSync: buildTaskMovementsAutoSyncInfo(),
          callsSync: buildCallsSyncPayload(null),
        });
      } catch (error) {
        res.status(resolveDbErrorStatus(error)).json({
          ok: false,
          error: sanitizeTextValue(error?.message, 400) || "Health check failed.",
        });
      }
    },
  );

  console.log(`[custom-dashboard] module routes enabled (table: ${dbSchema}.${tableName})`);
  if (!isGhlTasksSyncConfigured()) {
    console.warn("[custom-dashboard] GHL tasks sync is disabled. Set GHL_API_KEY and GHL_LOCATION_ID.");
  }
  if (!CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED) {
    console.warn("[custom-dashboard] GHL tasks auto sync is disabled (CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_ENABLED=false).");
  } else if (!pool) {
    console.warn("[custom-dashboard] GHL tasks auto sync is disabled because DATABASE_URL is missing.");
  } else if (!isGhlTasksSyncConfigured()) {
    console.warn("[custom-dashboard] GHL tasks auto sync is disabled because GHL credentials are missing.");
  } else if (startGhlTasksAutoSync()) {
    console.log(
      `[custom-dashboard] GHL tasks auto sync started: every ${Math.round(CUSTOM_DASHBOARD_GHL_TASKS_AUTO_SYNC_INTERVAL_MS / (60 * 1000))} min.`,
    );
  }

  if (!CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED) {
    console.warn(
      "[custom-dashboard] Task movements auto sync is disabled (CUSTOM_DASHBOARD_TASK_MOVEMENTS_AUTO_SYNC_ENABLED=false).",
    );
  } else if (!pool) {
    console.warn("[custom-dashboard] Task movements auto sync is disabled because DATABASE_URL is missing.");
  } else if (!isGhlTasksSyncConfigured()) {
    console.warn("[custom-dashboard] Task movements auto sync is disabled because GHL credentials are missing.");
  } else if (startTaskMovementsAutoSyncScheduler()) {
    const info = buildTaskMovementsAutoSyncInfo();
    console.log(
      `[custom-dashboard] Task movements auto sync scheduled: ${String(info.hour).padStart(2, "0")}:${String(info.minute).padStart(2, "0")} ${info.timeZone}. Next run: ${info.nextRunAt || "-"}.`,
    );
  }

  if (!CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED) {
    console.warn("[custom-dashboard] GHL calls auto sync is disabled (CUSTOM_DASHBOARD_GHL_CALLS_AUTO_SYNC_ENABLED=false).");
  } else if (!pool) {
    console.warn("[custom-dashboard] GHL calls auto sync is disabled because DATABASE_URL is missing.");
  } else if (!isGhlTasksSyncConfigured()) {
    console.warn("[custom-dashboard] GHL calls auto sync is disabled because GHL credentials are missing.");
  } else if (startGhlCallsAutoSyncScheduler()) {
    const info = buildGhlCallsAutoSyncInfo(null);
    console.log(
      `[custom-dashboard] GHL calls auto sync scheduled: ${String(info.hour).padStart(2, "0")}:${String(info.minute).padStart(2, "0")} ${info.timeZone}. Next run: ${info.nextRunAt || "-"}.`,
    );
  }
}

function hasWebAuthPermissionSafe(checkFn, userProfile, permissionKey) {
  if (typeof checkFn === "function") {
    try {
      return Boolean(checkFn(userProfile, permissionKey));
    } catch {
      return false;
    }
  }

  return Boolean(userProfile?.permissions?.[permissionKey]);
}

function createHttpError(message, status = 400) {
  const error = new Error(message);
  error.httpStatus = status;
  return error;
}

function resolveDbErrorStatus(error) {
  const explicitStatus = Number.parseInt(error?.httpStatus, 10);
  if (Number.isFinite(explicitStatus) && explicitStatus >= 400 && explicitStatus <= 599) {
    return explicitStatus;
  }

  const code = sanitizeTextValue(error?.code, 80).toLowerCase();
  if (code === "28p01") {
    return 503;
  }

  return 500;
}

function parseMultipartUpload(req, res, middleware) {
  return new Promise((resolve, reject) => {
    middleware(req, res, (error) => {
      if (!error) {
        resolve();
        return;
      }

      if (error instanceof multer.MulterError) {
        if (error.code === "LIMIT_FILE_SIZE") {
          reject(createHttpError(`File size exceeds ${Math.floor(CUSTOM_DASHBOARD_UPLOAD_MAX_FILE_SIZE_BYTES / (1024 * 1024))} MB.`, 400));
          return;
        }

        if (error.code === "LIMIT_FILE_COUNT" || error.code === "LIMIT_UNEXPECTED_FILE") {
          reject(createHttpError("Attach exactly one file per upload request.", 400));
          return;
        }
      }

      reject(error);
    });
  });
}

function resolveSafeSqlIdentifier(rawValue, fallbackValue) {
  const normalized = sanitizeTextValue(rawValue || fallbackValue, 120);
  if (!normalized) {
    return fallbackValue;
  }

  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(normalized)) {
    throw new Error(`Unsafe SQL identifier: "${normalized}"`);
  }

  return normalized;
}

function sanitizeTextValue(value, maxLength = 4000) {
  if (value === null || value === undefined) {
    return "";
  }

  const normalized = String(value).normalize("NFKC").replace(/\u0000/g, "").trim();
  if (!maxLength || maxLength <= 0) {
    return normalized;
  }

  if (normalized.length <= maxLength) {
    return normalized;
  }

  return normalized.slice(0, maxLength);
}

function normalizeComparableText(value, maxLength = 220) {
  return sanitizeTextValue(value, maxLength)
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeColumnKey(value) {
  return sanitizeTextValue(value, 220)
    .toLowerCase()
    .replace(/[^a-z0-9а-яё]+/gi, "");
}

function normalizeUsername(value) {
  return sanitizeTextValue(value, 220).toLowerCase();
}

function parsePositiveInteger(rawValue, fallbackValue = 0) {
  const parsed = Number.parseInt(sanitizeTextValue(rawValue, 80), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallbackValue;
  }
  return parsed;
}

function readObjectPath(source, pathExpression) {
  if (!pathExpression) {
    return source;
  }

  const steps = sanitizeTextValue(pathExpression, 320).split(".").filter(Boolean);
  if (!steps.length) {
    return source;
  }

  let cursor = source;
  for (const step of steps) {
    if (!cursor || typeof cursor !== "object") {
      return undefined;
    }
    cursor = cursor[step];
  }

  return cursor;
}

function pickValueFromObject(source, fieldPaths) {
  if (!source || typeof source !== "object") {
    return undefined;
  }

  for (const fieldPath of Array.isArray(fieldPaths) ? fieldPaths : []) {
    const value = readObjectPath(source, fieldPath);
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }

  return undefined;
}

function buildFullName(firstName, lastName) {
  const first = sanitizeTextValue(firstName, 120);
  const last = sanitizeTextValue(lastName, 120);
  return [first, last].filter(Boolean).join(" ").trim();
}

function resolveGhlContactsHasMore(payload, currentPage, pageLimit) {
  const source = payload && typeof payload === "object" ? payload : {};
  const normalizedPage = Math.max(1, toSafeInteger(currentPage, 1));
  const normalizedLimit = Math.max(1, toSafeInteger(pageLimit, 1));

  const nextPageCandidate = toSafeInteger(
    pickValueFromObject(source, ["meta.nextPage", "nextPage", "pagination.nextPage", "meta.next_page"]),
    0,
  );
  if (nextPageCandidate > normalizedPage) {
    return true;
  }

  const hasMoreRaw = resolveOptionalBoolean(
    pickValueFromObject(source, ["meta.hasMore", "hasMore", "pagination.hasMore", "meta.has_more"]),
  );
  if (hasMoreRaw !== null) {
    return hasMoreRaw;
  }

  const totalCountCandidate = toSafeInteger(
    pickValueFromObject(source, ["meta.total", "total", "pagination.total", "meta.count", "count"]),
    0,
  );
  if (totalCountCandidate > 0) {
    return normalizedPage * normalizedLimit < totalCountCandidate;
  }

  return null;
}

function normalizeVisibleNames(rawValues) {
  const values = Array.isArray(rawValues)
    ? rawValues
    : typeof rawValues === "string"
      ? rawValues.split(/[\n,;]+/)
      : [];

  const items = [];
  const seen = new Set();

  for (const raw of values) {
    const value = sanitizeTextValue(raw, 220);
    if (!value) {
      continue;
    }

    const comparable = normalizeComparableText(value, 220);
    if (!comparable || seen.has(comparable)) {
      continue;
    }

    seen.add(comparable);
    items.push(value);
    if (items.length >= 500) {
      break;
    }
  }

  return items;
}

function resolveOptionalBoolean(value) {
  const normalized = sanitizeTextValue(value, 30).toLowerCase();
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

function normalizeIsoDateOrEmpty(value) {
  const timestamp = Date.parse(sanitizeTextValue(value, 80));
  if (!Number.isFinite(timestamp)) {
    return "";
  }

  return new Date(timestamp).toISOString();
}

function normalizeIsoDateOrNow(value) {
  const normalized = normalizeIsoDateOrEmpty(value);
  return normalized || new Date().toISOString();
}

function uniqueSortedNames(values) {
  const map = new Map();

  for (const rawValue of Array.isArray(values) ? values : []) {
    const value = sanitizeTextValue(rawValue, 220);
    if (!value) {
      continue;
    }

    const comparable = normalizeComparableText(value, 220);
    if (!comparable || map.has(comparable)) {
      continue;
    }

    map.set(comparable, value);
  }

  return [...map.values()].sort((left, right) => left.localeCompare(right, "en-US", { sensitivity: "base" }));
}

function toSafeInteger(rawValue, fallbackValue = 0) {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return fallbackValue;
  }

  const cleaned = sanitizeTextValue(rawValue, 80).replace(/[^0-9-]/g, "");
  if (!cleaned || cleaned === "-" || cleaned === "--") {
    return fallbackValue;
  }

  const parsed = Number.parseInt(cleaned, 10);
  if (!Number.isFinite(parsed)) {
    return fallbackValue;
  }

  return parsed;
}

function parseCurrencyValue(rawValue) {
  const normalized = sanitizeTextValue(rawValue, 120)
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");

  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return 0;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function decodeTextBuffer(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    return "";
  }

  const utf8 = buffer.toString("utf8");
  if (!utf8.includes("\ufffd")) {
    return utf8;
  }

  // Fallback for legacy CSV exports.
  return buffer.toString("latin1");
}

function parseDateValue(rawValue) {
  const value = sanitizeTextValue(rawValue, 120);
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

  const usMatch = value.match(/^(\d{1,2})[\/.\-](\d{1,2})[\/.\-](\d{2}|\d{4})$/);
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

function parseDateTimeValue(rawValue) {
  const value = sanitizeTextValue(rawValue, 160);
  if (!value) {
    return null;
  }

  const normalized = value.replace(/\s+/g, " ").trim();
  const directTimestamp = Date.parse(normalized);
  if (Number.isFinite(directTimestamp)) {
    return directTimestamp;
  }

  return parseDateValue(normalized);
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function normalizeCallDirection(directionRaw, statusRaw) {
  const direction = normalizeComparableText(directionRaw, 100);
  if (/(incoming|inbound|in|вход|входящий)/i.test(direction)) {
    return CUSTOM_DASHBOARD_DIRECTION_INCOMING;
  }
  if (/(outgoing|outbound|out|исход|исходящий)/i.test(direction)) {
    return CUSTOM_DASHBOARD_DIRECTION_OUTGOING;
  }

  const status = normalizeComparableText(statusRaw, 100);
  if (status.includes("missed") || status.includes("пропущ")) {
    return CUSTOM_DASHBOARD_DIRECTION_INCOMING;
  }

  return "unknown";
}

function normalizePhone(rawPhone) {
  const digits = sanitizeTextValue(rawPhone, 80).replace(/\D/g, "");
  if (!digits) {
    return "";
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  return digits;
}

function buildArchiveTimestamp(isoDate) {
  const timestamp = Date.parse(isoDate);
  const date = Number.isFinite(timestamp) ? new Date(timestamp) : new Date();
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hours = String(date.getUTCHours()).padStart(2, "0");
  const minutes = String(date.getUTCMinutes()).padStart(2, "0");
  const seconds = String(date.getUTCSeconds()).padStart(2, "0");
  return `${year}${month}${day}T${hours}${minutes}${seconds}Z`;
}

function resolveSafeTimeZone(rawTimeZone, fallback = "UTC") {
  const candidate = sanitizeTextValue(rawTimeZone, 120);
  if (!candidate) {
    return fallback;
  }

  try {
    new Intl.DateTimeFormat("en-US", {
      timeZone: candidate,
      year: "numeric",
    }).format(new Date());
    return candidate;
  } catch {
    return fallback;
  }
}

function getDateTimePartsInTimeZone(timestamp, timeZone) {
  const date = new Date(Number.isFinite(timestamp) ? timestamp : Date.now());
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: resolveSafeTimeZone(timeZone, "UTC"),
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(date);
  const valueByType = {};
  for (const part of parts) {
    if (part.type === "literal") {
      continue;
    }
    valueByType[part.type] = part.value;
  }

  return {
    year: toSafeInteger(valueByType.year, date.getUTCFullYear()),
    month: toSafeInteger(valueByType.month, date.getUTCMonth() + 1),
    day: toSafeInteger(valueByType.day, date.getUTCDate()),
    hour: toSafeInteger(valueByType.hour, date.getUTCHours()),
    minute: toSafeInteger(valueByType.minute, date.getUTCMinutes()),
    second: toSafeInteger(valueByType.second, date.getUTCSeconds()),
  };
}

function getTimeZoneOffsetMilliseconds(timestamp, timeZone) {
  const sourceTimestamp = Number.isFinite(timestamp) ? timestamp : Date.now();
  const parts = getDateTimePartsInTimeZone(sourceTimestamp, timeZone);
  const representedAsUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return representedAsUtc - sourceTimestamp;
}

function zonedDateTimeToUtcTimestamp(timeZone, year, month, day, hour, minute, second = 0) {
  const safeYear = toSafeInteger(year, 1970);
  const safeMonth = Math.min(Math.max(toSafeInteger(month, 1), 1), 12);
  const safeDay = Math.min(Math.max(toSafeInteger(day, 1), 1), 31);
  const safeHour = Math.min(Math.max(toSafeInteger(hour, 0), 0), 23);
  const safeMinute = Math.min(Math.max(toSafeInteger(minute, 0), 0), 59);
  const safeSecond = Math.min(Math.max(toSafeInteger(second, 0), 0), 59);

  const approximateUtc = Date.UTC(safeYear, safeMonth - 1, safeDay, safeHour, safeMinute, safeSecond);
  const offsetMs = getTimeZoneOffsetMilliseconds(approximateUtc, timeZone);
  return approximateUtc - offsetMs;
}

function getNextZonedDailyRunTimestamp(options = {}) {
  const safeTimeZone = resolveSafeTimeZone(options.timeZone, "America/Chicago");
  const safeHour = Math.min(Math.max(toSafeInteger(options.hour, 22), 0), 23);
  const safeMinute = Math.min(Math.max(toSafeInteger(options.minute, 0), 0), 59);
  const nowTimestamp = Number.isFinite(options.nowTimestamp) ? options.nowTimestamp : Date.now();
  const nowParts = getDateTimePartsInTimeZone(nowTimestamp, safeTimeZone);

  let nextRun = zonedDateTimeToUtcTimestamp(
    safeTimeZone,
    nowParts.year,
    nowParts.month,
    nowParts.day,
    safeHour,
    safeMinute,
    0,
  );
  if (nextRun > nowTimestamp + 1000) {
    return nextRun;
  }

  const tomorrowUtcDate = new Date(Date.UTC(nowParts.year, nowParts.month - 1, nowParts.day) + DAY_IN_MS);
  return zonedDateTimeToUtcTimestamp(
    safeTimeZone,
    tomorrowUtcDate.getUTCFullYear(),
    tomorrowUtcDate.getUTCMonth() + 1,
    tomorrowUtcDate.getUTCDate(),
    safeHour,
    safeMinute,
    0,
  );
}

function getCurrentUtcDayStart() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

function getCurrentDayStartInTimeZone(timeZone) {
  const safeTimeZone = resolveSafeTimeZone(timeZone, "UTC");
  const nowTimestamp = Date.now();
  const parts = getDateTimePartsInTimeZone(nowTimestamp, safeTimeZone);
  return zonedDateTimeToUtcTimestamp(safeTimeZone, parts.year, parts.month, parts.day, 0, 0, 0);
}

function getUtcDayStart(timestamp) {
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  const date = new Date(timestamp);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function getDayStartInTimeZone(timestamp, timeZone) {
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  const safeTimeZone = resolveSafeTimeZone(timeZone, "UTC");
  const parts = getDateTimePartsInTimeZone(timestamp, safeTimeZone);
  return zonedDateTimeToUtcTimestamp(safeTimeZone, parts.year, parts.month, parts.day, 0, 0, 0);
}

function getCurrentWeekStartUtc(todayStart) {
  const dayStart = Number.isFinite(todayStart) ? todayStart : getCurrentUtcDayStart();
  const dayOfWeek = new Date(dayStart).getUTCDay();
  const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  return dayStart - mondayOffset * DAY_IN_MS;
}

function getCurrentMonthStartUtc(todayStart) {
  const dayStart = Number.isFinite(todayStart) ? todayStart : getCurrentUtcDayStart();
  const date = new Date(dayStart);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
}

function getDaysBetweenUtc(leftTimestamp, rightTimestamp) {
  const left = getUtcDayStart(leftTimestamp);
  const right = getUtcDayStart(rightTimestamp);
  if (left === null || right === null) {
    return 0;
  }

  if (right <= left) {
    return 0;
  }

  return Math.floor((right - left) / DAY_IN_MS);
}

function guessBooleanCountFromStatus(statusComparable, matchers) {
  if (!statusComparable) {
    return 0;
  }

  const tokens = Array.isArray(matchers) ? matchers : [];
  for (const token of tokens) {
    if (statusComparable.includes(token)) {
      return 1;
    }
  }

  return 0;
}

function summarizeSalesPeriod(contacts, calls, fromDayStart, toDayStart) {
  const summary = buildEmptySalesMetrics();
  const from = Number.isFinite(fromDayStart) ? fromDayStart : 0;
  const to = Number.isFinite(toDayStart) ? toDayStart : 0;

  for (const call of Array.isArray(calls) ? calls : []) {
    const callDay = getUtcDayStart(call.callAtTimestamp);
    if (callDay === null || callDay < from || callDay > to) {
      continue;
    }

    summary.calls += 1;
    summary.answers += call.isAnswered ? 1 : 0;
    summary.talks += call.isOver30Sec ? 1 : 0;
  }

  for (const contact of Array.isArray(contacts) ? contacts : []) {
    const day = getUtcDayStart(contact.eventTimestamp);
    if (day === null || day < from || day > to) {
      continue;
    }

    summary.calls += Math.max(0, contact.callsCount || 0);
    summary.answers += Math.max(0, contact.answersCount || 0);
    summary.talks += Math.max(0, contact.talksCount || 0);
    summary.interested += Math.max(0, contact.interestedCount || 0);
    summary.closedDeals += Math.max(0, contact.closedDealsCount || 0);
    summary.closedAmount += Number.isFinite(contact.closedAmount) ? contact.closedAmount : 0;
  }

  return summary;
}

function buildEmptySalesMetrics() {
  return {
    calls: 0,
    answers: 0,
    talks: 0,
    interested: 0,
    closedDeals: 0,
    closedAmount: 0,
  };
}

async function mapWithConcurrency(items, maxConcurrency, mapper) {
  const source = Array.isArray(items) ? items : [];
  if (!source.length) {
    return [];
  }

  const concurrency = Math.max(1, toSafeInteger(maxConcurrency, 1));
  const results = new Array(source.length);
  let nextIndex = 0;

  const workers = [];
  const workerCount = Math.min(concurrency, source.length);

  for (let workerIndex = 0; workerIndex < workerCount; workerIndex += 1) {
    workers.push(
      (async () => {
        while (true) {
          const currentIndex = nextIndex;
          nextIndex += 1;
          if (currentIndex >= source.length) {
            return;
          }

          results[currentIndex] = await mapper(source[currentIndex], currentIndex);
        }
      })(),
    );
  }

  await Promise.all(workers);
  return results;
}

module.exports = {
  registerCustomDashboardModule,
};
