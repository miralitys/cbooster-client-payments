const path = require("path");
const multer = require("multer");

let XLSX = null;
try {
  // Optional dependency: enables direct XLSX/XLS uploads.
  // If unavailable, CSV/TSV uploads are still supported.
  // eslint-disable-next-line global-require
  XLSX = require("xlsx");
} catch {
  XLSX = null;
}

const CUSTOM_DASHBOARD_TABLE_DEFAULT = "app_data";
const CUSTOM_DASHBOARD_DB_SCHEMA_DEFAULT = "public";
const CUSTOM_DASHBOARD_UPLOAD_MAX_FILE_SIZE_BYTES = 20 * 1024 * 1024;
const CUSTOM_DASHBOARD_MAX_ROWS_PER_UPLOAD = 50000;
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
      stats,
      missedCalls,
    };
  }

  async function buildDashboardPayload(req) {
    const usersConfigRaw = await readAppDataValue(CUSTOM_DASHBOARD_USERS_KEY, null);
    const usersConfig = normalizeUsersConfig(usersConfigRaw);

    const [tasksDataRaw, contactsDataRaw, callsDataRaw] = await Promise.all([
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.tasks, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.contacts, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null),
    ]);

    const tasksData = normalizeUploadData(tasksDataRaw, "tasks");
    const contactsData = normalizeUploadData(contactsDataRaw, "contacts");
    const callsData = normalizeUploadData(callsDataRaw, "calls");

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
        tasks: buildUploadMeta(tasksData),
        contacts: buildUploadMeta(contactsData),
        calls: buildUploadMeta(callsData),
      },
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

    const [tasksDataRaw, contactsDataRaw, callsDataRaw] = await Promise.all([
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.tasks, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.contacts, null),
      readAppDataValue(CUSTOM_DASHBOARD_LATEST_UPLOAD_KEYS.calls, null),
    ]);

    const tasksData = normalizeUploadData(tasksDataRaw, "tasks");
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
      if (!XLSX) {
        throw createHttpError(
          "XLSX parser is not available on this server. Upload CSV/TSV export from Excel.",
          400,
        );
      }

      const workbook = XLSX.read(content, {
        type: "buffer",
        raw: false,
        cellDates: true,
      });
      const firstSheetName = Array.isArray(workbook.SheetNames) && workbook.SheetNames.length
        ? workbook.SheetNames[0]
        : "";
      if (!firstSheetName || !workbook.Sheets[firstSheetName]) {
        throw createHttpError("Excel file has no readable sheets.", 400);
      }

      const rows = XLSX.utils.sheet_to_json(workbook.Sheets[firstSheetName], {
        defval: "",
        raw: false,
      });

      return normalizeRawRows(rows);
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

function getCurrentUtcDayStart() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

function getUtcDayStart(timestamp) {
  if (!Number.isFinite(timestamp)) {
    return null;
  }

  const date = new Date(timestamp);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
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

module.exports = {
  registerCustomDashboardModule,
};
