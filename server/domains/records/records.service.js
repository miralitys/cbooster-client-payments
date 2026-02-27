"use strict";

function createRecordsService(dependencies = {}) {
  const {
    simulateSlowRecords,
    simulateSlowRecordsDelayMs,
    delayMs,
    hasDatabase,
    getStoredRecordsForApiRecordsRoute,
    listClientHealthRecordsSafeMode,
    readV2Enabled,
    scheduleDualReadCompareForLegacyRecords,
    filterClientRecordsForWebAuthUser,
    sanitizeTextValue,
    saveStoredRecords,
    saveStoredRecordsPatch,
    publishPaymentReceivedEvents,
    logWarn,
  } = dependencies;
  const warn = typeof logWarn === "function" ? logWarn : () => {};

  async function getRecordsForApi({ webAuthProfile, webAuthUser, pagination = null, clientFilters = null }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          records: [],
          updatedAt: new Date().toISOString(),
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const state = await getStoredRecordsForApiRecordsRoute();
    if (!readV2Enabled && state.source === "legacy") {
      scheduleDualReadCompareForLegacyRecords(state.records, {
        source: "GET /api/records",
        requestedBy: webAuthUser,
      });
    }

    if (readV2Enabled && state.fallbackFromV2) {
      logWarn(
        `[records] READ_V2 served legacy fallback for user=${sanitizeTextValue(webAuthUser, 160) || "unknown"}`,
      );
    }

    const roleFilteredRecords = filterClientRecordsForWebAuthUser(state.records, webAuthProfile);
    const filteredRecords = applyClientApiFilters(roleFilteredRecords, clientFilters);
    if (pagination?.enabled) {
      const offset = Math.max(0, Number.parseInt(String(pagination.offset || 0), 10) || 0);
      const limit = Math.max(1, Number.parseInt(String(pagination.limit || 100), 10) || 100);
      const total = filteredRecords.length;
      const pagedRecords = filteredRecords.slice(offset, offset + limit);
      const nextOffset = offset + pagedRecords.length;

      return {
        status: 200,
        body: {
          records: pagedRecords,
          updatedAt: state.updatedAt,
          total,
          limit,
          offset,
          hasMore: nextOffset < total,
          nextOffset: nextOffset < total ? nextOffset : null,
        },
      };
    }

    return {
      status: 200,
      body: {
        records: filteredRecords,
        updatedAt: state.updatedAt,
      },
    };
  }

  async function getClientHealthSnapshotForApi({ webAuthProfile, webAuthUser }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          records: [],
          updatedAt: new Date().toISOString(),
          limit: 5,
          safeMode: true,
          source: "simulated",
          sampleMode: "latest_active",
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const safeLimit = 5;
    if (typeof listClientHealthRecordsSafeMode === "function") {
      try {
        const safeSnapshot = await listClientHealthRecordsSafeMode({ limit: safeLimit });
        const roleScopedRecords = filterClientRecordsForWebAuthUser(safeSnapshot.records, webAuthProfile);
        const sampledRecords = selectSafeModeClientSample(roleScopedRecords, safeLimit);
        const sampleMode = sanitizeTextValue(safeSnapshot.sampleMode, 40) || "latest_active";

        if (sampledRecords.length) {
          return {
            status: 200,
            body: {
              records: sampledRecords,
              updatedAt: safeSnapshot.updatedAt || null,
              limit: safeLimit,
              safeMode: true,
              source: "v2_safe_sql",
              sampleMode,
            },
          };
        }
      } catch (error) {
        const safeMessage = sanitizeTextValue(error?.message, 240) || "unknown error";
        warn(
          `[records] SAFE_MODE fallback to legacy snapshot for user=${sanitizeTextValue(webAuthUser, 160) || "unknown"}: ${safeMessage}`,
        );
      }
    }

    const state = await getStoredRecordsForApiRecordsRoute();
    const roleFilteredRecords = filterClientRecordsForWebAuthUser(state.records, webAuthProfile);
    const sampledRecords = selectSafeModeClientSample(roleFilteredRecords, safeLimit);

    return {
      status: 200,
      body: {
        records: sampledRecords,
        updatedAt: state.updatedAt || null,
        limit: safeLimit,
        safeMode: true,
        source: "legacy_fallback",
        sampleMode: "latest_active",
      },
    };
  }

  async function getClientFilterOptionsForApi({ webAuthProfile }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          closedByOptions: [],
          clientManagerOptions: [],
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const state = await getStoredRecordsForApiRecordsRoute();
    const roleFilteredRecords = filterClientRecordsForWebAuthUser(state.records, webAuthProfile);
    const options = extractClientFilterOptions(roleFilteredRecords);

    return {
      status: 200,
      body: options,
    };
  }

  async function saveRecordsForApi({ records, expectedUpdatedAt }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          ok: true,
          updatedAt: new Date().toISOString(),
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const previousRecords = await readCurrentRecordsForNotificationDiff({
      getStoredRecordsForApiRecordsRoute,
      warn,
      sanitizeTextValue,
    });

    const updatedAt = await saveStoredRecords(records, {
      expectedUpdatedAt,
    });

    await emitPaymentReceivedEventsFromDiff({
      previousRecords,
      nextRecords: records,
      publishPaymentReceivedEvents,
      warn,
      sanitizeTextValue,
    });

    return {
      status: 200,
      body: {
        ok: true,
        updatedAt,
      },
    };
  }

  async function patchRecordsForApi({ operations, expectedUpdatedAt }) {
    if (simulateSlowRecords) {
      await delayMs(simulateSlowRecordsDelayMs);
      return {
        status: 200,
        body: {
          ok: true,
          updatedAt: new Date().toISOString(),
          appliedOperations: Array.isArray(operations) ? operations.length : 0,
        },
      };
    }

    if (!hasDatabase()) {
      return {
        status: 503,
        body: {
          error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
        },
      };
    }

    const previousRecords = await readCurrentRecordsForNotificationDiff({
      getStoredRecordsForApiRecordsRoute,
      warn,
      sanitizeTextValue,
    });

    const result = await saveStoredRecordsPatch(operations, {
      expectedUpdatedAt,
    });

    const nextRecords = await readCurrentRecordsForNotificationDiff({
      getStoredRecordsForApiRecordsRoute,
      warn,
      sanitizeTextValue,
    });

    await emitPaymentReceivedEventsFromDiff({
      previousRecords,
      nextRecords,
      publishPaymentReceivedEvents,
      warn,
      sanitizeTextValue,
    });

    return {
      status: 200,
      body: {
        ok: true,
        updatedAt: result.updatedAt,
        appliedOperations: Array.isArray(operations) ? operations.length : 0,
      },
    };
  }

  return {
    getRecordsForApi,
    getClientHealthSnapshotForApi,
    getClientFilterOptionsForApi,
    saveRecordsForApi,
    patchRecordsForApi,
  };
}

module.exports = {
  createRecordsService,
};

const PAYMENT_DATE_FIELD_KEYS = Array.from({ length: 36 }, (_, index) => `payment${index + 1}Date`);
const PAYMENT_FIELD_KEYS = Array.from({ length: 36 }, (_, index) => `payment${index + 1}`);
const STATUS_FILTER_ALL = "all";
const STATUS_FILTER_WRITTEN_OFF = "written-off";
const STATUS_FILTER_FULLY_PAID = "fully-paid";
const STATUS_FILTER_AFTER_RESULT = "after-result";
const STATUS_FILTER_OVERDUE = "overdue";
const NO_MANAGER_LABEL = "No manager";
const ZERO_TOLERANCE = 1e-6;

function applyClientApiFilters(records, rawFilters) {
  if (!Array.isArray(records) || !records.length) {
    return [];
  }

  const filters = normalizeClientApiFilters(rawFilters);
  if (!hasClientApiFilters(filters)) {
    return records;
  }

  const filtered = [];
  for (const record of records) {
    if (!matchesClientSearch(record, filters.search)) {
      continue;
    }

    if (filters.closedBy && normalizeComparableText(record?.closedBy) !== filters.closedBy) {
      continue;
    }

    if (filters.clientManager && !matchesClientManager(record, filters.clientManager)) {
      continue;
    }

    if (filters.activeOnly && !isActiveEnabled(record?.active)) {
      continue;
    }

    if (filters.excludeWrittenOff && isTruthy(record?.writtenOff)) {
      continue;
    }

    if (!isDateWithinRange(record?.payment1Date, filters.createdFromTs, filters.createdToTs)) {
      continue;
    }

    if (filters.hasPaymentRange && !hasAnyPaymentDateWithinRange(record, filters.paymentFromTs, filters.paymentToTs)) {
      continue;
    }

    if (filters.requiresStatus) {
      const status = getRecordStatusFlags(record);
      if (
        filters.hasWrittenOffRange &&
        (!status.isWrittenOff || !isDateWithinRange(record?.dateWhenWrittenOff, filters.writtenOffFromTs, filters.writtenOffToTs))
      ) {
        continue;
      }
      if (
        filters.hasFullyPaidRange &&
        (!status.isFullyPaid || !isDateWithinRange(record?.dateWhenFullyPaid, filters.fullyPaidFromTs, filters.fullyPaidToTs))
      ) {
        continue;
      }
      if (!matchesStatusFilterByStatus(status, filters.status, filters.overdueRange)) {
        continue;
      }
    }

    filtered.push(record);
  }

  return filtered;
}

function normalizeClientApiFilters(rawFilters) {
  const filters = rawFilters && typeof rawFilters === "object" ? rawFilters : {};
  const normalizedStatus = normalizeComparableText(filters.status);
  const status = normalizeStatusFilter(normalizedStatus);
  const createdFromTs = parseDateValue(filters.createdFrom);
  const createdToTs = parseDateValue(filters.createdTo);
  const paymentFromTs = parseDateValue(filters.paymentFrom);
  const paymentToTs = parseDateValue(filters.paymentTo);
  const writtenOffFromTs = parseDateValue(filters.writtenOffFrom);
  const writtenOffToTs = parseDateValue(filters.writtenOffTo);
  const fullyPaidFromTs = parseDateValue(filters.fullyPaidFrom);
  const fullyPaidToTs = parseDateValue(filters.fullyPaidTo);
  const hasPaymentRange = paymentFromTs !== null || paymentToTs !== null;
  const hasWrittenOffRange = writtenOffFromTs !== null || writtenOffToTs !== null;
  const hasFullyPaidRange = fullyPaidFromTs !== null || fullyPaidToTs !== null;

  return {
    search: normalizeComparableText(filters.search),
    closedBy: normalizeComparableText(filters.closedBy),
    clientManager: normalizeComparableText(filters.clientManager),
    status,
    overdueRange: normalizeComparableText(filters.overdueRange),
    createdFromTs,
    createdToTs,
    paymentFromTs,
    paymentToTs,
    writtenOffFromTs,
    writtenOffToTs,
    fullyPaidFromTs,
    fullyPaidToTs,
    hasPaymentRange,
    hasWrittenOffRange,
    hasFullyPaidRange,
    requiresStatus: hasWrittenOffRange || hasFullyPaidRange || status !== STATUS_FILTER_ALL,
    activeOnly: filters.activeOnly === true,
    excludeWrittenOff: filters.excludeWrittenOff === true,
  };
}

function hasClientApiFilters(filters) {
  return Boolean(
    filters.search ||
      filters.closedBy ||
      filters.clientManager ||
      filters.status !== STATUS_FILTER_ALL ||
      filters.overdueRange ||
      filters.createdFromTs !== null ||
      filters.createdToTs !== null ||
      filters.paymentFromTs !== null ||
      filters.paymentToTs !== null ||
      filters.writtenOffFromTs !== null ||
      filters.writtenOffToTs !== null ||
      filters.fullyPaidFromTs !== null ||
      filters.fullyPaidToTs !== null ||
      filters.activeOnly ||
      filters.excludeWrittenOff,
  );
}

function normalizeStatusFilter(rawValue) {
  if (
    rawValue === STATUS_FILTER_WRITTEN_OFF ||
    rawValue === STATUS_FILTER_FULLY_PAID ||
    rawValue === STATUS_FILTER_AFTER_RESULT ||
    rawValue === STATUS_FILTER_OVERDUE
  ) {
    return rawValue;
  }
  return STATUS_FILTER_ALL;
}

function normalizeComparableText(rawValue) {
  return String(rawValue || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function matchesClientSearch(record, query) {
  if (!query) {
    return true;
  }

  const normalizedQuery = normalizeComparableText(query);
  if (!normalizedQuery) {
    return true;
  }

  const searchableFields = [
    record?.clientName,
    record?.clientEmailAddress,
    record?.clientPhoneNumber,
    record?.ssn,
  ];
  const hasTextMatch = searchableFields.some((value) =>
    normalizeComparableText(value).includes(normalizedQuery),
  );
  if (hasTextMatch) {
    return true;
  }

  const queryDigits = String(query || "").replace(/\D/g, "");
  if (!queryDigits) {
    return false;
  }
  const phoneDigits = String(record?.clientPhoneNumber || "").replace(/\D/g, "");
  const ssnDigits = String(record?.ssn || "").replace(/\D/g, "");
  return phoneDigits.includes(queryDigits) || ssnDigits.includes(queryDigits);
}

function matchesClientManager(record, selectedManagerComparable) {
  const managerNames = splitClientManagerNames(record?.clientManager);
  return managerNames.some((name) => normalizeComparableText(name) === selectedManagerComparable);
}

function splitClientManagerNames(rawLabel) {
  const value = String(rawLabel || "").trim();
  if (!value || value === "-" || value.toLowerCase() === "unassigned") {
    return [NO_MANAGER_LABEL];
  }

  const names = value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => {
      if (!item) {
        return false;
      }
      const normalized = item.toLowerCase();
      return normalized !== "-" && normalized !== "unassigned" && normalized !== "no manager";
    });

  if (!names.length) {
    return [NO_MANAGER_LABEL];
  }

  return Array.from(new Set(names));
}

function hasAnyPaymentDateWithinRange(record, fromDate, toDate) {
  for (const paymentDateField of PAYMENT_DATE_FIELD_KEYS) {
    if (isDateWithinRange(record?.[paymentDateField], fromDate, toDate)) {
      return true;
    }
  }
  return false;
}

function isDateWithinRange(value, fromDate, toDate) {
  if (fromDate === null && toDate === null) {
    return true;
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return false;
  }
  if (fromDate !== null && timestamp < fromDate) {
    return false;
  }
  if (toDate !== null && timestamp > toDate) {
    return false;
  }
  return true;
}

function parseDateValue(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return null;
  }

  const mmddyyyyMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mmddyyyyMatch) {
    const month = Number.parseInt(mmddyyyyMatch[1], 10);
    const day = Number.parseInt(mmddyyyyMatch[2], 10);
    const year = Number.parseInt(mmddyyyyMatch[3], 10);
    if (!Number.isFinite(month) || !Number.isFinite(day) || !Number.isFinite(year)) {
      return null;
    }

    if (month < 1 || month > 12 || day < 1 || day > 31 || year < 1900 || year > 9999) {
      return null;
    }

    const timestamp = Date.UTC(year, month - 1, day);
    const parsedDate = new Date(timestamp);
    if (
      parsedDate.getUTCFullYear() !== year ||
      parsedDate.getUTCMonth() !== month - 1 ||
      parsedDate.getUTCDate() !== day
    ) {
      return null;
    }
    return timestamp;
  }

  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return null;
  }
  const date = new Date(parsed);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function getRecordStatusFlags(record) {
  const isAfterResult = isTruthy(record?.afterResult);
  const isWrittenOff = isTruthy(record?.writtenOff);
  const isContractCompleted = isTruthy(record?.contractCompleted);
  const futureAmount = computeFuturePaymentsAmount(record);
  const isFullyPaid = !isWrittenOff && !isContractCompleted && futureAmount !== null && futureAmount <= ZERO_TOLERANCE;
  const latestPaymentDate = getLatestPaymentDateTimestamp(record);
  const overdueDays =
    !isAfterResult && !isWrittenOff && !isFullyPaid && !isContractCompleted && latestPaymentDate !== null
      ? getDaysSinceDate(latestPaymentDate)
      : 0;
  const overdueRange = getOverdueRangeLabel(overdueDays);

  return {
    isAfterResult,
    isWrittenOff,
    isFullyPaid,
    isOverdue: Boolean(overdueRange),
    overdueRange,
  };
}

function isTruthy(rawValue) {
  const value = String(rawValue || "").trim().toLowerCase();
  return value === "yes" || value === "true" || value === "1" || value === "on" || value === "completed";
}

function isActiveEnabled(rawValue) {
  const value = String(rawValue || "").trim().toLowerCase();
  return value === "yes" || value === "true" || value === "1" || value === "on" || value === "active";
}

function selectSafeModeClientSample(records, limit = 5) {
  const maxRows = Math.min(Math.max(Number.parseInt(String(limit || 5), 10) || 5, 1), 5);
  if (!Array.isArray(records) || !records.length || maxRows <= 0) {
    return [];
  }

  const normalized = records
    .filter((record) => record && typeof record === "object")
    .slice()
    .sort((left, right) => resolveRecordRecencyTimestamp(right) - resolveRecordRecencyTimestamp(left));

  const activeRows = normalized.filter((record) => isActiveEnabled(record?.active));
  if (activeRows.length >= maxRows) {
    return activeRows.slice(0, maxRows);
  }

  const seenIds = new Set(activeRows.map((record) => String(record?.id || "").trim()).filter(Boolean));
  const combined = [...activeRows];
  for (const record of normalized) {
    if (combined.length >= maxRows) {
      break;
    }
    const recordId = String(record?.id || "").trim();
    if (recordId && seenIds.has(recordId)) {
      continue;
    }
    if (recordId) {
      seenIds.add(recordId);
    }
    combined.push(record);
  }

  return combined.slice(0, maxRows);
}

function resolveRecordRecencyTimestamp(record) {
  const candidates = [
    parseDateValue(record?.createdAt),
    parseDateValue(record?.payment1Date),
    parseDateValue(record?.startedInWork),
    parseDateValue(record?.dateWhenFullyPaid),
    parseDateValue(record?.scoreUpdatedAt),
  ].filter((value) => value !== null);

  if (!candidates.length) {
    return 0;
  }

  return Math.max(...candidates);
}

function computeFuturePaymentsAmount(record) {
  const contractTotal = parseMoneyValue(record?.contractTotals);
  if (contractTotal === null) {
    return null;
  }
  let totalPayments = 0;
  let hasAnyPayment = false;
  for (const paymentField of PAYMENT_FIELD_KEYS) {
    const amount = parseMoneyValue(record?.[paymentField]);
    if (amount === null) {
      continue;
    }
    totalPayments += amount;
    hasAnyPayment = true;
  }
  if (!hasAnyPayment) {
    totalPayments = parseMoneyValue(record?.totalPayments) || 0;
  }
  return contractTotal - totalPayments;
}

function parseMoneyValue(rawValue) {
  const value = String(rawValue || "").trim();
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

function getLatestPaymentDateTimestamp(record) {
  let latestTimestamp = null;
  for (const paymentDateField of PAYMENT_DATE_FIELD_KEYS) {
    const timestamp = parseDateValue(record?.[paymentDateField]);
    if (timestamp === null) {
      continue;
    }
    if (latestTimestamp === null || timestamp > latestTimestamp) {
      latestTimestamp = timestamp;
    }
  }
  return latestTimestamp;
}

function getDaysSinceDate(timestamp) {
  const now = new Date();
  const currentDayStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const diff = currentDayStart - timestamp;
  if (diff <= 0) {
    return 0;
  }
  return Math.floor(diff / 86400000);
}

function getOverdueRangeLabel(daysOverdue) {
  if (daysOverdue <= 0) {
    return "";
  }
  if (daysOverdue <= 7) {
    return "1-7";
  }
  if (daysOverdue <= 30) {
    return "8-30";
  }
  if (daysOverdue <= 60) {
    return "31-60";
  }
  return "60+";
}

function matchesStatusFilterByStatus(status, statusFilter, overdueRangeFilter) {
  if (statusFilter === STATUS_FILTER_ALL) {
    return true;
  }
  if (statusFilter === STATUS_FILTER_WRITTEN_OFF) {
    return status.isWrittenOff;
  }
  if (statusFilter === STATUS_FILTER_FULLY_PAID) {
    return status.isFullyPaid;
  }
  if (statusFilter === STATUS_FILTER_AFTER_RESULT) {
    return status.isAfterResult;
  }
  if (statusFilter === STATUS_FILTER_OVERDUE) {
    if (!status.isOverdue) {
      return false;
    }
    if (!overdueRangeFilter) {
      return true;
    }
    return status.overdueRange === overdueRangeFilter;
  }
  return true;
}

function extractClientFilterOptions(records) {
  const collator = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });
  const closedByByComparable = new Map();
  const managersByComparable = new Map();
  let hasNoManager = false;

  for (const record of records) {
    const closedByRaw = String(record?.closedBy || "").trim();
    const closedByComparable = normalizeComparableText(closedByRaw);
    if (closedByComparable && !closedByByComparable.has(closedByComparable)) {
      closedByByComparable.set(closedByComparable, closedByRaw);
    }

    for (const managerName of splitClientManagerNames(record?.clientManager)) {
      if (managerName === NO_MANAGER_LABEL) {
        hasNoManager = true;
        continue;
      }
      const comparable = normalizeComparableText(managerName);
      if (!comparable || managersByComparable.has(comparable)) {
        continue;
      }
      managersByComparable.set(comparable, managerName);
    }
  }

  const closedByOptions = Array.from(closedByByComparable.values()).sort((left, right) =>
    collator.compare(left, right),
  );
  const managerOptions = Array.from(managersByComparable.values()).sort((left, right) =>
    collator.compare(left, right),
  );

  return {
    closedByOptions,
    clientManagerOptions: hasNoManager ? [NO_MANAGER_LABEL, ...managerOptions] : managerOptions,
  };
}

async function readCurrentRecordsForNotificationDiff(options = {}) {
  const {
    getStoredRecordsForApiRecordsRoute,
    warn,
    sanitizeTextValue,
  } = options;

  if (typeof getStoredRecordsForApiRecordsRoute !== "function") {
    return null;
  }

  try {
    const state = await getStoredRecordsForApiRecordsRoute();
    return Array.isArray(state?.records) ? state.records : [];
  } catch (error) {
    const safeMessage =
      typeof sanitizeTextValue === "function"
        ? sanitizeTextValue(error?.message, 320) || "unknown error"
        : "unknown error";
    if (typeof warn === "function") {
      warn(`[records notifications] Failed to read records diff snapshot: ${safeMessage}`);
    }
    return null;
  }
}

async function emitPaymentReceivedEventsFromDiff(options = {}) {
  const {
    previousRecords,
    nextRecords,
    publishPaymentReceivedEvents,
    warn,
    sanitizeTextValue,
  } = options;

  if (typeof publishPaymentReceivedEvents !== "function") {
    return;
  }

  if (!Array.isArray(previousRecords) || !Array.isArray(nextRecords)) {
    return;
  }

  const events = detectPaymentReceivedEvents(previousRecords, nextRecords);
  if (!events.length) {
    return;
  }

  try {
    await publishPaymentReceivedEvents(events);
  } catch (error) {
    const safeMessage =
      typeof sanitizeTextValue === "function"
        ? sanitizeTextValue(error?.message, 320) || "unknown error"
        : "unknown error";
    if (typeof warn === "function") {
      warn(`[records notifications] Failed to persist payment notifications: ${safeMessage}`);
    }
  }
}

function detectPaymentReceivedEvents(previousRecords, nextRecords) {
  const previousRecordById = buildRecordMapById(previousRecords);
  const events = [];

  for (const record of Array.isArray(nextRecords) ? nextRecords : []) {
    if (!record || typeof record !== "object") {
      continue;
    }

    const recordId = readRecordTextValue(record, "id");
    if (!recordId) {
      continue;
    }

    const previousRecord = previousRecordById.get(recordId) || null;
    const paymentSlot = resolveFirstReceivedPaymentSlot(previousRecord, record);
    if (paymentSlot <= 0) {
      continue;
    }

    const paymentFieldKey = `payment${paymentSlot}`;
    const paymentDateFieldKey = `payment${paymentSlot}Date`;
    const clientName = readRecordTextValue(record, "clientName");
    const paymentAmount = readRecordTextValue(record, paymentFieldKey);
    const paymentDate = readRecordTextValue(record, paymentDateFieldKey);
    const title = clientName ? `Payment received from ${clientName}` : "Payment received";
    let message = `Payment ${paymentSlot} was posted.`;
    if (paymentAmount) {
      message += ` Amount: ${paymentAmount}.`;
    }
    if (paymentDate) {
      message += ` Date: ${paymentDate}.`;
    }

    events.push({
      type: "client_payment_received",
      title,
      message,
      tone: "success",
      clientName,
      linkHref: "/app/client-payments",
      linkLabel: "Open",
      paymentSlot,
      paymentAmount,
      paymentDate,
      record,
    });
  }

  return events;
}

function buildRecordMapById(records) {
  const map = new Map();
  for (const record of Array.isArray(records) ? records : []) {
    if (!record || typeof record !== "object") {
      continue;
    }
    const id = readRecordTextValue(record, "id");
    if (!id) {
      continue;
    }
    map.set(id, record);
  }
  return map;
}

function resolveFirstReceivedPaymentSlot(previousRecord, nextRecord) {
  for (let slot = 1; slot <= 36; slot += 1) {
    if (isPaymentReceivedInSlot(previousRecord, nextRecord, slot)) {
      return slot;
    }
  }
  return 0;
}

function isPaymentReceivedInSlot(previousRecord, nextRecord, slotIndex) {
  const paymentKey = `payment${slotIndex}`;
  const paymentDateKey = `${paymentKey}Date`;
  const nextAmount = readRecordTextValue(nextRecord, paymentKey);
  const nextDate = readRecordTextValue(nextRecord, paymentDateKey);
  if (!nextAmount && !nextDate) {
    return false;
  }

  if (!previousRecord || typeof previousRecord !== "object") {
    return Boolean(nextAmount || nextDate);
  }

  const previousAmount = readRecordTextValue(previousRecord, paymentKey);
  const previousDate = readRecordTextValue(previousRecord, paymentDateKey);
  if (!previousAmount && nextAmount) {
    return true;
  }

  const nextAmountNumber = parseMoneyLikeValue(nextAmount);
  const previousAmountNumber = parseMoneyLikeValue(previousAmount);
  if (nextAmount && nextAmountNumber !== null) {
    if (previousAmountNumber === null) {
      return true;
    }
    if (nextAmountNumber > previousAmountNumber + 0.000001) {
      return true;
    }
  }

  return Boolean(nextAmount && !previousDate && nextDate);
}

function readRecordTextValue(record, key) {
  if (!record || typeof record !== "object") {
    return "";
  }

  return String(record[key] ?? "").trim();
}

function parseMoneyLikeValue(rawValue) {
  const value = String(rawValue ?? "").trim();
  if (!value) {
    return null;
  }

  let normalized = value.replace(/[$,\s]/g, "");
  let isNegative = false;
  if (normalized.startsWith("(") && normalized.endsWith(")")) {
    isNegative = true;
    normalized = normalized.slice(1, -1);
  }

  normalized = normalized.replace(/[^0-9.+-]/g, "");
  if (!normalized) {
    return null;
  }

  const parsed = Number.parseFloat(normalized);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return isNegative ? -parsed : parsed;
}
