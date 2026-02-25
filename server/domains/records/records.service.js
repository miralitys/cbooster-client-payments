"use strict";

function createRecordsService(dependencies = {}) {
  const {
    simulateSlowRecords,
    simulateSlowRecordsDelayMs,
    delayMs,
    hasDatabase,
    getStoredRecordsForApiRecordsRoute,
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

  async function getRecordsForApi({ webAuthProfile, webAuthUser }) {
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

    const filteredRecords = filterClientRecordsForWebAuthUser(state.records, webAuthProfile);
    return {
      status: 200,
      body: {
        records: filteredRecords,
        updatedAt: state.updatedAt,
      },
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
    saveRecordsForApi,
    patchRecordsForApi,
  };
}

module.exports = {
  createRecordsService,
};

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
