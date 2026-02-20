"use strict";

const PATCH_OPERATION_UPSERT = "upsert";
const PATCH_OPERATION_DELETE = "delete";

function normalizeRecordStateTimestamp(rawValue) {
  if (rawValue === null || rawValue === undefined || rawValue === "") {
    return null;
  }

  if (rawValue instanceof Date) {
    const timestamp = rawValue.getTime();
    return Number.isFinite(timestamp) ? timestamp : null;
  }

  const normalizedText = String(rawValue).trim();
  if (!normalizedText) {
    return null;
  }

  const timestamp = Date.parse(normalizedText);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function isRecordStateRevisionMatch(expectedUpdatedAt, currentUpdatedAt) {
  const expectedUpdatedAtMs = normalizeRecordStateTimestamp(expectedUpdatedAt);
  const currentUpdatedAtMs = normalizeRecordStateTimestamp(currentUpdatedAt);
  const expectsEmptyState = expectedUpdatedAt === null || expectedUpdatedAt === "";

  if (expectedUpdatedAtMs !== null) {
    return currentUpdatedAtMs !== null && currentUpdatedAtMs === expectedUpdatedAtMs;
  }

  return expectsEmptyState && currentUpdatedAtMs === null;
}

function applyRecordsPatchOperations(currentRecords, operations, options = {}) {
  const sourceRecords = Array.isArray(currentRecords) ? currentRecords : [];
  const patchOperations = Array.isArray(operations) ? operations : [];
  const nowIso = normalizeNowIso(options.nowIso);

  const nextRecords = sourceRecords.map((record) => {
    if (!record || typeof record !== "object" || Array.isArray(record)) {
      return {};
    }
    return { ...record };
  });

  let indexById = buildRecordIndexById(nextRecords);

  for (const operation of patchOperations) {
    const operationType = normalizeOperationType(operation?.type || operation?.op);
    const id = normalizeRecordId(operation?.id);
    if (!operationType || !id) {
      continue;
    }

    if (operationType === PATCH_OPERATION_DELETE) {
      const existingIndex = indexById.get(id);
      if (existingIndex === undefined) {
        continue;
      }
      nextRecords.splice(existingIndex, 1);
      indexById = buildRecordIndexById(nextRecords);
      continue;
    }

    const rawRecord = operation?.record && typeof operation.record === "object" && !Array.isArray(operation.record)
      ? operation.record
      : {};
    const existingIndex = indexById.get(id);

    const nextRecord =
      existingIndex === undefined
        ? { ...rawRecord, id }
        : { ...nextRecords[existingIndex], ...rawRecord, id };

    if (!normalizeRecordCreatedAt(nextRecord.createdAt)) {
      nextRecord.createdAt = nowIso;
    }

    if (existingIndex === undefined) {
      nextRecords.push(nextRecord);
      indexById.set(id, nextRecords.length - 1);
    } else {
      nextRecords[existingIndex] = nextRecord;
    }
  }

  return nextRecords;
}

function normalizeOperationType(rawType) {
  const normalized = String(rawType || "").trim().toLowerCase();
  if (normalized === PATCH_OPERATION_UPSERT || normalized === PATCH_OPERATION_DELETE) {
    return normalized;
  }
  return "";
}

function normalizeRecordId(rawValue) {
  const id = String(rawValue ?? "").trim();
  return id.slice(0, 180);
}

function normalizeRecordCreatedAt(rawValue) {
  const timestamp = normalizeRecordStateTimestamp(rawValue);
  return timestamp === null ? "" : new Date(timestamp).toISOString();
}

function normalizeNowIso(nowIso) {
  const timestamp = normalizeRecordStateTimestamp(nowIso);
  if (timestamp === null) {
    return new Date().toISOString();
  }
  return new Date(timestamp).toISOString();
}

function buildRecordIndexById(records) {
  const index = new Map();
  const items = Array.isArray(records) ? records : [];
  for (let recordIndex = 0; recordIndex < items.length; recordIndex += 1) {
    const id = normalizeRecordId(items[recordIndex]?.id);
    if (!id) {
      continue;
    }
    index.set(id, recordIndex);
  }
  return index;
}

module.exports = {
  PATCH_OPERATION_UPSERT,
  PATCH_OPERATION_DELETE,
  applyRecordsPatchOperations,
  isRecordStateRevisionMatch,
  normalizeRecordStateTimestamp,
};
