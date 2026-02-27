import { ApiError } from "@/shared/api/fetcher";
import type { ClientRecord, RecordsPatchOperation } from "@/shared/types/records";
import type { Session } from "@/shared/types/session";

const SESSION_PATCH_FLAG_KEYS = ["RECORDS_PATCH", "records_patch", "recordsPatch"] as const;

export function buildRecordsPatchOperations(
  previousRecords: ClientRecord[],
  nextRecords: ClientRecord[],
): RecordsPatchOperation[] {
  const previousById = new Map<string, ClientRecord>();
  for (const record of previousRecords) {
    const id = normalizeRecordId(record.id);
    if (id) {
      previousById.set(id, record);
    }
  }

  const nextById = new Map<string, ClientRecord>();
  for (const record of nextRecords) {
    const id = normalizeRecordId(record.id);
    if (id) {
      nextById.set(id, record);
    }
  }

  const operations: RecordsPatchOperation[] = [];

  for (const record of nextRecords) {
    const id = normalizeRecordId(record.id);
    if (!id) {
      continue;
    }

    const previous = previousById.get(id);
    if (!previous) {
      operations.push({
        type: "upsert",
        id,
        record: { ...record },
      });
      continue;
    }

    const changedFields = pickChangedRecordFields(previous, record);
    if (Object.keys(changedFields).length === 0) {
      continue;
    }

    operations.push({
      type: "upsert",
      id,
      record: changedFields,
    });
  }

  for (const record of previousRecords) {
    const id = normalizeRecordId(record.id);
    if (!id || nextById.has(id)) {
      continue;
    }

    operations.push({
      type: "delete",
      id,
    });
  }

  return operations;
}

export function shouldFallbackToPutFromPatch(error: unknown): boolean {
  if (!(error instanceof ApiError)) {
    return false;
  }

  if (error.code === "records_patch_disabled") {
    return true;
  }

  if (error.status === 404 || error.status === 405 || error.status === 501) {
    return true;
  }

  if (error.status !== 400) {
    return false;
  }

  const normalizedMessage = (error.message || "").toLowerCase();
  return normalizedMessage.includes("patch") && normalizedMessage.includes("disable");
}

export function resolveRecordsPatchEnabled(session: Session | null): boolean {
  const sessionFlag = readSessionPatchFlag(session);
  if (sessionFlag !== null) {
    return sessionFlag;
  }

  const rawEnvFlag = import.meta.env.VITE_RECORDS_PATCH;
  if (rawEnvFlag === undefined || rawEnvFlag === null) {
    return true;
  }

  if (typeof rawEnvFlag === "string" && !rawEnvFlag.trim()) {
    return true;
  }

  return parseBooleanLike(rawEnvFlag);
}

function pickChangedRecordFields(previous: ClientRecord, next: ClientRecord): Partial<ClientRecord> {
  const changed: Partial<ClientRecord> = {};
  const allKeys = new Set<string>([...Object.keys(previous), ...Object.keys(next)]);

  for (const key of allKeys) {
    if (key === "id") {
      continue;
    }

    const typedKey = key as keyof ClientRecord;
    const previousValue = normalizeFieldValue(previous[typedKey]);
    const nextValue = normalizeFieldValue(next[typedKey]);
    if (previousValue === nextValue) {
      continue;
    }

    changed[typedKey] = next[typedKey];
  }

  return changed;
}

function normalizeFieldValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value);
}

function normalizeRecordId(rawId: unknown): string {
  if (typeof rawId !== "string") {
    return "";
  }

  return rawId.trim();
}

function parseBooleanLike(rawValue: unknown): boolean {
  if (typeof rawValue === "boolean") {
    return rawValue;
  }

  if (typeof rawValue === "number") {
    return Number.isFinite(rawValue) && rawValue !== 0;
  }

  if (typeof rawValue !== "string") {
    return false;
  }

  const normalized = rawValue.trim().toLowerCase();
  if (!normalized) {
    return false;
  }

  return normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on";
}

function readSessionPatchFlag(session: Session | null): boolean | null {
  const featureFlags = session?.featureFlags;
  if (!featureFlags || typeof featureFlags !== "object") {
    return null;
  }

  for (const key of SESSION_PATCH_FLAG_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(featureFlags, key)) {
      continue;
    }

    return parseBooleanLike(featureFlags[key]);
  }

  return null;
}
