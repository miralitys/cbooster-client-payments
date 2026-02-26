import { apiRequest } from "@/shared/api/fetcher";
import type {
  ClientRecord,
  PatchRecordsPayload,
  PutRecordsPayload,
  RecordsPatchOperation,
  RecordsPayload,
} from "@/shared/types/records";

export async function getClients(): Promise<RecordsPayload> {
  const payload = await apiRequest<RecordsPayload>("/api/clients");
  return {
    records: Array.isArray(payload.records) ? payload.records : [],
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
    total: normalizeOptionalNumber(payload.total),
    limit: normalizeOptionalNumber(payload.limit),
    offset: normalizeOptionalNumber(payload.offset),
    hasMore: typeof payload.hasMore === "boolean" ? payload.hasMore : undefined,
    nextOffset: normalizeOptionalNumberOrNull(payload.nextOffset),
  };
}

export async function getClientsPage(limit: number, offset: number): Promise<RecordsPayload> {
  const safeLimit = clampInteger(limit, 1, 500);
  const safeOffset = Math.max(0, Number.isFinite(offset) ? Math.trunc(offset) : 0);
  const payload = await apiRequest<RecordsPayload>(`/api/clients?limit=${safeLimit}&offset=${safeOffset}`);
  return {
    records: Array.isArray(payload.records) ? payload.records : [],
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
    total: normalizeOptionalNumber(payload.total),
    limit: normalizeOptionalNumber(payload.limit),
    offset: normalizeOptionalNumber(payload.offset),
    hasMore: typeof payload.hasMore === "boolean" ? payload.hasMore : undefined,
    nextOffset: normalizeOptionalNumberOrNull(payload.nextOffset),
  };
}

export async function putClients(records: ClientRecord[], expectedUpdatedAt: string | null): Promise<PutRecordsPayload> {
  const payload = await apiRequest<PutRecordsPayload>("/api/clients", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      records,
      expectedUpdatedAt: expectedUpdatedAt || null,
    }),
  });

  return {
    ok: Boolean(payload.ok),
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
  };
}

export async function patchClients(
  operations: RecordsPatchOperation[],
  expectedUpdatedAt: string | null,
): Promise<PatchRecordsPayload> {
  const payload = await apiRequest<PatchRecordsPayload>("/api/clients", {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      operations,
      expectedUpdatedAt: expectedUpdatedAt || null,
    }),
  });

  return {
    ok: Boolean(payload.ok),
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
    appliedOperations:
      typeof payload.appliedOperations === "number" && Number.isFinite(payload.appliedOperations)
        ? payload.appliedOperations
        : undefined,
  };
}

function normalizeOptionalNumber(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }
  return Math.trunc(value);
}

function normalizeOptionalNumberOrNull(value: unknown): number | null | undefined {
  if (value === null) {
    return null;
  }
  const normalized = normalizeOptionalNumber(value);
  return normalized === undefined ? undefined : normalized;
}

function clampInteger(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) {
    return min;
  }
  const normalized = Math.trunc(value);
  return Math.max(min, Math.min(max, normalized));
}
