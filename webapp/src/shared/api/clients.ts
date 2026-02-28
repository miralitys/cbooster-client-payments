import { apiRequest } from "@/shared/api/fetcher";
import type {
  ClientsFilterOptionsPayload,
  ClientsTotalsPayload,
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

export async function getClientsPage(
  limit: number,
  offset: number,
  query: Record<string, string | number | boolean | null | undefined> = {},
): Promise<RecordsPayload> {
  const safeLimit = clampInteger(limit, 1, 500);
  const safeOffset = Math.max(0, Number.isFinite(offset) ? Math.trunc(offset) : 0);
  const params = new URLSearchParams();
  params.set("limit", String(safeLimit));
  params.set("offset", String(safeOffset));
  for (const [key, rawValue] of Object.entries(query)) {
    if (rawValue === null || rawValue === undefined) {
      continue;
    }
    const value = String(rawValue).trim();
    if (!value) {
      continue;
    }
    params.set(key, value);
  }
  const payload = await apiRequest<RecordsPayload>(`/api/clients?${params.toString()}`);
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

export async function getClientFilterOptions(): Promise<ClientsFilterOptionsPayload> {
  const payload = await apiRequest<ClientsFilterOptionsPayload>("/api/clients/filters");
  return {
    closedByOptions: Array.isArray(payload.closedByOptions)
      ? payload.closedByOptions.filter((item) => typeof item === "string" && item.trim())
      : [],
    clientManagerOptions: Array.isArray(payload.clientManagerOptions)
      ? payload.clientManagerOptions.filter((item) => typeof item === "string" && item.trim())
      : [],
  };
}

export async function getClientsTotals(
  query: Record<string, string | number | boolean | null | undefined> = {},
): Promise<ClientsTotalsPayload> {
  const params = buildClientsQueryParams(query);
  const path = params.toString() ? `/api/clients/totals?${params.toString()}` : "/api/clients/totals";
  const payload = await apiRequest<ClientsTotalsPayload>(path);
  return {
    totalsCents: {
      contractTotals: normalizeFiniteNumber(payload?.totalsCents?.contractTotals),
      totalPayments: normalizeFiniteNumber(payload?.totalsCents?.totalPayments),
      futurePayments: normalizeFiniteNumber(payload?.totalsCents?.futurePayments),
      collection: normalizeFiniteNumber(payload?.totalsCents?.collection),
    },
    rowCount: Math.max(0, Math.trunc(normalizeFiniteNumber(payload?.rowCount))),
    invalidFieldsCount: Math.max(0, Math.trunc(normalizeFiniteNumber(payload?.invalidFieldsCount))),
    source: typeof payload?.source === "string" ? payload.source : undefined,
    updatedAt: typeof payload?.updatedAt === "string" ? payload.updatedAt : null,
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

function normalizeFiniteNumber(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
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

function buildClientsQueryParams(
  query: Record<string, string | number | boolean | null | undefined>,
): URLSearchParams {
  const params = new URLSearchParams();
  for (const [key, rawValue] of Object.entries(query || {})) {
    if (rawValue === null || rawValue === undefined) {
      continue;
    }
    const value = String(rawValue).trim();
    if (!value) {
      continue;
    }
    params.set(key, value);
  }
  return params;
}
