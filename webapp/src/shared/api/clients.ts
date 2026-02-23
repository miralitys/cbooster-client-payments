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
