import { apiRequest } from "@/shared/api/fetcher";
import type { ClientRecord, PutRecordsPayload, RecordsPayload } from "@/shared/types/records";

export async function getRecords(): Promise<RecordsPayload> {
  const payload = await apiRequest<RecordsPayload>("/api/records");
  return {
    records: Array.isArray(payload.records) ? payload.records : [],
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
  };
}

export async function putRecords(records: ClientRecord[], expectedUpdatedAt: string | null): Promise<PutRecordsPayload> {
  const payload = await apiRequest<PutRecordsPayload>("/api/records", {
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
