import { apiRequest } from "@/shared/api/fetcher";
import type { ClientRecord, RecordsPayload } from "@/shared/types/records";

export async function getRecords(): Promise<ClientRecord[]> {
  const payload = await apiRequest<RecordsPayload>("/api/records");
  return Array.isArray(payload.records) ? payload.records : [];
}

export async function putRecords(records: ClientRecord[]): Promise<void> {
  await apiRequest<{ ok: boolean; updatedAt?: string }>("/api/records", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ records }),
  });
}
