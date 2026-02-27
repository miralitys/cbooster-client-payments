import { apiRequest } from "@/shared/api/fetcher";
import type { ClientHealthPayload } from "@/shared/types/clientHealth";

export async function getClientHealth(): Promise<ClientHealthPayload> {
  const payload = await apiRequest<ClientHealthPayload>("/api/client-health");

  return {
    records: Array.isArray(payload.records) ? payload.records : [],
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
    limit:
      typeof payload.limit === "number" && Number.isFinite(payload.limit)
        ? Math.max(1, Math.trunc(payload.limit))
        : 5,
    safeMode: payload.safeMode !== false,
    source: typeof payload.source === "string" ? payload.source : "",
    sampleMode: typeof payload.sampleMode === "string" ? payload.sampleMode : "",
  };
}
