import { apiRequest } from "@/shared/api/fetcher";
import type { ClientManagerKpiPayload } from "@/shared/types/clientManagerKpi";

export async function getClientManagerKpi(): Promise<ClientManagerKpiPayload> {
  const payload = await apiRequest<ClientManagerKpiPayload>("/api/clients/kpi-client-manager");
  return {
    month: typeof payload.month === "string" ? payload.month : "",
    rows: Array.isArray(payload.rows) ? payload.rows : [],
    updatedAt: typeof payload.updatedAt === "string" ? payload.updatedAt : null,
  };
}
