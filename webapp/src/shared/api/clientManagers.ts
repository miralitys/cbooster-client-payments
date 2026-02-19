import { apiRequest } from "@/shared/api/fetcher";
import type { ClientManagersPayload } from "@/shared/types/clientManagers";

export type ClientManagersRefreshMode = "none" | "incremental" | "full";

export async function getClientManagers(refresh: ClientManagersRefreshMode = "none"): Promise<ClientManagersPayload> {
  const query = new URLSearchParams();
  if (refresh !== "none") {
    query.set("refresh", refresh);
  }

  const suffix = query.toString();
  return apiRequest<ClientManagersPayload>(`/api/ghl/client-managers${suffix ? `?${suffix}` : ""}`);
}
