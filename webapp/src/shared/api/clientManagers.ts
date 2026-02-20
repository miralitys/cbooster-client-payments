import { apiRequest } from "@/shared/api/fetcher";
import type { ClientManagersPayload } from "@/shared/types/clientManagers";

export type ClientManagersRefreshMode = "none" | "incremental" | "full";

export async function getClientManagers(refresh: ClientManagersRefreshMode = "none"): Promise<ClientManagersPayload> {
  if (refresh !== "none") {
    return apiRequest<ClientManagersPayload>("/api/ghl/client-managers/refresh", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refresh,
      }),
    });
  }

  return apiRequest<ClientManagersPayload>("/api/ghl/client-managers");
}
