import { apiRequest } from "@/shared/api/fetcher";
import type { ClientManagersPayload } from "@/shared/types/clientManagers";

export type ClientManagersRefreshMode = "none" | "incremental" | "full";

interface GetClientManagersOptions {
  clientName?: string;
  clientNames?: string[];
}

export async function getClientManagers(
  refresh: ClientManagersRefreshMode = "none",
  options: GetClientManagersOptions = {},
): Promise<ClientManagersPayload> {
  if (refresh !== "none") {
    const clientName = (options.clientName || "").toString().trim();
    const clientNames = Array.isArray(options.clientNames)
      ? options.clientNames.map((value) => (value || "").toString().trim()).filter(Boolean)
      : [];
    return apiRequest<ClientManagersPayload>("/api/ghl/client-managers/refresh", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refresh,
        ...(clientName ? { clientName } : {}),
        ...(clientNames.length ? { clientNames } : {}),
      }),
    });
  }

  return apiRequest<ClientManagersPayload>("/api/ghl/client-managers");
}
