import { apiRequest } from "@/shared/api/fetcher";
import type { ClientManagersPayload } from "@/shared/types/clientManagers";

export type ClientManagersRefreshMode = "none" | "incremental" | "full";

export interface ClientManagersRefreshJob {
  id: string;
  status: string;
  done: boolean;
  requestedBy: string;
  totalClients: number;
  queuedAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  updatedAt: string | null;
  error: string | null;
  refresh?: {
    mode: string;
    performed: boolean;
    refreshedClientsCount: number;
    refreshedRowsWritten: number;
    deletedStaleRowsCount: number;
  } | null;
}

export interface StartClientManagersRefreshJobPayload {
  ok: boolean;
  reused: boolean;
  started: boolean;
  message?: string;
  job?: ClientManagersRefreshJob | null;
}

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
    const isScopedRefresh = Boolean(clientName || clientNames.length);
    const timeoutMs = refresh === "full" && !isScopedRefresh ? 120_000 : undefined;
    return apiRequest<ClientManagersPayload>("/api/ghl/client-managers/refresh", {
      method: "POST",
      ...(typeof timeoutMs === "number" ? { timeoutMs } : {}),
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

export async function startClientManagersRefreshBackgroundJob(): Promise<StartClientManagersRefreshJobPayload> {
  return apiRequest<StartClientManagersRefreshJobPayload>("/api/ghl/client-managers/refresh/background", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      refresh: "full",
    }),
  });
}
