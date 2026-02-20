import { apiRequest } from "@/shared/api/fetcher";
import type { GhlLeadsPayload } from "@/shared/types/ghlLeads";

export type GhlLeadsRefreshMode = "none" | "incremental" | "full";

interface GhlLeadsRequestOptions {
  todayOnly?: boolean;
}

export async function getGhlLeads(
  refresh: GhlLeadsRefreshMode = "none",
  options: GhlLeadsRequestOptions = {},
): Promise<GhlLeadsPayload> {
  const todayOnly = options.todayOnly !== false;
  if (refresh !== "none") {
    return apiRequest<GhlLeadsPayload>("/api/ghl/leads/refresh", {
      method: "POST",
      timeoutMs: 60_000,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refresh,
        todayOnly,
      }),
    });
  }

  const query = new URLSearchParams();
  query.set("todayOnly", todayOnly ? "1" : "0");

  return apiRequest<GhlLeadsPayload>(`/api/ghl/leads?${query.toString()}`, {
    timeoutMs: 60_000,
  });
}
