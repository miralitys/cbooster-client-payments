import { apiRequest } from "@/shared/api/fetcher";
import type { GhlLeadsPayload } from "@/shared/types/ghlLeads";

export type GhlLeadsRefreshMode = "none" | "incremental" | "full";
export type GhlLeadsRangeMode = "today" | "week" | "month" | "all";

interface GhlLeadsRequestOptions {
  rangeMode?: GhlLeadsRangeMode;
  todayOnly?: boolean;
}

export async function getGhlLeads(
  refresh: GhlLeadsRefreshMode = "none",
  options: GhlLeadsRequestOptions = {},
): Promise<GhlLeadsPayload> {
  const rangeMode = options.rangeMode || (options.todayOnly !== false ? "today" : "all");
  const todayOnly = rangeMode === "today";
  if (refresh !== "none") {
    return apiRequest<GhlLeadsPayload>("/api/ghl/leads/refresh", {
      method: "POST",
      timeoutMs: 90_000,
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refresh,
        rangeMode,
        todayOnly,
      }),
    });
  }

  const query = new URLSearchParams();
  query.set("rangeMode", rangeMode);
  query.set("todayOnly", todayOnly ? "1" : "0");

  return apiRequest<GhlLeadsPayload>(`/api/ghl/leads?${query.toString()}`, {
    timeoutMs: 60_000,
  });
}
