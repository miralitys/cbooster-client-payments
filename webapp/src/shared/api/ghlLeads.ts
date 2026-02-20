import { apiRequest } from "@/shared/api/fetcher";
import type { GhlLeadsPayload } from "@/shared/types/ghlLeads";

export type GhlLeadsRefreshMode = "none" | "incremental" | "full";

export async function getGhlLeads(refresh: GhlLeadsRefreshMode = "none"): Promise<GhlLeadsPayload> {
  if (refresh !== "none") {
    return apiRequest<GhlLeadsPayload>("/api/ghl/leads/refresh", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        refresh,
      }),
    });
  }

  return apiRequest<GhlLeadsPayload>("/api/ghl/leads");
}
