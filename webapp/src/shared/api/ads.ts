import { apiRequest } from "@/shared/api/fetcher";
import type { MetaAdsOverviewPayload } from "@/shared/types/ads";

interface GetAdsOverviewOptions {
  since?: string;
  until?: string;
}

export async function getAdsOverview(options: GetAdsOverviewOptions = {}): Promise<MetaAdsOverviewPayload> {
  const query = new URLSearchParams();
  const since = String(options.since || "").trim();
  const until = String(options.until || "").trim();

  if (since) {
    query.set("since", since);
  }
  if (until) {
    query.set("until", until);
  }

  const suffix = query.toString() ? `?${query.toString()}` : "";
  return apiRequest<MetaAdsOverviewPayload>(`/api/ads/overview${suffix}`);
}
