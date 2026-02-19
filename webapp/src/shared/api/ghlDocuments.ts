import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientDocumentsPayload } from "@/shared/types/ghlDocuments";

export async function getGhlClientDocuments(limit = 10): Promise<GhlClientDocumentsPayload> {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(50, Math.trunc(limit))) : 10;
  const query = new URLSearchParams({
    limit: String(normalizedLimit),
  });
  return apiRequest<GhlClientDocumentsPayload>(`/api/ghl/client-contracts?${query.toString()}`);
}
