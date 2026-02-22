import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientCommunicationsPayload } from "@/shared/types/ghlCommunications";

export async function getGhlClientCommunications(
  clientName: string,
  options: { signal?: AbortSignal } = {},
): Promise<GhlClientCommunicationsPayload> {
  const normalizedClientName = (clientName || "").toString().trim();
  const query = new URLSearchParams({
    clientName: normalizedClientName,
  });

  return apiRequest<GhlClientCommunicationsPayload>(`/api/ghl/client-communications?${query.toString()}`, {
    signal: options.signal,
  });
}
