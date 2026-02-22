import { apiRequest } from "@/shared/api/fetcher";
import type {
  GhlClientCommunicationTranscriptPayload,
  GhlClientCommunicationsPayload,
} from "@/shared/types/ghlCommunications";

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

export async function postGhlClientCommunicationTranscript(
  clientName: string,
  messageId: string,
): Promise<GhlClientCommunicationTranscriptPayload> {
  const normalizedClientName = (clientName || "").toString().trim();
  const normalizedMessageId = (messageId || "").toString().trim();
  return apiRequest<GhlClientCommunicationTranscriptPayload>("/api/ghl/client-communications/transcript", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      clientName: normalizedClientName,
      messageId: normalizedMessageId,
    }),
  });
}
