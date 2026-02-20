import { apiRequest } from "@/shared/api/fetcher";
import type {
  AssistantChatRequest,
  AssistantChatResponse,
  AssistantContextResetResponse,
  AssistantMode,
} from "@/shared/types/assistant";

export async function sendAssistantMessage(
  message: string,
  mode: AssistantMode = "text",
  signal?: AbortSignal,
  sessionId?: string,
): Promise<AssistantChatResponse> {
  const payload: AssistantChatRequest = {
    message,
    mode,
  };
  if (sessionId) {
    payload.sessionId = sessionId;
  }

  return apiRequest<AssistantChatResponse>("/api/assistant/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    signal,
  });
}

export async function resetAssistantSessionContext(
  sessionId?: string,
  signal?: AbortSignal,
): Promise<AssistantContextResetResponse> {
  const payload: { sessionId?: string } = {};
  if (sessionId) {
    payload.sessionId = sessionId;
  }

  return apiRequest<AssistantContextResetResponse>("/api/assistant/context/reset", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    signal,
  });
}
