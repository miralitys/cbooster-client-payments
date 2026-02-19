import { apiRequest } from "@/shared/api/fetcher";
import type { AssistantChatRequest, AssistantChatResponse, AssistantMode } from "@/shared/types/assistant";

export async function sendAssistantMessage(
  message: string,
  mode: AssistantMode = "text",
  signal?: AbortSignal,
): Promise<AssistantChatResponse> {
  const payload: AssistantChatRequest = {
    message,
    mode,
  };

  return apiRequest<AssistantChatResponse>("/api/assistant/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    signal,
  });
}
