export type AssistantMode = "text" | "voice";

export interface AssistantChatRequest {
  message: string;
  mode?: AssistantMode;
  sessionId?: string;
}

export interface AssistantChatSource {
  recordsUsed: number;
  updatedAt: string | null;
  provider?: string;
}

export interface AssistantChatResponse {
  ok: boolean;
  reply: string;
  clientMentions?: string[];
  suggestions?: string[];
  source?: AssistantChatSource;
}

export interface AssistantContextResetResponse {
  ok: boolean;
}
