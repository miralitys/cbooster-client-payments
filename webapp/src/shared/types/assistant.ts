export type AssistantMode = "text" | "voice";
export type AssistantScopeSource = "explicit" | "mention" | "none";

export interface AssistantChatRequest {
  message: string;
  mode?: AssistantMode;
  sessionId?: string;
  clientMessageSeq?: number;
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
  scope_source?: AssistantScopeSource;
  suggestions?: string[];
  source?: AssistantChatSource;
}

export interface AssistantContextResetResponse {
  ok: boolean;
}
