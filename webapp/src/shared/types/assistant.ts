export type AssistantMode = "text" | "voice";

export interface AssistantChatRequest {
  message: string;
  mode?: AssistantMode;
}

export interface AssistantChatSource {
  recordsUsed: number;
  updatedAt: string | null;
}

export interface AssistantChatResponse {
  ok: boolean;
  reply: string;
  clientMentions?: string[];
  suggestions?: string[];
  source?: AssistantChatSource;
}
