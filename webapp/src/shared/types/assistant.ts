export type AssistantMode = "text" | "voice";
export type AssistantScopeSource = "explicit" | "mention" | "none";
export type AssistantScopeEphemeralSource = "mention" | "none";

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
  degradedMode?: boolean;
  degradedReason?: string;
  staleSnapshotAgeMs?: number;
}

export interface AssistantChatResponse {
  ok: boolean;
  reply: string;
  clientMentions?: string[];
  scope_source?: AssistantScopeSource;
  scope_persisted?: boolean;
  scope_ephemeral_source?: AssistantScopeEphemeralSource;
  suggestions?: string[];
  source?: AssistantChatSource;
}

export interface AssistantContextResetResponse {
  ok: boolean;
}
