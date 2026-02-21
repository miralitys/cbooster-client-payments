import { apiRequest } from "@/shared/api/fetcher";
import type {
  AssistantChatRequest,
  AssistantChatResponse,
  AssistantContextResetResponse,
  AssistantMode,
} from "@/shared/types/assistant";

const ASSISTANT_CONTEXT_RESET_PATH = "/api/assistant/context/reset";
const WEB_CSRF_COOKIE_NAME = "cbooster_auth_csrf";

interface AssistantContextResetOptions {
  signal?: AbortSignal;
  keepalive?: boolean;
  timeoutMs?: number;
}

export async function sendAssistantMessage(
  message: string,
  mode: AssistantMode = "text",
  signal?: AbortSignal,
  sessionId?: string,
  clientMessageSeq?: number,
): Promise<AssistantChatResponse> {
  const payload: AssistantChatRequest = {
    message,
    mode,
  };
  if (sessionId) {
    payload.sessionId = sessionId;
  }
  if (Number.isFinite(clientMessageSeq) && Number(clientMessageSeq) > 0) {
    payload.clientMessageSeq = Math.min(Number(clientMessageSeq), Number.MAX_SAFE_INTEGER);
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
  options: AssistantContextResetOptions = {},
): Promise<AssistantContextResetResponse> {
  const payload: { sessionId?: string } = {};
  if (sessionId) {
    payload.sessionId = sessionId;
  }

  return apiRequest<AssistantContextResetResponse>(ASSISTANT_CONTEXT_RESET_PATH, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    signal: options.signal,
    keepalive: options.keepalive,
    timeoutMs: options.timeoutMs,
  });
}

export function queueAssistantSessionContextResetBeacon(sessionId?: string): boolean {
  if (typeof navigator === "undefined" || typeof navigator.sendBeacon !== "function") {
    return false;
  }

  const params = new URLSearchParams();
  if (sessionId) {
    params.set("sessionId", sessionId);
  }

  const csrfToken = readCookieValue(WEB_CSRF_COOKIE_NAME);
  if (csrfToken) {
    params.set("_csrf", csrfToken);
  }

  try {
    return navigator.sendBeacon(ASSISTANT_CONTEXT_RESET_PATH, params);
  } catch {
    return false;
  }
}

function readCookieValue(cookieName: string): string {
  if (typeof document === "undefined") {
    return "";
  }

  const rawCookie = String(document.cookie || "");
  if (!rawCookie) {
    return "";
  }

  const chunks = rawCookie.split(";");
  for (const chunk of chunks) {
    const [rawKey, ...rawValueParts] = chunk.split("=");
    if ((rawKey || "").trim() !== cookieName) {
      continue;
    }

    const rawValue = rawValueParts.join("=").trim();
    if (!rawValue) {
      return "";
    }

    try {
      return decodeURIComponent(rawValue);
    } catch {
      return rawValue;
    }
  }

  return "";
}
