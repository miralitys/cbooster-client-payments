export interface OpenClientCardEventDetail {
  clientName?: string;
}

interface PendingOpenClientCardRequest {
  clientName: string;
  requestedAt: number;
}

export const OPEN_CLIENT_CARD_EVENT_NAME = "cb-assistant-open-client";

const OPEN_CLIENT_CARD_STORAGE_KEY = "cbooster_open_client_card_request_v1";
const OPEN_CLIENT_CARD_MAX_AGE_MS = 5 * 60 * 1000;

export function requestOpenClientCard(clientName: string, options: { fallbackHref?: string } = {}): void {
  const normalizedClientName = String(clientName || "").trim();
  if (!normalizedClientName || typeof window === "undefined") {
    return;
  }

  queuePendingOpenClientCardRequest(normalizedClientName);
  window.dispatchEvent(new CustomEvent<OpenClientCardEventDetail>(OPEN_CLIENT_CARD_EVENT_NAME, { detail: { clientName: normalizedClientName } }));

  if (!isClientPaymentsPath(window.location.pathname)) {
    window.location.assign(resolveFallbackHref(options.fallbackHref));
  }
}

export function consumePendingOpenClientCardRequest(): string {
  if (typeof window === "undefined") {
    return "";
  }

  try {
    const rawValue = window.sessionStorage.getItem(OPEN_CLIENT_CARD_STORAGE_KEY);
    window.sessionStorage.removeItem(OPEN_CLIENT_CARD_STORAGE_KEY);
    if (!rawValue) {
      return "";
    }

    const parsed = JSON.parse(rawValue) as Partial<PendingOpenClientCardRequest>;
    const clientName = String(parsed.clientName || "").trim();
    const requestedAt = Number(parsed.requestedAt);
    if (!clientName || !Number.isFinite(requestedAt)) {
      return "";
    }

    if (Date.now() - requestedAt > OPEN_CLIENT_CARD_MAX_AGE_MS) {
      return "";
    }

    return clientName;
  } catch {
    return "";
  }
}

function queuePendingOpenClientCardRequest(clientName: string): void {
  const payload: PendingOpenClientCardRequest = {
    clientName,
    requestedAt: Date.now(),
  };

  window.sessionStorage.setItem(OPEN_CLIENT_CARD_STORAGE_KEY, JSON.stringify(payload));
}

function isClientPaymentsPath(pathname: string): boolean {
  return /\/client-payments(?:\/|$)/.test(String(pathname || ""));
}

function resolveFallbackHref(rawHref: string | undefined): string {
  const href = String(rawHref || "").trim();
  if (href.startsWith("/")) {
    return href;
  }
  return "/app/client-payments";
}
