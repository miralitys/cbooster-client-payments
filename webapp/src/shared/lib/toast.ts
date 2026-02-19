export type ToastType = "info" | "success" | "error";

export interface ToastAction {
  label: string;
  onClick: () => void;
}

export interface ToastPayload {
  type?: ToastType;
  message: string;
  dedupeKey?: string;
  cooldownMs?: number;
  durationMs?: number;
  action?: ToastAction;
}

export interface ToastMessage extends ToastPayload {
  id: string;
  type: ToastType;
}

type ToastListener = (message: ToastMessage) => void;

const DEFAULT_COOLDOWN_MS = 2500;
const DEFAULT_DURATION_MS = 3800;
const listeners = new Set<ToastListener>();
const dedupeCache = new Map<string, number>();
let activeHostId: symbol | null = null;
let sequence = 0;

export function showToast(payload: ToastPayload): void {
  const message = String(payload.message || "").trim();
  if (!message) {
    return;
  }

  const dedupeKey = payload.dedupeKey?.trim();
  const cooldownMs = Number.isFinite(payload.cooldownMs) ? Math.max(0, Number(payload.cooldownMs)) : DEFAULT_COOLDOWN_MS;
  const canDedupe = Boolean(dedupeKey) && !(payload.type === "error" && payload.action);
  if (canDedupe && dedupeKey) {
    const now = Date.now();
    const lastShown = dedupeCache.get(dedupeKey) || 0;
    if (now - lastShown < cooldownMs) {
      return;
    }
    dedupeCache.set(dedupeKey, now);
  }

  sequence += 1;
  const toast: ToastMessage = {
    id: `toast-${Date.now()}-${sequence}`,
    type: payload.type || "info",
    message,
    durationMs: Number.isFinite(payload.durationMs) ? Math.max(0, Number(payload.durationMs)) : DEFAULT_DURATION_MS,
    action: payload.action,
    dedupeKey,
    cooldownMs,
  };

  for (const listener of listeners) {
    listener(toast);
  }
}

export function acquireToastHost(id: symbol): boolean {
  if (activeHostId && activeHostId !== id) {
    return false;
  }
  activeHostId = id;
  return true;
}

export function releaseToastHost(id: symbol): void {
  if (activeHostId === id) {
    activeHostId = null;
  }
}

export function subscribeToasts(listener: ToastListener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
