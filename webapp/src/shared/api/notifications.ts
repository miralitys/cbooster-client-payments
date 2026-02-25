import { apiRequest } from "@/shared/api/fetcher";
import type { AppNotification, AppNotificationTone } from "@/shared/types/notifications";

interface NotificationsFeedPayload {
  ok: boolean;
  items?: unknown[];
}

interface NotificationWritePayload {
  ok: boolean;
}

interface PushPublicKeyPayload {
  ok: boolean;
  enabled?: unknown;
  publicKey?: unknown;
}

interface PushSubscribePayload {
  ok: boolean;
  subscribed?: unknown;
}

interface PushUnsubscribePayload {
  ok: boolean;
  unsubscribed?: unknown;
}

export interface PushPublicKeyConfig {
  enabled: boolean;
  publicKey: string;
}

export interface PushSubscriptionPayload {
  endpoint: string;
  expirationTime?: number | null;
  keys: {
    p256dh: string;
    auth: string;
  };
}

const FALLBACK_LINK_LABEL = "Open";

export async function getNotificationsFeed(): Promise<AppNotification[]> {
  const payload = await apiRequest<NotificationsFeedPayload>("/api/notifications");
  const items = Array.isArray(payload?.items) ? payload.items : [];
  return items
    .map((item) => normalizeNotification(item))
    .filter((item): item is AppNotification => Boolean(item));
}

export async function markNotificationRead(id: string): Promise<void> {
  const normalizedId = String(id || "").trim();
  if (!normalizedId) {
    return;
  }

  await apiRequest<NotificationWritePayload>(`/api/notifications/${encodeURIComponent(normalizedId)}/read`, {
    method: "POST",
  });
}

export async function markAllNotificationsRead(): Promise<void> {
  await apiRequest<NotificationWritePayload>("/api/notifications/read-all", {
    method: "POST",
  });
}

export async function getNotificationsPushPublicKey(): Promise<PushPublicKeyConfig> {
  const payload = await apiRequest<PushPublicKeyPayload>("/api/notifications/push/public-key");
  const enabled = payload?.enabled === true;
  const publicKey = enabled ? String(payload?.publicKey || "").trim() : "";
  return {
    enabled: enabled && Boolean(publicKey),
    publicKey,
  };
}

export async function subscribeNotificationsPush(subscription: PushSubscriptionPayload): Promise<boolean> {
  const normalizedSubscription = normalizePushSubscriptionPayload(subscription);
  if (!normalizedSubscription) {
    return false;
  }

  const payload = await apiRequest<PushSubscribePayload>("/api/notifications/push/subscribe", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      subscription: normalizedSubscription,
    }),
  });
  return payload?.subscribed === true;
}

export async function unsubscribeNotificationsPush(endpoint: string): Promise<boolean> {
  const normalizedEndpoint = String(endpoint || "").trim();
  if (!normalizedEndpoint) {
    return false;
  }

  const payload = await apiRequest<PushUnsubscribePayload>("/api/notifications/push/unsubscribe", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      endpoint: normalizedEndpoint,
    }),
  });
  return payload?.unsubscribed === true;
}

function normalizeNotification(rawValue: unknown): AppNotification | null {
  if (!rawValue || typeof rawValue !== "object") {
    return null;
  }

  const rawNotification = rawValue as Partial<AppNotification>;
  const id = String(rawNotification.id || "").trim();
  const title = String(rawNotification.title || "").trim();
  if (!id || !title) {
    return null;
  }

  const message = String(rawNotification.message || "").trim();
  const clientName = String(rawNotification.clientName || "").trim();
  const createdAt = normalizeCreatedAt(rawNotification.createdAt);
  const tone = normalizeTone(rawNotification.tone);

  const linkHref = String(rawNotification.link?.href || "").trim();
  const linkLabel = String(rawNotification.link?.label || "").trim();

  return {
    id,
    title,
    message: message || undefined,
    tone,
    createdAt,
    read: rawNotification.read === true,
    clientName: clientName || undefined,
    link: linkHref
      ? {
          href: linkHref,
          label: linkLabel || FALLBACK_LINK_LABEL,
        }
      : undefined,
  };
}

function normalizeCreatedAt(rawValue: unknown): string {
  const value = String(rawValue || "").trim();
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return new Date().toISOString();
  }

  return new Date(timestamp).toISOString();
}

function normalizeTone(rawValue: unknown): AppNotificationTone {
  const value = String(rawValue || "").trim().toLowerCase();
  if (value === "success" || value === "warning" || value === "error" || value === "info") {
    return value;
  }

  return "info";
}

function normalizePushSubscriptionPayload(rawValue: unknown): PushSubscriptionPayload | null {
  if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
    return null;
  }

  const endpoint = String((rawValue as { endpoint?: unknown }).endpoint || "").trim();
  const keys = (rawValue as { keys?: unknown }).keys;
  const p256dh = String((keys as { p256dh?: unknown } | undefined)?.p256dh || "").trim();
  const auth = String((keys as { auth?: unknown } | undefined)?.auth || "").trim();
  if (!endpoint || !p256dh || !auth) {
    return null;
  }

  const expirationTimeRaw = Number((rawValue as { expirationTime?: unknown }).expirationTime);
  return {
    endpoint,
    expirationTime: Number.isFinite(expirationTimeRaw) ? Math.trunc(expirationTimeRaw) : null,
    keys: {
      p256dh,
      auth,
    },
  };
}
