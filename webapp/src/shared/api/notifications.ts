import { apiRequest } from "@/shared/api/fetcher";
import type { AppNotification, AppNotificationTone } from "@/shared/types/notifications";

interface NotificationsFeedPayload {
  ok: boolean;
  items?: unknown[];
}

interface NotificationWritePayload {
  ok: boolean;
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
