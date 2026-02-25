import type {
  AppNotification,
  AppNotificationLink,
  AppNotificationPayload,
  AppNotificationTone,
} from "@/shared/types/notifications";

type NotificationsListener = (notifications: AppNotification[]) => void;

const STORAGE_KEY = "cbooster_notifications_v1";
const MAX_STORED_NOTIFICATIONS = 60;
const listeners = new Set<NotificationsListener>();
let cache: AppNotification[] = [];
let cacheLoaded = false;
let storageSyncAttached = false;
let sequence = 0;

export function getNotifications(): AppNotification[] {
  ensureCacheLoaded();
  return cloneNotifications(cache);
}

export function subscribeNotifications(listener: NotificationsListener): () => void {
  ensureCacheLoaded();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function pushNotification(payload: AppNotificationPayload): AppNotification | null {
  ensureCacheLoaded();

  const title = String(payload.title || "").trim();
  if (!title) {
    return null;
  }

  const message = String(payload.message || "").trim();
  const notification: AppNotification = {
    id: createNotificationId(),
    title,
    message: message || undefined,
    tone: isAppNotificationTone(payload.tone) ? payload.tone : "info",
    createdAt: new Date().toISOString(),
    read: false,
    link: normalizeLink(payload.link),
  };

  cache = sanitizeNotifications([notification, ...cache]);
  persistCache();
  emitUpdate();
  return notification;
}

export function markNotificationRead(id: string): void {
  ensureCacheLoaded();
  if (!id) {
    return;
  }

  let changed = false;
  cache = cache.map((item) => {
    if (item.id !== id || item.read) {
      return item;
    }
    changed = true;
    return { ...item, read: true };
  });

  if (!changed) {
    return;
  }

  persistCache();
  emitUpdate();
}

export function markAllNotificationsRead(): void {
  ensureCacheLoaded();
  if (!cache.length) {
    return;
  }

  let changed = false;
  cache = cache.map((item) => {
    if (item.read) {
      return item;
    }
    changed = true;
    return { ...item, read: true };
  });

  if (!changed) {
    return;
  }

  persistCache();
  emitUpdate();
}

export function dismissNotification(id: string): void {
  ensureCacheLoaded();
  if (!id) {
    return;
  }

  const next = cache.filter((item) => item.id !== id);
  if (next.length === cache.length) {
    return;
  }

  cache = next;
  persistCache();
  emitUpdate();
}

export function clearNotifications(): void {
  ensureCacheLoaded();
  if (!cache.length) {
    return;
  }

  cache = [];
  persistCache();
  emitUpdate();
}

export function getUnreadNotificationsCount(notifications: AppNotification[]): number {
  let unreadCount = 0;
  for (const notification of notifications) {
    if (!notification.read) {
      unreadCount += 1;
    }
  }
  return unreadCount;
}

function ensureCacheLoaded(): void {
  if (cacheLoaded) {
    return;
  }

  cacheLoaded = true;
  cache = readFromStorage();
  attachStorageSync();
}

function attachStorageSync(): void {
  if (storageSyncAttached || typeof window === "undefined") {
    return;
  }

  storageSyncAttached = true;
  window.addEventListener("storage", (event) => {
    if (event.key !== STORAGE_KEY) {
      return;
    }
    cache = readFromStorage();
    emitUpdate();
  });
}

function emitUpdate(): void {
  const snapshot = cloneNotifications(cache);
  for (const listener of listeners) {
    listener(snapshot);
  }
}

function persistCache(): void {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(cache));
}

function readFromStorage(): AppNotification[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return [];
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    const normalized = parsed
      .map((item) => normalizeNotification(item))
      .filter((item): item is AppNotification => Boolean(item));
    return sanitizeNotifications(normalized);
  } catch {
    return [];
  }
}

function normalizeNotification(value: unknown): AppNotification | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const parsed = value as Partial<AppNotification>;
  const title = typeof parsed.title === "string" ? parsed.title.trim() : "";
  if (!title) {
    return null;
  }

  const message = typeof parsed.message === "string" ? parsed.message.trim() : "";

  return {
    id: typeof parsed.id === "string" && parsed.id.trim() ? parsed.id : createNotificationId(),
    title,
    message: message || undefined,
    tone: isAppNotificationTone(parsed.tone) ? parsed.tone : "info",
    createdAt: normalizeCreatedAt(parsed.createdAt),
    read: Boolean(parsed.read),
    link: normalizeLink(parsed.link),
  };
}

function normalizeLink(value: unknown): AppNotificationLink | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const parsed = value as Partial<AppNotificationLink>;
  const href = typeof parsed.href === "string" ? parsed.href.trim() : "";
  if (!href) {
    return undefined;
  }

  const label = typeof parsed.label === "string" ? parsed.label.trim() : "";
  return {
    href,
    label: label || "Open",
  };
}

function normalizeCreatedAt(value: unknown): string {
  if (typeof value === "string") {
    const timestamp = Date.parse(value);
    if (!Number.isNaN(timestamp)) {
      return new Date(timestamp).toISOString();
    }
  }
  return new Date().toISOString();
}

function sanitizeNotifications(notifications: AppNotification[]): AppNotification[] {
  const sorted = [...notifications].sort((a, b) => {
    const aTime = Date.parse(a.createdAt);
    const bTime = Date.parse(b.createdAt);
    return (Number.isNaN(bTime) ? 0 : bTime) - (Number.isNaN(aTime) ? 0 : aTime);
  });
  return sorted.slice(0, MAX_STORED_NOTIFICATIONS);
}

function cloneNotifications(notifications: AppNotification[]): AppNotification[] {
  return notifications.map((item) => ({
    ...item,
    link: item.link ? { ...item.link } : undefined,
  }));
}

function createNotificationId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return `cb-notification-${crypto.randomUUID()}`;
  }

  sequence += 1;
  return `cb-notification-${Date.now()}-${sequence}`;
}

function isAppNotificationTone(value: unknown): value is AppNotificationTone {
  return value === "info" || value === "success" || value === "warning" || value === "error";
}
