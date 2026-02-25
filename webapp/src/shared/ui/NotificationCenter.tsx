import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  getNotificationsFeed,
  getNotificationsPushPublicKey,
  markAllNotificationsRead as markAllNotificationsReadRequest,
  markNotificationRead as markNotificationReadRequest,
  subscribeNotificationsPush,
  unsubscribeNotificationsPush,
  type PushPublicKeyConfig,
  type PushSubscriptionPayload,
} from "@/shared/api/notifications";
import { requestOpenClientCard } from "@/shared/lib/openClientCard";
import type { AppNotification } from "@/shared/types/notifications";

const notificationDateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "short",
  timeStyle: "short",
});

const NOTIFICATIONS_POLL_INTERVAL_MS = 30_000;
const PUSH_SERVICE_WORKER_URL = "/app/push-sw.js";
const PUSH_SERVICE_WORKER_SCOPE = "/app/";
const PUSH_URL_PARAM_NOTIFICATION_ID = "openNotificationId";
const PUSH_URL_PARAM_CLIENT_NAME = "openNotificationClient";
const PUSH_URL_PARAM_LINK_HREF = "openNotificationLink";
const PUSH_OPEN_MESSAGE_TYPE = "cbooster-notification-open";
const PUSH_VAPID_PUBLIC_KEY_STORAGE_KEY = "cbooster_push_vapid_public_key_v1";

interface PushOpenPayload {
  notificationId: string;
  clientName: string;
  linkHref: string;
}

export function NotificationCenter() {
  const [isOpen, setIsOpen] = useState(false);
  const [viewMode, setViewMode] = useState<"active" | "archive">("active");
  const [notifications, setNotifications] = useState<AppNotification[]>([]);
  const [pushPermission, setPushPermission] = useState<NotificationPermission>(() =>
    supportsBrowserNotifications() ? Notification.permission : "default",
  );
  const [pushServerEnabled, setPushServerEnabled] = useState<boolean | null>(null);
  const [pushSubscribed, setPushSubscribed] = useState(false);
  const [isPushSyncing, setIsPushSyncing] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const hasHandledPushUrlRef = useRef(false);

  const isPushSupported = supportsPushNotifications();
  const unreadCount = useMemo(() => getUnreadNotificationsCount(notifications), [notifications]);
  const activeNotifications = useMemo(() => notifications.filter((item) => !item.read), [notifications]);
  const displayedNotifications = viewMode === "active" ? activeNotifications : notifications;

  const loadNotifications = useCallback(async () => {
    try {
      const nextNotifications = await getNotificationsFeed();
      setNotifications(nextNotifications);
    } catch {
      // Keep existing notifications snapshot on transient API failures.
    }
  }, []);

  const markNotificationRead = useCallback(
    (notificationId: string): void => {
      const normalizedId = String(notificationId || "").trim();
      if (!normalizedId) {
        return;
      }

      setNotifications((currentNotifications) =>
        currentNotifications.map((notification) => {
          if (notification.id !== normalizedId || notification.read) {
            return notification;
          }
          return {
            ...notification,
            read: true,
          };
        }),
      );

      void markNotificationReadRequest(normalizedId).catch(() => {
        void loadNotifications();
      });
    },
    [loadNotifications],
  );

  const openNotificationByPayload = useCallback(
    (payload: PushOpenPayload): void => {
      if (payload.notificationId) {
        markNotificationRead(payload.notificationId);
      }

      if (payload.clientName) {
        requestOpenClientCard(payload.clientName, {
          fallbackHref: payload.linkHref || undefined,
        });
        return;
      }

      if (payload.linkHref) {
        window.location.assign(payload.linkHref);
      }
    },
    [markNotificationRead],
  );

  const syncPushSubscription = useCallback(
    async (knownConfig: PushPublicKeyConfig | null = null): Promise<boolean> => {
      if (!isPushSupported) {
        setPushServerEnabled(false);
        setPushSubscribed(false);
        return false;
      }

      const config =
        knownConfig ||
        (await getNotificationsPushPublicKey().catch(() => ({
          enabled: false,
          publicKey: "",
        })));
      const isEnabled = Boolean(config?.enabled && config?.publicKey);
      setPushServerEnabled(isEnabled);
      if (!isEnabled) {
        setPushSubscribed(false);
        return false;
      }

      if (Notification.permission !== "granted") {
        setPushSubscribed(false);
        return false;
      }

      try {
        const registration = await registerPushServiceWorker();
        const { subscription, rotated, previousEndpoint } = await ensurePushSubscription(registration, config.publicKey);
        const payload = normalizePushSubscriptionPayload(subscription);
        if (!payload) {
          setPushSubscribed(false);
          return false;
        }

        if (rotated && previousEndpoint) {
          await unsubscribeNotificationsPush(previousEndpoint).catch(() => false);
        }

        const subscribed = await subscribeNotificationsPush(payload);
        setPushSubscribed(subscribed);
        if (subscribed) {
          persistActivePushPublicKey(config.publicKey);
        }
        return subscribed;
      } catch {
        setPushSubscribed(false);
        return false;
      }
    },
    [isPushSupported],
  );

  const unsubscribePush = useCallback(async (): Promise<void> => {
    if (!isPushSupported) {
      setPushSubscribed(false);
      return;
    }

    try {
      const registration = await navigator.serviceWorker.getRegistration(PUSH_SERVICE_WORKER_SCOPE);
      const subscription = await registration?.pushManager.getSubscription();
      const endpoint = String(subscription?.endpoint || "").trim();
      if (endpoint) {
        await unsubscribeNotificationsPush(endpoint).catch(() => false);
      }
      if (subscription) {
        await subscription.unsubscribe().catch(() => false);
      }
    } catch {
      // Best-effort unsubscribe.
    }

    setPushSubscribed(false);
  }, [isPushSupported]);

  useEffect(() => {
    let cancelled = false;

    async function refreshNotifications() {
      if (cancelled) {
        return;
      }
      await loadNotifications();
    }

    void refreshNotifications();
    const pollTimer = window.setInterval(() => {
      void refreshNotifications();
    }, NOTIFICATIONS_POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(pollTimer);
    };
  }, [loadNotifications]);

  useEffect(() => {
    if (!isPushSupported) {
      return;
    }

    let cancelled = false;
    async function bootstrapPushConfig() {
      const config = await getNotificationsPushPublicKey().catch(() => ({
        enabled: false,
        publicKey: "",
      }));
      if (cancelled) {
        return;
      }
      setPushServerEnabled(config.enabled === true && Boolean(config.publicKey));

      if (Notification.permission === "granted") {
        await syncPushSubscription(config);
      }
    }

    void bootstrapPushConfig();
    return () => {
      cancelled = true;
    };
  }, [isPushSupported, syncPushSubscription]);

  useEffect(() => {
    if (!isPushSupported) {
      return;
    }

    function syncPermission(): void {
      setPushPermission(Notification.permission);
    }

    syncPermission();
    window.addEventListener("focus", syncPermission);
    document.addEventListener("visibilitychange", syncPermission);
    return () => {
      window.removeEventListener("focus", syncPermission);
      document.removeEventListener("visibilitychange", syncPermission);
    };
  }, [isPushSupported]);

  useEffect(() => {
    if (!isPushSupported) {
      return;
    }

    if (pushPermission === "granted") {
      void syncPushSubscription();
      return;
    }

    if (pushPermission === "denied") {
      void unsubscribePush();
    }
  }, [isPushSupported, pushPermission, syncPushSubscription, unsubscribePush]);

  useEffect(() => {
    if (!isPushSupported) {
      return;
    }

    function onPushMessage(event: MessageEvent<unknown>) {
      const payload = parsePushOpenPayload(event.data);
      if (!payload) {
        return;
      }
      openNotificationByPayload(payload);
    }

    navigator.serviceWorker.addEventListener("message", onPushMessage);
    return () => {
      navigator.serviceWorker.removeEventListener("message", onPushMessage);
    };
  }, [isPushSupported, openNotificationByPayload]);

  useEffect(() => {
    if (hasHandledPushUrlRef.current) {
      return;
    }
    hasHandledPushUrlRef.current = true;

    const payload = parsePushOpenPayloadFromCurrentUrl();
    if (!payload) {
      return;
    }

    clearPushOpenPayloadFromCurrentUrl();
    openNotificationByPayload(payload);
  }, [openNotificationByPayload]);

  useEffect(() => {
    function onPointerDown(event: MouseEvent) {
      if (!rootRef.current) {
        return;
      }

      const target = event.target;
      if (target instanceof Node && !rootRef.current.contains(target)) {
        setIsOpen(false);
      }
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setIsOpen(false);
      }
    }

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  function markAllNotificationsRead(): void {
    if (!unreadCount) {
      return;
    }

    setNotifications((currentNotifications) =>
      currentNotifications.map((notification) => {
        if (notification.read) {
          return notification;
        }
        return {
          ...notification,
          read: true,
        };
      }),
    );

    void markAllNotificationsReadRequest().catch(() => {
      void loadNotifications();
    });
  }

  function handleOpenNotification(notification: AppNotification): void {
    markNotificationRead(notification.id);
    setIsOpen(false);

    if (notification.clientName) {
      requestOpenClientCard(notification.clientName, {
        fallbackHref: notification.link?.href,
      });
      return;
    }

    if (notification.link?.href) {
      window.location.assign(notification.link.href);
    }
  }

  function handleNotificationItemClick(notification: AppNotification): void {
    if (notification.clientName || notification.link?.href) {
      handleOpenNotification(notification);
      return;
    }

    markNotificationRead(notification.id);
  }

  async function handleEnableChromeAlertsClick(): Promise<void> {
    if (!isPushSupported) {
      return;
    }

    setIsPushSyncing(true);
    try {
      let nextPermission = Notification.permission;
      if (nextPermission !== "granted") {
        nextPermission = await Notification.requestPermission();
      }
      setPushPermission(nextPermission);

      if (nextPermission === "granted") {
        await syncPushSubscription();
      }
    } finally {
      setIsPushSyncing(false);
    }
  }

  return (
    <div ref={rootRef} className={`notification-center ${isOpen ? "is-open" : ""}`.trim()}>
      <button
        type="button"
        className="notification-center__toggle"
        aria-haspopup="dialog"
        aria-expanded={isOpen}
        aria-controls="app-notification-center-panel"
        aria-label="Open notifications"
        onClick={() => setIsOpen((prev) => !prev)}
      >
        <svg className="notification-center__icon" viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 2a7 7 0 0 0-7 7v3.45c0 .86-.3 1.7-.86 2.35L2.5 16.7a1 1 0 0 0 .76 1.67h17.48a1 1 0 0 0 .76-1.67l-1.64-1.9a3.61 3.61 0 0 1-.86-2.35V9a7 7 0 0 0-7-7Zm0 20a3.14 3.14 0 0 0 2.97-2.14.5.5 0 0 0-.47-.66H9.5a.5.5 0 0 0-.47.66A3.14 3.14 0 0 0 12 22Z" />
        </svg>
        {unreadCount ? (
          <span className="notification-center__badge" aria-label={`${unreadCount} unread notifications`}>
            {unreadCount > 99 ? "99+" : unreadCount}
          </span>
        ) : null}
      </button>

      <section
        id="app-notification-center-panel"
        className="notification-center__panel"
        role="dialog"
        aria-label="Notification center"
        hidden={!isOpen}
      >
        <header className="notification-center__header">
          <p className="notification-center__title">Notifications</p>
          <p className="notification-center__count">{unreadCount} unread</p>
        </header>

        {isPushSupported ? (
          <div className="notification-center__browser">
            {pushServerEnabled === false ? (
              <p className="notification-center__browser-note">Chrome alerts are unavailable on the server.</p>
            ) : null}

            {pushServerEnabled !== false && pushPermission === "default" ? (
              <button
                type="button"
                className="notification-center__toolbar-btn"
                disabled={isPushSyncing}
                onClick={() => {
                  void handleEnableChromeAlertsClick();
                }}
              >
                Enable Chrome alerts
              </button>
            ) : null}

            {pushServerEnabled && pushPermission === "granted" ? (
              <p className="notification-center__browser-note notification-center__browser-note--enabled">
                {pushSubscribed ? "Chrome alerts enabled" : "Chrome alerts connecting..."}
              </p>
            ) : null}

            {pushServerEnabled && pushPermission === "denied" ? (
              <p className="notification-center__browser-note">
                Chrome alerts are blocked in browser settings for this site.
              </p>
            ) : null}
          </div>
        ) : null}

        <div className="notification-center__controls">
          <div className="notification-center__modes" role="tablist" aria-label="Notification views">
            <button
              type="button"
              className={`notification-center__mode-btn ${viewMode === "active" ? "is-active" : ""}`.trim()}
              role="tab"
              aria-selected={viewMode === "active"}
              onClick={() => setViewMode("active")}
            >
              Active
            </button>
            <button
              type="button"
              className={`notification-center__mode-btn ${viewMode === "archive" ? "is-active" : ""}`.trim()}
              role="tab"
              aria-selected={viewMode === "archive"}
              onClick={() => setViewMode("archive")}
            >
              Archive
            </button>
          </div>

          {viewMode === "active" ? (
            <div className="notification-center__toolbar notification-center__toolbar--right">
              <button
                type="button"
                className="notification-center__toolbar-btn"
                disabled={!unreadCount}
                onClick={() => markAllNotificationsRead()}
              >
                Mark all read
              </button>
            </div>
          ) : null}
        </div>

        {displayedNotifications.length ? (
          <ul className="notification-center__list">
            {displayedNotifications.map((notification) => (
              <li
                key={notification.id}
                className={`notification-center__item notification-center__item--${notification.tone} ${
                  notification.read ? "is-read" : "is-unread"
                }`.trim()}
              >
                <button
                  type="button"
                  className="notification-center__item-main notification-center__item-main-btn"
                  onClick={() => handleNotificationItemClick(notification)}
                >
                  <span className="notification-center__tone-dot" aria-hidden="true" />
                  <div className="notification-center__item-copy">
                    <p className="notification-center__item-title">{notification.title}</p>
                    {notification.message ? <p className="notification-center__item-message">{notification.message}</p> : null}
                    <time className="notification-center__item-time" dateTime={notification.createdAt}>
                      {formatNotificationDate(notification.createdAt)}
                    </time>
                  </div>
                </button>

                <div className="notification-center__item-actions">
                  {notification.link || notification.clientName ? (
                    <button
                      type="button"
                      className="notification-center__item-action"
                      onClick={() => handleOpenNotification(notification)}
                    >
                      Open
                    </button>
                  ) : null}
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <p className="notification-center__empty">
            {viewMode === "active"
              ? "No active notifications. Open Archive to see all notifications."
              : "Archive is empty."}
          </p>
        )}
      </section>
    </div>
  );
}

function formatNotificationDate(value: string): string {
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return "Unknown time";
  }

  return notificationDateFormatter.format(timestamp);
}

function getUnreadNotificationsCount(notifications: AppNotification[]): number {
  let unreadCount = 0;
  for (const notification of notifications) {
    if (!notification.read) {
      unreadCount += 1;
    }
  }
  return unreadCount;
}

function supportsBrowserNotifications(): boolean {
  return typeof window !== "undefined" && "Notification" in window;
}

function supportsPushNotifications(): boolean {
  return (
    typeof window !== "undefined" &&
    supportsBrowserNotifications() &&
    window.isSecureContext &&
    "serviceWorker" in navigator &&
    "PushManager" in window
  );
}

async function registerPushServiceWorker(): Promise<ServiceWorkerRegistration> {
  return navigator.serviceWorker.register(PUSH_SERVICE_WORKER_URL, {
    scope: PUSH_SERVICE_WORKER_SCOPE,
  });
}

async function ensurePushSubscription(
  registration: ServiceWorkerRegistration,
  applicationServerKey: string,
): Promise<{ subscription: PushSubscription; rotated: boolean; previousEndpoint: string }> {
  const existingSubscription = await registration.pushManager.getSubscription();
  const previousPublicKey = readPersistedPushPublicKey();
  const normalizedPublicKey = String(applicationServerKey || "").trim();
  const shouldRotateSubscription = Boolean(existingSubscription && previousPublicKey && previousPublicKey !== normalizedPublicKey);
  const previousEndpoint = String(existingSubscription?.endpoint || "").trim();

  if (existingSubscription && !shouldRotateSubscription) {
    return {
      subscription: existingSubscription,
      rotated: false,
      previousEndpoint: "",
    };
  }

  if (existingSubscription && shouldRotateSubscription) {
    try {
      await existingSubscription.unsubscribe();
    } catch {
      // Continue with a new subscription attempt.
    }
  }

  const nextSubscription = await registration.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: decodeBase64UrlToArrayBuffer(applicationServerKey),
  });

  return {
    subscription: nextSubscription,
    rotated: shouldRotateSubscription,
    previousEndpoint: shouldRotateSubscription ? previousEndpoint : "",
  };
}

function normalizePushSubscriptionPayload(subscription: PushSubscription | null): PushSubscriptionPayload | null {
  if (!subscription) {
    return null;
  }

  const serialized = subscription.toJSON();
  const endpoint = String(serialized.endpoint || subscription.endpoint || "").trim();
  const p256dh = String(serialized.keys?.p256dh || "").trim();
  const auth = String(serialized.keys?.auth || "").trim();
  if (!endpoint || !p256dh || !auth) {
    return null;
  }

  return {
    endpoint,
    expirationTime: serialized.expirationTime ?? null,
    keys: {
      p256dh,
      auth,
    },
  };
}

function decodeBase64UrlToArrayBuffer(value: string): ArrayBuffer {
  const normalizedValue = String(value || "").replace(/-/g, "+").replace(/_/g, "/");
  const padding = "=".repeat((4 - (normalizedValue.length % 4)) % 4);
  const base64 = normalizedValue + padding;
  const decoded = atob(base64);
  const bytes = new Uint8Array(decoded.length);
  for (let index = 0; index < decoded.length; index += 1) {
    bytes[index] = decoded.charCodeAt(index);
  }
  return bytes.buffer;
}

function parsePushOpenPayload(rawValue: unknown): PushOpenPayload | null {
  if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
    return null;
  }

  const messageType = String((rawValue as { type?: unknown }).type || "").trim();
  if (messageType !== PUSH_OPEN_MESSAGE_TYPE) {
    return null;
  }

  const notificationId = String((rawValue as { notificationId?: unknown }).notificationId || "").trim();
  const clientName = String((rawValue as { clientName?: unknown }).clientName || "").trim();
  const linkHref = normalizeInternalLinkHref((rawValue as { linkHref?: unknown }).linkHref);
  if (!notificationId && !clientName && !linkHref) {
    return null;
  }

  return {
    notificationId,
    clientName,
    linkHref,
  };
}

function parsePushOpenPayloadFromCurrentUrl(): PushOpenPayload | null {
  if (typeof window === "undefined") {
    return null;
  }

  const url = new URL(window.location.href);
  const notificationId = String(url.searchParams.get(PUSH_URL_PARAM_NOTIFICATION_ID) || "").trim();
  const clientName = String(url.searchParams.get(PUSH_URL_PARAM_CLIENT_NAME) || "").trim();
  const linkHref = normalizeInternalLinkHref(url.searchParams.get(PUSH_URL_PARAM_LINK_HREF) || "");
  if (!notificationId && !clientName && !linkHref) {
    return null;
  }

  return {
    notificationId,
    clientName,
    linkHref,
  };
}

function clearPushOpenPayloadFromCurrentUrl(): void {
  if (typeof window === "undefined") {
    return;
  }

  const url = new URL(window.location.href);
  const hadParams =
    url.searchParams.has(PUSH_URL_PARAM_NOTIFICATION_ID) ||
    url.searchParams.has(PUSH_URL_PARAM_CLIENT_NAME) ||
    url.searchParams.has(PUSH_URL_PARAM_LINK_HREF);
  if (!hadParams) {
    return;
  }

  url.searchParams.delete(PUSH_URL_PARAM_NOTIFICATION_ID);
  url.searchParams.delete(PUSH_URL_PARAM_CLIENT_NAME);
  url.searchParams.delete(PUSH_URL_PARAM_LINK_HREF);
  window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
}

function normalizeInternalLinkHref(rawValue: unknown): string {
  const href = String(rawValue || "").trim();
  if (href.startsWith("/")) {
    return href;
  }
  return "";
}

function readPersistedPushPublicKey(): string {
  if (typeof window === "undefined") {
    return "";
  }

  try {
    return String(window.localStorage.getItem(PUSH_VAPID_PUBLIC_KEY_STORAGE_KEY) || "").trim();
  } catch {
    return "";
  }
}

function persistActivePushPublicKey(publicKey: string): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalizedPublicKey = String(publicKey || "").trim();
  if (!normalizedPublicKey) {
    return;
  }

  try {
    window.localStorage.setItem(PUSH_VAPID_PUBLIC_KEY_STORAGE_KEY, normalizedPublicKey);
  } catch {
    // Ignore storage errors in privacy-restricted browser modes.
  }
}
