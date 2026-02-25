import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  getNotificationsFeed,
  markAllNotificationsRead as markAllNotificationsReadRequest,
  markNotificationRead as markNotificationReadRequest,
} from "@/shared/api/notifications";
import { requestOpenClientCard } from "@/shared/lib/openClientCard";
import type { AppNotification } from "@/shared/types/notifications";

const notificationDateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "short",
  timeStyle: "short",
});
const NOTIFICATIONS_POLL_INTERVAL_MS = 30_000;

export function NotificationCenter() {
  const [isOpen, setIsOpen] = useState(false);
  const [viewMode, setViewMode] = useState<"active" | "archive">("active");
  const [notifications, setNotifications] = useState<AppNotification[]>([]);
  const rootRef = useRef<HTMLDivElement | null>(null);
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

  function markNotificationRead(notificationId: string): void {
    setNotifications((currentNotifications) =>
      currentNotifications.map((notification) => {
        if (notification.id !== notificationId || notification.read) {
          return notification;
        }
        return {
          ...notification,
          read: true,
        };
      }),
    );

    void markNotificationReadRequest(notificationId).catch(() => {
      void loadNotifications();
    });
  }

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
