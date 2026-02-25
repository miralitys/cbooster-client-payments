import { useEffect, useMemo, useRef, useState } from "react";

import {
  clearNotifications,
  dismissNotification,
  getNotifications,
  getUnreadNotificationsCount,
  markAllNotificationsRead,
  markNotificationRead,
  subscribeNotifications,
} from "@/shared/lib/notifications";
import type { AppNotification } from "@/shared/types/notifications";

const notificationDateFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "short",
  timeStyle: "short",
});

export function NotificationCenter() {
  const [isOpen, setIsOpen] = useState(false);
  const [notifications, setNotifications] = useState<AppNotification[]>(() => getNotifications());
  const rootRef = useRef<HTMLDivElement | null>(null);
  const unreadCount = useMemo(() => getUnreadNotificationsCount(notifications), [notifications]);

  useEffect(() => subscribeNotifications((next) => setNotifications(next)), []);

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

        <div className="notification-center__toolbar">
          <button
            type="button"
            className="notification-center__toolbar-btn"
            disabled={!unreadCount}
            onClick={() => markAllNotificationsRead()}
          >
            Mark all read
          </button>
          <button
            type="button"
            className="notification-center__toolbar-btn"
            disabled={!notifications.length}
            onClick={() => clearNotifications()}
          >
            Clear all
          </button>
        </div>

        {notifications.length ? (
          <ul className="notification-center__list">
            {notifications.map((notification) => (
              <li
                key={notification.id}
                className={`notification-center__item notification-center__item--${notification.tone} ${
                  notification.read ? "is-read" : "is-unread"
                }`.trim()}
              >
                <div className="notification-center__item-main">
                  <span className="notification-center__tone-dot" aria-hidden="true" />
                  <div className="notification-center__item-copy">
                    <p className="notification-center__item-title">{notification.title}</p>
                    {notification.message ? <p className="notification-center__item-message">{notification.message}</p> : null}
                    <time className="notification-center__item-time" dateTime={notification.createdAt}>
                      {formatNotificationDate(notification.createdAt)}
                    </time>
                  </div>
                </div>

                <div className="notification-center__item-actions">
                  {!notification.read ? (
                    <button
                      type="button"
                      className="notification-center__item-action"
                      onClick={() => markNotificationRead(notification.id)}
                    >
                      Read
                    </button>
                  ) : null}

                  {notification.link ? (
                    <a
                      href={notification.link.href}
                      className="notification-center__item-action"
                      onClick={() => {
                        markNotificationRead(notification.id);
                        setIsOpen(false);
                      }}
                    >
                      {notification.link.label}
                    </a>
                  ) : null}

                  <button
                    type="button"
                    className="notification-center__item-action"
                    onClick={() => dismissNotification(notification.id)}
                  >
                    Dismiss
                  </button>
                </div>
              </li>
            ))}
          </ul>
        ) : (
          <p className="notification-center__empty">No notifications yet. New events will appear here.</p>
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
