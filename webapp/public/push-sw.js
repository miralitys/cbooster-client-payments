"use strict";

const PUSH_OPEN_MESSAGE_TYPE = "cbooster-notification-open";
const PUSH_URL_PARAM_NOTIFICATION_ID = "openNotificationId";
const PUSH_URL_PARAM_CLIENT_NAME = "openNotificationClient";
const PUSH_URL_PARAM_LINK_HREF = "openNotificationLink";
const DEFAULT_OPEN_PATH = "/app/client-payments";

self.addEventListener("push", (event) => {
  const payload = normalizePushPayload(readPushEventData(event));
  event.waitUntil(
    self.registration.showNotification(payload.title, {
      body: payload.body,
      tag: payload.notificationId ? `cbooster-push-${payload.notificationId}` : undefined,
      data: payload,
      requireInteraction: false,
      renotify: true,
    }),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const payload = normalizePushPayload(event.notification.data);
  event.waitUntil(handleNotificationClick(payload));
});

function readPushEventData(event) {
  if (!event || !event.data) {
    return {};
  }

  try {
    return event.data.json();
  } catch {
    try {
      return {
        body: event.data.text(),
      };
    } catch {
      return {};
    }
  }
}

function normalizePushPayload(rawValue) {
  const value = rawValue && typeof rawValue === "object" ? rawValue : {};
  const title = sanitizeText(value.title, 260) || "Credit Booster";
  const body = sanitizeText(value.body, 2000) || "Open the app to view details.";
  const notificationId = sanitizeText(value.notificationId, 180);
  const clientName = sanitizeText(value.clientName, 300);
  const linkHref = normalizeInternalHref(value.linkHref);

  return {
    title,
    body,
    notificationId,
    clientName,
    linkHref,
  };
}

async function handleNotificationClick(payload) {
  const messagePayload = {
    type: PUSH_OPEN_MESSAGE_TYPE,
    notificationId: payload.notificationId,
    clientName: payload.clientName,
    linkHref: payload.linkHref,
  };

  const clientsList = await self.clients.matchAll({
    type: "window",
    includeUncontrolled: true,
  });
  const targetClient = clientsList.find((candidate) => isAppClientUrl(candidate.url)) || null;
  if (targetClient) {
    await targetClient.focus();
    targetClient.postMessage(messagePayload);
    return;
  }

  const openUrl = buildOpenUrl(payload);
  const openedClient = await self.clients.openWindow(openUrl);
  if (openedClient) {
    openedClient.postMessage(messagePayload);
  }
}

function buildOpenUrl(payload) {
  const targetPath = payload.clientName ? DEFAULT_OPEN_PATH : payload.linkHref || DEFAULT_OPEN_PATH;
  const url = new URL(targetPath, self.location.origin);

  if (payload.notificationId) {
    url.searchParams.set(PUSH_URL_PARAM_NOTIFICATION_ID, payload.notificationId);
  }
  if (payload.clientName) {
    url.searchParams.set(PUSH_URL_PARAM_CLIENT_NAME, payload.clientName);
  }
  if (payload.linkHref) {
    url.searchParams.set(PUSH_URL_PARAM_LINK_HREF, payload.linkHref);
  }

  return url.toString();
}

function sanitizeText(rawValue, maxLength) {
  return String(rawValue || "").trim().slice(0, maxLength);
}

function normalizeInternalHref(rawValue) {
  const href = sanitizeText(rawValue, 1200);
  return href.startsWith("/") ? href : "";
}

function isAppClientUrl(rawUrl) {
  try {
    const url = new URL(rawUrl || "", self.location.origin);
    return url.origin === self.location.origin && url.pathname.startsWith("/app");
  } catch {
    return false;
  }
}
