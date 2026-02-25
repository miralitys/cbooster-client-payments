"use strict";

function createNotificationsService(dependencies = {}) {
  const {
    notificationsRepo,
    sanitizeTextValue,
    webPushClient,
    pushPublicKey,
    pushPrivateKey,
    pushSubject,
    logWarn,
  } = dependencies;

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);
  const warn = typeof logWarn === "function" ? logWarn : () => {};

  if (!notificationsRepo || typeof notificationsRepo.listNotificationsByRecipient !== "function") {
    throw new Error("createNotificationsService requires notificationsRepo.");
  }

  let webPushEnabled = Boolean(
    Boolean(webPushClient) &&
      typeof webPushClient?.sendNotification === "function" &&
      sanitize(pushPublicKey, 400) &&
      sanitize(pushPrivateKey, 400),
  );

  if (webPushEnabled && typeof webPushClient?.setVapidDetails === "function") {
    try {
      webPushClient.setVapidDetails(
        sanitize(pushSubject, 320) || "mailto:security@creditbooster.com",
        sanitize(pushPublicKey, 400),
        sanitize(pushPrivateKey, 400),
      );
    } catch (error) {
      webPushEnabled = false;
      warn(`[notifications push] Failed to configure VAPID details: ${sanitize(error?.message, 320) || "unknown error"}`);
    }
  }

  function normalizeUsername(rawValue) {
    return sanitize(rawValue, 200).toLowerCase();
  }

  function normalizeTone(rawValue) {
    const tone = sanitize(rawValue, 24).toLowerCase();
    if (tone === "success" || tone === "warning" || tone === "error" || tone === "info") {
      return tone;
    }
    return "info";
  }

  function normalizeNotificationLinkHref(rawValue) {
    const value = sanitize(rawValue, 1200);
    if (value.startsWith("/")) {
      return value;
    }
    return "/app/client-payments";
  }

  function mapInsertedNotificationRow(rawRow) {
    if (!rawRow || typeof rawRow !== "object") {
      return null;
    }

    const id = sanitize(rawRow.id, 180);
    const recipientUsername = normalizeUsername(rawRow.recipient_username);
    const title = sanitize(rawRow.title, 260);
    if (!id || !recipientUsername || !title) {
      return null;
    }

    const message = sanitize(rawRow.message, 2000);
    const clientName = sanitize(rawRow.client_name, 300);
    const linkHref = normalizeNotificationLinkHref(rawRow.link_href);
    const createdAtRaw = sanitize(rawRow.created_at, 80);
    const createdAtTimestamp = Date.parse(createdAtRaw);

    return {
      id,
      recipientUsername,
      title,
      message,
      clientName,
      linkHref,
      createdAt: Number.isFinite(createdAtTimestamp) ? new Date(createdAtTimestamp).toISOString() : new Date().toISOString(),
    };
  }

  function mapPushSubscriptionRow(rawRow) {
    if (!rawRow || typeof rawRow !== "object") {
      return null;
    }

    const endpoint = sanitize(rawRow.endpoint, 2000);
    const recipientUsername = normalizeUsername(rawRow.recipient_username);
    const p256dhKey = sanitize(rawRow.p256dh_key, 400);
    const authKey = sanitize(rawRow.auth_key, 220);
    if (!endpoint || !recipientUsername || !p256dhKey || !authKey) {
      return null;
    }

    const hasExpirationTime =
      rawRow.expiration_time !== null &&
      rawRow.expiration_time !== undefined &&
      String(rawRow.expiration_time).trim() !== "";
    const expirationTime = hasExpirationTime ? Number(rawRow.expiration_time) : Number.NaN;
    return {
      endpoint,
      recipientUsername,
      p256dhKey,
      authKey,
      expirationTime: hasExpirationTime && Number.isFinite(expirationTime) ? Math.trunc(expirationTime) : null,
    };
  }

  function buildPushPayload(notification) {
    return JSON.stringify({
      notificationId: notification.id,
      title: notification.title,
      body: notification.message || "Open the app to view details.",
      clientName: notification.clientName || "",
      linkHref: notification.linkHref || "/app/client-payments",
      createdAt: notification.createdAt,
    });
  }

  async function removePushSubscriptionByEndpoint(endpoint) {
    if (typeof notificationsRepo.deletePushSubscriptionByEndpoint !== "function") {
      return;
    }

    try {
      await notificationsRepo.deletePushSubscriptionByEndpoint(endpoint);
    } catch (error) {
      warn(
        `[notifications push] Failed to delete expired subscription endpoint: ${
          sanitize(error?.message, 320) || "unknown error"
        }`,
      );
    }
  }

  async function sendPushToSubscription(notification, subscription) {
    if (!webPushEnabled) {
      return;
    }

    try {
      await webPushClient.sendNotification(
        {
          endpoint: subscription.endpoint,
          expirationTime: subscription.expirationTime,
          keys: {
            p256dh: subscription.p256dhKey,
            auth: subscription.authKey,
          },
        },
        buildPushPayload(notification),
        {
          TTL: 6 * 60 * 60,
          urgency: "high",
        },
      );
    } catch (error) {
      const statusCode = Number(error?.statusCode || error?.status || 0);
      if (statusCode === 404 || statusCode === 410) {
        await removePushSubscriptionByEndpoint(subscription.endpoint);
      }

      warn(
        `[notifications push] Failed to send push notification id=${notification.id} endpoint=${sanitize(
          subscription.endpoint,
          120,
        )}: ${sanitize(error?.message, 320) || "unknown error"}`,
      );
    }
  }

  async function publishPushForInsertedNotifications(insertedRows) {
    if (!webPushEnabled) {
      return;
    }

    if (typeof notificationsRepo.listPushSubscriptionsByRecipients !== "function") {
      return;
    }

    const insertedNotifications = (Array.isArray(insertedRows) ? insertedRows : [])
      .map((row) => mapInsertedNotificationRow(row))
      .filter(Boolean);
    if (!insertedNotifications.length) {
      return;
    }

    const recipientUsernames = [...new Set(insertedNotifications.map((entry) => entry.recipientUsername))];
    if (!recipientUsernames.length) {
      return;
    }

    const subscriptions = (await notificationsRepo.listPushSubscriptionsByRecipients(recipientUsernames))
      .map((row) => mapPushSubscriptionRow(row))
      .filter(Boolean);
    if (!subscriptions.length) {
      return;
    }

    const notificationsByRecipient = new Map();
    for (const notification of insertedNotifications) {
      const current = notificationsByRecipient.get(notification.recipientUsername) || [];
      current.push(notification);
      notificationsByRecipient.set(notification.recipientUsername, current);
    }

    for (const subscription of subscriptions) {
      const recipientNotifications = notificationsByRecipient.get(subscription.recipientUsername) || [];
      for (const notification of recipientNotifications.slice(0, 20)) {
        await sendPushToSubscription(notification, subscription);
      }
    }
  }

  function mapNotificationRow(rawRow) {
    if (!rawRow || typeof rawRow !== "object") {
      return null;
    }

    const id = sanitize(rawRow.id, 180);
    const title = sanitize(rawRow.title, 260);
    if (!id || !title) {
      return null;
    }

    const createdAtRaw = sanitize(rawRow.created_at, 80);
    const createdAtTimestamp = Date.parse(createdAtRaw);
    const createdAt = Number.isFinite(createdAtTimestamp) ? new Date(createdAtTimestamp).toISOString() : new Date().toISOString();
    const message = sanitize(rawRow.message, 2000);
    const clientName = sanitize(rawRow.client_name, 300);
    const linkHref = sanitize(rawRow.link_href, 1200);
    const linkLabel = sanitize(rawRow.link_label, 80) || "Open";
    const readAt = sanitize(rawRow.read_at, 80);

    return {
      id,
      title,
      message: message || undefined,
      tone: normalizeTone(rawRow.tone),
      createdAt,
      read: Boolean(readAt),
      clientName: clientName || undefined,
      link: linkHref
        ? {
            href: linkHref,
            label: linkLabel,
          }
        : undefined,
    };
  }

  async function getNotificationsForUser(options = {}) {
    const username = normalizeUsername(options.username);
    if (!username) {
      return [];
    }

    const rows = await notificationsRepo.listNotificationsByRecipient(username, {
      limit: options.limit,
    });
    return rows
      .map((row) => mapNotificationRow(row))
      .filter(Boolean);
  }

  async function createNotifications(entries) {
    const insertedRows = await notificationsRepo.insertNotifications(entries);

    if (webPushEnabled && insertedRows.length) {
      try {
        await publishPushForInsertedNotifications(insertedRows);
      } catch (error) {
        warn(
          `[notifications push] Failed to publish push notifications: ${
            sanitize(error?.message, 320) || "unknown error"
          }`,
        );
      }
    }

    return insertedRows.length;
  }

  async function markNotificationReadForUser(options = {}) {
    const username = normalizeUsername(options.username);
    const notificationId = sanitize(options.id, 180);
    if (!username || !notificationId) {
      return false;
    }

    return notificationsRepo.markNotificationRead(username, notificationId);
  }

  async function markAllNotificationsReadForUser(options = {}) {
    const username = normalizeUsername(options.username);
    if (!username) {
      return 0;
    }

    return notificationsRepo.markAllNotificationsRead(username);
  }

  function getPushPublicKeyConfig() {
    return {
      enabled: webPushEnabled,
      publicKey: webPushEnabled ? sanitize(pushPublicKey, 400) : "",
    };
  }

  async function upsertPushSubscriptionForUser(options = {}) {
    const username = normalizeUsername(options.username);
    if (!username || !webPushEnabled) {
      return false;
    }

    if (typeof notificationsRepo.upsertPushSubscription !== "function") {
      return false;
    }

    return notificationsRepo.upsertPushSubscription(username, options.subscription, {
      userAgent: options.userAgent,
    });
  }

  async function removePushSubscriptionForUser(options = {}) {
    const username = normalizeUsername(options.username);
    const endpoint = sanitize(options.endpoint, 2000);
    if (!username || !endpoint) {
      return false;
    }

    if (typeof notificationsRepo.deletePushSubscriptionForRecipient !== "function") {
      return false;
    }

    return notificationsRepo.deletePushSubscriptionForRecipient(username, endpoint);
  }

  return {
    getNotificationsForUser,
    createNotifications,
    markNotificationReadForUser,
    markAllNotificationsReadForUser,
    getPushPublicKeyConfig,
    upsertPushSubscriptionForUser,
    removePushSubscriptionForUser,
  };
}

module.exports = {
  createNotificationsService,
};
