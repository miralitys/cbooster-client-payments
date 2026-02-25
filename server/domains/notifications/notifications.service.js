"use strict";

function createNotificationsService(dependencies = {}) {
  const {
    notificationsRepo,
    sanitizeTextValue,
  } = dependencies;

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  if (!notificationsRepo || typeof notificationsRepo.listNotificationsByRecipient !== "function") {
    throw new Error("createNotificationsService requires notificationsRepo.");
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
    return notificationsRepo.insertNotifications(entries);
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

  return {
    getNotificationsForUser,
    createNotifications,
    markNotificationReadForUser,
    markAllNotificationsReadForUser,
  };
}

module.exports = {
  createNotificationsService,
};
