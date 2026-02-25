"use strict";

function createNotificationsRepo(dependencies = {}) {
  const {
    db,
    ensureDatabaseReady,
    tables,
    helpers,
  } = dependencies;

  const query =
    typeof db?.query === "function"
      ? db.query
      : async () => {
          throw new Error("Notifications repository query function is not configured.");
        };
  const notificationsTable = tables?.notificationsTable;
  const sanitizeTextValue =
    typeof helpers?.sanitizeTextValue === "function"
      ? helpers.sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  if (typeof ensureDatabaseReady !== "function") {
    throw new Error("createNotificationsRepo requires ensureDatabaseReady().");
  }

  if (!notificationsTable) {
    throw new Error("createNotificationsRepo requires tables.notificationsTable.");
  }

  function normalizeUsername(rawValue) {
    return sanitizeTextValue(rawValue, 200).toLowerCase();
  }

  function normalizeTone(rawValue) {
    const tone = sanitizeTextValue(rawValue, 24).toLowerCase();
    if (tone === "success" || tone === "warning" || tone === "error" || tone === "info") {
      return tone;
    }
    return "info";
  }

  function normalizeLimit(rawValue) {
    const parsed = Number.parseInt(String(rawValue ?? ""), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 120;
    }
    return Math.min(Math.max(parsed, 1), 300);
  }

  function normalizePayload(rawValue) {
    if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
      return {};
    }
    return rawValue;
  }

  function normalizeNotificationEntry(rawEntry) {
    if (!rawEntry || typeof rawEntry !== "object" || Array.isArray(rawEntry)) {
      return null;
    }

    const id = sanitizeTextValue(rawEntry.id, 180);
    const recipientUsername = normalizeUsername(rawEntry.recipientUsername);
    const title = sanitizeTextValue(rawEntry.title, 260);
    if (!id || !recipientUsername || !title) {
      return null;
    }

    const createdAtRaw = sanitizeTextValue(rawEntry.createdAt, 80);
    const createdAtTimestamp = Date.parse(createdAtRaw);
    const createdAt = Number.isFinite(createdAtTimestamp) ? new Date(createdAtTimestamp).toISOString() : new Date().toISOString();
    const message = sanitizeTextValue(rawEntry.message, 2000);
    const clientName = sanitizeTextValue(rawEntry.clientName, 300);
    const linkHref = sanitizeTextValue(rawEntry.linkHref, 1200);
    const linkLabel = sanitizeTextValue(rawEntry.linkLabel, 80) || "Open";
    const type = sanitizeTextValue(rawEntry.type, 80) || "generic";

    return {
      id,
      recipientUsername,
      type,
      title,
      message,
      tone: normalizeTone(rawEntry.tone),
      clientName,
      linkHref,
      linkLabel,
      payload: normalizePayload(rawEntry.payload),
      createdAt,
    };
  }

  async function listNotificationsByRecipient(recipientUsername, options = {}) {
    const normalizedRecipientUsername = normalizeUsername(recipientUsername);
    if (!normalizedRecipientUsername) {
      return [];
    }

    const limit = normalizeLimit(options.limit);
    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          id,
          recipient_username,
          type,
          title,
          message,
          tone,
          client_name,
          link_href,
          link_label,
          payload,
          read_at,
          created_at
        FROM ${notificationsTable}
        WHERE recipient_username = $1
        ORDER BY created_at DESC, id DESC
        LIMIT $2
      `,
      [normalizedRecipientUsername, limit],
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  async function insertNotifications(entries) {
    const normalizedEntries = (Array.isArray(entries) ? entries : [])
      .map((entry) => normalizeNotificationEntry(entry))
      .filter(Boolean);
    if (!normalizedEntries.length) {
      return 0;
    }

    await ensureDatabaseReady();
    const values = [];
    const placeholders = normalizedEntries.map((entry, index) => {
      const base = index * 11;
      values.push(
        entry.id,
        entry.recipientUsername,
        entry.type,
        entry.title,
        entry.message,
        entry.tone,
        entry.clientName,
        entry.linkHref,
        entry.linkLabel,
        JSON.stringify(entry.payload),
        entry.createdAt,
      );
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}, $${base + 10}::jsonb, $${base + 11})`;
    });

    await query(
      `
        INSERT INTO ${notificationsTable} (
          id,
          recipient_username,
          type,
          title,
          message,
          tone,
          client_name,
          link_href,
          link_label,
          payload,
          created_at
        )
        VALUES ${placeholders.join(", ")}
        ON CONFLICT (id) DO NOTHING
      `,
      values,
    );

    return normalizedEntries.length;
  }

  async function markNotificationRead(recipientUsername, notificationId) {
    const normalizedRecipientUsername = normalizeUsername(recipientUsername);
    const normalizedNotificationId = sanitizeTextValue(notificationId, 180);
    if (!normalizedRecipientUsername || !normalizedNotificationId) {
      return false;
    }

    await ensureDatabaseReady();
    const result = await query(
      `
        UPDATE ${notificationsTable}
        SET read_at = COALESCE(read_at, NOW())
        WHERE recipient_username = $1
          AND id = $2
      `,
      [normalizedRecipientUsername, normalizedNotificationId],
    );

    return Number(result?.rowCount || 0) > 0;
  }

  async function markAllNotificationsRead(recipientUsername) {
    const normalizedRecipientUsername = normalizeUsername(recipientUsername);
    if (!normalizedRecipientUsername) {
      return 0;
    }

    await ensureDatabaseReady();
    const result = await query(
      `
        UPDATE ${notificationsTable}
        SET read_at = COALESCE(read_at, NOW())
        WHERE recipient_username = $1
          AND read_at IS NULL
      `,
      [normalizedRecipientUsername],
    );

    return Number(result?.rowCount || 0);
  }

  return {
    listNotificationsByRecipient,
    insertNotifications,
    markNotificationRead,
    markAllNotificationsRead,
  };
}

module.exports = {
  createNotificationsRepo,
};
