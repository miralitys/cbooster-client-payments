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
  const pushSubscriptionsTable = tables?.pushSubscriptionsTable;
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
  if (!pushSubscriptionsTable) {
    throw new Error("createNotificationsRepo requires tables.pushSubscriptionsTable.");
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

  function normalizePushSubscription(rawSubscription) {
    if (!rawSubscription || typeof rawSubscription !== "object" || Array.isArray(rawSubscription)) {
      return null;
    }

    const endpoint = sanitizeTextValue(rawSubscription.endpoint, 2000);
    const keys = rawSubscription.keys && typeof rawSubscription.keys === "object" ? rawSubscription.keys : {};
    const p256dhKey = sanitizeTextValue(keys.p256dh, 400);
    const authKey = sanitizeTextValue(keys.auth, 220);
    if (!endpoint || !p256dhKey || !authKey) {
      return null;
    }

    const hasExpirationTime =
      rawSubscription.expirationTime !== null &&
      rawSubscription.expirationTime !== undefined &&
      String(rawSubscription.expirationTime).trim() !== "";
    const expirationTime = hasExpirationTime ? Number(rawSubscription.expirationTime) : Number.NaN;
    return {
      endpoint,
      p256dhKey,
      authKey,
      expirationTime: hasExpirationTime && Number.isFinite(expirationTime) ? Math.trunc(expirationTime) : null,
    };
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
      return [];
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

    const result = await query(
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
        RETURNING
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
      `,
      values,
    );

    return Array.isArray(result?.rows) ? result.rows : [];
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

  async function listPushSubscriptionsByRecipients(recipientUsernames) {
    const normalizedRecipients = [...new Set((Array.isArray(recipientUsernames) ? recipientUsernames : []).map(normalizeUsername).filter(Boolean))];
    if (!normalizedRecipients.length) {
      return [];
    }

    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          endpoint,
          recipient_username,
          p256dh_key,
          auth_key,
          expiration_time
        FROM ${pushSubscriptionsTable}
        WHERE recipient_username = ANY($1::text[])
      `,
      [normalizedRecipients],
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  async function upsertPushSubscription(recipientUsername, subscription, options = {}) {
    const normalizedRecipientUsername = normalizeUsername(recipientUsername);
    const normalizedSubscription = normalizePushSubscription(subscription);
    if (!normalizedRecipientUsername || !normalizedSubscription) {
      return false;
    }

    const userAgent = sanitizeTextValue(options.userAgent, 900);
    await ensureDatabaseReady();
    const result = await query(
      `
        INSERT INTO ${pushSubscriptionsTable} (
          endpoint,
          recipient_username,
          p256dh_key,
          auth_key,
          expiration_time,
          user_agent
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (endpoint)
        DO UPDATE SET
          recipient_username = EXCLUDED.recipient_username,
          p256dh_key = EXCLUDED.p256dh_key,
          auth_key = EXCLUDED.auth_key,
          expiration_time = EXCLUDED.expiration_time,
          user_agent = EXCLUDED.user_agent,
          updated_at = NOW(),
          last_error = ''
      `,
      [
        normalizedSubscription.endpoint,
        normalizedRecipientUsername,
        normalizedSubscription.p256dhKey,
        normalizedSubscription.authKey,
        normalizedSubscription.expirationTime,
        userAgent,
      ],
    );
    return Number(result?.rowCount || 0) > 0;
  }

  async function deletePushSubscriptionForRecipient(recipientUsername, endpoint) {
    const normalizedRecipientUsername = normalizeUsername(recipientUsername);
    const normalizedEndpoint = sanitizeTextValue(endpoint, 2000);
    if (!normalizedRecipientUsername || !normalizedEndpoint) {
      return false;
    }

    await ensureDatabaseReady();
    const result = await query(
      `
        DELETE FROM ${pushSubscriptionsTable}
        WHERE recipient_username = $1
          AND endpoint = $2
      `,
      [normalizedRecipientUsername, normalizedEndpoint],
    );
    return Number(result?.rowCount || 0) > 0;
  }

  async function deletePushSubscriptionByEndpoint(endpoint) {
    const normalizedEndpoint = sanitizeTextValue(endpoint, 2000);
    if (!normalizedEndpoint) {
      return false;
    }

    await ensureDatabaseReady();
    const result = await query(
      `
        DELETE FROM ${pushSubscriptionsTable}
        WHERE endpoint = $1
      `,
      [normalizedEndpoint],
    );
    return Number(result?.rowCount || 0) > 0;
  }

  return {
    listNotificationsByRecipient,
    insertNotifications,
    markNotificationRead,
    markAllNotificationsRead,
    listPushSubscriptionsByRecipients,
    upsertPushSubscription,
    deletePushSubscriptionForRecipient,
    deletePushSubscriptionByEndpoint,
  };
}

module.exports = {
  createNotificationsRepo,
};
