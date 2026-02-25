"use strict";

function createNotificationsController(dependencies = {}) {
  const {
    notificationsService,
    sanitizeTextValue,
    buildPublicErrorPayload,
  } = dependencies;

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  if (!notificationsService) {
    throw new Error("createNotificationsController requires notificationsService.");
  }

  function resolveUsername(req) {
    return sanitize(req?.webAuthUser || req?.webAuthProfile?.username, 200).toLowerCase();
  }

  async function handleNotificationsGet(req, res) {
    const username = resolveUsername(req);
    if (!username) {
      res.status(401).json({
        error: "Authentication required.",
      });
      return;
    }

    try {
      const items = await notificationsService.getNotificationsForUser({
        username,
      });
      res.json({
        ok: true,
        items,
      });
    } catch (error) {
      console.error("GET /api/notifications failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to load notifications")
          : {
              error: sanitize(error?.message, 260) || "Failed to load notifications",
            };
      res.status(error?.httpStatus || 500).json(payload);
    }
  }

  async function handleNotificationsMarkReadPost(req, res) {
    const username = resolveUsername(req);
    if (!username) {
      res.status(401).json({
        error: "Authentication required.",
      });
      return;
    }

    const id = sanitize(req?.params?.id, 180);
    if (!id) {
      res.status(400).json({
        error: "Notification id is required.",
      });
      return;
    }

    try {
      const updated = await notificationsService.markNotificationReadForUser({
        username,
        id,
      });
      res.json({
        ok: true,
        updated,
      });
    } catch (error) {
      console.error("POST /api/notifications/:id/read failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to mark notification as read")
          : {
              error: sanitize(error?.message, 260) || "Failed to mark notification as read",
            };
      res.status(error?.httpStatus || 500).json(payload);
    }
  }

  async function handleNotificationsMarkAllReadPost(req, res) {
    const username = resolveUsername(req);
    if (!username) {
      res.status(401).json({
        error: "Authentication required.",
      });
      return;
    }

    try {
      const updated = await notificationsService.markAllNotificationsReadForUser({
        username,
      });
      res.json({
        ok: true,
        updated,
      });
    } catch (error) {
      console.error("POST /api/notifications/read-all failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to mark all notifications as read")
          : {
              error: sanitize(error?.message, 260) || "Failed to mark all notifications as read",
            };
      res.status(error?.httpStatus || 500).json(payload);
    }
  }

  return {
    handleNotificationsGet,
    handleNotificationsMarkReadPost,
    handleNotificationsMarkAllReadPost,
  };
}

module.exports = {
  createNotificationsController,
};
