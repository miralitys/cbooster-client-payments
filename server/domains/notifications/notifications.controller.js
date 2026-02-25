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

  async function handleNotificationsPushPublicKeyGet(_req, res) {
    try {
      const pushConfig =
        typeof notificationsService.getPushPublicKeyConfig === "function"
          ? notificationsService.getPushPublicKeyConfig()
          : {
              enabled: false,
              publicKey: "",
            };

      res.json({
        ok: true,
        enabled: pushConfig.enabled === true,
        publicKey: sanitize(pushConfig.publicKey, 400),
      });
    } catch (error) {
      console.error("GET /api/notifications/push/public-key failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to load push notifications config")
          : {
              error: sanitize(error?.message, 260) || "Failed to load push notifications config",
            };
      res.status(error?.httpStatus || 500).json(payload);
    }
  }

  async function handleNotificationsPushSubscribePost(req, res) {
    const username = resolveUsername(req);
    if (!username) {
      res.status(401).json({
        error: "Authentication required.",
      });
      return;
    }

    try {
      const subscribed =
        typeof notificationsService.upsertPushSubscriptionForUser === "function"
          ? await notificationsService.upsertPushSubscriptionForUser({
              username,
              subscription: req?.body?.subscription,
              userAgent: req?.headers?.["user-agent"],
            })
          : false;

      res.json({
        ok: true,
        subscribed: subscribed === true,
      });
    } catch (error) {
      console.error("POST /api/notifications/push/subscribe failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to subscribe to push notifications")
          : {
              error: sanitize(error?.message, 260) || "Failed to subscribe to push notifications",
            };
      res.status(error?.httpStatus || 500).json(payload);
    }
  }

  async function handleNotificationsPushUnsubscribePost(req, res) {
    const username = resolveUsername(req);
    if (!username) {
      res.status(401).json({
        error: "Authentication required.",
      });
      return;
    }

    const endpoint = sanitize(req?.body?.endpoint || req?.body?.subscription?.endpoint, 2000);
    if (!endpoint) {
      res.status(400).json({
        error: "Subscription endpoint is required.",
      });
      return;
    }

    try {
      const unsubscribed =
        typeof notificationsService.removePushSubscriptionForUser === "function"
          ? await notificationsService.removePushSubscriptionForUser({
              username,
              endpoint,
            })
          : false;

      res.json({
        ok: true,
        unsubscribed: unsubscribed === true,
      });
    } catch (error) {
      console.error("POST /api/notifications/push/unsubscribe failed:", error);
      const payload =
        typeof buildPublicErrorPayload === "function"
          ? buildPublicErrorPayload(error, "Failed to unsubscribe from push notifications")
          : {
              error: sanitize(error?.message, 260) || "Failed to unsubscribe from push notifications",
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
    handleNotificationsPushPublicKeyGet,
    handleNotificationsPushSubscribePost,
    handleNotificationsPushUnsubscribePost,
    handleNotificationsMarkReadPost,
    handleNotificationsMarkAllReadPost,
  };
}

module.exports = {
  createNotificationsController,
};
