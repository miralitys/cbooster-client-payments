"use strict";

function registerNotificationsRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;

  app.get(
    "/api/notifications",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsGet,
  );

  app.get(
    "/api/notifications/push/public-key",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsPushPublicKeyGet,
  );

  app.post(
    "/api/notifications/push/subscribe",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsPushSubscribePost,
  );

  app.post(
    "/api/notifications/push/unsubscribe",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsPushUnsubscribePost,
  );

  app.post(
    "/api/notifications/:id/read",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsMarkReadPost,
  );

  app.post(
    "/api/notifications/read-all",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleNotificationsMarkAllReadPost,
  );
}

module.exports = {
  registerNotificationsRoutes,
};
