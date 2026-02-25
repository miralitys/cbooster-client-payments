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
