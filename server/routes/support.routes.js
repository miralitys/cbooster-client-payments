"use strict";

function registerSupportRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
    supportUpload,
  } = context;

  app.get(
    "/api/support/requests",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportRequestsGet,
  );

  app.post(
    "/api/support/requests",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    supportUpload,
    handlers.handleSupportRequestPost,
  );

  app.get(
    "/api/support/requests/:id",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportRequestGet,
  );

  app.patch(
    "/api/support/requests/:id",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportRequestPatch,
  );

  app.post(
    "/api/support/requests/:id/actions/move-to",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    supportUpload,
    handlers.handleSupportRequestMoveToPost,
  );

  app.post(
    "/api/support/requests/:id/attachments",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    supportUpload,
    handlers.handleSupportRequestAttachmentsPost,
  );

  app.post(
    "/api/support/requests/:id/comments",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportRequestCommentPost,
  );

  app.get(
    "/api/support/reports",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportReportsGet,
  );

  app.get(
    "/api/support/attachments/:id",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportAttachmentGet,
  );

  app.get(
    "/api/support/stream",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD),
    handlers.handleSupportStreamGet,
  );
}

module.exports = {
  registerSupportRoutes,
};
