"use strict";

function registerModerationRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;

  app.get(
    "/api/moderation/submissions",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_MODERATION),
    handlers.handleModerationSubmissionsGet,
  );

  app.get(
    "/api/moderation/submissions/:id/files",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_MODERATION),
    handlers.handleModerationSubmissionFilesGet,
  );

  app.get(
    "/api/moderation/submissions/:id/files/:fileId",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_MODERATION),
    handlers.handleModerationSubmissionFileGet,
  );

  app.post(
    "/api/moderation/submissions/:id/approve",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_REVIEW_MODERATION),
    handlers.handleModerationApprovePost,
  );

  app.post(
    "/api/moderation/submissions/:id/reject",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_REVIEW_MODERATION),
    handlers.handleModerationRejectPost,
  );
}

module.exports = {
  registerModerationRoutes,
};
