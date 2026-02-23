"use strict";

function registerAssistantRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;

  app.post(
    "/api/assistant/context/reset",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleAssistantContextResetPost,
  );

  app.post(
    "/api/assistant/context/reset/telemetry",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleAssistantContextResetTelemetryPost,
  );

  app.post(
    "/api/assistant/chat",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleAssistantChatPost,
  );

  app.get(
    "/api/assistant/reviews",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleAssistantReviewsList,
  );

  app.put(
    "/api/assistant/reviews/:id",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleAssistantReviewUpdate,
  );

  app.post(
    "/api/assistant/tts",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleAssistantTtsPost,
  );
}

module.exports = {
  registerAssistantRoutes,
};
