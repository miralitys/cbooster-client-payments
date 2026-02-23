"use strict";

function registerRecordsRoutes(context) {
  const {
    app,
    requireWebPermission,
    requireOwnerOrAdminAccess,
    permissionKeys,
    handlers,
  } = context;

  app.post(
    "/api/payment-probability",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handlePaymentProbabilityPost,
  );

  app.post(
    "/api/identityiq/credit-score",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleIdentityIqCreditScorePost,
  );

  app.get("/api/health", handlers.handleHealthGet);

  app.get(
    "/api/diagnostics/performance",
    requireOwnerOrAdminAccess(),
    handlers.handlePerformanceDiagnosticsGet,
  );

  app.get(
    "/api/records",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleRecordsGet,
  );
  app.get(
    "/api/clients",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleRecordsGet,
  );

  app.put(
    "/api/records",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleRecordsPut,
  );
  app.put(
    "/api/clients",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleRecordsPut,
  );

  app.patch(
    "/api/records",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleRecordsPatch,
  );
  app.patch(
    "/api/clients",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleRecordsPatch,
  );
}

module.exports = {
  registerRecordsRoutes,
};
