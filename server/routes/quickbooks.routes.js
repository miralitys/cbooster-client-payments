"use strict";

function registerQuickBooksRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;

  app.all("/api/quickbooks/*", handlers.handleQuickbooksReadonlyGuard);

  app.get(
    "/api/quickbooks/payments/recent",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS),
    handlers.handleQuickBooksRecentPaymentsGet,
  );

  app.get(
    "/api/quickbooks/payments/outgoing",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS),
    handlers.handleQuickBooksOutgoingPaymentsGet,
  );

  app.post(
    "/api/quickbooks/payments/recent/sync",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS),
    handlers.handleQuickBooksRecentPaymentsSyncPost,
  );

  app.get(
    "/api/quickbooks/payments/recent/sync-jobs/:jobId",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS),
    handlers.handleQuickBooksSyncJobGet,
  );

  app.post(
    "/api/quickbooks/transaction-insight",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS),
    handlers.handleQuickBooksTransactionInsightPost,
  );
}

module.exports = {
  registerQuickBooksRoutes,
};
