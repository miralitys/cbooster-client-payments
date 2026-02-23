"use strict";

function registerQuickBooksRoutes(context) {
  const {
    app,
    requireOwnerOrAdminAccess,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;
  const requireQuickBooksAccess = typeof requireOwnerOrAdminAccess === "function"
    ? requireOwnerOrAdminAccess("Owner or admin access is required.")
    : requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS);

  app.all("/api/quickbooks/*", handlers.handleQuickbooksReadonlyGuard);

  app.get(
    "/api/quickbooks/payments/recent",
    requireQuickBooksAccess,
    handlers.handleQuickBooksRecentPaymentsGet,
  );

  app.get(
    "/api/quickbooks/payments/outgoing",
    requireQuickBooksAccess,
    handlers.handleQuickBooksOutgoingPaymentsGet,
  );

  app.post(
    "/api/quickbooks/payments/recent/sync",
    requireQuickBooksAccess,
    handlers.handleQuickBooksRecentPaymentsSyncPost,
  );

  app.get(
    "/api/quickbooks/payments/recent/sync-jobs/:jobId",
    requireQuickBooksAccess,
    handlers.handleQuickBooksSyncJobGet,
  );

  app.post(
    "/api/quickbooks/transaction-insight",
    requireQuickBooksAccess,
    handlers.handleQuickBooksTransactionInsightPost,
  );
}

module.exports = {
  registerQuickBooksRoutes,
};
