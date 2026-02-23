"use strict";

const QUICKBOOKS_DASHBOARD_TIME_ZONE = "America/Chicago";

function formatDateInChicago(dateValue = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: QUICKBOOKS_DASHBOARD_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(dateValue);

  const values = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      values[part.type] = part.value;
    }
  }

  const year = String(values.year || "").trim();
  const month = String(values.month || "").trim().padStart(2, "0");
  const day = String(values.day || "").trim().padStart(2, "0");

  if (!year || !month || !day) {
    return "";
  }
  return `${year}-${month}-${day}`;
}

function isDashboardTodayRangeRequest(req) {
  const from = String(req?.query?.from || "").trim();
  const to = String(req?.query?.to || "").trim();
  if (!from || !to || from !== to) {
    return false;
  }
  return from === formatDateInChicago(new Date());
}

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
  const requireDashboardAccess = permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD
    ? requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_DASHBOARD)
    : requireQuickBooksAccess;

  function requireQuickBooksRecentReadAccess(req, res, next) {
    if (isDashboardTodayRangeRequest(req)) {
      requireDashboardAccess(req, res, next);
      return;
    }
    requireQuickBooksAccess(req, res, next);
  }

  app.all("/api/quickbooks/*", handlers.handleQuickbooksReadonlyGuard);

  app.get(
    "/api/quickbooks/payments/recent",
    requireQuickBooksRecentReadAccess,
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
