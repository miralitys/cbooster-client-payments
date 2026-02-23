"use strict";

function createQuickBooksController(dependencies = {}) {
  const {
    sanitizeTextValue,
    enforceRateLimit,
    rateLimitProfileApiExpensive,
    rateLimitProfileApiSync,
    hasDatabase,
    isQuickBooksConfigured,
    getQuickBooksDateRange,
    parseQuickBooksSyncFlag,
    parseQuickBooksTotalRefreshFlag,
    listCachedQuickBooksTransactionsInRange,
    listQuickBooksOutgoingTransactionsInRange,
    buildQuickBooksSyncMeta,
    enqueueQuickBooksSyncJob,
    buildQuickBooksSyncJobPayload,
    getQuickBooksSyncJobById,
    hasWebAuthPermission,
    webAuthPermissionSyncQuickbooks,
    requestOpenAiQuickBooksInsight,
  } = dependencies;

  function hasDatabaseConfigured() {
    if (typeof hasDatabase === "function") {
      return hasDatabase();
    }
    return Boolean(hasDatabase);
  }

  function resolveQuickBooksDateRangeFromRequest(req, source = "query") {
    const payload = source === "body" ? req.body : req.query;
    return getQuickBooksDateRange(payload?.from, payload?.to);
  }

  function handleQuickbooksReadonlyGuard(req, res, next) {
    const pathname = sanitizeTextValue(req.path, 260);
    const isAllowedSyncPost =
      req.method === "POST" &&
      (pathname === "/api/quickbooks/payments/recent/sync" || pathname === "/payments/recent/sync");
    const isAllowedInsightPost =
      req.method === "POST" &&
      (pathname === "/api/quickbooks/transaction-insight" || pathname === "/transaction-insight");
    if (req.method === "GET" || isAllowedSyncPost || isAllowedInsightPost) {
      next();
      return;
    }

    res.status(405).json({
      error:
        "QuickBooks integration is read-only toward QuickBooks. Use GET for reads and POST /api/quickbooks/payments/recent/sync (sync) or POST /api/quickbooks/transaction-insight (Ask GPT).",
    });
  }

  async function respondQuickBooksRecentPayments(req, res, options = {}) {
    const range = options.range;
    const routeLabel = sanitizeTextValue(options.routeLabel, 120) || "api/quickbooks/payments/recent";

    if (
      !enforceRateLimit(req, res, {
        scope: "api.quickbooks.read",
        ipProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsIp,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        userProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsUser,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        message: "QuickBooks request limit reached. Please wait before retrying.",
        code: "quickbooks_rate_limited",
      })
    ) {
      return;
    }

    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    try {
      const syncMeta = buildQuickBooksSyncMeta({
        requested: false,
        syncMode: "incremental",
      });

      const items = await listCachedQuickBooksTransactionsInRange(range.from, range.to);

      res.json({
        ok: true,
        range: {
          from: range.from,
          to: range.to,
        },
        count: items.length,
        items,
        source: "cache",
        sync: syncMeta,
      });
    } catch (error) {
      console.error(`${routeLabel} failed:`, error);
      res.status(error.httpStatus || 502).json({
        error: sanitizeTextValue(error?.message, 600) || "Failed to load QuickBooks payments.",
      });
    }
  }

  async function respondQuickBooksOutgoingPayments(req, res, options = {}) {
    const range = options.range;
    const routeLabel = sanitizeTextValue(options.routeLabel, 120) || "api/quickbooks/payments/outgoing";

    if (
      !enforceRateLimit(req, res, {
        scope: "api.quickbooks.read",
        ipProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsIp,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        userProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsUser,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        message: "QuickBooks request limit reached. Please wait before retrying.",
        code: "quickbooks_rate_limited",
      })
    ) {
      return;
    }

    if (!isQuickBooksConfigured()) {
      res.status(503).json({
        error:
          "QuickBooks is not configured. Set QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET, QUICKBOOKS_REFRESH_TOKEN, and QUICKBOOKS_REALM_ID.",
      });
      return;
    }

    try {
      const syncMeta = buildQuickBooksSyncMeta({
        requested: false,
        syncMode: "incremental",
      });
      const items = await listQuickBooksOutgoingTransactionsInRange(range.from, range.to);

      res.json({
        ok: true,
        range: {
          from: range.from,
          to: range.to,
        },
        count: items.length,
        items,
        source: "quickbooks_live",
        sync: syncMeta,
      });
    } catch (error) {
      console.error(`${routeLabel} failed:`, error);
      res.status(error.httpStatus || 502).json({
        error: sanitizeTextValue(error?.message, 600) || "Failed to load QuickBooks outgoing transactions.",
      });
    }
  }

  async function handleQuickBooksRecentPaymentsGet(req, res) {
    const syncRequestedOnGet =
      parseQuickBooksSyncFlag(req.query.sync) ||
      parseQuickBooksTotalRefreshFlag(req.query.fullSync || req.query.totalRefresh);
    if (syncRequestedOnGet) {
      res.status(405).json({
        error: "State-changing sync is not allowed via GET. Use POST /api/quickbooks/payments/recent/sync.",
        code: "method_not_allowed_for_sync",
      });
      return;
    }

    let range;
    try {
      range = resolveQuickBooksDateRangeFromRequest(req, "query");
    } catch (error) {
      res.status(error.httpStatus || 400).json({
        error: sanitizeTextValue(error?.message, 300) || "Invalid date range.",
      });
      return;
    }

    await respondQuickBooksRecentPayments(req, res, {
      range,
      routeLabel: "GET /api/quickbooks/payments/recent",
    });
  }

  async function handleQuickBooksOutgoingPaymentsGet(req, res) {
    const syncRequestedOnGet =
      parseQuickBooksSyncFlag(req.query.sync) ||
      parseQuickBooksTotalRefreshFlag(req.query.fullSync || req.query.totalRefresh);
    if (syncRequestedOnGet) {
      res.status(405).json({
        error: "State-changing sync is not allowed via GET. Use POST /api/quickbooks/payments/recent/sync.",
        code: "method_not_allowed_for_sync",
      });
      return;
    }

    let range;
    try {
      range = resolveQuickBooksDateRangeFromRequest(req, "query");
    } catch (error) {
      res.status(error.httpStatus || 400).json({
        error: sanitizeTextValue(error?.message, 300) || "Invalid date range.",
      });
      return;
    }

    await respondQuickBooksOutgoingPayments(req, res, {
      range,
      routeLabel: "GET /api/quickbooks/payments/outgoing",
    });
  }

  async function handleQuickBooksRecentPaymentsSyncPost(req, res) {
    const shouldTotalRefresh = parseQuickBooksTotalRefreshFlag(req.body?.fullSync || req.body?.totalRefresh);
    let range;
    try {
      range = resolveQuickBooksDateRangeFromRequest(req, "body");
    } catch (error) {
      res.status(error.httpStatus || 400).json({
        error: sanitizeTextValue(error?.message, 300) || "Invalid date range.",
      });
      return;
    }

    if (
      !enforceRateLimit(req, res, {
        scope: "api.quickbooks.sync",
        ipProfile: {
          windowMs: rateLimitProfileApiSync.windowMs,
          maxHits: rateLimitProfileApiSync.maxHitsIp,
          blockMs: rateLimitProfileApiSync.blockMs,
        },
        userProfile: {
          windowMs: rateLimitProfileApiSync.windowMs,
          maxHits: rateLimitProfileApiSync.maxHitsUser,
          blockMs: rateLimitProfileApiSync.blockMs,
        },
        message: "QuickBooks request limit reached. Please wait before retrying.",
        code: "quickbooks_rate_limited",
      })
    ) {
      return;
    }

    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    if (!hasWebAuthPermission(req.webAuthProfile, webAuthPermissionSyncQuickbooks)) {
      res.status(403).json({
        error: "Access denied. You do not have permission to refresh QuickBooks data.",
      });
      return;
    }

    if (!isQuickBooksConfigured()) {
      res.status(503).json({
        error:
          "QuickBooks is not configured. Set QUICKBOOKS_CLIENT_ID, QUICKBOOKS_CLIENT_SECRET, QUICKBOOKS_REFRESH_TOKEN, and QUICKBOOKS_REALM_ID.",
      });
      return;
    }

    let enqueueResult;
    try {
      enqueueResult = enqueueQuickBooksSyncJob(range, {
        fullSync: shouldTotalRefresh,
        requestedBy: req.webAuthUser,
      });
    } catch (error) {
      res.status(error?.httpStatus || 429).json({
        error: sanitizeTextValue(error?.message, 400) || "QuickBooks sync queue is unavailable.",
        code: sanitizeTextValue(error?.code, 60) || "quickbooks_sync_queue_error",
      });
      return;
    }

    const { job, reused } = enqueueResult;
    res.status(202).json({
      ok: true,
      queued: true,
      reused,
      job: buildQuickBooksSyncJobPayload(job),
    });
  }

  function handleQuickBooksSyncJobGet(req, res) {
    if (
      !enforceRateLimit(req, res, {
        scope: "api.quickbooks.read",
        ipProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsIp,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        userProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsUser,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        message: "QuickBooks request limit reached. Please wait before retrying.",
        code: "quickbooks_rate_limited",
      })
    ) {
      return;
    }

    const job = getQuickBooksSyncJobById(req.params.jobId);
    if (!job) {
      res.status(404).json({
        error: "QuickBooks sync job not found.",
        code: "quickbooks_sync_job_not_found",
      });
      return;
    }

    res.json({
      ok: true,
      job: buildQuickBooksSyncJobPayload(job),
    });
  }

  async function handleQuickBooksTransactionInsightPost(req, res) {
    if (
      !enforceRateLimit(req, res, {
        scope: "api.quickbooks.read",
        ipProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsIp,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        userProfile: {
          windowMs: rateLimitProfileApiExpensive.windowMs,
          maxHits: rateLimitProfileApiExpensive.maxHitsUser,
          blockMs: rateLimitProfileApiExpensive.blockMs,
        },
        message: "QuickBooks request limit reached. Please wait before retrying.",
        code: "quickbooks_rate_limited",
      })
    ) {
      return;
    }

    const companyName = sanitizeTextValue(req.body?.companyName, 300);
    if (!companyName) {
      res.status(400).json({
        error: "companyName is required.",
        code: "quickbooks_insight_invalid_payload",
      });
      return;
    }

    const amount = Number.parseFloat(req.body?.amount);
    if (!Number.isFinite(amount)) {
      res.status(400).json({
        error: "amount must be a valid number.",
        code: "quickbooks_insight_invalid_payload",
      });
      return;
    }

    const date = sanitizeTextValue(req.body?.date, 80) || "-";
    const description = sanitizeTextValue(req.body?.description, 1200) || "-";

    try {
      const insight = await requestOpenAiQuickBooksInsight({
        companyName,
        amount,
        date,
        description,
      });

      res.json({
        ok: true,
        insight,
      });
    } catch (error) {
      console.error("POST /api/quickbooks/transaction-insight failed:", error);
      res.status(error?.httpStatus || 502).json({
        error: sanitizeTextValue(error?.message, 600) || "Failed to generate transaction insight.",
        code: sanitizeTextValue(error?.code, 80) || "quickbooks_insight_failed",
      });
    }
  }

  return {
    handleQuickbooksReadonlyGuard,
    handleQuickBooksRecentPaymentsGet,
    handleQuickBooksOutgoingPaymentsGet,
    handleQuickBooksRecentPaymentsSyncPost,
    handleQuickBooksSyncJobGet,
    handleQuickBooksTransactionInsightPost,
  };
}

module.exports = {
  createQuickBooksController,
};
