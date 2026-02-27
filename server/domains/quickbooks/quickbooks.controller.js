"use strict";

function createQuickBooksController(dependencies = {}) {
  const {
    quickBooksService,
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

  const listCachedTransactions =
    quickBooksService && typeof quickBooksService.listCachedQuickBooksTransactionsInRange === "function"
      ? quickBooksService.listCachedQuickBooksTransactionsInRange
      : listCachedQuickBooksTransactionsInRange;
  const listOutgoingTransactions =
    quickBooksService && typeof quickBooksService.listQuickBooksOutgoingTransactionsInRange === "function"
      ? quickBooksService.listQuickBooksOutgoingTransactionsInRange
      : listQuickBooksOutgoingTransactionsInRange;
  const buildSyncMeta =
    quickBooksService && typeof quickBooksService.buildQuickBooksSyncMeta === "function"
      ? quickBooksService.buildQuickBooksSyncMeta
      : buildQuickBooksSyncMeta;
  const enqueueSyncJob =
    quickBooksService && typeof quickBooksService.enqueueQuickBooksSyncJob === "function"
      ? quickBooksService.enqueueQuickBooksSyncJob
      : enqueueQuickBooksSyncJob;
  const buildSyncJobPayload =
    quickBooksService && typeof quickBooksService.buildQuickBooksSyncJobPayload === "function"
      ? quickBooksService.buildQuickBooksSyncJobPayload
      : buildQuickBooksSyncJobPayload;
  const getSyncJobById =
    quickBooksService && typeof quickBooksService.getQuickBooksSyncJobById === "function"
      ? quickBooksService.getQuickBooksSyncJobById
      : getQuickBooksSyncJobById;
  const requestInsight =
    quickBooksService && typeof quickBooksService.requestOpenAiQuickBooksInsight === "function"
      ? quickBooksService.requestOpenAiQuickBooksInsight
      : requestOpenAiQuickBooksInsight;
  const confirmPaymentMatch =
    quickBooksService && typeof quickBooksService.confirmQuickBooksPaymentMatch === "function"
      ? quickBooksService.confirmQuickBooksPaymentMatch
      : null;
  const listPendingPaymentMatchesByRecordId =
    quickBooksService && typeof quickBooksService.listPendingQuickBooksPaymentMatchesByRecordId === "function"
      ? quickBooksService.listPendingQuickBooksPaymentMatchesByRecordId
      : null;

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
    const isAllowedConfirmPost =
      req.method === "POST" &&
      (pathname === "/api/quickbooks/payments/recent/confirm" || pathname === "/payments/recent/confirm");
    if (req.method === "GET" || isAllowedSyncPost || isAllowedInsightPost || isAllowedConfirmPost) {
      next();
      return;
    }

    res.status(405).json({
      error:
        "QuickBooks integration is read-only toward QuickBooks. Use GET for reads and POST /api/quickbooks/payments/recent/sync (sync), POST /api/quickbooks/payments/recent/confirm (confirm), or POST /api/quickbooks/transaction-insight (Ask GPT).",
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
      const syncMeta = buildSyncMeta({
        requested: false,
        syncMode: "incremental",
      });

      const items = await listCachedTransactions(range.from, range.to);

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
      const syncMeta = buildSyncMeta({
        requested: false,
        syncMode: "incremental",
      });
      const items = await listOutgoingTransactions(range.from, range.to);

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
      enqueueResult = enqueueSyncJob(range, {
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
      job: buildSyncJobPayload(job),
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

    const job = getSyncJobById(req.params.jobId);
    if (!job) {
      res.status(404).json({
        error: "QuickBooks sync job not found.",
        code: "quickbooks_sync_job_not_found",
      });
      return;
    }

    res.json({
      ok: true,
      job: buildSyncJobPayload(job),
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
      const insight = await requestInsight({
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

  async function handleQuickBooksPendingConfirmationsGet(req, res) {
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

    const recordId = sanitizeTextValue(req.query?.recordId, 180);
    if (!recordId) {
      res.status(400).json({
        error: "recordId is required.",
        code: "quickbooks_pending_record_id_required",
      });
      return;
    }

    try {
      const rows = listPendingPaymentMatchesByRecordId
        ? await listPendingPaymentMatchesByRecordId(recordId)
        : [];

      const items = (Array.isArray(rows) ? rows : []).map((row) => ({
        transactionType: sanitizeTextValue(row?.transaction_type, 40).toLowerCase() || "payment",
        transactionId: sanitizeTextValue(row?.transaction_id, 160),
        matchedPaymentField: sanitizeTextValue(row?.matched_payment_field, 40),
        matchedPaymentDateField: sanitizeTextValue(row?.matched_payment_date_field, 40),
        paymentAmount: Number(row?.payment_amount) || 0,
        paymentDate: sanitizeTextValue(row?.payment_date, 20),
      }));

      res.json({
        ok: true,
        recordId,
        count: items.length,
        items,
      });
    } catch (error) {
      console.error("GET /api/quickbooks/payments/pending-confirmations failed:", error);
      res.status(error?.httpStatus || 502).json({
        error: sanitizeTextValue(error?.message, 600) || "Failed to load pending payment confirmations.",
        code: sanitizeTextValue(error?.code, 80) || "quickbooks_pending_confirmations_failed",
      });
    }
  }

  async function handleQuickBooksRecentPaymentsConfirmPost(req, res) {
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

    if (
      typeof hasWebAuthPermission === "function" &&
      !hasWebAuthPermission(req.webAuthProfile, webAuthPermissionSyncQuickbooks)
    ) {
      res.status(403).json({
        error: "Access denied. You do not have permission to confirm QuickBooks payments.",
      });
      return;
    }

    if (!confirmPaymentMatch) {
      res.status(503).json({
        error: "QuickBooks payment confirmation is unavailable.",
        code: "quickbooks_confirm_unavailable",
      });
      return;
    }

    const transactionId = sanitizeTextValue(req.body?.transactionId, 160);
    const transactionType = sanitizeTextValue(req.body?.transactionType, 40).toLowerCase() || "payment";
    if (!transactionId) {
      res.status(400).json({
        error: "transactionId is required.",
        code: "quickbooks_confirm_transaction_id_required",
      });
      return;
    }

    try {
      const item = await confirmPaymentMatch({
        transactionType,
        transactionId,
        confirmedBy: req.webAuthUser || req.webAuthProfile?.username || "unknown",
      });

      if (!item) {
        res.status(404).json({
          error: "QuickBooks payment match not found.",
          code: "quickbooks_confirm_not_found",
        });
        return;
      }

      res.json({
        ok: true,
        item: {
          transactionType: sanitizeTextValue(item?.transaction_type, 40).toLowerCase() || transactionType,
          transactionId: sanitizeTextValue(item?.transaction_id, 160) || transactionId,
          matchedRecordId: sanitizeTextValue(item?.matched_record_id, 180),
          matchedPaymentField: sanitizeTextValue(item?.matched_payment_field, 40),
          matchedPaymentDateField: sanitizeTextValue(item?.matched_payment_date_field, 40),
          matchedConfirmed: true,
          matchedConfirmedAt: sanitizeTextValue(item?.matched_confirmed_at, 80),
          matchedConfirmedBy: sanitizeTextValue(item?.matched_confirmed_by, 200),
        },
      });
    } catch (error) {
      console.error("POST /api/quickbooks/payments/recent/confirm failed:", error);
      res.status(error?.httpStatus || 502).json({
        error: sanitizeTextValue(error?.message, 600) || "Failed to confirm QuickBooks payment.",
        code: sanitizeTextValue(error?.code, 80) || "quickbooks_confirm_failed",
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
    handleQuickBooksPendingConfirmationsGet,
    handleQuickBooksRecentPaymentsConfirmPost,
  };
}

module.exports = {
  createQuickBooksController,
};
