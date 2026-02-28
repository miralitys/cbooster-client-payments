"use strict";

function createRecordsController(dependencies = {}) {
  const {
    enforceRateLimit,
    rateLimitProfileApiRecordsWrite,
    recordsPatchEnabled,
    validateRecordsPayload,
    validateRecordsPatchPayload,
    normalizeExpectedUpdatedAtFromRequest,
    recordsService,
    buildPublicErrorPayload,
    resolveDbHttpStatus,
    canAccessClientHealthProfile,
  } = dependencies;

  function enforceRecordsWriteRateLimit(req, res) {
    return enforceRateLimit(req, res, {
      scope: "api.records.write",
      ipProfile: {
        windowMs: rateLimitProfileApiRecordsWrite.windowMs,
        maxHits: rateLimitProfileApiRecordsWrite.maxHitsIp,
        blockMs: rateLimitProfileApiRecordsWrite.blockMs,
      },
      userProfile: {
        windowMs: rateLimitProfileApiRecordsWrite.windowMs,
        maxHits: rateLimitProfileApiRecordsWrite.maxHitsUser,
        blockMs: rateLimitProfileApiRecordsWrite.blockMs,
      },
      message: "Save request limit reached. Please wait before retrying.",
      code: "records_write_rate_limited",
    });
  }

  async function handleRecordsGet(req, res) {
    try {
      const isClientsRoute = req.path === "/api/clients" || req.originalUrl?.startsWith("/api/clients");
      const pagination = isClientsRoute ? resolvePaginationFromQuery(req.query) : null;
      const clientFilters = isClientsRoute ? resolveClientFiltersFromQuery(req.query) : null;
      const result = await recordsService.getRecordsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
        pagination,
        clientFilters,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/records failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load records"));
    }
  }

  async function handleClientHealthGet(req, res) {
    if (typeof canAccessClientHealthProfile === "function" && !canAccessClientHealthProfile(req.webAuthProfile)) {
      res.status(403).json({
        error: "Access denied. Owner, admin, or Client Service Department Head role is required.",
      });
      return;
    }

    try {
      const result = await recordsService.getClientHealthSnapshotForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/client-health failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load client health records"));
    }
  }

  async function handleClientsFiltersGet(req, res) {
    try {
      const result = await recordsService.getClientFilterOptionsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/clients/filters failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load client filters"));
    }
  }

  async function handleClientsTotalsGet(req, res) {
    try {
      const result = await recordsService.getClientTotalsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
        clientFilters: resolveClientFiltersFromQuery(req.query),
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/clients/totals failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load client totals"));
    }
  }

  async function handleClientManagerKpiGet(req, res) {
    try {
      const result = await recordsService.getClientManagerKpiForApi({
        webAuthProfile: req.webAuthProfile,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/clients/kpi-client-manager failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load KPI data"));
    }
  }

  async function handleRecordsPut(req, res) {
    if (!enforceRecordsWriteRateLimit(req, res)) {
      return;
    }

    const validationResult = validateRecordsPayload(req.body?.records);
    if (!validationResult.ok) {
      res.status(validationResult.httpStatus || 400).json({
        error: validationResult.message,
        code: validationResult.code,
      });
      return;
    }

    const expectedUpdatedAtResult = normalizeExpectedUpdatedAtFromRequest(req.body || {});
    if (!expectedUpdatedAtResult.ok) {
      res.status(expectedUpdatedAtResult.status || 400).json({
        error: expectedUpdatedAtResult.error || "Invalid expectedUpdatedAt.",
        code: expectedUpdatedAtResult.code || "invalid_expected_updated_at",
      });
      return;
    }

    try {
      const result = await recordsService.saveRecordsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
        records: validationResult.records,
        expectedUpdatedAt: expectedUpdatedAtResult.expectedUpdatedAt,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("PUT /api/records failed:", error);
      const payload = buildPublicErrorPayload(error, "Failed to save records");
      if (Object.prototype.hasOwnProperty.call(error || {}, "currentUpdatedAt")) {
        payload.updatedAt = error.currentUpdatedAt || null;
      }
      res.status(error.httpStatus || resolveDbHttpStatus(error)).json(payload);
    }
  }

  async function handleRecordsPatch(req, res) {
    if (!enforceRecordsWriteRateLimit(req, res)) {
      return;
    }

    const expectedUpdatedAtResult = normalizeExpectedUpdatedAtFromRequest(req.body || {});
    if (!expectedUpdatedAtResult.ok) {
      res.status(expectedUpdatedAtResult.status || 400).json({
        error: expectedUpdatedAtResult.error || "Invalid expectedUpdatedAt.",
        code: expectedUpdatedAtResult.code || "invalid_expected_updated_at",
      });
      return;
    }

    const validationResult = validateRecordsPatchPayload(req.body || {});
    if (!validationResult.ok) {
      res.status(validationResult.httpStatus || 400).json({
        error: validationResult.message,
        code: validationResult.code,
      });
      return;
    }

    if (!recordsPatchEnabled) {
      const hasNonUpsertOperation = validationResult.operations.some((operation) => operation?.type !== "upsert");
      if (hasNonUpsertOperation) {
        res.status(404).json({
          error: "API route not found",
          code: "records_patch_disabled",
        });
        return;
      }
    }

    try {
      const result = await recordsService.patchRecordsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
        operations: validationResult.operations,
        expectedUpdatedAt: expectedUpdatedAtResult.expectedUpdatedAt,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("PATCH /api/records failed:", error);
      const payload = buildPublicErrorPayload(error, "Failed to patch records");
      if (Object.prototype.hasOwnProperty.call(error || {}, "currentUpdatedAt")) {
        payload.updatedAt = error.currentUpdatedAt || null;
      }
      res.status(error.httpStatus || resolveDbHttpStatus(error)).json(payload);
    }
  }

  return {
    handleRecordsGet,
    handleClientHealthGet,
    handleClientsFiltersGet,
    handleClientsTotalsGet,
    handleClientManagerKpiGet,
    handleRecordsPut,
    handleRecordsPatch,
  };
}

module.exports = {
  createRecordsController,
};

function resolvePaginationFromQuery(query) {
  const rawLimit = parsePositiveInteger(query?.limit);
  const rawOffset = parseNonNegativeInteger(query?.offset);

  if (rawLimit === null && rawOffset === null) {
    return null;
  }

  const limit = clampInteger(rawLimit === null ? 100 : rawLimit, 1, 500);
  const offset = Math.max(0, rawOffset === null ? 0 : rawOffset);

  return {
    enabled: true,
    limit,
    offset,
  };
}

function parsePositiveInteger(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return null;
  }

  const parsed = Number.parseInt(String(rawValue), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }

  return parsed;
}

function parseNonNegativeInteger(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return null;
  }

  const parsed = Number.parseInt(String(rawValue), 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }

  return parsed;
}

function clampInteger(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function resolveClientFiltersFromQuery(query) {
  return {
    search: sanitizeFilterValue(query?.search, 320),
    closedBy: sanitizeFilterValue(query?.closedBy, 180),
    clientManager: sanitizeFilterValue(query?.clientManager, 180),
    status: sanitizeFilterValue(query?.status, 40),
    overdueRange: sanitizeFilterValue(query?.overdueRange, 40),
    createdFrom: sanitizeFilterValue(query?.createdFrom, 24),
    createdTo: sanitizeFilterValue(query?.createdTo, 24),
    paymentFrom: sanitizeFilterValue(query?.paymentFrom, 24),
    paymentTo: sanitizeFilterValue(query?.paymentTo, 24),
    writtenOffFrom: sanitizeFilterValue(query?.writtenOffFrom, 24),
    writtenOffTo: sanitizeFilterValue(query?.writtenOffTo, 24),
    fullyPaidFrom: sanitizeFilterValue(query?.fullyPaidFrom, 24),
    fullyPaidTo: sanitizeFilterValue(query?.fullyPaidTo, 24),
    activeOnly: parseBooleanFilterValue(query?.activeOnly),
    excludeWrittenOff: parseBooleanFilterValue(query?.excludeWrittenOff),
  };
}

function sanitizeFilterValue(rawValue, maxLen) {
  if (rawValue === null || rawValue === undefined) {
    return "";
  }
  const value = String(rawValue).trim();
  if (!value) {
    return "";
  }
  if (!Number.isFinite(maxLen) || maxLen <= 0) {
    return value;
  }
  return value.slice(0, maxLen);
}

function parseBooleanFilterValue(rawValue) {
  if (rawValue === null || rawValue === undefined) {
    return null;
  }
  const value = String(rawValue).trim().toLowerCase();
  if (!value) {
    return null;
  }
  if (value === "1" || value === "true" || value === "yes" || value === "on") {
    return true;
  }
  if (value === "0" || value === "false" || value === "no" || value === "off") {
    return false;
  }
  return null;
}
