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
      const result = await recordsService.getRecordsForApi({
        webAuthProfile: req.webAuthProfile,
        webAuthUser: req.webAuthUser,
        pagination,
      });
      res.status(result.status).json(result.body);
    } catch (error) {
      console.error("GET /api/records failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load records"));
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
    if (!recordsPatchEnabled) {
      res.status(404).json({
        error: "API route not found",
        code: "records_patch_disabled",
      });
      return;
    }

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

    try {
      const result = await recordsService.patchRecordsForApi({
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
