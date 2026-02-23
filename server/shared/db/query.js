"use strict";

const DB_METRICS_CLIENT_PATCHED_FLAG = Symbol("dbMetricsClientPatched");
const MAX_REQUEST_ID_LENGTH = 140;

function createRollingLatencySample(maxSize) {
  const normalizedMaxSize = Math.max(1, Number.parseInt(maxSize, 10) || 1);
  return {
    maxSize: normalizedMaxSize,
    values: new Array(normalizedMaxSize),
    cursor: 0,
    filled: 0,
  };
}

function pushRollingLatencySample(sample, value) {
  if (!sample || !Number.isFinite(value)) {
    return;
  }

  sample.values[sample.cursor] = value;
  sample.cursor = (sample.cursor + 1) % sample.maxSize;
  if (sample.filled < sample.maxSize) {
    sample.filled += 1;
  }
}

function resolveDbStatementType(rawQuery) {
  if (typeof rawQuery === "string") {
    const match = rawQuery.trim().match(/^([a-zA-Z]+)/);
    return match ? match[1].toUpperCase() : "UNKNOWN";
  }

  if (rawQuery && typeof rawQuery === "object" && typeof rawQuery.text === "string") {
    const match = rawQuery.text.trim().match(/^([a-zA-Z]+)/);
    return match ? match[1].toUpperCase() : "UNKNOWN";
  }

  return "UNKNOWN";
}

function recordDbPerformanceMetric(state, { durationMs, error, statementType }) {
  if (!state?.enabled || !Number.isFinite(durationMs)) {
    return;
  }

  const dbState = state.db;
  const normalizedStatementType = (statementType || "UNKNOWN").toString().toUpperCase().slice(0, 24) || "UNKNOWN";
  const isError = Boolean(error);
  const isSlow = durationMs >= dbState.slowQueryMs;

  dbState.queryCount += 1;
  if (isError) {
    dbState.errorCount += 1;
  }
  if (isSlow) {
    dbState.slowQueryCount += 1;
  }
  dbState.totalDurationMs += durationMs;
  dbState.maxDurationMs = Math.max(dbState.maxDurationMs, durationMs);
  dbState.lastDurationMs = durationMs;
  pushRollingLatencySample(dbState.latencySample, durationMs);

  let statementEntry = dbState.byStatementType.get(normalizedStatementType);
  if (!statementEntry) {
    statementEntry = {
      statementType: normalizedStatementType,
      count: 0,
      errorCount: 0,
      slowCount: 0,
      totalDurationMs: 0,
      maxDurationMs: 0,
      lastDurationMs: 0,
      latencySample: createRollingLatencySample(dbState.sampleSize),
    };
    dbState.byStatementType.set(normalizedStatementType, statementEntry);
  }

  statementEntry.count += 1;
  if (isError) {
    statementEntry.errorCount += 1;
  }
  if (isSlow) {
    statementEntry.slowCount += 1;
  }
  statementEntry.totalDurationMs += durationMs;
  statementEntry.maxDurationMs = Math.max(statementEntry.maxDurationMs, durationMs);
  statementEntry.lastDurationMs = durationMs;
  pushRollingLatencySample(statementEntry.latencySample, durationMs);
}

function wrapDbQueryWithMetrics(queryFn, state) {
  return function wrappedDbQuery(...args) {
    if (!state?.enabled) {
      return queryFn(...args);
    }

    const statementType = resolveDbStatementType(args[0]);
    const startedAtNs = process.hrtime.bigint();
    const finalize = (error) => {
      const endedAtNs = process.hrtime.bigint();
      const durationMs = Number(endedAtNs - startedAtNs) / 1_000_000;
      recordDbPerformanceMetric(state, {
        durationMs,
        error,
        statementType,
      });
    };

    const maybeCallback = args.length ? args[args.length - 1] : null;
    if (typeof maybeCallback === "function") {
      let callbackFinished = false;
      args[args.length - 1] = function wrappedQueryCallback(error, ...callbackArgs) {
        if (!callbackFinished) {
          callbackFinished = true;
          finalize(error);
        }
        return maybeCallback.call(this, error, ...callbackArgs);
      };

      try {
        return queryFn(...args);
      } catch (error) {
        if (!callbackFinished) {
          callbackFinished = true;
          finalize(error);
        }
        throw error;
      }
    }

    try {
      const result = queryFn(...args);
      if (result && typeof result.then === "function") {
        return result.then(
          (value) => {
            finalize(null);
            return value;
          },
          (error) => {
            finalize(error);
            throw error;
          },
        );
      }

      finalize(null);
      return result;
    } catch (error) {
      finalize(error);
      throw error;
    }
  };
}

function instrumentDbPoolWithMetrics(basePool, state) {
  if (!basePool || !state?.enabled) {
    return basePool;
  }

  if (typeof basePool.query === "function") {
    basePool.query = wrapDbQueryWithMetrics(basePool.query.bind(basePool), state);
  }

  if (typeof basePool.connect === "function") {
    const originalConnect = basePool.connect.bind(basePool);
    basePool.connect = async (...args) => {
      const client = await originalConnect(...args);
      if (!client || typeof client.query !== "function" || client[DB_METRICS_CLIENT_PATCHED_FLAG]) {
        return client;
      }

      client.query = wrapDbQueryWithMetrics(client.query.bind(client), state);
      try {
        client[DB_METRICS_CLIENT_PATCHED_FLAG] = true;
      } catch (_error) {
        // Ignore non-extensible clients; instrumentation still works for this object.
      }
      return client;
    };
  }

  return basePool;
}

function sanitizeRequestId(rawValue) {
  const normalized = rawValue === null || rawValue === undefined ? "" : String(rawValue).trim();
  if (!normalized) {
    return "";
  }
  return normalized.slice(0, MAX_REQUEST_ID_LENGTH);
}

function resolveDbQueryRequestId(options = {}) {
  const explicitRequestId = sanitizeRequestId(options.requestId || options.request_id);
  if (explicitRequestId) {
    return explicitRequestId;
  }

  const req = options.req && typeof options.req === "object" ? options.req : null;
  if (!req) {
    return "";
  }

  const requestIdFromReq = sanitizeRequestId(req.id || req.requestId);
  if (requestIdFromReq) {
    return requestIdFromReq;
  }

  const headers = req.headers && typeof req.headers === "object" ? req.headers : null;
  if (!headers) {
    return "";
  }

  return sanitizeRequestId(headers["x-request-id"] || headers["x-cbooster-request-id"]);
}

function resolveQueryPreview(queryConfig) {
  const rawQuery =
    typeof queryConfig === "string" ? queryConfig : queryConfig && typeof queryConfig.text === "string" ? queryConfig.text : "";
  if (!rawQuery) {
    return "";
  }
  return rawQuery.replace(/\s+/g, " ").trim().slice(0, 260);
}

function createDbQuery(queryTarget, options = {}) {
  if (!queryTarget || typeof queryTarget.query !== "function") {
    throw new Error("createDbQuery requires object with query() method.");
  }

  const logger = options.logger && typeof options.logger.error === "function" ? options.logger : console;
  const queryFn = queryTarget.query.bind(queryTarget);
  const logRequestTiming = options.logRequestTiming === true;
  const logErrors = options.logErrors !== false;

  return async function dbQuery(queryConfig, values, queryOptions = {}) {
    const requestId = resolveDbQueryRequestId(queryOptions);
    const startedAtNs = process.hrtime.bigint();

    try {
      return await queryFn(queryConfig, values);
    } catch (error) {
      if (logErrors) {
        logger.error("[db-query] failed", {
          requestId: requestId || undefined,
          statementType: resolveDbStatementType(queryConfig),
          query: resolveQueryPreview(queryConfig),
          code: error?.code || "",
          message: error?.message || "",
        });
      }
      throw error;
    } finally {
      if (logRequestTiming && requestId && typeof logger.info === "function") {
        const endedAtNs = process.hrtime.bigint();
        logger.info("[db-query] completed", {
          requestId,
          statementType: resolveDbStatementType(queryConfig),
          durationMs: Number(endedAtNs - startedAtNs) / 1_000_000,
        });
      }
    }
  };
}

module.exports = {
  createDbQuery,
  instrumentDbPoolWithMetrics,
  resolveDbStatementType,
  resolveDbQueryRequestId,
  wrapDbQueryWithMetrics,
};
