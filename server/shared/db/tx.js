"use strict";

const { createDbQuery, resolveDbQueryRequestId } = require("./query");

function createDbTx(pool, options = {}) {
  if (!pool || typeof pool.connect !== "function") {
    throw new Error("createDbTx requires object with connect() method.");
  }

  const logger = options.logger && typeof options.logger.error === "function" ? options.logger : console;

  return async function runInTransaction(work, txOptions = {}) {
    if (typeof work !== "function") {
      throw new Error("runInTransaction requires callback.");
    }

    const requestId = resolveDbQueryRequestId(txOptions);
    const client = await pool.connect();
    const query = createDbQuery(client, {
      logger,
      logErrors: options.logErrors !== false,
      logRequestTiming: options.logRequestTiming === true,
    });

    try {
      await query("BEGIN", [], { requestId });
      const result = await work({
        client,
        query,
        requestId,
      });
      await query("COMMIT", [], { requestId });
      return result;
    } catch (error) {
      try {
        await query("ROLLBACK", [], { requestId });
      } catch (rollbackError) {
        logger.error("[db-tx] rollback failed", {
          requestId: requestId || undefined,
          code: rollbackError?.code || "",
          message: rollbackError?.message || "",
        });
      }
      throw error;
    } finally {
      client.release();
    }
  };
}

async function runInDbTransaction(pool, work, options = {}) {
  const runInTransaction = createDbTx(pool, options);
  return runInTransaction(work, {
    requestId: options.requestId,
    request_id: options.request_id,
    req: options.req,
  });
}

module.exports = {
  createDbTx,
  runInDbTransaction,
};
