"use strict";

const { Pool } = require("pg");
const { instrumentDbPoolWithMetrics } = require("./query");

function shouldUseSsl(rawMode = process.env.PGSSLMODE) {
  const mode = (rawMode || "").toString().trim().toLowerCase();
  return mode !== "disable";
}

function createDbPool(options = {}) {
  const connectionString = (options.connectionString || "").toString().trim();
  if (!connectionString) {
    return null;
  }

  const sslEnabled = typeof options.sslEnabled === "boolean" ? options.sslEnabled : shouldUseSsl(options.pgSslMode);
  const pool = new Pool({
    connectionString,
    ssl: sslEnabled ? { rejectUnauthorized: false } : false,
  });

  if (options.instrumentationState) {
    return instrumentDbPoolWithMetrics(pool, options.instrumentationState);
  }

  return pool;
}

module.exports = {
  createDbPool,
  shouldUseSsl,
};
