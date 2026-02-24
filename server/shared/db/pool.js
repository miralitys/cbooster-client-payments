"use strict";

const fs = require("fs");
const { Pool } = require("pg");
const { instrumentDbPoolWithMetrics } = require("./query");

function shouldUseSsl(rawMode = process.env.PGSSLMODE) {
  const mode = (rawMode || "").toString().trim().toLowerCase();
  return mode !== "disable";
}

function normalizePemValue(rawValue) {
  const value = (rawValue || "").toString();
  if (!value.trim()) {
    return "";
  }

  if (value.includes("\\n")) {
    return value.replace(/\\n/g, "\n");
  }
  return value;
}

function resolvePgSslCa(options = {}, env = process.env) {
  const explicitCa = normalizePemValue(options.pgSslCaCert || options.sslCa || env.PGSSL_CA_CERT || "");
  if (explicitCa) {
    return explicitCa;
  }

  const base64Ca = (options.pgSslCaCertBase64 || env.PGSSL_CA_CERT_BASE64 || "").toString().trim();
  if (base64Ca) {
    return Buffer.from(base64Ca, "base64").toString("utf8");
  }

  const rootCertPath = (options.pgSslRootCertPath || env.PGSSLROOTCERT || "").toString().trim();
  if (!rootCertPath) {
    return "";
  }

  try {
    return fs.readFileSync(rootCertPath, "utf8");
  } catch (error) {
    const reason = error instanceof Error ? error.message : "unknown";
    throw new Error(`Failed to read PGSSLROOTCERT at ${rootCertPath}: ${reason}`);
  }
}

function createPgSslConfig(options = {}, env = process.env) {
  const sslEnabled = typeof options.sslEnabled === "boolean" ? options.sslEnabled : shouldUseSsl(options.pgSslMode || env.PGSSLMODE);
  if (!sslEnabled) {
    return false;
  }

  const sslConfig = {
    rejectUnauthorized: true,
  };
  const ca = resolvePgSslCa(options, env);
  if (ca) {
    sslConfig.ca = ca;
  }
  return sslConfig;
}

function createDbPool(options = {}) {
  const connectionString = (options.connectionString || "").toString().trim();
  if (!connectionString) {
    return null;
  }

  const runtimeEnv = options.env && typeof options.env === "object" ? options.env : process.env;
  const ssl = createPgSslConfig(options, runtimeEnv);
  const poolFactory = typeof options.poolFactory === "function" ? options.poolFactory : (config) => new Pool(config);

  const pool = poolFactory({
    connectionString,
    ssl,
  });

  if (options.instrumentationState) {
    return instrumentDbPoolWithMetrics(pool, options.instrumentationState);
  }

  return pool;
}

module.exports = {
  createPgSslConfig,
  createDbPool,
  resolvePgSslCa,
  shouldUseSsl,
};
