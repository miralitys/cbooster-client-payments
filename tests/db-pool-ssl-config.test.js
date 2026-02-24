"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { createDbPool, createPgSslConfig, resolvePgSslCa, shouldUseSsl } = require("../server/shared/db/pool");

test("shouldUseSsl disables TLS only for PGSSLMODE=disable", () => {
  assert.equal(shouldUseSsl("disable"), false);
  assert.equal(shouldUseSsl("DISABLE"), false);
  assert.equal(shouldUseSsl("require"), true);
  assert.equal(shouldUseSsl("verify-full"), true);
  assert.equal(shouldUseSsl(""), true);
});

test("createPgSslConfig returns false when SSL is explicitly disabled", () => {
  const ssl = createPgSslConfig({ sslEnabled: false }, { PGSSLMODE: "require" });
  assert.equal(ssl, false);
});

test("createPgSslConfig uses strict TLS by default", () => {
  const ssl = createPgSslConfig({}, { PGSSLMODE: "require" });
  assert.deepEqual(ssl, { rejectUnauthorized: true });
});

test("createPgSslConfig supports inline PEM via PGSSL_CA_CERT", () => {
  const ssl = createPgSslConfig({}, { PGSSLMODE: "require", PGSSL_CA_CERT: "line1\\nline2" });
  assert.equal(ssl.rejectUnauthorized, true);
  assert.equal(ssl.ca, "line1\nline2");
});

test("resolvePgSslCa reads PEM from PGSSLROOTCERT path", () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "db-pool-ca-"));
  const certPath = path.join(tempDir, "ca.pem");
  fs.writeFileSync(certPath, "-----BEGIN CERTIFICATE-----\nLOCAL_CA\n-----END CERTIFICATE-----\n", "utf8");

  const ca = resolvePgSslCa({}, { PGSSLROOTCERT: certPath });
  assert.match(ca, /LOCAL_CA/);
});

test("resolvePgSslCa throws when PGSSLROOTCERT path is invalid", () => {
  assert.throws(
    () => resolvePgSslCa({}, { PGSSLROOTCERT: "/path/that/does/not/exist-ca.pem" }),
    /Failed to read PGSSLROOTCERT/i,
  );
});

test("createDbPool passes strict SSL config to Pool factory", () => {
  let capturedConfig = null;
  const fakePool = { query: async () => ({ rows: [] }) };

  const created = createDbPool({
    connectionString: "postgresql://user:pass@localhost:5432/db",
    env: { PGSSLMODE: "require" },
    poolFactory: (config) => {
      capturedConfig = config;
      return fakePool;
    },
  });

  assert.equal(created, fakePool);
  assert.ok(capturedConfig, "Pool config should be passed to factory");
  assert.equal(capturedConfig.connectionString, "postgresql://user:pass@localhost:5432/db");
  assert.equal(capturedConfig.ssl.rejectUnauthorized, true);
});

test("createDbPool disables SSL when PGSSLMODE=disable", () => {
  let capturedConfig = null;

  createDbPool({
    connectionString: "postgresql://user:pass@localhost:5432/db",
    env: { PGSSLMODE: "disable" },
    poolFactory: (config) => {
      capturedConfig = config;
      return { query: async () => ({ rows: [] }) };
    },
  });

  assert.ok(capturedConfig, "Pool config should be passed to factory");
  assert.equal(capturedConfig.ssl, false);
});
