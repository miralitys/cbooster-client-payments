"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");
const FAKE_PG_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "fake-pg.cjs");

const TEST_OWNER_USERNAME = "owner.records@test.local";
const TEST_OWNER_PASSWORD = "Owner!Records123";
const TEST_WEB_AUTH_SESSION_SECRET = "records-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz-123456";
const WEB_AUTH_CSRF_COOKIE_NAME = "cbooster_auth_csrf";

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function reserveFreePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

async function waitForServerReady(baseUrl, child, timeoutMs = 25000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`Server exited early with code ${child.exitCode}`);
    }

    try {
      const response = await fetch(`${baseUrl}/login`, {
        method: "GET",
        headers: {
          Accept: "text/html",
        },
      });
      if (response.status >= 100 && response.status <= 599) {
        return;
      }
    } catch {
      // Retry until timeout.
    }

    await delay(120);
  }

  throw new Error(`Timed out waiting for server startup at ${baseUrl}`);
}

async function startServer(envOverrides = {}) {
  const port = await reserveFreePort();
  const env = {
    ...process.env,
    NODE_ENV: "test",
    SERVER_AUTOSTART_IN_TEST: "true",
    TEST_USE_FAKE_PG: "1",
    DATABASE_URL: "postgresql://fake-user:fake-pass@localhost:5432/fake-db",
    PORT: String(port),
    WEB_AUTH_OWNER_USERNAME: TEST_OWNER_USERNAME,
    WEB_AUTH_USERNAME: TEST_OWNER_USERNAME,
    WEB_AUTH_PASSWORD: TEST_OWNER_PASSWORD,
    WEB_AUTH_SESSION_SECRET: TEST_WEB_AUTH_SESSION_SECRET,
    RECORDS_PATCH: "true",
    GHL_API_KEY: "",
    GHL_LOCATION_ID: "",
    QUICKBOOKS_CLIENT_ID: "",
    QUICKBOOKS_CLIENT_SECRET: "",
    QUICKBOOKS_REFRESH_TOKEN: "",
    QUICKBOOKS_REALM_ID: "",
    ...envOverrides,
  };

  const child = spawn(process.execPath, ["--require", FAKE_PG_PRELOAD, SERVER_ENTRY], {
    cwd: PROJECT_ROOT,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk.toString();
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  const baseUrl = `http://127.0.0.1:${port}`;
  try {
    await waitForServerReady(baseUrl, child);
  } catch (error) {
    await stopServer(child);
    throw new Error(`${error.message}\n--- stdout ---\n${stdout}\n--- stderr ---\n${stderr}`);
  }

  return {
    child,
    baseUrl,
  };
}

async function stopServer(child) {
  if (!child || child.exitCode !== null) {
    return;
  }

  child.kill("SIGTERM");

  const exitedGracefully = await Promise.race([
    once(child, "exit").then(() => true),
    delay(4000).then(() => false),
  ]);

  if (!exitedGracefully && child.exitCode === null) {
    child.kill("SIGKILL");
    await once(child, "exit");
  }
}

async function withServer(callback) {
  const server = await startServer();
  try {
    await callback(server);
  } finally {
    await stopServer(server.child);
  }
}

function parseSetCookieHeaders(response) {
  if (!response || !response.headers) {
    return [];
  }

  if (typeof response.headers.getSetCookie === "function") {
    return response.headers.getSetCookie();
  }

  const raw = response.headers.get("set-cookie");
  return raw ? [raw] : [];
}

function buildCookieJar(response) {
  const jar = new Map();
  const setCookieHeaders = parseSetCookieHeaders(response);
  for (const entry of setCookieHeaders) {
    const rawPair = String(entry || "").split(";", 1)[0].trim();
    if (!rawPair) {
      continue;
    }
    const eqIndex = rawPair.indexOf("=");
    if (eqIndex <= 0) {
      continue;
    }
    const name = rawPair.slice(0, eqIndex).trim();
    const value = rawPair.slice(eqIndex + 1).trim();
    if (!name) {
      continue;
    }
    jar.set(name, value);
  }
  return jar;
}

function buildCookieHeader(cookieJar) {
  return [...cookieJar.entries()]
    .map(([name, value]) => `${name}=${value}`)
    .join("; ");
}

async function loginApi(baseUrl) {
  const response = await fetch(`${baseUrl}/api/auth/login`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      username: TEST_OWNER_USERNAME,
      password: TEST_OWNER_PASSWORD,
    }),
  });

  const body = await response.json();
  assert.equal(response.status, 200, `Login failed: ${JSON.stringify(body)}`);
  assert.equal(body?.ok, true);
  const cookies = buildCookieJar(response);
  const csrfToken = String(cookies.get(WEB_AUTH_CSRF_COOKIE_NAME) || "");
  assert.ok(csrfToken.length > 0, `Expected \"${WEB_AUTH_CSRF_COOKIE_NAME}\" cookie from /api/auth/login`);
  return {
    cookieHeader: buildCookieHeader(cookies),
    csrfToken,
  };
}

async function getRecords(baseUrl, auth) {
  const response = await fetch(`${baseUrl}/api/records`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Cookie: auth.cookieHeader,
    },
  });
  const body = await response.json();
  return { response, body };
}

test("records regression: GET/PUT/PATCH contracts and edge-cases", async () => {
  await withServer(async ({ baseUrl }) => {
    const auth = await loginApi(baseUrl);

    const initial = await getRecords(baseUrl, auth);
    assert.equal(initial.response.status, 200);
    assert.ok(Array.isArray(initial.body?.records));

    const missingPreconditionPut = await fetch(`${baseUrl}/api/records`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({ records: [] }),
    });
    const missingPreconditionPutBody = await missingPreconditionPut.json();
    assert.equal(missingPreconditionPut.status, 428);
    assert.equal(missingPreconditionPutBody?.code, "records_precondition_required");

    const invalidPayloadPut = await fetch(`${baseUrl}/api/records`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({ records: {}, expectedUpdatedAt: null }),
    });
    const invalidPayloadPutBody = await invalidPayloadPut.json();
    assert.equal(invalidPayloadPut.status, 400);
    assert.equal(invalidPayloadPutBody?.code, "invalid_records_payload");

    const putEmpty = await fetch(`${baseUrl}/api/records`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({ records: [], expectedUpdatedAt: initial.body?.updatedAt || null }),
    });
    const putEmptyBody = await putEmpty.json();
    assert.equal(putEmpty.status, 200);
    assert.equal(putEmptyBody?.ok, true);
    assert.ok(typeof putEmptyBody?.updatedAt === "string" && putEmptyBody.updatedAt.length > 0);

    const putRecord = await fetch(`${baseUrl}/api/records`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({
        records: [
          {
            id: "rec-1",
            clientName: "Regression Client",
            closedBy: "QA",
            serviceType: "Audit",
            contractTotals: "1000",
            payment1: "200",
            payment1Date: "02/20/2026",
          },
        ],
        expectedUpdatedAt: putEmptyBody.updatedAt,
      }),
    });
    const putRecordBody = await putRecord.json();
    assert.equal(putRecord.status, 200);
    assert.equal(putRecordBody?.ok, true);

    const stalePut = await fetch(`${baseUrl}/api/records`, {
      method: "PUT",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({
        records: [],
        expectedUpdatedAt: putEmptyBody.updatedAt,
      }),
    });
    const stalePutBody = await stalePut.json();
    assert.equal(stalePut.status, 409);
    assert.equal(stalePutBody?.code, "records_conflict");

    const invalidPatch = await fetch(`${baseUrl}/api/records`, {
      method: "PATCH",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({
        expectedUpdatedAt: putRecordBody.updatedAt,
      }),
    });
    const invalidPatchBody = await invalidPatch.json();
    assert.equal(invalidPatch.status, 400);
    assert.equal(invalidPatchBody?.code, "invalid_records_patch_payload");

    const patchOk = await fetch(`${baseUrl}/api/records`, {
      method: "PATCH",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({
        expectedUpdatedAt: putRecordBody.updatedAt,
        operations: [
          {
            type: "upsert",
            id: "rec-1",
            record: {
              id: "rec-1",
              clientName: "Regression Client",
              notes: "Patched from regression test",
            },
          },
        ],
      }),
    });
    const patchOkBody = await patchOk.json();
    assert.equal(patchOk.status, 200);
    assert.equal(patchOkBody?.ok, true);
    assert.equal(patchOkBody?.appliedOperations, 1);

    const stalePatch = await fetch(`${baseUrl}/api/records`, {
      method: "PATCH",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: auth.cookieHeader,
        "x-csrf-token": auth.csrfToken,
      },
      body: JSON.stringify({
        expectedUpdatedAt: putRecordBody.updatedAt,
        operations: [
          {
            type: "delete",
            id: "rec-1",
          },
        ],
      }),
    });
    const stalePatchBody = await stalePatch.json();
    assert.equal(stalePatch.status, 409);
    assert.equal(stalePatchBody?.code, "records_conflict");
  });
});
