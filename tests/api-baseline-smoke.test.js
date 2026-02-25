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

const TEST_OWNER_USERNAME = "owner.smoke@example.com";
const TEST_OWNER_PASSWORD = "Owner!Smoke123";
const TEST_WEB_AUTH_SESSION_SECRET = "smoke-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz-123456";

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
  assert.equal(body?.ok, true, "Login response must include ok=true.");
  const cookies = buildCookieJar(response);
  return {
    cookieHeader: buildCookieHeader(cookies),
  };
}

async function parseJsonBody(response) {
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();
  if (!contentType.includes("application/json")) {
    return null;
  }
  return await response.json();
}

function assertStatusIn(response, expectedStatuses, routeLabel) {
  assert.ok(
    expectedStatuses.includes(response.status),
    `${routeLabel} returned unexpected status ${response.status}. Expected one of: ${expectedStatuses.join(", ")}`,
  );
}

test("phase-0 smoke: public/unauth critical endpoints keep expected route/status behavior", async () => {
  await withServer(async ({ baseUrl }) => {
    const securityTxtResponse = await fetch(`${baseUrl}/.well-known/security.txt`, {
      redirect: "manual",
      headers: { Accept: "text/plain" },
    });
    const securityTxtBody = await securityTxtResponse.text();
    assert.equal(securityTxtResponse.status, 200);
    assert.match(String(securityTxtResponse.headers.get("content-type") || ""), /text\/plain/i);
    assert.equal(String(securityTxtResponse.headers.get("location") || ""), "");
    assert.match(securityTxtBody, /^Contact:\s+\S+/m);
    assert.match(securityTxtBody, /^Policy:\s+\S+/m);
    assert.match(securityTxtBody, /^Preferred-Languages:\s+.+/m);
    assert.match(securityTxtBody, /^Expires:\s+.+/m);
    assert.doesNotMatch(securityTxtBody, /(password|token|secret|database_url|private[_\s-]?key)/i);

    const healthResponse = await fetch(`${baseUrl}/api/health`, {
      headers: { Accept: "application/json" },
    });
    const healthBody = await parseJsonBody(healthResponse);
    assert.equal(healthResponse.status, 200);
    assert.equal(healthBody?.ok, true);
    assert.equal(Object.prototype.hasOwnProperty.call(healthBody || {}, "status"), false);

    const appResponse = await fetch(`${baseUrl}/app/client-payments`, {
      redirect: "manual",
      headers: { Accept: "text/html" },
    });
    assertStatusIn(appResponse, [200, 302, 303], "GET /app/client-payments (unauth)");
    assert.notEqual(appResponse.status, 404);

    const recordsResponse = await fetch(`${baseUrl}/api/records`, {
      redirect: "manual",
      headers: { Accept: "application/json" },
    });
    assertStatusIn(recordsResponse, [401, 302, 303, 403], "GET /api/records (unauth)");
    assert.notEqual(recordsResponse.status, 500);
  });
});

test("phase-0 smoke: authenticated critical endpoints keep route + JSON envelope", async () => {
  await withServer(async ({ baseUrl }) => {
    const auth = await loginApi(baseUrl);

    const checks = [
      {
        label: "GET /api/auth/session",
        path: "/api/auth/session",
        expectedStatuses: [200],
        assertBody(body) {
          assert.equal(body?.ok, true);
          assert.equal(typeof body?.user?.username, "string");
        },
      },
      {
        label: "GET /api/records",
        path: "/api/records",
        expectedStatuses: [200, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.ok(Array.isArray(body?.records), "records must be an array.");
            const updatedAtType = body?.updatedAt === null ? "null" : typeof body?.updatedAt;
            assert.ok(
              updatedAtType === "string" || updatedAtType === "null",
              "updatedAt must be string or null.",
            );
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/ghl/client-basic-note",
        path: "/api/ghl/client-basic-note?clientName=Smoke%20Client",
        expectedStatuses: [200, 403, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.equal(body?.ok, true);
            assert.equal(typeof body?.clientName, "string");
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/ghl/client-communications",
        path: "/api/ghl/client-communications?clientName=Smoke%20Client",
        expectedStatuses: [200, 403, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.equal(body?.ok, true);
            assert.ok(Array.isArray(body?.items), "communications items must be an array.");
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/quickbooks/payments/recent",
        path: "/api/quickbooks/payments/recent",
        expectedStatuses: [200, 400, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.equal(body?.ok, true);
            assert.ok(Array.isArray(body?.items), "quickbooks items must be an array.");
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/ghl/leads",
        path: "/api/ghl/leads?range=today",
        expectedStatuses: [200, 400, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.equal(body?.ok, true);
            assert.ok(Array.isArray(body?.items), "leads items must be an array.");
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/moderation/submissions",
        path: "/api/moderation/submissions?status=pending&limit=5",
        expectedStatuses: [200, 400, 503],
        assertBody(body, status) {
          if (status === 200) {
            assert.equal(typeof body?.status, "string");
            assert.ok(Array.isArray(body?.items), "moderation items must be an array.");
            return;
          }
          assert.equal(typeof body?.error, "string");
        },
      },
      {
        label: "GET /api/ghl/client-communications/recording (validation)",
        path: "/api/ghl/client-communications/recording?clientName=Smoke%20Client",
        expectedStatuses: [400],
        assertBody(body) {
          assert.equal(typeof body?.error, "string");
        },
      },
    ];

    for (const check of checks) {
      const response = await fetch(`${baseUrl}${check.path}`, {
        redirect: "manual",
        headers: {
          Accept: "application/json",
          Cookie: auth.cookieHeader,
        },
      });
      assertStatusIn(response, check.expectedStatuses, check.label);
      const body = await parseJsonBody(response);
      assert.ok(body && typeof body === "object", `${check.label} must return JSON body.`);
      check.assertBody(body, response.status);
    }
  });
});
