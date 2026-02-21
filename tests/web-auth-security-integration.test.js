"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");

const TEST_OWNER_USERNAME = "owner.integration@example.com";
const TEST_OWNER_PASSWORD = "Owner!Pass123";
const TEST_STAFF_USERNAME = "staff.integration@example.com";
const TEST_STAFF_PASSWORD = "Staff!Pass123";
const TEST_FIRST_PASSWORD_USERNAME = "firstpass.integration@example.com";
const TEST_FIRST_PASSWORD_PASSWORD = "Temp!Pass123";
const TEST_WEB_AUTH_SESSION_SECRET =
  "integration-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz-123456";

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

async function waitForServerReady(baseUrl, child, timeoutMs = 20000) {
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
    PORT: String(port),
    DATABASE_URL: "",
    WEB_AUTH_OWNER_USERNAME: TEST_OWNER_USERNAME,
    WEB_AUTH_USERNAME: TEST_OWNER_USERNAME,
    WEB_AUTH_PASSWORD: TEST_OWNER_PASSWORD,
    WEB_AUTH_SESSION_SECRET: TEST_WEB_AUTH_SESSION_SECRET,
    WEB_AUTH_USERS_JSON: JSON.stringify([
      {
        username: TEST_STAFF_USERNAME,
        password: TEST_STAFF_PASSWORD,
        departmentId: "sales",
        roleId: "manager",
      },
      {
        username: TEST_FIRST_PASSWORD_USERNAME,
        password: TEST_FIRST_PASSWORD_PASSWORD,
        departmentId: "client_service",
        roleId: "manager",
        mustChangePassword: true,
      },
    ]),
    ...envOverrides,
  };

  const child = spawn(process.execPath, [SERVER_ENTRY], {
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
    const details = `${error.message}\n--- stdout ---\n${stdout}\n--- stderr ---\n${stderr}`;
    throw new Error(details);
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

  const exitRace = await Promise.race([
    once(child, "exit").then(() => true),
    delay(4000).then(() => false),
  ]);

  if (!exitRace && child.exitCode === null) {
    child.kill("SIGKILL");
    await once(child, "exit");
  }
}

async function withServer(envOverrides, callback) {
  const server = await startServer(envOverrides);
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

async function loginApi(baseUrl, credentials) {
  const response = await fetch(`${baseUrl}/api/auth/login`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(credentials),
  });

  const body = await response.json();
  assert.equal(response.status, 200, `Login failed: ${JSON.stringify(body)}`);
  const cookies = buildCookieJar(response);

  const sessionCookie = cookies.get("cbooster_auth_session") || "";
  const csrfCookie = cookies.get("cbooster_auth_csrf") || "";
  assert.ok(sessionCookie, "Expected auth session cookie after login.");
  assert.ok(csrfCookie, "Expected CSRF cookie after login.");

  return {
    response,
    body,
    cookies,
    cookieHeader: buildCookieHeader(cookies),
    csrfToken: csrfCookie,
  };
}

test("web auth integration: csrf/rbac/cache/error scenarios", async (t) => {
  await withServer({}, async ({ baseUrl }) => {
    await t.test("GET /api/auth/session returns private no-store headers for authenticated user", async () => {
      const ownerLogin = await loginApi(baseUrl, {
        username: TEST_OWNER_USERNAME,
        password: TEST_OWNER_PASSWORD,
      });

      const response = await fetch(`${baseUrl}/api/auth/session`, {
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
        },
      });
      const body = await response.json();

      assert.equal(response.status, 200);
      assert.equal(body?.ok, true);
      assert.equal(body?.user?.username, TEST_OWNER_USERNAME);

      const cacheControl = String(response.headers.get("cache-control") || "").toLowerCase();
      assert.match(cacheControl, /no-store/);
      assert.match(cacheControl, /private/);
    });

    await t.test("POST /api/auth/logout requires CSRF for cookie-auth requests", async () => {
      const ownerLogin = await loginApi(baseUrl, {
        username: TEST_OWNER_USERNAME,
        password: TEST_OWNER_PASSWORD,
      });

      const missingCsrfResponse = await fetch(`${baseUrl}/api/auth/logout`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
        },
      });
      const missingCsrfBody = await missingCsrfResponse.json();
      assert.equal(missingCsrfResponse.status, 403);
      assert.equal(missingCsrfBody?.code, "csrf_invalid");

      const validCsrfResponse = await fetch(`${baseUrl}/api/auth/logout`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
          "x-csrf-token": ownerLogin.csrfToken,
        },
      });
      const validCsrfBody = await validCsrfResponse.json();
      assert.equal(validCsrfResponse.status, 200);
      assert.equal(validCsrfBody?.ok, true);
    });

    await t.test("RBAC: /api/auth/access-model is blocked for non-admin and allowed for owner", async () => {
      const staffLogin = await loginApi(baseUrl, {
        username: TEST_STAFF_USERNAME,
        password: TEST_STAFF_PASSWORD,
      });

      const deniedResponse = await fetch(`${baseUrl}/api/auth/access-model`, {
        headers: {
          Accept: "application/json",
          Cookie: staffLogin.cookieHeader,
        },
      });
      const deniedBody = await deniedResponse.json();
      assert.equal(deniedResponse.status, 403);
      assert.equal(typeof deniedBody?.error, "string");

      const ownerLogin = await loginApi(baseUrl, {
        username: TEST_OWNER_USERNAME,
        password: TEST_OWNER_PASSWORD,
      });
      const allowedResponse = await fetch(`${baseUrl}/api/auth/access-model`, {
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
        },
      });
      const allowedBody = await allowedResponse.json();
      assert.equal(allowedResponse.status, 200);
      assert.equal(allowedBody?.ok, true);
      assert.ok(allowedBody?.accessModel && typeof allowedBody.accessModel === "object");
    });

    await t.test("mobile first-password endpoint cannot be used with cookie session only", async () => {
      const firstPasswordLogin = await loginApi(baseUrl, {
        username: TEST_FIRST_PASSWORD_USERNAME,
        password: TEST_FIRST_PASSWORD_PASSWORD,
      });
      assert.equal(firstPasswordLogin.body?.mustChangePassword, true);

      const response = await fetch(`${baseUrl}/api/mobile/auth/first-password`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Cookie: firstPasswordLogin.cookieHeader,
          "x-csrf-token": firstPasswordLogin.csrfToken,
        },
        body: JSON.stringify({
          newPassword: "NewTemp!Pass123",
          confirmPassword: "NewTemp!Pass123",
        }),
      });
      const body = await response.json();

      assert.equal(response.status, 401);
      assert.ok(
        body?.code === "mobile_auth_missing" || body?.code === "mobile_session_missing",
        `Unexpected mobile auth code: ${String(body?.code || "")}`,
      );
      assert.match(String(body?.error || ""), /mobile auth token/i);
    });

    await t.test("db error path does not leak internal detail fields", async () => {
      const ownerLogin = await loginApi(baseUrl, {
        username: TEST_OWNER_USERNAME,
        password: TEST_OWNER_PASSWORD,
      });

      const response = await fetch(`${baseUrl}/api/records`, {
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
        },
      });
      const body = await response.json();

      assert.equal(response.status, 503);
      assert.equal(typeof body?.error, "string");
      assert.equal(Object.prototype.hasOwnProperty.call(body, "details"), false);
      assert.equal(Object.prototype.hasOwnProperty.call(body, "dbDetail"), false);
      assert.equal(Object.prototype.hasOwnProperty.call(body, "dbHint"), false);
    });
  });
});
