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

const TEST_OWNER_USERNAME = "owner.ghl.smoke@example.com";
const TEST_OWNER_PASSWORD = "Owner!GhlSmoke123";
const TEST_WEB_AUTH_SESSION_SECRET = "ghl-smoke-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz";

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

async function withServer(envOverridesOrCallback, maybeCallback) {
  const envOverrides =
    typeof envOverridesOrCallback === "function" || envOverridesOrCallback === undefined ? {} : envOverridesOrCallback;
  const callback = typeof envOverridesOrCallback === "function" ? envOverridesOrCallback : maybeCallback;
  if (typeof callback !== "function") {
    throw new Error("withServer requires callback.");
  }

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
  const csrfToken = String(cookies.get("cbooster_auth_csrf") || "").trim();
  assert.ok(csrfToken, "Login response must include CSRF cookie.");
  return {
    cookieHeader: buildCookieHeader(cookies),
    csrfToken,
  };
}

function assertNot404or500(status, label) {
  assert.notEqual(status, 404, `${label} must not be 404`);
  assert.notEqual(status, 500, `${label} must not be 500`);
}

test("GHL routes smoke: routes respond and keep stable status envelope", async () => {
  await withServer(async ({ baseUrl }) => {
    const auth = await loginApi(baseUrl);

    const checks = [
      {
        label: "GET /api/ghl/client-basic-note",
        path: "/api/ghl/client-basic-note?clientName=Smoke+Client",
      },
      {
        label: "GET /api/ghl/leads",
        path: "/api/ghl/leads",
      },
      {
        label: "GET /api/ghl/client-communications",
        path: "/api/ghl/client-communications?clientName=Smoke+Client",
      },
      {
        label: "GET /api/ghl/client-communications/recording",
        path: "/api/ghl/client-communications/recording?clientName=Smoke+Client&messageId=test-message-1",
      },
    ];

    for (const item of checks) {
      const response = await fetch(`${baseUrl}${item.path}`, {
        method: "GET",
        headers: {
          Accept: "application/json",
          Cookie: auth.cookieHeader,
        },
      });

      assertNot404or500(response.status, item.label);
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      if (contentType.includes("application/json")) {
        const body = await response.json();
        assert.ok(body && typeof body === "object", `${item.label} should return JSON object`);
      }
    }
  });
});

test("GHL contract archive ingest auth: query/body token channels are rejected", async () => {
  await withServer(
    {
      GHL_CONTRACT_ARCHIVE_INGEST_TOKEN: "ingest-secret-smoke",
    },
    async ({ baseUrl }) => {
      const auth = await loginApi(baseUrl);
      const queryTokenResponse = await fetch(`${baseUrl}/api/ghl/client-contracts/archive?token=ingest-secret-smoke`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Cookie: auth.cookieHeader,
          "X-CSRF-Token": auth.csrfToken,
        },
        body: JSON.stringify({
          clientName: "Smoke Client",
          contractUrl: "https://example.com/contract.pdf",
        }),
      });
      assert.equal(queryTokenResponse.status, 401);
      const queryTokenBody = await queryTokenResponse.json();
      assert.match(String(queryTokenBody?.error || ""), /Unauthorized ingest request/i);

      const bodyTokenResponse = await fetch(`${baseUrl}/api/ghl/client-contracts/archive`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Cookie: auth.cookieHeader,
          "X-CSRF-Token": auth.csrfToken,
        },
        body: JSON.stringify({
          token: "ingest-secret-smoke",
          clientName: "Smoke Client",
          contractUrl: "https://example.com/contract.pdf",
        }),
      });
      assert.equal(bodyTokenResponse.status, 401);
      const bodyTokenPayload = await bodyTokenResponse.json();
      assert.match(String(bodyTokenPayload?.error || ""), /Unauthorized ingest request/i);
    },
  );
});

test("GHL contract archive ingest auth: header and bearer channels are accepted", async () => {
  await withServer(
    {
      GHL_CONTRACT_ARCHIVE_INGEST_TOKEN: "ingest-secret-smoke",
    },
    async ({ baseUrl }) => {
      const auth = await loginApi(baseUrl);
      const headerResponse = await fetch(`${baseUrl}/api/ghl/client-contracts/archive`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Cookie: auth.cookieHeader,
          "X-CSRF-Token": auth.csrfToken,
          "x-ghl-contract-archive-token": "ingest-secret-smoke",
        },
        body: JSON.stringify({}),
      });
      assert.notEqual(headerResponse.status, 401);
      assert.equal(headerResponse.status, 400);
      const headerBody = await headerResponse.json();
      assert.match(String(headerBody?.error || ""), /missing clientName/i);

      const bearerResponse = await fetch(`${baseUrl}/api/ghl/client-contracts/archive`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          Cookie: auth.cookieHeader,
          "X-CSRF-Token": auth.csrfToken,
          Authorization: "Bearer ingest-secret-smoke",
        },
        body: JSON.stringify({}),
      });
      assert.notEqual(bearerResponse.status, 401);
      assert.equal(bearerResponse.status, 400);
      const bearerBody = await bearerResponse.json();
      assert.match(String(bearerBody?.error || ""), /missing clientName/i);
    },
  );
});
