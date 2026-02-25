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
const FAKE_FETCH_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "fake-fetch-ghl-contract-download.cjs");

const TEST_OWNER_USERNAME = "owner.ghl.contract.security@example.com";
const TEST_OWNER_PASSWORD = "Owner!GhlContract123";
const TEST_WEB_AUTH_SESSION_SECRET = "ghl-contract-security-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz";
const TEST_INGEST_TOKEN = "ingest-token-ssrf-security-test";
const TEST_ALLOWED_CONTRACT_URL = "https://services.leadconnectorhq.com/contacts/fake-contact/documents/fake-document";

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    GHL_CONTRACT_ARCHIVE_INGEST_TOKEN: TEST_INGEST_TOKEN,
    ...envOverrides,
  };

  const child = spawn(
    process.execPath,
    ["--require", FAKE_PG_PRELOAD, "--require", FAKE_FETCH_PRELOAD, SERVER_ENTRY],
    {
      cwd: PROJECT_ROOT,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

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
  for (const entry of parseSetCookieHeaders(response)) {
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
  const cookies = buildCookieJar(response);
  const csrfToken = String(cookies.get("cbooster_auth_csrf") || "").trim();
  assert.ok(csrfToken, "Login response must include CSRF cookie.");
  return {
    cookieHeader: buildCookieHeader(cookies),
    csrfToken,
  };
}

async function postContractArchive(baseUrl, auth, payload) {
  const response = await fetch(`${baseUrl}/api/ghl/client-contracts/archive`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "x-ghl-contract-archive-token": TEST_INGEST_TOKEN,
      Authorization: `Bearer ${TEST_INGEST_TOKEN}`,
      Cookie: auth.cookieHeader,
      "X-CSRF-Token": auth.csrfToken,
    },
    body: JSON.stringify(payload),
  });
  const body = await response.json();
  return { response, body };
}

test("GHL archive ingest blocks SSRF via unsafe redirect target", async () => {
  await withServer({ TEST_FAKE_GHL_FETCH_MODE: "redirect-private" }, async ({ baseUrl }) => {
    const auth = await loginApi(baseUrl);
    const result = await postContractArchive(baseUrl, auth, {
      clientName: "SSRF Block Test",
      contractTitle: "Contract",
      contractUrl: TEST_ALLOWED_CONTRACT_URL,
    });

    assert.equal(result.response.status, 400);
    assert.match(String(result.body?.error || ""), /redirect target is not allowed/i);
  });
});

test("GHL archive ingest accepts safe redirect targets and stores PDF", async () => {
  await withServer({ TEST_FAKE_GHL_FETCH_MODE: "redirect-public" }, async ({ baseUrl }) => {
    const auth = await loginApi(baseUrl);
    const result = await postContractArchive(baseUrl, auth, {
      clientName: "SSRF Allow Test",
      contractTitle: "Contract",
      contractUrl: TEST_ALLOWED_CONTRACT_URL,
    });

    assert.equal(result.response.status, 200, JSON.stringify(result.body));
    assert.equal(result.body?.ok, true);
    assert.equal(result.body?.archived, true);
    assert.ok(Number.isFinite(Number(result.body?.sizeBytes)));
    assert.ok(Number(result.body?.sizeBytes) > 0);
  });
});
