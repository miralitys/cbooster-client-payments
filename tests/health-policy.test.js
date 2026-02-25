"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");

const TEST_OWNER_USERNAME = "owner.health@example.com";
const TEST_OWNER_PASSWORD = "Owner!Health123";
const TEST_WEB_AUTH_SESSION_SECRET = "health-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz-123456";
const TEST_HEALTH_API_KEY = "health-key-for-tests-123";

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
    HEALTH_CHECK_API_KEY: TEST_HEALTH_API_KEY,
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

test("health policy: anonymous response is neutral and has no operational details", async () => {
  await withServer({}, async ({ baseUrl }) => {
    const response = await fetch(`${baseUrl}/api/health`, {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    });
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.deepEqual(body, { ok: true });
    assert.equal(Object.prototype.hasOwnProperty.call(body, "status"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "error"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "details"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "stack"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "version"), false);
  });
});

test("health policy: invalid token does not unlock detailed status", async () => {
  await withServer({}, async ({ baseUrl }) => {
    const response = await fetch(`${baseUrl}/api/health`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        "x-health-check-key": "wrong-token",
      },
    });
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.deepEqual(body, { ok: true });
    assert.equal(Object.prototype.hasOwnProperty.call(body, "status"), false);
  });
});

test("health policy: authorized key gets detailed operational status only", async () => {
  await withServer({}, async ({ baseUrl }) => {
    const response = await fetch(`${baseUrl}/api/health`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        "x-health-check-key": TEST_HEALTH_API_KEY,
      },
    });
    const body = await response.json();

    assert.equal(response.status, 503);
    assert.equal(body?.ok, false);
    assert.equal(body?.status, "unhealthy");
    assert.equal(Object.prototype.hasOwnProperty.call(body, "details"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "stack"), false);
    assert.equal(Object.prototype.hasOwnProperty.call(body, "version"), false);
  });
});

