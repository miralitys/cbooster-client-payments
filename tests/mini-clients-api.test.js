"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");
const TELEGRAM_FETCH_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "telegram-fetch-mock.cjs");
const FAKE_PG_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "fake-pg.cjs");
const TELEGRAM_BOT_TOKEN = "test_bot_token_for_mini_clients";

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
      const response = await fetch(`${baseUrl}/mini`, {
        method: "GET",
      });
      if (response.status >= 100 && response.status <= 599) {
        return;
      }
    } catch {
      // retry
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
    PORT: String(port),
    TELEGRAM_ALLOWED_USER_IDS: "",
    TELEGRAM_REQUIRED_CHAT_ID: "",
    TELEGRAM_NOTIFY_CHAT_ID: "",
    TELEGRAM_NOTIFY_THREAD_ID: "",
    ...envOverrides,
  };

  const child = spawn(
    process.execPath,
    ["--require", FAKE_PG_PRELOAD, "--require", TELEGRAM_FETCH_PRELOAD, SERVER_ENTRY],
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

function buildTelegramInitData({ authDate, user, botToken = TELEGRAM_BOT_TOKEN, invalidHash = false }) {
  const params = new URLSearchParams();
  params.set("auth_date", String(authDate));
  params.set("query_id", "AAEAAAE");
  params.set("user", JSON.stringify(user || {}));

  const dataCheckString = [...params.entries()]
    .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
    .map(([key, value]) => `${key}=${value}`)
    .join("\n");
  const secretKey = crypto.createHmac("sha256", "WebAppData").update(botToken).digest();
  const hash = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex").toLowerCase();
  params.set("hash", invalidHash ? "0".repeat(64) : hash);

  return params.toString();
}

async function postMiniClients(baseUrl, payload, headers = {}) {
  return await fetch(`${baseUrl}/api/mini/clients`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...headers,
    },
    body: JSON.stringify(payload || {}),
  });
}

test("POST /api/mini/clients returns 503 without database", async () => {
  await withServer(
    {
      DATABASE_URL: "",
      TELEGRAM_BOT_TOKEN,
    },
    async ({ baseUrl }) => {
      const response = await postMiniClients(baseUrl, {
        initData: "any",
        client: { clientName: "John Doe" },
      });
      const body = await response.json();

      assert.equal(response.status, 503);
      assert.equal(body.error, "Database is not configured. Add DATABASE_URL in Render environment variables.");
    },
  );
});

test("POST /api/mini/clients returns 400 for invalid payload", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 101, username: "payload_user" },
      });

      const response = await postMiniClients(baseUrl, {
        initData,
      });
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error, "Payload must include `client` object.");
    },
  );
});

test("POST /api/mini/clients returns 401 for Telegram auth fail", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 202, username: "auth_fail_user" },
        invalidHash: true,
      });

      const response = await postMiniClients(baseUrl, {
        initData,
        client: { clientName: "Auth Fail" },
      });
      const body = await response.json();

      assert.equal(response.status, 401);
      assert.equal(body.error, "Telegram signature check failed.");
    },
  );
});

test("POST /api/mini/clients returns 403 for disallowed user", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_ALLOWED_USER_IDS: "7777",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 303, username: "forbidden_user" },
      });

      const response = await postMiniClients(baseUrl, {
        initData,
        client: { clientName: "Forbidden User" },
      });
      const body = await response.json();

      assert.equal(response.status, 403);
      assert.equal(body.error, "Telegram user is not allowed.");
    },
  );
});

test("POST /api/mini/clients returns 201 for successful submission", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "120",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 404, username: "success_user" },
      });

      const response = await postMiniClients(baseUrl, {
        initData,
        client: {
          clientName: "Success Client",
          clientEmailAddress: "success@example.com",
        },
      });
      const body = await response.json();

      assert.equal(response.status, 201);
      assert.equal(body.ok, true);
      assert.equal(body.status, "pending");
      assert.equal(body.attachmentsCount, 0);
      assert.equal(typeof body.submissionId, "string");
      assert.ok(body.submissionId.startsWith("sub-"));
      assert.equal(typeof body.submittedAt, "string");
      assert.ok(!Number.isNaN(Date.parse(body.submittedAt)));
    },
  );
});

test("POST /api/mini/clients still returns 201 when Telegram notification fails", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_NOTIFY_CHAT_ID: "-100900900900",
      TEST_TELEGRAM_FETCH_MODE: "notify_fail",
      TELEGRAM_INIT_DATA_TTL_SEC: "120",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 505, username: "notify_fail_user" },
      });

      const response = await postMiniClients(baseUrl, {
        initData,
        client: {
          clientName: "Notify Failure Client",
        },
      });
      const body = await response.json();

      assert.equal(response.status, 201);
      assert.equal(body.ok, true);
      assert.equal(body.status, "pending");
      assert.equal(body.attachmentsCount, 0);
    },
  );
});

