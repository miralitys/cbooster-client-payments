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
const TELEGRAM_BOT_TOKEN = "test_bot_token_for_mini_access";

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
      const response = await fetch(`${baseUrl}/api/health`, {
        method: "GET",
        headers: {
          Accept: "application/json",
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
    PORT: String(port),
    DATABASE_URL: "",
    TELEGRAM_ALLOWED_USER_IDS: "",
    TELEGRAM_REQUIRED_CHAT_ID: "",
    TELEGRAM_NOTIFY_CHAT_ID: "",
    TELEGRAM_NOTIFY_THREAD_ID: "",
    ...envOverrides,
  };

  const child = spawn(process.execPath, ["--require", TELEGRAM_FETCH_PRELOAD, SERVER_ENTRY], {
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

async function postMiniAccess(baseUrl, payload) {
  return await fetch(`${baseUrl}/api/mini/access`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload || {}),
  });
}

function buildTelegramInitData({ botToken, authDate, user, includeHash = true, hashOverride = "" }) {
  const params = new URLSearchParams();
  params.set("auth_date", String(authDate));
  params.set("query_id", "AAEAAAE");
  params.set("user", JSON.stringify(user || {}));

  if (includeHash) {
    const entries = [...params.entries()]
      .filter(([key]) => key !== "hash")
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, value]) => `${key}=${value}`);
    const dataCheckString = entries.join("\n");
    const secretKey = crypto.createHmac("sha256", "WebAppData").update(botToken).digest();
    const expectedHash = crypto.createHmac("sha256", secretKey).update(dataCheckString).digest("hex").toLowerCase();
    params.set("hash", hashOverride || expectedHash);
  }

  return params.toString();
}

function assertExactObjectKeys(value, expectedKeys) {
  assert.ok(value && typeof value === "object" && !Array.isArray(value));
  const actualKeys = Object.keys(value).sort();
  const normalizedExpectedKeys = [...expectedKeys].sort();
  assert.deepEqual(actualKeys, normalizedExpectedKeys);
}

test("POST /api/mini/access returns 503 when Telegram auth token is missing", async () => {
  await withServer(
    {
      TELEGRAM_BOT_TOKEN: "",
    },
    async ({ baseUrl }) => {
      const response = await postMiniAccess(baseUrl, {
        initData: "auth_date=1&hash=deadbeef",
      });
      const body = await response.json();

      assert.equal(response.status, 503);
      assert.equal(body.error, "Telegram auth is not configured on server.");
    },
  );
});

test("POST /api/mini/access validates Telegram initData signature and TTL", async (t) => {
  await withServer(
    {
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "60",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const user = { id: 12345, username: "mini_tester" };

      await t.test("returns 401 for missing initData", async () => {
        const response = await postMiniAccess(baseUrl, {
          initData: "",
        });
        const body = await response.json();

        assert.equal(response.status, 401);
        assert.equal(body.error, "Missing Telegram initData.");
      });

      await t.test("returns 401 when hash is missing", async () => {
        const initData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: nowSeconds,
          user,
          includeHash: false,
        });

        const response = await postMiniAccess(baseUrl, {
          initData,
        });
        const body = await response.json();

        assert.equal(response.status, 401);
        assert.equal(body.error, "Invalid Telegram initData hash.");
      });

      await t.test("returns 401 for invalid hash", async () => {
        const initData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: nowSeconds,
          user,
          hashOverride: "0".repeat(64),
        });

        const response = await postMiniAccess(baseUrl, {
          initData,
        });
        const body = await response.json();

        assert.equal(response.status, 401);
        assert.equal(body.error, "Telegram signature check failed.");
      });

      await t.test("returns 401 for expired auth_date", async () => {
        const expiredAuthDate = nowSeconds - 3600;
        const initData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: expiredAuthDate,
          user,
        });

        const response = await postMiniAccess(baseUrl, {
          initData,
        });
        const body = await response.json();

        assert.equal(response.status, 401);
        assert.equal(body.error, "Telegram session expired. Reopen Mini App from Telegram chat.");
      });

      await t.test("returns 200 for valid signed initData", async () => {
        const validInitData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: nowSeconds,
          user,
        });

        const response = await postMiniAccess(baseUrl, {
          initData: validInitData,
        });
        const body = await response.json();

        assert.equal(response.status, 200);
        assert.equal(body.ok, true);
        assert.deepEqual(body.user, {
          id: "12345",
          username: "mini_tester",
        });
      });
    },
  );
});

test("POST /api/mini/access keeps stable response contract for auth fail/success", async (t) => {
  await withServer(
    {
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "120",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);

      await t.test("auth fail response shape", async () => {
        const response = await postMiniAccess(baseUrl, { initData: "" });
        const body = await response.json();

        assert.equal(response.status, 401);
        assertExactObjectKeys(body, ["error"]);
        assert.equal(typeof body.error, "string");
        assert.ok(body.error.length > 0);
      });

      await t.test("auth success response shape", async () => {
        const initData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: nowSeconds,
          user: {
            id: 456,
            username: "contract_user",
          },
        });

        const response = await postMiniAccess(baseUrl, { initData });
        const body = await response.json();

        assert.equal(response.status, 200);
        assertExactObjectKeys(body, ["ok", "user"]);
        assert.equal(body.ok, true);
        assert.ok(body.user && typeof body.user === "object" && !Array.isArray(body.user));
        assertExactObjectKeys(body.user, ["id", "username"]);
        assert.equal(body.user.id, "456");
        assert.equal(body.user.username, "contract_user");
      });
    },
  );
});

test("POST /api/mini/access returns 403 for disallowed Telegram user id", async () => {
  await withServer(
    {
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_ALLOWED_USER_IDS: "777",
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        botToken: TELEGRAM_BOT_TOKEN,
        authDate: nowSeconds,
        user: { id: 12345, username: "not_allowed" },
      });

      const response = await postMiniAccess(baseUrl, {
        initData,
      });
      const body = await response.json();

      assert.equal(response.status, 403);
      assert.equal(body.error, "Telegram user is not allowed.");
    },
  );
});

test("POST /api/mini/access enforces Telegram group membership checks", async (t) => {
  await withServer(
    {
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_REQUIRED_CHAT_ID: "-1001234567890",
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
      TEST_TELEGRAM_FETCH_MODE: "telegram_matrix",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);

      async function requestByUserId(userId) {
        const initData = buildTelegramInitData({
          botToken: TELEGRAM_BOT_TOKEN,
          authDate: nowSeconds,
          user: { id: userId, username: `user_${userId}` },
        });
        const response = await postMiniAccess(baseUrl, { initData });
        const body = await response.json();
        return { response, body };
      }

      await t.test("returns 503 when Telegram API request fails with network error", async () => {
        const { response, body } = await requestByUserId(9001);
        assert.equal(response.status, 503);
        assert.equal(body.error, "Telegram membership check failed. Try again in a moment.");
      });

      await t.test("returns 403 when Telegram API responds with 403", async () => {
        const { response, body } = await requestByUserId(9002);
        assert.equal(response.status, 403);
        assert.equal(body.error, "Only members of the allowed Telegram group can use Mini App.");
      });

      await t.test("returns 403 for disallowed member status", async () => {
        const { response, body } = await requestByUserId(9003);
        assert.equal(response.status, 403);
        assert.equal(body.error, "Only members of the allowed Telegram group can use Mini App.");
      });

      await t.test("returns 503 for upstream non-auth Telegram error", async () => {
        const { response, body } = await requestByUserId(9005);
        assert.equal(response.status, 503);
        assert.equal(body.error, "Telegram membership check failed. Try again in a moment.");
      });

      await t.test("returns 200 for allowed member status", async () => {
        const { response, body } = await requestByUserId(9004);
        assert.equal(response.status, 200);
        assert.equal(body.ok, true);
        assert.equal(body.user.id, "9004");
      });
    },
  );
});
