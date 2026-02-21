"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const net = require("node:net");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");
const TELEGRAM_FETCH_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "telegram-fetch-mock.cjs");
const FAKE_PG_PRELOAD = path.join(PROJECT_ROOT, "tests", "helpers", "fake-pg.cjs");
const TELEGRAM_BOT_TOKEN = "test_bot_token_for_mini_clients";
const TEST_WEB_AUTH_SESSION_SECRET =
  "mini-clients-test-session-secret-mini-clients-test-session-secret-123456";
const MINI_UPLOAD_TOKEN_HEADER_NAME = "x-mini-upload-token";

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
    WEB_AUTH_SESSION_SECRET: TEST_WEB_AUTH_SESSION_SECRET,
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

function encodeBase64Url(rawValue) {
  return Buffer.from(rawValue, "utf8")
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function signMiniUploadTokenPayload(encodedPayload, secret = TEST_WEB_AUTH_SESSION_SECRET) {
  return crypto.createHmac("sha256", secret).update(`mini-upload:${encodedPayload}`).digest("hex");
}

function createMiniUploadTokenForUser(userId, options = {}) {
  const expiresAtMs = Number.isFinite(options.expiresAtMs) ? options.expiresAtMs : Date.now() + 10 * 60 * 1000;
  const payload = JSON.stringify({
    u: String(userId || ""),
    e: expiresAtMs,
  });
  const encodedPayload = encodeBase64Url(payload);
  const signature = signMiniUploadTokenPayload(encodedPayload, options.secret || TEST_WEB_AUTH_SESSION_SECRET);
  return `${encodedPayload}.${signature}`;
}

async function fetchUploadTokenFromAccess(baseUrl, initData) {
  const response = await fetch(`${baseUrl}/api/mini/access`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ initData }),
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.ok, true);
  assert.equal(typeof body.uploadToken, "string");
  assert.ok(body.uploadToken.length > 20);
  return body.uploadToken;
}

function readCapturedSendMessageText(captureFilePath) {
  const raw = fs.existsSync(captureFilePath) ? fs.readFileSync(captureFilePath, "utf8") : "";
  const lines = raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const events = [];
  for (const line of lines) {
    try {
      events.push(JSON.parse(line));
    } catch {
      // Ignore malformed capture lines.
    }
  }

  const sendMessageEvent = [...events].reverse().find((event) => String(event?.url || "").includes("/sendMessage"));
  if (!sendMessageEvent || typeof sendMessageEvent.body !== "string" || !sendMessageEvent.body) {
    return "";
  }

  try {
    const body = JSON.parse(sendMessageEvent.body);
    return typeof body?.text === "string" ? body.text : "";
  } catch {
    return "";
  }
}

test("POST /api/mini/clients returns 503 without database", async () => {
  await withServer(
    {
      DATABASE_URL: "",
      TELEGRAM_BOT_TOKEN,
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 88, username: "no_db_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClients(baseUrl, {
        initData,
        client: { clientName: "John Doe" },
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
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
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClients(baseUrl, {
        initData,
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
      });
      const body = await response.json();

      assert.equal(response.status, 400);
      assert.equal(body.error, "Payload must include `client` object.");
    },
  );
});

test("POST /api/mini/clients parses client object/JSON string and rejects invalid client formats", async (t) => {
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
        user: { id: 111, username: "parse_payload_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      async function expectInvalidClient(clientValue) {
        const response = await postMiniClients(baseUrl, {
          initData,
          client: clientValue,
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const body = await response.json();
        assert.equal(response.status, 400);
        assert.equal(body.error, "Payload must include `client` object.");
      }

      await t.test("accepts object client payload", async () => {
        const response = await postMiniClients(baseUrl, {
          initData,
          client: {
            clientName: "Object Client",
            notes: "object payload",
          },
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const body = await response.json();

        assert.equal(response.status, 201);
        assert.equal(body.ok, true);
        assert.equal(body.status, "pending");
      });

      await t.test("accepts JSON string client payload", async () => {
        const response = await postMiniClients(baseUrl, {
          initData,
          client: JSON.stringify({
            clientName: "JSON Client",
            notes: "json string payload",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const body = await response.json();

        assert.equal(response.status, 201);
        assert.equal(body.ok, true);
        assert.equal(body.status, "pending");
      });

      await t.test("rejects garbage JSON string", async () => {
        await expectInvalidClient("{not-json");
      });

      await t.test("rejects empty client string", async () => {
        await expectInvalidClient("");
      });

      await t.test("rejects JSON array string", async () => {
        await expectInvalidClient("[]");
      });

      await t.test("rejects null client value", async () => {
        await expectInvalidClient(null);
      });
    },
  );
});

test("POST /api/mini/clients validates and normalizes Mini payload fields", async (t) => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-capture-"));
  const captureFilePath = path.join(captureDir, "telegram-requests.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_NOTIFY_CHAT_ID: "-100700700700",
        TEST_TELEGRAM_CAPTURE_FILE: captureFilePath,
        TELEGRAM_INIT_DATA_TTL_SEC: "120",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 909, username: "normalization_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        async function expect400(client, expectedError) {
          const response = await postMiniClients(baseUrl, { initData, client }, {
            [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
          });
          const body = await response.json();
          assert.equal(response.status, 400);
          assert.equal(body.error, expectedError);
        }

        await t.test("rejects missing required clientName", async () => {
          await expect400(
            {
              clientName: "   ",
            },
            "`clientName` is required.",
          );
        });

        await t.test("rejects invalid date fields", async () => {
          await expect400(
            {
              clientName: "Invalid Date Client",
              payment1Date: "13/45/2026",
            },
            'Invalid date in field "payment1Date". Use MM/DD/YYYY.',
          );
        });

        await t.test("rejects invalid SSN format", async () => {
          await expect400(
            {
              clientName: "Invalid SSN Client",
              ssn: "12-34",
            },
            "Invalid SSN format. Use XXX-XX-XXXX.",
          );
        });

        await t.test("rejects invalid phone format", async () => {
          await expect400(
            {
              clientName: "Invalid Phone Client",
              clientPhoneNumber: "12345",
            },
            "Invalid client phone format. Use +1(XXX)XXX-XXXX.",
          );
        });

        await t.test("rejects invalid email format", async () => {
          await expect400(
            {
              clientName: "Invalid Email Client",
              clientEmailAddress: "invalid-email",
            },
            "Invalid client email. Email must include @.",
          );
        });

        await t.test("normalizes valid payload fields and auto-fills written-off date", async () => {
          const response = await postMiniClients(baseUrl, {
            initData,
            client: {
              clientName: "Normalization Client",
              payment1Date: "2026-02-03",
              ssn: "123456789",
              clientPhoneNumber: "1234567890",
              clientEmailAddress: "  normalized@example.com  ",
              afterResult: true,
              writtenOff: true,
            },
          }, {
            [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
          });
          const body = await response.json();

          assert.equal(response.status, 201);
          assert.equal(body.ok, true);
          assert.equal(body.status, "pending");

          const messageText = readCapturedSendMessageText(captureFilePath);
          assert.ok(messageText.includes("- Payment 1 date: 02/03/2026"));
          assert.ok(messageText.includes("- SSN: 123-45-6789"));
          assert.ok(messageText.includes("- Client phone number: +1(123)456-7890"));
          assert.ok(messageText.includes("- Client email address: normalized@example.com"));
          assert.ok(messageText.includes("- After result: Yes"));
          assert.ok(messageText.includes("- Written off: Yes"));
          assert.match(messageText, /- Date when written off: \d{2}\/\d{2}\/\d{4}/);
        });
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
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
      const validInitData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 202, username: "auth_fail_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, validInitData);

      const invalidInitData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 202, username: "auth_fail_user" },
        invalidHash: true,
      });

      const response = await postMiniClients(baseUrl, {
        initData: invalidInitData,
        client: { clientName: "Auth Fail" },
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
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
      const uploadToken = createMiniUploadTokenForUser("303");

      const response = await postMiniClients(baseUrl, {
        initData,
        client: { clientName: "Forbidden User" },
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
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
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClients(baseUrl, {
        initData,
        client: {
          clientName: "Success Client",
          clientEmailAddress: "success@example.com",
        },
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
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
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClients(baseUrl, {
        initData,
        client: {
          clientName: "Notify Failure Client",
        },
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
      });
      const body = await response.json();

      assert.equal(response.status, 201);
      assert.equal(body.ok, true);
      assert.equal(body.status, "pending");
      assert.equal(body.attachmentsCount, 0);
    },
  );
});
