"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const http = require("node:http");
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
const TEST_WEB_AUTH_PASSWORD_HASH = "$2b$10$MpB./1tOb0ZE6.iPuOikWuHbK3svW2fleu34gqhmYNjy4jQLGn3Gi";
const MINI_UPLOAD_TOKEN_HEADER_NAME = "x-mini-upload-token";
const TEST_SERVER_STARTUP_TIMEOUT_MS = 30000;
const TEST_SERVER_STARTUP_ATTEMPTS = 2;

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
  let lastStartupError = null;

  for (let attempt = 1; attempt <= TEST_SERVER_STARTUP_ATTEMPTS; attempt += 1) {
    const requestedPort = envOverrides && Object.prototype.hasOwnProperty.call(envOverrides, "PORT")
      ? String(envOverrides.PORT || "")
      : "";
    const port = requestedPort || String(await reserveFreePort());
    const env = {
      ...process.env,
      NODE_ENV: "test",
      SERVER_AUTOSTART_IN_TEST: "true",
      PORT: port,
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

    const baseUrl = `http://127.0.0.1:${env.PORT}`;

    try {
      await waitForServerReady(baseUrl, child, TEST_SERVER_STARTUP_TIMEOUT_MS);
      return {
        child,
        baseUrl,
      };
    } catch (error) {
      await stopServer(child);
      lastStartupError = new Error(
        `Attempt ${attempt}/${TEST_SERVER_STARTUP_ATTEMPTS}: ${error.message}\n--- stdout ---\n${stdout}\n--- stderr ---\n${stderr}`,
      );
      if (attempt < TEST_SERVER_STARTUP_ATTEMPTS) {
        await delay(250);
      }
    }
  }

  throw lastStartupError || new Error("Failed to start test server.");
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

function buildTelegramInitData({ authDate, user, botToken = TELEGRAM_BOT_TOKEN, invalidHash = false, queryId = "AAEAAAE" }) {
  const params = new URLSearchParams();
  params.set("auth_date", String(authDate));
  params.set("query_id", String(queryId || "AAEAAAE"));
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

async function waitForCapturedSendMessageText(captureFilePath, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const text = readCapturedSendMessageText(captureFilePath);
    if (text) {
      return text;
    }
    await delay(50);
  }
  return readCapturedSendMessageText(captureFilePath);
}

function readPgCaptureEvents(captureFilePath) {
  const raw = fs.existsSync(captureFilePath) ? fs.readFileSync(captureFilePath, "utf8") : "";
  const lines = raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const events = [];
  for (const line of lines) {
    try {
      const parsed = JSON.parse(line);
      if (parsed && typeof parsed === "object") {
        events.push(parsed);
      }
    } catch {
      // Ignore malformed lines.
    }
  }
  return events;
}

function makeBytes(size, fillValue = 65) {
  const safeSize = Math.max(0, Number.parseInt(size, 10) || 0);
  const array = new Uint8Array(safeSize);
  if (safeSize > 0) {
    array.fill(fillValue);
  }
  return array;
}

function makePdfBytes(size = 64) {
  const safeSize = Math.max(8, Number.parseInt(size, 10) || 8);
  const bytes = makeBytes(safeSize, 32);
  const header = Buffer.from("%PDF-1.7\n", "utf8");
  bytes.set(header.subarray(0, Math.min(header.length, bytes.length)), 0);
  return bytes;
}

function buildValidMiniClient(overrides = {}) {
  return {
    clientName: "Test Client",
    closedBy: "Sales Manager",
    companyName: "Test Company",
    serviceType: "Credit Booster",
    contractTotals: "200",
    payment1: "100",
    payment1Date: "02/18/2026",
    ...overrides,
  };
}

async function postMiniClientsMultipart(baseUrl, options) {
  const form = new FormData();
  form.append("initData", String(options?.initData || ""));
  const client = options?.client;
  if (typeof client === "string") {
    form.append("client", client);
  } else {
    form.append("client", JSON.stringify(client || {}));
  }

  const attachments = Array.isArray(options?.attachments) ? options.attachments : [];
  for (const attachment of attachments) {
    const bytes = attachment?.bytes instanceof Uint8Array
      ? attachment.bytes
      : makeBytes(attachment?.size || 0);
    const blob = new Blob([bytes], {
      type: String(attachment?.mimeType || "application/octet-stream"),
    });
    const fieldName = String(attachment?.fieldName || "attachments");
    form.append(fieldName, blob, String(attachment?.fileName || "file.bin"));
  }

  return await fetch(`${baseUrl}/api/mini/clients`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      [MINI_UPLOAD_TOKEN_HEADER_NAME]: String(options?.uploadToken || ""),
    },
    body: form,
  });
}

async function postMiniClientsMultipartChunked(baseUrl, options) {
  const requestUrl = new URL("/api/mini/clients", baseUrl);
  const boundary = `----miniChunked${Date.now().toString(16)}${Math.random().toString(16).slice(2)}`;
  const chunkSize = Math.max(8 * 1024, Number.parseInt(options?.chunkSize, 10) || 256 * 1024);
  const attachments = Array.isArray(options?.attachments) ? options.attachments : [];
  const clientValue = typeof options?.client === "string" ? options.client : JSON.stringify(options?.client || {});

  return await new Promise((resolve, reject) => {
    const request = http.request(
      {
        method: "POST",
        hostname: requestUrl.hostname,
        port: requestUrl.port,
        path: requestUrl.pathname,
        headers: {
          Accept: "application/json",
          "Content-Type": `multipart/form-data; boundary=${boundary}`,
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: String(options?.uploadToken || ""),
        },
      },
      (response) => {
        const chunks = [];
        response.on("data", (chunk) => {
          chunks.push(Buffer.from(chunk));
        });
        response.on("end", () => {
          const bodyText = Buffer.concat(chunks).toString("utf8");
          resolve({
            status: response.statusCode || 0,
            headers: response.headers || {},
            bodyText,
            async json() {
              if (!bodyText) {
                return {};
              }
              return JSON.parse(bodyText);
            },
          });
        });
      },
    );

    request.on("error", reject);

    function writeField(name, value) {
      request.write(`--${boundary}\r\n`);
      request.write(`Content-Disposition: form-data; name="${name}"\r\n\r\n`);
      request.write(String(value || ""));
      request.write("\r\n");
    }

    writeField("initData", String(options?.initData || ""));
    writeField("client", clientValue);

    for (const attachment of attachments) {
      const fieldName = String(attachment?.fieldName || "attachments");
      const fileName = String(attachment?.fileName || "file.bin");
      const mimeType = String(attachment?.mimeType || "application/octet-stream");
      const bytes =
        attachment?.bytes instanceof Uint8Array ? Buffer.from(attachment.bytes) : Buffer.from(makeBytes(attachment?.size || 0));

      request.write(`--${boundary}\r\n`);
      request.write(`Content-Disposition: form-data; name="${fieldName}"; filename="${fileName}"\r\n`);
      request.write(`Content-Type: ${mimeType}\r\n\r\n`);

      for (let offset = 0; offset < bytes.length; offset += chunkSize) {
        request.write(bytes.subarray(offset, Math.min(offset + chunkSize, bytes.length)));
      }

      request.write("\r\n");
    }

    request.end(`--${boundary}--\r\n`);
  });
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
        client: buildValidMiniClient({ clientName: "John Doe" }),
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
      const secondInitData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 111, username: "parse_payload_user" },
        queryId: "AAEAAA_SECOND",
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
          client: buildValidMiniClient({
            clientName: "Object Client",
            notes: "object payload",
          }),
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
          initData: secondInitData,
          client: JSON.stringify(
            buildValidMiniClient({
              clientName: "JSON Client",
              notes: "json string payload",
            }),
          ),
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
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-normalization-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "120",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 909, username: "normalization_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        async function expect400(clientOverrides, expectedError) {
          const response = await postMiniClients(baseUrl, {
            initData,
            client: buildValidMiniClient(clientOverrides),
          }, {
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
            client: buildValidMiniClient({
              clientName: "Normalization Client",
              payment1Date: "2026-02-03",
              ssn: "123456789",
              clientPhoneNumber: "1234567890",
              clientEmailAddress: "  normalized@example.com  ",
              afterResult: true,
              writtenOff: true,
            }),
          }, {
            [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
          });
          const body = await response.json();

          assert.equal(response.status, 201);
          assert.equal(body.ok, true);
          assert.equal(body.status, "pending");

          const events = readPgCaptureEvents(pgCaptureFilePath);
          const submissionInsert = [...events].reverse().find((event) => event.type === "submission_insert");
          assert.ok(submissionInsert && typeof submissionInsert === "object");

          const record = submissionInsert.record || {};
          const miniData = submissionInsert.miniData || {};

          assert.equal(record.payment1Date, "02/03/2026");
          assert.equal(record.afterResult, "Yes");
          assert.equal(record.writtenOff, "Yes");
          assert.match(String(record.dateWhenWrittenOff || ""), /^\d{2}\/\d{2}\/\d{4}$/);

          assert.equal(miniData.ssn, "123-45-6789");
          assert.equal(miniData.clientPhoneNumber, "+1(123)456-7890");
          assert.equal(miniData.clientEmailAddress, "normalized@example.com");
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
        client: buildValidMiniClient({ clientName: "Auth Fail" }),
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
        client: buildValidMiniClient({ clientName: "Forbidden User" }),
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
        client: buildValidMiniClient({
          clientName: "Success Client",
          clientEmailAddress: "success@example.com",
        }),
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

test("POST /api/mini/clients enforces stricter write TTL than access endpoint", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "86400",
      TELEGRAM_INIT_DATA_WRITE_TTL_SEC: "60",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const staleInitData = buildTelegramInitData({
        authDate: nowSeconds - 120,
        user: { id: 441, username: "write_ttl_user" },
      });

      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, staleInitData);
      const response = await postMiniClients(baseUrl, {
        initData: staleInitData,
        client: buildValidMiniClient({
          clientName: "Write TTL Client",
        }),
      }, {
        [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
      });
      const body = await response.json();

      assert.equal(response.status, 401);
      assert.equal(body.error, "Telegram session expired. Reopen Mini App from Telegram chat.");
    },
  );
});

test("POST /api/mini/clients blocks replay of the same Telegram initData", async () => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-replay-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "600",
        TELEGRAM_INIT_DATA_WRITE_TTL_SEC: "600",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 442, username: "replay_user" },
          queryId: "AAEAAA_REPLAY",
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        const firstResponse = await postMiniClients(baseUrl, {
          initData,
          client: buildValidMiniClient({
            clientName: "Replay Client #1",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const firstBody = await firstResponse.json();
        assert.equal(firstResponse.status, 201);
        assert.equal(firstBody.ok, true);

        const secondResponse = await postMiniClients(baseUrl, {
          initData,
          client: buildValidMiniClient({
            clientName: "Replay Client #2",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const secondBody = await secondResponse.json();
        assert.equal(secondResponse.status, 409);
        assert.equal(secondBody.code, "mini_init_data_replay");

        const events = readPgCaptureEvents(pgCaptureFilePath);
        const submissionInsertEvents = events.filter((event) => event.type === "submission_insert");
        assert.equal(submissionInsertEvents.length, 1);
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
});

test("POST /api/mini/clients replays successful response for the same Idempotency-Key", async () => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-idempotency-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "600",
        TELEGRAM_INIT_DATA_WRITE_TTL_SEC: "600",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const user = { id: 443, username: "idempotency_user" };
        const initDataFirst = buildTelegramInitData({
          authDate: nowSeconds,
          user,
          queryId: "AAEAAA_IDEMP_1",
        });
        const initDataSecond = buildTelegramInitData({
          authDate: nowSeconds,
          user,
          queryId: "AAEAAA_IDEMP_2",
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initDataFirst);
        const idempotencyKey = "mini-submit-443-key-001";

        const firstResponse = await postMiniClients(baseUrl, {
          initData: initDataFirst,
          client: buildValidMiniClient({
            clientName: "Idempotent Client #1",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
          "idempotency-key": idempotencyKey,
        });
        const firstBody = await firstResponse.json();
        assert.equal(firstResponse.status, 201);
        assert.equal(firstBody.ok, true);

        const secondResponse = await postMiniClients(baseUrl, {
          initData: initDataSecond,
          client: buildValidMiniClient({
            clientName: "Idempotent Client #2",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
          "idempotency-key": idempotencyKey,
        });
        const secondBody = await secondResponse.json();
        assert.equal(secondResponse.status, 201);
        assert.equal(secondResponse.headers.get("idempotency-replayed"), "true");
        assert.deepEqual(secondBody, firstBody);

        const events = readPgCaptureEvents(pgCaptureFilePath);
        const submissionInsertEvents = events.filter((event) => event.type === "submission_insert");
        assert.equal(submissionInsertEvents.length, 1);
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
});

test("POST /api/mini/clients masks sensitive fields in Telegram notifications", async () => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-telegram-mask-"));
  const captureFilePath = path.join(captureDir, "telegram-capture.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_NOTIFY_CHAT_ID: "-100900900900",
        TELEGRAM_NOTIFY_FIELDS: "clientName,ssn,clientPhoneNumber,clientEmailAddress",
        TEST_TELEGRAM_CAPTURE_FILE: captureFilePath,
        TEST_TELEGRAM_FETCH_MODE: "status_member",
        TELEGRAM_INIT_DATA_TTL_SEC: "120",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 506, username: "privacy_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        const response = await postMiniClients(baseUrl, {
          initData,
          client: buildValidMiniClient({
            clientName: "Privacy Client",
            ssn: "123456789",
            clientPhoneNumber: "1234567890",
            clientEmailAddress: "normalized@example.com",
          }),
        }, {
          [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
        });
        const body = await response.json();

        assert.equal(response.status, 201);
        assert.equal(body.ok, true);

        const messageText = await waitForCapturedSendMessageText(captureFilePath);
        assert.ok(messageText.includes("- Client name: Privacy Client"));
        assert.ok(messageText.includes("- SSN: ***-**-6789"));
        assert.ok(messageText.includes("- Client phone number: ***-***-7890"));
        assert.ok(messageText.includes("- Client email address: n***@e***.com"));
        assert.equal(messageText.includes("123-45-6789"), false);
        assert.equal(messageText.includes("+1(123)456-7890"), false);
        assert.equal(messageText.includes("normalized@example.com"), false);
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
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
        client: buildValidMiniClient({
          clientName: "Notify Failure Client",
        }),
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

test("POST /api/mini/clients enforces attachment security before DB write", async (t) => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-attachments-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "600",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 606, username: "attachments_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        async function expectAttachmentError(attachments, expectedStatus, expectedMessagePart) {
          const eventsBefore = readPgCaptureEvents(pgCaptureFilePath).length;
          const response = await postMiniClientsMultipart(baseUrl, {
            initData,
            uploadToken,
            client: buildValidMiniClient({ clientName: "Attachment Client" }),
            attachments,
          });
          const body = await response.json();

          assert.equal(response.status, expectedStatus);
          assert.equal(typeof body.error, "string");
          assert.ok(body.error.includes(expectedMessagePart), `Unexpected error message: ${body.error}`);

          const eventsAfter = readPgCaptureEvents(pgCaptureFilePath).length;
          assert.equal(eventsAfter, eventsBefore, "Blocked attachment request should not write to DB.");
        }

        await t.test("blocks more than 10 files", async () => {
          const attachments = Array.from({ length: 11 }, (_, index) => ({
            fileName: `file-${index + 1}.txt`,
            mimeType: "text/plain",
            bytes: makeBytes(8),
          }));
          await expectAttachmentError(attachments, 400, "You can upload up to 10 files.");
        });

        await t.test("blocks single file over 10 MB", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "too-big.pdf",
                mimeType: "application/pdf",
                bytes: makeBytes(10 * 1024 * 1024 + 1),
              },
            ],
            400,
            "10 MB",
          );
        });

        await t.test("blocks total attachments over 40 MB", async () => {
          const nearMaxPerFile = 10 * 1024 * 1024 - 1024;
          const attachments = [
            {
              fileName: "bulk-1.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(nearMaxPerFile),
            },
            {
              fileName: "bulk-2.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(nearMaxPerFile),
            },
            {
              fileName: "bulk-3.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(nearMaxPerFile),
            },
            {
              fileName: "bulk-4.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(nearMaxPerFile),
            },
            {
              fileName: "bulk-5.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(5000),
            },
          ];
          await expectAttachmentError(attachments, 413, "40 MB");
        });

        await t.test("blocks dangerous extension", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "../evil.js",
                mimeType: "text/plain",
                bytes: makeBytes(64),
              },
            ],
            400,
            "not allowed",
          );
        });

        await t.test("blocks dangerous MIME type", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "safe.txt",
                mimeType: "text/html",
                bytes: makeBytes(64),
              },
            ],
            400,
            "MIME type does not match",
          );
        });

        await t.test("blocks dangerous MIME pattern", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "safe.txt",
                mimeType: "application/x-sh",
                bytes: makeBytes(64),
              },
            ],
            400,
            "MIME type does not match",
          );
        });

        await t.test("blocks extension/MIME spoofing with mismatched magic bytes", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "invoice.pdf",
                mimeType: "application/pdf",
                bytes: makeBytes(256, 65),
              },
            ],
            400,
            "content does not match",
          );
        });

        await t.test("rejects empty buffer/path attachment", async () => {
          await expectAttachmentError(
            [
              {
                fileName: "empty.pdf",
                mimeType: "application/pdf",
                bytes: makeBytes(0),
              },
            ],
            400,
            "Failed to read",
          );
        });

        await t.test("sanitizes file name before insert", async () => {
          const response = await postMiniClientsMultipart(baseUrl, {
            initData,
            uploadToken,
            client: buildValidMiniClient({ clientName: "Sanitize File Name Client" }),
            attachments: [
              {
                fileName: "../unsafe<>name?.pdf",
                mimeType: "application/pdf",
                bytes: makePdfBytes(128),
              },
            ],
          });
          const body = await response.json();

          assert.equal(response.status, 201);
          assert.equal(body.ok, true);

          const events = readPgCaptureEvents(pgCaptureFilePath);
          const lastFileInsert = [...events].reverse().find((event) => event.type === "file_insert");
          assert.ok(lastFileInsert, "Expected file_insert event in fake DB capture.");
          assert.equal(typeof lastFileInsert.fileName, "string");
          assert.ok(lastFileInsert.fileName.length > 0);
          assert.ok(lastFileInsert.fileName.length <= 180);
          assert.ok(!lastFileInsert.fileName.includes("/"));
          assert.ok(!lastFileInsert.fileName.includes("\\"));
          assert.ok(!lastFileInsert.fileName.includes("<"));
          assert.ok(!lastFileInsert.fileName.includes(">"));
          assert.ok(lastFileInsert.fileName.endsWith(".pdf"));
        });
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
});

test("POST /api/mini/clients enforces total attachment budget for chunked multipart uploads", async () => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-chunked-budget-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "600",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 1008, username: "chunked_budget_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        const nearMaxPerFile = 10 * 1024 * 1024 - 1024;
        const attachments = [
          { fileName: "chunked-1.pdf", mimeType: "application/pdf", bytes: makePdfBytes(nearMaxPerFile) },
          { fileName: "chunked-2.pdf", mimeType: "application/pdf", bytes: makePdfBytes(nearMaxPerFile) },
          { fileName: "chunked-3.pdf", mimeType: "application/pdf", bytes: makePdfBytes(nearMaxPerFile) },
          { fileName: "chunked-4.pdf", mimeType: "application/pdf", bytes: makePdfBytes(nearMaxPerFile) },
          { fileName: "chunked-5.pdf", mimeType: "application/pdf", bytes: makePdfBytes(5000) },
        ];

        const eventsBefore = readPgCaptureEvents(pgCaptureFilePath).length;
        const response = await postMiniClientsMultipartChunked(baseUrl, {
          initData,
          uploadToken,
          client: buildValidMiniClient({ clientName: "Chunked Budget Client" }),
          attachments,
          chunkSize: 64 * 1024,
        });
        const body = await response.json();

        assert.equal(response.status, 413);
        assert.equal(typeof body.error, "string");
        assert.ok(body.error.includes("40 MB"), `Unexpected error message: ${body.error}`);
        const eventsAfter = readPgCaptureEvents(pgCaptureFilePath).length;
        assert.equal(eventsAfter, eventsBefore, "Rejected chunked upload should not write to DB.");
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
});

test("POST /api/mini/clients accepts multipart string client payload", async () => {
  const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), "mini-clients-multipart-string-client-"));
  const pgCaptureFilePath = path.join(captureDir, "pg-events.jsonl");

  try {
    await withServer(
      {
        DATABASE_URL: "postgres://fake/fake",
        TEST_USE_FAKE_PG: "1",
        TEST_PG_CAPTURE_FILE: pgCaptureFilePath,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_INIT_DATA_TTL_SEC: "600",
      },
      async ({ baseUrl }) => {
        const nowSeconds = Math.floor(Date.now() / 1000);
        const initData = buildTelegramInitData({
          authDate: nowSeconds,
          user: { id: 1012, username: "multipart_string_client_user" },
        });
        const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

        const response = await postMiniClientsMultipart(baseUrl, {
          initData,
          uploadToken,
          client: JSON.stringify(
            buildValidMiniClient({
              clientName: "Multipart String Client",
            }),
          ),
          attachments: [
            {
              fileName: "multipart-string-client.pdf",
              mimeType: "application/pdf",
              bytes: makePdfBytes(256),
            },
          ],
        });
        const body = await response.json();

        assert.equal(response.status, 201);
        assert.equal(body.ok, true);
        assert.equal(body.status, "pending");

        const events = readPgCaptureEvents(pgCaptureFilePath);
        const lastFileInsert = [...events].reverse().find((event) => event.type === "file_insert");
        assert.ok(lastFileInsert, "Expected file_insert event for multipart attachment.");
        assert.equal(lastFileInsert.fileName, "multipart-string-client.pdf");
        assert.equal(lastFileInsert.mimeType, "application/pdf");
      },
    );
  } finally {
    fs.rmSync(captureDir, { recursive: true, force: true });
  }
});

test("POST /api/mini/clients rejects multipart helper defaults when auth headers are missing", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
    },
    async ({ baseUrl }) => {
      const response = await postMiniClientsMultipart(baseUrl, {
        client: buildValidMiniClient({
          clientName: "Multipart Missing Auth Client",
        }),
        attachments: null,
      });
      const body = await response.json();

      assert.equal(response.status, 401);
      assert.equal(typeof body.error, "string");
    },
  );
});

test("POST /api/mini/clients rejects chunked helper fallback payload without auth", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
    },
    async ({ baseUrl }) => {
      const response = await postMiniClientsMultipartChunked(baseUrl, {
        client: "",
        attachments: [{}],
      });
      const body = await response.json();

      assert.equal(response.status, 401);
      assert.equal(typeof body.error, "string");
    },
  );
});

test("POST /api/mini/clients fails closed when AV scan is enabled but unavailable", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
      MINI_ATTACHMENT_AV_SCAN_ENABLED: "true",
      MINI_ATTACHMENT_AV_SCAN_BIN: "",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 706, username: "av_scan_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClientsMultipart(baseUrl, {
        initData,
        uploadToken,
        client: buildValidMiniClient({ clientName: "AV Scan Client" }),
        attachments: [
          {
            fileName: "scan.pdf",
            mimeType: "application/pdf",
            bytes: makePdfBytes(256),
          },
        ],
      });
      const body = await response.json();

      assert.equal(response.status, 503);
      assert.equal(typeof body.error, "string");
      assert.ok(body.error.includes("security scan is unavailable"));
    },
  );
});

test("POST /api/mini/clients keeps uploads available in production when AV is not explicitly enabled", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
      NODE_ENV: "production",
      WEB_AUTH_USERNAME: "owner_secure",
      WEB_AUTH_PASSWORD_HASH: TEST_WEB_AUTH_PASSWORD_HASH,
      MINI_ATTACHMENT_AV_SCAN_ENABLED: "",
      MINI_ATTACHMENT_AV_SCAN_BIN: "",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 709, username: "av_scan_prod_default_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClientsMultipart(baseUrl, {
        initData,
        uploadToken,
        client: buildValidMiniClient({ clientName: "AV Scan Prod Default Client" }),
        attachments: [
          {
            fileName: "scan-default.pdf",
            mimeType: "application/pdf",
            bytes: makePdfBytes(256),
          },
        ],
      });
      const body = await response.json();

      assert.equal(response.status, 201);
      assert.equal(body.ok, true);
      assert.equal(body.status, "pending");
    },
  );
});

test("POST /api/mini/clients allows fail-open mode in production when AV scanner is unavailable", async () => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
      NODE_ENV: "production",
      WEB_AUTH_USERNAME: "owner_secure",
      WEB_AUTH_PASSWORD_HASH: TEST_WEB_AUTH_PASSWORD_HASH,
      MINI_ATTACHMENT_AV_SCAN_ENABLED: "true",
      MINI_ATTACHMENT_AV_SCAN_FAIL_OPEN: "true",
      MINI_ATTACHMENT_AV_SCAN_BIN: "",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 710, username: "av_scan_prod_fail_open_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      const response = await postMiniClientsMultipart(baseUrl, {
        initData,
        uploadToken,
        client: buildValidMiniClient({ clientName: "AV Scan Prod Fail Open Client" }),
        attachments: [
          {
            fileName: "scan-fail-open.pdf",
            mimeType: "application/pdf",
            bytes: makePdfBytes(256),
          },
        ],
      });
      const body = await response.json();

      assert.equal(response.status, 201);
      assert.equal(body.ok, true);
      assert.equal(body.status, "pending");
    },
  );
});

test("POST /api/mini/clients maps multer limit errors to safe 400 responses", async (t) => {
  await withServer(
    {
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_INIT_DATA_TTL_SEC: "600",
    },
    async ({ baseUrl }) => {
      const nowSeconds = Math.floor(Date.now() / 1000);
      const initData = buildTelegramInitData({
        authDate: nowSeconds,
        user: { id: 707, username: "multer_limits_user" },
      });
      const uploadToken = await fetchUploadTokenFromAccess(baseUrl, initData);

      function assertNoInternalLeak(errorText) {
        const normalized = String(errorText || "");
        assert.ok(normalized.length > 0);
        assert.equal(normalized.includes("MulterError"), false);
        assert.equal(normalized.includes("stack"), false);
        assert.equal(/\bat\s+\S+/.test(normalized), false);
      }

      async function assertMulterErrorCase(attachments, expectedMessagePart) {
        const response = await postMiniClientsMultipart(baseUrl, {
          initData,
          uploadToken,
          client: buildValidMiniClient({ clientName: "Multer Limits Client" }),
          attachments,
        });
        const body = await response.json();

        assert.equal(response.status, 400);
        assert.equal(typeof body.error, "string");
        assert.ok(body.error.includes(expectedMessagePart), `Unexpected error: ${body.error}`);
        assertNoInternalLeak(body.error);
      }

      await t.test("LIMIT_FILE_COUNT -> readable 400", async () => {
        const attachments = Array.from({ length: 11 }, (_, index) => ({
          fileName: `too-many-${index + 1}.txt`,
          mimeType: "text/plain",
          bytes: makeBytes(10),
        }));

        await assertMulterErrorCase(attachments, "You can upload up to 10 files.");
      });

      await t.test("LIMIT_FILE_SIZE -> readable 400", async () => {
        await assertMulterErrorCase(
          [
            {
              fileName: "large.pdf",
              mimeType: "application/pdf",
              bytes: makeBytes(10 * 1024 * 1024 + 1),
            },
          ],
          "Each file must be up to 10 MB.",
        );
      });

      await t.test("LIMIT_UNEXPECTED_FILE -> readable 400", async () => {
        await assertMulterErrorCase(
          [
            {
              fieldName: "unexpectedFileField",
              fileName: "unexpected.pdf",
              mimeType: "application/pdf",
              bytes: makeBytes(64),
            },
          ],
          "You can upload up to 10 files.",
        );
      });
    },
  );
});
