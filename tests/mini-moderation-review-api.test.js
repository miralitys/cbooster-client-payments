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
const TELEGRAM_BOT_TOKEN = "test_bot_token_for_moderation_reviews";
const TEST_WEB_AUTH_USERNAME = "owner_secure";
const TEST_WEB_AUTH_PASSWORD = "OwnerPass!123";
const TEST_SERVER_STARTUP_TIMEOUT_MS = 30000;
const TEST_SERVER_STARTUP_ATTEMPTS = 2;
const MINI_UPLOAD_TOKEN_HEADER_NAME = "x-mini-upload-token";
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
      DATABASE_URL: "postgres://fake/fake",
      TEST_USE_FAKE_PG: "1",
      TELEGRAM_BOT_TOKEN,
      TELEGRAM_ALLOWED_USER_IDS: "",
      TELEGRAM_REQUIRED_CHAT_ID: "",
      TELEGRAM_NOTIFY_CHAT_ID: "",
      TELEGRAM_NOTIFY_THREAD_ID: "",
      WEB_AUTH_USERNAME: TEST_WEB_AUTH_USERNAME,
      WEB_AUTH_PASSWORD: TEST_WEB_AUTH_PASSWORD,
      WEB_AUTH_OWNER_USERNAME: TEST_WEB_AUTH_USERNAME,
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

function buildTelegramInitData({ authDate, user, botToken = TELEGRAM_BOT_TOKEN, queryId }) {
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
  params.set("hash", hash);

  return params.toString();
}

function buildValidMiniClient(overrides = {}) {
  return {
    clientName: "Moderation Client",
    closedBy: "Sales Manager",
    leadSource: "Referral",
    ssn: "123-45-6789",
    clientPhoneNumber: "+1(312)555-7890",
    futurePayment: "03/15/2026",
    identityIq: "IdentityIQ checked",
    clientEmailAddress: "client@example.com",
    companyName: "Test Company",
    serviceType: "Credit Booster",
    contractTotals: "200",
    payment1: "100",
    payment1Date: "02/18/2026",
    notes: "Approved by QA matrix test",
    ...overrides,
  };
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

async function submitMiniClient(baseUrl, { initData, uploadToken, client }) {
  const response = await fetch(`${baseUrl}/api/mini/clients`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      [MINI_UPLOAD_TOKEN_HEADER_NAME]: uploadToken,
    },
    body: JSON.stringify({
      initData,
      client,
    }),
  });
  const body = await response.json();

  assert.equal(response.status, 201);
  assert.equal(body.ok, true);
  assert.equal(body.status, "pending");
  const submissionId = String(body.submissionId || body.id || "");
  assert.ok(submissionId.startsWith("sub-"));
  return submissionId;
}

function getSetCookieHeaders(response) {
  if (response?.headers && typeof response.headers.getSetCookie === "function") {
    return response.headers.getSetCookie();
  }

  const single = response?.headers?.get("set-cookie");
  if (!single) {
    return [];
  }

  return single.split(/,(?=[^;]+?=)/g);
}

function parseCookieMapFromSetCookie(setCookieHeaders) {
  const cookieMap = new Map();
  const items = Array.isArray(setCookieHeaders) ? setCookieHeaders : [];
  for (const item of items) {
    const raw = String(item || "");
    if (!raw) {
      continue;
    }
    const cookiePair = raw.split(";")[0] || "";
    const separatorIndex = cookiePair.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }
    const name = cookiePair.slice(0, separatorIndex).trim();
    const value = cookiePair.slice(separatorIndex + 1).trim();
    if (!name) {
      continue;
    }
    cookieMap.set(name, value);
  }
  return cookieMap;
}

function buildCookieHeader(cookieMap) {
  if (!(cookieMap instanceof Map) || !cookieMap.size) {
    return "";
  }
  return [...cookieMap.entries()].map(([name, value]) => `${name}=${value}`).join("; ");
}

async function loginWebSession(baseUrl) {
  const response = await fetch(`${baseUrl}/api/auth/login`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      username: TEST_WEB_AUTH_USERNAME,
      password: TEST_WEB_AUTH_PASSWORD,
    }),
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.ok, true);

  const cookieMap = parseCookieMapFromSetCookie(getSetCookieHeaders(response));
  const cookieHeader = buildCookieHeader(cookieMap);
  const csrfToken = String(cookieMap.get(WEB_AUTH_CSRF_COOKIE_NAME) || "");

  assert.ok(cookieHeader.length > 0, "Expected session cookies from /api/auth/login");
  assert.ok(csrfToken.length > 0, `Expected "${WEB_AUTH_CSRF_COOKIE_NAME}" cookie from /api/auth/login`);

  return {
    cookieHeader,
    csrfToken,
  };
}

async function fetchModerationSubmissions(baseUrl, session, status) {
  const response = await fetch(`${baseUrl}/api/moderation/submissions?status=${encodeURIComponent(status)}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Cookie: session.cookieHeader,
    },
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.ok(Array.isArray(body.items));
  return body.items;
}

async function reviewSubmission(baseUrl, session, submissionId, decision, reviewNote) {
  const response = await fetch(`${baseUrl}/api/moderation/submissions/${encodeURIComponent(submissionId)}/${decision}`, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Cookie: session.cookieHeader,
      "x-csrf-token": session.csrfToken,
    },
    body: JSON.stringify({
      reviewNote,
    }),
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.item?.id, submissionId);
  assert.equal(body.item?.status, decision === "approve" ? "approved" : "rejected");
  assert.equal(body.item?.reviewNote, reviewNote);
  return body.item;
}

async function fetchClientRecords(baseUrl, session) {
  const response = await fetch(`${baseUrl}/api/records`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Cookie: session.cookieHeader,
    },
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.ok(Array.isArray(body.records));
  return body.records;
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
      // Ignore malformed capture lines.
    }
  }
  return events;
}

function createUniqueQueryId(prefix) {
  const token = crypto.randomBytes(4).toString("hex");
  return `${prefix}_${Date.now()}_${token}`;
}

test("moderation approve/reject keeps read-after-approve consistency across write/read config matrix", async (t) => {
  const matrix = [
    {
      name: "legacy_only",
      writeV2: false,
      readV2: false,
      legacyMirror: false,
      expectV2Write: false,
      expectLegacyWrite: true,
    },
    {
      name: "write_v2_read_legacy",
      writeV2: true,
      readV2: false,
      legacyMirror: false,
      expectV2Write: true,
      expectLegacyWrite: true,
    },
    {
      name: "full_v2_no_legacy_mirror",
      writeV2: true,
      readV2: true,
      legacyMirror: false,
      expectV2Write: true,
      expectLegacyWrite: false,
    },
    {
      name: "full_v2_with_legacy_mirror",
      writeV2: true,
      readV2: true,
      legacyMirror: true,
      expectV2Write: true,
      expectLegacyWrite: true,
    },
  ];

  for (const config of matrix) {
    await t.test(config.name, async () => {
      const captureDir = fs.mkdtempSync(path.join(os.tmpdir(), `mini-review-${config.name}-`));
      const captureFilePath = path.join(captureDir, "pg-capture.ndjson");
      const nowSeconds = Math.floor(Date.now() / 1000);

      await withServer(
        {
          WRITE_V2: config.writeV2 ? "true" : "false",
          READ_V2: config.readV2 ? "true" : "false",
          LEGACY_MIRROR: config.legacyMirror ? "true" : "false",
          TELEGRAM_INIT_DATA_TTL_SEC: "600",
          TEST_PG_CAPTURE_FILE: captureFilePath,
        },
        async ({ baseUrl }) => {
          const approvedClientName = `Approved ${config.name}`;
          const rejectedClientName = `Rejected ${config.name}`;
          const telegramUser = {
            id: 9000 + matrix.findIndex((item) => item.name === config.name),
            username: `matrix_${config.name}`,
          };

          const approveInitData = buildTelegramInitData({
            authDate: nowSeconds,
            user: telegramUser,
            queryId: createUniqueQueryId(`approve_${config.name}`),
          });
          const approveUploadToken = await fetchUploadTokenFromAccess(baseUrl, approveInitData);
          const approvedSubmissionId = await submitMiniClient(baseUrl, {
            initData: approveInitData,
            uploadToken: approveUploadToken,
            client: buildValidMiniClient({
              clientName: approvedClientName,
              companyName: `Company ${config.name}`,
            }),
          });

          const session = await loginWebSession(baseUrl);

          const pendingBeforeReview = await fetchModerationSubmissions(baseUrl, session, "pending");
          assert.ok(
            pendingBeforeReview.some((item) => item.id === approvedSubmissionId),
            `Expected pending submission ${approvedSubmissionId} before approval`,
          );

          const approveNote = `approve note ${config.name}`;
          await reviewSubmission(baseUrl, session, approvedSubmissionId, "approve", approveNote);

          const approvedItems = await fetchModerationSubmissions(baseUrl, session, "approved");
          const approvedItem = approvedItems.find((item) => item.id === approvedSubmissionId) || null;
          assert.ok(approvedItem, `Expected approved submission ${approvedSubmissionId} in moderation list`);
          assert.equal(approvedItem.reviewNote, approveNote);
          assert.equal(typeof approvedItem.reviewedAt, "string");
          assert.ok(approvedItem.reviewedAt.length > 0);

          const recordsAfterApprove = await fetchClientRecords(baseUrl, session);
          assert.ok(
            recordsAfterApprove.some((record) => String(record?.clientName || "") === approvedClientName),
            `Expected "${approvedClientName}" in /api/records after approve (${config.name})`,
          );

          const rejectInitData = buildTelegramInitData({
            authDate: nowSeconds,
            user: telegramUser,
            queryId: createUniqueQueryId(`reject_${config.name}`),
          });
          const rejectUploadToken = await fetchUploadTokenFromAccess(baseUrl, rejectInitData);
          const rejectedSubmissionId = await submitMiniClient(baseUrl, {
            initData: rejectInitData,
            uploadToken: rejectUploadToken,
            client: buildValidMiniClient({
              clientName: rejectedClientName,
              companyName: `Rejected Company ${config.name}`,
            }),
          });

          const rejectNote = `reject note ${config.name}`;
          await reviewSubmission(baseUrl, session, rejectedSubmissionId, "reject", rejectNote);

          const rejectedItems = await fetchModerationSubmissions(baseUrl, session, "rejected");
          const rejectedItem = rejectedItems.find((item) => item.id === rejectedSubmissionId) || null;
          assert.ok(rejectedItem, `Expected rejected submission ${rejectedSubmissionId} in moderation list`);
          assert.equal(rejectedItem.reviewNote, rejectNote);

          const recordsAfterReject = await fetchClientRecords(baseUrl, session);
          assert.equal(
            recordsAfterReject.some((record) => String(record?.clientName || "") === rejectedClientName),
            false,
            `Rejected client "${rejectedClientName}" must not appear in /api/records (${config.name})`,
          );

          const captureEvents = readPgCaptureEvents(captureFilePath);
          const hasLegacyPrependWrite = captureEvents.some((event) => event?.type === "legacy_state_prepend");
          const hasV2UpsertWrite = captureEvents.some((event) => event?.type === "v2_record_upsert");

          assert.equal(
            hasLegacyPrependWrite,
            config.expectLegacyWrite,
            `Unexpected legacy write behavior for ${config.name}`,
          );
          assert.equal(
            hasV2UpsertWrite,
            config.expectV2Write,
            `Unexpected v2 write behavior for ${config.name}`,
          );
        },
      );
    });
  }
});
