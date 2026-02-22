"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const net = require("node:net");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_ENTRY = path.join(PROJECT_ROOT, "server.js");
const TEST_SERVER_STARTUP_TIMEOUT_MS = 30000;
const TEST_SERVER_STARTUP_ATTEMPTS = 2;
const TEST_TOTP_SECRET = "JBSWY3DPEHPK3PXP";
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
      DATABASE_URL: "",
      TELEGRAM_ALLOWED_USER_IDS: "",
      TELEGRAM_REQUIRED_CHAT_ID: "",
      TELEGRAM_NOTIFY_CHAT_ID: "",
      TELEGRAM_NOTIFY_THREAD_ID: "",
      WEB_AUTH_SESSION_SECRET: "mini_web_auth_totp_test_secret_value_1234567890",
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

function decodeBase32Secret(secret) {
  const normalized = (secret || "").toString().trim().toUpperCase().replace(/[\s=-]+/g, "");
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = 0;
  let value = 0;
  const bytes = [];

  for (const char of normalized) {
    const index = alphabet.indexOf(char);
    if (index < 0) {
      throw new Error(`Invalid base32 character: ${char}`);
    }
    value = (value << 5) | index;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }

  return Buffer.from(bytes);
}

function buildTotpCode(secret, nowMs = Date.now(), periodSec = 30) {
  const secretBuffer = decodeBase32Secret(secret);
  const counter = Math.floor(nowMs / (periodSec * 1000));
  const counterBuffer = Buffer.alloc(8);
  counterBuffer.writeBigUInt64BE(BigInt(counter), 0);
  const digest = require("node:crypto").createHmac("sha1", secretBuffer).update(counterBuffer).digest();
  const offset = digest[digest.length - 1] & 0x0f;
  const binary =
    (((digest[offset] & 0x7f) << 24) |
      ((digest[offset + 1] & 0xff) << 16) |
      ((digest[offset + 2] & 0xff) << 8) |
      (digest[offset + 3] & 0xff)) >>>
    0;
  return String(binary % 1_000_000).padStart(6, "0");
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

function buildCookieHeaderFromSetCookie(response) {
  const setCookieHeaders = getSetCookieHeaders(response);
  return setCookieHeaders
    .map((cookie) => cookie.split(";")[0]?.trim())
    .filter(Boolean)
    .join("; ");
}

test("POST /api/auth/login enforces TOTP for configured users", async () => {
  const users = [
    {
      username: "mfa.user",
      password: "TopSecret123!",
      displayName: "MFA User",
      department: "sales",
      role: "manager",
      totpSecret: TEST_TOTP_SECRET,
      totpEnabled: true,
    },
  ];

  await withServer(
    {
      WEB_AUTH_USERNAME: "owner",
      WEB_AUTH_PASSWORD: "OwnerPass123!",
      WEB_AUTH_OWNER_USERNAME: "owner",
      WEB_AUTH_USERS_JSON: JSON.stringify(users),
    },
    async ({ baseUrl }) => {
      const withoutCodeResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "mfa.user",
          password: "TopSecret123!",
        }),
      });
      const withoutCodeBody = await withoutCodeResponse.json();

      assert.equal(withoutCodeResponse.status, 401);
      assert.equal(withoutCodeBody.code, "two_factor_required");
      assert.equal(withoutCodeBody.twoFactorRequired, true);

      const invalidCodeResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "mfa.user",
          password: "TopSecret123!",
          totpCode: "000000",
        }),
      });
      const invalidCodeBody = await invalidCodeResponse.json();

      assert.equal(invalidCodeResponse.status, 401);
      assert.equal(invalidCodeBody.code, "two_factor_invalid");
      assert.equal(invalidCodeBody.twoFactorRequired, true);

      const validCode = buildTotpCode(TEST_TOTP_SECRET);
      const validCodeResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "mfa.user",
          password: "TopSecret123!",
          totpCode: validCode,
        }),
      });
      const validCodeBody = await validCodeResponse.json();

      assert.equal(validCodeResponse.status, 200);
      assert.equal(validCodeBody.ok, true);
      assert.equal(validCodeBody.user?.username, "mfa.user");
      assert.equal(validCodeBody.user?.totpEnabled, true);
      const setCookie = getSetCookieHeaders(validCodeResponse).join("; ");
      assert.match(setCookie, /cbooster_auth_session=/);
      assert.match(setCookie, new RegExp(`${WEB_AUTH_CSRF_COOKIE_NAME}=`));

      const ownerLoginResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "owner",
          password: "OwnerPass123!",
        }),
      });
      const ownerLoginBody = await ownerLoginResponse.json();
      assert.equal(ownerLoginResponse.status, 200);
      assert.equal(ownerLoginBody.ok, true);
      assert.equal(ownerLoginBody.user?.totpEnabled, false);
    },
  );
});

test("first login bypasses TOTP and first-password page includes QR setup", async () => {
  const users = [
    {
      username: "new.user",
      password: "TempPass123!",
      displayName: "New User",
      department: "sales",
      role: "manager",
      mustChangePassword: true,
      totpSecret: TEST_TOTP_SECRET,
      totpEnabled: true,
    },
  ];

  await withServer(
    {
      WEB_AUTH_USERNAME: "owner",
      WEB_AUTH_PASSWORD: "OwnerPass123!",
      WEB_AUTH_OWNER_USERNAME: "owner",
      WEB_AUTH_USERS_JSON: JSON.stringify(users),
    },
    async ({ baseUrl }) => {
      const firstApiLoginResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "new.user",
          password: "TempPass123!",
        }),
      });
      const firstApiLoginBody = await firstApiLoginResponse.json();

      assert.equal(firstApiLoginResponse.status, 200);
      assert.equal(firstApiLoginBody.ok, true);
      assert.equal(firstApiLoginBody.mustChangePassword, true);
      assert.equal(firstApiLoginBody.passwordChangePath, "/first-password");

      const webLoginResponse = await fetch(`${baseUrl}/login`, {
        method: "POST",
        redirect: "manual",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          username: "new.user",
          password: "TempPass123!",
          next: "/app/dashboard",
        }).toString(),
      });

      assert.equal(webLoginResponse.status, 302);
      const firstPasswordLocation = webLoginResponse.headers.get("location") || "";
      assert.match(firstPasswordLocation, /^\/first-password\?next=/);

      const sessionCookies = buildCookieHeaderFromSetCookie(webLoginResponse);
      assert.match(sessionCookies, /cbooster_auth_session=/);

      const firstPasswordPageResponse = await fetch(`${baseUrl}${firstPasswordLocation}`, {
        method: "GET",
        headers: {
          Cookie: sessionCookies,
          Accept: "text/html",
        },
      });
      const firstPasswordHtml = await firstPasswordPageResponse.text();

      assert.equal(firstPasswordPageResponse.status, 200);
      assert.match(firstPasswordHtml, /Authenticator setup QR code/);
      assert.match(firstPasswordHtml, /Install Google Authenticator/);
      assert.match(firstPasswordHtml, /name="totpSecret"/);

      const totpSecretMatch = firstPasswordHtml.match(/name="totpSecret"\s+value="([A-Z2-7]+)"/);
      assert.ok(totpSecretMatch, "Expected hidden totpSecret in /first-password form");
      const setupSecret = totpSecretMatch[1];

      const firstPasswordSubmitResponse = await fetch(`${baseUrl}/first-password`, {
        method: "POST",
        redirect: "manual",
        headers: {
          Cookie: sessionCookies,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: new URLSearchParams({
          next: "/app/dashboard",
          newPassword: "UpdatedPass123!",
          confirmPassword: "UpdatedPass123!",
          totpSecret: setupSecret,
        }).toString(),
      });

      assert.equal(firstPasswordSubmitResponse.status, 302);
      assert.equal(firstPasswordSubmitResponse.headers.get("location"), "/app/dashboard");

      const secondApiLoginWithoutCode = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "new.user",
          password: "UpdatedPass123!",
        }),
      });
      const secondApiLoginWithoutCodeBody = await secondApiLoginWithoutCode.json();

      assert.equal(secondApiLoginWithoutCode.status, 401);
      assert.equal(secondApiLoginWithoutCodeBody.code, "two_factor_required");
      assert.equal(secondApiLoginWithoutCodeBody.twoFactorRequired, true);

      const validCode = buildTotpCode(setupSecret);
      const secondApiLoginWithCode = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: "new.user",
          password: "UpdatedPass123!",
          totpCode: validCode,
        }),
      });
      const secondApiLoginWithCodeBody = await secondApiLoginWithCode.json();

      assert.equal(secondApiLoginWithCode.status, 200);
      assert.equal(secondApiLoginWithCodeBody.ok, true);
      assert.equal(secondApiLoginWithCodeBody.mustChangePassword, false);
      assert.equal(secondApiLoginWithCodeBody.user?.totpEnabled, true);
    },
  );
});
