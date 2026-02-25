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
const WEB_AUTH_LOGIN_CSRF_COOKIE_NAME = "cbooster_login_csrf";

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function escapeRegex(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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

function findSetCookieHeaderByName(response, cookieName) {
  const normalizedName = String(cookieName || "").trim();
  const header = parseSetCookieHeaders(response).find((entry) => {
    const normalized = String(entry || "").trim();
    return normalized.toLowerCase().startsWith(`${normalizedName.toLowerCase()}=`);
  });
  assert.ok(header, `Expected Set-Cookie for ${normalizedName}.`);
  return String(header);
}

function assertCoreSecurityHeaders(response, routeLabel) {
  const permissionsPolicy = String(response.headers.get("permissions-policy") || "").trim();
  assert.ok(permissionsPolicy, `${routeLabel} must include Permissions-Policy header.`);

  const xFrameOptions = String(response.headers.get("x-frame-options") || "").trim();
  assert.ok(xFrameOptions, `${routeLabel} must include X-Frame-Options header.`);

  const xContentTypeOptions = String(response.headers.get("x-content-type-options") || "").trim();
  assert.ok(xContentTypeOptions, `${routeLabel} must include X-Content-Type-Options header.`);

  const referrerPolicy = String(response.headers.get("referrer-policy") || "").trim();
  assert.ok(referrerPolicy, `${routeLabel} must include Referrer-Policy header.`);
}

function parseCspDirectives(cspHeader) {
  const directives = new Map();
  const rawCsp = String(cspHeader || "");
  for (const chunk of rawCsp.split(";")) {
    const normalized = chunk.trim();
    if (!normalized) {
      continue;
    }
    const [directiveName, ...sources] = normalized.split(/\s+/);
    if (!directiveName) {
      continue;
    }
    directives.set(directiveName.toLowerCase(), sources);
  }
  return directives;
}

function assertCspHeader(response, routeLabel) {
  const csp = String(response.headers.get("content-security-policy") || "").trim();
  assert.ok(csp, `${routeLabel} must include Content-Security-Policy header.`);
  return csp;
}

function assertNoInlineStylesInCsp(cspHeader, routeLabel) {
  const directives = parseCspDirectives(cspHeader);
  const styleSources = directives.get("style-src") || [];
  assert.ok(styleSources.length > 0, `${routeLabel} must include style-src directive.`);
  assert.equal(
    styleSources.includes("'unsafe-inline'"),
    false,
    `${routeLabel} style-src must not include unsafe-inline.`,
  );
}

function assertStrictConnectSrcInCsp(cspHeader, routeLabel) {
  const directives = parseCspDirectives(cspHeader);
  const connectSources = directives.get("connect-src") || [];
  assert.ok(connectSources.length > 0, `${routeLabel} must include connect-src directive.`);
  assert.ok(connectSources.includes("'self'"), `${routeLabel} connect-src must include 'self'.`);

  const expectedConnectSources = new Set([
    "'self'",
    "https://telegram.org",
    "https://web.telegram.org",
    "https://api.telegram.org",
    "wss://web.telegram.org",
  ]);
  for (const source of expectedConnectSources) {
    assert.ok(connectSources.includes(source), `${routeLabel} connect-src must allow ${source}.`);
  }

  assert.equal(connectSources.includes("https:"), false, `${routeLabel} connect-src must not include https: wildcard.`);
  assert.equal(connectSources.includes("wss:"), false, `${routeLabel} connect-src must not include wss: wildcard.`);
  assert.equal(connectSources.includes("http:"), false, `${routeLabel} connect-src must not include http: wildcard.`);
  assert.equal(
    connectSources.some((source) => source.includes("*")),
    false,
    `${routeLabel} connect-src must not include wildcard hosts.`,
  );
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

async function fetchLoginFormCsrf(baseUrl) {
  const response = await fetch(`${baseUrl}/login`, {
    method: "GET",
    redirect: "manual",
    headers: {
      Accept: "text/html",
    },
  });
  assert.equal(response.status, 200, "Expected GET /login to return 200.");
  const cookies = buildCookieJar(response);
  const csrfToken = cookies.get(WEB_AUTH_LOGIN_CSRF_COOKIE_NAME) || "";
  assert.ok(csrfToken, `Expected ${WEB_AUTH_LOGIN_CSRF_COOKIE_NAME} cookie from GET /login`);
  return {
    csrfToken,
    cookieHeader: buildCookieHeader(cookies),
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

      const crossSiteLogoutResponse = await fetch(`${baseUrl}/api/auth/logout`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
          "x-csrf-token": ownerLogin.csrfToken,
          Origin: "https://evil.example",
          Referer: "https://evil.example/attack",
        },
      });
      const crossSiteLogoutBody = await crossSiteLogoutResponse.json();
      assert.equal(crossSiteLogoutResponse.status, 403);
      assert.ok(
        crossSiteLogoutBody?.code === "csrf_origin_invalid" || crossSiteLogoutBody?.code === "csrf_referer_invalid",
        `Unexpected CSRF origin code: ${String(crossSiteLogoutBody?.code || "")}`,
      );
    });

    await t.test("POST /login requires CSRF token and blocks cross-site origin", async () => {
      const loginForm = await fetchLoginFormCsrf(baseUrl);

      const missingTokenResponse = await fetch(`${baseUrl}/login`, {
        method: "POST",
        redirect: "manual",
        headers: {
          Accept: "text/html",
          "Content-Type": "application/x-www-form-urlencoded",
          Cookie: loginForm.cookieHeader,
          Origin: baseUrl,
          Referer: `${baseUrl}/login`,
        },
        body: new URLSearchParams({
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
          next: "/dashboard",
        }).toString(),
      });
      assert.equal(missingTokenResponse.status, 403);

      const crossSiteResponse = await fetch(`${baseUrl}/login`, {
        method: "POST",
        redirect: "manual",
        headers: {
          Accept: "text/html",
          "Content-Type": "application/x-www-form-urlencoded",
          Cookie: loginForm.cookieHeader,
          Origin: "https://evil.example",
          Referer: "https://evil.example/attack",
        },
        body: new URLSearchParams({
          _csrf: loginForm.csrfToken,
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
          next: "/dashboard",
        }).toString(),
      });
      assert.equal(crossSiteResponse.status, 403);
    });

    await t.test("GET /logout is blocked with 405 and allows POST only", async () => {
      const response = await fetch(`${baseUrl}/logout`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });

      assert.equal(response.status, 405);
      assert.equal(String(response.headers.get("allow") || "").toUpperCase(), "POST");
      assertCoreSecurityHeaders(response, "GET /logout");
    });

    await t.test("login page CSP uses nonce-based style-src and no unsafe-inline", async () => {
      const response = await fetch(`${baseUrl}/login`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });

      assert.equal(response.status, 200);
      const csp = assertCspHeader(response, "GET /login");
      assertNoInlineStylesInCsp(csp, "GET /login");
      assertStrictConnectSrcInCsp(csp, "GET /login");
      const nonceMatch = csp.match(/style-src[^;]*'nonce-([^']+)'/i);
      assert.ok(nonceMatch && nonceMatch[1], "Expected nonce in style-src directive.");

      const body = await response.text();
      assert.match(
        body,
        new RegExp(`<style nonce="${escapeRegex(nonceMatch[1])}">`, "i"),
      );
    });

    await t.test("auth cookies default to SameSite=Strict", async () => {
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
      const sessionSetCookie = findSetCookieHeaderByName(response, "cbooster_auth_session");
      const csrfSetCookie = findSetCookieHeaderByName(response, "cbooster_auth_csrf");
      assert.match(sessionSetCookie, /;\s*HttpOnly(?:;|$)/i);
      assert.match(sessionSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.match(csrfSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.doesNotMatch(csrfSetCookie, /;\s*HttpOnly(?:;|$)/i);
    });

    await t.test("login CSRF cookie defaults to SameSite=Strict", async () => {
      const loginFormResponse = await fetch(`${baseUrl}/login`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(loginFormResponse.status, 200);
      const loginCsrfSetCookie = findSetCookieHeaderByName(loginFormResponse, WEB_AUTH_LOGIN_CSRF_COOKIE_NAME);
      assert.match(loginCsrfSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.doesNotMatch(loginCsrfSetCookie, /;\s*HttpOnly(?:;|$)/i);
    });

    await t.test("security headers and CSP are consistent on /login, /, /api/*, 401/404/302 responses", async () => {
      const loginResponse = await fetch(`${baseUrl}/login`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(loginResponse.status, 200);
      assertCoreSecurityHeaders(loginResponse, "GET /login");
      const loginCsp = assertCspHeader(loginResponse, "GET /login");
      assertNoInlineStylesInCsp(loginCsp, "GET /login");
      assertStrictConnectSrcInCsp(loginCsp, "GET /login");

      const rootResponse = await fetch(`${baseUrl}/`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(rootResponse.status, 302);
      assert.equal(String(rootResponse.headers.get("location") || ""), "/login?next=%2F");
      assertCoreSecurityHeaders(rootResponse, "GET /");
      const rootCsp = assertCspHeader(rootResponse, "GET /");
      assertNoInlineStylesInCsp(rootCsp, "GET /");
      assertStrictConnectSrcInCsp(rootCsp, "GET /");

      const unauthorizedApiResponse = await fetch(`${baseUrl}/api/records`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "application/json",
        },
      });
      assert.equal(unauthorizedApiResponse.status, 401);
      assertCoreSecurityHeaders(unauthorizedApiResponse, "GET /api/records (unauth)");
      const unauthorizedApiCsp = assertCspHeader(unauthorizedApiResponse, "GET /api/records (unauth)");
      assertNoInlineStylesInCsp(unauthorizedApiCsp, "GET /api/records (unauth)");
      assertStrictConnectSrcInCsp(unauthorizedApiCsp, "GET /api/records (unauth)");

      const healthApiResponse = await fetch(`${baseUrl}/api/health`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "application/json",
        },
      });
      assert.ok(
        healthApiResponse.status === 200 || healthApiResponse.status === 503,
        `Unexpected /api/health status: ${healthApiResponse.status}`,
      );
      assertCoreSecurityHeaders(healthApiResponse, "GET /api/health");
      const healthApiCsp = assertCspHeader(healthApiResponse, "GET /api/health");
      assertNoInlineStylesInCsp(healthApiCsp, "GET /api/health");
      assertStrictConnectSrcInCsp(healthApiCsp, "GET /api/health");

      const ownerLogin = await loginApi(baseUrl, {
        username: TEST_OWNER_USERNAME,
        password: TEST_OWNER_PASSWORD,
      });

      const notFoundApiResponse = await fetch(`${baseUrl}/api/this-route-does-not-exist`, {
        method: "GET",
        headers: {
          Accept: "application/json",
          Cookie: ownerLogin.cookieHeader,
        },
      });
      assert.equal(notFoundApiResponse.status, 404);
      assertCoreSecurityHeaders(notFoundApiResponse, "GET /api/this-route-does-not-exist");
      const notFoundApiCsp = assertCspHeader(notFoundApiResponse, "GET /api/this-route-does-not-exist");
      assertNoInlineStylesInCsp(notFoundApiCsp, "GET /api/this-route-does-not-exist");
      assertStrictConnectSrcInCsp(notFoundApiCsp, "GET /api/this-route-does-not-exist");

      const redirectResponse = await fetch(`${baseUrl}/dashboard`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(redirectResponse.status, 302);
      assert.equal(String(redirectResponse.headers.get("location") || ""), "/login?next=%2Fdashboard");
      assertCoreSecurityHeaders(redirectResponse, "GET /dashboard redirect");
      const redirectCsp = assertCspHeader(redirectResponse, "GET /dashboard redirect");
      assertNoInlineStylesInCsp(redirectCsp, "GET /dashboard redirect");
      assertStrictConnectSrcInCsp(redirectCsp, "GET /dashboard redirect");
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

test("web auth integration: repeated invalid logins trigger 429 lock with Retry-After", async () => {
  await withServer(
    {
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_MAX_FAILURES: "3",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_MAX_FAILURES: "3",
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_WINDOW_SEC: "300",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_WINDOW_SEC: "300",
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_LOCK_SEC: "120",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_LOCK_SEC: "120",
      WEB_AUTH_LOGIN_FAILURE_DELAY_BASE_MS: "10",
      WEB_AUTH_LOGIN_FAILURE_DELAY_MAX_MS: "30",
    },
    async ({ baseUrl }) => {
      let lastResponse = null;
      let lastBody = null;

      for (let attempt = 1; attempt <= 4; attempt += 1) {
        lastResponse = await fetch(`${baseUrl}/api/auth/login`, {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            "X-Forwarded-For": "198.51.100.10",
          },
          body: JSON.stringify({
            username: TEST_OWNER_USERNAME,
            password: "WrongPassword!123",
          }),
        });
        lastBody = await lastResponse.json();
      }

      assert.ok(lastResponse, "Expected a final login response.");
      assert.equal(lastResponse.status, 429, `Expected lock response, got ${lastResponse.status} with ${JSON.stringify(lastBody)}`);
      assert.equal(lastBody?.code, "login_locked");
      const retryAfterHeader = Number.parseInt(lastResponse.headers.get("retry-after") || "", 10);
      assert.ok(Number.isFinite(retryAfterHeader) && retryAfterHeader > 0, "Expected Retry-After header on lock response.");

      const differentIpResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Forwarded-For": "198.51.100.11",
        },
        body: JSON.stringify({
          username: TEST_OWNER_USERNAME,
          password: "WrongPassword!123",
        }),
      });
      const differentIpBody = await differentIpResponse.json();
      assert.equal(
        differentIpResponse.status,
        401,
        `Expected per-IP lock behavior (401 on different IP), got ${differentIpResponse.status} with ${JSON.stringify(differentIpBody)}`,
      );
    },
  );
});

test("web auth integration: cookie policy keeps Secure/HttpOnly flags when secure mode is forced", async () => {
  await withServer(
    {
      WEB_AUTH_COOKIE_SECURE: "true",
    },
    async ({ baseUrl }) => {
      const loginFormResponse = await fetch(`${baseUrl}/login`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(loginFormResponse.status, 200);
      const loginCsrfSetCookie = findSetCookieHeaderByName(loginFormResponse, WEB_AUTH_LOGIN_CSRF_COOKIE_NAME);
      assert.match(loginCsrfSetCookie, /;\s*Secure(?:;|$)/i);
      assert.match(loginCsrfSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.doesNotMatch(loginCsrfSetCookie, /;\s*HttpOnly(?:;|$)/i);

      const loginResponse = await fetch(`${baseUrl}/api/auth/login`, {
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
      const loginBody = await loginResponse.json();
      assert.equal(loginResponse.status, 200, `Login failed: ${JSON.stringify(loginBody)}`);

      const sessionSetCookie = findSetCookieHeaderByName(loginResponse, "cbooster_auth_session");
      const csrfSetCookie = findSetCookieHeaderByName(loginResponse, "cbooster_auth_csrf");
      assert.match(sessionSetCookie, /;\s*HttpOnly(?:;|$)/i);
      assert.match(sessionSetCookie, /;\s*Secure(?:;|$)/i);
      assert.match(sessionSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.match(csrfSetCookie, /;\s*Secure(?:;|$)/i);
      assert.match(csrfSetCookie, /;\s*SameSite=Strict(?:;|$)/i);
      assert.doesNotMatch(csrfSetCookie, /;\s*HttpOnly(?:;|$)/i);
    },
  );
});

test("web auth integration: SameSite cookie overrides are applied when configured", async () => {
  await withServer(
    {
      WEB_AUTH_SESSION_COOKIE_SAMESITE: "lax",
      WEB_AUTH_CSRF_COOKIE_SAMESITE: "lax",
      WEB_AUTH_LOGIN_CSRF_COOKIE_SAMESITE: "lax",
    },
    async ({ baseUrl }) => {
      const loginFormResponse = await fetch(`${baseUrl}/login`, {
        method: "GET",
        redirect: "manual",
        headers: {
          Accept: "text/html",
        },
      });
      assert.equal(loginFormResponse.status, 200);
      const loginCsrfSetCookie = findSetCookieHeaderByName(loginFormResponse, WEB_AUTH_LOGIN_CSRF_COOKIE_NAME);
      assert.match(loginCsrfSetCookie, /;\s*SameSite=Lax(?:;|$)/i);

      const loginResponse = await fetch(`${baseUrl}/api/auth/login`, {
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
      const loginBody = await loginResponse.json();
      assert.equal(loginResponse.status, 200, `Login failed: ${JSON.stringify(loginBody)}`);

      const sessionSetCookie = findSetCookieHeaderByName(loginResponse, "cbooster_auth_session");
      const csrfSetCookie = findSetCookieHeaderByName(loginResponse, "cbooster_auth_csrf");
      assert.match(sessionSetCookie, /;\s*SameSite=Lax(?:;|$)/i);
      assert.match(csrfSetCookie, /;\s*SameSite=Lax(?:;|$)/i);
    },
  );
});

test("web auth integration: adaptive step-up challenge is required at risk threshold and bound to device fingerprint", async () => {
  await withServer(
    {
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_MAX_FAILURES: "99",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_MAX_FAILURES: "99",
      WEB_AUTH_LOGIN_FAILURE_DEVICE_ACCOUNT_MAX_FAILURES: "99",
      WEB_AUTH_LOGIN_FAILURE_DELAY_BASE_MS: "0",
      WEB_AUTH_LOGIN_FAILURE_DELAY_MAX_MS: "0",
      WEB_AUTH_LOGIN_STEP_UP_ACCOUNT_FAILURES: "2",
      WEB_AUTH_LOGIN_STEP_UP_IP_ACCOUNT_FAILURES: "2",
      WEB_AUTH_LOGIN_STEP_UP_DEVICE_ACCOUNT_FAILURES: "2",
      WEB_AUTH_LOGIN_STEP_UP_ACCOUNT_UNIQUE_IPS: "9",
      WEB_AUTH_LOGIN_STEP_UP_ACCOUNT_UNIQUE_DEVICES: "9",
    },
    async ({ baseUrl }) => {
      for (let attempt = 1; attempt <= 2; attempt += 1) {
        const response = await fetch(`${baseUrl}/api/auth/login`, {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            "User-Agent": "integration-stepup-device-a",
            "X-Forwarded-For": "198.51.100.70",
          },
          body: JSON.stringify({
            username: TEST_OWNER_USERNAME,
            password: "WrongPassword!123",
          }),
        });
        assert.equal(response.status, 401);
      }

      const challengeResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "User-Agent": "integration-stepup-device-a",
          "X-Forwarded-For": "198.51.100.70",
        },
        body: JSON.stringify({
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
        }),
      });
      const challengeBody = await challengeResponse.json();
      assert.equal(challengeResponse.status, 403);
      assert.equal(challengeBody?.code, "login_step_up_required");
      assert.equal(challengeBody?.stepUpRequired, true);
      assert.equal(typeof challengeBody?.stepUpToken, "string");
      assert.ok(challengeBody.stepUpToken.length > 20);

      const mismatchedDeviceResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "User-Agent": "integration-stepup-device-b",
          "X-Forwarded-For": "198.51.100.70",
        },
        body: JSON.stringify({
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
          stepUpToken: challengeBody.stepUpToken,
        }),
      });
      const mismatchedDeviceBody = await mismatchedDeviceResponse.json();
      assert.equal(mismatchedDeviceResponse.status, 403);
      assert.equal(mismatchedDeviceBody?.code, "login_step_up_required");

      const solvedResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "User-Agent": "integration-stepup-device-a",
          "X-Forwarded-For": "198.51.100.70",
          "x-cbooster-login-step-up-token": challengeBody.stepUpToken,
        },
        body: JSON.stringify({
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
        }),
      });
      const solvedBody = await solvedResponse.json();
      assert.equal(solvedResponse.status, 200, JSON.stringify(solvedBody));
      assert.equal(solvedBody?.ok, true);
    },
  );
});

test("web auth integration: attacker IP lock does not force 429 lockout for the same username from another IP", async () => {
  await withServer(
    {
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_MAX_FAILURES: "3",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_MAX_FAILURES: "3",
      WEB_AUTH_LOGIN_FAILURE_DEVICE_ACCOUNT_MAX_FAILURES: "99",
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_WINDOW_SEC: "300",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_WINDOW_SEC: "300",
      WEB_AUTH_LOGIN_FAILURE_ACCOUNT_LOCK_SEC: "120",
      WEB_AUTH_LOGIN_FAILURE_IP_ACCOUNT_LOCK_SEC: "120",
      WEB_AUTH_LOGIN_FAILURE_DELAY_BASE_MS: "0",
      WEB_AUTH_LOGIN_FAILURE_DELAY_MAX_MS: "0",
      WEB_AUTH_LOGIN_STEP_UP_ACCOUNT_FAILURES: "99",
      WEB_AUTH_LOGIN_STEP_UP_IP_ACCOUNT_FAILURES: "99",
      WEB_AUTH_LOGIN_STEP_UP_DEVICE_ACCOUNT_FAILURES: "99",
    },
    async ({ baseUrl }) => {
      for (let attempt = 1; attempt <= 4; attempt += 1) {
        await fetch(`${baseUrl}/api/auth/login`, {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
            "X-Forwarded-For": "198.51.100.90",
            "User-Agent": "attack-device",
          },
          body: JSON.stringify({
            username: TEST_OWNER_USERNAME,
            password: "WrongPassword!123",
          }),
        });
      }

      const victimResponse = await fetch(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Forwarded-For": "198.51.100.91",
          "User-Agent": "victim-device",
        },
        body: JSON.stringify({
          username: TEST_OWNER_USERNAME,
          password: TEST_OWNER_PASSWORD,
        }),
      });
      const victimBody = await victimResponse.json();
      assert.equal(victimResponse.status, 200, `Expected victim login to avoid targeted lockout: ${JSON.stringify(victimBody)}`);
      assert.equal(victimBody?.ok, true);
    },
  );
});
