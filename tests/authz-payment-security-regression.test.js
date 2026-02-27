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

const TEST_OWNER_USERNAME = "owner.authz.security@test.local";
const TEST_OWNER_PASSWORD = "Owner!AuthZ123";
const TEST_ADMIN_USERNAME = "admin.authz.security@test.local";
const TEST_ADMIN_PASSWORD = "Admin!AuthZ123";
const TEST_SALES_USERNAME = "sales.authz.security@test.local";
const TEST_SALES_PASSWORD = "Sales!AuthZ123";
const TEST_CS_MANAGER_USERNAME = "cs.manager.authz.security@test.local";
const TEST_CS_MANAGER_PASSWORD = "CsManager!AuthZ123";
const TEST_CS_MIDDLE_MANAGER_USERNAME = "marynau@creditbooster.com";
const TEST_CS_MIDDLE_MANAGER_PASSWORD = "CsMiddle!AuthZ123";
const TEST_WEB_AUTH_SESSION_SECRET = "authz-security-test-web-auth-session-secret-abcdefghijklmnopqrstuvwxyz";
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
    RECORDS_PATCH: "true",
    WEB_AUTH_USERS_JSON: JSON.stringify([
      {
        username: TEST_ADMIN_USERNAME,
        password: TEST_ADMIN_PASSWORD,
        departmentId: "accounting",
        roleId: "admin",
      },
      {
        username: TEST_SALES_USERNAME,
        password: TEST_SALES_PASSWORD,
        departmentId: "sales",
        roleId: "manager",
      },
    ]),
    GHL_API_KEY: "",
    GHL_LOCATION_ID: "",
    QUICKBOOKS_CLIENT_ID: "",
    QUICKBOOKS_CLIENT_SECRET: "",
    QUICKBOOKS_REFRESH_TOKEN: "",
    QUICKBOOKS_REALM_ID: "",
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
  const hasOverrides = typeof envOverridesOrCallback !== "function";
  const envOverrides = hasOverrides ? envOverridesOrCallback || {} : {};
  const callback = hasOverrides ? maybeCallback : envOverridesOrCallback;
  if (typeof callback !== "function") {
    throw new TypeError("withServer requires callback function.");
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
  const csrfToken = String(cookies.get(WEB_AUTH_CSRF_COOKIE_NAME) || "");
  assert.ok(csrfToken.length > 0);
  return {
    cookieHeader: buildCookieHeader(cookies),
    csrfToken,
  };
}

async function getRecords(baseUrl, auth) {
  const response = await fetch(`${baseUrl}/api/records`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Cookie: auth.cookieHeader,
    },
  });
  const body = await response.json();
  return { response, body };
}

async function putRecords(baseUrl, auth, payload) {
  const response = await fetch(`${baseUrl}/api/records`, {
    method: "PUT",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      Cookie: auth.cookieHeader,
      "x-csrf-token": auth.csrfToken,
    },
    body: JSON.stringify(payload),
  });
  const body = await response.json();
  return { response, body };
}

function parseMoneyAmount(rawValue) {
  const normalized = String(rawValue || "")
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");
  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return NaN;
  }
  return Number(normalized);
}

test("authz regression: user/admin entity boundaries and BOLA visibility", async () => {
  await withServer(async ({ baseUrl }) => {
    const owner = await loginApi(baseUrl, {
      username: TEST_OWNER_USERNAME,
      password: TEST_OWNER_PASSWORD,
    });

    const initial = await getRecords(baseUrl, owner);
    assert.equal(initial.response.status, 200);

    const seedResult = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "authz-rec-1",
          clientName: "Visible For Sales",
          closedBy: "sales.authz.security",
          contractTotals: "$1,000.00",
          payment1: "$150.00",
          payment1Date: "02/10/2026",
          notes: "seed",
        },
        {
          id: "authz-rec-2",
          clientName: "Hidden For Sales",
          closedBy: "another.manager",
          contractTotals: "$1,300.00",
          payment1: "$100.00",
          payment1Date: "02/11/2026",
          notes: "seed",
        },
      ],
    });
    assert.equal(seedResult.response.status, 200);
    assert.equal(seedResult.body?.ok, true);

    const sales = await loginApi(baseUrl, {
      username: TEST_SALES_USERNAME,
      password: TEST_SALES_PASSWORD,
    });

    const salesRecords = await getRecords(baseUrl, sales);
    assert.equal(salesRecords.response.status, 200);
    assert.equal(Array.isArray(salesRecords.body?.records), true);
    assert.equal(salesRecords.body.records.length, 1);
    assert.equal(salesRecords.body.records[0]?.id, "authz-rec-1");

    const salesPutAttempt = await putRecords(baseUrl, sales, {
      expectedUpdatedAt: salesRecords.body?.updatedAt || null,
      records: salesRecords.body?.records || [],
    });
    assert.equal(salesPutAttempt.response.status, 403);

    const salesUsersList = await fetch(`${baseUrl}/api/auth/users`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Cookie: sales.cookieHeader,
      },
    });
    assert.equal(salesUsersList.status, 403);

    const salesQuickBooks = await fetch(`${baseUrl}/api/quickbooks/payments/outgoing`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Cookie: sales.cookieHeader,
      },
    });
    assert.equal(salesQuickBooks.status, 403);

    const admin = await loginApi(baseUrl, {
      username: TEST_ADMIN_USERNAME,
      password: TEST_ADMIN_PASSWORD,
    });
    const adminUsersList = await fetch(`${baseUrl}/api/auth/users`, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Cookie: admin.cookieHeader,
      },
    });
    assert.equal(adminUsersList.status, 200);
    const adminUsersBody = await adminUsersList.json();
    assert.equal(Array.isArray(adminUsersBody?.items), true);
    assert.ok(adminUsersBody.items.some((item) => item?.username === TEST_SALES_USERNAME));

    const adminRecords = await getRecords(baseUrl, admin);
    assert.equal(adminRecords.response.status, 200);
    assert.equal(adminRecords.body.records.length, 2);
  });
});

test("authz regression: client-service manager scope includes clientManager and middle-manager team", async () => {
  await withServer(
    {
      WEB_AUTH_USERS_JSON: JSON.stringify([
        {
          username: TEST_ADMIN_USERNAME,
          password: TEST_ADMIN_PASSWORD,
          departmentId: "accounting",
          roleId: "admin",
        },
        {
          username: TEST_SALES_USERNAME,
          password: TEST_SALES_PASSWORD,
          departmentId: "sales",
          roleId: "manager",
        },
        {
          username: TEST_CS_MANAGER_USERNAME,
          password: TEST_CS_MANAGER_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Ruanna Ordukhanova-Aslanyan",
        },
        {
          username: TEST_CS_MIDDLE_MANAGER_USERNAME,
          password: TEST_CS_MIDDLE_MANAGER_PASSWORD,
          departmentId: "client_service",
          roleId: "middle_manager",
          displayName: "Marina Urvanceva",
          teamUsernames: [TEST_CS_MANAGER_USERNAME],
        },
      ]),
    },
    async ({ baseUrl }) => {
    const owner = await loginApi(baseUrl, {
      username: TEST_OWNER_USERNAME,
      password: TEST_OWNER_PASSWORD,
    });

    const initial = await getRecords(baseUrl, owner);
    assert.equal(initial.response.status, 200);

    const seedResult = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "authz-cs-rec-1",
          clientName: "Visible For Client-Service Manager",
          closedBy: "unrelated.sales.manager",
          clientManager: "Ruanna Ordukhanova-Aslanyan",
          contractTotals: "$1,000.00",
          payment1: "$100.00",
          payment1Date: "02/10/2026",
        },
        {
          id: "authz-cs-rec-2",
          clientName: "Visible For Middle Manager Self",
          closedBy: "unrelated.sales.manager",
          clientManager: "Maryna Urvantseva",
          contractTotals: "$1,200.00",
          payment1: "$120.00",
          payment1Date: "02/11/2026",
        },
        {
          id: "authz-cs-rec-3",
          clientName: "Hidden For Client-Service Scope",
          closedBy: "unrelated.sales.manager",
          clientManager: "Another Manager",
          contractTotals: "$1,400.00",
          payment1: "$140.00",
          payment1Date: "02/12/2026",
        },
      ],
    });
    assert.equal(seedResult.response.status, 200);
    assert.equal(seedResult.body?.ok, true);

    const manager = await loginApi(baseUrl, {
      username: TEST_CS_MANAGER_USERNAME,
      password: TEST_CS_MANAGER_PASSWORD,
    });
    const middleManager = await loginApi(baseUrl, {
      username: TEST_CS_MIDDLE_MANAGER_USERNAME,
      password: TEST_CS_MIDDLE_MANAGER_PASSWORD,
    });

    const managerRecords = await getRecords(baseUrl, manager);
    assert.equal(managerRecords.response.status, 200);
    assert.deepEqual(
      managerRecords.body.records.map((item) => item.id).sort(),
      ["authz-cs-rec-1"],
    );

    const middleManagerRecords = await getRecords(baseUrl, middleManager);
    assert.equal(middleManagerRecords.response.status, 200);
    assert.deepEqual(
      middleManagerRecords.body.records.map((item) => item.id).sort(),
      ["authz-cs-rec-1", "authz-cs-rec-2"],
    );
    },
  );
});

test("payment security regression: amount/status invariants and race protections", async () => {
  await withServer(async ({ baseUrl }) => {
    const owner = await loginApi(baseUrl, {
      username: TEST_OWNER_USERNAME,
      password: TEST_OWNER_PASSWORD,
    });

    const initial = await getRecords(baseUrl, owner);
    assert.equal(initial.response.status, 200);

    const invalidAmountPut = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "pay-sec-1",
          clientName: "Invalid Amount",
          closedBy: "sales.authz.security",
          contractTotals: "not-a-money-value",
          payment1: "$100.00",
          payment1Date: "02/01/2026",
        },
      ],
    });
    assert.equal(invalidAmountPut.response.status, 400);
    assert.equal(invalidAmountPut.body?.code, "records_payload_invalid_amount");

    const negativeAmountPut = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "pay-sec-2",
          clientName: "Negative Payment",
          closedBy: "sales.authz.security",
          contractTotals: "$1000.00",
          payment1: "-$100.00",
          payment1Date: "02/01/2026",
        },
      ],
    });
    assert.equal(negativeAmountPut.response.status, 400);
    assert.equal(negativeAmountPut.body?.code, "records_payload_negative_amount");

    const duplicateIdPut = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "dup-pay-id",
          clientName: "Duplicate 1",
          closedBy: "sales.authz.security",
          contractTotals: "$1000.00",
          payment1: "$100.00",
          payment1Date: "02/01/2026",
        },
        {
          id: "dup-pay-id",
          clientName: "Duplicate 2",
          closedBy: "sales.authz.security",
          contractTotals: "$1200.00",
          payment1: "$200.00",
          payment1Date: "02/01/2026",
        },
      ],
    });
    assert.equal(duplicateIdPut.response.status, 400);
    assert.equal(duplicateIdPut.body?.code, "records_payload_duplicate_id");

    const validPut = await putRecords(baseUrl, owner, {
      expectedUpdatedAt: initial.body?.updatedAt || null,
      records: [
        {
          id: "pay-sec-3",
          clientName: "Tampered Totals",
          closedBy: "sales.authz.security",
          contractTotals: "$1000.00",
          payment1: "$250.00",
          payment1Date: "02/04/2026",
          payment2: "$250.00",
          payment2Date: "02/05/2026",
          totalPayments: "$1.00",
          futurePayments: "$9999.00",
          afterResult: "Yes",
          writtenOff: "Yes",
          dateWhenWrittenOff: "",
          notes: "tamper check",
        },
      ],
    });
    assert.equal(validPut.response.status, 200);
    assert.equal(validPut.body?.ok, true);

    const afterValidGet = await getRecords(baseUrl, owner);
    assert.equal(afterValidGet.response.status, 200);
    assert.equal(afterValidGet.body.records.length, 1);
    const normalizedRecord = afterValidGet.body.records[0];
    assert.equal(parseMoneyAmount(normalizedRecord.totalPayments), 500);
    assert.equal(parseMoneyAmount(normalizedRecord.futurePayments), 500);
    assert.equal(normalizedRecord.afterResult, "");
    assert.ok(String(normalizedRecord.dateWhenWrittenOff || "").trim().length > 0);

    const raceExpectedUpdatedAt = afterValidGet.body?.updatedAt || null;
    const payloadA = {
      expectedUpdatedAt: raceExpectedUpdatedAt,
      records: [
        {
          ...normalizedRecord,
          payment3: "$100.00",
          payment3Date: "02/06/2026",
          notes: "race-a",
        },
      ],
    };
    const payloadB = {
      expectedUpdatedAt: raceExpectedUpdatedAt,
      records: [
        {
          ...normalizedRecord,
          payment3: "$200.00",
          payment3Date: "02/07/2026",
          notes: "race-b",
        },
      ],
    };

    const [raceA, raceB] = await Promise.all([
      putRecords(baseUrl, owner, payloadA),
      putRecords(baseUrl, owner, payloadB),
    ]);
    const statuses = [raceA.response.status, raceB.response.status].sort((left, right) => left - right);
    assert.deepEqual(statuses, [200, 409]);

    const raceAfterGet = await getRecords(baseUrl, owner);
    assert.equal(raceAfterGet.response.status, 200);
    const raceRecord = raceAfterGet.body.records[0];
    const racePayment3Amount = parseMoneyAmount(raceRecord.payment3);
    assert.ok(racePayment3Amount === 100 || racePayment3Amount === 200);
  });
});
