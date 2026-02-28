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
const TEST_SALES_VLAD_USERNAME = "vlad.burnis@creditbooster.com";
const TEST_SALES_VLAD_PASSWORD = "SalesVlad!AuthZ123";
const TEST_CS_MANAGER_USERNAME = "cs.manager.authz.security@test.local";
const TEST_CS_MANAGER_PASSWORD = "CsManager!AuthZ123";
const TEST_CS_MIDDLE_MANAGER_USERNAME = "marynau@creditbooster.com";
const TEST_CS_MIDDLE_MANAGER_PASSWORD = "CsMiddle!AuthZ123";
const TEST_CS_HEAD_USERNAME = "nataly.regush@creditbooster.com";
const TEST_CS_HEAD_PASSWORD = "CsHead!AuthZ123";
const TEST_CS_NATASHA_USERNAME = "natasha.grek@creditbooster.com";
const TEST_CS_NATASHA_PASSWORD = "CsNatasha!AuthZ123";
const TEST_CS_KRISTINA_USERNAME = "kristina.troinova@creditbooster.com";
const TEST_CS_KRISTINA_PASSWORD = "CsKristina!AuthZ123";
const TEST_CS_LIUDMYLA_USERNAME = "liudmyla.sidachenko@creditbooster.com";
const TEST_CS_LIUDMYLA_PASSWORD = "CsLiudmyla!AuthZ123";
const TEST_CS_VADIM_USERNAME = "vadim.kozorezov@creditbooster.com";
const TEST_CS_VADIM_PASSWORD = "CsVadim!AuthZ123";
const TEST_ACCOUNTING_MANAGER_USERNAME = "accounting.manager.authz.security@test.local";
const TEST_ACCOUNTING_MANAGER_PASSWORD = "AccountingManager!AuthZ123";
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

async function getClients(baseUrl, auth, query = "") {
  const normalizedQuery = String(query || "");
  const url = `${baseUrl}/api/clients${normalizedQuery ? `?${normalizedQuery}` : ""}`;
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Cookie: auth.cookieHeader,
    },
  });
  const body = await response.json();
  return { response, body };
}

async function getClientsFilters(baseUrl, auth) {
  const response = await fetch(`${baseUrl}/api/clients/filters`, {
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

async function patchRecords(baseUrl, auth, payload, routePath = "/api/records") {
  const response = await fetch(`${baseUrl}${routePath}`, {
    method: "PATCH",
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

test("authz regression: client-service manager scope uses clientManager only and middle-manager team", async () => {
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
          username: TEST_SALES_VLAD_USERNAME,
          password: TEST_SALES_VLAD_PASSWORD,
          departmentId: "sales",
          roleId: "manager",
          displayName: "Vlad Burnis",
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
          teamUsernames: [TEST_CS_MANAGER_USERNAME, TEST_SALES_VLAD_USERNAME, "yurii kis"],
        },
        {
          username: TEST_CS_HEAD_USERNAME,
          password: TEST_CS_HEAD_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Nataly Regush",
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
        {
          id: "authz-cs-rec-kristina",
          clientName: "Visible For Maryna Canonical Team Kristina",
          closedBy: "unrelated.sales.manager",
          clientManager: "Kristina Troinova",
          contractTotals: "$1,410.00",
          payment1: "$141.00",
          payment1Date: "02/12/2026",
        },
        {
          id: "authz-cs-rec-liudmyla",
          clientName: "Visible For Maryna Canonical Team Liudmyla",
          closedBy: "unrelated.sales.manager",
          clientManager: "Liudmyla Sidachenko",
          contractTotals: "$1,420.00",
          payment1: "$142.00",
          payment1Date: "02/12/2026",
        },
        {
          id: "authz-cs-rec-vadim",
          clientName: "Visible For Maryna Canonical Team Vadim",
          closedBy: "unrelated.sales.manager",
          clientManager: "Vadim Kozorezov",
          contractTotals: "$1,430.00",
          payment1: "$143.00",
          payment1Date: "02/12/2026",
        },
        {
          id: "authz-cs-rec-4",
          clientName: "Must Stay Hidden When Only closedBy Matches",
          closedBy: "Ruanna Ordukhanova-Aslanyan",
          clientManager: "Another Manager",
          contractTotals: "$1,500.00",
          payment1: "$150.00",
          payment1Date: "02/13/2026",
        },
        {
          id: "authz-cs-rec-5",
          clientName: "Must Stay Hidden For Cross Department Team Username",
          closedBy: "unrelated.sales.manager",
          clientManager: "Vlad Burnis",
          contractTotals: "$1,600.00",
          payment1: "$160.00",
          payment1Date: "02/14/2026",
        },
        {
          id: "authz-cs-rec-6",
          clientName: "Must Stay Hidden For Unresolved Team String",
          closedBy: "unrelated.sales.manager",
          clientManager: "Yurii Kis",
          contractTotals: "$1,700.00",
          payment1: "$170.00",
          payment1Date: "02/15/2026",
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
    const clientServiceHead = await loginApi(baseUrl, {
      username: TEST_CS_HEAD_USERNAME,
      password: TEST_CS_HEAD_PASSWORD,
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
      [
        "authz-cs-rec-1",
        "authz-cs-rec-2",
        "authz-cs-rec-kristina",
        "authz-cs-rec-liudmyla",
        "authz-cs-rec-vadim",
      ],
    );

    const clientServiceHeadRecords = await getRecords(baseUrl, clientServiceHead);
    assert.equal(clientServiceHeadRecords.response.status, 200);
    assert.deepEqual(
      clientServiceHeadRecords.body.records.map((item) => item.id).sort(),
      [
        "authz-cs-rec-1",
        "authz-cs-rec-2",
        "authz-cs-rec-3",
        "authz-cs-rec-4",
        "authz-cs-rec-5",
        "authz-cs-rec-6",
        "authz-cs-rec-kristina",
        "authz-cs-rec-liudmyla",
        "authz-cs-rec-vadim",
      ],
    );

    const managerClients = await getClients(baseUrl, manager);
    assert.equal(managerClients.response.status, 200);
    assert.deepEqual(
      managerClients.body.records.map((item) => item.id).sort(),
      ["authz-cs-rec-1"],
    );

    const middleManagerClients = await getClients(baseUrl, middleManager);
    assert.equal(middleManagerClients.response.status, 200);
    assert.deepEqual(
      middleManagerClients.body.records.map((item) => item.id).sort(),
      [
        "authz-cs-rec-1",
        "authz-cs-rec-2",
        "authz-cs-rec-kristina",
        "authz-cs-rec-liudmyla",
        "authz-cs-rec-vadim",
      ],
    );

    const clientServiceHeadClients = await getClients(baseUrl, clientServiceHead);
    assert.equal(clientServiceHeadClients.response.status, 200);
    assert.deepEqual(
      clientServiceHeadClients.body.records.map((item) => item.id).sort(),
      [
        "authz-cs-rec-1",
        "authz-cs-rec-2",
        "authz-cs-rec-3",
        "authz-cs-rec-4",
        "authz-cs-rec-5",
        "authz-cs-rec-6",
        "authz-cs-rec-kristina",
        "authz-cs-rec-liudmyla",
        "authz-cs-rec-vadim",
      ],
    );

    const managerClientFilters = await getClientsFilters(baseUrl, manager);
    assert.equal(managerClientFilters.response.status, 200);
    assert.deepEqual(managerClientFilters.body.clientManagerOptions, ["Ruanna Ordukhanova-Aslanyan"]);

    const middleManagerClientFilters = await getClientsFilters(baseUrl, middleManager);
    assert.equal(middleManagerClientFilters.response.status, 200);
    assert.deepEqual(
      middleManagerClientFilters.body.clientManagerOptions,
      [
        "Kristina Troinova",
        "Liudmyla Sidachenko",
        "Maryna Urvantseva",
        "Ruanna Ordukhanova-Aslanyan",
        "Vadim Kozorezov",
      ],
    );

    const clientServiceHeadClientFilters = await getClientsFilters(baseUrl, clientServiceHead);
    assert.equal(clientServiceHeadClientFilters.response.status, 200);
    assert.deepEqual(
      clientServiceHeadClientFilters.body.clientManagerOptions,
      [
        "Another Manager",
        "Kristina Troinova",
        "Liudmyla Sidachenko",
        "Maryna Urvantseva",
        "Ruanna Ordukhanova-Aslanyan",
        "Vadim Kozorezov",
        "Vlad Burnis",
        "Yurii Kis",
      ],
    );
    },
  );
});

test("authz regression: natasha middle-manager scope is consistent across records and clients endpoints", async () => {
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
          username: TEST_SALES_VLAD_USERNAME,
          password: TEST_SALES_VLAD_PASSWORD,
          departmentId: "sales",
          roleId: "manager",
          displayName: "Vlad Burnis",
        },
        {
          username: TEST_CS_NATASHA_USERNAME,
          password: TEST_CS_NATASHA_PASSWORD,
          departmentId: "client_service",
          roleId: "middle_manager",
          displayName: "Natasha Grek",
          teamUsernames: [TEST_CS_KRISTINA_USERNAME, TEST_CS_LIUDMYLA_USERNAME, TEST_SALES_VLAD_USERNAME],
        },
        {
          username: TEST_CS_KRISTINA_USERNAME,
          password: TEST_CS_KRISTINA_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Kristina Troinova",
        },
        {
          username: TEST_CS_LIUDMYLA_USERNAME,
          password: TEST_CS_LIUDMYLA_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Liudmyla Sidachenko",
        },
        {
          username: TEST_CS_VADIM_USERNAME,
          password: TEST_CS_VADIM_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Vadim Kozorezov",
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
            id: "authz-natasha-rec-self",
            clientName: "Visible Natasha Self",
            closedBy: "unrelated.sales.manager",
            clientManager: "Natasha Grek",
            contractTotals: "$1,000.00",
            payment1: "$100.00",
            payment1Date: "02/10/2026",
          },
          {
            id: "authz-natasha-rec-kristina",
            clientName: "Visible Natasha Team Kristina",
            closedBy: "unrelated.sales.manager",
            clientManager: "Kristina Troinova",
            contractTotals: "$1,100.00",
            payment1: "$110.00",
            payment1Date: "02/11/2026",
          },
          {
            id: "authz-natasha-rec-liudmyla",
            clientName: "Visible Natasha Team Liudmyla",
            closedBy: "unrelated.sales.manager",
            clientManager: "Liudmyla Sidachenko",
            contractTotals: "$1,200.00",
            payment1: "$120.00",
            payment1Date: "02/12/2026",
          },
          {
            id: "authz-natasha-rec-hidden-vadim",
            clientName: "Hidden Natasha Non Team",
            closedBy: "unrelated.sales.manager",
            clientManager: "Vadim Kozorezov",
            contractTotals: "$1,300.00",
            payment1: "$130.00",
            payment1Date: "02/13/2026",
          },
          {
            id: "authz-natasha-rec-hidden-vlad",
            clientName: "Hidden Natasha Cross Department",
            closedBy: "unrelated.sales.manager",
            clientManager: "Vlad Burnis",
            contractTotals: "$1,400.00",
            payment1: "$140.00",
            payment1Date: "02/14/2026",
          },
          {
            id: "authz-natasha-rec-hidden-closedby-only",
            clientName: "Hidden Natasha ClosedBy Only",
            closedBy: "Natasha Grek",
            clientManager: "Another Manager",
            contractTotals: "$1,500.00",
            payment1: "$150.00",
            payment1Date: "02/15/2026",
          },
        ],
      });
      assert.equal(seedResult.response.status, 200);
      assert.equal(seedResult.body?.ok, true);

      const natasha = await loginApi(baseUrl, {
        username: TEST_CS_NATASHA_USERNAME,
        password: TEST_CS_NATASHA_PASSWORD,
      });

      const natashaRecords = await getRecords(baseUrl, natasha);
      assert.equal(natashaRecords.response.status, 200);
      assert.deepEqual(
        natashaRecords.body.records.map((item) => item.id).sort(),
        ["authz-natasha-rec-kristina", "authz-natasha-rec-liudmyla", "authz-natasha-rec-self"],
      );

      const natashaClients = await getClients(baseUrl, natasha);
      assert.equal(natashaClients.response.status, 200);
      assert.deepEqual(
        natashaClients.body.records.map((item) => item.id).sort(),
        ["authz-natasha-rec-kristina", "authz-natasha-rec-liudmyla", "authz-natasha-rec-self"],
      );

      const natashaClientFilters = await getClientsFilters(baseUrl, natasha);
      assert.equal(natashaClientFilters.response.status, 200);
      assert.deepEqual(
        natashaClientFilters.body.clientManagerOptions,
        ["Kristina Troinova", "Liudmyla Sidachenko", "Natasha Grek"],
      );
    },
  );
});

test("authz regression: accounting write scope excludes status/delete and client-service head can delete", async () => {
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
          username: TEST_ACCOUNTING_MANAGER_USERNAME,
          password: TEST_ACCOUNTING_MANAGER_PASSWORD,
          departmentId: "accounting",
          roleId: "manager",
          displayName: "Accounting Manager",
        },
        {
          username: TEST_CS_HEAD_USERNAME,
          password: TEST_CS_HEAD_PASSWORD,
          departmentId: "client_service",
          roleId: "manager",
          displayName: "Nataly Regush",
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
            id: "authz-write-scope-rec-1",
            clientName: "Write Scope Record",
            clientManager: "Ruanna Ordukhanova-Aslanyan",
            closedBy: "Accounting Manager",
            active: "Yes",
            contractTotals: "$1200.00",
            payment1: "$100.00",
            payment1Date: "02/10/2026",
            notes: "seed",
          },
        ],
      });
      assert.equal(seedResult.response.status, 200);
      assert.equal(seedResult.body?.ok, true);

      const accounting = await loginApi(baseUrl, {
        username: TEST_ACCOUNTING_MANAGER_USERNAME,
        password: TEST_ACCOUNTING_MANAGER_PASSWORD,
      });

      const accountingRecords = await getRecords(baseUrl, accounting);
      assert.equal(accountingRecords.response.status, 200);
      assert.equal(accountingRecords.body.records.length, 1);
      let accountingUpdatedAt = accountingRecords.body?.updatedAt || null;

      const accountingRecordsNonStatusPatch = await patchRecords(baseUrl, accounting, {
        expectedUpdatedAt: accountingUpdatedAt,
        operations: [
          {
            type: "upsert",
            id: "authz-write-scope-rec-1",
            record: {
              notes: "accounting-updated-via-records",
              contractTotals: "$1300.00",
            },
          },
        ],
      });
      assert.equal(accountingRecordsNonStatusPatch.response.status, 200);
      assert.equal(accountingRecordsNonStatusPatch.body?.ok, true);
      accountingUpdatedAt = accountingRecordsNonStatusPatch.body?.updatedAt || accountingUpdatedAt;

      const accountingRecordsStatusPatch = await patchRecords(baseUrl, accounting, {
        expectedUpdatedAt: accountingUpdatedAt,
        operations: [
          {
            type: "upsert",
            id: "authz-write-scope-rec-1",
            record: {
              active: "No",
            },
          },
        ],
      });
      assert.equal(accountingRecordsStatusPatch.response.status, 403);
      assert.equal(accountingRecordsStatusPatch.body?.code, "records_forbidden_status_edit");

      const accountingRecordsDeletePatch = await patchRecords(baseUrl, accounting, {
        expectedUpdatedAt: accountingUpdatedAt,
        operations: [
          {
            type: "delete",
            id: "authz-write-scope-rec-1",
          },
        ],
      });
      assert.equal(accountingRecordsDeletePatch.response.status, 403);
      assert.equal(accountingRecordsDeletePatch.body?.code, "records_forbidden_delete");

      const accountingClients = await getClients(baseUrl, accounting);
      assert.equal(accountingClients.response.status, 200);
      let accountingClientsUpdatedAt = accountingClients.body?.updatedAt || null;

      const accountingClientsNonStatusPatch = await patchRecords(
        baseUrl,
        accounting,
        {
          expectedUpdatedAt: accountingClientsUpdatedAt,
          operations: [
            {
              type: "upsert",
              id: "authz-write-scope-rec-1",
              record: {
                notes: "accounting-updated-via-clients",
              },
            },
          ],
        },
        "/api/clients",
      );
      assert.equal(accountingClientsNonStatusPatch.response.status, 200);
      assert.equal(accountingClientsNonStatusPatch.body?.ok, true);
      accountingClientsUpdatedAt = accountingClientsNonStatusPatch.body?.updatedAt || accountingClientsUpdatedAt;

      const accountingClientsStatusPatch = await patchRecords(
        baseUrl,
        accounting,
        {
          expectedUpdatedAt: accountingClientsUpdatedAt,
          operations: [
            {
              type: "upsert",
              id: "authz-write-scope-rec-1",
              record: {
                writtenOff: "Yes",
              },
            },
          ],
        },
        "/api/clients",
      );
      assert.equal(accountingClientsStatusPatch.response.status, 403);
      assert.equal(accountingClientsStatusPatch.body?.code, "records_forbidden_status_edit");

      const accountingClientsDeletePatch = await patchRecords(
        baseUrl,
        accounting,
        {
          expectedUpdatedAt: accountingClientsUpdatedAt,
          operations: [
            {
              type: "delete",
              id: "authz-write-scope-rec-1",
            },
          ],
        },
        "/api/clients",
      );
      assert.equal(accountingClientsDeletePatch.response.status, 403);
      assert.equal(accountingClientsDeletePatch.body?.code, "records_forbidden_delete");

      const clientServiceHead = await loginApi(baseUrl, {
        username: TEST_CS_HEAD_USERNAME,
        password: TEST_CS_HEAD_PASSWORD,
      });

      const headClientsBeforePatch = await getClients(baseUrl, clientServiceHead);
      assert.equal(headClientsBeforePatch.response.status, 200);
      assert.equal(headClientsBeforePatch.body.records.length, 1);
      let headUpdatedAt = headClientsBeforePatch.body?.updatedAt || null;

      const headStatusPatch = await patchRecords(
        baseUrl,
        clientServiceHead,
        {
          expectedUpdatedAt: headUpdatedAt,
          operations: [
            {
              type: "upsert",
              id: "authz-write-scope-rec-1",
              record: {
                active: "No",
                writtenOff: "Yes",
              },
            },
          ],
        },
        "/api/clients",
      );
      assert.equal(headStatusPatch.response.status, 200);
      assert.equal(headStatusPatch.body?.ok, true);
      headUpdatedAt = headStatusPatch.body?.updatedAt || headUpdatedAt;

      const headDeletePatch = await patchRecords(
        baseUrl,
        clientServiceHead,
        {
          expectedUpdatedAt: headUpdatedAt,
          operations: [
            {
              type: "delete",
              id: "authz-write-scope-rec-1",
            },
          ],
        },
        "/api/clients",
      );
      assert.equal(headDeletePatch.response.status, 200);
      assert.equal(headDeletePatch.body?.ok, true);

      const ownerAfterDelete = await getRecords(baseUrl, owner);
      assert.equal(ownerAfterDelete.response.status, 200);
      assert.deepEqual(ownerAfterDelete.body.records, []);
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
