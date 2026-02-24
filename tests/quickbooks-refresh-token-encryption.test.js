const test = require("node:test");
const assert = require("node:assert/strict");

const { createQuickBooksRepo } = require("../server/domains/quickbooks/quickbooks.repo");
const {
  createQuickBooksRefreshTokenCrypto,
  isEncryptedQuickBooksRefreshToken,
} = require("../server/domains/quickbooks/quickbooks-token-crypto");

function normalizeSql(rawValue) {
  return String(rawValue || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function createQuickBooksRepoHarness(options = {}) {
  const state = {
    rowExists: Boolean(options.rowExists || options.refreshToken),
    refreshToken: String(options.refreshToken || ""),
    refreshTokenExpiresAt: null,
  };

  const calls = [];

  async function query(sql, params = []) {
    const normalizedSql = normalizeSql(sql);
    calls.push({
      sql: normalizedSql,
      params: [...params],
    });

    if (normalizedSql.startsWith("create table") || normalizedSql.startsWith("alter table") || normalizedSql.startsWith("create index")) {
      return { rows: [], rowCount: 0 };
    }

    if (
      normalizedSql.includes("insert into public.quickbooks_auth_state") &&
      normalizedSql.includes("on conflict (id) do nothing")
    ) {
      if (!state.rowExists) {
        state.rowExists = true;
        state.refreshToken = String(params[1] || "");
      }
      return { rows: [], rowCount: 1 };
    }

    if (
      normalizedSql.includes("update public.quickbooks_auth_state") &&
      normalizedSql.includes("coalesce(refresh_token, '') = ''")
    ) {
      if (state.rowExists && String(state.refreshToken || "") === "") {
        state.refreshToken = String(params[1] || "");
        return { rows: [], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    }

    if (
      normalizedSql.includes("insert into public.quickbooks_auth_state") &&
      normalizedSql.includes("refresh_token_expires_at") &&
      normalizedSql.includes("on conflict (id)") &&
      normalizedSql.includes("do update set")
    ) {
      state.rowExists = true;
      state.refreshToken = String(params[1] || "");
      state.refreshTokenExpiresAt = params[2] || null;
      return { rows: [], rowCount: 1 };
    }

    if (normalizedSql.includes("select refresh_token") && normalizedSql.includes("from public.quickbooks_auth_state")) {
      if (!state.rowExists) {
        return { rows: [], rowCount: 0 };
      }
      return {
        rows: [{ refresh_token: state.refreshToken }],
        rowCount: 1,
      };
    }

    throw new Error(`Unexpected SQL in quickbooks test harness: ${normalizedSql}`);
  }

  const repo = createQuickBooksRepo({
    db: {
      query,
      tx: async (executor) => executor({ query }),
    },
    ensureDatabaseReady: async () => {},
    tables: {
      quickBooksTransactionsTable: "public.quickbooks_payments_recent",
      quickBooksTransactionsTableName: "quickbooks_payments_recent",
      quickBooksCustomersCacheTable: "public.quickbooks_customers_cache",
      quickBooksCustomersCacheTableName: "quickbooks_customers_cache",
      quickBooksAuthStateTable: "public.quickbooks_auth_state",
      quickBooksAuthStateRowId: 1,
    },
    constants: {
      quickBooksRefreshTokenEncryptionKey:
        Object.prototype.hasOwnProperty.call(options, "encryptionKey")
          ? options.encryptionKey
          : Buffer.alloc(32, 7).toString("base64"),
      quickBooksRefreshTokenEncryptionKeyId: options.encryptionKeyId || "test-key",
    },
    helpers: {
      sanitizeTextValue(value, maxLength = 4000) {
        return (value ?? "").toString().trim().slice(0, maxLength);
      },
    },
  });

  return {
    repo,
    state,
    calls,
  };
}

test("quickbooks token crypto returns versioned encrypted payload and decrypts back", () => {
  const cryptoLayer = createQuickBooksRefreshTokenCrypto({
    encryptionKey: Buffer.alloc(32, 19).toString("base64"),
    encryptionKeyId: "key-2026-02",
    randomBytes: () => Buffer.alloc(12, 3),
  });

  const encrypted = cryptoLayer.encrypt("refresh-token-value");
  assert.match(encrypted, /^enc:v1:key-2026-02:/);
  assert.equal(encrypted.includes("refresh-token-value"), false);
  assert.equal(cryptoLayer.decrypt(encrypted), "refresh-token-value");
});

test("quickbooks repo persist stores encrypted refresh token and load returns plaintext", async () => {
  const { repo, state } = createQuickBooksRepoHarness();

  await repo.persistQuickBooksRefreshToken("qb-refresh-1", "2026-02-24T12:00:00.000Z");
  assert.match(state.refreshToken, /^enc:v1:test-key:/);
  assert.equal(state.refreshToken.includes("qb-refresh-1"), false);

  const loadedToken = await repo.loadQuickBooksRefreshTokenFromStateStore();
  assert.equal(loadedToken, "qb-refresh-1");
});

test("quickbooks repo reads legacy plaintext and migrates to encrypted format on next persist", async () => {
  const { repo, state } = createQuickBooksRepoHarness({
    refreshToken: "legacy-plaintext-token",
  });

  const loadedLegacyToken = await repo.loadQuickBooksRefreshTokenFromStateStore();
  assert.equal(loadedLegacyToken, "legacy-plaintext-token");
  assert.equal(isEncryptedQuickBooksRefreshToken(state.refreshToken), false);

  await repo.persistQuickBooksRefreshToken("legacy-plaintext-token", "2026-02-24T12:00:00.000Z");
  assert.equal(isEncryptedQuickBooksRefreshToken(state.refreshToken), true);
  assert.equal(state.refreshToken.includes("legacy-plaintext-token"), false);

  const loadedMigratedToken = await repo.loadQuickBooksRefreshTokenFromStateStore();
  assert.equal(loadedMigratedToken, "legacy-plaintext-token");
});

test("quickbooks schema bootstrap stores initial refresh token in encrypted format", async () => {
  const { repo, state } = createQuickBooksRepoHarness({
    rowExists: false,
  });

  const schemaState = await repo.ensureQuickBooksSchema({
    initialRefreshToken: "seed-initial-refresh-token",
  });

  assert.equal(isEncryptedQuickBooksRefreshToken(state.refreshToken), true);
  assert.equal(state.refreshToken.includes("seed-initial-refresh-token"), false);
  assert.equal(schemaState.storedRefreshToken, "seed-initial-refresh-token");
});

test("quickbooks repo refuses to persist refresh token without encryption key", async () => {
  const { repo } = createQuickBooksRepoHarness({
    encryptionKey: "",
  });

  await assert.rejects(
    () => repo.persistQuickBooksRefreshToken("token-without-key"),
    /encryption key is required/i,
  );
});
