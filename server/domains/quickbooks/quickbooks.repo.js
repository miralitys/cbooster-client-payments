"use strict";

const { createQuickBooksRefreshTokenCrypto } = require("./quickbooks-token-crypto");

function createQuickBooksRepo(dependencies = {}) {
  const { db, ensureDatabaseReady, tables, constants, helpers } = dependencies;

  const sanitizeTextValue =
    typeof helpers?.sanitizeTextValue === "function"
      ? helpers.sanitizeTextValue
      : (value, maxLength = 4000) => (value ?? "").toString().trim().slice(0, maxLength);

  const query =
    typeof db?.query === "function"
      ? db.query
      : async () => {
          const error = new Error("Database is not configured. Set DATABASE_URL.");
          error.code = "db_not_configured";
          throw error;
        };
  const runInTransaction =
    typeof db?.tx === "function"
      ? db.tx
      : async () => {
          const error = new Error("Database transaction helper is not configured.");
          error.code = "db_not_configured";
          throw error;
        };
  const ensureReady = typeof ensureDatabaseReady === "function" ? ensureDatabaseReady : async () => {};

  const QUICKBOOKS_TRANSACTIONS_TABLE = tables?.quickBooksTransactionsTable;
  const QUICKBOOKS_CUSTOMERS_CACHE_TABLE = tables?.quickBooksCustomersCacheTable;
  const QUICKBOOKS_AUTH_STATE_TABLE = tables?.quickBooksAuthStateTable;
  const QUICKBOOKS_AUTH_STATE_ROW_ID = Number.parseInt(String(tables?.quickBooksAuthStateRowId || "1"), 10) || 1;

  const QUICKBOOKS_TRANSACTIONS_TABLE_NAME = sanitizeTextValue(tables?.quickBooksTransactionsTableName, 120);
  const QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME = sanitizeTextValue(tables?.quickBooksCustomersCacheTableName, 120);

  const QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE = Math.max(1, Number(constants?.quickBooksCacheUpsertBatchSize) || 100);
  const QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT = Math.max(0.0001, Number(constants?.quickBooksMinVisibleAbsAmount) || 0.0001);
  const QUICKBOOKS_ZERO_RECONCILE_MAX_ROWS = Math.max(1, Number(constants?.quickBooksZeroReconcileMaxRows) || 500);
  const quickBooksRefreshTokenCrypto = createQuickBooksRefreshTokenCrypto({
    encryptionKey: constants?.quickBooksRefreshTokenEncryptionKey,
    encryptionKeyId: constants?.quickBooksRefreshTokenEncryptionKeyId,
  });

  function prepareRefreshTokenForStorage(rawTokenValue) {
    const normalizedToken = sanitizeTextValue(rawTokenValue, 6000);
    if (!normalizedToken) {
      return "";
    }
    if (!quickBooksRefreshTokenCrypto.isConfigured()) {
      const error = new Error(
        "QuickBooks refresh token encryption key is required to persist token. Set QUICKBOOKS_REFRESH_TOKEN_ENCRYPTION_KEY.",
      );
      error.code = "quickbooks_refresh_token_encryption_key_missing";
      throw error;
    }
    return quickBooksRefreshTokenCrypto.encrypt(normalizedToken);
  }

  function restoreRefreshTokenFromStorage(rawStoredValue) {
    const normalizedStoredValue = sanitizeTextValue(rawStoredValue, 12000);
    if (!normalizedStoredValue) {
      return "";
    }
    return sanitizeTextValue(quickBooksRefreshTokenCrypto.decrypt(normalizedStoredValue), 6000);
  }

  async function ensureQuickBooksSchema(options = {}) {
    const initialRefreshToken = sanitizeTextValue(options.initialRefreshToken, 6000);
    const initialRefreshTokenForStorage = prepareRefreshTokenForStorage(initialRefreshToken);

    await query(`
      CREATE TABLE IF NOT EXISTS ${QUICKBOOKS_TRANSACTIONS_TABLE} (
        transaction_type TEXT NOT NULL,
        transaction_id TEXT NOT NULL,
        customer_id TEXT NOT NULL DEFAULT '',
        client_name TEXT NOT NULL,
        client_phone TEXT NOT NULL DEFAULT '',
        client_email TEXT NOT NULL DEFAULT '',
        payment_amount NUMERIC(18, 2) NOT NULL DEFAULT 0,
        payment_date DATE NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (transaction_type, transaction_id)
      )
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS customer_id TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS client_phone TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS client_email TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_record_id TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_payment_field TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_payment_date_field TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_confirmed BOOLEAN NOT NULL DEFAULT FALSE
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_confirmed_at TIMESTAMPTZ
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_confirmed_by TEXT NOT NULL DEFAULT ''
    `);

    await query(`
      ALTER TABLE ${QUICKBOOKS_TRANSACTIONS_TABLE}
      ADD COLUMN IF NOT EXISTS matched_at TIMESTAMPTZ
    `);

    await query(`
      CREATE INDEX IF NOT EXISTS ${QUICKBOOKS_TRANSACTIONS_TABLE_NAME}_payment_date_idx
      ON ${QUICKBOOKS_TRANSACTIONS_TABLE} (payment_date DESC)
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE} (
        customer_id TEXT PRIMARY KEY,
        client_name TEXT NOT NULL DEFAULT '',
        client_phone TEXT NOT NULL DEFAULT '',
        client_email TEXT NOT NULL DEFAULT '',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await query(`
      CREATE INDEX IF NOT EXISTS ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE_NAME}_updated_at_idx
      ON ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE} (updated_at DESC)
    `);

    await query(`
      CREATE TABLE IF NOT EXISTS ${QUICKBOOKS_AUTH_STATE_TABLE} (
        id BIGINT PRIMARY KEY,
        refresh_token TEXT NOT NULL DEFAULT '',
        refresh_token_expires_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    await query(
      `
        INSERT INTO ${QUICKBOOKS_AUTH_STATE_TABLE} (
          id,
          refresh_token
        )
        VALUES ($1, $2)
        ON CONFLICT (id) DO NOTHING
      `,
      [QUICKBOOKS_AUTH_STATE_ROW_ID, initialRefreshTokenForStorage],
    );

    if (initialRefreshToken) {
      await query(
        `
          UPDATE ${QUICKBOOKS_AUTH_STATE_TABLE}
          SET refresh_token = $2,
              updated_at = NOW()
          WHERE id = $1
            AND COALESCE(refresh_token, '') = ''
        `,
        [QUICKBOOKS_AUTH_STATE_ROW_ID, initialRefreshTokenForStorage],
      );
    }

    const quickBooksAuthStateResult = await query(
      `
        SELECT refresh_token
        FROM ${QUICKBOOKS_AUTH_STATE_TABLE}
        WHERE id = $1
        LIMIT 1
      `,
      [QUICKBOOKS_AUTH_STATE_ROW_ID],
    );

    return {
      storedRefreshToken: restoreRefreshTokenFromStorage(quickBooksAuthStateResult.rows[0]?.refresh_token),
    };
  }

  async function persistQuickBooksRefreshToken(tokenValue, refreshTokenExpiresAtIso = "") {
    const normalizedToken = sanitizeTextValue(tokenValue, 6000);
    if (!normalizedToken) {
      return;
    }
    const refreshTokenForStorage = prepareRefreshTokenForStorage(normalizedToken);

    const normalizedRefreshTokenExpiresAt = sanitizeTextValue(refreshTokenExpiresAtIso, 80) || null;
    await ensureReady();
    await query(
      `
        INSERT INTO ${QUICKBOOKS_AUTH_STATE_TABLE} (
          id,
          refresh_token,
          refresh_token_expires_at,
          updated_at
        )
        VALUES ($1, $2, $3::timestamptz, NOW())
        ON CONFLICT (id)
        DO UPDATE SET
          refresh_token = EXCLUDED.refresh_token,
          refresh_token_expires_at = EXCLUDED.refresh_token_expires_at,
          updated_at = NOW()
      `,
      [QUICKBOOKS_AUTH_STATE_ROW_ID, refreshTokenForStorage, normalizedRefreshTokenExpiresAt],
    );
  }

  async function loadQuickBooksRefreshTokenFromStateStore() {
    await ensureReady();
    const quickBooksAuthStateResult = await query(
      `
        SELECT refresh_token
        FROM ${QUICKBOOKS_AUTH_STATE_TABLE}
        WHERE id = $1
        LIMIT 1
      `,
      [QUICKBOOKS_AUTH_STATE_ROW_ID],
    );
    return restoreRefreshTokenFromStorage(quickBooksAuthStateResult.rows[0]?.refresh_token);
  }

  async function listCachedQuickBooksCustomerContacts(customerIds) {
    const normalizedIds = [...new Set(Array.isArray(customerIds) ? customerIds.map((value) => sanitizeTextValue(value, 120)) : [])].filter(
      Boolean,
    );
    if (!normalizedIds.length) {
      return [];
    }

    await ensureReady();
    const result = await query(
      `
        SELECT customer_id, client_name, client_phone, client_email
        FROM ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE}
        WHERE customer_id = ANY($1::text[])
      `,
      [normalizedIds],
    );
    return Array.isArray(result.rows) ? result.rows : [];
  }

  async function upsertQuickBooksCustomerContacts(items) {
    const normalizedItems = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!normalizedItems.length) {
      return {
        writtenCount: 0,
      };
    }

    await ensureReady();
    return runInTransaction(async ({ query: txQuery }) => {
      let writtenCount = 0;

      for (let offset = 0; offset < normalizedItems.length; offset += QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE) {
        const batch = normalizedItems.slice(offset, offset + QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE);
        const placeholders = [];
        const values = [];

        for (let index = 0; index < batch.length; index += 1) {
          const item = batch[index];
          const base = index * 4;
          placeholders.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`);
          values.push(item.customerId, item.clientName, item.clientPhone, item.clientEmail);
        }

        const result = await txQuery(
          `
            INSERT INTO ${QUICKBOOKS_CUSTOMERS_CACHE_TABLE}
              (customer_id, client_name, client_phone, client_email)
            VALUES ${placeholders.join(", ")}
            ON CONFLICT (customer_id)
            DO UPDATE
            SET
              client_name = EXCLUDED.client_name,
              client_phone = EXCLUDED.client_phone,
              client_email = EXCLUDED.client_email,
              updated_at = NOW()
          `,
          values,
        );

        writtenCount += Number.isFinite(result?.rowCount) ? result.rowCount : 0;
      }

      return {
        writtenCount,
      };
    });
  }

  async function listCachedQuickBooksTransactionsInRange(fromDate, toDate) {
    await ensureReady();
    const result = await query(
      `
        SELECT
          transaction_type,
          transaction_id,
          customer_id,
          client_name,
          client_phone,
          client_email,
          payment_amount,
          payment_date::text AS payment_date,
          matched_record_id,
          matched_payment_field,
          matched_payment_date_field,
          matched_confirmed,
          matched_confirmed_at,
          matched_confirmed_by
        FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
        WHERE payment_date >= $1::date
          AND payment_date <= $2::date
        ORDER BY payment_date DESC, updated_at DESC, transaction_type ASC, transaction_id ASC
      `,
      [fromDate, toDate],
    );

    return Array.isArray(result.rows) ? result.rows : [];
  }

  async function getLatestCachedQuickBooksPaymentDate(fromDate, toDate) {
    await ensureReady();
    const result = await query(
      `
        SELECT MAX(payment_date)::text AS max_date
        FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
        WHERE payment_date >= $1::date
          AND payment_date <= $2::date
      `,
      [fromDate, toDate],
    );

    return sanitizeTextValue(result.rows[0]?.max_date, 20);
  }

  async function listCachedQuickBooksZeroPaymentsInRange(fromDate, toDate) {
    await ensureReady();
    const result = await query(
      `
        SELECT
          transaction_id,
          customer_id,
          client_name,
          client_phone,
          client_email,
          payment_date::text AS payment_date
        FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
        WHERE transaction_type = 'payment'
          AND payment_date >= $1::date
          AND payment_date <= $2::date
          AND ABS(payment_amount) < $3
        ORDER BY payment_date DESC, updated_at ASC, transaction_id ASC
        LIMIT $4
      `,
      [fromDate, toDate, QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT, QUICKBOOKS_ZERO_RECONCILE_MAX_ROWS],
    );

    return Array.isArray(result.rows) ? result.rows : [];
  }

  async function upsertQuickBooksTransactions(items) {
    const normalizedItems = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!normalizedItems.length) {
      return {
        insertedCount: 0,
        writtenCount: 0,
      };
    }

    await ensureReady();
    return runInTransaction(async ({ query: txQuery }) => {
      let insertedCount = 0;
      let writtenCount = 0;

      for (let offset = 0; offset < normalizedItems.length; offset += QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE) {
        const batch = normalizedItems.slice(offset, offset + QUICKBOOKS_CACHE_UPSERT_BATCH_SIZE);
        const placeholders = [];
        const values = [];

        for (let index = 0; index < batch.length; index += 1) {
          const item = batch[index];
          const base = index * 8;
          placeholders.push(
            `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}::date)`,
          );
          values.push(
            item.transactionType,
            item.transactionId,
            item.customerId,
            item.clientName,
            item.clientPhone,
            item.clientEmail,
            item.paymentAmount,
            item.paymentDate,
          );
        }

        const result = await txQuery(
          `
            INSERT INTO ${QUICKBOOKS_TRANSACTIONS_TABLE}
              (transaction_type, transaction_id, customer_id, client_name, client_phone, client_email, payment_amount, payment_date)
            VALUES ${placeholders.join(", ")}
            ON CONFLICT (transaction_type, transaction_id)
            DO UPDATE
            SET
              customer_id = EXCLUDED.customer_id,
              client_name = EXCLUDED.client_name,
              client_phone = EXCLUDED.client_phone,
              client_email = EXCLUDED.client_email,
              payment_amount = EXCLUDED.payment_amount,
              payment_date = EXCLUDED.payment_date,
              updated_at = NOW()
            RETURNING (xmax = 0) AS inserted
          `,
          values,
        );

        writtenCount += Number.isFinite(result?.rowCount) ? result.rowCount : 0;
        for (const row of result.rows || []) {
          if (row?.inserted) {
            insertedCount += 1;
          }
        }
      }

      return {
        insertedCount,
        writtenCount,
      };
    });
  }

  async function listUnmatchedQuickBooksPositivePaymentsInRange(fromDate, toDate) {
    await ensureReady();
    const result = await query(
      `
        SELECT
          transaction_type,
          transaction_id,
          customer_id,
          client_name,
          client_phone,
          client_email,
          payment_amount,
          payment_date::text AS payment_date
        FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
        WHERE transaction_type = 'payment'
          AND payment_amount >= $3
          AND payment_date >= $1::date
          AND payment_date <= $2::date
          AND COALESCE(matched_record_id, '') = ''
        ORDER BY payment_date ASC, updated_at ASC, transaction_id ASC
      `,
      [fromDate, toDate, QUICKBOOKS_MIN_VISIBLE_ABS_AMOUNT],
    );
    return Array.isArray(result.rows) ? result.rows : [];
  }

  async function markQuickBooksPaymentMatched(payload = {}) {
    const transactionType = sanitizeTextValue(payload.transactionType, 40).toLowerCase();
    const transactionId = sanitizeTextValue(payload.transactionId, 160);
    const recordId = sanitizeTextValue(payload.recordId, 180);
    const paymentField = sanitizeTextValue(payload.paymentField, 40);
    const paymentDateField = sanitizeTextValue(payload.paymentDateField, 40);
    if (!transactionType || !transactionId || !recordId || !paymentField || !paymentDateField) {
      return null;
    }

    await ensureReady();
    const result = await query(
      `
        UPDATE ${QUICKBOOKS_TRANSACTIONS_TABLE}
        SET
          matched_record_id = $3,
          matched_payment_field = $4,
          matched_payment_date_field = $5,
          matched_confirmed = FALSE,
          matched_confirmed_at = NULL,
          matched_confirmed_by = '',
          matched_at = NOW(),
          updated_at = NOW()
        WHERE transaction_type = $1
          AND transaction_id = $2
        RETURNING
          transaction_type,
          transaction_id,
          matched_record_id,
          matched_payment_field,
          matched_payment_date_field,
          matched_confirmed,
          matched_confirmed_at,
          matched_confirmed_by
      `,
      [transactionType, transactionId, recordId, paymentField, paymentDateField],
    );
    return result?.rows?.[0] || null;
  }

  async function confirmQuickBooksPaymentMatch(payload = {}) {
    const transactionType = sanitizeTextValue(payload.transactionType, 40).toLowerCase();
    const transactionId = sanitizeTextValue(payload.transactionId, 160);
    const confirmedBy = sanitizeTextValue(payload.confirmedBy, 200);
    if (!transactionType || !transactionId) {
      return null;
    }

    await ensureReady();
    const result = await query(
      `
        UPDATE ${QUICKBOOKS_TRANSACTIONS_TABLE}
        SET
          matched_confirmed = TRUE,
          matched_confirmed_at = NOW(),
          matched_confirmed_by = $3,
          updated_at = NOW()
        WHERE transaction_type = $1
          AND transaction_id = $2
          AND COALESCE(matched_record_id, '') <> ''
        RETURNING
          transaction_type,
          transaction_id,
          matched_record_id,
          matched_payment_field,
          matched_payment_date_field,
          matched_confirmed,
          matched_confirmed_at,
          matched_confirmed_by
      `,
      [transactionType, transactionId, confirmedBy],
    );
    return result?.rows?.[0] || null;
  }

  async function listPendingQuickBooksPaymentMatchesByRecordId(recordId) {
    const normalizedRecordId = sanitizeTextValue(recordId, 180);
    if (!normalizedRecordId) {
      return [];
    }

    await ensureReady();
    const result = await query(
      `
        SELECT
          transaction_type,
          transaction_id,
          payment_amount,
          payment_date::text AS payment_date,
          matched_payment_field,
          matched_payment_date_field,
          matched_confirmed,
          matched_confirmed_at,
          matched_confirmed_by
        FROM ${QUICKBOOKS_TRANSACTIONS_TABLE}
        WHERE matched_record_id = $1
          AND matched_confirmed = FALSE
        ORDER BY payment_date DESC, updated_at DESC, transaction_id ASC
      `,
      [normalizedRecordId],
    );
    return Array.isArray(result.rows) ? result.rows : [];
  }

  return {
    ensureQuickBooksSchema,
    persistQuickBooksRefreshToken,
    loadQuickBooksRefreshTokenFromStateStore,
    listCachedQuickBooksCustomerContacts,
    upsertQuickBooksCustomerContacts,
    listCachedQuickBooksTransactionsInRange,
    getLatestCachedQuickBooksPaymentDate,
    listCachedQuickBooksZeroPaymentsInRange,
    upsertQuickBooksTransactions,
    listUnmatchedQuickBooksPositivePaymentsInRange,
    markQuickBooksPaymentMatched,
    confirmQuickBooksPaymentMatch,
    listPendingQuickBooksPaymentMatchesByRecordId,
  };
}

module.exports = {
  createQuickBooksRepo,
};
