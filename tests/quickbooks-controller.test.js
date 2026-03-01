"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { createQuickBooksController } = require("../server/domains/quickbooks/quickbooks.controller");

function sanitizeTextValue(value, maxLength = 500) {
  return String(value ?? "")
    .trim()
    .slice(0, maxLength);
}

function createMockResponse() {
  return {
    statusCode: 200,
    payload: null,
    status(code) {
      this.statusCode = Number(code);
      return this;
    },
    json(body) {
      this.payload = body;
      return this;
    },
  };
}

function createController(overrides = {}) {
  const dependencies = {
    quickBooksService: {
      async listCachedQuickBooksTransactionsInRange() {
        return [];
      },
      buildQuickBooksSyncMeta() {
        return {
          requested: false,
          syncMode: "incremental",
        };
      },
      ...overrides.quickBooksService,
    },
    sanitizeTextValue,
    enforceRateLimit: () => true,
    rateLimitProfileApiExpensive: {
      windowMs: 60_000,
      maxHitsIp: 100,
      maxHitsUser: 100,
      blockMs: 60_000,
    },
    rateLimitProfileApiSync: {
      windowMs: 60_000,
      maxHitsIp: 100,
      maxHitsUser: 100,
      blockMs: 60_000,
    },
    hasDatabase: true,
    isQuickBooksConfigured: () => true,
    getQuickBooksDateRange: (from, to) => ({ from: sanitizeTextValue(from, 20), to: sanitizeTextValue(to, 20) }),
    parseQuickBooksSyncFlag: () => false,
    parseQuickBooksTotalRefreshFlag: () => false,
    listCachedQuickBooksTransactionsInRange: async () => [],
    listQuickBooksOutgoingTransactionsInRange: async () => [],
    buildQuickBooksSyncMeta: () => ({
      requested: false,
      syncMode: "incremental",
    }),
    enqueueQuickBooksSyncJob: () => ({
      job: null,
      reused: false,
    }),
    buildQuickBooksSyncJobPayload: () => null,
    getQuickBooksSyncJobById: () => null,
    hasWebAuthPermission: () => true,
    webAuthPermissionSyncQuickbooks: "sync_quickbooks",
    requestOpenAiQuickBooksInsight: async () => "",
    ...overrides,
  };

  return createQuickBooksController(dependencies);
}

test("GET /api/quickbooks/payments/recent runs auto-apply on unmatched positive payments and returns refreshed rows", async () => {
  const firstRows = [
    {
      transactionType: "payment",
      transactionId: "tx-1",
      clientName: "Gregory Pugach",
      paymentAmount: 800,
      paymentDate: "2026-02-28",
      matchedRecordId: "",
    },
  ];
  const secondRows = [
    {
      ...firstRows[0],
      matchedRecordId: "record-123",
      matchedPaymentField: "payment3",
      matchedPaymentDateField: "payment3Date",
    },
  ];

  let listCallCount = 0;
  let autoApplyCallCount = 0;
  const controller = createController({
    quickBooksService: {
      async listCachedQuickBooksTransactionsInRange() {
        listCallCount += 1;
        return listCallCount === 1 ? firstRows : secondRows;
      },
      async autoApplyQuickBooksPaymentsToRecordsInRange(fromDate, toDate) {
        autoApplyCallCount += 1;
        assert.equal(fromDate, "2026-02-28");
        assert.equal(toDate, "2026-02-28");
        return {
          matchedCount: 1,
          skippedCount: 0,
        };
      },
    },
  });

  const req = {
    method: "GET",
    query: {
      from: "2026-02-28",
      to: "2026-02-28",
    },
  };
  const res = createMockResponse();

  await controller.handleQuickBooksRecentPaymentsGet(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(autoApplyCallCount, 1);
  assert.equal(listCallCount, 2);
  assert.equal(res.payload?.count, 1);
  assert.equal(res.payload?.items?.[0]?.matchedRecordId, "record-123");
});

test("GET /api/quickbooks/payments/recent does not run auto-apply when all rows are already matched", async () => {
  let autoApplyCallCount = 0;
  const rows = [
    {
      transactionType: "payment",
      transactionId: "tx-2",
      clientName: "Yevhen Kobzar",
      paymentAmount: 1000,
      paymentDate: "2026-02-28",
      matchedRecordId: "record-456",
    },
  ];

  const controller = createController({
    quickBooksService: {
      async listCachedQuickBooksTransactionsInRange() {
        return rows;
      },
      async autoApplyQuickBooksPaymentsToRecordsInRange() {
        autoApplyCallCount += 1;
        return null;
      },
    },
  });

  const req = {
    method: "GET",
    query: {
      from: "2026-02-28",
      to: "2026-02-28",
    },
  };
  const res = createMockResponse();

  await controller.handleQuickBooksRecentPaymentsGet(req, res);

  assert.equal(res.statusCode, 200);
  assert.equal(autoApplyCallCount, 0);
  assert.equal(res.payload?.count, 1);
});
