const test = require("node:test");
const assert = require("node:assert/strict");

const { createQuickBooksService } = require("../server/domains/quickbooks/quickbooks.service");

test("createQuickBooksService requires quickbooks repo", () => {
  assert.throws(() => createQuickBooksService({}), /requires quickbooks repo/i);
});

test("listCachedQuickBooksTransactionsInRange uses injected normalized loader when provided", async () => {
  const injectedResult = [{ clientName: "Client A", paymentAmount: 100, paymentDate: "2026-02-20", transactionType: "payment" }];
  let injectedCallCount = 0;
  let repoCallCount = 0;

  const service = createQuickBooksService({
    repo: {
      async listCachedQuickBooksTransactionsInRange() {
        repoCallCount += 1;
        return [{ client_name: "raw_row" }];
      },
    },
    listCachedQuickBooksTransactionsInRange: async () => {
      injectedCallCount += 1;
      return injectedResult;
    },
  });

  const result = await service.listCachedQuickBooksTransactionsInRange("2026-02-01", "2026-02-28");

  assert.equal(injectedCallCount, 1);
  assert.equal(repoCallCount, 0);
  assert.deepEqual(result, injectedResult);
});

test("listCachedQuickBooksTransactionsInRange falls back to repo loader", async () => {
  const repoResult = [{ client_name: "Raw Client", payment_amount: "120.00", payment_date: "2026-02-18" }];
  let repoCallCount = 0;

  const service = createQuickBooksService({
    repo: {
      async listCachedQuickBooksTransactionsInRange() {
        repoCallCount += 1;
        return repoResult;
      },
    },
  });

  const result = await service.listCachedQuickBooksTransactionsInRange("2026-02-01", "2026-02-28");

  assert.equal(repoCallCount, 1);
  assert.deepEqual(result, repoResult);
});

test("autoApplyQuickBooksPaymentsToRecordsInRange uses injected matcher when provided", async () => {
  const expected = { matchedCount: 2, skippedCount: 0 };
  let callCount = 0;

  const service = createQuickBooksService({
    repo: {
      async listCachedQuickBooksTransactionsInRange() {
        return [];
      },
    },
    autoApplyQuickBooksPaymentsToRecordsInRange: async (fromDate, toDate) => {
      callCount += 1;
      assert.equal(fromDate, "2026-02-01");
      assert.equal(toDate, "2026-02-28");
      return expected;
    },
  });

  const result = await service.autoApplyQuickBooksPaymentsToRecordsInRange("2026-02-01", "2026-02-28");
  assert.equal(callCount, 1);
  assert.deepEqual(result, expected);
});

test("autoApplyQuickBooksPaymentsToRecordsInRange returns null when matcher is not injected", async () => {
  const service = createQuickBooksService({
    repo: {
      async listCachedQuickBooksTransactionsInRange() {
        return [];
      },
    },
  });

  const result = await service.autoApplyQuickBooksPaymentsToRecordsInRange("2026-02-01", "2026-02-28");
  assert.equal(result, null);
});
