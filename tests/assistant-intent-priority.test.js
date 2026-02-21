"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { __assistantInternals } = require("../server.js");

function createRecord({
  clientName,
  managerName,
  companyName,
  contractAmount,
  payments,
  createdAt,
}) {
  const safePayments = Array.isArray(payments) ? payments : [];
  const totalPaid = safePayments.reduce((sum, item) => sum + Number(item?.amount || 0), 0);
  const record = {
    clientName,
    closedBy: managerName,
    companyName,
    contractTotals: String(contractAmount),
    futurePayments: String(Math.max(0, Number(contractAmount || 0) - totalPaid)),
    createdAt,
  };

  safePayments.forEach((item, index) => {
    const paymentIndex = index + 1;
    record[`payment${paymentIndex}`] = String(item.amount);
    record[`payment${paymentIndex}Date`] = item.date;
  });

  return record;
}

const FIXTURE_RECORDS = [
  createRecord({
    clientName: "John Smith",
    managerName: "Vlad Burnis",
    companyName: "Alpha Inc",
    contractAmount: 1000,
    payments: [{ amount: 300, date: "2026-02-05" }],
    createdAt: "2026-02-01T10:00:00Z",
  }),
  createRecord({
    clientName: "Alice Johnson",
    managerName: "Nenad Nash",
    companyName: "Beta LLC",
    contractAmount: 500,
    payments: [{ amount: 500, date: "2026-02-06" }],
    createdAt: "2026-02-02T10:00:00Z",
  }),
  createRecord({
    clientName: "Robert Miles",
    managerName: "Nenad Nash",
    companyName: "Gamma Ltd",
    contractAmount: 2200,
    payments: [{ amount: 200, date: "2026-02-07" }],
    createdAt: "2026-02-03T10:00:00Z",
  }),
];

const UPDATED_AT = "2026-02-10T00:00:00Z";

test("assistant internals export is available for contract tests", () => {
  assert.ok(__assistantInternals);
  assert.equal(typeof __assistantInternals.buildAssistantReplyPayload, "function");
  assert.equal(typeof __assistantInternals.getAssistantIntentPriorityTable, "function");
});

test("assistant intent priority table is explicit and ordered", () => {
  const table = __assistantInternals.getAssistantIntentPriorityTable();
  assert.ok(Array.isArray(table));
  assert.equal(table.length >= 20, true);
  assert.deepEqual(
    table.slice(0, 6).map((item) => item.key),
    ["help", "context_reset", "scope_follow_up", "manager_compare", "client_range_list_scope", "new_clients"],
  );
  assert.deepEqual(
    table.slice(-3).map((item) => item.key),
    ["summary_metrics", "client_lookup", "fallback_summary"],
  );
  table.forEach((item, index) => {
    assert.equal(item.rank, index + 1);
  });
});

test("context reset intent has priority over additional commands", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "сбрось контекст и покажи топ-5 должников",
    FIXTURE_RECORDS,
    UPDATED_AT,
    {
      clientComparables: ["john smith"],
      scopeEstablished: true,
    },
  );

  assert.match(result.reply, /Контекст предыдущей выборки очищен/i);
  assert.equal(result.handledByRules, true);
  assert.equal(result.scope, null);
});

test("scope follow-up uses saved scope and does not aggregate over all records", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Какой общий долг по ним?",
    FIXTURE_RECORDS,
    UPDATED_AT,
    {
      clientComparables: ["john smith"],
      scopeEstablished: true,
    },
  );

  assert.match(result.reply, /Общий остаток долга/i);
  assert.match(result.reply, /700\.00/);
  assert.ok(result.scope);
  assert.deepEqual(result.scope.clientComparables, ["john smith"]);
});

test("explicit fresh-scope intent dominates over follow-up reference in one message", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Покажи клиентов с 2026-02-01 по 2026-02-09 и посчитай долг по ним",
    FIXTURE_RECORDS,
    UPDATED_AT,
    {
      clientComparables: ["john smith"],
      scopeEstablished: true,
    },
  );

  assert.equal(result.handledByRules, true);
  assert.ok(result.scope);
  assert.equal(result.scope.scopeEstablished, true);
  assert.ok(Array.isArray(result.scope.clientComparables));
  assert.ok(result.scope.clientComparables.length >= 2);
  assert.notDeepEqual(result.scope.clientComparables, ["john smith"]);
  assert.doesNotMatch(result.reply, /Контекст найден/i);
});

test("scope follow-up still applies when message has no explicit fresh-scope intent", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Посчитай общий долг по ним за последнюю неделю",
    FIXTURE_RECORDS,
    UPDATED_AT,
    {
      clientComparables: ["john smith"],
      scopeEstablished: true,
    },
  );

  assert.match(result.reply, /Общий остаток долга/i);
  assert.match(result.reply, /700\.00/);
  assert.ok(result.scope);
  assert.deepEqual(result.scope.clientComparables, ["john smith"]);
});

test("manager comparison intent has priority over generic manager list", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Сравни менеджеров Vlad Burnis и Nenad Nash",
    FIXTURE_RECORDS,
    UPDATED_AT,
  );

  assert.match(result.reply, /Сравнение менеджеров/i);
  assert.match(result.reply, /Vlad Burnis/i);
  assert.match(result.reply, /Nenad Nash/i);
});

test("client listing by explicit date range establishes explicit scope", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Покажи клиентов с 2026-02-01 по 2026-02-09",
    FIXTURE_RECORDS,
    UPDATED_AT,
  );

  assert.equal(result.handledByRules, true);
  assert.ok(result.scope);
  assert.equal(result.scope.scopeEstablished, true);
  assert.ok(Array.isArray(result.scope.clientComparables));
  assert.ok(result.scope.clientComparables.length >= 2);
});

test("top debt intent keeps priority over generic summary intent", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "покажи топ должников и сводку",
    FIXTURE_RECORDS,
    UPDATED_AT,
  );

  assert.match(result.reply, /Топ/i);
  assert.doesNotMatch(result.reply, /Доступно клиентов:/i);
});

test("compare managers intent dominates manager ranking in same message", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Сравни менеджеров Vlad Burnis и Nenad Nash и покажи рейтинг менеджеров по долгу",
    FIXTURE_RECORDS,
    UPDATED_AT,
  );

  assert.match(result.reply, /Сравнение менеджеров/i);
  assert.doesNotMatch(result.reply, /Рейтинг менеджеров/i);
});

test("fresh range intent with follow-up wording does not reuse stale scoped list", () => {
  const result = __assistantInternals.buildAssistantReplyPayload(
    "Покажи клиентов с 2026-02-01 по 2026-02-09 и топ должников по ним",
    FIXTURE_RECORDS,
    UPDATED_AT,
    {
      clientComparables: ["john smith"],
      scopeEstablished: true,
    },
  );

  assert.ok(result.scope);
  assert.ok(Array.isArray(result.scope.clientComparables));
  assert.ok(result.scope.clientComparables.length >= 2);
  assert.notDeepEqual(result.scope.clientComparables, ["john smith"]);
  assert.doesNotMatch(result.reply, /Контекст найден/i);
});
