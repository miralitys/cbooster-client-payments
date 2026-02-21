"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { __assistantInternals } = require("../server.js");

test("assistant scope tenant key uses trusted auth profile over client headers", () => {
  const tenantKey = __assistantInternals.resolveAssistantSessionScopeTenantKeyFromRequest({
    headers: {
      "x-cbooster-tenant": "spoof-tenant",
      "x-tenant-id": "spoof-tenant-2",
    },
    hostname: "evil.example.com",
    webAuthProfile: {
      tenantId: "Org Alpha",
    },
  });

  assert.equal(tenantKey, "org-alpha");
});

test("assistant scope tenant key falls back to profile org fields", () => {
  const tenantKey = __assistantInternals.resolveAssistantSessionScopeTenantKeyFromRequest({
    webAuthProfile: {
      orgId: "Enterprise_North",
    },
  });

  assert.equal(tenantKey, "enterprise_north");
});

test("assistant scope tenant key ignores tenant headers when auth profile has no tenant", () => {
  const tenantKey = __assistantInternals.resolveAssistantSessionScopeTenantKeyFromRequest({
    headers: {
      "x-cbooster-tenant": "spoof-tenant",
      "x-tenant-id": "spoof-tenant-2",
    },
    hostname: "tenant-from-hostname.example.com",
    webAuthProfile: {},
  });

  assert.equal(tenantKey, "default");
});

test("assistant client message seq normalization accepts positive integers only", () => {
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq(1), 1);
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq("42"), 42);
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq("0007"), 7);
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq("0"), 0);
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq("-5"), 0);
  assert.equal(__assistantInternals.normalizeAssistantClientMessageSeq("abc"), 0);
});

test("assistant stale snapshot fallback returns degraded state when cache is fresh enough", () => {
  const nowMs = 1_700_000_000_000;
  const fallbackState = __assistantInternals.buildAssistantStaleSnapshotFallbackState(
    {
      records: [{ id: "c1" }],
      updatedAt: "2026-02-20T10:00:00.000Z",
      source: "legacy",
      fallbackFromV2: false,
      refreshedAtMs: nowMs - 900,
    },
    {
      nowMs,
      maxAgeMs: 5_000,
      enabled: true,
      degradedReason: "db_read_failed",
    },
  );

  assert.ok(fallbackState);
  assert.equal(fallbackState.degradedMode, true);
  assert.equal(fallbackState.degradedReason, "db_read_failed");
  assert.equal(fallbackState.staleSnapshotAgeMs, 900);
  assert.equal(Array.isArray(fallbackState.records), true);
  assert.equal(fallbackState.records.length, 1);
});

test("assistant stale snapshot fallback returns null when cache is too old", () => {
  const nowMs = 1_700_000_000_000;
  const fallbackState = __assistantInternals.buildAssistantStaleSnapshotFallbackState(
    {
      records: [{ id: "c1" }],
      updatedAt: "2026-02-20T10:00:00.000Z",
      source: "legacy",
      fallbackFromV2: false,
      refreshedAtMs: nowMs - 12_000,
    },
    {
      nowMs,
      maxAgeMs: 5_000,
      enabled: true,
    },
  );

  assert.equal(fallbackState, null);
});
