"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { __assistantInternals } = require("../server.js");

test("assistant context reset telemetry stage normalization is strict", () => {
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureStage("beacon_failed"), "beacon_failed");
  assert.equal(
    __assistantInternals.normalizeAssistantContextResetFailureStage("keepalive_retry_exhausted"),
    "keepalive_retry_exhausted",
  );
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureStage("Beacon_Failed"), "beacon_failed");
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureStage("custom_stage"), "unknown");
});

test("assistant context reset telemetry reason code normalization is strict", () => {
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureReasonCode("timeout"), "timeout");
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureReasonCode("NETWORK_ERROR"), "network_error");
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureReasonCode("csrf"), "csrf");
  assert.equal(__assistantInternals.normalizeAssistantContextResetFailureReasonCode("socket_hangup"), "unknown_error");
});

test("assistant context reset telemetry browser parser returns major version buckets", () => {
  const chrome = __assistantInternals.parseAssistantContextResetBrowserFromUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.6943.55 Safari/537.36",
  );
  assert.deepEqual(chrome, {
    name: "chrome",
    version: "133",
  });

  const edge = __assistantInternals.parseAssistantContextResetBrowserFromUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.3065.51",
  );
  assert.deepEqual(edge, {
    name: "edge",
    version: "133",
  });

  const safari = __assistantInternals.parseAssistantContextResetBrowserFromUserAgent(
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
  );
  assert.deepEqual(safari, {
    name: "safari",
    version: "17",
  });
});

test("assistant context reset telemetry browser key is normalized", () => {
  assert.equal(
    __assistantInternals.buildAssistantContextResetBrowserVersionKey({
      name: "CHROME",
      version: "133.0.1",
    }),
    "chrome/133",
  );
  assert.equal(
    __assistantInternals.buildAssistantContextResetBrowserVersionKey({
      name: "",
      version: "",
    }),
    "unknown/0",
  );
});
