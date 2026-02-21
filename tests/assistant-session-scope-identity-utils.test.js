"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { normalizeAuthUsernameForScopeKey } = require("../assistant-session-scope-identity-utils");

function legacyLossyScopeUserNormalization(rawValue) {
  return (rawValue || "")
    .toString()
    .normalize("NFKC")
    .trim()
    .toLowerCase()
    .replace(/[−–—]/g, "-")
    .replace(/[^\p{L}\p{N}\s@._-]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

test("normalizeAuthUsernameForScopeKey keeps auth-username identity stable", () => {
  assert.equal(normalizeAuthUsernameForScopeKey(" Owner "), "owner");
  assert.equal(normalizeAuthUsernameForScopeKey("TeAm+Lead@CreditBooster.Com"), "team+lead@creditbooster.com");
  assert.equal(normalizeAuthUsernameForScopeKey("Ａdmin"), "admin");
});

test("normalizeAuthUsernameForScopeKey avoids lossy collisions from legacy normalization", () => {
  const collisionMatrix = [
    ["ops+west@creditbooster.com", "ops west@creditbooster.com"],
    ["qa|owner@creditbooster.com", "qa owner@creditbooster.com"],
    ["alex—north@creditbooster.com", "alex-north@creditbooster.com"],
    ["collector#1@creditbooster.com", "collector 1@creditbooster.com"],
  ];

  for (const [left, right] of collisionMatrix) {
    assert.equal(
      legacyLossyScopeUserNormalization(left),
      legacyLossyScopeUserNormalization(right),
      `Legacy lossy normalization must collide for pair: "${left}" vs "${right}"`,
    );
    assert.notEqual(
      normalizeAuthUsernameForScopeKey(left),
      normalizeAuthUsernameForScopeKey(right),
      `Scope key user segment must stay unique for pair: "${left}" vs "${right}"`,
    );
  }
});
