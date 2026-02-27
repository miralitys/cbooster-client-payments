const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizePriority,
} = require("../server/domains/support/support.service");

test("normalizePriority maps russian labels", () => {
  assert.equal(normalizePriority("не срочно"), "low");
  assert.equal(normalizePriority("срочно"), "urgent");
  assert.equal(normalizePriority("очень срочно"), "critical");
});

test("normalizePriority defaults to normal", () => {
  assert.equal(normalizePriority(""), "normal");
  assert.equal(normalizePriority("unknown"), "normal");
});
