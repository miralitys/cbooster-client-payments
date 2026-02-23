"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { createGhlReadOnlyGuard, isAllowedGhlRequest } = require("../server/integrations/ghl/client");

test("GHL read-only guard allows GET/read requests and search POST", () => {
  assert.equal(isAllowedGhlRequest("GET", "/contacts/123"), true);
  assert.equal(isAllowedGhlRequest("HEAD", "/opportunities/123"), true);
  assert.equal(isAllowedGhlRequest("POST", "/contacts/search"), true);
  assert.equal(isAllowedGhlRequest("POST", "/opportunities/search"), true);
  assert.equal(isAllowedGhlRequest("POST", "/conversations/search"), true);
});

test("GHL read-only guard blocks write-like requests and logs attempt", () => {
  const logs = [];
  const guard = createGhlReadOnlyGuard({
    logger: {
      warn: (message) => logs.push(String(message || "")),
    },
  });

  assert.throws(
    () => {
      guard.assertAllowedRequest({
        method: "POST",
        pathname: "/contacts/abc/notes",
        source: "unit-test",
      });
    },
    (error) => {
      assert.equal(error?.httpStatus, 403);
      assert.equal(error?.code, "ghl_read_only_blocked");
      return true;
    },
  );

  assert.ok(logs.length >= 1);
  assert.match(logs[0], /ghl-read-only/i);
  assert.match(logs[0], /blocked POST \/contacts\/abc\/notes/i);
});
