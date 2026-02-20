"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const {
  PATCH_OPERATION_DELETE,
  PATCH_OPERATION_UPSERT,
  applyRecordsPatchOperations,
  isRecordStateRevisionMatch,
} = require("../records-patch-utils");

test("applyRecordsPatchOperations applies upsert/delete deltas by id in order", () => {
  const nowIso = "2026-02-20T10:00:00.000Z";
  const initialRecords = [
    {
      id: "a",
      clientName: "Alice",
      companyName: "Alpha LLC",
      createdAt: "2026-01-02T08:00:00.000Z",
    },
    {
      id: "b",
      clientName: "Bob",
      companyName: "Old Co",
      createdAt: "2026-01-03T08:00:00.000Z",
    },
  ];

  const operations = [
    {
      type: PATCH_OPERATION_UPSERT,
      id: "b",
      record: {
        companyName: "New Co",
      },
    },
    {
      type: PATCH_OPERATION_UPSERT,
      id: "c",
      record: {
        clientName: "Charlie",
      },
    },
    {
      type: PATCH_OPERATION_DELETE,
      id: "a",
    },
  ];

  const nextRecords = applyRecordsPatchOperations(initialRecords, operations, { nowIso });
  assert.deepEqual(
    nextRecords.map((record) => record.id),
    ["b", "c"],
  );
  assert.equal(nextRecords[0].clientName, "Bob");
  assert.equal(nextRecords[0].companyName, "New Co");
  assert.equal(nextRecords[1].clientName, "Charlie");
  assert.equal(nextRecords[1].createdAt, nowIso);
});

test("applyRecordsPatchOperations is resilient to noop operations", () => {
  const initialRecords = [{ id: "x", clientName: "Client X", createdAt: "2026-01-05T00:00:00.000Z" }];
  const operations = [
    { type: "delete", id: "missing" },
    { type: "unknown", id: "x" },
    { type: "upsert", id: "", record: { clientName: "Should be ignored" } },
  ];

  const nextRecords = applyRecordsPatchOperations(initialRecords, operations);
  assert.deepEqual(nextRecords, initialRecords);
});

test("isRecordStateRevisionMatch returns false on stale expectedUpdatedAt", () => {
  const currentUpdatedAt = "2026-02-20T10:30:00.000Z";
  const staleExpectedUpdatedAt = "2026-02-20T10:00:00.000Z";

  assert.equal(isRecordStateRevisionMatch(staleExpectedUpdatedAt, currentUpdatedAt), false);
  assert.equal(isRecordStateRevisionMatch(currentUpdatedAt, currentUpdatedAt), true);
  assert.equal(isRecordStateRevisionMatch(null, null), true);
  assert.equal(isRecordStateRevisionMatch(null, currentUpdatedAt), false);
});
