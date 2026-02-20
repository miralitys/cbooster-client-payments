const test = require("node:test");
const assert = require("node:assert/strict");

const {
  computeRowsChecksum,
  normalizeLegacyRecordToV2Row,
  normalizeLegacyRecordsSnapshot,
  stableStringify,
} = require("../client-records-v2-utils");

test("normalizeLegacyRecordToV2Row builds deterministic row payload", () => {
  const row = normalizeLegacyRecordToV2Row(
    {
      id: " client-1 ",
      clientName: " John Smith ",
      companyName: " ACME ",
      closedBy: " Manager ",
      createdAt: "2026-02-01T10:11:12.000Z",
      payment1: 100,
      writtenOff: true,
    },
    {
      sourceStateUpdatedAt: "2026-02-20T00:00:00.000Z",
      sourceStateRowId: 1,
    },
  );

  assert.equal(row.id, "client-1");
  assert.equal(row.clientName, "John Smith");
  assert.equal(row.companyName, "ACME");
  assert.equal(row.closedBy, "Manager");
  assert.equal(row.createdAt, "2026-02-01T10:11:12.000Z");
  assert.equal(row.sourceStateUpdatedAt, "2026-02-20T00:00:00.000Z");
  assert.equal(row.record.payment1, "100");
  assert.equal(row.record.writtenOff, "Yes");
  assert.equal(typeof row.recordHash, "string");
  assert.equal(row.recordHash.length, 64);
});

test("normalizeLegacyRecordsSnapshot is idempotent and handles duplicates", () => {
  const snapshot = normalizeLegacyRecordsSnapshot([
    { id: "a", clientName: "First" },
    { id: "a", clientName: "Second" },
    { id: "", clientName: "Missing id" },
    null,
  ]);

  assert.equal(snapshot.rows.length, 1);
  assert.equal(snapshot.rows[0].id, "a");
  assert.equal(snapshot.rows[0].clientName, "Second");
  assert.equal(snapshot.duplicateIdCount, 1);
  assert.equal(snapshot.skippedMissingIdCount, 1);
  assert.equal(snapshot.skippedInvalidRecordCount, 1);
});

test("computeRowsChecksum is stable for sorted/unsorted input", () => {
  const rows = [
    normalizeLegacyRecordToV2Row({ id: "b", clientName: "B" }),
    normalizeLegacyRecordToV2Row({ id: "a", clientName: "A" }),
  ];

  const checksum1 = computeRowsChecksum(rows);
  const checksum2 = computeRowsChecksum([...rows].reverse());
  assert.equal(checksum1, checksum2);
});

test("stableStringify sorts object keys deterministically", () => {
  const left = stableStringify({ b: 2, a: 1, nested: { z: 1, y: 2 } });
  const right = stableStringify({ nested: { y: 2, z: 1 }, a: 1, b: 2 });
  assert.equal(left, right);
});
