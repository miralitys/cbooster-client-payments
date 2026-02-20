const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("path");

const {
  buildAttachmentStorageKey,
  buildAttachmentStorageUrl,
  normalizeAttachmentStorageKey,
  resolveAttachmentStoragePath,
} = require("../attachments-storage-utils");

test("buildAttachmentStorageKey sanitizes input segments", () => {
  const key = buildAttachmentStorageKey({
    submissionId: "sub 123/..",
    fileId: "file:45",
    fileName: "contract signed (v1).pdf",
  });

  assert.equal(key, "sub_123/file_45-contract_signed_v1_.pdf");
});

test("normalizeAttachmentStorageKey strips unsafe path pieces", () => {
  const normalized = normalizeAttachmentStorageKey("../../../../secret\\docs/contract.pdf");
  assert.equal(normalized, "file/file/file/file/secret/docs/contract.pdf");
});

test("resolveAttachmentStoragePath keeps path inside storage root", () => {
  const root = path.resolve("/tmp/cbooster-test-root");
  const resolved = resolveAttachmentStoragePath(root, "submission/file-contract.pdf");

  assert.equal(resolved, path.join(root, "submission", "file-contract.pdf"));
  assert.ok(resolved.startsWith(root));
});

test("buildAttachmentStorageUrl returns encoded URL", () => {
  const url = buildAttachmentStorageUrl("https://cdn.example.com/uploads/", "submission id/file name.pdf");
  assert.equal(url, "https://cdn.example.com/uploads/submission_id/file_name.pdf");
});
