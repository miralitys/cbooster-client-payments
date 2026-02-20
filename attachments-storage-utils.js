const path = require("path");

function sanitizeAttachmentStorageSegment(rawValue, fallback = "file") {
  const value = (rawValue || "").toString().trim();
  const normalized = value.replace(/[^a-zA-Z0-9._-]+/g, "_").replace(/^_+|_+$/g, "");
  return normalized || fallback;
}

function buildAttachmentStorageKey({ submissionId, fileId, fileName }) {
  const safeSubmissionId = sanitizeAttachmentStorageSegment(submissionId, "submission");
  const safeFileId = sanitizeAttachmentStorageSegment(fileId, "file");
  const safeFileName = sanitizeAttachmentStorageSegment(fileName, "attachment");
  return `${safeSubmissionId}/${safeFileId}-${safeFileName}`;
}

function normalizeAttachmentStorageBaseUrl(rawValue) {
  return (rawValue || "").toString().trim().replace(/\/+$/, "");
}

function buildAttachmentStorageUrl(baseUrl, storageKey) {
  const normalizedBase = normalizeAttachmentStorageBaseUrl(baseUrl);
  if (!normalizedBase) {
    return "";
  }

  const normalizedKey = normalizeAttachmentStorageKey(storageKey);
  if (!normalizedKey) {
    return "";
  }

  const encodedPath = normalizedKey
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return `${normalizedBase}/${encodedPath}`;
}

function normalizeAttachmentStorageKey(rawKey) {
  const value = (rawKey || "").toString().replace(/\\+/g, "/").trim();
  if (!value) {
    return "";
  }

  const segments = value
    .split("/")
    .map((segment) => sanitizeAttachmentStorageSegment(segment, "file"))
    .filter(Boolean);

  return segments.join("/");
}

function resolveAttachmentStoragePath(storageRoot, storageKey) {
  const root = (storageRoot || "").toString().trim();
  if (!root) {
    return "";
  }

  const normalizedRoot = path.resolve(root);
  const normalizedKey = normalizeAttachmentStorageKey(storageKey);
  if (!normalizedKey) {
    return "";
  }

  const candidatePath = path.resolve(normalizedRoot, normalizedKey);
  const isInsideRoot = candidatePath === normalizedRoot || candidatePath.startsWith(`${normalizedRoot}${path.sep}`);
  if (!isInsideRoot) {
    return "";
  }

  return candidatePath;
}

module.exports = {
  buildAttachmentStorageKey,
  buildAttachmentStorageUrl,
  normalizeAttachmentStorageBaseUrl,
  normalizeAttachmentStorageKey,
  resolveAttachmentStoragePath,
  sanitizeAttachmentStorageSegment,
};
