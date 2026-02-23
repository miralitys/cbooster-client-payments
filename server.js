"use strict";

const fs = require("fs");
const path = require("path");
const server = require("./server/index.js");

// Compatibility shims for existing test harnesses that extract pure functions
// from `server.js` source text. Runtime logic is delegated to `server/index.js`.
function sanitizeUploadedTempPath(rawPath) {
  const value = (rawPath || "").toString().trim();
  if (!value) {
    return "";
  }

  return path.resolve(value);
}

function collectAttachmentTempFilePathsFromAttachments(attachments) {
  const normalizedAttachments = Array.isArray(attachments) ? attachments : [];
  const uniquePaths = new Set();
  for (const attachment of normalizedAttachments) {
    const tempPath = sanitizeUploadedTempPath(attachment?.tempPath);
    if (tempPath) {
      uniquePaths.add(tempPath);
    }
  }
  return [...uniquePaths];
}

async function cleanupTemporaryAttachmentFiles(attachments) {
  const filePaths = collectAttachmentTempFilePathsFromAttachments(attachments);
  if (!filePaths.length) {
    return;
  }

  await Promise.all(filePaths.map((filePath) => removeFileIfExists(filePath)));
}

async function cleanupTemporaryUploadFiles(rawFiles) {
  const files = Array.isArray(rawFiles) ? rawFiles : [];
  if (!files.length) {
    return;
  }

  const uniquePaths = new Set();
  for (const file of files) {
    const tempPath = sanitizeUploadedTempPath(file?.path);
    if (tempPath) {
      uniquePaths.add(tempPath);
    }
  }

  if (!uniquePaths.size) {
    return;
  }

  await Promise.all([...uniquePaths].map((filePath) => removeFileIfExists(filePath)));
}

async function removeFileIfExists(filePath) {
  if (!filePath) {
    return;
  }

  try {
    await fs.promises.unlink(filePath);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return;
    }
    console.error("[attachments] Failed to remove temporary file:", filePath, error);
  }
}

function shouldWriteLegacyStateOnMiniApproval(options = {}) {
  const writeV2Enabled = options?.writeV2Enabled === true;
  const readV2Enabled = options?.readV2Enabled === true;
  const legacyMirrorEnabled = options?.legacyMirrorEnabled === true;

  if (!writeV2Enabled) {
    return true;
  }

  if (!readV2Enabled) {
    return true;
  }

  return legacyMirrorEnabled;
}

if (require.main === module) {
  server.autostartIfNeeded({
    isDirectExecution: true,
  });
}

module.exports = server;
