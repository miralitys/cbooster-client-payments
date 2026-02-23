"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const ALLOWED_PG_IMPORT_FILE = "server/shared/db/pool.js";

const BACKEND_ROOT_ENTRIES = [
  "server-legacy.js",
  "server.js",
  "server",
  "custom-dashboard-module.js",
  "attachments-storage-utils.js",
  "client-records-v2-utils.js",
  "records-patch-utils.js",
  "assistant-session-scope-identity-utils.js",
];

const EXCLUDED_SEGMENTS = new Set(["docs", "tests", "dist", "coverage", "node_modules"]);

function toPosixRelative(absPath) {
  return path.relative(PROJECT_ROOT, absPath).split(path.sep).join("/");
}

function shouldExcludePath(absPath) {
  const relativePath = toPosixRelative(absPath);
  const parts = relativePath.split("/");
  return parts.some((segment) => EXCLUDED_SEGMENTS.has(segment));
}

function collectJsFilesFromEntry(entryPath, results) {
  if (!fs.existsSync(entryPath)) {
    return;
  }

  const stats = fs.statSync(entryPath);
  if (stats.isDirectory()) {
    const names = fs.readdirSync(entryPath);
    for (const name of names) {
      collectJsFilesFromEntry(path.join(entryPath, name), results);
    }
    return;
  }

  if (!stats.isFile()) {
    return;
  }

  if (!entryPath.endsWith(".js")) {
    return;
  }

  if (shouldExcludePath(entryPath)) {
    return;
  }

  results.push(entryPath);
}

function collectBackendJsFiles() {
  const files = [];
  for (const entry of BACKEND_ROOT_ENTRIES) {
    collectJsFilesFromEntry(path.join(PROJECT_ROOT, entry), files);
  }
  return [...new Set(files)].sort();
}

test("legacy backend file has no direct pool.query/client.query calls", () => {
  const legacyPath = path.join(PROJECT_ROOT, "server-legacy.js");
  const source = fs.readFileSync(legacyPath, "utf8");

  assert.equal(/\bpool\.query\(/.test(source), false, "server-legacy.js must not contain pool.query(");
  assert.equal(/\bclient\.query\(/.test(source), false, "server-legacy.js must not contain client.query(");
});

test("backend JS imports pg only from server/shared/db/pool.js", () => {
  const backendFiles = collectBackendJsFiles();
  const pgImportPattern = /require\(["']pg["']\)|\bfrom\s+["']pg["']|\bimport\s+[^;\n]+\s+from\s+["']pg["']/g;

  const violations = [];
  for (const filePath of backendFiles) {
    const source = fs.readFileSync(filePath, "utf8");
    const matches = source.match(pgImportPattern);
    if (!matches || matches.length === 0) {
      continue;
    }

    const relativePath = toPosixRelative(filePath);
    if (relativePath !== ALLOWED_PG_IMPORT_FILE) {
      violations.push(`${relativePath}: ${matches.join(" | ")}`);
    }
  }

  assert.deepEqual(violations, [], `Unexpected pg import usage:\n${violations.join("\n")}`);
});
