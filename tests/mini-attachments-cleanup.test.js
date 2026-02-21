"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const vm = require("node:vm");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_FILE = path.join(PROJECT_ROOT, "server.js");

function extractNamedFunctionSource(source, functionName) {
  const marker = new RegExp(`(?:async\\s+)?function\\s+${functionName}\\s*\\(`);
  const match = marker.exec(source);
  if (!match) {
    throw new Error(`Function "${functionName}" was not found in server.js`);
  }

  const startIndex = match.index;
  const bodyStart = source.indexOf("{", match.index);
  if (bodyStart < 0) {
    throw new Error(`Function "${functionName}" body start was not found`);
  }

  let depth = 0;
  let endIndex = -1;
  for (let index = bodyStart; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") {
      depth += 1;
      continue;
    }
    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        endIndex = index + 1;
        break;
      }
    }
  }

  if (endIndex < 0) {
    throw new Error(`Function "${functionName}" body end was not found`);
  }

  return source.slice(startIndex, endIndex);
}

function loadAttachmentCleanupFunctions({
  fsModule = fs,
  pathModule = path,
  consoleModule = console,
} = {}) {
  const source = fs.readFileSync(SERVER_FILE, "utf8");

  const snippets = [
    extractNamedFunctionSource(source, "sanitizeUploadedTempPath"),
    extractNamedFunctionSource(source, "collectAttachmentTempFilePathsFromAttachments"),
    extractNamedFunctionSource(source, "cleanupTemporaryAttachmentFiles"),
    extractNamedFunctionSource(source, "cleanupTemporaryUploadFiles"),
    extractNamedFunctionSource(source, "removeFileIfExists"),
  ];

  const scriptSource = `
${snippets.join("\n\n")}
module.exports = {
  sanitizeUploadedTempPath,
  collectAttachmentTempFilePathsFromAttachments,
  cleanupTemporaryAttachmentFiles,
  cleanupTemporaryUploadFiles,
  removeFileIfExists,
};
`;

  const sandbox = {
    module: { exports: {} },
    exports: {},
    fs: fsModule,
    path: pathModule,
    console: consoleModule,
  };

  vm.runInNewContext(scriptSource, sandbox, {
    filename: "server-cleanup-extract.vm.js",
  });

  return sandbox.module.exports;
}

function createFsDouble({
  failCodeByPath = new Map(),
  calls = [],
} = {}) {
  return {
    promises: {
      unlink: async (targetPath) => {
        const normalizedPath = String(targetPath || "");
        calls.push(normalizedPath);

        if (failCodeByPath.has(normalizedPath)) {
          const error = new Error(`Synthetic unlink failure (${failCodeByPath.get(normalizedPath)})`);
          error.code = failCodeByPath.get(normalizedPath);
          throw error;
        }

        return fs.promises.unlink(normalizedPath);
      },
    },
  };
}

test("cleanupTemporaryUploadFiles tolerates repeated cleanup and ENOENT", async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "mini-cleanup-repeat-"));
  try {
    const filePath = path.join(tempDir, "upload.tmp");
    await fs.promises.writeFile(filePath, "content", "utf8");

    const unlinkCalls = [];
    const cleanupFns = loadAttachmentCleanupFunctions({
      fsModule: createFsDouble({ calls: unlinkCalls }),
    });

    await cleanupFns.cleanupTemporaryUploadFiles([{ path: filePath }]);
    assert.equal(fs.existsSync(filePath), false);

    await cleanupFns.cleanupTemporaryUploadFiles([{ path: filePath }]);
    assert.equal(fs.existsSync(filePath), false);

    // First unlink removes existing file, second hits ENOENT and is ignored.
    assert.equal(unlinkCalls.length, 2);
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
});

test("cleanup deduplicates identical file paths before unlink", async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "mini-cleanup-dedupe-"));
  try {
    const uploadPath = path.join(tempDir, "same-upload.tmp");
    const attachmentPath = path.join(tempDir, "same-attachment.tmp");
    await fs.promises.writeFile(uploadPath, "upload", "utf8");
    await fs.promises.writeFile(attachmentPath, "attachment", "utf8");

    const uploadUnlinkCalls = [];
    const uploadCleanupFns = loadAttachmentCleanupFunctions({
      fsModule: createFsDouble({ calls: uploadUnlinkCalls }),
    });

    await uploadCleanupFns.cleanupTemporaryUploadFiles([
      { path: uploadPath },
      { path: uploadPath },
      { path: path.resolve(uploadPath) },
    ]);
    assert.equal(fs.existsSync(uploadPath), false);
    assert.deepEqual(uploadUnlinkCalls, [path.resolve(uploadPath)]);

    const attachmentUnlinkCalls = [];
    const attachmentCleanupFns = loadAttachmentCleanupFunctions({
      fsModule: createFsDouble({ calls: attachmentUnlinkCalls }),
    });

    await attachmentCleanupFns.cleanupTemporaryAttachmentFiles([
      { tempPath: attachmentPath },
      { tempPath: ` ${attachmentPath} ` },
      { tempPath: "" },
      {},
    ]);
    assert.equal(fs.existsSync(attachmentPath), false);
    assert.deepEqual(attachmentUnlinkCalls, [path.resolve(attachmentPath)]);
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
});

test("cleanup remains resilient on partial unlink errors", async () => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "mini-cleanup-partial-"));
  try {
    const lockedPath = path.join(tempDir, "locked.tmp");
    const removablePath = path.join(tempDir, "removable.tmp");
    await fs.promises.writeFile(lockedPath, "locked", "utf8");
    await fs.promises.writeFile(removablePath, "removable", "utf8");

    const errorLogs = [];
    const consoleDouble = {
      error: (...args) => {
        errorLogs.push(args.map((item) => String(item)).join(" "));
      },
    };

    const unlinkCalls = [];
    const cleanupFns = loadAttachmentCleanupFunctions({
      fsModule: createFsDouble({
        calls: unlinkCalls,
        failCodeByPath: new Map([[path.resolve(lockedPath), "EACCES"]]),
      }),
      consoleModule: consoleDouble,
    });

    await cleanupFns.cleanupTemporaryUploadFiles([
      { path: lockedPath },
      { path: removablePath },
    ]);

    assert.equal(fs.existsSync(lockedPath), true);
    assert.equal(fs.existsSync(removablePath), false);
    assert.equal(unlinkCalls.length, 2);
    assert.ok(errorLogs.some((line) => line.includes("Failed to remove temporary file")));
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
});

