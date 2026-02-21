"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const PROJECT_ROOT = path.resolve(__dirname, "..");
const SERVER_FILE = path.join(PROJECT_ROOT, "server.js");

function extractNamedFunctionSource(source, functionName) {
  const marker = new RegExp(`function\\s+${functionName}\\s*\\(`);
  const match = marker.exec(source);
  if (!match) {
    throw new Error(`Function "${functionName}" was not found in server.js`);
  }

  const paramsStart = source.indexOf("(", match.index);
  if (paramsStart < 0) {
    throw new Error(`Function "${functionName}" params start was not found`);
  }

  let paramsDepth = 0;
  let paramsEnd = -1;
  for (let index = paramsStart; index < source.length; index += 1) {
    const char = source[index];
    if (char === "(") {
      paramsDepth += 1;
      continue;
    }
    if (char === ")") {
      paramsDepth -= 1;
      if (paramsDepth === 0) {
        paramsEnd = index;
        break;
      }
    }
  }
  if (paramsEnd < 0) {
    throw new Error(`Function "${functionName}" params end was not found`);
  }

  const bodyStart = source.indexOf("{", paramsEnd + 1);
  if (bodyStart < 0) {
    throw new Error(`Function "${functionName}" body start was not found`);
  }

  let depth = 0;
  let bodyEnd = -1;
  for (let index = bodyStart; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") {
      depth += 1;
      continue;
    }
    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        bodyEnd = index + 1;
        break;
      }
    }
  }

  if (bodyEnd < 0) {
    throw new Error(`Function "${functionName}" body end was not found`);
  }

  return source.slice(match.index, bodyEnd);
}

function loadShouldWriteLegacyStateOnMiniApproval() {
  const source = fs.readFileSync(SERVER_FILE, "utf8");
  const snippet = extractNamedFunctionSource(source, "shouldWriteLegacyStateOnMiniApproval");
  const scriptSource = `
${snippet}
module.exports = { shouldWriteLegacyStateOnMiniApproval };
`;
  const sandbox = {
    module: { exports: {} },
    exports: {},
  };
  vm.runInNewContext(scriptSource, sandbox, {
    filename: "server-mini-approve-read-path-invariant.vm.js",
  });
  return sandbox.module.exports.shouldWriteLegacyStateOnMiniApproval;
}

test("mini approval writes legacy when WRITE_V2 is disabled", () => {
  const shouldWriteLegacyStateOnMiniApproval = loadShouldWriteLegacyStateOnMiniApproval();
  assert.equal(
    shouldWriteLegacyStateOnMiniApproval({
      writeV2Enabled: false,
      readV2Enabled: false,
      legacyMirrorEnabled: false,
    }),
    true,
  );
});

test("mini approval forces legacy write when read path is legacy", () => {
  const shouldWriteLegacyStateOnMiniApproval = loadShouldWriteLegacyStateOnMiniApproval();
  assert.equal(
    shouldWriteLegacyStateOnMiniApproval({
      writeV2Enabled: true,
      readV2Enabled: false,
      legacyMirrorEnabled: false,
    }),
    true,
  );
});

test("mini approval keeps legacy write when both read and mirror are enabled", () => {
  const shouldWriteLegacyStateOnMiniApproval = loadShouldWriteLegacyStateOnMiniApproval();
  assert.equal(
    shouldWriteLegacyStateOnMiniApproval({
      writeV2Enabled: true,
      readV2Enabled: true,
      legacyMirrorEnabled: true,
    }),
    true,
  );
});

test("mini approval can skip legacy write only on full v2 read path without mirror", () => {
  const shouldWriteLegacyStateOnMiniApproval = loadShouldWriteLegacyStateOnMiniApproval();
  assert.equal(
    shouldWriteLegacyStateOnMiniApproval({
      writeV2Enabled: true,
      readV2Enabled: true,
      legacyMirrorEnabled: false,
    }),
    false,
  );
});
