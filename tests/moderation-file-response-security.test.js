"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
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

function loadSetAttachmentResponseSecurityHeaders() {
  const source = fs.readFileSync(SERVER_FILE, "utf8");
  const snippet = extractNamedFunctionSource(source, "setAttachmentResponseSecurityHeaders");

  const scriptSource = `
${snippet}
module.exports = {
  setAttachmentResponseSecurityHeaders,
};
`;

  const sandbox = {
    module: { exports: {} },
    exports: {},
  };

  vm.runInNewContext(scriptSource, sandbox, {
    filename: "server-attachment-security-headers.vm.js",
  });

  return sandbox.module.exports.setAttachmentResponseSecurityHeaders;
}

function createResponseDouble() {
  const headers = new Map();
  return {
    headers,
    setHeader(name, value) {
      headers.set(String(name || "").toLowerCase(), String(value || ""));
    },
  };
}

test("setAttachmentResponseSecurityHeaders sets nosniff for all attachment responses", () => {
  const applyHeaders = loadSetAttachmentResponseSecurityHeaders();
  const response = createResponseDouble();

  applyHeaders(response, {
    isInline: false,
  });

  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.equal(response.headers.has("content-security-policy"), false);
});

test("setAttachmentResponseSecurityHeaders adds sandbox CSP for inline preview", () => {
  const applyHeaders = loadSetAttachmentResponseSecurityHeaders();
  const response = createResponseDouble();

  applyHeaders(response, {
    isInline: true,
  });

  const csp = response.headers.get("content-security-policy");
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.equal(typeof csp, "string");
  assert.ok(csp.includes("sandbox"));
  assert.ok(csp.includes("default-src 'none'"));
});

test("setAttachmentResponseSecurityHeaders tolerates invalid response object", () => {
  const applyHeaders = loadSetAttachmentResponseSecurityHeaders();
  assert.doesNotThrow(() => {
    applyHeaders(null, {
      isInline: true,
    });
  });
});
