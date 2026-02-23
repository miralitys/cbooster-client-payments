"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const PROJECT_ROOT = path.resolve(__dirname, "..");

function collectRouteSignatures(app) {
  const layers = Array.isArray(app?._router?.stack) ? app._router.stack : [];
  const signatures = new Set();

  for (const layer of layers) {
    const route = layer?.route;
    if (!route || typeof route.path !== "string") {
      continue;
    }

    const methods = Object.entries(route.methods || {})
      .filter(([, enabled]) => enabled === true)
      .map(([method]) => method.toUpperCase());

    for (const method of methods) {
      signatures.add(`${method} ${route.path}`);
    }
  }

  return signatures;
}

test("backend architecture smoke: production route registry keeps critical endpoints", () => {
  process.env.NODE_ENV = process.env.NODE_ENV || "test";
  process.env.SERVER_AUTOSTART_IN_TEST = "false";

  const serverModule = require("../server.js");
  assert.ok(serverModule);
  assert.equal(typeof serverModule.startServer, "function");
  assert.ok(serverModule.app);

  const signatures = collectRouteSignatures(serverModule.app);

  const expected = [
    "GET /api/records",
    "PUT /api/records",
    "PATCH /api/records",
    "GET /api/clients",
    "PUT /api/clients",
    "PATCH /api/clients",
    "GET /api/quickbooks/payments/recent",
    "POST /api/mini/clients",
    "GET /api/moderation/submissions",
    "POST /api/assistant/chat",
  ];

  for (const routeSignature of expected) {
    assert.equal(
      signatures.has(routeSignature),
      true,
      `Missing critical route signature: ${routeSignature}`,
    );
  }
});

test("backend architecture smoke: assistant routes are wired via dedicated router module", () => {
  const legacyServerPath = path.join(PROJECT_ROOT, "server-legacy.js");
  const legacySource = fs.readFileSync(legacyServerPath, "utf8");

  assert.match(
    legacySource,
    /require\(["']\.\/server\/routes\/assistant\.routes["']\)/,
    "server-legacy.js must import assistant routes module",
  );

  assert.match(
    legacySource,
    /registerAssistantRoutes\s*\(/,
    "server-legacy.js must register assistant routes through registerAssistantRoutes",
  );
});
