"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const express = require("express");
const { registerAssistantRoutes } = require("../server/routes/assistant.routes");

function createNoopHandler() {
  return (_req, _res, next) => {
    if (typeof next === "function") {
      next();
    }
  };
}

function getAssistantRouteSignatures(app) {
  const layers = Array.isArray(app?._router?.stack) ? app._router.stack : [];
  const signatures = [];

  for (const layer of layers) {
    const route = layer?.route;
    if (!route || typeof route.path !== "string" || !route.path.startsWith("/api/assistant/")) {
      continue;
    }

    const methods = Object.entries(route.methods || {})
      .filter(([, enabled]) => enabled === true)
      .map(([method]) => method.toUpperCase())
      .sort();

    for (const method of methods) {
      signatures.push(`${method} ${route.path}`);
    }
  }

  return signatures.sort();
}

test("assistant routes stack includes expected /api/assistant/* endpoints only", () => {
  const app = express();

  registerAssistantRoutes({
    app,
    requireWebPermission: () => createNoopHandler(),
    permissionKeys: {
      WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS: "view_client_payments",
      WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL: "manage_access_control",
    },
    handlers: {
      handleAssistantContextResetPost: createNoopHandler(),
      handleAssistantContextResetTelemetryPost: createNoopHandler(),
      handleAssistantChatPost: createNoopHandler(),
      handleAssistantReviewsList: createNoopHandler(),
      handleAssistantReviewUpdate: createNoopHandler(),
      handleAssistantTtsPost: createNoopHandler(),
    },
  });

  const expected = [
    "GET /api/assistant/reviews",
    "POST /api/assistant/chat",
    "POST /api/assistant/context/reset",
    "POST /api/assistant/context/reset/telemetry",
    "POST /api/assistant/tts",
    "PUT /api/assistant/reviews/:id",
  ].sort();

  const actual = getAssistantRouteSignatures(app);
  assert.deepEqual(actual, expected);
});
