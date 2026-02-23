"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { once } = require("node:events");
const express = require("express");
const { registerAssistantRoutes } = require("../server/routes/assistant.routes");

const PERMISSIONS = {
  WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS: "view_client_payments",
  WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL: "manage_access_control",
};

async function createAssistantRoutesTestServer() {
  const app = express();
  app.use(express.json());

  const calls = {
    chat: 0,
    reviews: 0,
  };

  const requireWebPermission = (requiredPermission) => (req, res, next) => {
    const grantedPermission = String(req.headers["x-allow-permission"] || "").trim();
    if (grantedPermission === requiredPermission) {
      next();
      return;
    }

    res.status(403).json({
      error: "Access denied.",
      permission: requiredPermission,
    });
  };

  registerAssistantRoutes({
    app,
    requireWebPermission,
    permissionKeys: PERMISSIONS,
    handlers: {
      handleAssistantContextResetPost: (_req, res) => res.json({ ok: true }),
      handleAssistantContextResetTelemetryPost: (_req, res) => res.json({ ok: true }),
      handleAssistantChatPost: (_req, res) => {
        calls.chat += 1;
        res.json({
          ok: true,
          reply: "fake-assistant-reply",
        });
      },
      handleAssistantReviewsList: (_req, res) => {
        calls.reviews += 1;
        res.json({
          ok: true,
          items: [],
        });
      },
      handleAssistantReviewUpdate: (_req, res) => res.json({ ok: true }),
      handleAssistantTtsPost: (_req, res) => res.json({ ok: true }),
    },
  });

  const server = app.listen(0);
  if (!server.listening) {
    await once(server, "listening");
  }
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;

  return {
    server,
    baseUrl: `http://127.0.0.1:${port}`,
    calls,
  };
}

test("assistant routes smoke: POST /api/assistant/chat returns fake response through new router", async (t) => {
  const instance = await createAssistantRoutesTestServer();
  t.after(() => {
    instance.server.close();
  });

  const response = await fetch(`${instance.baseUrl}/api/assistant/chat`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-allow-permission": PERMISSIONS.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS,
    },
    body: JSON.stringify({
      message: "hello",
    }),
  });

  const body = await response.json();
  assert.equal(response.status, 200);
  assert.equal(body?.ok, true);
  assert.equal(body?.reply, "fake-assistant-reply");
  assert.equal(instance.calls.chat, 1);
});

test("assistant routes smoke: GET /api/assistant/reviews is permission-gated", async (t) => {
  const instance = await createAssistantRoutesTestServer();
  t.after(() => {
    instance.server.close();
  });

  const deniedResponse = await fetch(`${instance.baseUrl}/api/assistant/reviews`, {
    method: "GET",
    headers: {
      accept: "application/json",
    },
  });
  const deniedBody = await deniedResponse.json();
  assert.equal(deniedResponse.status, 403);
  assert.equal(deniedBody?.permission, PERMISSIONS.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL);
  assert.equal(instance.calls.reviews, 0);

  const allowedResponse = await fetch(`${instance.baseUrl}/api/assistant/reviews`, {
    method: "GET",
    headers: {
      accept: "application/json",
      "x-allow-permission": PERMISSIONS.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL,
    },
  });
  const allowedBody = await allowedResponse.json();
  assert.equal(allowedResponse.status, 200);
  assert.equal(allowedBody?.ok, true);
  assert.equal(Array.isArray(allowedBody?.items), true);
  assert.equal(instance.calls.reviews, 1);
});
