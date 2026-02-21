"use strict";

const originalFetch = globalThis.fetch;

function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

function textResponse(status, bodyText) {
  return new Response(String(bodyText || ""), {
    status,
    headers: {
      "Content-Type": "text/plain",
    },
  });
}

function buildMatrixResponse(urlString) {
  const url = new URL(urlString);
  const userId = String(url.searchParams.get("user_id") || "");

  if (userId === "9001") {
    throw new Error("Simulated Telegram network failure");
  }
  if (userId === "9002") {
    return jsonResponse(403, {
      ok: false,
      description: "Forbidden: user is not a chat member",
    });
  }
  if (userId === "9003") {
    return jsonResponse(200, {
      ok: true,
      result: {
        status: "kicked",
      },
    });
  }
  if (userId === "9004") {
    return jsonResponse(200, {
      ok: true,
      result: {
        status: "member",
      },
    });
  }
  if (userId === "9005") {
    return textResponse(500, "Telegram upstream failure");
  }

  return jsonResponse(200, {
    ok: true,
    result: {
      status: "member",
    },
  });
}

globalThis.fetch = async function patchedFetch(input, init) {
  const urlString = typeof input === "string" ? input : input?.url || "";
  const isTelegramRequest = urlString.startsWith("https://api.telegram.org/");
  if (!isTelegramRequest) {
    return originalFetch(input, init);
  }

  const mode = String(process.env.TEST_TELEGRAM_FETCH_MODE || "").trim();
  if (mode === "network_error") {
    throw new Error("Simulated Telegram network error");
  }
  if (mode === "http_403") {
    return jsonResponse(403, { ok: false, description: "Forbidden" });
  }
  if (mode === "http_500") {
    return textResponse(500, "Telegram server error");
  }
  if (mode === "status_kicked") {
    return jsonResponse(200, { ok: true, result: { status: "kicked" } });
  }
  if (mode === "status_member") {
    return jsonResponse(200, { ok: true, result: { status: "member" } });
  }
  if (mode === "telegram_matrix") {
    return buildMatrixResponse(urlString);
  }

  return originalFetch(input, init);
};

