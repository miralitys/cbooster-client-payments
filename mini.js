"use strict";

const form = document.querySelector("#mini-client-form");
const message = document.querySelector("#mini-message");
const submitButton = document.querySelector("#mini-submit-button");
const closedByInput = document.querySelector("#closedBy");
const telegramApp = window.Telegram?.WebApp || null;

let initData = "";
let telegramUser = null;

initializeTelegramContext();

if (form) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setMessage("", "");

    if (!initData) {
      setMessage("Open this page from Telegram Mini App.", "error");
      return;
    }

    const payload = buildPayload();
    if (!payload.client.clientName) {
      setMessage("Client Name is required.", "error");
      return;
    }

    setSubmittingState(true);

    try {
      const response = await fetch("/api/mini/clients", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
      });

      const responseBody = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(responseBody.error || `Request failed (${response.status})`);
      }

      form.reset();
      prefillClosedBy();
      setMessage("Submitted for moderation. Client will appear after approval.", "success");
      telegramApp?.HapticFeedback?.notificationOccurred?.("success");
    } catch (error) {
      setMessage(error.message || "Failed to add client.", "error");
      telegramApp?.HapticFeedback?.notificationOccurred?.("error");
    } finally {
      setSubmittingState(false);
    }
  });
}

function initializeTelegramContext() {
  if (!telegramApp) {
    setMessage("Telegram WebApp SDK is not available. Open this page in Telegram.", "error");
    return;
  }

  telegramApp.ready();
  telegramApp.expand();
  initData = (telegramApp.initData || "").toString().trim();
  telegramUser = telegramApp.initDataUnsafe?.user || null;

  if (!initData) {
    setMessage("Telegram auth data is missing. Reopen Mini App from bot menu.", "error");
    return;
  }

  prefillClosedBy();
}

function prefillClosedBy() {
  if (!closedByInput || !telegramUser || closedByInput.value.trim()) {
    return;
  }

  const username = (telegramUser.username || "").toString().trim();
  if (username) {
    closedByInput.value = `@${username}`;
    return;
  }

  const firstName = (telegramUser.first_name || "").toString().trim();
  const lastName = (telegramUser.last_name || "").toString().trim();
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    closedByInput.value = fullName;
    return;
  }

  if (telegramUser.id) {
    closedByInput.value = `tg:${telegramUser.id}`;
  }
}

function buildPayload() {
  const formData = new FormData(form);
  const client = {
    clientName: normalizeValue(formData.get("clientName")),
    closedBy: normalizeValue(formData.get("closedBy")),
    companyName: normalizeValue(formData.get("companyName")),
    serviceType: normalizeValue(formData.get("serviceType")),
    contractTotals: normalizeValue(formData.get("contractTotals")),
    payment1: normalizeValue(formData.get("payment1")),
    payment1Date: normalizeValue(formData.get("payment1Date")),
    notes: normalizeValue(formData.get("notes")),
    afterResult: formData.get("afterResult") === "on",
    writtenOff: formData.get("writtenOff") === "on",
  };

  return {
    initData,
    client,
  };
}

function normalizeValue(rawValue) {
  return (rawValue || "").toString().trim();
}

function setSubmittingState(isSubmitting) {
  if (submitButton) {
    submitButton.disabled = Boolean(isSubmitting);
  }
}

function setMessage(text, tone) {
  if (!message) {
    return;
  }

  message.textContent = text;
  message.className = `message ${tone || ""}`.trim();
}
