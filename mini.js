"use strict";

const form = document.querySelector("#mini-client-form");
const message = document.querySelector("#mini-message");
const submitButton = document.querySelector("#mini-submit-button");
const closedByInput = document.querySelector("#closedBy");
const payment1DateInput = document.querySelector("#payment1Date");
const telegramApp = window.Telegram?.WebApp || null;

let initData = "";
let telegramUser = null;

initializeTelegramContext();
initializeDateField(payment1DateInput);
setDefaultDateIfEmpty(payment1DateInput);

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
      setDefaultDateIfEmpty(payment1DateInput);
      setMessage("Submitted for moderation. Client will appear after approval.", "success");
      showSubmissionPopup();
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

function initializeDateField(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  input.addEventListener("input", () => {
    const previousPosition = input.selectionStart ?? input.value.length;
    const hadSlashBeforeCursor = input.value.slice(0, previousPosition).includes("/");
    const formatted = formatDateInputValue(input.value);
    input.value = formatted;

    if (formatted.length <= previousPosition || hadSlashBeforeCursor) {
      return;
    }

    input.setSelectionRange(formatted.length, formatted.length);
  });

  const trigger = document.querySelector(`.date-picker-trigger[data-date-target="${input.id}"]`);
  const proxy = document.querySelector(`.date-picker-proxy[data-date-proxy-for="${input.id}"]`);
  if (!(trigger instanceof HTMLButtonElement) || !(proxy instanceof HTMLInputElement)) {
    return;
  }

  trigger.addEventListener("click", () => {
    const currentDate = parseUsDateToIso(input.value);
    if (currentDate) {
      proxy.value = currentDate;
    } else if (!proxy.value) {
      proxy.value = getTodayDateIso();
    }

    if (typeof proxy.showPicker === "function") {
      proxy.showPicker();
      return;
    }

    proxy.focus();
    proxy.click();
  });

  proxy.addEventListener("change", () => {
    const dateValue = (proxy.value || "").trim();
    if (!dateValue) {
      return;
    }

    input.value = formatIsoToUsDate(dateValue);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
}

function setDefaultDateIfEmpty(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  if (input.value.trim()) {
    return;
  }

  input.value = formatIsoToUsDate(getTodayDateIso());
}

function formatDateInputValue(rawValue) {
  const digits = (rawValue || "").replace(/\D/g, "").slice(0, 8);
  if (digits.length <= 2) {
    return digits;
  }

  if (digits.length <= 4) {
    return `${digits.slice(0, 2)}/${digits.slice(2)}`;
  }

  return `${digits.slice(0, 2)}/${digits.slice(2, 4)}/${digits.slice(4)}`;
}

function formatIsoToUsDate(isoDate) {
  const match = (isoDate || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return "";
  }

  return `${match[2]}/${match[3]}/${match[1]}`;
}

function parseUsDateToIso(usDate) {
  const match = (usDate || "").match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{4})$/);
  if (!match) {
    return "";
  }

  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  if (!isValidDateParts(year, month, day)) {
    return "";
  }

  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function getTodayDateIso() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const year = String(now.getFullYear());
  return `${year}-${month}-${day}`;
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(year, month - 1, day);
  return date.getFullYear() === year && date.getMonth() === month - 1 && date.getDate() === day;
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

function showSubmissionPopup() {
  const popupMessage = "Клиент отправлен на модерацию. Он появится после подтверждения.";

  if (telegramApp && typeof telegramApp.showPopup === "function") {
    try {
      telegramApp.showPopup({
        title: "Клиент отправлен",
        message: popupMessage,
        buttons: [{ type: "ok", text: "ОК" }],
      });
      return;
    } catch {
      // Continue to fallback.
    }
  }

  if (telegramApp && typeof telegramApp.showAlert === "function") {
    try {
      telegramApp.showAlert(popupMessage);
      return;
    } catch {
      // Continue to browser fallback.
    }
  }

  if (typeof window !== "undefined" && typeof window.alert === "function") {
    window.alert(popupMessage);
  }
}
