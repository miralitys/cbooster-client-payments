"use strict";

const form = document.querySelector("#mini-client-form");
const message = document.querySelector("#mini-message");
const submitButton = document.querySelector("#mini-submit-button");
const payment1DateInput = document.querySelector("#payment1Date");
const ssnInput = document.querySelector("#ssn");
const clientPhoneInput = document.querySelector("#clientPhoneNumber");
const clientEmailInput = document.querySelector("#clientEmailAddress");
const attachmentsInput = document.querySelector("#attachments");
const attachmentsPreview = document.querySelector("#attachments-preview");
const telegramApp = window.Telegram?.WebApp || null;
const MAX_ATTACHMENTS_COUNT = 10;
const BLOCKED_ATTACHMENT_EXTENSIONS = new Set([
  ".html",
  ".htm",
  ".xhtml",
  ".shtml",
  ".js",
  ".mjs",
  ".cjs",
  ".jsx",
  ".ts",
  ".tsx",
  ".sh",
  ".bash",
  ".zsh",
  ".bat",
  ".cmd",
  ".ps1",
  ".py",
  ".rb",
  ".php",
  ".pl",
  ".cgi",
]);

let initData = "";
let isMiniAccessAllowed = false;

initializeDateField(payment1DateInput);
initializeSsnField(ssnInput);
initializePhoneField(clientPhoneInput);
initializeEmailField(clientEmailInput);
setDefaultDateIfEmpty(payment1DateInput);
setSubmittingState(true);
void initializeTelegramContext();
initializeAttachmentsInput();

if (form) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setMessage("", "");

    if (!initData) {
      setMessage("Open this page from Telegram Mini App.", "error");
      return;
    }

    if (!isMiniAccessAllowed) {
      setMessage("Access denied. Only members of the allowed Telegram group can submit clients.", "error");
      return;
    }

    const formData = new FormData(form);
    if (!normalizeValue(formData.get("clientName"))) {
      setMessage("Client Name is required.", "error");
      return;
    }

    const ssnValidation = validateSsnField();
    if (!ssnValidation.ok) {
      setMessage("SSN must match XXX-XX-XXXX.", "error");
      return;
    }

    const phoneValidation = validatePhoneField();
    if (!phoneValidation.ok) {
      setMessage("Client Phone Number must match +1(XXX)XXX-XXXX.", "error");
      return;
    }

    const emailValidation = validateEmailField();
    if (!emailValidation.ok) {
      setMessage("Client Email Address must include @.", "error");
      return;
    }

    const payload = buildPayload();
    if (payload.attachmentsError) {
      setMessage(payload.attachmentsError, "error");
      return;
    }

    setSubmittingState(true);

    try {
      const formPayload = buildMultipartPayload(payload);
      const response = await fetch("/api/mini/clients", {
        method: "POST",
        headers: {
          Accept: "application/json",
        },
        body: formPayload,
      });

      const responseBody = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(responseBody.error || `Request failed (${response.status})`);
      }

      form.reset();
      setInputInvalidState(ssnInput, false);
      setInputInvalidState(clientPhoneInput, false);
      setInputInvalidState(clientEmailInput, false);
      setDefaultDateIfEmpty(payment1DateInput);
      renderAttachmentsPreview([]);
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

async function initializeTelegramContext() {
  if (!telegramApp) {
    setMessage("Telegram WebApp SDK is not available. Open this page in Telegram.", "error");
    return;
  }

  telegramApp.ready();
  telegramApp.expand();
  initData = (telegramApp.initData || "").toString().trim();

  if (!initData) {
    setMessage("Telegram auth data is missing. Reopen Mini App from bot menu.", "error");
    return;
  }

  await verifyMiniAccess();
}

async function verifyMiniAccess() {
  if (!initData) {
    return;
  }

  try {
    const response = await fetch("/api/mini/access", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ initData }),
    });

    const responseBody = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(responseBody.error || `Access check failed (${response.status})`);
    }

    isMiniAccessAllowed = true;
    setSubmittingState(false);
  } catch (error) {
    isMiniAccessAllowed = false;
    setSubmittingState(true);
    setMessage(error.message || "Access denied for Mini App.", "error");
  }
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

function initializeSsnField(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  input.addEventListener("input", () => {
    const formatted = formatSsnInputValue(input.value);
    input.value = formatted;
    const hasValue = Boolean(formatted.trim());
    setInputInvalidState(input, hasValue && !isValidSsnFormat(formatted));
  });

  input.addEventListener("blur", () => {
    validateSsnField();
  });
}

function formatSsnInputValue(rawValue) {
  const digits = (rawValue || "").replace(/\D/g, "").slice(0, 9);
  if (digits.length <= 3) {
    return digits;
  }

  if (digits.length <= 5) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }

  return `${digits.slice(0, 3)}-${digits.slice(3, 5)}-${digits.slice(5)}`;
}

function isValidSsnFormat(rawValue) {
  return /^\d{3}-\d{2}-\d{4}$/.test((rawValue || "").trim());
}

function validateSsnField() {
  if (!(ssnInput instanceof HTMLInputElement)) {
    return {
      ok: true,
      value: "",
    };
  }

  const rawValue = (ssnInput.value || "").trim();
  if (!rawValue) {
    setInputInvalidState(ssnInput, false);
    return {
      ok: true,
      value: "",
    };
  }

  const formatted = formatSsnInputValue(rawValue);
  ssnInput.value = formatted;
  const isValid = isValidSsnFormat(formatted);
  setInputInvalidState(ssnInput, !isValid);

  return {
    ok: isValid,
    value: formatted,
  };
}

function initializePhoneField(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  input.addEventListener("input", () => {
    const formatted = formatUsPhoneInputValue(input.value);
    input.value = formatted;
    const hasValue = Boolean(formatted.trim());
    setInputInvalidState(input, hasValue && !isValidUsPhoneFormat(formatted));
  });

  input.addEventListener("blur", () => {
    validatePhoneField();
  });
}

function formatUsPhoneInputValue(rawValue) {
  let digits = (rawValue || "").replace(/\D/g, "");
  if (digits.startsWith("1") && digits.length > 10) {
    digits = digits.slice(1);
  }
  digits = digits.slice(0, 10);

  if (!digits.length) {
    return "";
  }

  let result = "+1(";
  result += digits.slice(0, 3);

  if (digits.length >= 3) {
    result += ")";
  }

  if (digits.length > 3) {
    result += digits.slice(3, 6);
  }

  if (digits.length > 6) {
    result += `-${digits.slice(6, 10)}`;
  }

  return result;
}

function isValidUsPhoneFormat(rawValue) {
  return /^\+1\(\d{3}\)\d{3}-\d{4}$/.test((rawValue || "").trim());
}

function validatePhoneField() {
  if (!(clientPhoneInput instanceof HTMLInputElement)) {
    return {
      ok: true,
      value: "",
    };
  }

  const rawValue = (clientPhoneInput.value || "").trim();
  if (!rawValue) {
    setInputInvalidState(clientPhoneInput, false);
    return {
      ok: true,
      value: "",
    };
  }

  const formatted = formatUsPhoneInputValue(rawValue);
  clientPhoneInput.value = formatted;
  const isValid = isValidUsPhoneFormat(formatted);
  setInputInvalidState(clientPhoneInput, !isValid);

  return {
    ok: isValid,
    value: formatted,
  };
}

function initializeEmailField(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  input.addEventListener("input", () => {
    const value = (input.value || "").trim();
    setInputInvalidState(input, Boolean(value) && !isValidEmailWithAt(value));
  });

  input.addEventListener("blur", () => {
    validateEmailField();
  });
}

function isValidEmailWithAt(rawValue) {
  return (rawValue || "").includes("@");
}

function validateEmailField() {
  if (!(clientEmailInput instanceof HTMLInputElement)) {
    return {
      ok: true,
      value: "",
    };
  }

  const value = (clientEmailInput.value || "").trim();
  clientEmailInput.value = value;
  if (!value) {
    setInputInvalidState(clientEmailInput, false);
    return {
      ok: true,
      value: "",
    };
  }

  const isValid = isValidEmailWithAt(value);
  setInputInvalidState(clientEmailInput, !isValid);
  return {
    ok: isValid,
    value,
  };
}

function setInputInvalidState(input, hasError) {
  if (!(input instanceof HTMLElement)) {
    return;
  }

  input.classList.toggle("input-invalid", Boolean(hasError));
  input.setAttribute("aria-invalid", hasError ? "true" : "false");
}

function buildPayload() {
  const formData = new FormData(form);
  const client = {
    clientName: normalizeValue(formData.get("clientName")),
    closedBy: normalizeValue(formData.get("closedBy")),
    leadSource: normalizeValue(formData.get("leadSource")),
    ssn: normalizeValue(formData.get("ssn")),
    clientPhoneNumber: normalizeValue(formData.get("clientPhoneNumber")),
    futurePayment: normalizeValue(formData.get("futurePayment")),
    identityIq: normalizeValue(formData.get("identityIq")),
    clientEmailAddress: normalizeValue(formData.get("clientEmailAddress")),
    companyName: normalizeValue(formData.get("companyName")),
    serviceType: normalizeValue(formData.get("serviceType")),
    contractTotals: normalizeValue(formData.get("contractTotals")),
    payment1: normalizeValue(formData.get("payment1")),
    payment1Date: normalizeValue(formData.get("payment1Date")),
    notes: normalizeValue(formData.get("notes")),
    afterResult: formData.get("afterResult") === "on",
  };

  const attachmentsValidation = validateSelectedAttachments();

  return {
    initData,
    client,
    attachments: attachmentsValidation.files,
    attachmentsError: attachmentsValidation.error,
  };
}

function normalizeValue(rawValue) {
  return (rawValue || "").toString().trim();
}

function buildMultipartPayload(payload) {
  const multipart = new FormData();
  multipart.append("initData", (payload?.initData || "").toString());
  multipart.append("client", JSON.stringify(payload?.client || {}));

  const attachments = Array.isArray(payload?.attachments) ? payload.attachments : [];
  for (const file of attachments) {
    multipart.append("attachments", file, file.name);
  }

  return multipart;
}

function initializeAttachmentsInput() {
  if (!(attachmentsInput instanceof HTMLInputElement)) {
    return;
  }

  attachmentsInput.addEventListener("change", () => {
    const validation = validateSelectedAttachments();
    renderAttachmentsPreview(validation.files);
    if (validation.error) {
      setMessage(validation.error, "error");
      return;
    }

    if (message?.classList.contains("error")) {
      setMessage("", "");
    }
  });

  renderAttachmentsPreview([]);
}

function validateSelectedAttachments() {
  if (!(attachmentsInput instanceof HTMLInputElement)) {
    return {
      files: [],
      error: "",
    };
  }

  const selectedFiles = Array.from(attachmentsInput.files || []);
  if (!selectedFiles.length) {
    return {
      files: [],
      error: "",
    };
  }

  const limitedFiles = selectedFiles.slice(0, MAX_ATTACHMENTS_COUNT);
  if (selectedFiles.length > MAX_ATTACHMENTS_COUNT) {
    replaceSelectedAttachments(limitedFiles);
    return {
      files: limitedFiles,
      error: `You can upload up to ${MAX_ATTACHMENTS_COUNT} files.`,
    };
  }

  for (const file of limitedFiles) {
    const name = (file?.name || "").toString().trim().toLowerCase();
    const extension = getFileExtension(name);
    if (BLOCKED_ATTACHMENT_EXTENSIONS.has(extension)) {
      replaceSelectedAttachments([]);
      return {
        files: [],
        error: `File "${file.name}" is not allowed. Script and HTML files are blocked.`,
      };
    }
  }

  return {
    files: limitedFiles,
    error: "",
  };
}

function replaceSelectedAttachments(files) {
  if (!(attachmentsInput instanceof HTMLInputElement) || typeof DataTransfer === "undefined") {
    return;
  }

  const transfer = new DataTransfer();
  for (const file of files) {
    transfer.items.add(file);
  }
  attachmentsInput.files = transfer.files;
}

function getFileExtension(fileName) {
  const lastDotIndex = fileName.lastIndexOf(".");
  if (lastDotIndex <= 0 || lastDotIndex === fileName.length - 1) {
    return "";
  }

  return fileName.slice(lastDotIndex).toLowerCase();
}

function renderAttachmentsPreview(files) {
  if (!attachmentsPreview) {
    return;
  }

  if (!files.length) {
    attachmentsPreview.hidden = true;
    attachmentsPreview.replaceChildren();
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const file of files) {
    const item = document.createElement("div");
    item.className = "attachments-preview__item";
    item.textContent = file.name || "attachment";

    const meta = document.createElement("span");
    meta.className = "attachments-preview__meta";
    meta.textContent = `(${formatBytes(file.size || 0)})`;
    item.append(meta);

    fragment.append(item);
  }

  attachmentsPreview.hidden = false;
  attachmentsPreview.replaceChildren(fragment);
}

function formatBytes(value) {
  const bytes = Number.parseInt(value, 10);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const KB = 1024;
  const MB = 1024 * 1024;
  if (bytes >= MB) {
    return `${(bytes / MB).toFixed(bytes >= 10 * MB ? 0 : 1)} MB`;
  }

  if (bytes >= KB) {
    return `${Math.round(bytes / KB)} KB`;
  }

  return `${bytes} B`;
}

function setSubmittingState(isSubmitting) {
  if (submitButton) {
    submitButton.disabled = Boolean(isSubmitting);
  }

  if (attachmentsInput) {
    attachmentsInput.disabled = Boolean(isSubmitting);
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
