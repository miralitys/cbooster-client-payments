"use strict";

const form = document.querySelector("#mini-client-form");
const message = document.querySelector("#mini-message");
const accessRetryButton = document.querySelector("#mini-access-retry-button");
const submitButton = document.querySelector("#mini-submit-button");
const submitButtonLabel = submitButton?.querySelector(".button-label") || null;
const payment1DateInput = document.querySelector("#payment1Date");
const ssnInput = document.querySelector("#ssn");
const clientPhoneInput = document.querySelector("#clientPhoneNumber");
const clientEmailInput = document.querySelector("#clientEmailAddress");
const attachmentsInput = document.querySelector("#attachments");
const attachmentsUploadButton = document.querySelector("#attachments-upload-button");
const attachmentsPreview = document.querySelector("#attachments-preview");
const telegramApp = window.Telegram?.WebApp || null;
const MAX_ATTACHMENTS_COUNT = 10;
const REQUIRED_MINI_FIELDS = [
  { id: "clientName", label: "Client Name" },
  { id: "closedBy", label: "Closed By" },
  { id: "companyName", label: "Company Name" },
  { id: "serviceType", label: "Service Type" },
  { id: "contractTotals", label: "Contract Totals" },
  { id: "payment1", label: "Payment 1" },
  { id: "payment1Date", label: "Payment 1 Date" },
];
const DEFAULT_SUBMIT_BUTTON_LABEL = "Add Client";
const MINI_ACCESS_RETRY_BASE_DELAY_MS = 1200;
const MINI_ACCESS_RETRY_MAX_DELAY_MS = 12000;
const MINI_ACCESS_RETRY_MAX_ATTEMPTS = 4;
const MINI_ACCESS_FETCH_TIMEOUT_MS = 8000;
const MINI_ATTACHMENTS_DEFAULT_HELP_TEXT =
  "Allowed formats: images, PDF, DOC/DOCX, XLS/XLSX, PPT/PPTX, TXT, CSV, RTF.";
const FALLBACK_BLOCKED_ATTACHMENT_EXTENSIONS = new Set([
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
let isSubmitting = false;
let isAccessCheckInProgress = true;
let miniUploadToken = "";
let hasRecoverableAccessError = false;
let miniAccessRetryAttempt = 0;
let miniAccessRetryTimeoutId = 0;
let miniAttachmentAllowlistExtensions = null;
let miniAttachmentMaxCount = MAX_ATTACHMENTS_COUNT;
let miniAttachmentAllowedFormatsHelpText = MINI_ATTACHMENTS_DEFAULT_HELP_TEXT;

initializeDateField(payment1DateInput);
initializeSsnField(ssnInput);
initializePhoneField(clientPhoneInput);
initializeEmailField(clientEmailInput);
initializeRequiredFieldValidation();
initializeAttachmentsInput();
updateFormInteractivity();
void initializeTelegramContext();

if (form) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setMessage("", "");

    if (!initData) {
      setMessage("Open this page from Telegram Mini App.", "error");
      return;
    }

    if (!isMiniAccessAllowed) {
      if (hasRecoverableAccessError) {
        setMessage("Access check is temporarily unavailable. Tap Retry access.", "error");
        return;
      }
      setMessage("Access denied. Only members of the allowed Telegram group can submit clients.", "error");
      return;
    }

    if (!miniUploadToken) {
      await verifyMiniAccess({ quiet: true });
      if (!miniUploadToken) {
        if (hasRecoverableAccessError) {
          setMessage("Access check is temporarily unavailable. Tap Retry access.", "error");
          return;
        }
        setMessage("Session expired. Reopen Mini App and try again.", "error");
        return;
      }
    }

    const requiredValidation = validateRequiredMiniFields();
    if (!requiredValidation.ok) {
      setMessage(requiredValidation.error, "error");
      return;
    }

    const payment1DateValidation = validatePayment1DateField();
    if (!payment1DateValidation.ok) {
      setMessage("Payment 1 Date must be valid MM/DD/YYYY.", "error");
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
          "X-Mini-Upload-Token": miniUploadToken,
        },
        body: formPayload,
      });

      const responseBody = await response.json().catch(() => ({}));
      if (!response.ok) {
        const responseCode = normalizeValue(responseBody?.code);
        if (
          response.status === 401 &&
          responseCode.startsWith("mini_upload_token_")
        ) {
          miniUploadToken = "";
          await verifyMiniAccess({ quiet: true });
          if (miniUploadToken) {
            throw new Error("Session refreshed. Tap Add Client again.");
          }
          throw new Error("Session expired. Reopen Mini App and try again.");
        }
        throw new Error(responseBody.error || `Request failed (${response.status})`);
      }

      form.reset();
      clearMiniValidationStates();
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

if (accessRetryButton instanceof HTMLButtonElement) {
  accessRetryButton.addEventListener("click", () => {
    if (isAccessCheckInProgress || isSubmitting || !initData) {
      return;
    }

    clearMiniAccessRetryTimer();
    miniAccessRetryAttempt = 0;
    void verifyMiniAccess();
  });
}

async function initializeTelegramContext() {
  if (!telegramApp) {
    isAccessCheckInProgress = false;
    updateFormInteractivity();
    setMessage("Telegram WebApp SDK is not available. Open this page in Telegram.", "error");
    return;
  }

  telegramApp.ready();
  telegramApp.expand();
  initData = (telegramApp.initData || "").toString().trim();

  if (!initData) {
    isAccessCheckInProgress = false;
    updateFormInteractivity();
    setMessage("Telegram auth data is missing. Reopen Mini App from bot menu.", "error");
    return;
  }

  await verifyMiniAccess();
}

async function verifyMiniAccess(options = {}) {
  const quiet = options?.quiet === true;
  if (!initData) {
    isAccessCheckInProgress = false;
    updateFormInteractivity();
    return;
  }

  isAccessCheckInProgress = true;
  updateFormInteractivity();

  try {
    const response = await fetchMiniAccessWithTimeout();

    const responseBody = await response.json().catch(() => ({}));
    if (!response.ok) {
      const accessError = new Error(responseBody.error || `Access check failed (${response.status})`);
      accessError.httpStatus = response.status;
      accessError.code = normalizeValue(responseBody?.code);
      throw accessError;
    }

    miniUploadToken = normalizeValue(responseBody?.uploadToken);
    if (!miniUploadToken) {
      throw new Error("Mini upload token is missing. Reopen Mini App.");
    }
    applyMiniAttachmentsConfig(responseBody?.miniConfig?.attachments);

    isMiniAccessAllowed = true;
    if (hasRecoverableAccessError) {
      setMessage("", "");
    }
    clearMiniAccessRetryTimer();
    hasRecoverableAccessError = false;
    miniAccessRetryAttempt = 0;
    isAccessCheckInProgress = false;
    updateFormInteractivity();
  } catch (error) {
    miniUploadToken = "";
    isMiniAccessAllowed = false;
    isAccessCheckInProgress = false;
    const hadRecoverableAccessError = hasRecoverableAccessError;
    const recoverableError = isRecoverableMiniAccessFailure(error);
    if (recoverableError) {
      hasRecoverableAccessError = true;
      scheduleMiniAccessRetry();
    } else {
      clearMiniAccessRetryTimer();
      hasRecoverableAccessError = false;
      miniAccessRetryAttempt = 0;
    }
    updateFormInteractivity();
    if (quiet && hadRecoverableAccessError && !recoverableError) {
      setMessage(error.message || "Access denied for Mini App.", "error");
    }
    if (!quiet) {
      if (recoverableError) {
        setMessage("Temporary access issue. We will retry automatically. Tap Retry access to try now.", "error");
      } else {
        setMessage(error.message || "Access denied for Mini App.", "error");
      }
    }
  }
}

async function fetchMiniAccessWithTimeout() {
  const requestOptions = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({ initData }),
  };

  const supportsAbortController = typeof AbortController === "function";
  const abortController = supportsAbortController ? new AbortController() : null;
  let timedOut = false;
  let timeoutId = 0;
  const fetchPromise = fetch("/api/mini/access", {
    ...requestOptions,
    ...(abortController ? { signal: abortController.signal } : {}),
  });

  const timeoutPromise = new Promise((_, reject) => {
    timeoutId = setTimeout(() => {
      timedOut = true;
      abortController?.abort();
      reject(createMiniAccessTimeoutError());
    }, MINI_ACCESS_FETCH_TIMEOUT_MS);
  });

  try {
    return await Promise.race([fetchPromise, timeoutPromise]);
  } catch (error) {
    if (timedOut) {
      void fetchPromise.catch(() => {});
      throw createMiniAccessTimeoutError();
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

function createMiniAccessTimeoutError() {
  const timeoutError = new Error("Access check timed out. Please retry.");
  timeoutError.name = "AbortError";
  timeoutError.code = "mini_access_timeout";
  timeoutError.httpStatus = 408;
  return timeoutError;
}

function applyMiniAttachmentsConfig(rawConfig) {
  const allowlist = parseMiniAttachmentAllowlistExtensions(rawConfig?.allowedExtensions);
  miniAttachmentAllowlistExtensions = allowlist;

  const maxCount = Number.parseInt(rawConfig?.maxCount, 10);
  if (Number.isFinite(maxCount) && maxCount > 0 && maxCount <= 20) {
    miniAttachmentMaxCount = maxCount;
  } else {
    miniAttachmentMaxCount = MAX_ATTACHMENTS_COUNT;
  }

  const helpText = normalizeValue(rawConfig?.allowedFormatsHelpText);
  miniAttachmentAllowedFormatsHelpText = helpText || MINI_ATTACHMENTS_DEFAULT_HELP_TEXT;
}

function parseMiniAttachmentAllowlistExtensions(rawExtensions) {
  if (!Array.isArray(rawExtensions)) {
    return null;
  }

  const normalizedExtensions = rawExtensions
    .map((value) => normalizeAttachmentExtension(value))
    .filter(Boolean);
  if (!normalizedExtensions.length) {
    return null;
  }

  return new Set(normalizedExtensions);
}

function normalizeAttachmentExtension(rawValue) {
  const value = normalizeValue(rawValue).toLowerCase().replace(/^\.+/, "");
  if (!value || !/^[a-z0-9]{1,12}$/.test(value)) {
    return "";
  }

  return `.${value}`;
}

function clearMiniAccessRetryTimer() {
  if (miniAccessRetryTimeoutId) {
    clearTimeout(miniAccessRetryTimeoutId);
    miniAccessRetryTimeoutId = 0;
  }
}

function scheduleMiniAccessRetry() {
  if (!hasRecoverableAccessError || isMiniAccessAllowed || isAccessCheckInProgress) {
    return;
  }
  if (miniAccessRetryAttempt >= MINI_ACCESS_RETRY_MAX_ATTEMPTS) {
    return;
  }

  clearMiniAccessRetryTimer();
  miniAccessRetryAttempt += 1;
  const delayMs = resolveMiniAccessRetryDelayMs(miniAccessRetryAttempt);
  miniAccessRetryTimeoutId = setTimeout(() => {
    miniAccessRetryTimeoutId = 0;
    if (isMiniAccessAllowed || isAccessCheckInProgress || isSubmitting) {
      return;
    }
    void verifyMiniAccess({ quiet: true });
  }, delayMs);
}

function resolveMiniAccessRetryDelayMs(attemptNumber) {
  const safeAttempt = Math.max(1, Number.parseInt(attemptNumber, 10) || 1);
  const exponentialDelay = Math.min(
    MINI_ACCESS_RETRY_MAX_DELAY_MS,
    MINI_ACCESS_RETRY_BASE_DELAY_MS * 2 ** (safeAttempt - 1),
  );
  const jitterMax = Math.max(40, Math.floor(exponentialDelay * 0.25));
  const jitter = Math.floor(Math.random() * jitterMax);
  return Math.min(MINI_ACCESS_RETRY_MAX_DELAY_MS, exponentialDelay + jitter);
}

function isRecoverableMiniAccessFailure(error) {
  const status = Number.parseInt(error?.httpStatus, 10);
  if (new Set([408, 425, 429, 500, 502, 503, 504]).has(status)) {
    return true;
  }

  const errorCode = normalizeValue(error?.code);
  if (new Set(["failed_to_fetch", "networkerror", "mini_access_timeout"]).has(errorCode)) {
    return true;
  }

  const errorName = normalizeValue(error?.name);
  if (errorName === "typeerror" || errorName === "aborterror") {
    return true;
  }

  const errorMessage = normalizeValue(error?.message);
  return errorMessage.includes("network") || errorMessage.includes("failed to fetch") || errorMessage.includes("load failed");
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

function getRequiredMiniInput(fieldId) {
  const input = document.querySelector(`#${fieldId}`);
  return input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement ? input : null;
}

function initializeRequiredFieldValidation() {
  for (const field of REQUIRED_MINI_FIELDS) {
    const input = getRequiredMiniInput(field.id);
    if (!input) {
      continue;
    }

    input.addEventListener("input", () => {
      if (normalizeValue(input.value)) {
        setInputInvalidState(input, false);
      }
      updateFormInteractivity();
    });
  }
}

function hasAllRequiredMiniFields() {
  for (const field of REQUIRED_MINI_FIELDS) {
    const input = getRequiredMiniInput(field.id);
    if (!normalizeValue(input?.value)) {
      return false;
    }
  }

  return true;
}

function validateRequiredMiniFields() {
  let firstMissingLabel = "";

  for (const field of REQUIRED_MINI_FIELDS) {
    const input = getRequiredMiniInput(field.id);
    const value = normalizeValue(input?.value);
    const isMissing = !value;
    if (input) {
      setInputInvalidState(input, isMissing);
    }
    if (!firstMissingLabel && isMissing) {
      firstMissingLabel = field.label;
    }
  }

  if (firstMissingLabel) {
    return {
      ok: false,
      error: `${firstMissingLabel} is required.`,
    };
  }

  return {
    ok: true,
    error: "",
  };
}

function validatePayment1DateField() {
  if (!(payment1DateInput instanceof HTMLInputElement)) {
    return {
      ok: true,
    };
  }

  const value = normalizeValue(payment1DateInput.value);
  if (!value) {
    setInputInvalidState(payment1DateInput, false);
    return {
      ok: true,
    };
  }

  const isValid = Boolean(parseUsDateToIso(value));
  setInputInvalidState(payment1DateInput, !isValid);
  return {
    ok: isValid,
  };
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
    moveCaretToEnd(input);
    const hasValue = Boolean(formatted.trim());
    setInputInvalidState(input, hasValue && !isValidUsPhoneFormat(formatted));
  });

  input.addEventListener("blur", () => {
    validatePhoneField();
  });
}

function formatUsPhoneInputValue(rawValue) {
  const rawText = (rawValue || "").toString().trim();
  let digits = rawText.replace(/\D/g, "");

  // Keep +1 as a fixed country prefix and strip it from editable local digits.
  const hasMaskedCountryPrefix = rawText.includes("+1(") || rawText.startsWith("+1");
  if (hasMaskedCountryPrefix && digits.startsWith("1")) {
    digits = digits.slice(1);
  }

  // Support pasted NANP numbers like 1XXXXXXXXXX.
  if (digits.length > 10 && digits.startsWith("1")) {
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

function moveCaretToEnd(input) {
  if (!(input instanceof HTMLInputElement)) {
    return;
  }

  const position = input.value.length;
  requestAnimationFrame(() => {
    if (document.activeElement !== input) {
      return;
    }
    input.setSelectionRange(position, position);
  });
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

function clearMiniValidationStates() {
  for (const field of REQUIRED_MINI_FIELDS) {
    setInputInvalidState(getRequiredMiniInput(field.id), false);
  }
  setInputInvalidState(ssnInput, false);
  setInputInvalidState(clientPhoneInput, false);
  setInputInvalidState(clientEmailInput, false);
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

  if (attachmentsUploadButton instanceof HTMLButtonElement) {
    attachmentsUploadButton.addEventListener("click", () => {
      if (attachmentsInput.disabled) {
        return;
      }
      attachmentsInput.click();
    });
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

  const maxAttachmentsCount = Math.max(1, Number.parseInt(miniAttachmentMaxCount, 10) || MAX_ATTACHMENTS_COUNT);
  const limitedFiles = selectedFiles.slice(0, maxAttachmentsCount);
  if (selectedFiles.length > maxAttachmentsCount) {
    replaceSelectedAttachments(limitedFiles);
    return {
      files: limitedFiles,
      error: `You can upload up to ${maxAttachmentsCount} files.`,
    };
  }

  for (const file of limitedFiles) {
    const name = (file?.name || "").toString().trim().toLowerCase();
    const extension = getFileExtension(name);
    const blockReason = resolveAttachmentBlockReason(file.name || "attachment", extension);
    if (blockReason) {
      replaceSelectedAttachments([]);
      return {
        files: [],
        error: blockReason,
      };
    }
  }

  return {
    files: limitedFiles,
    error: "",
  };
}

function resolveAttachmentBlockReason(fileName, extension) {
  if (miniAttachmentAllowlistExtensions instanceof Set && miniAttachmentAllowlistExtensions.size) {
    if (!miniAttachmentAllowlistExtensions.has(extension)) {
      return `File "${fileName}" is not allowed. ${miniAttachmentAllowedFormatsHelpText}`;
    }
    return "";
  }

  if (FALLBACK_BLOCKED_ATTACHMENT_EXTENSIONS.has(extension)) {
    return `File "${fileName}" is not allowed. Script and HTML files are blocked.`;
  }

  return "";
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

function setSubmittingState(nextSubmittingState) {
  isSubmitting = Boolean(nextSubmittingState);
  updateFormInteractivity();
}

function updateFormInteractivity() {
  const canSubmit =
    !isSubmitting && !isAccessCheckInProgress && isMiniAccessAllowed && hasAllRequiredMiniFields();
  const showAccessRetryButton = !isMiniAccessAllowed && hasRecoverableAccessError;

  if (submitButton) {
    submitButton.disabled = !canSubmit;
    submitButton.classList.toggle("is-loading", isSubmitting);
    submitButton.setAttribute("aria-busy", isSubmitting ? "true" : "false");
  }

  if (submitButtonLabel) {
    submitButtonLabel.textContent = isSubmitting ? "Submitting..." : DEFAULT_SUBMIT_BUTTON_LABEL;
  }

  if (attachmentsInput) {
    attachmentsInput.disabled = isSubmitting;
  }

  if (attachmentsUploadButton instanceof HTMLButtonElement) {
    attachmentsUploadButton.disabled = isSubmitting;
  }

  if (accessRetryButton instanceof HTMLButtonElement) {
    accessRetryButton.hidden = !showAccessRetryButton;
    accessRetryButton.disabled = isSubmitting || isAccessCheckInProgress;
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
