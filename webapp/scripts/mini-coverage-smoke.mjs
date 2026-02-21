import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { JSDOM } from "jsdom";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, "../..");
const MINI_JS_PATH = path.join(REPO_ROOT, "mini.js");

const MINI_HTML = `
  <form id="mini-client-form">
    <input id="clientName" name="clientName" />
    <input id="closedBy" name="closedBy" />
    <input id="leadSource" name="leadSource" />
    <input id="payment1Date" name="payment1Date" />
    <input id="ssn" name="ssn" />
    <input id="clientPhoneNumber" name="clientPhoneNumber" />
    <input id="futurePayment" name="futurePayment" />
    <input id="identityIq" name="identityIq" />
    <input id="clientEmailAddress" name="clientEmailAddress" />
    <input id="companyName" name="companyName" />
    <input id="serviceType" name="serviceType" />
    <input id="contractTotals" name="contractTotals" />
    <input id="payment1" name="payment1" />
    <textarea id="notes" name="notes"></textarea>
    <input id="afterResult" name="afterResult" type="checkbox" />
    <input id="attachments" name="attachments" type="file" multiple />
    <button id="attachments-upload-button" type="button">Upload</button>
    <button id="mini-access-retry-button" type="button">Retry access</button>
    <button class="date-picker-trigger" data-date-target="payment1Date" type="button">Pick date</button>
    <input class="date-picker-proxy" data-date-proxy-for="payment1Date" type="date" />
    <button id="mini-submit-button" type="submit">
      <span class="button-label">Add Client</span>
    </button>
  </form>
  <div id="attachments-preview"></div>
  <div id="mini-message"></div>
`;

const miniSource = fs.readFileSync(MINI_JS_PATH, "utf8");
const dom = new JSDOM(MINI_HTML, {
  url: "https://example.test/mini",
});

const { window } = dom;
const { document } = window;

let accessChecks = 0;
let submitAttempts = 0;
let popupCalls = 0;
let telegramAlertCalls = 0;
let browserAlertCalls = 0;
let popupBehavior = "throw";
let alertBehavior = "ok";
let failNextAccessAsRecoverable = false;

const submitResponseQueue = [
  { ok: true, status: 201, body: { ok: true } },
  { ok: true, status: 201, body: { ok: true } },
  { ok: false, status: 500, body: { error: "submit failed" } },
];

const realSetTimeout = globalThis.setTimeout;
const realClearTimeout = globalThis.clearTimeout;
let nextTimerId = 1;
const timerHandles = new Map();

function sandboxSetTimeout(callback, delayMs = 0) {
  const timerId = nextTimerId++;
  const handle = realSetTimeout(() => {
    timerHandles.delete(timerId);
    callback();
  }, Math.min(Number(delayMs) || 0, 1));
  timerHandles.set(timerId, handle);
  return timerId;
}

function sandboxClearTimeout(timerId) {
  const handle = timerHandles.get(timerId);
  if (!handle) {
    return;
  }
  realClearTimeout(handle);
  timerHandles.delete(timerId);
}

async function fetchMock(input) {
  const url = typeof input === "string" ? input : input?.toString?.() || "";

  if (url.includes("/api/mini/access")) {
    accessChecks += 1;
    if (failNextAccessAsRecoverable) {
      failNextAccessAsRecoverable = false;
      return {
        ok: false,
        status: 503,
        json: async () => ({
          error: "Temporary access issue",
          code: "failed_to_fetch",
        }),
      };
    }

    return {
      ok: true,
      status: 200,
      json: async () => ({
        ok: true,
        user: { id: 777 },
        uploadToken: "mini-upload-token",
      }),
    };
  }

  if (url.includes("/api/mini/clients")) {
    submitAttempts += 1;
    const responseConfig = submitResponseQueue.shift() || {
      ok: false,
      status: 500,
      body: { error: "submit failed" },
    };

    return {
      ok: responseConfig.ok,
      status: responseConfig.status,
      json: async () => responseConfig.body,
    };
  }

  return {
    ok: false,
    status: 404,
    json: async () => ({}),
  };
}

const telegramWebApp = {
  initData: "signed_init_data_for_coverage",
  ready: () => {},
  expand: () => {},
  HapticFeedback: {
    notificationOccurred: () => {},
  },
  showPopup: () => {
    popupCalls += 1;
    if (popupBehavior === "throw") {
      throw new Error("showPopup failed");
    }
  },
  showAlert: () => {
    telegramAlertCalls += 1;
    if (alertBehavior === "throw") {
      throw new Error("showAlert failed");
    }
  },
};

window.Telegram = { WebApp: telegramWebApp };
window.alert = () => {
  browserAlertCalls += 1;
};

const sandbox = {
  module: { exports: {} },
  exports: {},
  window,
  document,
  console,
  fetch: fetchMock,
  FormData: window.FormData,
  DataTransfer: window.DataTransfer,
  File: window.File,
  Event: window.Event,
  URLSearchParams: window.URLSearchParams,
  URL: window.URL,
  navigator: window.navigator,
  setTimeout: sandboxSetTimeout,
  clearTimeout: sandboxClearTimeout,
  requestAnimationFrame: (callback) => {
    callback(0);
    return 0;
  },
  cancelAnimationFrame: () => {},
  HTMLInputElement: window.HTMLInputElement,
  HTMLTextAreaElement: window.HTMLTextAreaElement,
  HTMLButtonElement: window.HTMLButtonElement,
  HTMLFormElement: window.HTMLFormElement,
  HTMLElement: window.HTMLElement,
  alert: window.alert,
};
sandbox.globalThis = sandbox;

vm.runInNewContext(miniSource, sandbox, {
  filename: MINI_JS_PATH,
});

async function flushAsync(times = 4) {
  for (let index = 0; index < times; index += 1) {
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

function getInput(id) {
  const input = document.querySelector(`#${id}`);
  if (!input) {
    throw new Error(`Missing input #${id}`);
  }
  return input;
}

async function submitForm() {
  const form = document.querySelector("#mini-client-form");
  assert.ok(form, "mini form is required");
  form.dispatchEvent(new window.Event("submit", { bubbles: true, cancelable: true }));
  await flushAsync();
}

function setCommonValidValues() {
  getInput("clientName").value = "Jane Smith";
  getInput("closedBy").value = "Closer";
  getInput("companyName").value = "Company";
  getInput("serviceType").value = "Service";
  getInput("contractTotals").value = "1000";
  getInput("payment1").value = "500";
  getInput("payment1Date").value = "02/28/2026";
}

function dispatchInputEvent(id, value) {
  const input = getInput(id);
  input.value = value;
  input.dispatchEvent(new window.Event("input", { bubbles: true }));
}

function dispatchAttachmentsChange(files) {
  const attachmentsInput = getInput("attachments");
  Object.defineProperty(attachmentsInput, "files", {
    configurable: true,
    value: files,
  });
  attachmentsInput.dispatchEvent(new window.Event("change", { bubbles: true }));
}

function getRetryButton() {
  const retryButton = document.querySelector("#mini-access-retry-button");
  assert.ok(retryButton instanceof window.HTMLButtonElement, "mini access retry button is required");
  return retryButton;
}

function currentMessage() {
  const element = document.querySelector("#mini-message");
  assert.ok(element, "mini message element is required");
  return (element.textContent || "").trim();
}

await flushAsync(8);
assert.ok(accessChecks >= 1, "mini access check should run on startup");

dispatchInputEvent("payment1Date", "02292024");
assert.equal(getInput("payment1Date").value, "02/29/2024");

const dateProxy = document.querySelector('.date-picker-proxy[data-date-proxy-for="payment1Date"]');
assert.ok(dateProxy instanceof window.HTMLInputElement, "date proxy is required");
dateProxy.value = "2026-02-21";
dateProxy.dispatchEvent(new window.Event("change", { bubbles: true }));
assert.equal(getInput("payment1Date").value, "02/21/2026");

dispatchInputEvent("clientPhoneNumber", "12223334444");
await flushAsync();
assert.match(getInput("clientPhoneNumber").value, /^\+1\(\d{3}\)\d{3}-\d{4}$/);

await submitForm();
assert.match(currentMessage(), /required/i);

setCommonValidValues();
getInput("payment1Date").value = "02/30/2026";
await submitForm();
assert.match(currentMessage(), /MM\/DD\/YYYY/i);

setCommonValidValues();
getInput("ssn").value = "123-45-678";
await submitForm();
assert.match(currentMessage(), /SSN/i);

setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "12345";
await submitForm();
assert.match(currentMessage(), /Phone Number/i);

setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "+1(222)333-4444";
getInput("clientEmailAddress").value = "bad-email";
await submitForm();
assert.match(currentMessage(), /Email Address/i);

setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "+1(222)333-4444";
getInput("clientEmailAddress").value = "jane@example.com";
await submitForm();
assert.match(currentMessage(), /Submitted for moderation/i);
assert.equal(submitAttempts, 1);
assert.equal(popupCalls, 1);
assert.equal(telegramAlertCalls, 1);
assert.equal(browserAlertCalls, 0);

alertBehavior = "throw";
setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "+1(222)333-4444";
getInput("clientEmailAddress").value = "jane@example.com";
await submitForm();
assert.match(currentMessage(), /Submitted for moderation/i);
assert.equal(submitAttempts, 2);
assert.equal(popupCalls, 2);
assert.equal(telegramAlertCalls, 2);
assert.equal(browserAlertCalls, 1);

setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "+1(222)333-4444";
getInput("clientEmailAddress").value = "jane@example.com";
await submitForm();
assert.match(currentMessage(), /submit failed/i);
assert.equal(submitAttempts, 3);

dispatchAttachmentsChange([
  new window.File(["photo"], "safe.png", { type: "image/png" }),
]);
assert.match(getInput("attachments-preview").textContent || "", /safe\.png/i);

dispatchAttachmentsChange([
  new window.File(["x"], "script.js", { type: "text/javascript" }),
]);
assert.match(currentMessage(), /not allowed/i);

dispatchAttachmentsChange(
  Array.from({ length: 11 }, (_, index) => (
    new window.File([String(index)], `file-${index}.png`, { type: "image/png" })
  )),
);
assert.match(currentMessage(), /up to 10 files/i);

failNextAccessAsRecoverable = true;
getRetryButton().click();
await flushAsync(10);
assert.ok(accessChecks >= 3, "recoverable access failure should trigger retry");

document.querySelector("#mini-message")?.remove();
getInput("clientName").value = "";
await submitForm();
