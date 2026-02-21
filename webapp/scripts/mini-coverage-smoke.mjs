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

async function fetchMock(input) {
  const url = typeof input === "string" ? input : input?.toString?.() || "";

  if (url.includes("/api/mini/access")) {
    accessChecks += 1;
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

    if (submitAttempts === 1) {
      return {
        ok: true,
        status: 201,
        json: async () => ({ ok: true }),
      };
    }

    return {
      ok: false,
      status: 500,
      json: async () => ({ error: "submit failed" }),
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
  showPopup: () => {},
  showAlert: () => {},
};

window.Telegram = { WebApp: telegramWebApp };

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
  setTimeout: window.setTimeout.bind(window),
  clearTimeout: window.clearTimeout.bind(window),
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
  alert: () => {},
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

function currentMessage() {
  const element = document.querySelector("#mini-message");
  assert.ok(element, "mini message element is required");
  return (element.textContent || "").trim();
}

await flushAsync(8);
assert.ok(accessChecks >= 1, "mini access check should run on startup");

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

setCommonValidValues();
getInput("ssn").value = "123-45-6789";
getInput("clientPhoneNumber").value = "+1(222)333-4444";
getInput("clientEmailAddress").value = "jane@example.com";
await submitForm();
assert.match(currentMessage(), /submit failed/i);
assert.equal(submitAttempts, 2);
