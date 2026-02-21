// @vitest-environment jsdom

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const MINI_JS_PATH = path.resolve(TEST_DIR, "../../../../mini.js");

const MINI_HTML = `
  <form id="mini-client-form">
    <input id="clientName" name="clientName" />
    <input id="closedBy" name="closedBy" />
    <input id="companyName" name="companyName" />
    <input id="serviceType" name="serviceType" />
    <input id="contractTotals" name="contractTotals" />
    <input id="payment1" name="payment1" />
    <input id="payment1Date" name="payment1Date" />
    <input id="ssn" name="ssn" />
    <input id="clientPhoneNumber" name="clientPhoneNumber" />
    <input id="clientEmailAddress" name="clientEmailAddress" />
    <input id="attachments" name="attachments" type="file" multiple />
    <button id="attachments-upload-button" type="button">Upload</button>
    <button id="mini-submit-button" type="submit"><span class="button-label">Add Client</span></button>
  </form>
  <div id="attachments-preview"></div>
  <div id="mini-message"></div>
`;

const MINI_EXPORTS = `
module.exports = {
  validateRequiredMiniFields,
  validatePayment1DateField,
  validateSsnField,
  validatePhoneField,
  validateEmailField,
};
`;

let miniSource = "";

beforeAll(() => {
  miniSource = fs.readFileSync(MINI_JS_PATH, "utf8");
});

type MiniValidators = {
  validateRequiredMiniFields: () => { ok: boolean; error: string };
  validatePayment1DateField: () => { ok: boolean };
  validateSsnField: () => { ok: boolean; value: string };
  validatePhoneField: () => { ok: boolean; value: string };
  validateEmailField: () => { ok: boolean; value: string };
};

type VmSandbox = Record<string, unknown> & {
  module: { exports: unknown };
  exports: Record<string, unknown>;
  globalThis?: unknown;
};

function loadMiniValidators() {
  document.body.innerHTML = MINI_HTML;

  const sandbox: VmSandbox = {
    module: { exports: {} },
    exports: {},
    window,
    document,
    console,
    fetch: async () => ({ ok: false, status: 503, json: async () => ({}) }),
    FormData,
    DataTransfer: globalThis.DataTransfer,
    File: globalThis.File,
    Event,
    URLSearchParams,
    URL,
    navigator,
    setTimeout,
    clearTimeout,
    requestAnimationFrame: (callback: FrameRequestCallback) => {
      callback(0);
      return 0;
    },
    cancelAnimationFrame: () => {},
    HTMLInputElement,
    HTMLTextAreaElement,
    HTMLButtonElement,
    HTMLElement,
    alert: () => {},
  };

  sandbox.globalThis = sandbox;

  vm.runInNewContext(`${miniSource}\n${MINI_EXPORTS}`, sandbox, {
    filename: "mini.validators.test.vm.js",
  });

  return sandbox.module.exports as MiniValidators;
}

describe("mini.js validators", () => {
  it("validateRequiredMiniFields marks missing required input", () => {
    const validators = loadMiniValidators();
    const clientName = document.querySelector("#clientName") as HTMLInputElement;
    const closedBy = document.querySelector("#closedBy") as HTMLInputElement;
    const companyName = document.querySelector("#companyName") as HTMLInputElement;
    const serviceType = document.querySelector("#serviceType") as HTMLInputElement;
    const contractTotals = document.querySelector("#contractTotals") as HTMLInputElement;
    const payment1 = document.querySelector("#payment1") as HTMLInputElement;
    const payment1Date = document.querySelector("#payment1Date") as HTMLInputElement;

    clientName.value = "   ";
    const result = validators.validateRequiredMiniFields();

    expect(result).toEqual({
      ok: false,
      error: "Client Name is required.",
    });
    expect(clientName.getAttribute("aria-invalid")).toBe("true");

    clientName.value = "  Jane Doe  ";
    closedBy.value = "Manager";
    companyName.value = "Credit Booster";
    serviceType.value = "Repair";
    contractTotals.value = "1000";
    payment1.value = "200";
    payment1Date.value = "02/20/2026";
    const validResult = validators.validateRequiredMiniFields();
    expect(validResult).toEqual({ ok: true, error: "" });
    expect(clientName.getAttribute("aria-invalid")).toBe("false");
  });

  it("validatePayment1DateField accepts valid and rejects invalid dates", () => {
    const validators = loadMiniValidators();
    const paymentDate = document.querySelector("#payment1Date") as HTMLInputElement;

    paymentDate.value = "";
    expect(validators.validatePayment1DateField()).toEqual({ ok: true });
    expect(paymentDate.getAttribute("aria-invalid")).toBe("false");

    paymentDate.value = "02/30/2026";
    expect(validators.validatePayment1DateField()).toEqual({ ok: false });
    expect(paymentDate.getAttribute("aria-invalid")).toBe("true");

    paymentDate.value = "02/29/2024";
    expect(validators.validatePayment1DateField()).toEqual({ ok: true });
    expect(paymentDate.getAttribute("aria-invalid")).toBe("false");
  });

  it("validateSsnField normalizes valid SSN and rejects invalid format", () => {
    const validators = loadMiniValidators();
    const ssn = document.querySelector("#ssn") as HTMLInputElement;

    ssn.value = "123456789";
    expect(validators.validateSsnField()).toEqual({ ok: true, value: "123-45-6789" });
    expect(ssn.value).toBe("123-45-6789");
    expect(ssn.getAttribute("aria-invalid")).toBe("false");

    ssn.value = "123-45-678";
    expect(validators.validateSsnField()).toEqual({ ok: false, value: "123-45-678" });
    expect(ssn.getAttribute("aria-invalid")).toBe("true");

    ssn.value = "   ";
    expect(validators.validateSsnField()).toEqual({ ok: true, value: "" });
    expect(ssn.getAttribute("aria-invalid")).toBe("false");
  });

  it("validatePhoneField formats valid phone and rejects invalid values", () => {
    const validators = loadMiniValidators();
    const phone = document.querySelector("#clientPhoneNumber") as HTMLInputElement;

    phone.value = "1 (312) 555-7890";
    expect(validators.validatePhoneField()).toEqual({ ok: true, value: "+1(312)555-7890" });
    expect(phone.value).toBe("+1(312)555-7890");
    expect(phone.getAttribute("aria-invalid")).toBe("false");

    phone.value = "+1(312)555-789";
    expect(validators.validatePhoneField()).toEqual({ ok: false, value: "+1(312)555-789" });
    expect(phone.getAttribute("aria-invalid")).toBe("true");

    phone.value = "   ";
    expect(validators.validatePhoneField()).toEqual({ ok: true, value: "" });
    expect(phone.getAttribute("aria-invalid")).toBe("false");
  });

  it("validateEmailField trims value and requires @", () => {
    const validators = loadMiniValidators();
    const email = document.querySelector("#clientEmailAddress") as HTMLInputElement;

    email.value = "  user@example.com  ";
    expect(validators.validateEmailField()).toEqual({ ok: true, value: "user@example.com" });
    expect(email.value).toBe("user@example.com");
    expect(email.getAttribute("aria-invalid")).toBe("false");

    email.value = "invalid-email";
    expect(validators.validateEmailField()).toEqual({ ok: false, value: "invalid-email" });
    expect(email.getAttribute("aria-invalid")).toBe("true");

    email.value = "   ";
    expect(validators.validateEmailField()).toEqual({ ok: true, value: "" });
    expect(email.getAttribute("aria-invalid")).toBe("false");
  });
});
