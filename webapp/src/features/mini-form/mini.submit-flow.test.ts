// @vitest-environment jsdom

import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, describe, expect, it } from "vitest";

const TEST_DIR = path.dirname(fileURLToPath(import.meta.url));
const MINI_JS_PATH = path.resolve(TEST_DIR, "../../../../mini.js");

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
    <button id="mini-submit-button" type="submit"><span class="button-label">Add Client</span></button>
  </form>
  <div id="attachments-preview"></div>
  <div id="mini-message"></div>
`;

type FetchResponseConfig = {
  ok: boolean;
  status: number;
  body?: unknown;
};

type FetchCall = {
  url: string;
  init: RequestInit | undefined;
};

type TelegramWebAppStub = {
  initData?: string;
  ready: () => void;
  expand: () => void;
  HapticFeedback?: {
    notificationOccurred?: (type: string) => void;
  };
  showPopup?: (params: unknown) => void;
  showAlert?: (message: string) => void;
};

type TelegramHarness = {
  webApp: TelegramWebAppStub;
  readyCalls: number;
  expandCalls: number;
  hapticCalls: string[];
  popupCalls: unknown[];
  alertCalls: string[];
  notificationOrder: string[];
};

type TelegramWebAppOptions = {
  showPopup?: "ok" | "throw" | "omit";
  showAlert?: "ok" | "throw" | "omit";
};

type MiniAppHarness = {
  fetchCalls: FetchCall[];
};

type LoadMiniAppOptions = {
  telegramWebApp?: TelegramWebAppStub | null;
  fetchResponses?: FetchResponseConfig[];
  fetchMock?: (input: RequestInfo | URL, init?: RequestInit) => Promise<{
    ok: boolean;
    status: number;
    json: () => Promise<unknown>;
  }>;
};

type VmSandbox = Record<string, unknown> & {
  module: { exports: unknown };
  exports: Record<string, unknown>;
  globalThis?: unknown;
};

let miniSource = "";
const ORIGINAL_WINDOW_ALERT = window.alert;
const POPUP_FALLBACK_MESSAGE = "Клиент отправлен на модерацию. Он появится после подтверждения.";

beforeAll(() => {
  miniSource = fs.readFileSync(MINI_JS_PATH, "utf8");
});

afterEach(() => {
  const windowWithTelegram = window as Window & {
    Telegram?: {
      WebApp: TelegramWebAppStub;
    };
  };

  delete windowWithTelegram.Telegram;
  window.alert = ORIGINAL_WINDOW_ALERT;
  document.body.innerHTML = "";
});

function createTelegramWebApp(initData: string, options: TelegramWebAppOptions = {}): TelegramHarness {
  const hapticCalls: string[] = [];
  const popupCalls: unknown[] = [];
  const alertCalls: string[] = [];
  const notificationOrder: string[] = [];

  const harness: TelegramHarness = {
    readyCalls: 0,
    expandCalls: 0,
    hapticCalls,
    popupCalls,
    alertCalls,
    notificationOrder,
    webApp: {
      initData,
      ready: () => {
        harness.readyCalls += 1;
      },
      expand: () => {
        harness.expandCalls += 1;
      },
      HapticFeedback: {
        notificationOccurred: (type: string) => {
          hapticCalls.push(type);
        },
      },
    },
  };

  if (options.showPopup !== "omit") {
    harness.webApp.showPopup = (params: unknown) => {
      notificationOrder.push("showPopup");
      popupCalls.push(params);
      if (options.showPopup === "throw") {
        throw new Error("showPopup failed");
      }
    };
  }

  if (options.showAlert !== "omit") {
    harness.webApp.showAlert = (message: string) => {
      notificationOrder.push("showAlert");
      alertCalls.push(message);
      if (options.showAlert === "throw") {
        throw new Error("showAlert failed");
      }
    };
  }

  return harness;
}

function createFetchMock(fetchResponses: FetchResponseConfig[]) {
  const queue = [...fetchResponses];
  const fetchCalls: FetchCall[] = [];

  const fetchMock = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    fetchCalls.push({ url, init });

    const response = queue.shift();
    if (!response) {
      throw new Error(`Unexpected fetch call: ${url}`);
    }

    return {
      ok: response.ok,
      status: response.status,
      json: async () => response.body ?? {},
    };
  };

  return {
    fetchCalls,
    fetchMock,
  };
}

function toFetchResponse(response: FetchResponseConfig) {
  return {
    ok: response.ok,
    status: response.status,
    json: async () => response.body ?? {},
  };
}

function createDeferredFetchMock() {
  const fetchCalls: FetchCall[] = [];
  const pendingResolvers: Array<(response: ReturnType<typeof toFetchResponse>) => void> = [];

  const fetchMock = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    fetchCalls.push({ url, init });

    return await new Promise<ReturnType<typeof toFetchResponse>>((resolve) => {
      pendingResolvers.push(resolve);
    });
  };

  return {
    fetchCalls,
    fetchMock,
    resolveNext(response: FetchResponseConfig) {
      const resolver = pendingResolvers.shift();
      if (!resolver) {
        throw new Error("No pending fetch calls to resolve");
      }
      resolver(toFetchResponse(response));
    },
  };
}

function loadMiniApp(options: LoadMiniAppOptions = {}): MiniAppHarness {
  document.body.innerHTML = MINI_HTML;

  const windowWithTelegram = window as Window & {
    Telegram?: {
      WebApp: TelegramWebAppStub;
    };
  };

  if (options.telegramWebApp) {
    windowWithTelegram.Telegram = {
      WebApp: options.telegramWebApp,
    };
  } else {
    delete windowWithTelegram.Telegram;
  }

  const staticFetch = createFetchMock(options.fetchResponses ?? []);
  const fetchMock = options.fetchMock ?? staticFetch.fetchMock;

  const sandbox: VmSandbox = {
    module: { exports: {} },
    exports: {},
    window,
    document,
    console,
    fetch: fetchMock,
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
    HTMLFormElement,
    HTMLElement,
    alert: () => {},
  };

  sandbox.globalThis = sandbox;

  vm.runInNewContext(miniSource, sandbox, {
    filename: "mini.submit-flow.test.vm.js",
  });

  return {
    fetchCalls: staticFetch.fetchCalls,
  };
}

async function flushAsync(iterations = 4) {
  for (let index = 0; index < iterations; index += 1) {
    await Promise.resolve();
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

async function submitForm() {
  const form = document.querySelector("#mini-client-form");
  if (!(form instanceof HTMLFormElement)) {
    throw new Error("#mini-client-form was not found");
  }

  form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
  await flushAsync();
}

function getMessageText() {
  const message = document.querySelector("#mini-message");
  if (!(message instanceof HTMLElement)) {
    throw new Error("#mini-message was not found");
  }

  return message.textContent || "";
}

function getSubmitButton() {
  const submitButton = document.querySelector("#mini-submit-button");
  if (!(submitButton instanceof HTMLButtonElement)) {
    throw new Error("#mini-submit-button was not found");
  }

  return submitButton;
}

function getSubmitButtonLabel() {
  const label = document.querySelector("#mini-submit-button .button-label");
  if (!(label instanceof HTMLElement)) {
    throw new Error("Submit button label was not found");
  }

  return label;
}

function getRequiredClientNameInput() {
  const input = document.querySelector("#clientName");
  if (!(input instanceof HTMLInputElement)) {
    throw new Error("#clientName was not found");
  }

  return input;
}

function fillRequiredClientName(value = "John Doe") {
  const input = getRequiredClientNameInput();
  input.value = value;
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

describe("mini.js submit flow", () => {
  it("shows error when Telegram SDK is unavailable", async () => {
    const { fetchCalls } = loadMiniApp();
    await flushAsync();

    expect(getMessageText()).toBe("Telegram WebApp SDK is not available. Open this page in Telegram.");

    const submitButton = document.querySelector("#mini-submit-button") as HTMLButtonElement;
    expect(submitButton.disabled).toBe(true);
    expect(fetchCalls).toHaveLength(0);
  });

  it("blocks submit when initData is missing", async () => {
    const telegram = createTelegramWebApp("   ");
    const { fetchCalls } = loadMiniApp({
      telegramWebApp: telegram.webApp,
    });

    await flushAsync();

    expect(telegram.readyCalls).toBe(1);
    expect(telegram.expandCalls).toBe(1);
    expect(getMessageText()).toBe("Telegram auth data is missing. Reopen Mini App from bot menu.");

    const clientName = document.querySelector("#clientName") as HTMLInputElement;
    clientName.value = "John";
    await submitForm();

    expect(getMessageText()).toBe("Open this page from Telegram Mini App.");
    expect(fetchCalls).toHaveLength(0);
  });

  it("keeps user blocked when access is denied", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const { fetchCalls } = loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchResponses: [{ ok: false, status: 403, body: { error: "Access denied for Mini App." } }],
    });

    await flushAsync();

    expect(getMessageText()).toBe("Access denied for Mini App.");
    expect(fetchCalls).toHaveLength(1);
    expect(fetchCalls[0]?.url).toBe("/api/mini/access");

    const clientName = document.querySelector("#clientName") as HTMLInputElement;
    clientName.value = "John";
    await submitForm();

    expect(getMessageText()).toBe("Access denied. Only members of the allowed Telegram group can submit clients.");
    expect(fetchCalls).toHaveLength(1);
    expect(telegram.hapticCalls).toEqual([]);
  });

  it("shows API error and triggers error haptic on submit failure", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const { fetchCalls } = loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchResponses: [
        { ok: true, status: 200, body: { ok: true, uploadToken: "token-1" } },
        { ok: false, status: 500, body: { error: "Moderation service unavailable." } },
      ],
    });

    await flushAsync();

    const clientName = document.querySelector("#clientName") as HTMLInputElement;
    clientName.value = "  John Doe  ";

    await submitForm();

    expect(getMessageText()).toBe("Moderation service unavailable.");
    expect(telegram.hapticCalls).toContain("error");
    expect(fetchCalls).toHaveLength(2);
    expect(fetchCalls[1]?.url).toBe("/api/mini/clients");

    const submitHeaders = (fetchCalls[1]?.init?.headers ?? {}) as Record<string, string>;
    expect(submitHeaders["X-Mini-Upload-Token"]).toBe("token-1");

    const payload = fetchCalls[1]?.init?.body;
    if (!(payload instanceof FormData)) {
      throw new Error("submit payload must be FormData");
    }

    const clientRaw = payload.get("client");
    if (typeof clientRaw !== "string") {
      throw new Error("client payload must be serialized JSON string");
    }

    const parsedClient = JSON.parse(clientRaw) as {
      clientName?: string;
    };

    expect(parsedClient.clientName).toBe("John Doe");
  });

  it("submits successfully, resets form and triggers success haptic", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const { fetchCalls } = loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchResponses: [
        { ok: true, status: 200, body: { ok: true, uploadToken: "token-2" } },
        { ok: true, status: 201, body: { ok: true } },
      ],
    });

    await flushAsync();

    const clientName = document.querySelector("#clientName") as HTMLInputElement;
    const paymentDate = document.querySelector("#payment1Date") as HTMLInputElement;

    clientName.value = "  Jane Smith  ";
    paymentDate.value = "";

    await submitForm();

    expect(getMessageText()).toBe("Submitted for moderation. Client will appear after approval.");
    expect(telegram.hapticCalls).toContain("success");
    expect(telegram.popupCalls.length).toBeGreaterThanOrEqual(1);

    expect(clientName.value).toBe("");
    expect(paymentDate.value).toMatch(/^\d{2}\/\d{2}\/\d{4}$/);

    const payload = fetchCalls[1]?.init?.body;
    if (!(payload instanceof FormData)) {
      throw new Error("submit payload must be FormData");
    }

    expect(payload.get("initData")).toBe("auth_payload");

    const clientRaw = payload.get("client");
    if (typeof clientRaw !== "string") {
      throw new Error("client payload must be serialized JSON string");
    }

    const parsedClient = JSON.parse(clientRaw) as {
      clientName?: string;
      payment1Date?: string;
    };

    expect(parsedClient.clientName).toBe("Jane Smith");
    expect(parsedClient.payment1Date).toBe("");
  });

  it("keeps submit button disabled while mini access check is in progress", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const deferredFetch = createDeferredFetchMock();
    loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchMock: deferredFetch.fetchMock,
    });

    fillRequiredClientName("John");
    await flushAsync();

    const submitButton = getSubmitButton();
    expect(submitButton.disabled).toBe(true);
    expect(submitButton.getAttribute("aria-busy")).toBe("false");

    deferredFetch.resolveNext({
      ok: true,
      status: 200,
      body: { ok: true, uploadToken: "token-access-ready" },
    });
    await flushAsync();

    expect(submitButton.disabled).toBe(false);
    expect(getSubmitButtonLabel().textContent).toBe("Add Client");
  });

  it("disables submit button during in-flight submit and restores it afterwards", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const deferredFetch = createDeferredFetchMock();
    loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchMock: deferredFetch.fetchMock,
    });

    fillRequiredClientName("John");
    await flushAsync();

    deferredFetch.resolveNext({
      ok: true,
      status: 200,
      body: { ok: true, uploadToken: "token-submit" },
    });
    await flushAsync();

    const submitButton = getSubmitButton();
    expect(submitButton.disabled).toBe(false);

    submitButton.click();
    await flushAsync(1);

    expect(submitButton.disabled).toBe(true);
    expect(submitButton.getAttribute("aria-busy")).toBe("true");
    expect(submitButton.classList.contains("is-loading")).toBe(true);
    expect(getSubmitButtonLabel().textContent).toBe("Submitting...");

    deferredFetch.resolveNext({
      ok: false,
      status: 500,
      body: { error: "Submit failed." },
    });
    await flushAsync();

    expect(submitButton.disabled).toBe(false);
    expect(submitButton.getAttribute("aria-busy")).toBe("false");
    expect(submitButton.classList.contains("is-loading")).toBe(false);
    expect(getSubmitButtonLabel().textContent).toBe("Add Client");
    expect(getMessageText()).toBe("Submit failed.");
  });

  it("prevents duplicate submit requests on rapid double-click", async () => {
    const telegram = createTelegramWebApp("auth_payload");
    const deferredFetch = createDeferredFetchMock();
    loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchMock: deferredFetch.fetchMock,
    });

    fillRequiredClientName("John");
    await flushAsync();

    deferredFetch.resolveNext({
      ok: true,
      status: 200,
      body: { ok: true, uploadToken: "token-double-click" },
    });
    await flushAsync();

    const submitButton = getSubmitButton();
    submitButton.click();
    submitButton.click();
    await flushAsync(1);

    const submitCalls = deferredFetch.fetchCalls.filter((call) => call.url === "/api/mini/clients");
    expect(submitCalls).toHaveLength(1);

    deferredFetch.resolveNext({
      ok: true,
      status: 201,
      body: { ok: true },
    });
    await flushAsync();
  });

  it("falls back from showPopup to showAlert when showPopup throws", async () => {
    const telegram = createTelegramWebApp("auth_payload", {
      showPopup: "throw",
      showAlert: "ok",
    });
    const browserAlertCalls: string[] = [];
    window.alert = ((message?: string) => {
      browserAlertCalls.push((message || "").toString());
    }) as typeof window.alert;

    loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchResponses: [
        { ok: true, status: 200, body: { ok: true, uploadToken: "token-popup-fallback" } },
        { ok: true, status: 201, body: { ok: true } },
      ],
    });

    fillRequiredClientName("John");
    await flushAsync();
    await submitForm();

    expect(telegram.notificationOrder).toEqual(["showPopup", "showAlert"]);
    expect(telegram.popupCalls).toHaveLength(1);
    expect(telegram.alertCalls).toEqual([POPUP_FALLBACK_MESSAGE]);
    expect(browserAlertCalls).toEqual([]);
    expect(getMessageText()).toBe("Submitted for moderation. Client will appear after approval.");
  });

  it("falls back to window.alert when showPopup and showAlert throw", async () => {
    const telegram = createTelegramWebApp("auth_payload", {
      showPopup: "throw",
      showAlert: "throw",
    });
    const browserAlertCalls: string[] = [];
    window.alert = ((message?: string) => {
      browserAlertCalls.push((message || "").toString());
    }) as typeof window.alert;

    loadMiniApp({
      telegramWebApp: telegram.webApp,
      fetchResponses: [
        { ok: true, status: 200, body: { ok: true, uploadToken: "token-window-alert" } },
        { ok: true, status: 201, body: { ok: true } },
      ],
    });

    fillRequiredClientName("John");
    await flushAsync();
    await submitForm();

    expect(telegram.notificationOrder).toEqual(["showPopup", "showAlert"]);
    expect(telegram.alertCalls).toEqual([POPUP_FALLBACK_MESSAGE]);
    expect(browserAlertCalls).toEqual([POPUP_FALLBACK_MESSAGE]);
    expect(getMessageText()).toBe("Submitted for moderation. Client will appear after approval.");
  });
});
