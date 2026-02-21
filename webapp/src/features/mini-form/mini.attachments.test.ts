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

type VmSandbox = Record<string, unknown> & {
  module: { exports: unknown };
  exports: Record<string, unknown>;
  globalThis?: unknown;
};

let miniSource = "";

beforeAll(() => {
  miniSource = fs.readFileSync(MINI_JS_PATH, "utf8");
});

afterEach(() => {
  document.body.innerHTML = "";

  const windowWithTelegram = window as Window & {
    Telegram?: unknown;
  };
  delete windowWithTelegram.Telegram;
});

function loadMiniUi(options: { dataTransfer?: unknown } = {}) {
  document.body.innerHTML = MINI_HTML;

  const sandbox: VmSandbox = {
    module: { exports: {} },
    exports: {},
    window,
    document,
    console,
    fetch: async () => ({ ok: false, status: 503, json: async () => ({}) }),
    FormData,
    DataTransfer: options.dataTransfer,
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
    filename: MINI_JS_PATH,
  });
}

function getAttachmentsInput() {
  const input = document.querySelector("#attachments");
  if (!(input instanceof HTMLInputElement)) {
    throw new Error("#attachments was not found");
  }

  return input;
}

function getAttachmentsPreview() {
  const preview = document.querySelector("#attachments-preview");
  if (!(preview instanceof HTMLElement)) {
    throw new Error("#attachments-preview was not found");
  }

  return preview;
}

function getMessageNode() {
  const message = document.querySelector("#mini-message");
  if (!(message instanceof HTMLElement)) {
    throw new Error("#mini-message was not found");
  }

  return message;
}

function setSelectedFiles(input: HTMLInputElement, files: File[]) {
  Object.defineProperty(input, "files", {
    configurable: true,
    writable: true,
    value: files,
  });
}

function dispatchAttachmentsChange(files: File[]) {
  const input = getAttachmentsInput();
  setSelectedFiles(input, files);
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function makeFiles(count: number, extension = ".txt") {
  return Array.from({ length: count }, (_item, index) => {
    const number = index + 1;
    return new File([`file-${number}`], `file-${number}${extension}`, {
      type: "text/plain",
    });
  });
}

describe("mini.js attachments UI", () => {
  it("renders preview for selected safe attachments", () => {
    loadMiniUi({ dataTransfer: undefined });

    const files = [
      new File(["abc"], "invoice.pdf", { type: "application/pdf" }),
      new File(["x".repeat(2048)], "photo.png", { type: "image/png" }),
    ];

    dispatchAttachmentsChange(files);

    const preview = getAttachmentsPreview();
    const items = Array.from(preview.querySelectorAll(".attachments-preview__item"));

    expect(preview.hidden).toBe(false);
    expect(items).toHaveLength(2);
    expect(items[0]?.textContent || "").toContain("invoice.pdf");
    expect(items[0]?.textContent || "").toContain("(3 B)");
    expect(items[1]?.textContent || "").toContain("photo.png");
    expect(items[1]?.textContent || "").toContain("(2 KB)");
    expect(getMessageNode().textContent || "").toBe("");
  });

  it("blocks .js and .html attachments and clears preview", () => {
    loadMiniUi({ dataTransfer: undefined });

    dispatchAttachmentsChange([new File(["ok"], "safe.pdf", { type: "application/pdf" })]);
    expect(getAttachmentsPreview().hidden).toBe(false);

    dispatchAttachmentsChange([new File(["alert(1)"], "script.js", { type: "text/javascript" })]);

    expect(getMessageNode().textContent || "").toBe(
      'File "script.js" is not allowed. Script and HTML files are blocked.',
    );
    expect(getAttachmentsPreview().hidden).toBe(true);
    expect(getAttachmentsPreview().children).toHaveLength(0);

    dispatchAttachmentsChange([new File(["<html></html>"], "index.HTML", { type: "text/html" })]);

    expect(getMessageNode().textContent || "").toBe(
      'File "index.HTML" is not allowed. Script and HTML files are blocked.',
    );
    expect(getAttachmentsPreview().hidden).toBe(true);
  });

  it("applies 10-file UI limit and shows readable error", () => {
    loadMiniUi({ dataTransfer: undefined });

    const files = makeFiles(11);
    dispatchAttachmentsChange(files);

    const preview = getAttachmentsPreview();
    const items = Array.from(preview.querySelectorAll(".attachments-preview__item"));

    expect(getMessageNode().textContent || "").toBe("You can upload up to 10 files.");
    expect(preview.hidden).toBe(false);
    expect(items).toHaveLength(10);
    expect(preview.textContent || "").toContain("file-1.txt");
    expect(preview.textContent || "").not.toContain("file-11.txt");
  });

  it("falls back safely when DataTransfer is unavailable", () => {
    loadMiniUi({ dataTransfer: undefined });

    const files = makeFiles(11);
    dispatchAttachmentsChange(files);

    const input = getAttachmentsInput();
    const selectedAfterValidation = input.files as unknown as File[] | null;

    expect(getMessageNode().textContent || "").toBe("You can upload up to 10 files.");
    expect(selectedAfterValidation).not.toBeNull();
    expect(selectedAfterValidation?.length).toBe(11);
    expect(getAttachmentsPreview().querySelectorAll(".attachments-preview__item")).toHaveLength(10);
  });
});
