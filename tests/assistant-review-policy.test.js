"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { __assistantInternals } = require("../server.js");

test("assistant review storage policy redacts structured personal fields in minimal mode", () => {
  const input = [
    "Клиент: John Smith",
    "Менеджер: Kate Burnis",
    "Компания: Acme Inc",
    "Примечание: Private note with passport",
    "Email: john.smith@example.com",
    "Phone: +1 (555) 123-4567",
    "Amount: USD 1200.00",
    "По клиенту John Smith есть вопрос.",
  ].join("\n");

  const sanitized = __assistantInternals.sanitizeAssistantReviewTextForStorage(input, {
    piiMode: "minimal",
    clientMentions: ["John Smith"],
    maxLength: 8000,
  });

  assert.match(sanitized, /\[redacted\]/i);
  assert.match(sanitized, /\[redacted-email\]/i);
  assert.match(sanitized, /\[redacted-phone\]/i);
  assert.match(sanitized, /\[redacted-amount\]/i);
  assert.doesNotMatch(sanitized, /John Smith/i);
  assert.doesNotMatch(sanitized, /Kate Burnis/i);
  assert.doesNotMatch(sanitized, /Acme Inc/i);
  assert.doesNotMatch(sanitized, /Private note/i);
  assert.doesNotMatch(sanitized, /john\.smith@example\.com/i);
});

test("assistant review storage policy supports full and redact modes", () => {
  const input = "Client: John Smith. Amount: $500.00";

  const fullMode = __assistantInternals.sanitizeAssistantReviewTextForStorage(input, {
    piiMode: "full",
    maxLength: 8000,
  });
  const redactMode = __assistantInternals.sanitizeAssistantReviewTextForStorage(input, {
    piiMode: "redact",
    maxLength: 8000,
  });

  assert.equal(fullMode, input);
  assert.equal(redactMode, "[redacted by assistant review policy]");
});

test("assistant review minimal mode redacts sensitive labels even without mention hints", () => {
  const input = [
    "Manager: Kate Burnis",
    "Company: Acme Inc",
    "Note: keep private",
    "Amount: USD 700.00",
  ].join("\n");

  const sanitized = __assistantInternals.sanitizeAssistantReviewTextForStorage(input, {
    piiMode: "minimal",
    maxLength: 8000,
  });

  assert.match(sanitized, /\[redacted\]/i);
  assert.match(sanitized, /\[redacted-amount\]/i);
  assert.doesNotMatch(sanitized, /Kate Burnis/i);
  assert.doesNotMatch(sanitized, /Acme Inc/i);
  assert.doesNotMatch(sanitized, /keep private/i);
});
