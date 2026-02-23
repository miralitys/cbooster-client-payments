"use strict";

function resolveOptionalBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) {
    return null;
  }
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return null;
}

function isTestRuntime() {
  return (process.env.NODE_ENV || "").toString().trim().toLowerCase() === "test";
}

function isAutostartForcedInTest() {
  return resolveOptionalBoolean(process.env.SERVER_AUTOSTART_IN_TEST) === true;
}

function shouldAutostartServer({ isDirectExecution }) {
  if (!isDirectExecution) {
    return false;
  }

  if (!isTestRuntime()) {
    return true;
  }

  return isAutostartForcedInTest();
}

module.exports = {
  resolveOptionalBoolean,
  isTestRuntime,
  isAutostartForcedInTest,
  shouldAutostartServer,
};
