"use strict";

const READ_ONLY_ALLOWED_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);
const READ_ONLY_ALLOWED_POST_PATH_PATTERNS = [
  /^\/contacts\/search\/?$/i,
  /^\/opportunities\/search\/?$/i,
  /^\/conversations\/search\/?$/i,
];

function sanitizeToken(value, maxLength = 400) {
  const normalized = (value === null || value === undefined ? "" : String(value)).trim();
  if (!normalized) {
    return "";
  }
  return normalized.slice(0, maxLength);
}

function normalizeMethod(rawMethod) {
  return sanitizeToken(rawMethod || "GET", 20).toUpperCase() || "GET";
}

function normalizePathname(rawPathname) {
  const value = sanitizeToken(rawPathname, 4000);
  if (!value) {
    return "/";
  }

  try {
    if (/^https?:\/\//i.test(value)) {
      const url = new URL(value);
      const path = sanitizeToken(url.pathname, 2000);
      return `/${path.replace(/^\/+/, "")}`;
    }
  } catch {
    // Fallback to raw value normalization.
  }

  const withoutQuery = value.split("?")[0].split("#")[0];
  return `/${withoutQuery.replace(/^\/+/, "")}`;
}

function isAllowedSearchPostPath(pathname) {
  const normalizedPath = normalizePathname(pathname);
  for (const pattern of READ_ONLY_ALLOWED_POST_PATH_PATTERNS) {
    if (pattern.test(normalizedPath)) {
      return true;
    }
  }
  return false;
}

function isAllowedGhlRequest(method, pathname) {
  const normalizedMethod = normalizeMethod(method);
  const normalizedPath = normalizePathname(pathname);

  if (READ_ONLY_ALLOWED_METHODS.has(normalizedMethod)) {
    return true;
  }

  if (normalizedMethod === "POST" && isAllowedSearchPostPath(normalizedPath)) {
    return true;
  }

  return false;
}

function createGhlReadOnlyGuard(options = {}) {
  const {
    logger,
    errorFactory,
    enabled = true,
  } = options;

  function logBlockedAttempt(details) {
    if (!logger || typeof logger.warn !== "function") {
      return;
    }

    const source = sanitizeToken(details?.source, 200) || "unknown";
    const method = sanitizeToken(details?.method, 20) || "GET";
    const pathname = sanitizeToken(details?.pathname, 2000) || "/";
    logger.warn(`[ghl-read-only] blocked ${method} ${pathname}; source=${source}`);
  }

  function createDefaultError(message) {
    const error = new Error(message || "GHL read-only guard blocked a write operation.");
    error.httpStatus = 403;
    error.code = "ghl_read_only_blocked";
    return error;
  }

  function assertAllowedRequest(request = {}) {
    if (!enabled) {
      return;
    }

    const method = normalizeMethod(request.method);
    const pathname = normalizePathname(request.pathname);
    const source = sanitizeToken(request.source, 240);

    if (isAllowedGhlRequest(method, pathname)) {
      return;
    }

    logBlockedAttempt({ method, pathname, source });

    const message = `GHL request blocked by read-only policy: ${method} ${pathname}.`;
    const error = typeof errorFactory === "function" ? errorFactory(message, { method, pathname, source }) : createDefaultError(message);
    if (error && typeof error === "object") {
      if (!error.httpStatus) {
        error.httpStatus = 403;
      }
      if (!error.code) {
        error.code = "ghl_read_only_blocked";
      }
    }
    throw error;
  }

  return {
    assertAllowedRequest,
  };
}

module.exports = {
  createGhlReadOnlyGuard,
  isAllowedGhlRequest,
  normalizePathname,
  normalizeMethod,
};
