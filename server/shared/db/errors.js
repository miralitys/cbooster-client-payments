"use strict";

const DB_UNAVAILABLE_ERROR_CODES = new Set([
  "28P01",
  "3D000",
  "08001",
  "08003",
  "08004",
  "08006",
  "57P01",
  "57P02",
  "57P03",
  "ENOTFOUND",
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT",
  "EHOSTUNREACH",
]);

function normalizeDbErrorCode(error) {
  return (error?.code || "").toString().trim().toUpperCase();
}

function isDatabaseUnavailableError(error) {
  return DB_UNAVAILABLE_ERROR_CODES.has(normalizeDbErrorCode(error));
}

function resolveDbHttpStatus(error, fallbackStatus = 500) {
  if (isDatabaseUnavailableError(error)) {
    return 503;
  }
  return fallbackStatus;
}

module.exports = {
  DB_UNAVAILABLE_ERROR_CODES,
  isDatabaseUnavailableError,
  normalizeDbErrorCode,
  resolveDbHttpStatus,
};
