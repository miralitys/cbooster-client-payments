"use strict";

function parsePort(value, fallback = 10000) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return fallback;
}

const PORT = parsePort(process.env.PORT, 10000);

module.exports = {
  PORT,
  parsePort,
};
