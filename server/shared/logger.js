"use strict";

function logBootstrapEvent(message, meta = null) {
  if (!message) {
    return;
  }
  if (meta && typeof meta === "object") {
    console.log(`[bootstrap] ${message}`, meta);
    return;
  }
  console.log(`[bootstrap] ${message}`);
}

module.exports = {
  logBootstrapEvent,
};
