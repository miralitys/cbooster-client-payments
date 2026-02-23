"use strict";

const { app, startServer, __assistantInternals } = require("./app");
const { PORT } = require("./shared/config");
const { shouldAutostartServer } = require("./shared/runtime");
const { logBootstrapEvent } = require("./shared/logger");

function autostartIfNeeded(options = {}) {
  const isDirectExecution = options.isDirectExecution === true;
  const listenPort = Number.isFinite(options.port) ? options.port : PORT;

  if (!shouldAutostartServer({ isDirectExecution })) {
    return null;
  }

  logBootstrapEvent("starting http server", { port: listenPort });
  return startServer(listenPort);
}

if (require.main === module) {
  autostartIfNeeded({
    isDirectExecution: true,
    port: PORT,
  });
}

module.exports = {
  app,
  startServer,
  autostartIfNeeded,
  __assistantInternals,
};
