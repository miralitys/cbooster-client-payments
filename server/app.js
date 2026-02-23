"use strict";

const legacyServer = require("../server-legacy.js");

module.exports = {
  app: legacyServer.app,
  startServer: legacyServer.startServer,
  __assistantInternals: legacyServer.__assistantInternals,
};
