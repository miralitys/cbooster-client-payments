"use strict";

const { createAssistantController } = require("./assistant.controller");
const { createAssistantService } = require("./assistant.service");
const { createAssistantRepo } = require("./assistant.repo");

module.exports = {
  createAssistantController,
  createAssistantService,
  createAssistantRepo,
};
