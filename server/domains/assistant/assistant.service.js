"use strict";

function createAssistantService(dependencies = {}) {
  const handlers = dependencies.handlers || {};

  return {
    handleAssistantContextResetPost: handlers.handleAssistantContextResetPost,
    handleAssistantContextResetTelemetryPost: handlers.handleAssistantContextResetTelemetryPost,
    handleAssistantChatPost: handlers.handleAssistantChatPost,
    handleAssistantReviewsList: handlers.handleAssistantReviewsList,
    handleAssistantReviewUpdate: handlers.handleAssistantReviewUpdate,
    handleAssistantTtsPost: handlers.handleAssistantTtsPost,
  };
}

module.exports = {
  createAssistantService,
};
