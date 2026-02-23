"use strict";

function createAssistantController(dependencies = {}) {
  const { assistantService } = dependencies;

  return {
    handleAssistantContextResetPost: assistantService?.handleAssistantContextResetPost,
    handleAssistantContextResetTelemetryPost: assistantService?.handleAssistantContextResetTelemetryPost,
    handleAssistantChatPost: assistantService?.handleAssistantChatPost,
    handleAssistantReviewsList: assistantService?.handleAssistantReviewsList,
    handleAssistantReviewUpdate: assistantService?.handleAssistantReviewUpdate,
    handleAssistantTtsPost: assistantService?.handleAssistantTtsPost,
  };
}

module.exports = {
  createAssistantController,
};
