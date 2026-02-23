"use strict";

function createGhlCommunicationsController(handlers = {}) {
  return {
    handleGhlClientCommunicationsGet: handlers.handleGhlClientCommunicationsGet,
    handleGhlClientCommunicationsRecordingGet: handlers.handleGhlClientCommunicationsRecordingGet,
    handleGhlClientCommunicationsTranscriptPost: handlers.handleGhlClientCommunicationsTranscriptPost,
    handleGhlClientCommunicationsNormalizeTranscriptsPost: handlers.handleGhlClientCommunicationsNormalizeTranscriptsPost,
  };
}

module.exports = {
  createGhlCommunicationsController,
};
