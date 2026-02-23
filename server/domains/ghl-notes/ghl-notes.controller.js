"use strict";

function createGhlNotesController(handlers = {}) {
  return {
    handleGhlClientBasicNotesRefreshAllGet: handlers.handleGhlClientBasicNotesRefreshAllGet,
    handleGhlClientBasicNotesRefreshAllPost: handlers.handleGhlClientBasicNotesRefreshAllPost,
    handleGhlClientBasicNotesMissingGet: handlers.handleGhlClientBasicNotesMissingGet,
    handleGhlClientBasicNoteGet: handlers.handleGhlClientBasicNoteGet,
    handleGhlClientBasicNoteRefreshPost: handlers.handleGhlClientBasicNoteRefreshPost,
  };
}

module.exports = {
  createGhlNotesController,
};
