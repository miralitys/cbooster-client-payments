"use strict";

function createGhlLeadsController(handlers = {}) {
  return {
    handleGhlLeadsGet: handlers.handleGhlLeadsGet,
    handleGhlLeadsRefreshPost: handlers.handleGhlLeadsRefreshPost,
    handleGhlClientManagersGet: handlers.handleGhlClientManagersGet,
    handleGhlClientManagersRefreshPost: handlers.handleGhlClientManagersRefreshPost,
  };
}

module.exports = {
  createGhlLeadsController,
};
