"use strict";

function createGhlLeadsController(handlers = {}) {
  return {
    handleGhlLeadsGet: handlers.handleGhlLeadsGet,
    handleGhlLeadsRefreshPost: handlers.handleGhlLeadsRefreshPost,
    handleGhlClientManagersGet: handlers.handleGhlClientManagersGet,
    handleGhlClientManagersRefreshPost: handlers.handleGhlClientManagersRefreshPost,
    handleGhlClientManagersRefreshBackgroundPost: handlers.handleGhlClientManagersRefreshBackgroundPost,
    handleGhlClientManagersRefreshBackgroundJobGet: handlers.handleGhlClientManagersRefreshBackgroundJobGet,
  };
}

module.exports = {
  createGhlLeadsController,
};
