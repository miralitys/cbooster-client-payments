"use strict";

function registerAdsRoutes(context) {
  const {
    app,
    requireOwnerOrAdminAccess,
    handlers,
  } = context;

  const requireAdsAccess = requireOwnerOrAdminAccess("Owner or admin access is required.");

  app.get(
    "/api/ads/overview",
    requireAdsAccess,
    handlers.handleAdsOverviewGet,
  );
}

module.exports = {
  registerAdsRoutes,
};
