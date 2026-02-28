"use strict";

function createAdsController(dependencies = {}) {
  const {
    adsService,
    sanitizeTextValue,
  } = dependencies;

  if (!adsService) {
    throw new Error("createAdsController requires adsService.");
  }

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value || "").trim().slice(0, maxLength);

  async function handleAdsOverviewGet(req, res) {
    try {
      const payload = await adsService.getOverview({
        since: sanitize(req.query?.since, 20),
        until: sanitize(req.query?.until, 20),
      });

      res.json(payload);
    } catch (error) {
      const status = Number.isInteger(error?.httpStatus) ? error.httpStatus : 502;
      res.status(status).json({
        error: sanitize(error?.message, 500) || "Failed to load Meta Ads data.",
        code: sanitize(error?.code, 120) || "meta_ads_failed",
      });
    }
  }

  return {
    handleAdsOverviewGet,
  };
}

module.exports = {
  createAdsController,
};
