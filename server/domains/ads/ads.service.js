"use strict";

const DEFAULT_GRAPH_API_BASE_URL = "https://graph.facebook.com";
const DEFAULT_GRAPH_API_VERSION = "v21.0";
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const DEFAULT_MAX_PAGES_PER_ACCOUNT = 15;
const DEFAULT_PAGE_LIMIT = 500;

function createAdsService(dependencies = {}) {
  const {
    sanitizeTextValue,
    fetchImpl,
    graphApiBaseUrl,
    graphApiVersion,
    accessToken,
    accountIdsRaw,
    requestTimeoutMs,
    maxPagesPerAccount,
    pageLimit,
  } = dependencies;

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value || "").trim().slice(0, maxLength);
  const safeFetch = typeof fetchImpl === "function" ? fetchImpl : globalThis.fetch;

  if (typeof safeFetch !== "function") {
    throw new Error("createAdsService requires fetch implementation.");
  }

  const token = sanitize(accessToken, 8000);
  const accountIds = parseMetaAdsAccountIds(accountIdsRaw, sanitize);
  const apiBase = sanitize(graphApiBaseUrl, 300) || DEFAULT_GRAPH_API_BASE_URL;
  const apiVersion = sanitize(graphApiVersion, 40) || DEFAULT_GRAPH_API_VERSION;
  const timeoutMs = clampInteger(requestTimeoutMs, 1000, 120_000, DEFAULT_REQUEST_TIMEOUT_MS);
  const maxPages = clampInteger(maxPagesPerAccount, 1, 100, DEFAULT_MAX_PAGES_PER_ACCOUNT);
  const recordsPerPage = clampInteger(pageLimit, 25, 1000, DEFAULT_PAGE_LIMIT);

  function isConfigured() {
    return Boolean(token && accountIds.length);
  }

  async function getOverview(options = {}) {
    const range = resolveDateRange(options, sanitize);

    if (!isConfigured()) {
      throw createHttpError(
        "Meta Ads integration is not configured. Set META_ADS_ACCESS_TOKEN and META_ADS_ACCOUNT_IDS.",
        503,
        "meta_ads_not_configured",
      );
    }

    const rows = [];
    for (const accountId of accountIds) {
      const accountRows = await fetchInsightsForAccount({
        fetch: safeFetch,
        sanitize,
        token,
        accountId,
        apiBase,
        apiVersion,
        range,
        timeoutMs,
        maxPages,
        recordsPerPage,
      });
      rows.push(...accountRows);
    }

    const ads = rows.sort((left, right) => {
      const spendDelta = right.spend - left.spend;
      if (spendDelta !== 0) {
        return spendDelta;
      }
      return right.dateStart.localeCompare(left.dateStart);
    });

    const campaigns = buildCampaignSummaries(ads);
    const adsets = buildAdSetSummaries(ads);
    const summary = buildSummary(ads, campaigns, adsets, accountIds.length);

    return {
      ok: true,
      configured: true,
      range,
      summary,
      campaigns,
      adsets,
      ads,
    };
  }

  return {
    isConfigured,
    getOverview,
  };
}

async function fetchInsightsForAccount(context) {
  const {
    fetch,
    sanitize,
    token,
    accountId,
    apiBase,
    apiVersion,
    range,
    timeoutMs,
    maxPages,
    recordsPerPage,
  } = context;

  const rows = [];
  let afterCursor = "";
  let pageNumber = 0;

  while (pageNumber < maxPages) {
    pageNumber += 1;

    const endpoint = `${trimTrailingSlash(apiBase)}/${apiVersion}/act_${encodeURIComponent(accountId)}/insights`;
    const url = new URL(endpoint);
    url.searchParams.set(
      "fields",
      [
        "account_id",
        "account_name",
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "ad_id",
        "ad_name",
        "spend",
        "impressions",
        "clicks",
        "reach",
        "ctr",
        "cpc",
        "date_start",
        "date_stop",
      ].join(","),
    );
    url.searchParams.set("level", "ad");
    url.searchParams.set("limit", String(recordsPerPage));
    url.searchParams.set("time_range", JSON.stringify({ since: range.since, until: range.until }));
    url.searchParams.set("access_token", token);
    if (afterCursor) {
      url.searchParams.set("after", afterCursor);
    }

    const payload = await requestMetaJson(fetch, url, timeoutMs, sanitize);
    const responseRows = Array.isArray(payload?.data) ? payload.data : [];

    for (const row of responseRows) {
      const normalizedRow = normalizeAdsRow(row, sanitize, accountId);
      if (normalizedRow) {
        rows.push(normalizedRow);
      }
    }

    const nextCursor = sanitize(payload?.paging?.cursors?.after, 8000);
    if (!nextCursor || responseRows.length === 0) {
      break;
    }

    afterCursor = nextCursor;
  }

  return rows;
}

async function requestMetaJson(fetch, url, timeoutMs, sanitize) {
  const abortController = new AbortController();
  const timeoutId = setTimeout(() => {
    abortController.abort("timeout");
  }, timeoutMs);

  try {
    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      signal: abortController.signal,
    });

    const payload = await response.json().catch(() => ({}));
    const graphError = payload?.error;
    if (!response.ok || graphError) {
      const statusCode = Number(response.status) || 502;
      const message = sanitize(
        graphError?.message || payload?.error?.error_user_msg || `Meta Ads request failed with status ${statusCode}.`,
        500,
      );
      const code = sanitize(graphError?.code || graphError?.error_subcode || "meta_ads_request_failed", 120);
      const httpStatus = statusCode === 429 ? 429 : statusCode >= 400 ? statusCode : 502;
      throw createHttpError(message || "Failed to fetch Meta Ads data.", httpStatus, code || "meta_ads_request_failed");
    }

    return payload;
  } catch (error) {
    if (error && typeof error === "object" && Number.isInteger(error.httpStatus)) {
      throw error;
    }

    if (error?.name === "AbortError" || error === "timeout") {
      throw createHttpError("Meta Ads request timed out. Try a smaller date range.", 504, "meta_ads_timeout");
    }

    throw createHttpError(
      sanitize(error?.message, 500) || "Meta Ads request failed.",
      502,
      "meta_ads_request_failed",
    );
  } finally {
    clearTimeout(timeoutId);
  }
}

function resolveDateRange(options, sanitize) {
  const now = new Date();
  const defaultUntil = formatDateIso(now);
  const defaultSinceDate = new Date(now.getTime() - 29 * 24 * 60 * 60 * 1000);
  const defaultSince = formatDateIso(defaultSinceDate);

  const rawSince = sanitize(options?.since, 20);
  const rawUntil = sanitize(options?.until, 20);
  const since = normalizeDateString(rawSince) || defaultSince;
  const until = normalizeDateString(rawUntil) || defaultUntil;

  if (since > until) {
    return {
      since: until,
      until: since,
    };
  }

  return {
    since,
    until,
  };
}

function normalizeAdsRow(rawRow, sanitize, fallbackAccountId) {
  if (!rawRow || typeof rawRow !== "object" || Array.isArray(rawRow)) {
    return null;
  }

  const dateStart = normalizeDateString(sanitize(rawRow.date_start, 20));
  const dateStop = normalizeDateString(sanitize(rawRow.date_stop, 20));
  if (!dateStart || !dateStop) {
    return null;
  }

  const campaignId = sanitize(rawRow.campaign_id, 120);
  const adSetId = sanitize(rawRow.adset_id, 120);
  const adId = sanitize(rawRow.ad_id, 120);
  if (!campaignId && !adSetId && !adId) {
    return null;
  }

  const spend = normalizeNumber(rawRow.spend);
  const impressions = normalizeInteger(rawRow.impressions);
  const clicks = normalizeInteger(rawRow.clicks);
  const reach = normalizeInteger(rawRow.reach);

  return {
    accountId: sanitize(rawRow.account_id, 120) || fallbackAccountId,
    accountName: sanitize(rawRow.account_name, 240) || "-",
    campaignId: campaignId || "-",
    campaignName: sanitize(rawRow.campaign_name, 320) || "Unnamed campaign",
    adSetId: adSetId || "-",
    adSetName: sanitize(rawRow.adset_name, 320) || "Unnamed ad set",
    adId: adId || "-",
    adName: sanitize(rawRow.ad_name, 320) || "Unnamed ad",
    spend,
    impressions,
    clicks,
    reach,
    ctr: impressions > 0 ? (clicks / impressions) * 100 : null,
    cpc: clicks > 0 ? spend / clicks : null,
    dateStart,
    dateStop,
  };
}

function buildCampaignSummaries(rows) {
  const aggregationMap = new Map();

  for (const row of rows) {
    const key = `${row.campaignId}::${row.campaignName}`;
    const current =
      aggregationMap.get(key) ||
      {
        campaignId: row.campaignId,
        campaignName: row.campaignName,
        accountName: row.accountName,
        spend: 0,
        impressions: 0,
        clicks: 0,
        reach: 0,
        adSetIds: new Set(),
        adIds: new Set(),
      };

    current.spend += row.spend;
    current.impressions += row.impressions;
    current.clicks += row.clicks;
    current.reach += row.reach;
    if (row.adSetId && row.adSetId !== "-") {
      current.adSetIds.add(row.adSetId);
    }
    if (row.adId && row.adId !== "-") {
      current.adIds.add(row.adId);
    }

    aggregationMap.set(key, current);
  }

  return [...aggregationMap.values()]
    .map((entry) => ({
      campaignId: entry.campaignId,
      campaignName: entry.campaignName,
      accountName: entry.accountName,
      adSetCount: entry.adSetIds.size,
      adCount: entry.adIds.size,
      spend: roundCurrency(entry.spend),
      impressions: entry.impressions,
      clicks: entry.clicks,
      reach: entry.reach,
      ctr: entry.impressions > 0 ? (entry.clicks / entry.impressions) * 100 : null,
      cpc: entry.clicks > 0 ? entry.spend / entry.clicks : null,
    }))
    .sort((left, right) => right.spend - left.spend);
}

function buildAdSetSummaries(rows) {
  const aggregationMap = new Map();

  for (const row of rows) {
    const key = `${row.adSetId}::${row.adSetName}`;
    const current =
      aggregationMap.get(key) ||
      {
        adSetId: row.adSetId,
        adSetName: row.adSetName,
        campaignName: row.campaignName,
        accountName: row.accountName,
        spend: 0,
        impressions: 0,
        clicks: 0,
        reach: 0,
        adIds: new Set(),
      };

    current.spend += row.spend;
    current.impressions += row.impressions;
    current.clicks += row.clicks;
    current.reach += row.reach;
    if (row.adId && row.adId !== "-") {
      current.adIds.add(row.adId);
    }

    aggregationMap.set(key, current);
  }

  return [...aggregationMap.values()]
    .map((entry) => ({
      adSetId: entry.adSetId,
      adSetName: entry.adSetName,
      campaignName: entry.campaignName,
      accountName: entry.accountName,
      adCount: entry.adIds.size,
      spend: roundCurrency(entry.spend),
      impressions: entry.impressions,
      clicks: entry.clicks,
      reach: entry.reach,
      ctr: entry.impressions > 0 ? (entry.clicks / entry.impressions) * 100 : null,
      cpc: entry.clicks > 0 ? entry.spend / entry.clicks : null,
    }))
    .sort((left, right) => right.spend - left.spend);
}

function buildSummary(rows, campaigns, adsets, accountCount) {
  const totals = rows.reduce(
    (accumulator, row) => {
      accumulator.spend += row.spend;
      accumulator.impressions += row.impressions;
      accumulator.clicks += row.clicks;
      accumulator.reach += row.reach;
      return accumulator;
    },
    {
      spend: 0,
      impressions: 0,
      clicks: 0,
      reach: 0,
    },
  );

  return {
    accountCount,
    campaignCount: campaigns.length,
    adSetCount: adsets.length,
    adCount: rows.length,
    spend: roundCurrency(totals.spend),
    impressions: totals.impressions,
    clicks: totals.clicks,
    reach: totals.reach,
    ctr: totals.impressions > 0 ? (totals.clicks / totals.impressions) * 100 : null,
    cpc: totals.clicks > 0 ? totals.spend / totals.clicks : null,
  };
}

function parseMetaAdsAccountIds(rawValue, sanitize) {
  const raw = sanitize(rawValue, 2000);
  if (!raw) {
    return [];
  }

  const segments = raw
    .split(/[\s,;|]+/)
    .map((value) => sanitize(value, 120).replace(/^act_/i, ""))
    .map((value) => value.replace(/[^0-9]/g, ""))
    .filter(Boolean);

  return [...new Set(segments)];
}

function normalizeDateString(rawValue) {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "";
  }

  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return "";
  }

  const timestamp = Date.parse(`${match[1]}-${match[2]}-${match[3]}T00:00:00.000Z`);
  if (!Number.isFinite(timestamp)) {
    return "";
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

function formatDateIso(value) {
  const year = value.getUTCFullYear();
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  const day = String(value.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function normalizeNumber(rawValue) {
  const value = Number.parseFloat(String(rawValue ?? "0"));
  if (!Number.isFinite(value)) {
    return 0;
  }
  return roundCurrency(value);
}

function normalizeInteger(rawValue) {
  const value = Number.parseInt(String(rawValue ?? "0"), 10);
  if (!Number.isFinite(value)) {
    return 0;
  }
  return value;
}

function clampInteger(rawValue, min, max, fallback) {
  const parsed = Number.parseInt(String(rawValue ?? ""), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  if (parsed < min) {
    return min;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
}

function trimTrailingSlash(value) {
  return String(value || "").replace(/\/+$/g, "");
}

function roundCurrency(value) {
  return Math.round(value * 100) / 100;
}

function createHttpError(message, httpStatus = 500, code = "internal_error") {
  const error = new Error(message);
  error.httpStatus = httpStatus;
  error.code = code;
  return error;
}

module.exports = {
  createAdsService,
};
