"use strict";

function createQuickBooksService(dependencies = {}) {
  const {
    repo,
    listCachedQuickBooksTransactionsInRange: listCachedTransactionsInRange,
    listQuickBooksOutgoingTransactionsInRange,
    buildQuickBooksSyncMeta,
    enqueueQuickBooksSyncJob,
    buildQuickBooksSyncJobPayload,
    getQuickBooksSyncJobById,
    requestOpenAiQuickBooksInsight,
  } = dependencies;

  if (!repo || typeof repo.listCachedQuickBooksTransactionsInRange !== "function") {
    throw new Error("createQuickBooksService requires quickbooks repo.");
  }

  return {
    listCachedQuickBooksTransactionsInRange(rangeFrom, rangeTo) {
      if (typeof listCachedTransactionsInRange === "function") {
        return listCachedTransactionsInRange(rangeFrom, rangeTo);
      }
      return repo.listCachedQuickBooksTransactionsInRange(rangeFrom, rangeTo);
    },
    listQuickBooksOutgoingTransactionsInRange(rangeFrom, rangeTo) {
      return listQuickBooksOutgoingTransactionsInRange(rangeFrom, rangeTo);
    },
    buildQuickBooksSyncMeta(options) {
      return buildQuickBooksSyncMeta(options);
    },
    enqueueQuickBooksSyncJob(range, options) {
      return enqueueQuickBooksSyncJob(range, options);
    },
    buildQuickBooksSyncJobPayload(job) {
      return buildQuickBooksSyncJobPayload(job);
    },
    getQuickBooksSyncJobById(jobId) {
      return getQuickBooksSyncJobById(jobId);
    },
    requestOpenAiQuickBooksInsight(payload) {
      return requestOpenAiQuickBooksInsight(payload);
    },
  };
}

module.exports = {
  createQuickBooksService,
};
