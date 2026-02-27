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
    confirmQuickBooksPaymentMatch(payload) {
      if (typeof repo.confirmQuickBooksPaymentMatch !== "function") {
        return Promise.resolve(null);
      }
      return repo.confirmQuickBooksPaymentMatch(payload);
    },
    listPendingQuickBooksPaymentMatchesByRecordId(recordId) {
      if (typeof repo.listPendingQuickBooksPaymentMatchesByRecordId !== "function") {
        return Promise.resolve([]);
      }
      return repo.listPendingQuickBooksPaymentMatchesByRecordId(recordId);
    },
  };
}

module.exports = {
  createQuickBooksService,
};
