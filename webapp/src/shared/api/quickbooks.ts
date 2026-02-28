import { apiRequest } from "@/shared/api/fetcher";
import type {
  QuickBooksConfirmPaymentPayload,
  QuickBooksConfirmPaymentRequest,
  QuickBooksPendingConfirmationRecordIdsPayload,
  QuickBooksPendingConfirmationsPayload,
  QuickBooksPaymentsPayload,
  QuickBooksSyncJobPayload,
  QuickBooksTransactionInsightPayload,
  QuickBooksTransactionInsightRequest,
} from "@/shared/types/quickbooks";

interface GetQuickBooksTransactionsOptions {
  from: string;
  to: string;
}

export async function getQuickBooksPayments(options: GetQuickBooksTransactionsOptions): Promise<QuickBooksPaymentsPayload> {
  const query = new URLSearchParams({
    from: options.from,
    to: options.to,
  });
  return apiRequest<QuickBooksPaymentsPayload>(`/api/quickbooks/payments/recent?${query.toString()}`);
}

export async function getQuickBooksOutgoingPayments(
  options: GetQuickBooksTransactionsOptions,
): Promise<QuickBooksPaymentsPayload> {
  const query = new URLSearchParams({
    from: options.from,
    to: options.to,
  });
  return apiRequest<QuickBooksPaymentsPayload>(`/api/quickbooks/payments/outgoing?${query.toString()}`);
}

interface CreateQuickBooksSyncJobOptions {
  from: string;
  to: string;
  fullSync?: boolean;
}

export async function createQuickBooksSyncJob(options: CreateQuickBooksSyncJobOptions): Promise<QuickBooksSyncJobPayload> {
  return apiRequest<QuickBooksSyncJobPayload>("/api/quickbooks/payments/recent/sync", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: options.from,
      to: options.to,
      fullSync: options.fullSync === true,
    }),
  });
}

export async function getQuickBooksSyncJob(jobId: string): Promise<QuickBooksSyncJobPayload> {
  return apiRequest<QuickBooksSyncJobPayload>(
    `/api/quickbooks/payments/recent/sync-jobs/${encodeURIComponent(String(jobId || "").trim())}`,
  );
}

export async function getQuickBooksTransactionInsight(
  payload: QuickBooksTransactionInsightRequest,
): Promise<QuickBooksTransactionInsightPayload> {
  return apiRequest<QuickBooksTransactionInsightPayload>("/api/quickbooks/transaction-insight", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function getQuickBooksPendingConfirmations(recordId: string): Promise<QuickBooksPendingConfirmationsPayload> {
  const query = new URLSearchParams({
    recordId: String(recordId || "").trim(),
  });
  return apiRequest<QuickBooksPendingConfirmationsPayload>(
    `/api/quickbooks/payments/pending-confirmations?${query.toString()}`,
  );
}

export async function getQuickBooksPendingConfirmationRecordIds(): Promise<QuickBooksPendingConfirmationRecordIdsPayload> {
  const payload = await apiRequest<QuickBooksPendingConfirmationRecordIdsPayload>(
    "/api/quickbooks/payments/pending-confirmations?scope=records",
  );
  return {
    ok: Boolean(payload?.ok),
    count: typeof payload?.count === "number" && Number.isFinite(payload.count) ? payload.count : 0,
    recordIds: Array.isArray(payload?.recordIds)
      ? payload.recordIds.map((value) => String(value || "").trim()).filter(Boolean)
      : [],
  };
}

export async function confirmQuickBooksRecentPayment(
  payload: QuickBooksConfirmPaymentRequest,
): Promise<QuickBooksConfirmPaymentPayload> {
  return apiRequest<QuickBooksConfirmPaymentPayload>("/api/quickbooks/payments/recent/confirm", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      transactionId: String(payload.transactionId || "").trim(),
      transactionType: String(payload.transactionType || "").trim() || "payment",
    }),
  });
}
