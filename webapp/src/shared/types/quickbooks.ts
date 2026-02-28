export interface QuickBooksPaymentRow {
  transactionId?: string;
  clientName: string;
  clientPhone: string;
  clientEmail: string;
  categoryName?: string;
  categoryDetails?: string;
  description?: string;
  paymentAmount: number;
  paymentDate: string;
  transactionType: string;
  matchedRecordId?: string;
  matchedPaymentField?: string;
  matchedPaymentDateField?: string;
  matchedConfirmed?: boolean;
  matchedConfirmedAt?: string;
  matchedConfirmedBy?: string;
}

export interface QuickBooksSyncMeta {
  requested: boolean;
  syncMode: "full" | "incremental" | string;
  performed?: boolean;
  syncFrom?: string;
  fetchedCount?: number;
  insertedCount?: number;
  updatedCount?: number;
  reconciledCount?: number;
  writtenCount?: number;
  reconciledScannedCount?: number;
  reconciledWrittenCount?: number;
}

export interface QuickBooksPaymentsPayload {
  ok: boolean;
  range?: {
    from: string;
    to: string;
  };
  count?: number;
  source?: string;
  items: QuickBooksPaymentRow[];
  sync?: QuickBooksSyncMeta;
}

export type QuickBooksSyncJobStatus = "queued" | "running" | "completed" | "failed" | string;

export interface QuickBooksSyncJob {
  id: string;
  status: QuickBooksSyncJobStatus;
  done?: boolean;
  syncMode?: "full" | "incremental" | string;
  range?: {
    from: string;
    to: string;
  };
  requestedBy?: string;
  queuedAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  updatedAt?: string | null;
  error?: string | null;
  sync?: QuickBooksSyncMeta | null;
}

export interface QuickBooksSyncJobPayload {
  ok: boolean;
  queued?: boolean;
  reused?: boolean;
  job?: QuickBooksSyncJob | null;
}

export interface QuickBooksTransactionInsightRequest {
  companyName: string;
  amount: number;
  date: string;
  description?: string;
}

export interface QuickBooksTransactionInsightPayload {
  ok: boolean;
  insight: string;
}

export interface QuickBooksPendingConfirmationRow {
  transactionType: string;
  transactionId: string;
  matchedPaymentField: string;
  matchedPaymentDateField: string;
  paymentAmount: number;
  paymentDate: string;
}

export interface QuickBooksPendingConfirmationsPayload {
  ok: boolean;
  recordId: string;
  count: number;
  items: QuickBooksPendingConfirmationRow[];
}

export interface QuickBooksPendingConfirmationRecordIdsPayload {
  ok: boolean;
  count: number;
  recordIds: string[];
}

export interface QuickBooksConfirmPaymentRequest {
  transactionId: string;
  transactionType?: string;
}

export interface QuickBooksConfirmPaymentPayload {
  ok: boolean;
  item: {
    transactionType: string;
    transactionId: string;
    matchedRecordId: string;
    matchedPaymentField: string;
    matchedPaymentDateField: string;
    matchedConfirmed: boolean;
    matchedConfirmedAt?: string;
    matchedConfirmedBy?: string;
  };
}
