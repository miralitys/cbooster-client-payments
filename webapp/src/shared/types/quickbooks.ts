export interface QuickBooksPaymentRow {
  clientName: string;
  clientPhone: string;
  clientEmail: string;
  paymentAmount: number;
  paymentDate: string;
  transactionType: string;
}

export interface QuickBooksSyncMeta {
  requested: boolean;
  syncMode: "full" | "incremental" | string;
  insertedCount?: number;
  updatedCount?: number;
  reconciledCount?: number;
  writtenCount?: number;
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
