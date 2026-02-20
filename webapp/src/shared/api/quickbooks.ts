import { apiRequest } from "@/shared/api/fetcher";
import type { QuickBooksPaymentsPayload, QuickBooksSyncJobPayload } from "@/shared/types/quickbooks";

interface GetQuickBooksPaymentsOptions {
  from: string;
  to: string;
}

export async function getQuickBooksPayments(options: GetQuickBooksPaymentsOptions): Promise<QuickBooksPaymentsPayload> {
  const query = new URLSearchParams({
    from: options.from,
    to: options.to,
  });
  return apiRequest<QuickBooksPaymentsPayload>(`/api/quickbooks/payments/recent?${query.toString()}`);
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
