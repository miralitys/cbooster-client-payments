import { apiRequest } from "@/shared/api/fetcher";
import type { QuickBooksPaymentsPayload } from "@/shared/types/quickbooks";

interface GetQuickBooksPaymentsOptions {
  from: string;
  to: string;
  sync?: boolean;
  fullSync?: boolean;
}

export async function getQuickBooksPayments(options: GetQuickBooksPaymentsOptions): Promise<QuickBooksPaymentsPayload> {
  const query = new URLSearchParams({
    from: options.from,
    to: options.to,
  });

  if (options.sync) {
    query.set("sync", "1");
  }

  if (options.fullSync) {
    query.set("fullSync", "1");
  }

  return apiRequest<QuickBooksPaymentsPayload>(`/api/quickbooks/payments/recent?${query.toString()}`);
}
