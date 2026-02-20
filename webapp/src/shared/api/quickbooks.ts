import { apiRequest } from "@/shared/api/fetcher";
import type { QuickBooksPaymentsPayload } from "@/shared/types/quickbooks";

interface GetQuickBooksPaymentsOptions {
  from: string;
  to: string;
  sync?: boolean;
  fullSync?: boolean;
}

export async function getQuickBooksPayments(options: GetQuickBooksPaymentsOptions): Promise<QuickBooksPaymentsPayload> {
  const shouldSync = options.sync === true || options.fullSync === true;
  if (shouldSync) {
    return apiRequest<QuickBooksPaymentsPayload>("/api/quickbooks/payments/recent/sync", {
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

  const query = new URLSearchParams({
    from: options.from,
    to: options.to,
  });
  return apiRequest<QuickBooksPaymentsPayload>(`/api/quickbooks/payments/recent?${query.toString()}`);
}
