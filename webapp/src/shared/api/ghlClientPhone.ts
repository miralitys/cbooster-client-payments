import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientPhoneRefreshPayload, GhlClientPhonesBulkRefreshPayload } from "@/shared/types/ghlClientPhone";

export async function postGhlClientPhoneRefresh(clientName: string): Promise<GhlClientPhoneRefreshPayload> {
  const normalizedClientName = (clientName || "").toString().trim();
  return apiRequest<GhlClientPhoneRefreshPayload>("/api/ghl/client-phone/refresh", {
    timeoutMs: 90_000,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      clientName: normalizedClientName,
    }),
  });
}

export async function postGhlClientPhonesRefresh(clientNames: string[]): Promise<GhlClientPhonesBulkRefreshPayload> {
  const normalizedClientNames = Array.isArray(clientNames)
    ? clientNames.map((clientName) => (clientName || "").toString().trim()).filter(Boolean)
    : [];

  return apiRequest<GhlClientPhonesBulkRefreshPayload>("/api/ghl/client-phone/refresh/bulk", {
    timeoutMs: 300_000,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      clientNames: normalizedClientNames,
    }),
  });
}
