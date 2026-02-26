import { apiRequest } from "@/shared/api/fetcher";
import type { GhlClientPhoneRefreshPayload } from "@/shared/types/ghlClientPhone";

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
