import { apiRequest } from "@/shared/api/fetcher";
import type { IdentityIqCreditScorePayload, IdentityIqCreditScoreRequest } from "@/shared/types/identityIq";

export async function getIdentityIqCreditScore(
  payload: IdentityIqCreditScoreRequest,
): Promise<IdentityIqCreditScorePayload> {
  return apiRequest<IdentityIqCreditScorePayload>("/api/identityiq/credit-score", {
    method: "POST",
    timeoutMs: 120_000,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
