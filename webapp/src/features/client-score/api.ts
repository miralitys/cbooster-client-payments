import { apiRequest } from "@/shared/api/fetcher";
import type { PaymentFeatures } from "@/features/client-score/domain/scoring";

export interface PaymentProbabilityResponse {
  p1: number;
  p2: number;
  p3: number;
  modelVersion: string;
  featureImportances?: Record<string, number>;
}

export async function fetchPaymentProbability(features: PaymentFeatures): Promise<PaymentProbabilityResponse> {
  return apiRequest<PaymentProbabilityResponse>("/api/payment-probability", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ features }),
  });
}
