import type { PaymentFeatures } from "@/features/client-score/domain/scoring";

export interface PaymentProbabilityResponse {
  p1: number;
  p2: number;
  p3: number;
  modelVersion: string;
  featureImportances?: Record<string, number>;
}

export async function fetchPaymentProbability(features: PaymentFeatures): Promise<PaymentProbabilityResponse> {
  const response = await fetch("/api/payment-probability", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ features }),
  });

  if (!response.ok) {
    throw new Error("Failed to fetch payment probability");
  }

  return (await response.json()) as PaymentProbabilityResponse;
}
