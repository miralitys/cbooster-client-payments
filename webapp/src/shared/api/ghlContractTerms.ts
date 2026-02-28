import { apiRequest } from "@/shared/api/fetcher";
import type { GhlContractTermsPayload, GhlContractTermsRequest } from "@/shared/types/ghlContractTerms";

export async function getGhlContractTerms(payload: GhlContractTermsRequest): Promise<GhlContractTermsPayload> {
  return apiRequest<GhlContractTermsPayload>("/api/ghl/contract-terms", {
    method: "POST",
    timeoutMs: 180_000,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
