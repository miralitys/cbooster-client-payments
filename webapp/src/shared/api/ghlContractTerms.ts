import { apiRequest } from "@/shared/api/fetcher";
import type {
  GhlContractTermsPayload,
  GhlContractTermsRecentPayload,
  GhlContractTermsRequest,
} from "@/shared/types/ghlContractTerms";

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

export async function getGhlContractTermsRecent(limit = 20): Promise<GhlContractTermsRecentPayload> {
  const query = new URLSearchParams({
    limit: Number.isFinite(limit) ? String(limit) : "20",
  });
  return apiRequest<GhlContractTermsRecentPayload>(`/api/ghl/contract-terms/recent?${query.toString()}`);
}

export async function getGhlContractTermsCache(id: string): Promise<GhlContractTermsPayload> {
  return apiRequest<GhlContractTermsPayload>(`/api/ghl/contract-terms/cache/${encodeURIComponent(id)}`);
}
