import { apiRequest } from "@/shared/api/fetcher";
import type { GhlContractTextPayload, GhlContractTextRequest } from "@/shared/types/ghlContractText";

export async function getGhlContractText(payload: GhlContractTextRequest): Promise<GhlContractTextPayload> {
  return apiRequest<GhlContractTextPayload>("/api/ghl/contract-text", {
    method: "POST",
    timeoutMs: 180_000,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
