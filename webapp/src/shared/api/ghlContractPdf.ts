import { apiRequest } from "@/shared/api/fetcher";
import type { GhlContractPdfPayload, GhlContractPdfRequest } from "@/shared/types/ghlContractPdf";

export async function getGhlContractPdf(payload: GhlContractPdfRequest): Promise<GhlContractPdfPayload> {
  return apiRequest<GhlContractPdfPayload>("/api/ghl/contract-pdf", {
    method: "POST",
    timeoutMs: 180_000,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
