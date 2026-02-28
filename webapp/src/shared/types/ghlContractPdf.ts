export interface GhlContractPdfRequest {
  clientName: string;
  locationId?: string;
}

export interface GhlContractPdfResult {
  provider: string;
  status: "ok" | string;
  clientName: string;
  contactName: string;
  contactId: string;
  contractTitle: string;
  source: string;
  contractUrl: string;
  fileName: string;
  mimeType: string;
  sizeBytes: number;
  fileBase64: string;
  fetchedAt: string;
  elapsedMs: number;
  note?: string;
}

export interface GhlContractPdfPayload {
  ok: boolean;
  result: GhlContractPdfResult;
}
