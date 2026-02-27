export interface GhlContractTextRequest {
  clientName: string;
  locationId?: string;
}

export interface GhlContractTextResult {
  provider: string;
  status: "ok" | "partial" | string;
  clientName: string;
  contactName: string;
  contactId: string;
  contractTitle: string;
  candidateId: string;
  source: string;
  fallbackMode: string;
  contractText: string;
  textLength: number;
  dashboardUrl: string;
  fetchedAt: string;
  elapsedMs: number;
  note?: string;
}

export interface GhlContractTextPayload {
  ok: boolean;
  result: GhlContractTextResult;
}
