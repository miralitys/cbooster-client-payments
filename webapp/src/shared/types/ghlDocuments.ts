export type GhlDocumentsStatus = "found" | "possible" | "not_found" | "error" | string;

export interface GhlClientDocument {
  title: string;
  url: string;
  snippet: string;
  source: string;
  contactName: string;
  contactId: string;
  isContractMatch: boolean;
}

export interface GhlClientDocumentsRow {
  clientName: string;
  contactName: string;
  matchedContacts: number;
  documentsCount: number;
  documents: GhlClientDocument[];
  contractTitle: string;
  contractUrl: string;
  source: string;
  status: GhlDocumentsStatus;
  error?: string;
}

export interface GhlClientDocumentsPayload {
  ok: boolean;
  count: number;
  limit: number;
  items: GhlClientDocumentsRow[];
  source?: string;
  updatedAt?: string | null;
  matcherVersion?: string;
}
