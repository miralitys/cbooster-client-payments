export type GhlClientContractStatus = "ready" | "no_contact" | "no_contract" | "error" | string;

export interface GhlClientContractRow {
  clientName: string;
  contactName: string;
  contactId: string;
  matchedContacts: number;
  contractTitle: string;
  contractUrl: string;
  source: string;
  status: GhlClientContractStatus;
  error?: string;
}

export interface GhlClientContractsPayload {
  ok: boolean;
  count: number;
  readyCount?: number;
  limit: number;
  items: GhlClientContractRow[];
  source?: string;
  updatedAt?: string | null;
  matcherVersion?: string;
}
