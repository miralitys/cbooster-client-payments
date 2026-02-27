export interface GhlClientPhoneRefreshPayload {
  ok: boolean;
  status: "found" | "not_found";
  clientName: string;
  contactName: string;
  contactId: string;
  phone: string;
  source: string;
  matchedContacts: number;
  inspectedContacts: number;
  savedRecordsCount?: number;
  updatedAt?: string | null;
}

export interface GhlClientPhonesBulkRefreshPayload {
  ok: boolean;
  requestedClientsCount: number;
  scopedClientsCount: number;
  refreshedClientsCount: number;
  notFoundClientsCount: number;
  failedClientsCount: number;
  savedRecordsCount: number;
  updatedAt?: string | null;
  failures?: Array<{
    clientName: string;
    error: string;
    code?: string;
    status?: number;
  }>;
}
