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
