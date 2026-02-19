export type GhlClientBasicNoteStatus = "found" | "not_found" | "error" | string;

export interface GhlClientBasicNotePayload {
  ok: boolean;
  status: GhlClientBasicNoteStatus;
  clientName: string;
  contactName: string;
  contactId: string;
  noteTitle: string;
  noteBody: string;
  noteCreatedAt: string;
  memoTitle: string;
  memoBody: string;
  memoCreatedAt: string;
  source: string;
  matchedContacts: number;
  inspectedContacts: number;
  updatedAt?: string | null;
  nextRefreshAt?: string | null;
  isWrittenOff?: boolean;
  refreshPolicy?: string;
  cached?: boolean;
  stale?: boolean;
  error?: string;
}
