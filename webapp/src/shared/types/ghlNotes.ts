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
  source: string;
  matchedContacts: number;
  inspectedContacts: number;
}
