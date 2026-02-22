export type GhlClientCommunicationStatus = "found" | "not_found" | "error" | string;
export type GhlClientCommunicationKind = "sms" | "call" | string;
export type GhlClientCommunicationDirection = "inbound" | "outbound" | "unknown" | string;

export interface GhlClientCommunicationItem {
  id: string;
  conversationId: string;
  kind: GhlClientCommunicationKind;
  direction: GhlClientCommunicationDirection;
  body: string;
  status: string;
  createdAt: string;
  source: string;
  recordingUrls: string[];
  attachmentUrls: string[];
}

export interface GhlClientCommunicationsPayload {
  ok: boolean;
  status: GhlClientCommunicationStatus;
  clientName: string;
  contactName: string;
  contactId: string;
  source: string;
  matchedContacts: number;
  inspectedContacts: number;
  smsCount: number;
  callCount: number;
  items: GhlClientCommunicationItem[];
}
