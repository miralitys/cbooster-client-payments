export interface GhlContractTermsRequest {
  clientName: string;
  locationId?: string;
}

export interface GhlContractTermsContactDetails {
  fullName: string;
  phone: string;
  email: string;
  address: string;
  monitoringService: string;
  monitoringEmail: string;
  monitoringPassword: string;
  monitoringSecret: string;
  ssn: string;
  dob: string;
}

export interface GhlContractTermsPayment {
  dueDate: string;
  amount: string;
}

export interface GhlContractTermsResult {
  documentId: string;
  documentName: string;
  status: string;
  source: string;
  updatedAt: string;
  contactName: string;
  contactEmail: string;
  signedAt: string;
  contactDetails: GhlContractTermsContactDetails;
  terms: string[];
  payments: GhlContractTermsPayment[];
  signatureDataUrl: string;
  clientName: string;
  fetchedAt: string;
  elapsedMs: number;
}

export interface GhlContractTermsPayload {
  ok: boolean;
  result: GhlContractTermsResult;
}

export interface GhlContractTermsRecentItem extends GhlContractTermsResult {
  id: string;
}

export interface GhlContractTermsRecentPayload {
  ok: boolean;
  items: GhlContractTermsRecentItem[];
}
