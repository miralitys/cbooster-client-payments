export interface ClientRecord {
  id: string;
  createdAt: string;
  clientName: string;
  closedBy: string;
  companyName: string;
  serviceType: string;
  purchasedService: string;
  address: string;
  dateOfBirth: string;
  ssn: string;
  creditMonitoringLogin: string;
  creditMonitoringPassword: string;
  leadSource: string;
  clientPhoneNumber: string;
  clientEmailAddress: string;
  futurePayment: string;
  identityIq: string;
  contractTotals: string;
  totalPayments: string;
  payment1: string;
  payment1Date: string;
  payment2: string;
  payment2Date: string;
  payment3: string;
  payment3Date: string;
  payment4: string;
  payment4Date: string;
  payment5: string;
  payment5Date: string;
  payment6: string;
  payment6Date: string;
  payment7: string;
  payment7Date: string;
  futurePayments: string;
  afterResult: string;
  writtenOff: string;
  notes: string;
  collection: string;
  dateOfCollection: string;
  dateWhenWrittenOff: string;
  dateWhenFullyPaid: string;
}

export interface RecordsPayload {
  records: ClientRecord[];
  updatedAt?: string | null;
}

export interface PutRecordsPayload {
  ok: boolean;
  updatedAt?: string | null;
}

export type RecordsPatchOperationType = "upsert" | "delete";

export interface RecordsPatchUpsertOperation {
  type: "upsert";
  id: string;
  record: Partial<ClientRecord>;
}

export interface RecordsPatchDeleteOperation {
  type: "delete";
  id: string;
}

export type RecordsPatchOperation = RecordsPatchUpsertOperation | RecordsPatchDeleteOperation;

export interface PatchRecordsPayload {
  ok: boolean;
  updatedAt?: string | null;
  appliedOperations?: number;
}
