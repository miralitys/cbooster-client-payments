export interface ClientRecord {
  id: string;
  createdAt: string;
  clientName: string;
  closedBy: string;
  companyName: string;
  ownerCompany: string;
  contractCompleted: string;
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
  payment8: string;
  payment8Date: string;
  payment9: string;
  payment9Date: string;
  payment10: string;
  payment10Date: string;
  payment11: string;
  payment11Date: string;
  payment12: string;
  payment12Date: string;
  payment13: string;
  payment13Date: string;
  payment14: string;
  payment14Date: string;
  payment15: string;
  payment15Date: string;
  payment16: string;
  payment16Date: string;
  payment17: string;
  payment17Date: string;
  payment18: string;
  payment18Date: string;
  payment19: string;
  payment19Date: string;
  payment20: string;
  payment20Date: string;
  payment21: string;
  payment21Date: string;
  payment22: string;
  payment22Date: string;
  payment23: string;
  payment23Date: string;
  payment24: string;
  payment24Date: string;
  payment25: string;
  payment25Date: string;
  payment26: string;
  payment26Date: string;
  payment27: string;
  payment27Date: string;
  payment28: string;
  payment28Date: string;
  payment29: string;
  payment29Date: string;
  payment30: string;
  payment30Date: string;
  payment31: string;
  payment31Date: string;
  payment32: string;
  payment32Date: string;
  payment33: string;
  payment33Date: string;
  payment34: string;
  payment34Date: string;
  payment35: string;
  payment35Date: string;
  payment36: string;
  payment36Date: string;
  futurePayments: string;
  afterResult: string;
  writtenOff: string;
  contractSigned: string;
  startedInWork: string;
  cachedScore: string;
  cachedScoreTone: string;
  scoreUpdatedAt: string;
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
