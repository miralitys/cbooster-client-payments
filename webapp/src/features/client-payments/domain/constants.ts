import type { ClientRecord } from "@/shared/types/records";

export const DAY_IN_MS = 24 * 60 * 60 * 1000;
export const ZERO_TOLERANCE = 0.005;

export const REMOTE_SYNC_DEBOUNCE_MS = 900;
export const REMOTE_SYNC_RETRY_MS = 5000;
export const REMOTE_SYNC_MAX_RETRIES = 5;
export const REMOTE_SYNC_MAX_RETRY_DELAY_MS = 30_000;

export const STATUS_FILTER_ALL = "all";
export const STATUS_FILTER_WRITTEN_OFF = "written-off";
export const STATUS_FILTER_FULLY_PAID = "fully-paid";
export const STATUS_FILTER_AFTER_RESULT = "after-result";
export const STATUS_FILTER_OVERDUE = "overdue";

export type StatusFilter =
  | typeof STATUS_FILTER_ALL
  | typeof STATUS_FILTER_WRITTEN_OFF
  | typeof STATUS_FILTER_FULLY_PAID
  | typeof STATUS_FILTER_AFTER_RESULT
  | typeof STATUS_FILTER_OVERDUE;

export type SortDirection = "asc" | "desc";

export type OverdueRangeFilter = "" | "1-7" | "8-30" | "31-60" | "60+";

export const PAYMENT_PAIRS: Array<[keyof ClientRecord, keyof ClientRecord]> = [
  ["payment1", "payment1Date"],
  ["payment2", "payment2Date"],
  ["payment3", "payment3Date"],
  ["payment4", "payment4Date"],
  ["payment5", "payment5Date"],
  ["payment6", "payment6Date"],
  ["payment7", "payment7Date"],
  ["payment8", "payment8Date"],
  ["payment9", "payment9Date"],
  ["payment10", "payment10Date"],
  ["payment11", "payment11Date"],
  ["payment12", "payment12Date"],
  ["payment13", "payment13Date"],
  ["payment14", "payment14Date"],
  ["payment15", "payment15Date"],
  ["payment16", "payment16Date"],
  ["payment17", "payment17Date"],
  ["payment18", "payment18Date"],
  ["payment19", "payment19Date"],
  ["payment20", "payment20Date"],
  ["payment21", "payment21Date"],
  ["payment22", "payment22Date"],
  ["payment23", "payment23Date"],
  ["payment24", "payment24Date"],
  ["payment25", "payment25Date"],
  ["payment26", "payment26Date"],
  ["payment27", "payment27Date"],
  ["payment28", "payment28Date"],
  ["payment29", "payment29Date"],
  ["payment30", "payment30Date"],
  ["payment31", "payment31Date"],
  ["payment32", "payment32Date"],
  ["payment33", "payment33Date"],
  ["payment34", "payment34Date"],
  ["payment35", "payment35Date"],
  ["payment36", "payment36Date"],
];

export const PAYMENT_FIELDS = PAYMENT_PAIRS.map(([paymentFieldKey]) => paymentFieldKey);
export const PAYMENT_DATE_FIELDS = PAYMENT_PAIRS.map(([, paymentDateFieldKey]) => paymentDateFieldKey);

export const OVERVIEW_PERIOD_OPTIONS = [
  { key: "currentWeek", label: "Current Week" },
  { key: "previousWeek", label: "Previous Week" },
  { key: "currentMonth", label: "Current Month" },
  { key: "last30Days", label: "Last 30 Days" },
] as const;

export type OverviewPeriodKey = (typeof OVERVIEW_PERIOD_OPTIONS)[number]["key"];

export const STATUS_FILTER_OPTIONS: Array<{ key: StatusFilter; label: string }> = [
  { key: STATUS_FILTER_ALL, label: "All" },
  { key: STATUS_FILTER_WRITTEN_OFF, label: "Written Off" },
  { key: STATUS_FILTER_FULLY_PAID, label: "Fully Paid" },
  { key: STATUS_FILTER_AFTER_RESULT, label: "After Result" },
  { key: STATUS_FILTER_OVERDUE, label: "Overdue" },
];

export const OVERDUE_RANGE_OPTIONS: Array<{ key: OverdueRangeFilter; label: string }> = [
  { key: "", label: "Any" },
  { key: "1-7", label: "1-7" },
  { key: "8-30", label: "8-30" },
  { key: "31-60", label: "31-60" },
  { key: "60+", label: "60+" },
];

export interface FieldDefinition {
  key: keyof ClientRecord;
  label: string;
  type: "text" | "textarea" | "checkbox" | "date";
  required?: boolean;
  computed?: boolean;
}

export const FIELD_DEFINITIONS: FieldDefinition[] = [
  { key: "clientName", label: "Client Name", type: "text", required: true },
  { key: "closedBy", label: "Closed By", type: "text" },
  { key: "companyName", label: "Company Name", type: "text" },
  { key: "contractCompleted", label: "Contract", type: "checkbox" },
  { key: "serviceType", label: "Service Type", type: "text" },
  { key: "purchasedService", label: "Purchased Service", type: "text" },
  { key: "address", label: "Address", type: "text" },
  { key: "dateOfBirth", label: "Date of Birth", type: "date" },
  { key: "ssn", label: "SSN", type: "text" },
  { key: "creditMonitoringLogin", label: "Credit Monitoring Login", type: "text" },
  { key: "creditMonitoringPassword", label: "Credit Monitoring Password", type: "text" },
  { key: "leadSource", label: "Lead Source", type: "text" },
  { key: "clientPhoneNumber", label: "Client Phone Number", type: "text" },
  { key: "clientEmailAddress", label: "Client Email Address", type: "text" },
  { key: "futurePayment", label: "Future Payment", type: "text" },
  { key: "identityIq", label: "IdentityIQ", type: "text" },
  { key: "contractTotals", label: "Contract Totals", type: "text" },
  { key: "totalPayments", label: "Total Payments", type: "text", computed: true },
  { key: "payment1", label: "Payment 1", type: "text" },
  { key: "payment1Date", label: "Payment 1 Date", type: "date" },
  { key: "payment2", label: "Payment 2", type: "text" },
  { key: "payment2Date", label: "Payment 2 Date", type: "date" },
  { key: "payment3", label: "Payment 3", type: "text" },
  { key: "payment3Date", label: "Payment 3 Date", type: "date" },
  { key: "payment4", label: "Payment 4", type: "text" },
  { key: "payment4Date", label: "Payment 4 Date", type: "date" },
  { key: "payment5", label: "Payment 5", type: "text" },
  { key: "payment5Date", label: "Payment 5 Date", type: "date" },
  { key: "payment6", label: "Payment 6", type: "text" },
  { key: "payment6Date", label: "Payment 6 Date", type: "date" },
  { key: "payment7", label: "Payment 7", type: "text" },
  { key: "payment7Date", label: "Payment 7 Date", type: "date" },
  { key: "payment8", label: "Payment 8", type: "text" },
  { key: "payment8Date", label: "Payment 8 Date", type: "date" },
  { key: "payment9", label: "Payment 9", type: "text" },
  { key: "payment9Date", label: "Payment 9 Date", type: "date" },
  { key: "payment10", label: "Payment 10", type: "text" },
  { key: "payment10Date", label: "Payment 10 Date", type: "date" },
  { key: "payment11", label: "Payment 11", type: "text" },
  { key: "payment11Date", label: "Payment 11 Date", type: "date" },
  { key: "payment12", label: "Payment 12", type: "text" },
  { key: "payment12Date", label: "Payment 12 Date", type: "date" },
  { key: "payment13", label: "Payment 13", type: "text" },
  { key: "payment13Date", label: "Payment 13 Date", type: "date" },
  { key: "payment14", label: "Payment 14", type: "text" },
  { key: "payment14Date", label: "Payment 14 Date", type: "date" },
  { key: "payment15", label: "Payment 15", type: "text" },
  { key: "payment15Date", label: "Payment 15 Date", type: "date" },
  { key: "payment16", label: "Payment 16", type: "text" },
  { key: "payment16Date", label: "Payment 16 Date", type: "date" },
  { key: "payment17", label: "Payment 17", type: "text" },
  { key: "payment17Date", label: "Payment 17 Date", type: "date" },
  { key: "payment18", label: "Payment 18", type: "text" },
  { key: "payment18Date", label: "Payment 18 Date", type: "date" },
  { key: "payment19", label: "Payment 19", type: "text" },
  { key: "payment19Date", label: "Payment 19 Date", type: "date" },
  { key: "payment20", label: "Payment 20", type: "text" },
  { key: "payment20Date", label: "Payment 20 Date", type: "date" },
  { key: "payment21", label: "Payment 21", type: "text" },
  { key: "payment21Date", label: "Payment 21 Date", type: "date" },
  { key: "payment22", label: "Payment 22", type: "text" },
  { key: "payment22Date", label: "Payment 22 Date", type: "date" },
  { key: "payment23", label: "Payment 23", type: "text" },
  { key: "payment23Date", label: "Payment 23 Date", type: "date" },
  { key: "payment24", label: "Payment 24", type: "text" },
  { key: "payment24Date", label: "Payment 24 Date", type: "date" },
  { key: "payment25", label: "Payment 25", type: "text" },
  { key: "payment25Date", label: "Payment 25 Date", type: "date" },
  { key: "payment26", label: "Payment 26", type: "text" },
  { key: "payment26Date", label: "Payment 26 Date", type: "date" },
  { key: "payment27", label: "Payment 27", type: "text" },
  { key: "payment27Date", label: "Payment 27 Date", type: "date" },
  { key: "payment28", label: "Payment 28", type: "text" },
  { key: "payment28Date", label: "Payment 28 Date", type: "date" },
  { key: "payment29", label: "Payment 29", type: "text" },
  { key: "payment29Date", label: "Payment 29 Date", type: "date" },
  { key: "payment30", label: "Payment 30", type: "text" },
  { key: "payment30Date", label: "Payment 30 Date", type: "date" },
  { key: "payment31", label: "Payment 31", type: "text" },
  { key: "payment31Date", label: "Payment 31 Date", type: "date" },
  { key: "payment32", label: "Payment 32", type: "text" },
  { key: "payment32Date", label: "Payment 32 Date", type: "date" },
  { key: "payment33", label: "Payment 33", type: "text" },
  { key: "payment33Date", label: "Payment 33 Date", type: "date" },
  { key: "payment34", label: "Payment 34", type: "text" },
  { key: "payment34Date", label: "Payment 34 Date", type: "date" },
  { key: "payment35", label: "Payment 35", type: "text" },
  { key: "payment35Date", label: "Payment 35 Date", type: "date" },
  { key: "payment36", label: "Payment 36", type: "text" },
  { key: "payment36Date", label: "Payment 36 Date", type: "date" },
  { key: "futurePayments", label: "Future Payments", type: "text", computed: true },
  { key: "afterResult", label: "After Result", type: "checkbox" },
  { key: "writtenOff", label: "Written Off", type: "checkbox" },
  { key: "notes", label: "Notes", type: "textarea" },
  { key: "collection", label: "Collection", type: "text" },
  { key: "dateOfCollection", label: "Date Of Collection", type: "date" },
  { key: "dateWhenWrittenOff", label: "Date When Written Off", type: "date" },
  { key: "dateWhenFullyPaid", label: "Date When Fully Paid", type: "date", computed: true },
];

export const TABLE_COLUMNS: Array<keyof ClientRecord> = [
  "clientName",
  "closedBy",
  "companyName",
  "serviceType",
  "contractTotals",
  "totalPayments",
  "payment1",
  "payment1Date",
  "payment2",
  "payment2Date",
  "payment3",
  "payment3Date",
  "payment4",
  "payment4Date",
  "payment5",
  "payment5Date",
  "payment6",
  "payment6Date",
  "payment7",
  "payment7Date",
  "payment8",
  "payment8Date",
  "payment9",
  "payment9Date",
  "payment10",
  "payment10Date",
  "payment11",
  "payment11Date",
  "payment12",
  "payment12Date",
  "payment13",
  "payment13Date",
  "payment14",
  "payment14Date",
  "payment15",
  "payment15Date",
  "payment16",
  "payment16Date",
  "payment17",
  "payment17Date",
  "payment18",
  "payment18Date",
  "payment19",
  "payment19Date",
  "payment20",
  "payment20Date",
  "payment21",
  "payment21Date",
  "payment22",
  "payment22Date",
  "payment23",
  "payment23Date",
  "payment24",
  "payment24Date",
  "payment25",
  "payment25Date",
  "payment26",
  "payment26Date",
  "payment27",
  "payment27Date",
  "payment28",
  "payment28Date",
  "payment29",
  "payment29Date",
  "payment30",
  "payment30Date",
  "payment31",
  "payment31Date",
  "payment32",
  "payment32Date",
  "payment33",
  "payment33Date",
  "payment34",
  "payment34Date",
  "payment35",
  "payment35Date",
  "payment36",
  "payment36Date",
  "futurePayments",
  "afterResult",
  "notes",
  "collection",
  "dateOfCollection",
  "dateWhenWrittenOff",
];
