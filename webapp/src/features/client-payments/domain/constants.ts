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
  "futurePayments",
  "afterResult",
  "notes",
  "collection",
  "dateOfCollection",
  "dateWhenWrittenOff",
];
