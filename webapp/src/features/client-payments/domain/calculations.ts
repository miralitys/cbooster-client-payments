import type { ClientRecord } from "@/shared/types/records";
import {
  DAY_IN_MS,
  FIELD_DEFINITIONS,
  OVERDUE_RANGE_OPTIONS,
  PAYMENT_PAIRS,
  STATUS_FILTER_AFTER_RESULT,
  STATUS_FILTER_ALL,
  STATUS_FILTER_FULLY_PAID,
  STATUS_FILTER_OVERDUE,
  STATUS_FILTER_WRITTEN_OFF,
  ZERO_TOLERANCE,
  type OverdueRangeFilter,
  type OverviewPeriodKey,
  type SortDirection,
  type StatusFilter,
} from "@/features/client-payments/domain/constants";

const TEXT_COLLATOR = new Intl.Collator("en-US", {
  numeric: true,
  sensitivity: "base",
});

const AFTER_RESULT_CLIENT_NAMES = new Set(
  [
    "Liviu Gurin",
    "Volodymyr Kasprii",
    "Filip Cvetkov",
    "Mekan Gurbanbayev",
    "Atai Taalaibekov",
    "Maksim Lenin",
    "Anastasiia Dovhaniuk",
    "Telman Akipov",
    "Artur Pyrogov",
    "Dmytro Shakin",
    "Mahir Aliyev",
    "Vasyl Feduniak",
    "Dmytro Kovalchuk",
    "Ilyas Veliev",
    "Muyassar Tulaganova",
    "Rostyslav Khariuk",
    "Kanat Omuraliev",
  ].map(normalizeClientName),
);

const WRITTEN_OFF_CLIENT_NAMES = new Set(
  [
    "Ghenadie Nipomici",
    "Andrii Kuziv",
    "Alina Seiitbek Kyzy",
    "Syimyk Alymov",
    "Urmatbek Aliman Adi",
    "Maksatbek Nadyrov",
    "Ismayil Hajiyev",
    "Artur Maltsev",
    "Maksim Burlaev",
    "Serhii Vasylchuk",
    "Denys Vatsyk",
    "Rinat Kadirmetov",
    "Pavlo Mykhailov",
  ].map(normalizeClientName),
);

export interface DateRange {
  from: string;
  to: string;
}

export interface ClientPaymentsFilters {
  search: string;
  status: StatusFilter;
  overdueRange: OverdueRangeFilter;
  closedBy: string;
  createdAtRange: DateRange;
  paymentDateRange: DateRange;
  writtenOffDateRange: DateRange;
  fullyPaidDateRange: DateRange;
}

export interface RecordStatusFlags {
  isAfterResult: boolean;
  isWrittenOff: boolean;
  isFullyPaid: boolean;
  isOverdue: boolean;
  overdueRange: OverdueRangeFilter;
  overdueDays: number;
}

export interface OverviewMetrics {
  sales: number;
  received: number;
  debt: number;
}

export interface TableTotals {
  contractTotals: number;
  totalPayments: number;
  futurePayments: number;
  collection: number;
}

export interface SortState {
  key: keyof ClientRecord;
  direction: SortDirection;
}

export const MONEY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export const KPI_MONEY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

export function formatMoney(value: number): string {
  return MONEY_FORMATTER.format(Number.isFinite(value) ? value : 0);
}

export function formatKpiMoney(value: number): string {
  return KPI_MONEY_FORMATTER.format(Number.isFinite(value) ? value : 0);
}

export function normalizeRecords(records: ClientRecord[]): ClientRecord[] {
  return records.map((record, index) => normalizeRecord(record, index));
}

export function normalizeRecord(record: Partial<ClientRecord>, index = 0): ClientRecord {
  const normalized: ClientRecord = createEmptyRecord();

  for (const [key, rawValue] of Object.entries(record)) {
    const nextKey = key as keyof ClientRecord;
    if (nextKey === "id") {
      normalized.id = sanitizeText(rawValue) || normalized.id;
      continue;
    }

    if (nextKey === "createdAt") {
      normalized.createdAt = normalizeCreatedAt(rawValue);
      continue;
    }

    if (nextKey in normalized) {
      normalized[nextKey] = sanitizeText(rawValue);
    }
  }

  if (!normalized.id) {
    normalized.id = `record_${index}_${Date.now()}`;
  }

  if (!normalized.createdAt) {
    normalized.createdAt = new Date().toISOString();
  }

  applyAfterResultFlag(normalized);
  applyDerivedRecordState(normalized);
  return normalized;
}

export function createEmptyRecord(): ClientRecord {
  const base: ClientRecord = {
    id: generateId(),
    createdAt: new Date().toISOString(),
    clientName: "",
    closedBy: "",
    companyName: "",
    serviceType: "",
    contractTotals: "",
    totalPayments: "",
    payment1: "",
    payment1Date: "",
    payment2: "",
    payment2Date: "",
    payment3: "",
    payment3Date: "",
    payment4: "",
    payment4Date: "",
    payment5: "",
    payment5Date: "",
    payment6: "",
    payment6Date: "",
    payment7: "",
    payment7Date: "",
    futurePayments: "",
    afterResult: "",
    writtenOff: "",
    notes: "",
    collection: "",
    dateOfCollection: "",
    dateWhenWrittenOff: "",
    dateWhenFullyPaid: "",
  };

  return base;
}

export function applyDerivedRecordState(record: ClientRecord, previousRecord?: ClientRecord | null): void {
  record.totalPayments = computeTotalPayments(record);
  record.futurePayments = computeFuturePayments(record);
  record.dateWhenFullyPaid = computeDateWhenFullyPaid(record, previousRecord || undefined);
}

export function getRecordStatusFlags(record: ClientRecord): RecordStatusFlags {
  const isAfterResult = isAfterResultEnabled(record.afterResult) || AFTER_RESULT_CLIENT_NAMES.has(normalizeClientName(record.clientName));
  const isWrittenOff = isWrittenOffEnabled(record.writtenOff) || WRITTEN_OFF_CLIENT_NAMES.has(normalizeClientName(record.clientName));

  const futureAmount = computeFuturePaymentsAmount(record);
  const isFullyPaid = !isWrittenOff && futureAmount !== null && futureAmount <= ZERO_TOLERANCE;

  const latestPaymentDate = getLatestPaymentDateTimestamp(record);
  const overdueDays = !isAfterResult && !isWrittenOff && !isFullyPaid && latestPaymentDate !== null
    ? getDaysSinceDate(latestPaymentDate)
    : 0;
  const overdueRange = getOverdueRangeLabel(overdueDays);
  const isOverdue = Boolean(overdueRange);

  return {
    isAfterResult,
    isWrittenOff,
    isFullyPaid,
    isOverdue,
    overdueRange,
    overdueDays,
  };
}

export function matchesStatusFilter(
  record: ClientRecord,
  statusFilter: StatusFilter,
  overdueRangeFilter: OverdueRangeFilter,
): boolean {
  const status = getRecordStatusFlags(record);

  if (statusFilter === STATUS_FILTER_ALL) {
    return true;
  }

  if (statusFilter === STATUS_FILTER_WRITTEN_OFF) {
    return status.isWrittenOff;
  }

  if (statusFilter === STATUS_FILTER_FULLY_PAID) {
    return status.isFullyPaid;
  }

  if (statusFilter === STATUS_FILTER_AFTER_RESULT) {
    return status.isAfterResult;
  }

  if (statusFilter === STATUS_FILTER_OVERDUE) {
    if (!status.isOverdue) {
      return false;
    }

    if (!overdueRangeFilter) {
      return true;
    }

    return status.overdueRange === overdueRangeFilter;
  }

  return true;
}

export function filterRecords(records: ClientRecord[], filters: ClientPaymentsFilters): ClientRecord[] {
  const query = filters.search.trim().toLowerCase();
  const selectedClosedBy = filters.closedBy.trim().toLowerCase();
  const createdRange = getDateRangeBounds(filters.createdAtRange);
  const paymentRange = getDateRangeBounds(filters.paymentDateRange);
  const writtenOffRange = getDateRangeBounds(filters.writtenOffDateRange);
  const fullyPaidRange = getDateRangeBounds(filters.fullyPaidDateRange);

  return records
    .filter((record) => {
      if (!query) {
        return true;
      }

      const searchable = [record.clientName, record.companyName, record.closedBy, record.serviceType]
        .map((value) => value.toLowerCase())
        .join(" ");

      return searchable.includes(query);
    })
    .filter((record) => {
      if (!selectedClosedBy) {
        return true;
      }

      return record.closedBy.trim().toLowerCase() === selectedClosedBy;
    })
    .filter((record) => isDateWithinRange(record.createdAt, createdRange.from, createdRange.to))
    .filter((record) => {
      if (!hasDateRangeValues(paymentRange)) {
        return true;
      }

      return PAYMENT_PAIRS.some(([, paymentDateField]) => {
        const paymentDate = (record[paymentDateField] || "").toString();
        return isDateWithinRange(paymentDate, paymentRange.from, paymentRange.to);
      });
    })
    .filter((record) => {
      if (!hasDateRangeValues(writtenOffRange)) {
        return true;
      }

      if (!getRecordStatusFlags(record).isWrittenOff) {
        return false;
      }

      return isDateWithinRange(record.dateWhenWrittenOff, writtenOffRange.from, writtenOffRange.to);
    })
    .filter((record) => {
      if (!hasDateRangeValues(fullyPaidRange)) {
        return true;
      }

      if (!getRecordStatusFlags(record).isFullyPaid) {
        return false;
      }

      return isDateWithinRange(record.dateWhenFullyPaid, fullyPaidRange.from, fullyPaidRange.to);
    })
    .filter((record) => matchesStatusFilter(record, filters.status, filters.overdueRange));
}

export function sortRecords(records: ClientRecord[], sortState: SortState): ClientRecord[] {
  const { key, direction } = sortState;

  return [...records].sort((left, right) => {
    const result = compareField(left[key], right[key], key);
    return direction === "asc" ? result : result * -1;
  });
}

export function calculateOverviewMetrics(records: ClientRecord[], period: OverviewPeriodKey): OverviewMetrics {
  const ranges = getPeriodRanges();
  const selectedRange = ranges[period];
  const periodMetrics = calculatePeriodMetrics(records, selectedRange.from, selectedRange.to);

  return {
    sales: periodMetrics.sales,
    received: periodMetrics.received,
    debt: calculateOverallDebt(records),
  };
}

export function calculateTableTotals(records: ClientRecord[]): TableTotals {
  return {
    contractTotals: sumField(records, "contractTotals"),
    totalPayments: sumField(records, "totalPayments"),
    futurePayments: sumField(records, "futurePayments"),
    collection: sumField(records, "collection"),
  };
}

export function getClosedByOptions(records: ClientRecord[]): string[] {
  const unique = new Map<string, string>();

  for (const record of records) {
    const value = record.closedBy.trim();
    if (!value) {
      continue;
    }

    const normalized = value.toLowerCase();
    if (!unique.has(normalized)) {
      unique.set(normalized, value);
    }
  }

  return [...unique.values()].sort((a, b) => TEXT_COLLATOR.compare(a, b));
}

export function normalizeFormRecord(input: Partial<ClientRecord>): ClientRecord {
  const nextRecord = normalizeRecord(input);

  for (const field of FIELD_DEFINITIONS) {
    const key = field.key;
    if (field.type === "checkbox") {
      nextRecord[key] = isAfterResultEnabled(nextRecord[key]) || isWrittenOffEnabled(nextRecord[key]) ? "Yes" : "";
      continue;
    }

    if (field.type === "date") {
      const normalizedDate = normalizeDateForStorage(nextRecord[key]);
      if (normalizedDate !== null) {
        nextRecord[key] = normalizedDate;
      }
      continue;
    }

    nextRecord[key] = sanitizeText(nextRecord[key]);
  }

  applyAfterResultFlag(nextRecord);
  applyDerivedRecordState(nextRecord);

  return nextRecord;
}

export function normalizeDateForStorage(value: string): string | null {
  const trimmed = sanitizeText(value);
  if (!trimmed) {
    return "";
  }

  const timestamp = parseDateValue(trimmed);
  if (timestamp === null) {
    return null;
  }

  return formatDateTimestampUs(timestamp);
}

export function parseMoneyValue(rawValue: unknown): number | null {
  const value = sanitizeText(rawValue);
  if (!value) {
    return null;
  }

  const normalized = value
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");

  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

export function parseDateValue(rawValue: unknown): number | null {
  const value = sanitizeText(rawValue);
  if (!value) {
    return null;
  }

  const usMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    const year = Number(usMatch[3]);
    if (!isValidDateParts(year, month, day)) {
      return null;
    }

    return Date.UTC(year, month - 1, day);
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    if (!isValidDateParts(year, month, day)) {
      return null;
    }

    return Date.UTC(year, month - 1, day);
  }

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return null;
  }

  const date = new Date(timestamp);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

export function formatDateTime(rawValue: string): string {
  const date = rawValue ? new Date(rawValue) : null;
  if (!date || Number.isNaN(date.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export function formatDate(rawValue: string): string {
  const timestamp = parseDateValue(rawValue);
  if (timestamp === null) {
    return rawValue || "-";
  }

  return formatDateTimestampUs(timestamp);
}

function sumField(records: ClientRecord[], key: keyof ClientRecord): number {
  return records.reduce((sum, record) => sum + (parseMoneyValue(record[key]) || 0), 0);
}

function computeTotalPayments(record: ClientRecord): string {
  const total = computeTotalPaymentsAmount(record);
  return total === null ? "" : String(total);
}

function computeTotalPaymentsAmount(record: ClientRecord): number | null {
  let hasAnyValue = false;
  let total = 0;

  for (const [paymentFieldKey] of PAYMENT_PAIRS) {
    const amount = parseMoneyValue(record[paymentFieldKey]);
    if (amount === null) {
      continue;
    }

    hasAnyValue = true;
    total += amount;
  }

  return hasAnyValue ? total : null;
}

function computeFuturePayments(record: ClientRecord): string {
  const futureAmount = computeFuturePaymentsAmount(record);
  return futureAmount === null ? "" : String(Math.max(0, futureAmount));
}

function computeFuturePaymentsAmount(record: ClientRecord): number | null {
  if (isRecordWrittenOff(record)) {
    return 0;
  }

  const contractTotal = parseMoneyValue(record.contractTotals);
  if (contractTotal === null) {
    return null;
  }

  const totalPayments = computeTotalPaymentsAmount(record) || 0;
  return contractTotal - totalPayments;
}

function computeDateWhenFullyPaid(record: ClientRecord, previousRecord?: ClientRecord): string {
  if (isRecordWrittenOff(record)) {
    return "";
  }

  const futureAmount = computeFuturePaymentsAmount(record);
  const latestPaymentDate = getLatestPaymentDateTimestamp(record);

  if (futureAmount !== null && futureAmount <= ZERO_TOLERANCE && latestPaymentDate !== null) {
    return formatDateTimestampUs(latestPaymentDate);
  }

  const previousStoredDate = normalizeDateForStorage(previousRecord?.dateWhenFullyPaid || "");
  if (previousStoredDate && previousStoredDate !== "") {
    return previousStoredDate;
  }

  const currentStoredDate = normalizeDateForStorage(record.dateWhenFullyPaid || "");
  if (currentStoredDate && currentStoredDate !== "") {
    return currentStoredDate;
  }

  return "";
}

function getLatestPaymentDateTimestamp(record: ClientRecord): number | null {
  let latestTimestamp: number | null = null;

  for (const [, paymentDateFieldKey] of PAYMENT_PAIRS) {
    const timestamp = parseDateValue(record[paymentDateFieldKey]);
    if (timestamp === null) {
      continue;
    }

    if (latestTimestamp === null || timestamp > latestTimestamp) {
      latestTimestamp = timestamp;
    }
  }

  return latestTimestamp;
}

function getDaysSinceDate(timestamp: number): number {
  const currentDayStart = getCurrentUtcDayStart();
  const diff = currentDayStart - timestamp;
  if (diff <= 0) {
    return 0;
  }

  return Math.floor(diff / DAY_IN_MS);
}

function getOverdueRangeLabel(daysOverdue: number): OverdueRangeFilter {
  if (daysOverdue <= 0) {
    return "";
  }

  if (daysOverdue <= 7) {
    return "1-7";
  }

  if (daysOverdue <= 30) {
    return "8-30";
  }

  if (daysOverdue <= 60) {
    return "31-60";
  }

  return "60+";
}

function isAfterResultEnabled(value: unknown): boolean {
  const normalized = sanitizeText(value).toLowerCase();
  return normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on";
}

function isWrittenOffEnabled(value: unknown): boolean {
  const normalized = sanitizeText(value).toLowerCase();
  return normalized === "yes" || normalized === "true" || normalized === "1" || normalized === "on";
}

function isRecordWrittenOff(record: ClientRecord): boolean {
  return isWrittenOffEnabled(record.writtenOff) || WRITTEN_OFF_CLIENT_NAMES.has(normalizeClientName(record.clientName));
}

function applyAfterResultFlag(record: ClientRecord): void {
  const isMarkedAfterResult = AFTER_RESULT_CLIENT_NAMES.has(normalizeClientName(record.clientName));
  if (isMarkedAfterResult && !isAfterResultEnabled(record.afterResult)) {
    record.afterResult = "Yes";
  }
}

function normalizeClientName(value: unknown): string {
  return sanitizeText(value)
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function sanitizeText(value: unknown): string {
  return (value ?? "").toString().trim();
}

function formatDateTimestampUs(timestamp: number): string {
  const date = new Date(timestamp);
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const year = String(date.getUTCFullYear());
  return `${month}/${day}/${year}`;
}

function normalizeCreatedAt(rawValue: unknown): string {
  const text = sanitizeText(rawValue);
  if (!text) {
    return new Date().toISOString();
  }

  const timestamp = Date.parse(text);
  return Number.isNaN(timestamp) ? new Date().toISOString() : new Date(timestamp).toISOString();
}

function getCurrentUtcDayStart(): number {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

function getCurrentWeekStartUtc(dayUtcStart: number): number {
  const dayOfWeek = new Date(dayUtcStart).getUTCDay();
  const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  return dayUtcStart - mondayOffset * DAY_IN_MS;
}

function calculatePeriodMetrics(records: ClientRecord[], fromTimestamp: number, toTimestamp: number): {
  sales: number;
  received: number;
} {
  let sales = 0;
  let received = 0;

  for (const record of records) {
    const firstPaymentDate = parseDateValue(record.payment1Date);
    const isSaleInRange = isTimestampWithinInclusiveRange(firstPaymentDate, fromTimestamp, toTimestamp);

    if (isSaleInRange) {
      const contractAmount = parseMoneyValue(record.contractTotals);
      if (contractAmount !== null) {
        sales += contractAmount;
      }
    }

    for (const [paymentFieldKey, paymentDateFieldKey] of PAYMENT_PAIRS) {
      const paymentDate = parseDateValue(record[paymentDateFieldKey]);
      if (!isTimestampWithinInclusiveRange(paymentDate, fromTimestamp, toTimestamp)) {
        continue;
      }

      const paymentAmount = parseMoneyValue(record[paymentFieldKey]);
      if (paymentAmount !== null) {
        received += paymentAmount;
      }
    }
  }

  return { sales, received };
}

function calculateOverallDebt(records: ClientRecord[]): number {
  let debt = 0;

  for (const record of records) {
    const futureAmount = computeFuturePaymentsAmount(record);
    if (futureAmount !== null && futureAmount > ZERO_TOLERANCE) {
      debt += futureAmount;
    }
  }

  return debt;
}

function getPeriodRanges(): Record<OverviewPeriodKey, { from: number; to: number }> {
  const todayStart = getCurrentUtcDayStart();
  const currentWeekStart = getCurrentWeekStartUtc(todayStart);
  const currentMonthStart = Date.UTC(new Date(todayStart).getUTCFullYear(), new Date(todayStart).getUTCMonth(), 1);

  return {
    currentWeek: {
      from: currentWeekStart,
      to: todayStart,
    },
    previousWeek: {
      from: currentWeekStart - 7 * DAY_IN_MS,
      to: currentWeekStart - DAY_IN_MS,
    },
    currentMonth: {
      from: currentMonthStart,
      to: todayStart,
    },
    last30Days: {
      from: todayStart - 29 * DAY_IN_MS,
      to: todayStart,
    },
  };
}

function isTimestampWithinInclusiveRange(
  timestamp: number | null,
  fromTimestamp: number,
  toTimestamp: number,
): boolean {
  if (timestamp === null) {
    return false;
  }

  return timestamp >= fromTimestamp && timestamp <= toTimestamp;
}

function compareField(
  leftRaw: string,
  rightRaw: string,
  key: keyof ClientRecord,
): number {
  const moneyLikeFields = new Set<keyof ClientRecord>([
    "contractTotals",
    "totalPayments",
    "futurePayments",
    "collection",
    ...PAYMENT_PAIRS.map(([paymentFieldKey]) => paymentFieldKey),
  ]);

  const dateLikeFields = new Set<keyof ClientRecord>([
    "createdAt",
    "dateOfCollection",
    "dateWhenWrittenOff",
    "dateWhenFullyPaid",
    ...PAYMENT_PAIRS.map(([, paymentDateFieldKey]) => paymentDateFieldKey),
  ]);

  if (moneyLikeFields.has(key)) {
    return compareNullableNumbers(parseMoneyValue(leftRaw), parseMoneyValue(rightRaw));
  }

  if (dateLikeFields.has(key)) {
    return compareNullableNumbers(parseDateValue(leftRaw), parseDateValue(rightRaw));
  }

  return TEXT_COLLATOR.compare(leftRaw || "", rightRaw || "");
}

function compareNullableNumbers(leftValue: number | null, rightValue: number | null): number {
  const leftHasValue = leftValue !== null;
  const rightHasValue = rightValue !== null;

  if (!leftHasValue && !rightHasValue) {
    return 0;
  }

  if (!leftHasValue) {
    return 1;
  }

  if (!rightHasValue) {
    return -1;
  }

  return leftValue - rightValue;
}

function getDateRangeBounds(range: DateRange): { from: number | null; to: number | null } {
  return {
    from: parseDateValue(range.from),
    to: parseDateValue(range.to),
  };
}

function hasDateRangeValues(range: { from: number | null; to: number | null }): boolean {
  return range.from !== null || range.to !== null;
}

function isDateWithinRange(value: string, fromDate: number | null, toDate: number | null): boolean {
  if (fromDate === null && toDate === null) {
    return true;
  }

  const timestamp = parseDateValue(value);
  if (timestamp === null) {
    return false;
  }

  if (fromDate !== null && timestamp < fromDate) {
    return false;
  }

  if (toDate !== null && timestamp > toDate) {
    return false;
  }

  return true;
}

function isValidDateParts(year: number, month: number, day: number): boolean {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function generateId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  return `record_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

export function getOverdueRanges(): OverdueRangeFilter[] {
  return OVERDUE_RANGE_OPTIONS.map((option) => option.key);
}
