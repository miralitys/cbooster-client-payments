import {
  formatDate,
  getRecordStatusFlags,
  parseDateValue,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import type { ClientRecord } from "@/shared/types/records";

const PAYMENT_SLOTS: Array<{ amountKey: keyof ClientRecord; dateKey: keyof ClientRecord }> = [
  { amountKey: "payment1", dateKey: "payment1Date" },
  { amountKey: "payment2", dateKey: "payment2Date" },
  { amountKey: "payment3", dateKey: "payment3Date" },
  { amountKey: "payment4", dateKey: "payment4Date" },
  { amountKey: "payment5", dateKey: "payment5Date" },
  { amountKey: "payment6", dateKey: "payment6Date" },
  { amountKey: "payment7", dateKey: "payment7Date" },
];

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const RECENT_WINDOW_DAYS = 90;
const SCORE_WINDOW_MONTHS = 12;
const SCORE_START = 100;
const SCORE_MIN = 0;
const SCORE_MAX = 110;
const EARLY_BONUS_DAYS = 10;
const BONUS_POINTS = 5;
const RECOVERY_POINTS = 5;
const LATE_GRACE_DAYS = 3;
const EPSILON = 0.0001;

const LEGACY_BASELINE_Z = -2.45;
const LEGACY_SCORE_DEFAULT = 50;
const LEGACY_SCORE_FACTOR_MAX = 1.1;
const LEGACY_PAID_RATIO_MAX = 1.5;
const LEGACY_PAYMENT_PACE_MAX = 2;
const LEGACY_OVERDUE_MAX_DAYS = 120;
const LEGACY_MONTH_2_DISCOUNT = 0.9;
const LEGACY_MONTH_3_DISCOUNT = 0.8;

interface PaymentEvent {
  amount: number;
  dateUtc: number;
}

interface MilestoneEval {
  index: number;
  dueUtc: number;
  plannedCumulative: number;
  catchUpUtc: number | null;
  delayDays: number;
  penaltyPoints: number;
  consideredInScoreWindow: boolean;
  isOpenDebt: boolean;
  isEarlyCoverage: boolean;
}

export interface ClientScoreResult {
  score: number | null;
  displayScore: number | null;
  tone: "neutral" | "success" | "info" | "warning" | "danger";
  penaltyPoints: number;
  bonusPoints: number;
  recoveryPoints: number;
  consideredMilestones: number;
  lateMilestones: number;
  maxDelayDays: number;
  openMilestones: number;
  explanation: string;
}

export interface PaymentFeatures {
  contractTotal: number;
  paidTotal: number;
  paidRatio: number;
  paymentPace: number;
  displayScore: number | null;
  overdueDays: number;
  openMilestones: number;
  futurePayments?: number | null;
  writtenOff?: boolean;
  balance: number;
  [key: string]: number | boolean | null | undefined;
}

export interface PaymentProbabilities {
  p1: number;
  p2: number;
  p3: number;
}

export function computePaymentFeatures(input: {
  contractTotal: number;
  totalPayments?: number | null;
  payments?: number[];
  monthlyPayment: number;
  displayScore?: number | null;
  overdueDays: number;
  openMilestones: number;
  futurePayments?: number | null;
  writtenOff?: boolean;
}): PaymentFeatures {
  const contractTotal = Math.max(0, toFiniteNumber(input.contractTotal));
  const paidTotal = resolvePaidTotal(input.totalPayments, input.payments);
  const futurePayments = toFiniteNullableNumber(input.futurePayments);

  const fallbackBalance = contractTotal - paidTotal;
  const balance = Math.max(0, futurePayments ?? fallbackBalance);
  const paidRatio = contractTotal > 0 ? clampNumber(paidTotal / contractTotal, 0, LEGACY_PAID_RATIO_MAX) : 0;
  const paymentPace = clampNumber(toFiniteNumber(input.monthlyPayment) / Math.max(balance, 1), 0, LEGACY_PAYMENT_PACE_MAX);

  return {
    contractTotal,
    paidTotal,
    paidRatio,
    paymentPace,
    displayScore: toFiniteNullableNumber(input.displayScore),
    overdueDays: Math.max(0, toFiniteNumber(input.overdueDays)),
    openMilestones: Math.max(0, toFiniteNumber(input.openMilestones)),
    futurePayments,
    writtenOff: input.writtenOff === true,
    balance,
  };
}

export function computeLegacyPaymentProbabilities(features: PaymentFeatures): PaymentProbabilities {
  if (features.writtenOff === true || features.balance <= 0) {
    return { p1: 0, p2: 0, p3: 0 };
  }

  const scoreFactor = clampNumber(
    (toFiniteNullableNumber(features.displayScore) ?? LEGACY_SCORE_DEFAULT) / 100,
    0,
    LEGACY_SCORE_FACTOR_MAX,
  );
  const overduePenalty = clampNumber(toFiniteNumber(features.overdueDays), 0, LEGACY_OVERDUE_MAX_DAYS);
  const paidRatio = clampNumber(toFiniteNumber(features.paidRatio), 0, LEGACY_PAID_RATIO_MAX);
  const paymentPace = clampNumber(toFiniteNumber(features.paymentPace), 0, LEGACY_PAYMENT_PACE_MAX);
  const openMilestones = Math.max(0, toFiniteNumber(features.openMilestones));

  const z =
    LEGACY_BASELINE_Z +
    scoreFactor * 3.2 +
    paidRatio * 1.15 +
    paymentPace * 0.4 -
    overduePenalty * 0.02 -
    openMilestones * 0.35;

  const p1 = clampNumber(sigmoid(z), 0.05, 0.95);
  const p2 = clampNumber(p1 * LEGACY_MONTH_2_DISCOUNT, 0.03, 0.9);
  const p3 = clampNumber(p1 * LEGACY_MONTH_3_DISCOUNT, 0.02, 0.85);

  return { p1, p2, p3 };
}

export function evaluateClientScore(record: ClientRecord, asOfDate = new Date()): ClientScoreResult {
  const status = getRecordStatusFlags(record);
  if (status.isContractCompleted) {
    return unavailableScore("Inactive client.");
  }

  if (status.isWrittenOff) {
    return unavailableScore("Written Off client.");
  }

  if (status.isAfterResult) {
    return unavailableScore("After Result client.");
  }

  if (status.isFullyPaid) {
    return unavailableScore("Fully Paid client.");
  }

  const contractTotal = parseMoneyValue(record.contractTotals);
  if (contractTotal === null || contractTotal <= 0) {
    return unavailableScore("No contract amount.");
  }

  const firstDueUtc = parseDateValue(record.payment1Date);
  if (firstDueUtc === null) {
    return unavailableScore("No Payment 1 date.");
  }

  const asOfUtc = getUtcDayStart(asOfDate.getTime());
  const scoreWindowStartUtc = shiftUtcMonths(asOfUtc, -SCORE_WINDOW_MONTHS);
  const paymentEvents = collectPaymentEvents(record, asOfUtc);
  const milestones = evaluateMilestones(record, contractTotal, firstDueUtc, asOfUtc, scoreWindowStartUtc, paymentEvents);

  const consideredMilestones = milestones.filter((item) => item.consideredInScoreWindow);
  const penaltyPoints = consideredMilestones.reduce((sum, item) => sum + item.penaltyPoints, 0);
  const lateMilestones = consideredMilestones.filter((item) => item.delayDays > 0).length;
  const maxDelayDays = consideredMilestones.reduce((maxValue, item) => Math.max(maxValue, item.delayDays), 0);
  const openMilestones = consideredMilestones.filter((item) => item.isOpenDebt).length;

  const recentWindowStartUtc = asOfUtc - RECENT_WINDOW_DAYS * DAY_IN_MS;
  const recentMilestones = milestones.filter((item) => item.dueUtc <= asOfUtc && item.dueUtc >= recentWindowStartUtc);
  const recentMilestonesOnTime =
    recentMilestones.length > 0 && recentMilestones.every((item) => item.delayDays <= LATE_GRACE_DAYS);
  const hasOlderDelay = milestones.some(
    (item) => item.dueUtc < recentWindowStartUtc && item.delayDays > LATE_GRACE_DAYS,
  );
  const hasEarlyCoverage = milestones.some((item) => item.isEarlyCoverage);

  const bonusPoints = hasEarlyCoverage && recentMilestonesOnTime ? BONUS_POINTS : 0;
  const recoveryPoints = penaltyPoints > 0 && recentMilestonesOnTime && hasOlderDelay ? RECOVERY_POINTS : 0;
  const rawScore = SCORE_START - penaltyPoints + bonusPoints + recoveryPoints;
  const score = clampNumber(Math.round(rawScore), SCORE_MIN, SCORE_MAX);
  const displayScore = toExternalDisplayScore(score);

  const explanation = buildExplanation({
    consideredMilestones: consideredMilestones.length,
    lateMilestones,
    maxDelayDays,
    penaltyPoints,
    bonusPoints,
    recoveryPoints,
    openMilestones,
  });

  return {
    score,
    displayScore,
    tone: resolveScoreTone(displayScore),
    penaltyPoints,
    bonusPoints,
    recoveryPoints,
    consideredMilestones: consideredMilestones.length,
    lateMilestones,
    maxDelayDays,
    openMilestones,
    explanation,
  };
}

function evaluateMilestones(
  record: ClientRecord,
  contractTotal: number,
  firstDueUtc: number,
  asOfUtc: number,
  scoreWindowStartUtc: number,
  paymentEvents: PaymentEvent[],
): MilestoneEval[] {
  const totalMilestones = PAYMENT_SLOTS.length;
  const downPayment = parseMoneyValue(record.payment1);
  const hasPositiveDownPayment = downPayment !== null && downPayment > 0;
  const firstPlannedAmount = hasPositiveDownPayment
    ? clampNumber(downPayment, 0, contractTotal)
    : contractTotal / totalMilestones;
  const recurringAmount =
    totalMilestones > 1
      ? hasPositiveDownPayment
        ? Math.max(0, (contractTotal - firstPlannedAmount) / (totalMilestones - 1))
        : contractTotal / totalMilestones
      : contractTotal;
  const dueDayOfMonth = new Date(firstDueUtc).getUTCDate();

  const cumulativeByEvent = buildCumulativeEvents(paymentEvents);

  return Array.from({ length: totalMilestones }, (_, index) => {
    const milestoneIndex = index + 1;
    const dueUtc = shiftUtcMonths(firstDueUtc, index, dueDayOfMonth);
    const plannedCumulative =
      milestoneIndex === totalMilestones
        ? contractTotal
        : firstPlannedAmount + recurringAmount * Math.max(0, milestoneIndex - 1);
    const catchUpUtc = findCatchUpDate(cumulativeByEvent, plannedCumulative);
    const effectiveCatchUpUtc = catchUpUtc ?? asOfUtc;
    const delayDays = effectiveCatchUpUtc > dueUtc ? Math.floor((effectiveCatchUpUtc - dueUtc) / DAY_IN_MS) : 0;
    const consideredInScoreWindow = dueUtc <= asOfUtc && dueUtc >= scoreWindowStartUtc;
    const penaltyPoints = consideredInScoreWindow ? penaltyForDelay(delayDays) : 0;
    const isOpenDebt = catchUpUtc === null && dueUtc <= asOfUtc;
    const isEarlyCoverage = catchUpUtc !== null && catchUpUtc <= dueUtc - EARLY_BONUS_DAYS * DAY_IN_MS;

    return {
      index: milestoneIndex,
      dueUtc,
      plannedCumulative,
      catchUpUtc,
      delayDays,
      penaltyPoints,
      consideredInScoreWindow,
      isOpenDebt,
      isEarlyCoverage,
    };
  });
}

function collectPaymentEvents(record: ClientRecord, asOfUtc: number): PaymentEvent[] {
  const events: PaymentEvent[] = [];

  for (const slot of PAYMENT_SLOTS) {
    const amount = parseMoneyValue(record[slot.amountKey]);
    const dateUtc = parseDateValue(record[slot.dateKey]);
    if (amount === null || dateUtc === null || amount <= 0 || dateUtc > asOfUtc) {
      continue;
    }

    events.push({
      amount,
      dateUtc,
    });
  }

  events.sort((left, right) => left.dateUtc - right.dateUtc);
  return events;
}

function buildCumulativeEvents(events: PaymentEvent[]): Array<{ dateUtc: number; cumulativePaid: number }> {
  let cumulativePaid = 0;
  return events.map((event) => {
    cumulativePaid += event.amount;
    return {
      dateUtc: event.dateUtc,
      cumulativePaid,
    };
  });
}

function findCatchUpDate(
  cumulativeEvents: Array<{ dateUtc: number; cumulativePaid: number }>,
  plannedCumulative: number,
): number | null {
  if (plannedCumulative <= 0) {
    return cumulativeEvents[0]?.dateUtc ?? null;
  }

  for (const event of cumulativeEvents) {
    if (event.cumulativePaid + EPSILON >= plannedCumulative) {
      return event.dateUtc;
    }
  }

  return null;
}

function penaltyForDelay(delayDays: number): number {
  if (delayDays <= LATE_GRACE_DAYS) {
    return 0;
  }

  if (delayDays < 30) {
    return delayDays;
  }

  if (delayDays < 60) {
    return 50;
  }

  if (delayDays < 90) {
    return 70;
  }

  return 90;
}

function buildExplanation(params: {
  consideredMilestones: number;
  lateMilestones: number;
  maxDelayDays: number;
  penaltyPoints: number;
  bonusPoints: number;
  recoveryPoints: number;
  openMilestones: number;
}): string {
  const parts: string[] = [];

  if (params.consideredMilestones === 0) {
    parts.push("No due milestones in the last 12 months.");
  } else if (params.lateMilestones === 0) {
    parts.push("All due milestones are on time (inside 3-day grace).");
  } else {
    parts.push(
      `Late milestones: ${params.lateMilestones}/${params.consideredMilestones}, max delay ${params.maxDelayDays} day${
        params.maxDelayDays === 1 ? "" : "s"
      }.`,
    );
  }

  parts.push(`Penalty: ${params.penaltyPoints}.`);

  if (params.openMilestones > 0) {
    parts.push(`Open overdue milestones: ${params.openMilestones}.`);
  }

  if (params.bonusPoints > 0) {
    parts.push(`Early payment bonus: +${params.bonusPoints}.`);
  }

  if (params.recoveryPoints > 0) {
    parts.push(`Recovery credit: +${params.recoveryPoints}.`);
  }

  return parts.join(" ");
}

function unavailableScore(reason: string): ClientScoreResult {
  return {
    score: null,
    displayScore: null,
    tone: "neutral",
    penaltyPoints: 0,
    bonusPoints: 0,
    recoveryPoints: 0,
    consideredMilestones: 0,
    lateMilestones: 0,
    maxDelayDays: 0,
    openMilestones: 0,
    explanation: reason,
  };
}

function resolveScoreTone(score: number | null): ClientScoreResult["tone"] {
  if (score === null) {
    return "neutral";
  }

  if (score >= 95) {
    return "success";
  }
  if (score >= 80) {
    return "info";
  }
  if (score >= 60) {
    return "warning";
  }
  return "danger";
}

function getUtcDayStart(timestamp: number): number {
  const value = new Date(timestamp);
  return Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate());
}

function shiftUtcMonths(baseUtc: number, deltaMonths: number, pinnedDay?: number): number {
  const baseDate = new Date(baseUtc);
  const day = pinnedDay ?? baseDate.getUTCDate();
  const firstDayTargetMonth = Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth() + deltaMonths, 1);
  const targetMonthDate = new Date(firstDayTargetMonth);
  const daysInTargetMonth = new Date(
    Date.UTC(targetMonthDate.getUTCFullYear(), targetMonthDate.getUTCMonth() + 1, 0),
  ).getUTCDate();
  const safeDay = Math.min(day, daysInTargetMonth);

  return Date.UTC(targetMonthDate.getUTCFullYear(), targetMonthDate.getUTCMonth(), safeDay);
}

function resolvePaidTotal(totalPayments?: number | null, payments?: number[]): number {
  const parsedTotalPayments = toFiniteNullableNumber(totalPayments);
  if (parsedTotalPayments !== null) {
    return parsedTotalPayments;
  }

  if (!Array.isArray(payments) || payments.length === 0) {
    return 0;
  }

  let total = 0;
  for (const amount of payments) {
    if (typeof amount !== "number" || !Number.isFinite(amount)) {
      continue;
    }

    total += amount;
  }

  return total;
}

function sigmoid(value: number): number {
  return 1 / (1 + Math.exp(-value));
}

function toFiniteNumber(value: number | null | undefined): number {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function toFiniteNullableNumber(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function clampNumber(value: number, minValue: number, maxValue: number): number {
  return Math.min(maxValue, Math.max(minValue, value));
}

function toExternalDisplayScore(internalScore: number): number {
  const capped = clampNumber(internalScore, SCORE_MIN, SCORE_START);
  if (capped <= 0) {
    return 0;
  }
  return Math.ceil(capped / 10) * 10;
}

export function formatScoreAsOfDate(value: Date): string {
  return formatDate(value.toISOString());
}
