import { parseDateValue, parseMoneyValue } from "@/features/client-payments/domain/calculations";
import type { GhlClientCommunicationsPayload } from "@/shared/types/ghlCommunications";
import type { GhlClientBasicNotePayload } from "@/shared/types/ghlNotes";
import type { ClientRecord } from "@/shared/types/records";

const DAY_MS = 24 * 60 * 60 * 1000;
const RESPONSE_PAIR_MAX_HOURS = 72;
const SAFE_CLIENT_LIMIT = 5;
const RISK_PHRASES = ["cancel", "refund", "scam", "not working"] as const;

const POSITIVE_TOKENS = ["thanks", "thank you", "great", "good", "resolved", "perfect", "ok", "paid"];
const NEGATIVE_TOKENS = [
  "cancel",
  "refund",
  "scam",
  "not working",
  "bad",
  "terrible",
  "angry",
  "complaint",
  "lawyer",
  "fraud",
  "chargeback",
];
const FAILED_PAYMENT_TOKENS = [
  "failed payment",
  "payment failed",
  "declined",
  "chargeback",
  "insufficient funds",
  "nsf",
  "неуспеш",
  "ошибка платеж",
];
const TRUE_VALUES = new Set(["1", "true", "yes", "on", "active", "completed"]);
const ABOUT_CLIENT_NOTES_KEYS = [
  "aboutClient",
  "aboutClientNotes",
  "about_client",
  "about_client_notes",
  "about client",
  "About Client",
] as const;
const ABOUT_CLIENT_DATE_KEYS = [
  "aboutClientDate",
  "aboutClientNegotiationDate",
  "about_client_date",
  "about_client_negotiation_date",
  "negotiationDate",
  "dateOfNegotiation",
] as const;

export const CLIENT_HEALTH_SQL_EXAMPLE = `-- SAFE MODE: LIMITED TO 5 CLIENTS
SELECT id, record, source_state_updated_at, updated_at
FROM public.client_records_v2
WHERE source_state_row_id = $1
  AND LOWER(BTRIM(COALESCE(record->>'active', ''))) IN ('1', 'true', 'yes', 'on')
ORDER BY COALESCE(source_state_updated_at, updated_at, created_at) DESC NULLS LAST, id DESC
LIMIT 5;`;

export const CLIENT_HEALTH_SCORING_PSEUDOCODE = [
  "healthIndex = launchSpeed * 0.15 + executionScore * 0.30 + paymentsScore * 0.25 + engagementScore * 0.15 + communicationScore * 0.15",
  "churnProbability = clamp(100 - healthIndex + launchPenalty + paymentPenalty + opsPenalty + communicationPenalty, 0, 100)",
  "status = healthIndex >= 80 ? 'Здоровый' : healthIndex >= 60 ? 'Предупреждение' : healthIndex >= 40 ? 'Риск' : 'Критично'",
];

export const CLIENT_HEALTH_FLAGS_LOGIC = [
  "Риск запуска: delayStartDays > 7",
  "Риск оплаты: avgPaymentDelayDays > 7 OR failedPayment = true OR consecutiveOverdue >= 2",
  "Операционная пауза: daysSinceLastActivity > 14",
  "Риск коммуникации: negativeStreak >= 2 OR riskPhraseFound OR daysSinceLastContact > 14",
  "Риск отмены: churnProbability >= 60",
];

export interface ClientHealthSource {
  record: ClientRecord;
  memo: GhlClientBasicNotePayload | null;
  communications: GhlClientCommunicationsPayload | null;
}

export type ClientHealthStatus = "Здоровый" | "Предупреждение" | "Риск" | "Критично";

export interface ClientHealthScoreBreakdown {
  launchSpeed: number;
  execution: number;
  payments: number;
  engagement: number;
  communication: number;
  launchContribution: number;
  executionContribution: number;
  paymentsContribution: number;
  engagementContribution: number;
  communicationContribution: number;
  total: number;
}

export interface ClientHealthExplanation {
  what: string[];
  when: string[];
  why: string[];
  aboutClient: string[];
  launch: string[];
  payments: string[];
  execution: string[];
  communication: string[];
  risks: string[];
  scoreBreakdown: ClientHealthScoreBreakdown;
}

export interface ClientHealthRow {
  clientId: string;
  clientName: string;
  clientSurname: string;
  overview: {
    status: ClientHealthStatus;
    healthIndex: number;
    churnProbability: number;
    daysInWork: number;
    totalRevenue: number;
    monthlyPayment: number;
    tone: "green" | "yellow" | "red";
  };
  timeline: {
    saleDate: string;
    startDate: string;
    daysSaleToStart: number | null;
    daysSinceLastActivity: number | null;
    daysSinceLastContact: number | null;
    daysSinceLastPayment: number | null;
    launchDelayFlag: boolean;
    inactivityFlag: boolean;
  };
  payments: {
    expectedMonthlyPayment: number;
    lastPaymentDate: string;
    averageDelayDays: number;
    onTimePercent: number;
    overdueCount: number;
    consecutiveOverdue: number;
    disciplineScore: number;
    hasFailedPayment: boolean;
    riskFlags: string[];
  };
  execution: {
    promisedWorkVolume: string;
    expectedTermDays: number;
    daysInWork: number;
    elapsedTermPercent: number;
    activityCount: number;
    activity30d: number;
    score: number;
    riskFlag: boolean;
  };
  communication: {
    contacts30d: number;
    inboundPercent: number;
    outboundPercent: number;
    avgResponseHours: number;
    sentimentIndex: number;
    score: number;
    riskPhrases: string[];
    negativeStreak: number;
    flags: string[];
  };
  risks: {
    launchRisk: boolean;
    paymentRisk: boolean;
    operationalPauseRisk: boolean;
    communicationRisk: boolean;
    cancellationRisk: boolean;
    warnings: string[];
    churnCategory: "Стабильно" | "Наблюдать" | "Высокий риск";
  };
  explanation: ClientHealthExplanation;
}

interface PaymentAnalysis {
  expectedMonthlyPayment: number;
  totalRevenue: number;
  lastPaymentAt: number | null;
  lastPaymentDate: string;
  averageDelayDays: number;
  onTimePercent: number;
  overdueCount: number;
  consecutiveOverdue: number;
  hasFailedPayment: boolean;
  disciplineScore: number;
  disciplineScore100: number;
  riskFlags: string[];
  paymentEventsCount: number;
  paymentEvents30d: number;
}

interface CommunicationAnalysis {
  contacts30d: number;
  inboundPercent: number;
  outboundPercent: number;
  avgResponseHours: number;
  sentimentIndex: number;
  score: number;
  score100: number;
  riskPhrases: string[];
  negativeStreak: number;
  flags: string[];
  lastContactAt: number | null;
  contactsTotal: number;
}

interface AboutClientContext {
  notes: string;
  negotiationAt: number | null;
}

export function buildClientHealthRows(sources: ClientHealthSource[], asOfDate = new Date()): ClientHealthRow[] {
  const now = Number.isFinite(asOfDate.getTime()) ? asOfDate.getTime() : Date.now();

  return (Array.isArray(sources) ? sources : [])
    .slice(0, SAFE_CLIENT_LIMIT)
    .map((source, index) => buildClientHealthRow(source, now, index))
    .sort((left, right) => right.overview.healthIndex - left.overview.healthIndex);
}

function buildClientHealthRow(source: ClientHealthSource, nowMs: number, index: number): ClientHealthRow {
  const record = source?.record;
  const memo = source?.memo;
  const communications = source?.communications;

  const clientName = normalizeText(record?.clientName) || `Клиент ${index + 1}`;
  const clientId = normalizeText(record?.id) || `safe-client-${index + 1}`;
  const clientSurname = resolveClientSurname(clientName);
  const aboutClient = resolveAboutClientContext(record, memo);

  const saleAt = resolveSaleDate(record);
  const startAt = resolveStartDate(record, memo, aboutClient);
  const daysSaleToStart = saleAt !== null && startAt !== null ? Math.max(0, diffDays(startAt, saleAt)) : null;
  const daysInWork = resolveDaysInWork(startAt, saleAt, nowMs);

  const paymentAnalysis = analyzePayments(record, memo, communications, nowMs, aboutClient.notes);
  const communicationAnalysis = analyzeCommunications(communications, nowMs);
  const memoTimestamp = parseDateValue(memo?.memoCreatedAt || memo?.noteCreatedAt || "");

  const lastActivityAt = pickMostRecentTimestamp([
    paymentAnalysis.lastPaymentAt,
    communicationAnalysis.lastContactAt,
    memoTimestamp,
    parseDateValue(record?.scoreUpdatedAt),
    aboutClient.negotiationAt,
  ]);

  const daysSinceLastActivity = lastActivityAt !== null ? diffDays(nowMs, lastActivityAt) : null;
  const daysSinceLastContact = communicationAnalysis.lastContactAt !== null
    ? diffDays(nowMs, communicationAnalysis.lastContactAt)
    : null;
  const daysSinceLastPayment = paymentAnalysis.lastPaymentAt !== null ? diffDays(nowMs, paymentAnalysis.lastPaymentAt) : null;

  const launchSpeed = scoreLaunchSpeed(daysSaleToStart);
  const engagementScore100 = scoreEngagement(daysSinceLastActivity, communicationAnalysis.contacts30d);
  const execution = analyzeExecution(record, memo, communicationAnalysis, paymentAnalysis, daysInWork, aboutClient.notes);

  const healthIndex = clampNumber(
    Math.round(
      launchSpeed * 0.15 +
        execution.score100 * 0.3 +
        paymentAnalysis.disciplineScore100 * 0.25 +
        engagementScore100 * 0.15 +
        communicationAnalysis.score100 * 0.15,
    ),
    0,
    100,
  );

  const status = resolveHealthStatus(healthIndex);
  const tone = resolveHealthTone(healthIndex);

  const launchDelayFlag = daysSaleToStart !== null && daysSaleToStart > 7;
  const inactivityFlag = daysSinceLastActivity !== null && daysSinceLastActivity > 14;

  const churnProbability = computeChurnProbability({
    healthIndex,
    launchDelayFlag,
    paymentRisk: paymentAnalysis.riskFlags.length > 0,
    communicationRisk: communicationAnalysis.flags.length > 0,
    executionRisk: execution.riskFlag,
    daysSinceLastContact,
  });

  const risks = buildRiskPanel({
    launchDelayFlag,
    paymentRisk: paymentAnalysis.riskFlags.length > 0,
    inactivityFlag,
    communicationRisk: communicationAnalysis.flags.length > 0,
    churnProbability,
  });

  const scoreBreakdown: ClientHealthScoreBreakdown = {
    launchSpeed,
    execution: execution.score100,
    payments: paymentAnalysis.disciplineScore100,
    engagement: engagementScore100,
    communication: communicationAnalysis.score100,
    launchContribution: roundTo1(launchSpeed * 0.15),
    executionContribution: roundTo1(execution.score100 * 0.3),
    paymentsContribution: roundTo1(paymentAnalysis.disciplineScore100 * 0.25),
    engagementContribution: roundTo1(engagementScore100 * 0.15),
    communicationContribution: roundTo1(communicationAnalysis.score100 * 0.15),
    total: healthIndex,
  };

  const explanation = buildClientExplanation({
    status,
    healthIndex,
    churnProbability,
    saleAt,
    startAt,
    daysSaleToStart,
    daysInWork,
    daysSinceLastActivity,
    daysSinceLastContact,
    daysSinceLastPayment,
    paymentAnalysis,
    execution,
    communicationAnalysis,
    risks,
    scoreBreakdown,
    aboutClient,
  });

  return {
    clientId,
    clientName,
    clientSurname,
    overview: {
      status,
      healthIndex,
      churnProbability,
      daysInWork,
      totalRevenue: paymentAnalysis.totalRevenue,
      monthlyPayment: paymentAnalysis.expectedMonthlyPayment,
      tone,
    },
    timeline: {
      saleDate: formatDateSafe(saleAt),
      startDate: formatDateSafe(startAt),
      daysSaleToStart,
      daysSinceLastActivity,
      daysSinceLastContact,
      daysSinceLastPayment,
      launchDelayFlag,
      inactivityFlag,
    },
    payments: {
      expectedMonthlyPayment: paymentAnalysis.expectedMonthlyPayment,
      lastPaymentDate: paymentAnalysis.lastPaymentDate,
      averageDelayDays: paymentAnalysis.averageDelayDays,
      onTimePercent: paymentAnalysis.onTimePercent,
      overdueCount: paymentAnalysis.overdueCount,
      consecutiveOverdue: paymentAnalysis.consecutiveOverdue,
      disciplineScore: paymentAnalysis.disciplineScore,
      hasFailedPayment: paymentAnalysis.hasFailedPayment,
      riskFlags: paymentAnalysis.riskFlags,
    },
    execution,
    communication: {
      contacts30d: communicationAnalysis.contacts30d,
      inboundPercent: communicationAnalysis.inboundPercent,
      outboundPercent: communicationAnalysis.outboundPercent,
      avgResponseHours: communicationAnalysis.avgResponseHours,
      sentimentIndex: communicationAnalysis.sentimentIndex,
      score: communicationAnalysis.score,
      riskPhrases: communicationAnalysis.riskPhrases,
      negativeStreak: communicationAnalysis.negativeStreak,
      flags: communicationAnalysis.flags,
    },
    risks,
    explanation,
  };
}

interface BuildClientExplanationInput {
  status: ClientHealthStatus;
  healthIndex: number;
  churnProbability: number;
  saleAt: number | null;
  startAt: number | null;
  daysSaleToStart: number | null;
  daysInWork: number;
  daysSinceLastActivity: number | null;
  daysSinceLastContact: number | null;
  daysSinceLastPayment: number | null;
  paymentAnalysis: PaymentAnalysis;
  execution: ClientHealthRow["execution"] & { score100: number };
  communicationAnalysis: CommunicationAnalysis;
  risks: ClientHealthRow["risks"];
  scoreBreakdown: ClientHealthScoreBreakdown;
  aboutClient: AboutClientContext;
}

function buildClientExplanation(input: BuildClientExplanationInput): ClientHealthExplanation {
  const what = [
    `Статус клиента: ${input.status}.`,
    `Индекс здоровья: ${input.healthIndex}/100.`,
    `Вероятность отмены: ${input.churnProbability}%.`,
    `Категория риска: ${input.risks.churnCategory}.`,
  ];

  const when = [
    `Дата продажи: ${formatDateSafe(input.saleAt)}.`,
    `Дата старта: ${formatDateSafe(input.startAt)}.`,
    `Дата переговоров (About Client): ${formatDateSafe(input.aboutClient.negotiationAt)}.`,
    `Дней от продажи до старта: ${formatOptionalNumber(input.daysSaleToStart)}.`,
    `Дней в работе: ${input.daysInWork}.`,
    `С последней активности: ${formatOptionalNumber(input.daysSinceLastActivity)} дн.`,
    `С последнего контакта: ${formatOptionalNumber(input.daysSinceLastContact)} дн.`,
    `С последнего платежа: ${formatOptionalNumber(input.daysSinceLastPayment)} дн.`,
  ];

  const formulaLine =
    `Индекс = запуск ${input.scoreBreakdown.launchContribution} + исполнение ${input.scoreBreakdown.executionContribution}` +
    ` + платежи ${input.scoreBreakdown.paymentsContribution} + вовлечённость ${input.scoreBreakdown.engagementContribution}` +
    ` + коммуникация ${input.scoreBreakdown.communicationContribution} = ${input.scoreBreakdown.total}.`;

  const why = [
    formulaLine,
    `Скорость запуска: ${input.scoreBreakdown.launchSpeed}/100, исполнение: ${input.scoreBreakdown.execution}/100,` +
      ` платежи: ${input.scoreBreakdown.payments}/100, вовлечённость: ${input.scoreBreakdown.engagement}/100,` +
      ` коммуникация: ${input.scoreBreakdown.communication}/100.`,
    "Контекст из блока About Client использован для интерпретации договорённостей и даты переговоров.",
  ];

  const aboutClient = buildAboutClientExplanation(input.aboutClient);

  const launch: string[] = [];
  if (input.daysSaleToStart === null) {
    launch.push("Не удалось точно определить дату запуска, поэтому метрика запуска усреднена.");
  } else if (input.daysSaleToStart <= 7) {
    launch.push(`Запуск без критичной задержки (${input.daysSaleToStart} дн.), поэтому штраф за старт минимальный.`);
  } else {
    launch.push(`Запуск с задержкой ${input.daysSaleToStart} дн. (> 7), это снижает индекс и повышает риск отмены.`);
  }

  const payments: string[] = [
    `Платёжная дисциплина: ${input.paymentAnalysis.disciplineScore.toFixed(1)}/10.`,
    `Оплаты вовремя: ${input.paymentAnalysis.onTimePercent}%, средняя задержка: ${input.paymentAnalysis.averageDelayDays.toFixed(1)} дн.`,
    `Просрочек: ${input.paymentAnalysis.overdueCount}, подряд: ${input.paymentAnalysis.consecutiveOverdue}.`,
  ];
  if (input.paymentAnalysis.riskFlags.length > 0) {
    payments.push(`Причины риска оплаты: ${input.paymentAnalysis.riskFlags.join("; ")}.`);
  } else {
    payments.push("Критичных платёжных флагов не обнаружено.");
  }

  const execution: string[] = [
    `Оценка исполнения: ${input.execution.score.toFixed(1)}/10.`,
    `Ожидаемый срок: ${input.execution.expectedTermDays} дн., прошедшая доля срока: ${input.execution.elapsedTermPercent}%.`,
    `Активностей всего: ${input.execution.activityCount}, за 30 дней: ${input.execution.activity30d}.`,
  ];
  if (input.execution.riskFlag) {
    execution.push("Сработал флаг исполнения: прошло >50% срока при низкой активности.");
  } else {
    execution.push("Флаг исполнения не сработал: активность соответствует текущему этапу срока.");
  }

  const communication: string[] = [
    `Оценка коммуникации: ${input.communicationAnalysis.score.toFixed(1)}/10.`,
    `Контактов за 30 дней: ${input.communicationAnalysis.contacts30d}, негативных подряд: ${input.communicationAnalysis.negativeStreak}.`,
    `Тональность: ${input.communicationAnalysis.sentimentIndex.toFixed(2)}, среднее время ответа: ${input.communicationAnalysis.avgResponseHours.toFixed(1)} ч.`,
  ];
  if (input.communicationAnalysis.flags.length > 0) {
    communication.push(`Причины риска коммуникации: ${input.communicationAnalysis.flags.join("; ")}.`);
  } else {
    communication.push("Критичных коммуникационных флагов не обнаружено.");
  }

  const risks = input.risks.warnings.length
    ? input.risks.warnings.map((warning) => `Активный риск: ${warning}.`)
    : ["Активных предупреждений нет."];

  return {
    what,
    when,
    why,
    aboutClient,
    launch,
    payments,
    execution,
    communication,
    risks,
    scoreBreakdown: input.scoreBreakdown,
  };
}

function resolveAboutClientContext(
  record: ClientRecord,
  memo: GhlClientBasicNotePayload | null,
): AboutClientContext {
  const memoNotes = normalizeText(memo?.aboutClientBody) || normalizeText(memo?.aboutClientTitle);
  const recordNotes = readRecordTextByKeys(record, ABOUT_CLIENT_NOTES_KEYS);
  const notes = memoNotes || recordNotes;
  const memoNegotiationAt = parseDateValue(memo?.aboutClientCreatedAt || "");
  const directNegotiationDateValue = readRecordTextByKeys(record, ABOUT_CLIENT_DATE_KEYS);
  const directNegotiationAt = memoNegotiationAt ?? parseDateValue(directNegotiationDateValue);
  const fallbackNegotiationAt =
    directNegotiationAt !== null
      ? directNegotiationAt
      : parseDateFromText(notes, ["переговор", "договор", "meeting", "call", "agreement", "agreed"]);

  return {
    notes,
    negotiationAt: fallbackNegotiationAt,
  };
}

function buildAboutClientExplanation(context: AboutClientContext): string[] {
  const lines: string[] = [];

  if (context.notes) {
    lines.push(`Заметки менеджера (About Client): ${truncateExplanationText(context.notes, 380)}.`);
  } else {
    lines.push("В карточке About Client заметки менеджера не заполнены.");
  }

  if (context.negotiationAt !== null) {
    lines.push(`Дата переговоров из About Client: ${formatDateSafe(context.negotiationAt)}.`);
  } else {
    lines.push("Дата переговоров в блоке About Client не найдена.");
  }

  return lines;
}

function analyzePayments(
  record: ClientRecord,
  memo: GhlClientBasicNotePayload | null,
  communications: GhlClientCommunicationsPayload | null,
  nowMs: number,
  aboutClientNotes: string,
): PaymentAnalysis {
  const paymentEvents: Array<{ slot: number; amount: number; actualAt: number | null; expectedAt: number | null; delayDays: number | null }> = [];

  const firstPlannedAt = parseDateValue(record?.payment1Date);
  const paymentAmounts: number[] = [];

  for (let slot = 1; slot <= 36; slot += 1) {
    const amount = parseMoneyValue(record?.[`payment${slot}` as keyof ClientRecord]);
    const actualAt = parseDateValue(record?.[`payment${slot}Date` as keyof ClientRecord]);
    if (amount === null && actualAt === null) {
      continue;
    }

    const safeAmount = amount === null ? 0 : Math.max(0, amount);
    const expectedAt = firstPlannedAt !== null ? addMonthsUtc(firstPlannedAt, slot - 1) : null;
    const delayDays = expectedAt !== null && actualAt !== null ? Math.max(0, diffDays(actualAt, expectedAt)) : null;

    paymentEvents.push({
      slot,
      amount: safeAmount,
      actualAt,
      expectedAt,
      delayDays,
    });

    if (safeAmount > 0) {
      paymentAmounts.push(safeAmount);
    }
  }

  const totalRevenue = resolveTotalRevenue(record, paymentAmounts);
  const expectedMonthlyPayment = resolveExpectedMonthlyPayment(record, paymentAmounts);

  const actualPayments = paymentEvents.filter((event) => event.actualAt !== null);
  const lastPaymentAt = pickMostRecentTimestamp(actualPayments.map((event) => event.actualAt));

  const delays = actualPayments.map((event) => event.delayDays).filter((value): value is number => value !== null);
  const averageDelayDays = delays.length ? roundTo1(delays.reduce((sum, value) => sum + value, 0) / delays.length) : 0;

  const onTimePayments = delays.filter((delay) => delay <= 3).length;
  const onTimePercent = delays.length ? clampNumber(Math.round((onTimePayments / delays.length) * 100), 0, 100) : 100;
  const overdueCount = delays.filter((delay) => delay > 7).length;
  const consecutiveOverdue = computeConsecutiveOverdue(actualPayments);

  const textCorpus = [
    record?.notes,
    aboutClientNotes,
    memo?.noteBody,
    memo?.memoBody,
    extractCommunicationCorpus(communications),
  ]
    .map((value) => normalizeText(value).toLowerCase())
    .join(" \n");

  const hasFailedPayment = FAILED_PAYMENT_TOKENS.some((token) => textCorpus.includes(token));

  const riskFlags: string[] = [];
  if (averageDelayDays > 7) {
    riskFlags.push("Средняя задержка > 7 дней");
  }
  if (hasFailedPayment) {
    riskFlags.push("Обнаружен неуспешный платёж");
  }
  if (consecutiveOverdue >= 2) {
    riskFlags.push("2+ просрочки подряд");
  }

  const disciplineScore = resolvePaymentDisciplineScore({
    averageDelayDays,
    onTimePercent,
    overdueCount,
    consecutiveOverdue,
    hasFailedPayment,
  });

  const paymentEvents30d = actualPayments.filter((event) => {
    if (event.actualAt === null) {
      return false;
    }
    return nowMs - event.actualAt <= 30 * DAY_MS;
  }).length;

  return {
    expectedMonthlyPayment,
    totalRevenue,
    lastPaymentAt,
    lastPaymentDate: formatDateSafe(lastPaymentAt),
    averageDelayDays,
    onTimePercent,
    overdueCount,
    consecutiveOverdue,
    hasFailedPayment,
    disciplineScore,
    disciplineScore100: clampNumber(Math.round(disciplineScore * 10), 0, 100),
    riskFlags,
    paymentEventsCount: actualPayments.length,
    paymentEvents30d,
  };
}

function analyzeExecution(
  record: ClientRecord,
  memo: GhlClientBasicNotePayload | null,
  communicationAnalysis: CommunicationAnalysis,
  paymentAnalysis: PaymentAnalysis,
  daysInWork: number,
  aboutClientNotes: string,
): ClientHealthRow["execution"] & { score100: number } {
  const promisedText = [memo?.memoBody, memo?.noteBody, record?.notes, aboutClientNotes]
    .map((value) => normalizeText(value))
    .join(" \n");
  const promisedWorkVolume = parsePromisedWorkVolume(promisedText);
  const expectedTermDays = parseExpectedTermDays(promisedText);
  const elapsedTermPercent = expectedTermDays > 0
    ? clampNumber(Math.round((daysInWork / expectedTermDays) * 100), 0, 400)
    : 0;

  const activityCount = communicationAnalysis.contactsTotal + paymentAnalysis.paymentEventsCount + (normalizeText(memo?.memoBody) ? 1 : 0);
  const activity30d = communicationAnalysis.contacts30d + paymentAnalysis.paymentEvents30d;

  let score = 6;
  if (activity30d >= 6) {
    score += 2;
  } else if (activity30d >= 3) {
    score += 1;
  } else if (activity30d <= 1) {
    score -= 2;
  }

  if (elapsedTermPercent > 50 && activity30d < 2) {
    score -= 2;
  }
  if (elapsedTermPercent > 90 && activity30d < 3) {
    score -= 1;
  }

  const finalScore = clampNumber(roundTo1(score), 0, 10);
  const riskFlag = elapsedTermPercent > 50 && activity30d < 2;

  return {
    promisedWorkVolume,
    expectedTermDays,
    daysInWork,
    elapsedTermPercent,
    activityCount,
    activity30d,
    score: finalScore,
    score100: clampNumber(Math.round(finalScore * 10), 0, 100),
    riskFlag,
  };
}

function analyzeCommunications(
  communications: GhlClientCommunicationsPayload | null,
  nowMs: number,
): CommunicationAnalysis {
  const items = Array.isArray(communications?.items) ? communications.items : [];
  const normalizedItems = items
    .map((item) => {
      const createdAt = parseDateValue(item?.createdAt);
      if (createdAt === null) {
        return null;
      }
      const directionRaw = normalizeText(item?.direction).toLowerCase();
      const direction = directionRaw.includes("in")
        ? "inbound"
        : directionRaw.includes("out")
          ? "outbound"
          : "unknown";
      const text = `${normalizeText(item?.body)} ${normalizeText(item?.transcript)}`.trim();
      return {
        createdAt,
        direction,
        text,
      };
    })
    .filter((item): item is { createdAt: number; direction: "inbound" | "outbound" | "unknown"; text: string } => Boolean(item))
    .sort((left, right) => left.createdAt - right.createdAt);

  const lastContactAt = pickMostRecentTimestamp(normalizedItems.map((item) => item.createdAt));
  const contacts30d = normalizedItems.filter((item) => nowMs - item.createdAt <= 30 * DAY_MS).length;

  const inbound = normalizedItems.filter((item) => item.direction === "inbound").length;
  const outbound = normalizedItems.filter((item) => item.direction === "outbound").length;
  const knownDirections = inbound + outbound;

  const inboundPercent = knownDirections ? clampNumber(Math.round((inbound / knownDirections) * 100), 0, 100) : 0;
  const outboundPercent = knownDirections ? clampNumber(Math.round((outbound / knownDirections) * 100), 0, 100) : 0;

  const avgResponseHours = computeAverageResponseHours(normalizedItems);
  const sentimentScores = normalizedItems.map((item) => scoreSentiment(item.text));
  const sentimentIndex = sentimentScores.length
    ? clampNumber(roundTo2(sentimentScores.reduce((sum, value) => sum + value, 0) / sentimentScores.length), -1, 1)
    : 0;

  const fullCorpus = normalizedItems.map((item) => item.text.toLowerCase()).join(" \n");
  const riskPhrases = RISK_PHRASES.filter((phrase) => fullCorpus.includes(phrase));
  const negativeStreak = computeNegativeStreak(sentimentScores);

  const daysSinceLastContact = lastContactAt !== null ? diffDays(nowMs, lastContactAt) : null;

  const flags: string[] = [];
  if (negativeStreak >= 2) {
    flags.push("2+ негативных контакта подряд");
  }
  if (riskPhrases.length > 0) {
    flags.push(`Найдены риск-фразы: ${riskPhrases.join(", ")}`);
  }
  if (daysSinceLastContact !== null && daysSinceLastContact > 14) {
    flags.push("Нет контакта более 14 дней");
  }

  let score = 10;
  if (daysSinceLastContact !== null && daysSinceLastContact > 14) {
    score -= 3;
  }
  if (negativeStreak >= 2) {
    score -= 2;
  }
  if (riskPhrases.length > 0) {
    score -= 2;
  }
  if (sentimentIndex < -0.2) {
    score -= 2;
  }
  if (avgResponseHours > 48) {
    score -= 1;
  }
  if (contacts30d === 0) {
    score -= 1;
  }

  const finalScore = clampNumber(roundTo1(score), 0, 10);

  return {
    contacts30d,
    inboundPercent,
    outboundPercent,
    avgResponseHours,
    sentimentIndex,
    score: finalScore,
    score100: clampNumber(Math.round(finalScore * 10), 0, 100),
    riskPhrases,
    negativeStreak,
    flags,
    lastContactAt,
    contactsTotal: normalizedItems.length,
  };
}

function computeAverageResponseHours(
  items: Array<{ createdAt: number; direction: "inbound" | "outbound" | "unknown" }>,
): number {
  if (!items.length) {
    return 0;
  }

  const waits: number[] = [];
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (item.direction !== "outbound") {
      continue;
    }

    for (let nextIndex = index + 1; nextIndex < items.length; nextIndex += 1) {
      const nextItem = items[nextIndex];
      if (nextItem.direction !== "inbound") {
        continue;
      }

      const deltaHours = (nextItem.createdAt - item.createdAt) / (60 * 60 * 1000);
      if (deltaHours >= 0 && deltaHours <= RESPONSE_PAIR_MAX_HOURS) {
        waits.push(deltaHours);
      }
      break;
    }
  }

  if (!waits.length) {
    return 0;
  }

  return roundTo1(waits.reduce((sum, value) => sum + value, 0) / waits.length);
}

function scoreSentiment(textRaw: string): number {
  const text = normalizeText(textRaw).toLowerCase();
  if (!text) {
    return 0;
  }

  let positive = 0;
  for (const token of POSITIVE_TOKENS) {
    if (text.includes(token)) {
      positive += 1;
    }
  }

  let negative = 0;
  for (const token of NEGATIVE_TOKENS) {
    if (text.includes(token)) {
      negative += 1;
    }
  }

  if (positive === 0 && negative === 0) {
    return 0;
  }

  return clampNumber((positive - negative) / Math.max(positive + negative, 1), -1, 1);
}

function computeNegativeStreak(sentimentScores: number[]): number {
  let streak = 0;
  for (let index = sentimentScores.length - 1; index >= 0; index -= 1) {
    if (sentimentScores[index] < 0) {
      streak += 1;
      continue;
    }
    break;
  }
  return streak;
}

function resolvePaymentDisciplineScore(input: {
  averageDelayDays: number;
  onTimePercent: number;
  overdueCount: number;
  consecutiveOverdue: number;
  hasFailedPayment: boolean;
}): number {
  let score = 10;

  if (input.averageDelayDays > 7) {
    score -= 3;
  } else if (input.averageDelayDays > 3) {
    score -= 1.5;
  }

  if (input.onTimePercent < 50) {
    score -= 3;
  } else if (input.onTimePercent < 70) {
    score -= 2;
  } else if (input.onTimePercent < 85) {
    score -= 1;
  }

  if (input.overdueCount >= 2) {
    score -= 1.5;
  }
  if (input.consecutiveOverdue >= 2) {
    score -= 2;
  }
  if (input.hasFailedPayment) {
    score -= 2;
  }

  return clampNumber(roundTo1(score), 0, 10);
}

function computeConsecutiveOverdue(
  events: Array<{ slot: number; delayDays: number | null }>,
): number {
  const sorted = events
    .slice()
    .sort((left, right) => left.slot - right.slot)
    .filter((event) => event.delayDays !== null);

  let maxStreak = 0;
  let currentStreak = 0;
  for (const event of sorted) {
    if ((event.delayDays || 0) > 7) {
      currentStreak += 1;
      if (currentStreak > maxStreak) {
        maxStreak = currentStreak;
      }
      continue;
    }
    currentStreak = 0;
  }

  return maxStreak;
}

function resolveExpectedMonthlyPayment(record: ClientRecord, paymentAmounts: number[]): number {
  const futurePayment = parseMoneyValue(record?.futurePayment);
  if (futurePayment !== null && futurePayment > 0) {
    return roundTo2(futurePayment);
  }

  const payment1 = parseMoneyValue(record?.payment1);
  if (payment1 !== null && payment1 > 0) {
    return roundTo2(payment1);
  }

  const contractTotal = parseMoneyValue(record?.contractTotals) || 0;
  if (contractTotal > 0) {
    const estimated = contractTotal / 6;
    return roundTo2(estimated);
  }

  if (paymentAmounts.length > 0) {
    return roundTo2(paymentAmounts[0]);
  }

  return 0;
}

function resolveTotalRevenue(record: ClientRecord, paymentAmounts: number[]): number {
  const total = parseMoneyValue(record?.totalPayments);
  if (total !== null && total >= 0) {
    return roundTo2(total);
  }

  if (!paymentAmounts.length) {
    return 0;
  }

  return roundTo2(paymentAmounts.reduce((sum, value) => sum + value, 0));
}

function resolveSaleDate(record: ClientRecord): number | null {
  return parseDateValue(record?.createdAt) ?? parseDateValue(record?.payment1Date);
}

function resolveStartDate(
  record: ClientRecord,
  memo: GhlClientBasicNotePayload | null,
  aboutClient: AboutClientContext,
): number | null {
  const directRecordDate = parseDateValue(record?.startedInWork);
  if (directRecordDate !== null) {
    return directRecordDate;
  }

  const text = `${normalizeText(memo?.memoBody)} ${normalizeText(memo?.noteBody)} ${normalizeText(aboutClient.notes)}`;
  const parsedFromText = parseDateFromText(text, ["start", "старт", "начал", "started"]);
  if (parsedFromText !== null) {
    return parsedFromText;
  }

  if (aboutClient.negotiationAt !== null) {
    return aboutClient.negotiationAt;
  }

  return parseDateValue(record?.payment1Date);
}

function resolveDaysInWork(startAt: number | null, saleAt: number | null, nowMs: number): number {
  const baseline = startAt ?? saleAt;
  if (baseline === null) {
    return 0;
  }

  return Math.max(0, diffDays(nowMs, baseline));
}

function parsePromisedWorkVolume(textRaw: string): string {
  const text = normalizeText(textRaw).toLowerCase();
  if (!text) {
    return "Не указано";
  }

  const match = text.match(/(\d+)\s*(диспут|спор|этап|задач|ticket|task|item|час|hour|case|документ)/i);
  if (!match) {
    return "Не указано";
  }

  return `${match[1]} ${normalizeText(match[2])}`;
}

function parseExpectedTermDays(textRaw: string): number {
  const text = normalizeText(textRaw).toLowerCase();
  if (!text) {
    return 180;
  }

  const match = text.match(/(\d+)\s*(day|days|дн|дней|week|weeks|недел|month|months|месяц|месяцев)/i);
  if (!match) {
    return 180;
  }

  const value = Number.parseInt(match[1], 10);
  if (!Number.isFinite(value) || value <= 0) {
    return 180;
  }

  const unit = match[2].toLowerCase();
  if (unit.includes("week") || unit.includes("нед")) {
    return value * 7;
  }
  if (unit.includes("month") || unit.includes("меся")) {
    return value * 30;
  }
  return value;
}

function parseDateFromText(textRaw: string, anchorWords: string[]): number | null {
  const text = normalizeText(textRaw).toLowerCase();
  if (!text) {
    return null;
  }

  const hasAnchor = anchorWords.some((word) => text.includes(word.toLowerCase()));
  if (!hasAnchor) {
    return null;
  }

  const dateMatch = text.match(/(\d{1,2}\/\d{1,2}\/\d{4}|\d{4}-\d{2}-\d{2})/);
  if (!dateMatch) {
    return null;
  }

  return parseDateValue(dateMatch[1]);
}

function scoreLaunchSpeed(daysSaleToStart: number | null): number {
  if (daysSaleToStart === null) {
    return 55;
  }
  if (daysSaleToStart <= 2) {
    return 100;
  }
  if (daysSaleToStart <= 7) {
    return 80;
  }
  if (daysSaleToStart <= 14) {
    return 60;
  }
  if (daysSaleToStart <= 21) {
    return 40;
  }
  return 20;
}

function scoreEngagement(daysSinceLastActivity: number | null, contacts30d: number): number {
  let score = 80;
  if (daysSinceLastActivity === null) {
    score = 50;
  } else if (daysSinceLastActivity > 30) {
    score = 20;
  } else if (daysSinceLastActivity > 14) {
    score = 45;
  } else if (daysSinceLastActivity > 7) {
    score = 65;
  }

  if (contacts30d >= 4) {
    score += 10;
  } else if (contacts30d === 0) {
    score -= 10;
  }

  return clampNumber(Math.round(score), 0, 100);
}

function computeChurnProbability(input: {
  healthIndex: number;
  launchDelayFlag: boolean;
  paymentRisk: boolean;
  communicationRisk: boolean;
  executionRisk: boolean;
  daysSinceLastContact: number | null;
}): number {
  let probability = 100 - input.healthIndex;

  if (input.launchDelayFlag) {
    probability += 8;
  }
  if (input.paymentRisk) {
    probability += 14;
  }
  if (input.communicationRisk) {
    probability += 10;
  }
  if (input.executionRisk) {
    probability += 10;
  }
  if (input.daysSinceLastContact !== null && input.daysSinceLastContact > 14) {
    probability += 8;
  }

  return clampNumber(Math.round(probability), 0, 100);
}

function buildRiskPanel(input: {
  launchDelayFlag: boolean;
  paymentRisk: boolean;
  inactivityFlag: boolean;
  communicationRisk: boolean;
  churnProbability: number;
}): ClientHealthRow["risks"] {
  const warnings: string[] = [];

  if (input.launchDelayFlag) {
    warnings.push("Риск запуска");
  }
  if (input.paymentRisk) {
    warnings.push("Риск оплаты");
  }
  if (input.inactivityFlag) {
    warnings.push("Операционная пауза");
  }
  if (input.communicationRisk) {
    warnings.push("Риск коммуникации");
  }
  if (input.churnProbability >= 60) {
    warnings.push("Риск отмены");
  }

  return {
    launchRisk: input.launchDelayFlag,
    paymentRisk: input.paymentRisk,
    operationalPauseRisk: input.inactivityFlag,
    communicationRisk: input.communicationRisk,
    cancellationRisk: input.churnProbability >= 60,
    warnings,
    churnCategory: resolveChurnCategory(input.churnProbability),
  };
}

function resolveHealthStatus(healthIndex: number): ClientHealthStatus {
  if (healthIndex >= 80) {
    return "Здоровый";
  }
  if (healthIndex >= 60) {
    return "Предупреждение";
  }
  if (healthIndex >= 40) {
    return "Риск";
  }
  return "Критично";
}

function resolveHealthTone(healthIndex: number): "green" | "yellow" | "red" {
  if (healthIndex >= 80) {
    return "green";
  }
  if (healthIndex >= 60) {
    return "yellow";
  }
  return "red";
}

function resolveChurnCategory(probability: number): "Стабильно" | "Наблюдать" | "Высокий риск" {
  if (probability < 30) {
    return "Стабильно";
  }
  if (probability < 60) {
    return "Наблюдать";
  }
  return "Высокий риск";
}

function extractCommunicationCorpus(communications: GhlClientCommunicationsPayload | null): string {
  const items = Array.isArray(communications?.items) ? communications.items : [];
  return items
    .map((item) => `${normalizeText(item?.body)} ${normalizeText(item?.transcript)}`.trim())
    .filter(Boolean)
    .join(" \n");
}

function pickMostRecentTimestamp(values: Array<number | null>): number | null {
  let latest: number | null = null;
  for (const value of values) {
    if (value === null || !Number.isFinite(value)) {
      continue;
    }
    if (latest === null || value > latest) {
      latest = value;
    }
  }
  return latest;
}

function diffDays(left: number, right: number): number {
  return Math.max(0, Math.floor((left - right) / DAY_MS));
}

function addMonthsUtc(timestamp: number, months: number): number {
  const source = new Date(timestamp);
  const year = source.getUTCFullYear();
  const month = source.getUTCMonth();
  const day = source.getUTCDate();

  const targetDate = new Date(Date.UTC(year, month + months, 1));
  const lastDayOfTargetMonth = new Date(Date.UTC(targetDate.getUTCFullYear(), targetDate.getUTCMonth() + 1, 0)).getUTCDate();
  const normalizedDay = Math.min(day, lastDayOfTargetMonth);
  targetDate.setUTCDate(normalizedDay);

  return Date.UTC(targetDate.getUTCFullYear(), targetDate.getUTCMonth(), targetDate.getUTCDate());
}

function formatDateSafe(timestamp: number | null): string {
  if (timestamp === null || !Number.isFinite(timestamp)) {
    return "—";
  }

  return new Date(timestamp).toLocaleDateString("ru-RU");
}

function formatOptionalNumber(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  return String(Math.max(0, Math.trunc(value)));
}

function resolveClientSurname(clientNameRaw: string): string {
  const parts = normalizeText(clientNameRaw)
    .split(/\s+/)
    .filter(Boolean);
  if (!parts.length) {
    return "Без фамилии";
  }
  return parts.length > 1 ? parts[parts.length - 1] : parts[0];
}

function readRecordTextByKeys(record: ClientRecord, keys: readonly string[]): string {
  const source = record as unknown as Record<string, unknown>;
  for (const key of keys) {
    const value = normalizeText(source[key]);
    if (value) {
      return value;
    }
  }
  return "";
}

function truncateExplanationText(value: string, maxChars: number): string {
  const normalized = normalizeText(value);
  if (!normalized) {
    return "";
  }
  const limit = Math.max(40, Math.trunc(maxChars || 380));
  if (normalized.length <= limit) {
    return normalized;
  }
  return `${normalized.slice(0, limit - 1).trim()}…`;
}

function normalizeText(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function clampNumber(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.min(max, Math.max(min, value));
}

function roundTo1(value: number): number {
  return Math.round(value * 10) / 10;
}

function roundTo2(value: number): number {
  return Math.round(value * 100) / 100;
}

export function isTruthy(rawValue: unknown): boolean {
  return TRUE_VALUES.has(normalizeText(rawValue).toLowerCase());
}
