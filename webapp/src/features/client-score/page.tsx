import { useCallback, useEffect, useMemo, useState } from "react";

import { getRecords } from "@/shared/api";
import {
  formatMoney,
  getRecordStatusFlags,
  normalizeRecords,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { evaluateClientScore, formatScoreAsOfDate, type ClientScoreResult } from "@/features/client-score/domain/scoring";
import type { ClientRecord } from "@/shared/types/records";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const MAX_CLIENTS = 20;
const MONTH_2_DISCOUNT = 0.9;
const MONTH_3_DISCOUNT = 0.8;

const PAYMENT_AMOUNT_KEYS: Array<keyof ClientRecord> = [
  "payment1",
  "payment2",
  "payment3",
  "payment4",
  "payment5",
  "payment6",
  "payment7",
];

interface ClientProbabilityRow {
  id: string;
  clientName: string;
  companyName: string;
  closedBy: string;
  score: ClientScoreResult;
  contractTotal: number | null;
  paidTotal: number | null;
  balance: number;
  probabilityMonth1: number;
  probabilityMonth2: number;
  probabilityMonth3: number;
}

export default function ClientScorePage() {
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [asOfDate, setAsOfDate] = useState(() => new Date());

  const loadProbabilityTable = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const payload = await getRecords();
      const normalized = normalizeRecords(Array.isArray(payload.records) ? payload.records : []);
      setRecords(normalized);
      setAsOfDate(new Date());
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load client records.";
      setLoadError(message);
      setRecords([]);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadProbabilityTable();
  }, [loadProbabilityTable]);

  const monthLabels = useMemo(
    () => ({
      month1: formatFutureMonthLabel(asOfDate, 1),
      month2: formatFutureMonthLabel(asOfDate, 2),
      month3: formatFutureMonthLabel(asOfDate, 3),
    }),
    [asOfDate],
  );

  const rows = useMemo<ClientProbabilityRow[]>(() => {
    const source = records.slice(0, MAX_CLIENTS);
    return source.map((record) => buildProbabilityRow(record, asOfDate));
  }, [asOfDate, records]);

  const columns = useMemo<TableColumn<ClientProbabilityRow>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client",
        align: "left",
        cell: (row) => (
          <div>
            <strong>{row.clientName}</strong>
            {row.companyName ? <div className="react-user-footnote">{row.companyName}</div> : null}
            {row.closedBy ? <div className="react-user-footnote">Closed by: {row.closedBy}</div> : null}
          </div>
        ),
      },
      {
        key: "score",
        label: "Score",
        align: "center",
        cell: (row) => (
          <Badge tone={row.score.tone}>
            {row.score.displayScore === null ? "N/A" : String(row.score.displayScore)}
          </Badge>
        ),
      },
      {
        key: "month1",
        label: `Month +1 (${monthLabels.month1})`,
        align: "center",
        cell: (row) => <Badge tone={resolveProbabilityTone(row.probabilityMonth1)}>{formatProbability(row.probabilityMonth1)}</Badge>,
      },
      {
        key: "month2",
        label: `Month +2 (${monthLabels.month2})`,
        align: "center",
        cell: (row) => <Badge tone={resolveProbabilityTone(row.probabilityMonth2)}>{formatProbability(row.probabilityMonth2)}</Badge>,
      },
      {
        key: "month3",
        label: `Month +3 (${monthLabels.month3})`,
        align: "center",
        cell: (row) => <Badge tone={resolveProbabilityTone(row.probabilityMonth3)}>{formatProbability(row.probabilityMonth3)}</Badge>,
      },
      {
        key: "balance",
        label: "Balance",
        align: "right",
        cell: (row) => formatMoney(row.balance),
      },
    ];
  }, [monthLabels.month1, monthLabels.month2, monthLabels.month3]);

  const statusText = useMemo(() => {
    if (isLoading) {
      return "Calculating payment probability...";
    }
    if (loadError) {
      return loadError;
    }
    return `Showing first ${rows.length} clients. As of ${formatScoreAsOfDate(asOfDate)}.`;
  }, [asOfDate, isLoading, loadError, rows.length]);

  return (
    <PageShell className="client-score-react-page">
      <PageHeader
        actions={
          <Button type="button" size="sm" onClick={() => void loadProbabilityTable()} isLoading={isLoading}>
            Refresh
          </Button>
        }
        meta={
          <>
            <p className={`dashboard-message ${loadError ? "error" : ""}`.trim()}>{statusText}</p>
            <p className="react-user-footnote">
              Model v1: score + overdue + paid ratio + payment pace. Horizon discount: Month +2 = Month +1 x 0.9, Month +3 = Month +1 x 0.8.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Client Payment Probability">
        {isLoading ? <LoadingSkeleton rows={8} /> : null}
        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load payment probability table"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadProbabilityTable()}
          />
        ) : null}
        {!isLoading && !loadError && !rows.length ? (
          <EmptyState title="No client records found." />
        ) : null}
        {!isLoading && !loadError && rows.length ? (
          <Table
            className="client-managers-react-table-wrap"
            columns={columns}
            rows={rows}
            rowKey={(row) => row.id}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function buildProbabilityRow(record: ClientRecord, asOfDate: Date): ClientProbabilityRow {
  const score = evaluateClientScore(record, asOfDate);
  const status = getRecordStatusFlags(record);

  const contractTotal = parseMoneyValue(record.contractTotals);
  const paidTotal = parseMoneyValue(record.totalPayments) ?? estimatePaidTotal(record);
  const balanceFromRecord = parseMoneyValue(record.futurePayments);

  const paidValue = paidTotal ?? 0;
  const fallbackBalance = contractTotal === null ? 0 : contractTotal - paidValue;
  const balance = Math.max(0, balanceFromRecord ?? fallbackBalance);
  const paidRatio = contractTotal && contractTotal > 0 ? clamp(paidValue / contractTotal, 0, 1.5) : 0;

  const monthlyPayment = estimateMonthlyPayment(record, paidValue, contractTotal);
  const probabilities = calculateProbabilities({
    score: score.displayScore,
    paidRatio,
    overdueDays: status.overdueDays,
    monthlyPayment,
    balance,
    openMilestones: score.openMilestones,
    writtenOff: status.isWrittenOff,
  });

  return {
    id: record.id,
    clientName: record.clientName || "Unnamed",
    companyName: record.companyName || "",
    closedBy: record.closedBy || "",
    score,
    contractTotal,
    paidTotal,
    balance,
    probabilityMonth1: probabilities.month1,
    probabilityMonth2: probabilities.month2,
    probabilityMonth3: probabilities.month3,
  };
}

function estimatePaidTotal(record: ClientRecord): number | null {
  let total = 0;
  let hasValue = false;

  for (const key of PAYMENT_AMOUNT_KEYS) {
    const amount = parseMoneyValue(record[key]);
    if (amount === null) {
      continue;
    }

    hasValue = true;
    total += amount;
  }

  return hasValue ? total : null;
}

function estimateMonthlyPayment(record: ClientRecord, paidValue: number, contractTotal: number | null): number {
  const payments = PAYMENT_AMOUNT_KEYS.map((key) => parseMoneyValue(record[key])).filter(
    (value): value is number => value !== null && value > 0,
  );

  if (payments.length > 0) {
    const sum = payments.reduce((total, value) => total + value, 0);
    return sum / payments.length;
  }

  if (paidValue > 0) {
    return paidValue;
  }

  if (contractTotal !== null && contractTotal > 0) {
    return contractTotal / 7;
  }

  return 0;
}

function calculateProbabilities(input: {
  score: number | null;
  paidRatio: number;
  overdueDays: number;
  monthlyPayment: number;
  balance: number;
  openMilestones: number;
  writtenOff: boolean;
}): { month1: number; month2: number; month3: number } {
  if (input.writtenOff || input.balance <= 0) {
    return { month1: 0, month2: 0, month3: 0 };
  }

  const scoreFactor = clamp((input.score ?? 50) / 100, 0, 1.1);
  const overduePenalty = Math.min(Math.max(input.overdueDays, 0), 120);
  const paymentPace = clamp(input.monthlyPayment / Math.max(input.balance, 1), 0, 2);

  const z =
    -2.45 +
    scoreFactor * 3.2 +
    input.paidRatio * 1.15 +
    paymentPace * 0.4 -
    overduePenalty * 0.02 -
    input.openMilestones * 0.35;

  const month1 = clamp(sigmoid(z), 0.05, 0.95);
  const month2 = clamp(month1 * MONTH_2_DISCOUNT, 0.03, 0.9);
  const month3 = clamp(month1 * MONTH_3_DISCOUNT, 0.02, 0.85);

  return { month1, month2, month3 };
}

function resolveProbabilityTone(probability: number): "success" | "info" | "warning" | "danger" {
  if (probability >= 0.75) {
    return "success";
  }
  if (probability >= 0.55) {
    return "info";
  }
  if (probability >= 0.35) {
    return "warning";
  }
  return "danger";
}

function formatProbability(value: number): string {
  return `${Math.round(clamp(value, 0, 1) * 100)}%`;
}

function formatFutureMonthLabel(baseDate: Date, offset: number): string {
  const value = new Date(Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth() + offset, 1));
  return value.toLocaleDateString("en-US", {
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  });
}

function sigmoid(value: number): number {
  return 1 / (1 + Math.exp(-value));
}

function clamp(value: number, minValue: number, maxValue: number): number {
  return Math.min(maxValue, Math.max(minValue, value));
}
