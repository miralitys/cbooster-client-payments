import { useCallback, useEffect, useMemo, useState } from "react";

import { fetchPaymentProbability } from "@/features/client-score/api";
import {
  formatMoney,
  getRecordStatusFlags,
  normalizeRecords,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import {
  computeLegacyPaymentProbabilities,
  computePaymentFeatures,
  evaluateClientScore,
  formatScoreAsOfDate,
  type ClientScoreResult,
  type PaymentFeatures,
  type PaymentProbabilities,
} from "@/features/client-score/domain/scoring";
import { getRecords } from "@/shared/api";
import type { ClientRecord } from "@/shared/types/records";
import {
  Badge,
  Button,
  EmptyState,
  ErrorState,
  LoadingSkeleton,
  PageHeader,
  PageShell,
  Panel,
  SegmentedControl,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const MAX_CLIENTS = 20;
const VERSION_OPTIONS = [
  { key: "v1", label: "Версия 1" },
  { key: "v2", label: "Версия 2" },
] as const;

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
  probabilitySource: "legacy" | "ml";
}

interface ProbabilityRowSeed {
  id: string;
  clientName: string;
  companyName: string;
  closedBy: string;
  score: ClientScoreResult;
  contractTotal: number | null;
  paidTotal: number | null;
  features: PaymentFeatures;
}

export default function ClientScorePage() {
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [asOfDate, setAsOfDate] = useState(() => new Date());
  const [activeVersion, setActiveVersion] = useState<"v1" | "v2">("v2");
  const [version2Rows, setVersion2Rows] = useState<ClientProbabilityRow[]>([]);
  const [isVersion2Loading, setIsVersion2Loading] = useState(false);

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

  const legacyRows = useMemo<ClientProbabilityRow[]>(() => {
    return records.slice(0, MAX_CLIENTS).map((record) => buildLegacyProbabilityRow(record, asOfDate));
  }, [asOfDate, records]);

  useEffect(() => {
    let isCancelled = false;
    const source = records.slice(0, MAX_CLIENTS);

    if (isLoading || loadError) {
      setVersion2Rows([]);
      setIsVersion2Loading(false);
      return () => {
        isCancelled = true;
      };
    }

    if (!source.length) {
      setVersion2Rows([]);
      setIsVersion2Loading(false);
      return () => {
        isCancelled = true;
      };
    }

    const loadVersion2Rows = async () => {
      setIsVersion2Loading(true);

      try {
        const nextRows = await Promise.all(source.map((record) => buildVersion2ProbabilityRow(record, asOfDate)));

        if (!isCancelled) {
          setVersion2Rows(nextRows);
        }
      } catch {
        if (!isCancelled) {
          setVersion2Rows(source.map((record) => buildLegacyProbabilityRow(record, asOfDate)));
        }
      } finally {
        if (!isCancelled) {
          setIsVersion2Loading(false);
        }
      }
    };

    void loadVersion2Rows();

    return () => {
      isCancelled = true;
    };
  }, [asOfDate, isLoading, loadError, records]);

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
          <Badge tone={row.score.tone}>{row.score.displayScore === null ? "N/A" : String(row.score.displayScore)}</Badge>
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
    return `Showing first ${legacyRows.length} clients. As of ${formatScoreAsOfDate(asOfDate)}.`;
  }, [asOfDate, isLoading, legacyRows.length, loadError]);

  const version2StatusText = useMemo(() => {
    if (isLoading || isVersion2Loading) {
      return "Calculating payment probability (Version 2)...";
    }
    if (loadError) {
      return loadError;
    }
    const mlRows = version2Rows.filter((row) => row.probabilitySource === "ml").length;
    const fallbackRows = Math.max(0, version2Rows.length - mlRows);
    return `Showing first ${version2Rows.length} clients (ML: ${mlRows}, fallback: ${fallbackRows}). As of ${formatScoreAsOfDate(asOfDate)}.`;
  }, [asOfDate, isLoading, isVersion2Loading, loadError, version2Rows]);

  const activeStatusText = activeVersion === "v1" ? statusText : version2StatusText;
  const activeRows = activeVersion === "v1" ? legacyRows : version2Rows;
  const activeLoading = activeVersion === "v1" ? isLoading : isLoading || isVersion2Loading;
  const activeTitle = activeVersion === "v1" ? "Версия 1" : "Версия 2";

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
            <p className={`dashboard-message ${loadError ? "error" : ""}`.trim()}>{activeStatusText}</p>
            <p className="react-user-footnote">
              Version 1: deterministic model (score + overdue + paid ratio + payment pace).
            </p>
            <p className="react-user-footnote">
              Version 2: backend ML endpoint (`/api/payment-probability`) + frontend guard + legacy fallback.
            </p>
          </>
        }
      />

      <Panel
        className="table-panel"
        title={activeTitle}
        actions={
          <SegmentedControl
            value={activeVersion}
            options={VERSION_OPTIONS.map((option) => ({ key: option.key, label: option.label }))}
            onChange={(value) => setActiveVersion(value === "v1" ? "v1" : "v2")}
          />
        }
      >
        {activeLoading ? <LoadingSkeleton rows={8} /> : null}
        {!activeLoading && loadError ? (
          <ErrorState
            title="Failed to load payment probability table"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadProbabilityTable()}
          />
        ) : null}
        {!activeLoading && !loadError && !activeRows.length ? <EmptyState title="No client records found." /> : null}
        {!activeLoading && !loadError && activeRows.length ? (
          <Table
            className="client-managers-react-table-wrap"
            columns={columns}
            rows={activeRows}
            rowKey={(row) => row.id}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function buildLegacyProbabilityRow(record: ClientRecord, asOfDate: Date): ClientProbabilityRow {
  const seed = buildProbabilityRowSeed(record, asOfDate);
  const probabilities = computeLegacyPaymentProbabilities(seed.features);
  return createProbabilityRow(seed, probabilities, "legacy");
}

async function buildVersion2ProbabilityRow(record: ClientRecord, asOfDate: Date): Promise<ClientProbabilityRow> {
  const seed = buildProbabilityRowSeed(record, asOfDate);

  if (seed.features.writtenOff === true || seed.features.balance <= 0) {
    return createProbabilityRow(seed, { p1: 0, p2: 0, p3: 0 }, "legacy");
  }

  try {
    const probabilities = await fetchPaymentProbability(seed.features);
    return createProbabilityRow(seed, probabilities, "ml");
  } catch {
    const fallback = computeLegacyPaymentProbabilities(seed.features);
    return createProbabilityRow(seed, fallback, "legacy");
  }
}

function buildProbabilityRowSeed(record: ClientRecord, asOfDate: Date): ProbabilityRowSeed {
  const score = evaluateClientScore(record, asOfDate);
  const status = getRecordStatusFlags(record);

  const contractTotal = parseMoneyValue(record.contractTotals);
  const totalPayments = parseMoneyValue(record.totalPayments);
  const futurePayments = parseMoneyValue(record.futurePayments);
  const payments = collectPaymentAmounts(record);

  const monthlyPayment = estimateMonthlyPayment(payments, totalPayments, contractTotal);
  const features = computePaymentFeatures({
    contractTotal: contractTotal ?? 0,
    totalPayments,
    payments,
    monthlyPayment,
    displayScore: score.displayScore,
    overdueDays: status.overdueDays,
    openMilestones: score.openMilestones,
    futurePayments,
    writtenOff: status.isWrittenOff,
  });

  return {
    id: record.id,
    clientName: record.clientName || "Unnamed",
    companyName: record.companyName || "",
    closedBy: record.closedBy || "",
    score,
    contractTotal,
    paidTotal: totalPayments ?? (payments.length > 0 ? features.paidTotal : null),
    features,
  };
}

function createProbabilityRow(
  seed: ProbabilityRowSeed,
  probabilities: PaymentProbabilities,
  probabilitySource: ClientProbabilityRow["probabilitySource"],
): ClientProbabilityRow {
  return {
    id: seed.id,
    clientName: seed.clientName,
    companyName: seed.companyName,
    closedBy: seed.closedBy,
    score: seed.score,
    contractTotal: seed.contractTotal,
    paidTotal: seed.paidTotal,
    balance: seed.features.balance,
    probabilityMonth1: clampProbability(probabilities.p1),
    probabilityMonth2: clampProbability(probabilities.p2),
    probabilityMonth3: clampProbability(probabilities.p3),
    probabilitySource,
  };
}

function collectPaymentAmounts(record: ClientRecord): number[] {
  const values: number[] = [];

  for (const key of PAYMENT_AMOUNT_KEYS) {
    const amount = parseMoneyValue(record[key]);
    if (amount === null) {
      continue;
    }

    values.push(amount);
  }

  return values;
}

function estimateMonthlyPayment(payments: number[], paidTotal: number | null, contractTotal: number | null): number {
  if (payments.length > 0) {
    const sum = payments.reduce((total, value) => total + value, 0);
    return sum / payments.length;
  }

  if (paidTotal !== null && paidTotal > 0) {
    return paidTotal;
  }

  if (contractTotal !== null && contractTotal > 0) {
    return contractTotal / 7;
  }

  return 0;
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

function clampProbability(value: number): number {
  return clamp(value, 0, 1);
}

function clamp(value: number, minValue: number, maxValue: number): number {
  return Math.min(maxValue, Math.max(minValue, value));
}
