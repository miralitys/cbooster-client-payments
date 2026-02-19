import { useCallback, useEffect, useMemo, useState } from "react";

import { getRecords } from "@/shared/api";
import { formatMoney, parseMoneyValue } from "@/features/client-payments/domain/calculations";
import { evaluateClientScore, formatScoreAsOfDate, type ClientScoreResult } from "@/features/client-score/domain/scoring";
import type { ClientRecord } from "@/shared/types/records";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const MAX_CLIENTS = 20;

interface ClientScoreRow {
  id: string;
  clientName: string;
  companyName: string;
  closedBy: string;
  contractTotal: number | null;
  paidTotal: number | null;
  score: ClientScoreResult;
}

export default function ClientScorePage() {
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [asOfDate, setAsOfDate] = useState(() => new Date());

  const loadClientScores = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const payload = await getRecords();
      setRecords(Array.isArray(payload) ? payload : []);
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
    void loadClientScores();
  }, [loadClientScores]);

  const rows = useMemo<ClientScoreRow[]>(() => {
    return records.slice(0, MAX_CLIENTS).map((record) => ({
      id: record.id,
      clientName: record.clientName || "Unnamed",
      companyName: record.companyName || "",
      closedBy: record.closedBy || "",
      contractTotal: parseMoneyValue(record.contractTotals),
      paidTotal: parseMoneyValue(record.totalPayments),
      score: evaluateClientScore(record, asOfDate),
    }));
  }, [asOfDate, records]);

  const columns = useMemo<TableColumn<ClientScoreRow>[]>(() => {
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
            {row.score.score === null ? "Score N/A" : `Score ${row.score.score}`}
          </Badge>
        ),
      },
      {
        key: "contractPaid",
        label: "Contract / Paid",
        align: "right",
        cell: (row) => (
          <div>
            <div>{row.contractTotal === null ? "-" : formatMoney(row.contractTotal)}</div>
            <div className="react-user-footnote">
              Paid: {row.paidTotal === null ? "-" : formatMoney(row.paidTotal)}
            </div>
          </div>
        ),
      },
      {
        key: "reason",
        label: "Why This Score",
        align: "left",
        cell: (row) => (
          <div>
            <div>{row.score.explanation}</div>
            <div className="react-user-footnote">
              Penalty {row.score.penaltyPoints}, Bonus +{row.score.bonusPoints}, Recovery +{row.score.recoveryPoints}
            </div>
          </div>
        ),
      },
    ];
  }, []);

  const statusText = useMemo(() => {
    if (isLoading) {
      return "Calculating client score...";
    }
    if (loadError) {
      return loadError;
    }
    return `Showing first ${rows.length} clients. As of ${formatScoreAsOfDate(asOfDate)}.`;
  }, [asOfDate, isLoading, loadError, rows.length]);

  return (
    <PageShell className="client-score-react-page">
      <PageHeader
        title="Client Score"
        subtitle="Payment discipline score based on contract schedule and actual payments"
        actions={
          <Button type="button" size="sm" onClick={() => void loadClientScores()} isLoading={isLoading}>
            Refresh
          </Button>
        }
        meta={<p className="dashboard-message">{statusText}</p>}
      />

      <Panel className="table-panel" title="Client Score Table">
        <p className="react-user-footnote">
          Rules: first 3 late days are ignored, 30/60/90+ day delays use stepped penalties, early coverage can add
          +5, and on-time recent months can recover +5.
        </p>

        {isLoading ? <LoadingSkeleton rows={8} /> : null}
        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load client score"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadClientScores()}
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
