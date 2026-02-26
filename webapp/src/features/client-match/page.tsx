import { useCallback, useEffect, useMemo, useState } from "react";

import { PAYMENT_PAIRS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, parseDateValue, parseMoneyValue } from "@/features/client-payments/domain/calculations";
import { getClients, getQuickBooksPayments } from "@/shared/api";
import type { QuickBooksPaymentRow } from "@/shared/types/quickbooks";
import type { ClientRecord } from "@/shared/types/records";
import { Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const MATCH_FROM_DATE = "2026-01-01";
const NAME_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });

interface PaymentPair {
  date: string;
  amount: number | null;
}

interface ClientMatchRow {
  id: string;
  clientName: string;
  quickBooksPayments: PaymentPair[];
  databasePayments: PaymentPair[];
}

interface ClientMatchSummary {
  quickBooksPaymentsCount: number;
  quickBooksClientsCount: number;
  databaseMatchedClientsCount: number;
  rangeFrom: string;
  rangeTo: string;
}

const EMPTY_SUMMARY: ClientMatchSummary = {
  quickBooksPaymentsCount: 0,
  quickBooksClientsCount: 0,
  databaseMatchedClientsCount: 0,
  rangeFrom: MATCH_FROM_DATE,
  rangeTo: MATCH_FROM_DATE,
};

export default function ClientMatchPage() {
  const [rows, setRows] = useState<ClientMatchRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [summary, setSummary] = useState<ClientMatchSummary>(EMPTY_SUMMARY);

  const loadMatches = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const to = formatDateForApi(new Date());
      const [quickBooksPayload, clientsPayload] = await Promise.all([
        getQuickBooksPayments({ from: MATCH_FROM_DATE, to }),
        getClients(),
      ]);

      const quickBooksItems = Array.isArray(quickBooksPayload?.items) ? quickBooksPayload.items : [];
      const clientRecords = Array.isArray(clientsPayload?.records) ? clientsPayload.records : [];

      const quickBooksByClient = groupQuickBooksPaymentsByClientName(quickBooksItems);
      const databaseByClient = groupDatabasePaymentsByClientName(clientRecords);

      const nextRows: ClientMatchRow[] = [...quickBooksByClient.entries()]
        .map(([clientKey, quickBooksMatch]) => {
          const databasePayments = databaseByClient.get(clientKey) || [];
          return {
            id: clientKey,
            clientName: quickBooksMatch.clientName,
            quickBooksPayments: quickBooksMatch.payments,
            databasePayments,
          };
        })
        .sort((left, right) => NAME_SORTER.compare(left.clientName, right.clientName));

      setRows(nextRows);
      setSummary({
        quickBooksPaymentsCount: quickBooksItems.length,
        quickBooksClientsCount: nextRows.length,
        databaseMatchedClientsCount: nextRows.filter((row) => row.databasePayments.length > 0).length,
        rangeFrom: MATCH_FROM_DATE,
        rangeTo: to,
      });
    } catch (error) {
      setRows([]);
      setSummary({
        ...EMPTY_SUMMARY,
        rangeTo: formatDateForApi(new Date()),
      });
      setLoadError(error instanceof Error ? error.message : "Failed to load Client Match.");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadMatches();
  }, [loadMatches]);

  const maxPaymentColumns = useMemo(() => {
    const maxColumns = rows.reduce((max, row) => {
      return Math.max(max, row.quickBooksPayments.length, row.databasePayments.length);
    }, 0);
    return Math.max(1, maxColumns);
  }, [rows]);

  const tableColumns = useMemo<TableColumn<ClientMatchRow>[]>(() => {
    const columns: TableColumn<ClientMatchRow>[] = [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        className: "client-match-column-client",
        headerClassName: "client-match-column-client",
        cell: (row) => row.clientName,
      },
    ];

    for (let index = 0; index < maxPaymentColumns; index += 1) {
      const slot = index + 1;
      columns.push(
        {
          key: `qbDate_${slot}`,
          label: `QB Date ${slot}`,
          align: "center",
          cell: (row) => formatMatchDate(row.quickBooksPayments[index]?.date || ""),
        },
        {
          key: `qbAmount_${slot}`,
          label: `QB Amount ${slot}`,
          align: "right",
          cell: (row) => formatMatchAmount(row.quickBooksPayments[index]?.amount),
        },
        {
          key: `dbDate_${slot}`,
          label: `DB Date ${slot}`,
          align: "center",
          cell: (row) => formatMatchDate(row.databasePayments[index]?.date || ""),
        },
        {
          key: `dbAmount_${slot}`,
          label: `DB Amount ${slot}`,
          align: "right",
          cell: (row) => formatMatchAmount(row.databasePayments[index]?.amount),
        },
      );
    }

    return columns;
  }, [maxPaymentColumns]);

  const headerMeta = (
    <div className="client-match-meta">
      <span>QuickBooks payments: {summary.quickBooksPaymentsCount}</span>
      <span>QuickBooks clients: {summary.quickBooksClientsCount}</span>
      <span>Matched in DB: {summary.databaseMatchedClientsCount}</span>
      <span>
        Range: {summary.rangeFrom} -&gt; {summary.rangeTo}
      </span>
    </div>
  );

  return (
    <PageShell className="client-match-page">
      <PageHeader
        title="Client Match"
        subtitle="Temporary comparison of QuickBooks payments against Client Payment DB"
        meta={headerMeta}
        actions={(
          <Button type="button" onClick={() => void loadMatches()} disabled={isLoading}>
            {isLoading ? "Refreshing..." : "Refresh"}
          </Button>
        )}
      />

      <Panel title="QuickBooks Clients (from 2026-01-01)">
        {isLoading ? <LoadingSkeleton rows={8} /> : null}
        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load Client Match"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadMatches()}
          />
        ) : null}
        {!isLoading && !loadError && !rows.length ? (
          <EmptyState title="No QuickBooks payments found" description="No payments were returned for the selected range." />
        ) : null}
        {!isLoading && !loadError && rows.length ? (
          <Table
            columns={tableColumns}
            rows={rows}
            rowKey={(row) => row.id}
            className="client-match-table-wrap"
            tableClassName="client-match-table"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function normalizeClientName(rawValue: unknown): string {
  return String(rawValue || "")
    .toLowerCase()
    .replace(/\[wo\]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function groupQuickBooksPaymentsByClientName(items: QuickBooksPaymentRow[]): Map<string, { clientName: string; payments: PaymentPair[] }> {
  const grouped = new Map<string, { clientName: string; payments: PaymentPair[] }>();

  for (const item of items) {
    const clientName = String(item?.clientName || "").trim();
    const normalizedClientName = normalizeClientName(clientName);
    if (!normalizedClientName) {
      continue;
    }

    const existing = grouped.get(normalizedClientName);
    if (!existing) {
      grouped.set(normalizedClientName, {
        clientName,
        payments: [
          {
            date: String(item?.paymentDate || "").trim(),
            amount: normalizeQuickBooksAmount(item?.paymentAmount),
          },
        ],
      });
      continue;
    }

    existing.payments.push({
      date: String(item?.paymentDate || "").trim(),
      amount: normalizeQuickBooksAmount(item?.paymentAmount),
    });
  }

  for (const [, value] of grouped) {
    value.payments.sort(comparePaymentPairs);
  }

  return grouped;
}

function groupDatabasePaymentsByClientName(records: ClientRecord[]): Map<string, PaymentPair[]> {
  const grouped = new Map<string, PaymentPair[]>();

  for (const record of records) {
    const normalizedClientName = normalizeClientName(record?.clientName);
    if (!normalizedClientName) {
      continue;
    }

    const payments = grouped.get(normalizedClientName) || [];
    for (const [paymentKey, paymentDateKey] of PAYMENT_PAIRS) {
      const amount = parseMoneyValue(record[paymentKey]);
      const date = String(record[paymentDateKey] || "").trim();
      if (amount === null && !date) {
        continue;
      }
      payments.push({ date, amount });
    }

    grouped.set(normalizedClientName, payments);
  }

  for (const [, payments] of grouped) {
    payments.sort(comparePaymentPairs);
  }

  return grouped;
}

function comparePaymentPairs(left: PaymentPair, right: PaymentPair): number {
  const leftDate = parseDateValue(left.date);
  const rightDate = parseDateValue(right.date);

  if (leftDate !== null && rightDate !== null && leftDate !== rightDate) {
    return leftDate - rightDate;
  }

  if (leftDate !== null && rightDate === null) {
    return -1;
  }

  if (leftDate === null && rightDate !== null) {
    return 1;
  }

  const leftAmount = left.amount ?? Number.NEGATIVE_INFINITY;
  const rightAmount = right.amount ?? Number.NEGATIVE_INFINITY;
  if (leftAmount !== rightAmount) {
    return leftAmount - rightAmount;
  }

  return 0;
}

function normalizeQuickBooksAmount(value: unknown): number | null {
  const numberValue = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function formatMatchDate(rawValue: string): string {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "-";
  }
  return formatDate(value);
}

function formatMatchAmount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return formatMoney(value);
}

function formatDateForApi(value: Date): string {
  const year = String(value.getUTCFullYear());
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  const day = String(value.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}
