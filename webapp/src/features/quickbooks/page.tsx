import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import { withStableRowKeys, type RowWithKey } from "@/shared/lib/stableRowKeys";
import { getSession, getQuickBooksPayments } from "@/shared/api";
import type { QuickBooksPaymentRow } from "@/shared/types/quickbooks";
import { Button, EmptyState, ErrorState, Input, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const QUICKBOOKS_FROM_DATE = "2026-01-01";
const CURRENCY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

interface LoadOptions {
  sync?: boolean;
  fullSync?: boolean;
}

type QuickBooksViewRow = RowWithKey<QuickBooksPaymentRow>;

export default function QuickBooksPage() {
  const [canSync, setCanSync] = useState(false);
  const [allTransactions, setAllTransactions] = useState<QuickBooksViewRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [statusText, setStatusText] = useState("Loading saved transactions...");
  const [rangeText, setRangeText] = useState("");
  const [lastLoadPrefix, setLastLoadPrefix] = useState("");

  const [search, setSearch] = useState("");
  const [refundOnly, setRefundOnly] = useState(false);
  const transactionsRef = useRef<QuickBooksViewRow[]>([]);
  const rowKeySequenceRef = useRef(0);

  const filteredTransactions = useMemo(
    () => filterTransactions(allTransactions, search, refundOnly),
    [allTransactions, refundOnly, search],
  );

  const tableColumns = useMemo<TableColumn<QuickBooksViewRow>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (item) => {
          const amount = Number(item.paymentAmount) || 0;
          return formatQuickBooksClientLabel(item.clientName, item.transactionType, amount);
        },
      },
      {
        key: "clientPhone",
        label: "Phone",
        align: "left",
        cell: (item) => formatContactCellValue(item.clientPhone),
      },
      {
        key: "clientEmail",
        label: "Email",
        align: "left",
        cell: (item) => formatContactCellValue(item.clientEmail),
      },
      {
        key: "paymentAmount",
        label: "Payment Amount",
        align: "right",
        cell: (item) => CURRENCY_FORMATTER.format(Number(item.paymentAmount) || 0),
      },
      {
        key: "paymentDate",
        label: "Payment Date",
        align: "center",
        cell: (item) => formatDate(item.paymentDate),
      },
    ];
  }, []);

  const loadRecentQuickBooksPayments = useCallback(async (options: LoadOptions = {}) => {
    const shouldSync = Boolean(options.sync);
    const shouldTotalRefresh = Boolean(options.fullSync);
    const previousItems = [...transactionsRef.current];
    setIsLoading(true);
    setLoadError("");

    if (shouldTotalRefresh) {
      setStatusText("Running total refresh from QuickBooks...");
    } else {
      setStatusText(shouldSync ? "Refreshing from QuickBooks..." : "Loading saved transactions...");
    }

    try {
      const payload = await getQuickBooksPayments({
        from: QUICKBOOKS_FROM_DATE,
        to: formatDateForApi(new Date()),
        sync: shouldSync,
        fullSync: shouldTotalRefresh,
      });
      const items = Array.isArray(payload.items) ? payload.items : [];
      setAllTransactions(
        withStableRowKeys(items, previousItems, rowKeySequenceRef, {
          prefix: "qb",
          signature: quickBooksRowSignature,
        }),
      );
      setRangeText(payload.range?.from && payload.range?.to ? `Range: ${payload.range.from} -> ${payload.range.to}` : "");
      setLastLoadPrefix(buildLoadPrefixFromPayload(payload, shouldSync, shouldTotalRefresh));
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load transactions.";
      setLoadError(message);
      setStatusText(message);
      if (!previousItems.length) {
        setAllTransactions([]);
        setRangeText("");
      } else {
        setAllTransactions(previousItems);
      }
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    transactionsRef.current = allTransactions;
  }, [allTransactions]);

  useEffect(() => {
    void getSession()
      .then((session) => {
        setCanSync(Boolean(session?.permissions?.sync_quickbooks));
      })
      .catch(() => {
        setCanSync(false);
      });
    void loadRecentQuickBooksPayments();
  }, [loadRecentQuickBooksPayments]);

  useEffect(() => {
    setStatusText(buildFilterStatusMessage(allTransactions.length, filteredTransactions.length, search, refundOnly, lastLoadPrefix));
  }, [allTransactions.length, filteredTransactions.length, search, refundOnly, lastLoadPrefix]);

  useEffect(() => {
    if (!loadError) {
      return;
    }

    showToast({
      type: "error",
      message: loadError,
      dedupeKey: `quickbooks-load-error-${loadError}`,
      cooldownMs: 3200,
      action: canSync
        ? {
            label: "Retry",
            onClick: () => {
              void loadRecentQuickBooksPayments();
            },
          }
        : undefined,
    });
  }, [canSync, loadError, loadRecentQuickBooksPayments]);

  return (
    <PageShell className="quickbooks-react-page">
      <PageHeader
        title="QuickBooks"
        subtitle="Payments feed"
        actions={
          <div className="cb-page-header-toolbar">
            <Button
              id="refresh-button"
              type="button"
              size="sm"
              onClick={() => void loadRecentQuickBooksPayments({ sync: true })}
              isLoading={isLoading}
              disabled={isLoading || !canSync}
            >
              Refresh
            </Button>
            <Button
              id="total-refresh-button"
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => void loadRecentQuickBooksPayments({ sync: true, fullSync: true })}
              isLoading={isLoading}
              disabled={isLoading || !canSync}
            >
              Total Refresh
            </Button>
          </div>
        }
        meta={rangeText ? <p className="quickbooks-range">{rangeText}</p> : null}
      />

      <Panel
        className="table-panel quickbooks-table-panel"
        title="Transactions"
        actions={
          <div className="quickbooks-toolbar-react">
            <div className="quickbooks-search-field">
              <label htmlFor="quickbooks-client-search" className="search-label quickbooks-search-field__label">
                Search by client
              </label>
              <Input
                id="quickbooks-client-search"
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Type client name"
                autoComplete="off"
                spellCheck={false}
              />
            </div>
            <label htmlFor="quickbooks-refund-only" className="cb-checkbox-row quickbooks-refund-filter">
              <input
                id="quickbooks-refund-only"
                type="checkbox"
                checked={refundOnly}
                onChange={(event) => setRefundOnly(event.target.checked)}
              />
              Only refunds
            </label>
          </div>
        }
      >
        {!loadError ? <p className="dashboard-message quickbooks-status">{statusText}</p> : null}

        {isLoading ? <LoadingSkeleton rows={7} /> : null}
        {!isLoading && loadError && !filteredTransactions.length ? (
          <ErrorState
            title="Failed to load transactions"
            description={loadError}
            actionLabel={canSync ? "Retry" : undefined}
            onAction={canSync ? () => void loadRecentQuickBooksPayments() : undefined}
          />
        ) : null}
        {!isLoading && !loadError && !filteredTransactions.length ? (
          <EmptyState
            title={
              search.trim()
                ? refundOnly
                  ? `No refunds found for "${search.trim()}".`
                  : `No transactions found for "${search.trim()}".`
                : refundOnly
                  ? "No refunds found for the selected period."
                  : "No transactions found for the selected period."
            }
          />
        ) : null}

        {!isLoading && filteredTransactions.length ? (
          <Table
            className="quickbooks-table-wrap"
            columns={tableColumns}
            rows={filteredTransactions}
            rowKey={(item) => item._rowKey}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function filterTransactions<RowType extends QuickBooksPaymentRow>(
  items: RowType[],
  query: string,
  showOnlyRefunds: boolean,
): RowType[] {
  const normalizedQuery = query.toLowerCase().trim();
  return items.filter((item) => {
    if (showOnlyRefunds && String(item.transactionType || "").toLowerCase() !== "refund") {
      return false;
    }
    if (!normalizedQuery) {
      return true;
    }
    return String(item.clientName || "").toLowerCase().includes(normalizedQuery);
  });
}

function quickBooksRowSignature(row: QuickBooksPaymentRow): string {
  return [
    String(row.clientName || "").trim(),
    String(row.clientPhone || "").trim(),
    String(row.clientEmail || "").trim(),
    String(row.paymentDate || "").trim(),
    String(row.paymentAmount ?? "").trim(),
    String(row.transactionType || "").trim(),
  ].join("|");
}

function buildFilterStatusMessage(
  totalCount: number,
  visibleCount: number,
  query: string,
  showOnlyRefunds: boolean,
  prefix = "",
): string {
  const normalizedQuery = query.trim();
  const normalizedPrefix = prefix.trim();
  let mainMessage = "";

  if (!normalizedQuery) {
    if (showOnlyRefunds) {
      mainMessage = `Showing ${visibleCount} refund${visibleCount === 1 ? "" : "s"}.`;
    } else {
      mainMessage = `Loaded ${totalCount} transaction${totalCount === 1 ? "" : "s"}.`;
    }
  } else if (visibleCount === 0) {
    mainMessage = showOnlyRefunds
      ? `No refunds found for "${normalizedQuery}".`
      : `No transactions found for "${normalizedQuery}".`;
  } else {
    mainMessage = showOnlyRefunds
      ? `Showing ${visibleCount} refund${visibleCount === 1 ? "" : "s"} for "${normalizedQuery}".`
      : `Showing ${visibleCount} of ${totalCount} transactions for "${normalizedQuery}".`;
  }

  return normalizedPrefix ? `${normalizedPrefix} ${mainMessage}` : mainMessage;
}

function buildLoadPrefixFromPayload(
  payload: {
    sync?: {
      requested?: boolean;
      syncMode?: string;
      insertedCount?: number;
      reconciledCount?: number;
      writtenCount?: number;
    };
  },
  syncRequested: boolean,
  totalRefreshRequested: boolean,
): string {
  if (!syncRequested) {
    return "Saved data:";
  }
  const syncMeta = payload.sync && typeof payload.sync === "object" ? payload.sync : null;
  if (!syncMeta?.requested) {
    return "Saved data:";
  }

  const syncMode = String(syncMeta.syncMode || "").toLowerCase();
  const isFullSync = syncMode === "full" || totalRefreshRequested;
  if (isFullSync) {
    const writtenCount = Number.parseInt(String(syncMeta.writtenCount || ""), 10);
    if (Number.isFinite(writtenCount) && writtenCount >= 0) {
      return `Total refresh: ${writtenCount} synced.`;
    }
    return "Total refresh completed.";
  }

  const insertedCount = Number.parseInt(String(syncMeta.insertedCount || ""), 10);
  if (Number.isFinite(insertedCount) && insertedCount > 0) {
    return `Refresh: +${insertedCount} new.`;
  }

  const reconciledCount = Number.parseInt(String(syncMeta.reconciledCount || ""), 10);
  if (Number.isFinite(reconciledCount) && reconciledCount > 0) {
    return `Refresh: updated ${reconciledCount} zero rows.`;
  }

  return "Refresh: no new.";
}

function formatQuickBooksClientLabel(clientName: string, transactionType: string, amount: number): string {
  const normalizedName = String(clientName || "Unknown client").trim() || "Unknown client";
  const normalizedType = String(transactionType || "").trim().toLowerCase();
  if (normalizedType === "refund") {
    return `${normalizedName} (Refund)`;
  }
  if (normalizedType === "creditmemo" || (normalizedType === "payment" && amount < 0)) {
    return `${normalizedName} (Write-off)`;
  }
  return normalizedName;
}

function formatContactCellValue(value: string): string {
  const text = String(value || "").trim();
  return text || "-";
}

function formatDate(rawValue: string): string {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "-";
  }
  const plainDateMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (plainDateMatch) {
    return `${plainDateMatch[2]}/${plainDateMatch[3]}/${plainDateMatch[1]}`;
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }
  return new Date(timestamp).toLocaleDateString("en-US");
}

function formatDateForApi(value: Date): string {
  const year = String(value.getUTCFullYear());
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  const day = String(value.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}
