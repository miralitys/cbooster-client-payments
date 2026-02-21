import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import { withStableRowKeys, type RowWithKey } from "@/shared/lib/stableRowKeys";
import {
  createQuickBooksSyncJob,
  getQuickBooksOutgoingPayments,
  getQuickBooksPayments,
  getQuickBooksSyncJob,
  getSession,
} from "@/shared/api";
import type { QuickBooksPaymentRow, QuickBooksSyncJob, QuickBooksSyncMeta } from "@/shared/types/quickbooks";
import {
  Button,
  EmptyState,
  ErrorState,
  Input,
  LoadingSkeleton,
  PageHeader,
  PageShell,
  Panel,
  SegmentedControl,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const QUICKBOOKS_FROM_DATE = "2026-01-01";
const QUICKBOOKS_SYNC_POLL_INTERVAL_MS = 1200;
const QUICKBOOKS_SYNC_POLL_MAX_ATTEMPTS = 150;
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

const QUICKBOOKS_MONEY_FLOW_TABS = [
  {
    key: "incoming",
    label: "Входящие деньги",
  },
  {
    key: "outgoing",
    label: "Исходящие деньги",
  },
] as const;

type QuickBooksTab = (typeof QUICKBOOKS_MONEY_FLOW_TABS)[number]["key"];
type QuickBooksViewRow = RowWithKey<QuickBooksPaymentRow>;

export default function QuickBooksPage() {
  const [activeTab, setActiveTab] = useState<QuickBooksTab>("incoming");
  const [canSync, setCanSync] = useState(false);
  const [incomingTransactions, setIncomingTransactions] = useState<QuickBooksViewRow[]>([]);
  const [outgoingTransactions, setOutgoingTransactions] = useState<QuickBooksViewRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [syncWarning, setSyncWarning] = useState("");
  const [statusText, setStatusText] = useState("Loading incoming transactions...");
  const [rangeText, setRangeText] = useState("");
  const [lastLoadPrefix, setLastLoadPrefix] = useState("");

  const [search, setSearch] = useState("");
  const [refundOnly, setRefundOnly] = useState(false);
  const incomingTransactionsRef = useRef<QuickBooksViewRow[]>([]);
  const outgoingTransactionsRef = useRef<QuickBooksViewRow[]>([]);
  const rowKeySequenceRef = useRef(0);
  const allTransactions = activeTab === "incoming" ? incomingTransactions : outgoingTransactions;
  const showOnlyRefunds = activeTab === "incoming" && refundOnly;

  const filteredTransactions = useMemo(
    () => filterTransactions(allTransactions, search, showOnlyRefunds),
    [allTransactions, search, showOnlyRefunds],
  );

  const tableColumns = useMemo<TableColumn<QuickBooksViewRow>[]>(() => {
    if (activeTab === "outgoing") {
      return [
        {
          key: "clientName",
          label: "Payee",
          align: "left",
          cell: (item) => formatQuickBooksPayeeLabel(item.clientName),
        },
        {
          key: "transactionType",
          label: "Type",
          align: "center",
          cell: (item) => formatQuickBooksOutgoingTypeLabel(item.transactionType),
        },
        {
          key: "categoryName",
          label: "Category",
          align: "left",
          cell: (item) => formatQuickBooksOutgoingCategoryLabel(item.categoryName),
        },
        {
          key: "categoryDetails",
          label: "Category details",
          align: "left",
          cell: (item) => formatQuickBooksOutgoingCategoryDetailsLabel(item.categoryDetails),
        },
        {
          key: "description",
          label: "Description",
          align: "left",
          cell: (item) => formatQuickBooksOutgoingDescriptionLabel(item.description),
        },
        {
          key: "paymentAmount",
          label: "Outgoing Amount",
          align: "right",
          cell: (item) => CURRENCY_FORMATTER.format(Number(item.paymentAmount) || 0),
        },
        {
          key: "paymentDate",
          label: "Date",
          align: "center",
          cell: (item) => formatDate(item.paymentDate),
        },
      ];
    }

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
  }, [activeTab]);

  const pollQuickBooksSyncJob = useCallback(async (jobId: string, shouldTotalRefresh: boolean) => {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId) {
      throw new Error("QuickBooks sync job id is missing.");
    }

    for (let attempt = 0; attempt < QUICKBOOKS_SYNC_POLL_MAX_ATTEMPTS; attempt += 1) {
      const payload = await getQuickBooksSyncJob(normalizedJobId);
      const job = payload?.job || null;
      if (job) {
        setStatusText(buildQuickBooksSyncProgressMessage(job, shouldTotalRefresh));
        if (isQuickBooksSyncJobDone(job)) {
          return job;
        }
      }
      await waitForMs(QUICKBOOKS_SYNC_POLL_INTERVAL_MS);
    }

    throw new Error("QuickBooks sync is taking too long. Please check again in a minute.");
  }, []);

  const loadIncomingQuickBooksPayments = useCallback(async (options: LoadOptions = {}) => {
    const shouldSync = Boolean(options.sync);
    const shouldTotalRefresh = Boolean(options.fullSync);
    const rangeTo = formatDateForApi(new Date());
    const previousItems = [...incomingTransactionsRef.current];
    setIsLoading(true);
    setLoadError("");
    setSyncWarning("");

    if (shouldTotalRefresh) {
      setStatusText("Running total refresh from QuickBooks...");
    } else {
      setStatusText(shouldSync ? "Refreshing from QuickBooks..." : "Loading saved transactions...");
    }

    try {
      let syncMetaOverride: QuickBooksSyncMeta | null = null;
      if (shouldSync) {
        const syncJobPayload = await createQuickBooksSyncJob({
          from: QUICKBOOKS_FROM_DATE,
          to: rangeTo,
          fullSync: shouldTotalRefresh,
        });
        const syncJobId = String(syncJobPayload?.job?.id || "").trim();
        const finishedJob = await pollQuickBooksSyncJob(syncJobId, shouldTotalRefresh);
        const failed = String(finishedJob?.status || "").toLowerCase() === "failed";
        if (failed) {
          const failureMessage = String(finishedJob?.error || "").trim();
          throw new Error(failureMessage || "QuickBooks sync failed.");
        }
        syncMetaOverride = finishedJob?.sync || null;
      }

      const payload = await getQuickBooksPayments({
        from: QUICKBOOKS_FROM_DATE,
        to: rangeTo,
      });
      const items = Array.isArray(payload.items) ? payload.items : [];
      setIncomingTransactions(
        withStableRowKeys(items, previousItems, rowKeySequenceRef, {
          prefix: "qb-in",
          signature: quickBooksRowSignature,
        }),
      );
      setRangeText(payload.range?.from && payload.range?.to ? `Range: ${payload.range.from} -> ${payload.range.to}` : "");
      setLastLoadPrefix(buildLoadPrefixFromPayload(payload, shouldSync, shouldTotalRefresh, syncMetaOverride));
      setSyncWarning("");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load transactions.";
      if (!previousItems.length) {
        setLoadError(message);
        setStatusText(message);
        setSyncWarning("");
        setIncomingTransactions([]);
        setRangeText("");
      } else {
        setLoadError("");
        setSyncWarning(message);
        setStatusText(`Saved data is shown. QuickBooks sync failed: ${message}`);
        setIncomingTransactions(previousItems);
      }
    } finally {
      setIsLoading(false);
    }
  }, [pollQuickBooksSyncJob]);

  const loadOutgoingQuickBooksPayments = useCallback(async () => {
    const rangeTo = formatDateForApi(new Date());
    const previousItems = [...outgoingTransactionsRef.current];
    setIsLoading(true);
    setLoadError("");
    setSyncWarning("");
    setStatusText("Loading expense transactions from QuickBooks...");

    try {
      const payload = await getQuickBooksOutgoingPayments({
        from: QUICKBOOKS_FROM_DATE,
        to: rangeTo,
      });
      const items = Array.isArray(payload.items) ? payload.items : [];
      setOutgoingTransactions(
        withStableRowKeys(items, previousItems, rowKeySequenceRef, {
          prefix: "qb-out",
          signature: quickBooksRowSignature,
        }),
      );
      setRangeText(payload.range?.from && payload.range?.to ? `Range: ${payload.range.from} -> ${payload.range.to}` : "");
      setLastLoadPrefix(buildOutgoingLoadPrefixFromPayload(payload));
      setSyncWarning("");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load expense transactions.";
      if (!previousItems.length) {
        setLoadError(message);
        setStatusText(message);
        setSyncWarning("");
        setOutgoingTransactions([]);
        setRangeText("");
      } else {
        setLoadError("");
        setSyncWarning(message);
        setStatusText(`Previous expense data is shown. Latest QuickBooks read failed: ${message}`);
        setOutgoingTransactions(previousItems);
      }
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    incomingTransactionsRef.current = incomingTransactions;
  }, [incomingTransactions]);

  useEffect(() => {
    outgoingTransactionsRef.current = outgoingTransactions;
  }, [outgoingTransactions]);

  useEffect(() => {
    void getSession()
      .then((session) => {
        setCanSync(Boolean(session?.permissions?.sync_quickbooks));
      })
      .catch(() => {
        setCanSync(false);
      });
  }, []);

  useEffect(() => {
    if (activeTab === "incoming") {
      void loadIncomingQuickBooksPayments();
      return;
    }

    setRefundOnly(false);
    void loadOutgoingQuickBooksPayments();
  }, [activeTab, loadIncomingQuickBooksPayments, loadOutgoingQuickBooksPayments]);

  useEffect(() => {
    setStatusText(
      buildFilterStatusMessage(
        allTransactions.length,
        filteredTransactions.length,
        search,
        showOnlyRefunds,
        lastLoadPrefix,
        activeTab,
      ),
    );
  }, [activeTab, allTransactions.length, filteredTransactions.length, lastLoadPrefix, search, showOnlyRefunds]);

  const retryLoad = useCallback(() => {
    if (activeTab === "incoming") {
      void loadIncomingQuickBooksPayments();
      return;
    }
    void loadOutgoingQuickBooksPayments();
  }, [activeTab, loadIncomingQuickBooksPayments, loadOutgoingQuickBooksPayments]);

  useEffect(() => {
    if (!loadError || allTransactions.length > 0) {
      return;
    }

    showToast({
      type: "error",
      message: loadError,
      dedupeKey: `quickbooks-load-error-${activeTab}-${loadError}`,
      cooldownMs: 3200,
      action: {
        label: "Retry",
        onClick: retryLoad,
      },
    });
  }, [activeTab, allTransactions.length, loadError, retryLoad]);

  return (
    <PageShell className="quickbooks-react-page">
      <PageHeader
        actions={
          <div className="cb-page-header-toolbar">
            {activeTab === "incoming" ? (
              <>
                <Button
                  id="refresh-button"
                  type="button"
                  size="sm"
                  onClick={() => void loadIncomingQuickBooksPayments({ sync: true })}
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
                  onClick={() => void loadIncomingQuickBooksPayments({ sync: true, fullSync: true })}
                  isLoading={isLoading}
                  disabled={isLoading || !canSync}
                >
                  Total Refresh
                </Button>
              </>
            ) : (
              <Button
                id="outgoing-reload-button"
                type="button"
                size="sm"
                onClick={() => void loadOutgoingQuickBooksPayments()}
                isLoading={isLoading}
                disabled={isLoading}
              >
                Reload
              </Button>
            )}
          </div>
        }
        meta={rangeText ? <p className="quickbooks-range">{rangeText}</p> : null}
      />

      <Panel
        className="table-panel quickbooks-table-panel"
        title={activeTab === "incoming" ? "Incoming Transactions" : "Expense Transactions"}
        actions={
          <div className="quickbooks-toolbar-react">
            <div className="quickbooks-tabs-wrap">
              <p className="search-label quickbooks-search-field__label">Денежный поток</p>
              <div id="quickbooks-money-flow-tab" className="quickbooks-money-flow-segmented">
                <SegmentedControl
                  value={activeTab}
                  options={QUICKBOOKS_MONEY_FLOW_TABS.map((tab) => ({
                    key: tab.key,
                    label: tab.label,
                  }))}
                  onChange={(rawNextTab) => {
                    const nextTab: QuickBooksTab = rawNextTab === "outgoing" ? "outgoing" : "incoming";
                    setActiveTab(nextTab);
                  }}
                />
              </div>
            </div>
            <div className="quickbooks-search-field">
              <label htmlFor="quickbooks-client-search" className="search-label quickbooks-search-field__label">
                {activeTab === "incoming" ? "Search by client" : "Search by payee"}
              </label>
              <Input
                id="quickbooks-client-search"
                type="search"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder={activeTab === "incoming" ? "Type client name" : "Type payee name"}
                autoComplete="off"
                spellCheck={false}
              />
            </div>
            {activeTab === "incoming" ? (
              <label htmlFor="quickbooks-refund-only" className="cb-checkbox-row quickbooks-refund-filter">
                <input
                  id="quickbooks-refund-only"
                  type="checkbox"
                  checked={refundOnly}
                  onChange={(event) => setRefundOnly(event.target.checked)}
                />
                Only refunds
              </label>
            ) : null}
          </div>
        }
      >
        <p className={`dashboard-message quickbooks-status ${loadError || syncWarning ? "error" : ""}`.trim()}>
          {syncWarning || statusText}
        </p>

        {isLoading ? <LoadingSkeleton rows={7} /> : null}
        {!isLoading && loadError && !filteredTransactions.length ? (
          <ErrorState
            title={activeTab === "incoming" ? "Failed to load incoming transactions" : "Failed to load expense transactions"}
            description={loadError}
            actionLabel="Retry"
            onAction={retryLoad}
          />
        ) : null}
        {!isLoading && !loadError && !filteredTransactions.length ? (
          <EmptyState
            title={
              activeTab === "incoming"
                ? search.trim()
                  ? refundOnly
                    ? `No refunds found for "${search.trim()}".`
                    : `No transactions found for "${search.trim()}".`
                  : refundOnly
                    ? "No refunds found for the selected period."
                    : "No transactions found for the selected period."
                : search.trim()
                  ? `No expense transactions found for "${search.trim()}".`
                  : "No expense transactions found for the selected period."
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
    String(row.categoryName || "").trim(),
    String(row.categoryDetails || "").trim(),
    String(row.description || "").trim(),
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
  tab: QuickBooksTab = "incoming",
): string {
  const normalizedQuery = query.trim();
  const normalizedPrefix = prefix.trim();

  if (tab === "outgoing") {
    let outgoingMessage = "";
    if (!normalizedQuery) {
      outgoingMessage = `Loaded ${totalCount} expense transaction${totalCount === 1 ? "" : "s"}.`;
    } else if (visibleCount === 0) {
      outgoingMessage = `No expense transactions found for "${normalizedQuery}".`;
    } else {
      outgoingMessage = `Showing ${visibleCount} of ${totalCount} expense transactions for "${normalizedQuery}".`;
    }
    return normalizedPrefix ? `${normalizedPrefix} ${outgoingMessage}` : outgoingMessage;
  }

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

function isQuickBooksSyncJobDone(job: QuickBooksSyncJob | null | undefined): boolean {
  if (!job) {
    return false;
  }

  if (job.done === true) {
    return true;
  }

  const status = String(job.status || "").toLowerCase();
  return status === "completed" || status === "failed";
}

function buildQuickBooksSyncProgressMessage(job: QuickBooksSyncJob, totalRefreshRequested: boolean): string {
  const status = String(job?.status || "").toLowerCase();
  const isFullSync = String(job?.syncMode || "").toLowerCase() === "full" || totalRefreshRequested;
  const operationText = isFullSync ? "Total refresh" : "Refresh";

  if (status === "queued") {
    return `${operationText} queued...`;
  }

  if (status === "running") {
    return `${operationText} in progress...`;
  }

  if (status === "completed") {
    return `${operationText} completed. Loading saved transactions...`;
  }

  if (status === "failed") {
    const failureMessage = String(job?.error || "").trim();
    return failureMessage || `${operationText} failed.`;
  }

  return `${operationText} in progress...`;
}

function waitForMs(durationMs: number): Promise<void> {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, Math.max(0, durationMs));
  });
}

function buildLoadPrefixFromPayload(
  payload: {
    sync?: {
      requested?: boolean;
      syncMode?: string;
      performed?: boolean;
      syncFrom?: string;
      fetchedCount?: number;
      insertedCount?: number;
      reconciledCount?: number;
      writtenCount?: number;
      reconciledWrittenCount?: number;
    };
  },
  syncRequested: boolean,
  totalRefreshRequested: boolean,
  syncMetaOverride: QuickBooksSyncMeta | null = null,
): string {
  if (!syncRequested) {
    return "Saved data:";
  }

  const payloadSyncMeta = payload.sync && typeof payload.sync === "object" ? payload.sync : null;
  const syncMeta = syncMetaOverride && typeof syncMetaOverride === "object" ? syncMetaOverride : payloadSyncMeta;
  if (!syncMeta) {
    return "Saved data:";
  }
  if (!syncMetaOverride && !syncMeta.requested) {
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

  const reconciledCount = Number.parseInt(
    String(syncMeta.reconciledWrittenCount ?? syncMeta.reconciledCount ?? ""),
    10,
  );
  if (Number.isFinite(reconciledCount) && reconciledCount > 0) {
    return `Refresh: updated ${reconciledCount} zero rows.`;
  }

  return "Refresh: no new.";
}

function buildOutgoingLoadPrefixFromPayload(payload: { source?: string }): string {
  const source = String(payload?.source || "").trim().toLowerCase();
  if (source === "quickbooks_live") {
    return "QuickBooks live data:";
  }
  return "Expense data:";
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

function formatQuickBooksPayeeLabel(payeeName: string): string {
  const normalizedName = String(payeeName || "").trim();
  return normalizedName || "Unknown payee";
}

function formatQuickBooksOutgoingTypeLabel(transactionType: string): string {
  const normalizedType = String(transactionType || "").trim().toLowerCase();
  if (normalizedType === "expense" || normalizedType === "purchase") {
    return "Expense";
  }
  if (normalizedType === "billpayment") {
    return "Bill Payment";
  }
  if (normalizedType === "check") {
    return "Check";
  }
  return normalizedType ? normalizedType.charAt(0).toUpperCase() + normalizedType.slice(1) : "-";
}

function formatQuickBooksOutgoingCategoryLabel(categoryName: string | undefined): string {
  const normalizedName = String(categoryName || "").trim();
  return normalizedName || "-";
}

function formatQuickBooksOutgoingCategoryDetailsLabel(categoryDetails: string | undefined): string {
  const normalizedValue = String(categoryDetails || "").trim();
  return normalizedValue || "-";
}

function formatQuickBooksOutgoingDescriptionLabel(description: string | undefined): string {
  const normalizedValue = String(description || "").trim();
  return normalizedValue || "-";
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
