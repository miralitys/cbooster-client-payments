import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import { getClientManagers, postGhlClientPhoneRefresh, startClientManagersRefreshBackgroundJob } from "@/shared/api";
import {
  canConfirmQuickBooksPaymentsSession,
  canRefreshClientManagerFromGhlSession,
  canRefreshClientPhoneFromGhlSession,
} from "@/shared/lib/access";
import {
  formatDate,
  formatKpiMoney,
  formatMoney,
  getRecordStatusFlags,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { evaluateClientScore, type ClientScoreResult } from "@/features/client-score/domain/scoring";
import {
  PAYMENT_DATE_FIELDS,
  OVERDUE_RANGE_OPTIONS,
  OVERVIEW_PERIOD_OPTIONS,
  STATUS_FILTER_OVERDUE,
  STATUS_FILTER_OPTIONS,
  TABLE_COLUMNS,
  type OverviewPeriodKey,
} from "@/features/client-payments/domain/constants";
import { exportRecordsToPdf, exportRecordsToXls } from "@/features/client-payments/domain/export";
import { RecordDetails } from "@/features/client-payments/components/RecordDetails";
import { RecordEditorForm } from "@/features/client-payments/components/RecordEditorForm";
import { StatusBadges } from "@/features/client-payments/components/StatusBadges";
import { useClientPayments } from "@/features/client-payments/hooks/useClientPayments";
import type { ClientRecord } from "@/shared/types/records";
import {
  Button,
  Badge,
  DateInput,
  EmptyState,
  ErrorState,
  Input,
  Modal,
  PageHeader,
  PageShell,
  Panel,
  SegmentedControl,
  Select,
  Table,
} from "@/shared/ui";
import type { TableAlign, TableColumn } from "@/shared/ui/Table";

type ScoreFilter = "all" | "0-30" | "30-60" | "60-99" | "100";

interface ScoredClientRecord {
  record: ClientRecord;
  score: ClientScoreResult;
  clientManager: string;
  clientManagerNames: string[];
}

type ClientManagersRefreshMode = "none" | "incremental" | "full";

const SCORE_FILTER_OPTIONS: Array<{ key: ScoreFilter; label: string }> = [
  { key: "all", label: "All" },
  { key: "0-30", label: "0-30" },
  { key: "30-60", label: "30-60" },
  { key: "60-99", label: "60-99" },
  { key: "100", label: "100" },
];
const PAYMENT_COLUMN_MATCH = /^payment(\d+)(Date)?$/;
const buildPaymentColumns = (from: number, to: number): Array<keyof ClientRecord> => {
  const columns: Array<keyof ClientRecord> = [];
  for (let index = from; index <= to; index += 1) {
    columns.push(`payment${index}` as keyof ClientRecord);
    columns.push(`payment${index}Date` as keyof ClientRecord);
  }
  return columns;
};
const EXTENDED_PAYMENT_COLUMNS = buildPaymentColumns(8, 36);
const EXTENDED_PAYMENT_COLUMN_SET = new Set(EXTENDED_PAYMENT_COLUMNS);
const MANAGER_FILTER_ALL = "__all__";
const NO_MANAGER_LABEL = "No manager";
const TEXT_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });

const COLUMN_LABELS: Record<string, string> = {
  clientName: "Client Name",
  clientManager: "Client Manager",
  closedBy: "Closed By",
  companyName: "Company",
  serviceType: "Service",
  contractTotals: "Contract",
  totalPayments: "Paid",
  payment1: "Payment 1",
  payment1Date: "Payment 1 Date",
  payment2: "Payment 2",
  payment2Date: "Payment 2 Date",
  payment3: "Payment 3",
  payment3Date: "Payment 3 Date",
  payment4: "Payment 4",
  payment4Date: "Payment 4 Date",
  payment5: "Payment 5",
  payment5Date: "Payment 5 Date",
  payment6: "Payment 6",
  payment6Date: "Payment 6 Date",
  payment7: "Payment 7",
  payment7Date: "Payment 7 Date",
  futurePayments: "Balance",
  afterResult: "After Result",
  notes: "Notes",
  collection: "COLLECTION",
  dateOfCollection: "Date of collection",
  dateWhenWrittenOff: "Date when written off",
};

const SUMMABLE_TABLE_COLUMNS = new Set<keyof ClientRecord>([
  "contractTotals",
  "totalPayments",
  ...buildPaymentColumns(1, 36).filter((field) => !field.toString().endsWith("Date")),
  "futurePayments",
  "collection",
]);

function resolveColumnLabel(column: string): string {
  const match = column.match(PAYMENT_COLUMN_MATCH);
  if (match) {
    const index = match[1];
    const isDate = Boolean(match[2]);
    return isDate ? `Payment ${index} Date` : `Payment ${index}`;
  }
  return COLUMN_LABELS[column] || column;
}

function getOverviewContextLabel(period: OverviewPeriodKey): string {
  const found = OVERVIEW_PERIOD_OPTIONS.find((option) => option.key === period);
  return found?.label || "Current Week";
}

export default function ClientPaymentsPage() {
  const {
    session,
    canManage,
    isLoading,
    loadError,
    records,
    visibleRecords,
    filters,
    sortState,
    overviewPeriod,
    overviewMetrics,
    closedByOptions,
    filtersCollapsed,
    isSaving,
    saveError,
    saveRetryCount,
    saveRetryMax,
    saveRetryGiveUp,
    saveSuccessNotice,
    hasUnsavedChanges,
    lastSyncedAt,
    modalState,
    isDiscardConfirmOpen,
    activeRecord,
    updateFilter,
    setDateRange,
    setOverviewPeriod,
    setFiltersCollapsed,
    toggleSort,
    forceRefresh,
    openCreateModal,
    openRecordModal,
    startEditRecord,
    requestCloseModal,
    cancelDiscardModalClose,
    discardDraftAndCloseModal,
    updateDraftField,
    saveDraft,
    retrySave,
  } = useClientPayments();

  const [scoreFilter, setScoreFilter] = useState<ScoreFilter>("all");
  const [managerFilter, setManagerFilter] = useState<string>(MANAGER_FILTER_ALL);
  const [isScoreFilterOpen, setIsScoreFilterOpen] = useState(false);
  const [isManagersLoading, setIsManagersLoading] = useState(false);
  const [managersError, setManagersError] = useState("");
  const [managersRefreshMode, setManagersRefreshMode] = useState<ClientManagersRefreshMode>("none");
  const [managersStatusNote, setManagersStatusNote] = useState("Client managers source: local database.");
  const [refreshingCardClientManagerKey, setRefreshingCardClientManagerKey] = useState("");
  const [refreshingCardClientPhoneKey, setRefreshingCardClientPhoneKey] = useState("");
  const [showAllPayments, setShowAllPayments] = useState(false);
  const [isRefreshMenuOpen, setIsRefreshMenuOpen] = useState(false);
  const [isPhonesRefreshLoading, setIsPhonesRefreshLoading] = useState(false);
  const refreshMenuRef = useRef<HTMLDivElement | null>(null);
  const canSyncClientManagers = Boolean(session?.permissions?.sync_client_managers);
  const canRefreshClientManagerInCard = canRefreshClientManagerFromGhlSession(session);
  const canRefreshClientPhoneInCard = canRefreshClientPhoneFromGhlSession(session);
  const canConfirmPendingQuickBooksPayments = canConfirmQuickBooksPaymentsSession(session);
  const canViewRefreshMenu = canRefreshClientManagerInCard;

  const visibleTableColumns = useMemo<Array<keyof ClientRecord>>(
    () =>
      showAllPayments
        ? TABLE_COLUMNS
        : TABLE_COLUMNS.filter((column) => !EXTENDED_PAYMENT_COLUMN_SET.has(column)),
    [showAllPayments],
  );
  const tableColumnKeys = useMemo<Array<keyof ClientRecord | "score" | "clientManager">>(
    () => ["clientName", "clientManager", "score", ...visibleTableColumns.filter((column) => column !== "clientName")],
    [visibleTableColumns],
  );

  const sortableColumns = useMemo(
    () =>
      new Set<keyof ClientRecord>(
        visibleTableColumns.filter((column) => column !== "afterResult" && column !== "writtenOff"),
      ),
    [visibleTableColumns],
  );

  const scoreByRecordId = useMemo(() => {
    const asOfDate = new Date();
    const scoredMap = new Map<string, ClientScoreResult>();

    for (const record of records) {
      scoredMap.set(record.id, evaluateClientScore(record, asOfDate));
    }

    return scoredMap;
  }, [records]);

  const managerFilterOptions = useMemo<string[]>(() => {
    let hasNoManager = false;
    const uniqueByComparable = new Map<string, string>();

    for (const record of visibleRecords) {
      const managerNames = resolveClientManagerNamesFromRecord(record);
      for (const managerName of managerNames) {
        if (managerName === NO_MANAGER_LABEL) {
          hasNoManager = true;
          continue;
        }

        const comparable = normalizeComparableClientName(managerName);
        if (!comparable || uniqueByComparable.has(comparable)) {
          continue;
        }
        uniqueByComparable.set(comparable, managerName);
      }
    }

    const sorted = [...uniqueByComparable.values()].sort((left, right) => TEXT_SORTER.compare(left, right));
    return hasNoManager ? [NO_MANAGER_LABEL, ...sorted] : sorted;
  }, [visibleRecords]);

  const scoredVisibleRecords = useMemo<ScoredClientRecord[]>(() => {
    return visibleRecords
      .map((record) => {
        const clientManagerNames = resolveClientManagerNamesFromRecord(record);
        return {
          record,
          score: scoreByRecordId.get(record.id) || evaluateClientScore(record),
          clientManager: clientManagerNames.join(", "),
          clientManagerNames,
        };
      })
      .filter((item) => matchesScoreFilter(item.score.displayScore, scoreFilter))
      .filter((item) => matchesClientManagerFilter(item.clientManagerNames, managerFilter));
  }, [managerFilter, scoreByRecordId, scoreFilter, visibleRecords]);

  const filteredRecords = useMemo(() => scoredVisibleRecords.map((item) => item.record), [scoredVisibleRecords]);
  const filteredClientNamesForPhoneRefresh = useMemo<string[]>(() => {
    const uniqueByComparable = new Map<string, string>();

    for (const record of filteredRecords) {
      const clientName = (record?.clientName || "").toString().trim();
      if (!clientName) {
        continue;
      }

      const comparableClientName = normalizeComparableClientName(clientName);
      if (!comparableClientName || uniqueByComparable.has(comparableClientName)) {
        continue;
      }

      uniqueByComparable.set(comparableClientName, clientName);
    }

    return [...uniqueByComparable.values()];
  }, [filteredRecords]);
  const summedColumnValues = useMemo(() => {
    const totals = new Map<keyof ClientRecord, number | null>();
    const runningTotals = new Map<keyof ClientRecord, number>();
    const populatedColumns = new Set<keyof ClientRecord>();

    for (const column of SUMMABLE_TABLE_COLUMNS) {
      runningTotals.set(column, 0);
      totals.set(column, null);
    }

    for (const record of filteredRecords) {
      for (const column of SUMMABLE_TABLE_COLUMNS) {
        const amount = parseMoneyValue(record[column]);
        if (amount === null) {
          continue;
        }

        populatedColumns.add(column);
        runningTotals.set(column, (runningTotals.get(column) || 0) + amount);
      }
    }

    for (const column of SUMMABLE_TABLE_COLUMNS) {
      totals.set(column, populatedColumns.has(column) ? runningTotals.get(column) || 0 : null);
    }

    return totals;
  }, [filteredRecords]);
  const managerStatusText = useMemo(() => {
    if (isManagersLoading) {
      if (managersRefreshMode === "full") {
        return "Client managers: running total active-client refresh...";
      }
      if (managersRefreshMode === "incremental") {
        return "Client managers: starting active No-manager refresh...";
      }
      return "Client managers: loading saved data...";
    }

    if (managersError) {
      return `Client managers: ${managersError}`;
    }

    return managersStatusNote;
  }, [isManagersLoading, managersError, managersRefreshMode, managersStatusNote]);

  const isViewMode = modalState.mode === "view";
  const activeRecordClientManagerLabel = useMemo(() => {
    if (!activeRecord) {
      return "";
    }
    return resolveClientManagerNamesFromRecord(activeRecord).join(", ");
  }, [activeRecord]);

  const refreshClientManagers = useCallback(
    async (mode: ClientManagersRefreshMode) => {
      setIsManagersLoading(true);
      setManagersError("");
      setManagersRefreshMode(mode);

      try {
        if (mode === "incremental") {
          const payload = await startClientManagersRefreshBackgroundJob({
            activeOnly: true,
            noManagerOnly: true,
          });
          const totalClientsRaw = Number(payload?.job?.totalClients);
          const totalClients = Number.isFinite(totalClientsRaw) && totalClientsRaw > 0 ? totalClientsRaw : null;
          const detail = totalClients === null ? "" : ` for ${totalClients} active clients with No manager`;
          const message =
            payload?.reused === true
              ? "Active No-manager refresh is already running in background. You will receive a notification when it finishes."
              : `Active No-manager refresh started in background${detail}. You will receive a notification when it finishes.`;

          showToast({
            type: "success",
            message,
          });
          setManagersStatusNote(message);
          return;
        }

        const payload = await getClientManagers(mode, {
          activeOnly: true,
        });
        await forceRefresh();

        const refreshedClientsCountRaw = Number(payload?.refresh?.refreshedClientsCount);
        const refreshedClientsCount =
          Number.isFinite(refreshedClientsCountRaw) && refreshedClientsCountRaw >= 0 ? refreshedClientsCountRaw : 0;
        const savedRecordsCountRaw = Number(payload?.refresh?.savedRecordsCount);
        const savedRecordsCount = Number.isFinite(savedRecordsCountRaw) && savedRecordsCountRaw >= 0 ? savedRecordsCountRaw : 0;

        setManagersStatusNote(
          `Active clients refreshed from GoHighLevel. Refreshed clients: ${refreshedClientsCount}. Saved records: ${savedRecordsCount}.`,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to refresh client-manager data.";
        setManagersError(message);
        showToast({
          type: "error",
          message,
        });
      } finally {
        setIsManagersLoading(false);
        setManagersRefreshMode("none");
      }
    },
    [forceRefresh],
  );

  const startBackgroundClientManagersRefresh = useCallback(async () => {
    setIsManagersLoading(true);
    setManagersError("");
    setManagersRefreshMode("full");

    try {
      const payload = await startClientManagersRefreshBackgroundJob({
        activeOnly: true,
      });
      const totalClientsRaw = Number(payload?.job?.totalClients);
      const totalClients = Number.isFinite(totalClientsRaw) && totalClientsRaw > 0 ? totalClientsRaw : null;
      const detail = totalClients === null ? "" : ` for ${totalClients} active clients`;
      const message =
        payload?.reused === true
          ? "Total active-client refresh is already running in background. You will receive a notification when it finishes."
          : `Total active-client refresh started in background${detail}. You will receive a notification when it finishes.`;

      showToast({
        type: "success",
        message,
      });
      setManagersStatusNote(message);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to start background Client Manager refresh.";
      setManagersError(message);
      showToast({
        type: "error",
        message,
      });
    } finally {
      setIsManagersLoading(false);
      setManagersRefreshMode("none");
    }
  }, []);

  const refreshSingleClientManager = useCallback(
    async (clientName: string) => {
      const clientNameDisplay = (clientName || "").toString().trim();
      const comparableClientName = normalizeComparableClientName(clientNameDisplay);
      if (!comparableClientName) {
        throw new Error("Client name is required.");
      }
      if (!canRefreshClientManagerInCard) {
        throw new Error("Only owner/admin/client-service department head can refresh Client Manager.");
      }

      setRefreshingCardClientManagerKey(comparableClientName);
      setIsManagersLoading(true);
      setManagersRefreshMode("full");
      setManagersError("");
      try {
        await getClientManagers("full", {
          clientName: clientNameDisplay,
        });
        await forceRefresh();
        setManagersStatusNote("Client manager refreshed from GoHighLevel and saved to local database.");

        showToast({
          type: "success",
          message: "Client Manager updated and saved to database.",
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to refresh Client Manager.";
        setManagersError(message);
        showToast({
          type: "error",
          message,
        });
        throw new Error(message);
      } finally {
        setIsManagersLoading(false);
        setManagersRefreshMode("none");
        setRefreshingCardClientManagerKey("");
      }
    },
    [canRefreshClientManagerInCard, forceRefresh],
  );

  const refreshSingleClientPhone = useCallback(
    async (clientName: string) => {
      const clientNameDisplay = (clientName || "").toString().trim();
      const comparableClientName = normalizeComparableClientName(clientNameDisplay);
      if (!comparableClientName) {
        throw new Error("Client name is required.");
      }
      if (!canRefreshClientPhoneInCard) {
        throw new Error("Only owner/admin/client-service department head can refresh Phone.");
      }
      if (!activeRecord || normalizeComparableClientName(activeRecord.clientName) !== comparableClientName) {
        throw new Error("Active client record is not available.");
      }

      setRefreshingCardClientPhoneKey(comparableClientName);
      try {
        const payload = await postGhlClientPhoneRefresh(clientNameDisplay);
        const nextPhone = (payload?.phone || "").toString().trim();
        if (payload?.status !== "found" || !nextPhone) {
          throw new Error("Phone was not returned by GoHighLevel.");
        }
        await forceRefresh();
        showToast({
          type: "success",
          message: `Phone updated: ${nextPhone}`,
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to refresh Phone.";
        showToast({
          type: "error",
          message,
        });
        throw new Error(message);
      } finally {
        setRefreshingCardClientPhoneKey("");
      }
    },
    [activeRecord, canRefreshClientPhoneInCard, forceRefresh],
  );

  const refreshFilteredClientPhones = useCallback(async () => {
    if (!canRefreshClientPhoneInCard) {
      showToast({
        type: "error",
        message: "Only owner/admin/client-service department head can refresh Phone.",
      });
      return;
    }

    if (!filteredClientNamesForPhoneRefresh.length) {
      showToast({
        type: "info",
        message: "No filtered clients to refresh phone.",
      });
      return;
    }

    setIsPhonesRefreshLoading(true);
    try {
      let refreshedClientsCount = 0;
      let notFoundClientsCount = 0;
      let failedClientsCount = 0;
      const refreshedClientPhones = new Set<string>();

      for (const clientName of filteredClientNamesForPhoneRefresh) {
        try {
          const payload = await postGhlClientPhoneRefresh(clientName);
          const nextPhone = (payload?.phone || "").toString().trim();
          if (payload?.status === "found" && nextPhone) {
            refreshedClientsCount += 1;
            refreshedClientPhones.add(normalizeComparableClientName(clientName));
          } else {
            notFoundClientsCount += 1;
          }
        } catch {
          failedClientsCount += 1;
        }
      }

      await forceRefresh();

      let savedRecordsCount = 0;
      for (const record of filteredRecords) {
        const comparableClientName = normalizeComparableClientName(record?.clientName || "");
        if (!comparableClientName || !refreshedClientPhones.has(comparableClientName)) {
          continue;
        }
        savedRecordsCount += 1;
      }

      const summary = `Phones refresh finished for filtered clients. Refreshed: ${refreshedClientsCount}. Not found: ${notFoundClientsCount}. Failed: ${failedClientsCount}. Saved records: ${savedRecordsCount}.`;
      showToast({
        type: failedClientsCount > 0 ? "info" : "success",
        message: summary,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to refresh filtered client phones.";
      showToast({
        type: "error",
        message,
      });
    } finally {
      setIsPhonesRefreshLoading(false);
    }
  }, [canRefreshClientPhoneInCard, filteredClientNamesForPhoneRefresh, filteredRecords, forceRefresh]);

  const counters = useMemo(() => {
    let writtenOffCount = 0;
    let fullyPaidCount = 0;
    let overdueCount = 0;

    for (const record of filteredRecords) {
      const status = getRecordStatusFlags(record);
      if (status.isWrittenOff) {
        writtenOffCount += 1;
      }
      if (status.isFullyPaid) {
        fullyPaidCount += 1;
      }
      if (status.isOverdue) {
        overdueCount += 1;
      }
    }

    return {
      totalCount: records.length,
      filteredCount: filteredRecords.length,
      writtenOffCount,
      fullyPaidCount,
      overdueCount,
    };
  }, [filteredRecords, records.length]);

  const tableColumns = useMemo<TableColumn<ScoredClientRecord>[]>(() => {
    return tableColumnKeys.map((column) => {
      const isSortable = column !== "score" && column !== "clientManager" ? sortableColumns.has(column) : false;
      const isActive = column !== "score" && column !== "clientManager" ? sortState.key === column : false;
    const headerLabel = column === "score" ? "Score" : resolveColumnLabel(column);

      return {
        key: column,
        label: isSortable ? (
          <button
            type="button"
            className={`th-sort-btn ${isActive ? "is-active" : ""}`.trim()}
            onClick={() => {
              if (column !== "score" && column !== "clientManager") {
                toggleSort(column);
              }
            }}
          >
            <span className="th-sort-label">{headerLabel}</span>
            {isActive ? <span className="th-sort-indicator">{sortState.direction === "asc" ? "↑" : "↓"}</span> : null}
          </button>
        ) : (
          <span className="th-sort-label">{headerLabel}</span>
        ),
        headerClassName: getClientPaymentsColumnClassName(column),
        className: getClientPaymentsColumnClassName(column),
        align: getColumnAlign(column),
        cell: (row) => {
          const record = row.record;

          if (column === "score") {
            if (row.score.displayScore === null) {
              return "-";
            }

            return <Badge tone={row.score.tone}>{row.score.displayScore}</Badge>;
          }

          switch (column) {
            case "clientName":
              return (
                <div className="client-name-cell">
                  <strong className="client-name-cell__name">{record.clientName || "Unnamed"}</strong>
                  <StatusBadges record={record} />
                </div>
              );
            case "clientManager":
              return row.clientManager;
            case "closedBy":
              return record.closedBy || "-";
            case "companyName":
              return record.companyName || "-";
            case "serviceType":
              return record.serviceType || "-";
            case "contractTotals":
              return formatMoneyCell(record.contractTotals);
            case "totalPayments":
              return formatMoneyCell(record.totalPayments);
            case "payment1":
            case "payment2":
            case "payment3":
            case "payment4":
            case "payment5":
            case "payment6":
            case "payment7":
              return formatMoneyCell(record[column]);
            case "payment1Date":
            case "payment2Date":
            case "payment3Date":
            case "payment4Date":
            case "payment5Date":
            case "payment6Date":
            case "payment7Date":
            case "dateOfCollection":
            case "dateWhenWrittenOff":
              return formatDate((record[column] || "").toString());
            case "futurePayments":
              return formatMoneyCell(record.futurePayments);
            case "afterResult":
              return record.afterResult ? "Yes" : "No";
            case "notes":
              return record.notes || "-";
            case "collection":
              return formatMoneyCell(record.collection);
            default:
              if (typeof column === "string") {
                const match = column.match(PAYMENT_COLUMN_MATCH);
                if (match) {
                  const isDate = Boolean(match[2]);
                  return isDate
                    ? formatDate((record[column as keyof ClientRecord] || "").toString())
                    : formatMoneyCell(record[column as keyof ClientRecord]);
                }
              }
              return "";
          }
        },
      };
    });
  }, [sortState.direction, sortState.key, sortableColumns, tableColumnKeys, toggleSort]);

  useEffect(() => {
    function onPointerDown(event: MouseEvent) {
      if (!refreshMenuRef.current) {
        return;
      }

      const target = event.target;
      if (target instanceof Node && !refreshMenuRef.current.contains(target)) {
        setIsRefreshMenuOpen(false);
      }
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setIsRefreshMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  useEffect(() => {
    if (isManagersLoading || isPhonesRefreshLoading) {
      setIsRefreshMenuOpen(false);
    }
  }, [isManagersLoading, isPhonesRefreshLoading]);

  useEffect(() => {
    if (!saveError) {
      return;
    }

    showToast({
      type: "error",
      message: saveError,
      dedupeKey: `client-payments-save-error-${saveError}`,
      cooldownMs: 3000,
      action: saveRetryGiveUp
        ? {
            label: "Retry",
            onClick: retrySave,
          }
        : undefined,
      durationMs: saveRetryGiveUp ? 6000 : 4200,
    });
  }, [retrySave, saveError, saveRetryGiveUp]);

  useEffect(() => {
    if (!saveSuccessNotice || saveError || hasUnsavedChanges || isSaving) {
      return;
    }

    showToast({
      type: "success",
      message: saveSuccessNotice,
      dedupeKey: "client-payments-save-success",
      cooldownMs: 3200,
      durationMs: 2200,
    });
  }, [hasUnsavedChanges, isSaving, saveError, saveSuccessNotice]);

  function clearAllFilters() {
    updateFilter("search", "");
    updateFilter("closedBy", "");
    updateFilter("status", "all");
    updateFilter("overdueRange", "");
    setDateRange("createdAtRange", "from", "");
    setDateRange("createdAtRange", "to", "");
    setDateRange("paymentDateRange", "from", "");
    setDateRange("paymentDateRange", "to", "");
    setDateRange("writtenOffDateRange", "from", "");
    setDateRange("writtenOffDateRange", "to", "");
    setDateRange("fullyPaidDateRange", "from", "");
    setDateRange("fullyPaidDateRange", "to", "");
    setScoreFilter("all");
    setManagerFilter(MANAGER_FILTER_ALL);
    setIsScoreFilterOpen(false);
  }

  return (
    <PageShell className="client-payments-react-page">
      <PageHeader
        actions={
          canManage ? (
            <Button size="sm" onClick={openCreateModal}>
              Add Client
            </Button>
          ) : null
        }
        meta={
          <div className="client-payments-page-header-meta">
            <div className="page-header__stats">
              <span className="stat-chip">
                <span className="stat-chip__label">Clients:</span>
                <span className="stat-chip__value">{counters.totalCount}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Filtered:</span>
                <span className="stat-chip__value">{counters.filteredCount}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Written Off:</span>
                <span className="stat-chip__value">{counters.writtenOffCount}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Fully Paid:</span>
                <span className="stat-chip__value">{counters.fullyPaidCount}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Overdue:</span>
                <span className="stat-chip__value">{counters.overdueCount}</span>
              </span>
            </div>
            <div className="table-panel-sync-row">
              <p className="table-panel-updated-at">Last sync: {lastSyncedAt}</p>
              {isSaving ? (
                <span className="cb-badge cb-badge--info cb-badge--with-spinner">
                  <span className="cb-inline-spinner" aria-hidden="true" />
                  Saving...
                </span>
              ) : hasUnsavedChanges ? (
                <span className="cb-badge cb-badge--warning">Unsaved changes</span>
              ) : null}
              {!saveRetryGiveUp && saveRetryCount > 0 ? (
                <span className="react-user-footnote">
                  Retry {saveRetryCount}/{saveRetryMax}
                </span>
              ) : null}
              {saveRetryGiveUp ? (
                <Button type="button" variant="secondary" size="sm" onClick={retrySave}>
                  Retry Save
                </Button>
              ) : null}
            </div>
          </div>
        }
      />

      <div className={`grid dashboard-grid-react ${filtersCollapsed ? "is-filters-collapsed" : ""}`.trim()}>
        <Panel
          className="filters-panel"
          title="Filters"
          actions={
            <button
              type="button"
              className="filters-toggle-btn"
              aria-label={filtersCollapsed ? "Expand filters panel" : "Collapse filters panel"}
              aria-expanded={!filtersCollapsed}
              onClick={() => setFiltersCollapsed(!filtersCollapsed)}
            >
              <span className="filters-toggle-icon" aria-hidden="true">
                {filtersCollapsed ? "›" : "‹"}
              </span>
            </button>
          }
        >
          {!filtersCollapsed ? (
            <div className="filters-panel__content">
              <label className="search-label" htmlFor="records-search-input">
                Search by name, email, phone, or SSN
              </label>
              <div className="search-row">
                <Input
                  id="records-search-input"
                  value={filters.search}
                  onChange={(event) => updateFilter("search", event.target.value)}
                  placeholder="For example: John Smith, john@email.com, +1(555)555-5555, 123-45-6789"
                />
                <Button type="button" variant="secondary" size="sm" onClick={clearAllFilters}>
                  Reset
                </Button>
              </div>

              <div className="filters-grid-react">
                <div className="filter-field">
                  <label htmlFor="created-from-input">New Client From</label>
                  <DateInput
                    id="created-from-input"
                    value={filters.createdAtRange.from}
                    onChange={(nextValue) => setDateRange("createdAtRange", "from", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="created-to-input">To</label>
                  <DateInput
                    id="created-to-input"
                    value={filters.createdAtRange.to}
                    onChange={(nextValue) => setDateRange("createdAtRange", "to", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="payments-from-input">Payments From</label>
                  <DateInput
                    id="payments-from-input"
                    value={filters.paymentDateRange.from}
                    onChange={(nextValue) => setDateRange("paymentDateRange", "from", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="payments-to-input">To</label>
                  <DateInput
                    id="payments-to-input"
                    value={filters.paymentDateRange.to}
                    onChange={(nextValue) => setDateRange("paymentDateRange", "to", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field filter-field--full">
                  <label htmlFor="closed-by-filter-select">Closed By</label>
                  <Select
                    id="closed-by-filter-select"
                    value={filters.closedBy}
                    onChange={(event) => updateFilter("closedBy", event.target.value)}
                  >
                    <option value="">All</option>
                    {closedByOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </Select>
                </div>
                <div className="filter-field filter-field--full">
                  <label htmlFor="client-manager-filter-select">Client Manager</label>
                  <Select
                    id="client-manager-filter-select"
                    value={managerFilter}
                    onChange={(event) => setManagerFilter(event.target.value)}
                  >
                    <option value={MANAGER_FILTER_ALL}>All</option>
                    {managerFilterOptions.map((managerName) => (
                      <option key={managerName} value={managerName}>
                        {managerName}
                      </option>
                    ))}
                  </Select>
                </div>
              </div>

              <SegmentedControl
                value={filters.status}
                options={STATUS_FILTER_OPTIONS}
                onChange={(value) => updateFilter("status", value as typeof filters.status)}
              />

              {filters.status === STATUS_FILTER_OVERDUE ? (
                <SegmentedControl
                  value={filters.overdueRange}
                  options={OVERDUE_RANGE_OPTIONS}
                  onChange={(value) => updateFilter("overdueRange", value as typeof filters.overdueRange)}
                />
              ) : null}

              {filters.status === "written-off" ? (
                <div className="written-off-date-filter-react">
                  <div className="filter-field">
                    <label htmlFor="written-off-from-input">Written Off Date From</label>
                    <DateInput
                      id="written-off-from-input"
                      value={filters.writtenOffDateRange.from}
                      onChange={(nextValue) => setDateRange("writtenOffDateRange", "from", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                  <div className="filter-field">
                    <label htmlFor="written-off-to-input">To</label>
                    <DateInput
                      id="written-off-to-input"
                      value={filters.writtenOffDateRange.to}
                      onChange={(nextValue) => setDateRange("writtenOffDateRange", "to", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                </div>
              ) : null}

              {filters.status === "fully-paid" ? (
                <div className="written-off-date-filter-react">
                  <div className="filter-field">
                    <label htmlFor="fully-paid-from-input">Fully Paid Date From</label>
                    <DateInput
                      id="fully-paid-from-input"
                      value={filters.fullyPaidDateRange.from}
                      onChange={(nextValue) => setDateRange("fullyPaidDateRange", "from", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                  <div className="filter-field">
                    <label htmlFor="fully-paid-to-input">To</label>
                    <DateInput
                      id="fully-paid-to-input"
                      value={filters.fullyPaidDateRange.to}
                      onChange={(nextValue) => setDateRange("fullyPaidDateRange", "to", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                </div>
              ) : null}

              <div className="score-filter-block">
                <div className="score-filter-block__header">
                  <p className="search-label">Score</p>
                  <Button
                    type="button"
                    variant={isScoreFilterOpen ? "primary" : "secondary"}
                    size="sm"
                    onClick={() => setIsScoreFilterOpen((prev) => !prev)}
                  >
                    Score
                  </Button>
                </div>
                {isScoreFilterOpen ? (
                  <SegmentedControl
                    value={scoreFilter}
                    options={SCORE_FILTER_OPTIONS}
                    onChange={(value) => setScoreFilter(value as ScoreFilter)}
                  />
                ) : null}
              </div>
            </div>
          ) : null}
        </Panel>

        <Panel
          className="period-dashboard-shell-react"
          title="Overview"
          actions={
            <SegmentedControl
              value={overviewPeriod}
              options={OVERVIEW_PERIOD_OPTIONS.map((option) => ({ key: option.key, label: option.label }))}
              onChange={(value) => setOverviewPeriod(value as OverviewPeriodKey)}
            />
          }
        >
          <div className="overview-cards">
            <article className="overview-card">
              <p className="overview-card__title">Sales</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.sales)}</p>
              <p className="overview-card__context">{getOverviewContextLabel(overviewPeriod)}</p>
            </article>
            <article className="overview-card">
              <p className="overview-card__title">Received</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.received)}</p>
              <p className="overview-card__context">{getOverviewContextLabel(overviewPeriod)}</p>
            </article>
            <article className="overview-card">
              <p className="overview-card__title">Debt</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.debt)}</p>
              <p className="overview-card__context">As of today</p>
            </article>
          </div>
        </Panel>

        <Panel
          className="table-panel"
          title="Records"
          actions={
            <div className="table-panel__actions">
              <Button variant="secondary" size="sm" onClick={() => void forceRefresh()} isLoading={isLoading}>
                Refresh
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setShowAllPayments((prev) => !prev)}
              >
                {showAllPayments ? "Hide Extra Payments" : "Show All Payments"}
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToXls(filteredRecords)}>
                Export XLS
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToPdf(filteredRecords)}>
                Export PDF
              </Button>
              {canViewRefreshMenu ? (
                <div ref={refreshMenuRef} className={`refresh-manager-menu ${isRefreshMenuOpen ? "is-open" : ""}`.trim()}>
                  <Button
                    variant="secondary"
                    size="sm"
                    className="refresh-manager-menu__toggle"
                    onClick={() => setIsRefreshMenuOpen((prev) => !prev)}
                    isLoading={isManagersLoading || isPhonesRefreshLoading}
                    aria-haspopup="menu"
                    aria-expanded={isRefreshMenuOpen}
                    aria-controls="client-payments-refresh-manager-menu"
                  >
                    {`Refresh ${isRefreshMenuOpen ? "▴" : "▾"}`}
                  </Button>
                  <div
                    id="client-payments-refresh-manager-menu"
                    className="refresh-manager-menu__panel"
                    role="menu"
                    hidden={!isRefreshMenuOpen}
                  >
                    <button
                      type="button"
                      className="refresh-manager-menu__item"
                      role="menuitem"
                      onClick={() => {
                        setIsRefreshMenuOpen(false);
                        void refreshClientManagers("incremental");
                      }}
                      disabled={isManagersLoading || isPhonesRefreshLoading || !canSyncClientManagers}
                    >
                      Refresh Manager
                    </button>
                    <button
                      type="button"
                      className="refresh-manager-menu__item"
                      role="menuitem"
                      onClick={() => {
                        setIsRefreshMenuOpen(false);
                        void startBackgroundClientManagersRefresh();
                      }}
                      disabled={isManagersLoading || isPhonesRefreshLoading || !canSyncClientManagers}
                    >
                      Total Refresh Manager
                    </button>
                    <button
                      type="button"
                      className="refresh-manager-menu__item"
                      role="menuitem"
                      onClick={() => {
                        setIsRefreshMenuOpen(false);
                        void refreshFilteredClientPhones();
                      }}
                      disabled={isManagersLoading || isPhonesRefreshLoading || !canRefreshClientPhoneInCard}
                    >
                      Add/Refresh Phones
                    </button>
                  </div>
                </div>
              ) : null}
            </div>
          }
        >
          <p className="dashboard-message client-payments-manager-status">{managerStatusText}</p>
          {isLoading ? <TableLoadingSkeleton columnCount={tableColumnKeys.length} /> : null}
          {!isLoading && loadError ? (
            <ErrorState
              title="Failed to load records"
              description={loadError}
              actionLabel="Retry"
              onAction={() => void forceRefresh()}
            />
          ) : null}
          {!isLoading && !loadError && !scoredVisibleRecords.length ? (
            <EmptyState title="No records found" description="Adjust filters or add a new client." />
          ) : null}

          {!isLoading && !loadError && scoredVisibleRecords.length ? (
            <Table
              className="table-wrap"
              columns={tableColumns}
              rows={scoredVisibleRecords}
              rowKey={(row) => row.record.id}
              density="compact"
              virtualizeRows
              virtualRowHeight={52}
              virtualOverscan={10}
              virtualThreshold={80}
              onRowActivate={(row) => openRecordModal(row.record)}
              footer={
                <tr>
                  {tableColumnKeys.map((column) => {
                    if (column === "clientName") {
                      return (
                        <td key={column}>
                          <strong>Totals</strong>
                        </td>
                      );
                    }

                    if (column === "score") {
                      return <td key={column}>-</td>;
                    }

                    if (column === "clientManager") {
                      return <td key={column}>-</td>;
                    }

                    if (column === "closedBy") {
                      return <td key={column}>{`${filteredRecords.length} clients`}</td>;
                    }

                    if (SUMMABLE_TABLE_COLUMNS.has(column)) {
                      const sum = summedColumnValues.get(column as keyof ClientRecord) ?? null;
                      return (
                        <td key={column} className={getColumnAlign(column) === "right" ? "cb-table__cell--align-right" : undefined}>
                          {sum === null ? "-" : formatMoney(sum)}
                        </td>
                      );
                    }

                    return <td key={column}>-</td>;
                  })}
                </tr>
              }
            />
          ) : null}
        </Panel>
      </div>

      <Modal
        open={modalState.open}
        title={
          modalState.mode === "create"
            ? "Create Client"
            : modalState.mode === "edit"
              ? "Edit Client"
              : activeRecord?.clientName || "Client Details"
        }
        onClose={requestCloseModal}
        footer={
          <div className="client-payments__modal-actions">
            <Button variant="secondary" size="sm" onClick={requestCloseModal}>
              Close
            </Button>
            {isViewMode ? (
              <Button size="sm" onClick={startEditRecord}>
                Edit
              </Button>
            ) : null}
            {!isViewMode ? (
              <Button size="sm" onClick={saveDraft}>
                Save
              </Button>
            ) : null}
          </div>
        }
      >
        {isViewMode && activeRecord ? (
          <RecordDetails
            record={activeRecord}
            clientManagerLabel={activeRecordClientManagerLabel}
            canRefreshClientManager={canRefreshClientManagerInCard}
            isRefreshingClientManager={refreshingCardClientManagerKey === normalizeComparableClientName(activeRecord.clientName)}
            onRefreshClientManager={refreshSingleClientManager}
            canRefreshClientPhone={canRefreshClientPhoneInCard}
            isRefreshingClientPhone={refreshingCardClientPhoneKey === normalizeComparableClientName(activeRecord.clientName)}
            onRefreshClientPhone={refreshSingleClientPhone}
            canConfirmPendingQuickBooksPayments={canConfirmPendingQuickBooksPayments}
          />
        ) : null}
        {!isViewMode ? <RecordEditorForm draft={modalState.draft} onChange={updateDraftField} /> : null}
      </Modal>

      <Modal
        open={isDiscardConfirmOpen}
        title="Discard Changes?"
        onClose={cancelDiscardModalClose}
        footer={
          <div className="client-payments__modal-actions">
            <Button type="button" variant="secondary" size="sm" onClick={cancelDiscardModalClose}>
              Cancel
            </Button>
            <Button type="button" variant="danger" size="sm" onClick={discardDraftAndCloseModal}>
              Discard
            </Button>
          </div>
        }
      >
        <p>You have unsaved changes. Discard?</p>
      </Modal>

      {session?.user?.displayName ? <p className="react-user-footnote">Signed in as: {session.user.displayName}</p> : null}
    </PageShell>
  );
}

function matchesScoreFilter(displayScore: number | null, filter: ScoreFilter): boolean {
  if (filter === "all") {
    return true;
  }

  if (displayScore === null) {
    return false;
  }

  if (filter === "0-30") {
    return displayScore >= 0 && displayScore <= 30;
  }

  if (filter === "30-60") {
    return displayScore > 30 && displayScore <= 60;
  }

  if (filter === "60-99") {
    return displayScore > 60 && displayScore < 100;
  }

  return displayScore === 100;
}

function getColumnAlign(column: keyof ClientRecord | "score" | "clientManager"): TableAlign {
  if (column === "score") {
    return "center";
  }

  if (column === "clientManager") {
    return "left";
  }

  if (
    column === "createdAt" ||
    column === "dateOfCollection" ||
    column === "dateWhenWrittenOff" ||
    column === "dateWhenFullyPaid" ||
    column === "afterResult" ||
    column === "writtenOff" ||
    PAYMENT_DATE_FIELDS.includes(column)
  ) {
    return "center";
  }

  if (
    column === "contractTotals" ||
    column === "totalPayments" ||
    column === "futurePayments" ||
    column === "collection" ||
    column === "payment1" ||
    column === "payment2" ||
    column === "payment3" ||
    column === "payment4" ||
    column === "payment5" ||
    column === "payment6" ||
    column === "payment7"
  ) {
    return "right";
  }

  return "left";
}

function getClientPaymentsColumnClassName(column: keyof ClientRecord | "score" | "clientManager"): string | undefined {
  if (column === "clientName") {
    return "client-name-column";
  }

  if (column === "clientManager") {
    return "client-manager-column";
  }

  return undefined;
}

function resolveClientManagerNamesFromRecord(record: ClientRecord): string[] {
  return splitClientManagerLabel(record.clientManager);
}

function splitClientManagerLabel(rawLabel: string): string[] {
  const value = (rawLabel || "").toString().trim();
  if (!value || value === "-" || value.toLowerCase() === "unassigned") {
    return [NO_MANAGER_LABEL];
  }

  const names = value
    .split(",")
    .map((value) => value.trim())
    .filter((item) => {
      if (!item) {
        return false;
      }
      const normalized = item.toLowerCase();
      return normalized !== "-" && normalized !== "unassigned" && normalized !== "no manager";
    });
  if (!names.length) {
    return [NO_MANAGER_LABEL];
  }

  return [...new Set(names)];
}

function matchesClientManagerFilter(managerNames: string[], selectedManager: string): boolean {
  if (!selectedManager || selectedManager === MANAGER_FILTER_ALL) {
    return true;
  }

  const selectedComparable = normalizeComparableClientName(selectedManager);
  if (!selectedComparable) {
    return true;
  }

  return managerNames.some((name) => normalizeComparableClientName(name) === selectedComparable);
}

function normalizeComparableClientName(rawValue: string): string {
  return (rawValue || "")
    .toString()
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function formatMoneyCell(rawValue: string): string {
  const amount = parseMoneyValue(rawValue);
  if (amount === null) {
    return "-";
  }
  return formatMoney(amount);
}

function TableLoadingSkeleton({ columnCount }: { columnCount: number }) {
  return (
    <div className="table-wrap table-wrap--loading">
      <table className="cb-table cb-table--compact">
        <thead>
          <tr>
            {Array.from({ length: columnCount }).map((_, index) => (
              <th key={index}>
                <span className="cb-table-skeleton__head" />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 8 }).map((_, rowIndex) => (
            <tr key={rowIndex}>
              {Array.from({ length: columnCount }).map((_, cellIndex) => (
                <td key={cellIndex}>
                  <span className="cb-table-skeleton__cell" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
