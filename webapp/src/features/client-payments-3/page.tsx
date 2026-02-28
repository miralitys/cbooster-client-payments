import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import {
  getClientFilterOptions,
  getClientManagers,
  getClientsTotals,
  postGhlClientPhoneRefresh,
  startClientManagersRefreshBackgroundJob,
} from "@/shared/api";
import {
  canConfirmQuickBooksPaymentsSession,
  canDeleteClientSession,
  canRefreshClientManagerFromGhlSession,
  canRefreshClientPhoneFromGhlSession,
} from "@/shared/lib/access";
import {
  formatDate,
  getRecordStatusFlags,
  type ClientPaymentsFilters,
} from "@/features/client-payments/domain/calculations";
import { OVERDUE_RANGE_OPTIONS, STATUS_FILTER_ALL, TABLE_COLUMNS, type OverviewPeriodKey } from "@/features/client-payments/domain/constants";
import { exportRecordsToPdf, exportRecordsToXls } from "@/features/client-payments/domain/export";
import { RecordDetails } from "@/features/client-payments/components/RecordDetails";
import { RecordEditorForm } from "@/features/client-payments/components/RecordEditorForm";
import { StatusBadges } from "@/features/client-payments/components/StatusBadges";
import { useClientPayments } from "@/features/client-payments/hooks/useClientPayments";
import { evaluateClientScore } from "@/features/client-score/domain/scoring";
import { ActiveFiltersBar, type ActiveFilterChip } from "@/features/client-payments-3/components/ActiveFiltersBar";
import { type GridDensity, ToolbarMenu } from "@/features/client-payments-3/components/ToolbarMenu";
import { MetricCards } from "@/features/client-payments-3/components/MetricCards";
import { formatMoneyFromCents, parseMoneyToCents } from "@/features/client-payments-3/domain/money";
import { calculateTotalsByFieldCents, getDefaultTotalsKeys } from "@/features/client-payments-3/domain/totals";
import type { ClientRecord } from "@/shared/types/records";
import { Badge, Button, DateInput, EmptyState, ErrorState, Input, Modal, PageHeader, PageShell, Select, Table } from "@/shared/ui";
import type { TableAlign, TableColumn } from "@/shared/ui/Table";

import "@/features/client-payments-3/styles/tokens.css";
import "@/features/client-payments-3/styles/page.css";

const MANAGER_FILTER_ALL = "__all__";
const NO_MANAGER_LABEL = "No manager";
const PAYMENT_COLUMN_MATCH = /^payment(\d+)(Date)?$/;
const PAYMENT_AMOUNT_COLUMN_MATCH = /^payment(\d+)$/;
const EXTENDED_PAYMENT_COLUMNS = buildPaymentColumns(8, 36);
const EXTENDED_PAYMENT_COLUMN_SET = new Set(EXTENDED_PAYMENT_COLUMNS);

const STATUS_OPTIONS_RU: Array<{ key: string; label: string }> = [
  { key: "all", label: "Все" },
  { key: "active", label: "Активные" },
  { key: "written-off", label: "Списанные" },
  { key: "fully-paid", label: "Полностью оплаченные" },
  { key: "after-result", label: "After Result" },
  { key: "overdue", label: "Просроченные" },
];

const OVERVIEW_PERIOD_OPTIONS_RU: Array<{ key: OverviewPeriodKey; label: string }> = [
  { key: "currentWeek", label: "Текущая неделя" },
  { key: "previousWeek", label: "Прошлая неделя" },
  { key: "currentMonth", label: "Текущий месяц" },
  { key: "last30Days", label: "30 дней" },
];

const COLUMN_LABELS_RU: Record<string, string> = {
  clientName: "Клиент",
  clientManager: "Client Manager",
  closedBy: "Sales Manager",
  score: "Скор",
  contractTotals: "Контракт",
  totalPayments: "Оплачено",
  futurePayments: "Долг",
  companyName: "Компания",
  afterResult: "After Result",
  writtenOff: "Written Off",
  notes: "Заметки",
  collection: "Collection",
  dateOfCollection: "Дата Collection",
  dateWhenWrittenOff: "Дата списания",
};

interface ScoredRow {
  record: ClientRecord;
  scoreValue: number | null;
  scoreTone: "neutral" | "success" | "warning" | "danger" | "info";
  clientManagerLabel: string;
}

interface ServerTotalsState {
  loading: boolean;
  error: string;
  totalsCents: {
    contractTotals: number;
    totalPayments: number;
    futurePayments: number;
    collection: number;
  };
  rowCount: number;
  invalidFieldsCount: number;
  source: "server" | "loaded";
}

interface LocalAnalyticsEvent {
  event: string;
  meta?: Record<string, string | number | boolean | null>;
}

export default function ClientPayments3Page() {
  const [managerFilter, setManagerFilter] = useState<string>(MANAGER_FILTER_ALL);
  const [showAllPayments, setShowAllPayments] = useState(false);
  const [density, setDensity] = useState<GridDensity>("compact");
  const [isFilterOptionsLoading, setIsFilterOptionsLoading] = useState(true);
  const [filterOptionsError, setFilterOptionsError] = useState("");
  const [serverFilterOptions, setServerFilterOptions] = useState<{
    closedByOptions: string[];
    clientManagerOptions: string[];
  }>({
    closedByOptions: [],
    clientManagerOptions: [],
  });
  const [isManagersLoading, setIsManagersLoading] = useState(false);
  const [isPhonesRefreshLoading, setIsPhonesRefreshLoading] = useState(false);
  const [managersStatusNote, setManagersStatusNote] = useState("Client managers source: local database.");
  const [refreshingCardClientManagerKey, setRefreshingCardClientManagerKey] = useState("");
  const [refreshingCardClientPhoneKey, setRefreshingCardClientPhoneKey] = useState("");
  const [serverTotals, setServerTotals] = useState<ServerTotalsState>({
    loading: false,
    error: "",
    totalsCents: {
      contractTotals: 0,
      totalPayments: 0,
      futurePayments: 0,
      collection: 0,
    },
    rowCount: 0,
    invalidFieldsCount: 0,
    source: "loaded",
  });

  const firstActionTrackedRef = useRef(false);
  const mountedAtRef = useRef(typeof performance !== "undefined" ? performance.now() : Date.now());

  const buildClientsApiQuery = useCallback(
    (currentFilters: ClientPaymentsFilters): Record<string, string | undefined> => ({
      search: currentFilters.search || undefined,
      closedBy: currentFilters.closedBy || undefined,
      clientManager: managerFilter !== MANAGER_FILTER_ALL ? managerFilter : undefined,
      status: currentFilters.status !== "all" ? currentFilters.status : undefined,
      overdueRange: currentFilters.overdueRange || undefined,
      createdFrom: currentFilters.createdAtRange.from || undefined,
      createdTo: currentFilters.createdAtRange.to || undefined,
      paymentFrom: currentFilters.paymentDateRange.from || undefined,
      paymentTo: currentFilters.paymentDateRange.to || undefined,
      writtenOffFrom: currentFilters.writtenOffDateRange.from || undefined,
      writtenOffTo: currentFilters.writtenOffDateRange.to || undefined,
      fullyPaidFrom: currentFilters.fullyPaidDateRange.from || undefined,
      fullyPaidTo: currentFilters.fullyPaidDateRange.to || undefined,
    }),
    [managerFilter],
  );

  const {
    session,
    canManage,
    isLoading,
    hasMoreRecords,
    isLoadingMoreRecords,
    totalRecordsCount,
    loadError,
    records,
    visibleRecords,
    filters,
    sortState,
    overviewPeriod,
    overviewMetrics,
    closedByOptions: closedByOptionsFromLoadedRecords,
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
    loadMoreRecords,
    openCreateModal,
    openRecordModal,
    startEditRecord,
    requestCloseModal,
    cancelDiscardModalClose,
    discardDraftAndCloseModal,
    updateDraftField,
    saveDraft,
    deleteActiveRecord,
    retrySave,
  } = useClientPayments({
    enabled: !isFilterOptionsLoading,
    pagination: {
      enabled: true,
      pageSize: 120,
    },
    buildClientsApiQuery,
  });

  const canRefreshClientManagerInCard = canRefreshClientManagerFromGhlSession(session);
  const canRefreshClientPhoneInCard = canRefreshClientPhoneFromGhlSession(session);
  const canConfirmPendingQuickBooksPayments = canConfirmQuickBooksPaymentsSession(session);
  const canManageRefreshActions = canRefreshClientManagerInCard;

  const isPageLoading = isFilterOptionsLoading || isLoading;
  const isViewMode = modalState.mode === "view";
  const canDeleteActiveRecord = !isViewMode && modalState.mode !== "create" && canDeleteClientSession(session);
  const [searchInputValue, setSearchInputValue] = useState("");

  useEffect(() => {
    setSearchInputValue(filters.search);
  }, [filters.search]);

  useEffect(() => {
    const timerId = window.setTimeout(() => {
      if (searchInputValue !== filters.search) {
        updateFilter("search", searchInputValue);
      }
    }, 250);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [filters.search, searchInputValue, updateFilter]);

  const visibleTableColumns = useMemo<Array<keyof ClientRecord>>(
    () =>
      showAllPayments
        ? TABLE_COLUMNS
        : TABLE_COLUMNS.filter((column) => !EXTENDED_PAYMENT_COLUMN_SET.has(column)),
    [showAllPayments],
  );

  const tableColumnKeys = useMemo<Array<keyof ClientRecord | "score" | "clientManager">>(
    () => [
      "clientName",
      "clientManager",
      "closedBy",
      "score",
      ...visibleTableColumns.filter((column) => column !== "clientName" && column !== "closedBy"),
    ],
    [visibleTableColumns],
  );

  const sortableColumns = useMemo(
    () =>
      new Set<keyof ClientRecord>(
        visibleTableColumns.filter(
          (column) => column !== "afterResult" && column !== "writtenOff" && !String(column).includes("Date"),
        ),
      ),
    [visibleTableColumns],
  );

  const fallbackManagerFilterOptions = useMemo<string[]>(() => {
    const items = new Map<string, string>();
    let hasNoManager = false;

    for (const record of visibleRecords) {
      const managerNames = resolveClientManagerNamesFromRecord(record);
      for (const managerName of managerNames) {
        if (managerName === NO_MANAGER_LABEL) {
          hasNoManager = true;
          continue;
        }
        const comparable = normalizeComparableText(managerName);
        if (!comparable || items.has(comparable)) {
          continue;
        }
        items.set(comparable, managerName);
      }
    }

    const sorted = [...items.values()].sort((left, right) => left.localeCompare(right, "en", { sensitivity: "base" }));
    return hasNoManager ? [NO_MANAGER_LABEL, ...sorted] : sorted;
  }, [visibleRecords]);

  const managerFilterOptions = useMemo<string[]>(
    () =>
      serverFilterOptions.clientManagerOptions.length
        ? serverFilterOptions.clientManagerOptions
        : fallbackManagerFilterOptions,
    [fallbackManagerFilterOptions, serverFilterOptions.clientManagerOptions],
  );

  const closedByOptions = useMemo<string[]>(
    () => (serverFilterOptions.closedByOptions.length ? serverFilterOptions.closedByOptions : closedByOptionsFromLoadedRecords),
    [closedByOptionsFromLoadedRecords, serverFilterOptions.closedByOptions],
  );

  useEffect(() => {
    if (managerFilter === MANAGER_FILTER_ALL) {
      return;
    }
    if (managerFilterOptions.includes(managerFilter)) {
      return;
    }
    setManagerFilter(MANAGER_FILTER_ALL);
  }, [managerFilter, managerFilterOptions]);

  useEffect(() => {
    let cancelled = false;

    async function loadFilterOptions() {
      setIsFilterOptionsLoading(true);
      setFilterOptionsError("");
      try {
        const payload = await getClientFilterOptions();
        if (cancelled) {
          return;
        }
        setServerFilterOptions({
          closedByOptions: payload.closedByOptions,
          clientManagerOptions: payload.clientManagerOptions,
        });
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Не удалось загрузить фильтры.";
        setFilterOptionsError(message);
      } finally {
        if (!cancelled) {
          setIsFilterOptionsLoading(false);
        }
      }
    }

    void loadFilterOptions();

    return () => {
      cancelled = true;
    };
  }, []);

  const scoredRows = useMemo<ScoredRow[]>(() => {
    return visibleRecords.map((record) => {
      const score = evaluateClientScore(record);
      return {
        record,
        scoreValue: score.displayScore,
        scoreTone: score.tone,
        clientManagerLabel: resolveClientManagerNamesFromRecord(record).join(", "),
      };
    });
  }, [visibleRecords]);

  const filteredRecords = useMemo(() => scoredRows.map((row) => row.record), [scoredRows]);

  const visibleMoneyColumns = useMemo<string[]>(() => {
    const unique = new Set<string>();
    for (const column of tableColumnKeys) {
      if (column === "score" || column === "clientManager") {
        continue;
      }
      if (!isMoneyFieldKey(column)) {
        continue;
      }
      unique.add(column);
    }
    return [...unique];
  }, [tableColumnKeys]);

  const loadedTotalsByVisibleColumns = useMemo(
    () => calculateTotalsByFieldCents(filteredRecords, visibleMoneyColumns),
    [filteredRecords, visibleMoneyColumns],
  );

  const loadedDefaultTotals = useMemo(
    () => calculateTotalsByFieldCents(filteredRecords, getDefaultTotalsKeys()),
    [filteredRecords],
  );

  const totalsQuery = useMemo(() => buildClientsApiQuery(filters), [buildClientsApiQuery, filters]);
  const totalsQueryKey = useMemo(() => JSON.stringify(Object.entries(totalsQuery).sort(([a], [b]) => a.localeCompare(b))), [totalsQuery]);

  useEffect(() => {
    let cancelled = false;

    async function loadTotals() {
      setServerTotals((prev) => ({ ...prev, loading: true, error: "" }));
      try {
        const payload = await getClientsTotals(totalsQuery);
        if (cancelled) {
          return;
        }
        setServerTotals({
          loading: false,
          error: "",
          totalsCents: payload.totalsCents,
          rowCount: payload.rowCount,
          invalidFieldsCount: payload.invalidFieldsCount,
          source: "server",
        });
      } catch (error) {
        if (cancelled) {
          return;
        }
        const message = error instanceof Error ? error.message : "Не удалось загрузить server totals.";
        setServerTotals({
          loading: false,
          error: message,
          totalsCents: {
            contractTotals: loadedDefaultTotals.totalsCents.contractTotals || 0,
            totalPayments: loadedDefaultTotals.totalsCents.totalPayments || 0,
            futurePayments: loadedDefaultTotals.totalsCents.futurePayments || 0,
            collection: loadedDefaultTotals.totalsCents.collection || 0,
          },
          rowCount: filteredRecords.length,
          invalidFieldsCount: loadedDefaultTotals.invalidFieldsCount,
          source: "loaded",
        });
      }
    }

    void loadTotals();

    return () => {
      cancelled = true;
    };
  }, [filteredRecords.length, loadedDefaultTotals, totalsQuery, totalsQueryKey]);

  const activeRecordClientManagerLabel = useMemo(() => {
    if (!activeRecord) {
      return "";
    }
    return resolveClientManagerNamesFromRecord(activeRecord).join(", ");
  }, [activeRecord]);

  const filteredClientNamesForPhoneRefresh = useMemo<string[]>(() => {
    const uniqueByComparable = new Map<string, string>();

    for (const record of filteredRecords) {
      const clientName = (record?.clientName || "").toString().trim();
      if (!clientName) {
        continue;
      }
      const comparable = normalizeComparableText(clientName);
      if (!comparable || uniqueByComparable.has(comparable)) {
        continue;
      }
      uniqueByComparable.set(comparable, clientName);
    }

    return [...uniqueByComparable.values()];
  }, [filteredRecords]);

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

  const chips = useMemo<ActiveFilterChip[]>(() => {
    const next: ActiveFilterChip[] = [];
    if (filters.search.trim()) {
      next.push({
        id: "search",
        label: `Поиск: ${filters.search.trim()}`,
        onRemove: () => updateFilter("search", ""),
      });
    }

    if (filters.closedBy.trim()) {
      next.push({
        id: "closedBy",
        label: `Закрыл: ${filters.closedBy}`,
        onRemove: () => updateFilter("closedBy", ""),
      });
    }

    if (managerFilter !== MANAGER_FILTER_ALL) {
      next.push({
        id: "manager",
        label: `Менеджер: ${managerFilter}`,
        onRemove: () => setManagerFilter(MANAGER_FILTER_ALL),
      });
    }

    if (filters.status !== STATUS_FILTER_ALL) {
      const statusLabel = STATUS_OPTIONS_RU.find((item) => item.key === filters.status)?.label || filters.status;
      next.push({
        id: "status",
        label: `Статус: ${statusLabel}`,
        onRemove: () => {
          updateFilter("status", STATUS_FILTER_ALL);
          updateFilter("overdueRange", "");
        },
      });
    }

    if (filters.overdueRange) {
      next.push({
        id: "overdueRange",
        label: `Просрочка: ${filters.overdueRange}`,
        onRemove: () => updateFilter("overdueRange", ""),
      });
    }

    addDateRangeChip(next, "created", "Период клиентов", filters.createdAtRange, () => {
      setDateRange("createdAtRange", "from", "");
      setDateRange("createdAtRange", "to", "");
    });
    addDateRangeChip(next, "payment", "Платежи", filters.paymentDateRange, () => {
      setDateRange("paymentDateRange", "from", "");
      setDateRange("paymentDateRange", "to", "");
    });
    addDateRangeChip(next, "writtenOff", "Дата списания", filters.writtenOffDateRange, () => {
      setDateRange("writtenOffDateRange", "from", "");
      setDateRange("writtenOffDateRange", "to", "");
    });
    addDateRangeChip(next, "fullyPaid", "Дата полной оплаты", filters.fullyPaidDateRange, () => {
      setDateRange("fullyPaidDateRange", "from", "");
      setDateRange("fullyPaidDateRange", "to", "");
    });

    return next;
  }, [filters, managerFilter, setDateRange, updateFilter]);

  const clearAllFilters = useCallback(() => {
    updateFilter("search", "");
    updateFilter("status", STATUS_FILTER_ALL);
    updateFilter("overdueRange", "");
    updateFilter("closedBy", "");
    setDateRange("createdAtRange", "from", "");
    setDateRange("createdAtRange", "to", "");
    setDateRange("paymentDateRange", "from", "");
    setDateRange("paymentDateRange", "to", "");
    setDateRange("writtenOffDateRange", "from", "");
    setDateRange("writtenOffDateRange", "to", "");
    setDateRange("fullyPaidDateRange", "from", "");
    setDateRange("fullyPaidDateRange", "to", "");
    setManagerFilter(MANAGER_FILTER_ALL);
  }, [setDateRange, updateFilter]);

  const trackClientPayments3Event = useCallback((payload: LocalAnalyticsEvent) => {
    if (typeof window === "undefined") {
      return;
    }
    window.dispatchEvent(
      new CustomEvent("client-payments-3:telemetry", {
        detail: {
          ...payload,
          at: new Date().toISOString(),
        },
      }),
    );
  }, []);

  const markFirstAction = useCallback(
    (actionName: string) => {
      if (firstActionTrackedRef.current) {
        return;
      }
      firstActionTrackedRef.current = true;
      const now = typeof performance !== "undefined" ? performance.now() : Date.now();
      trackClientPayments3Event({
        event: "time_to_first_action",
        meta: {
          action: actionName,
          elapsedMs: Math.max(0, Math.round(now - mountedAtRef.current)),
        },
      });
    },
    [trackClientPayments3Event],
  );

  const refreshSingleClientManager = useCallback(
    async (clientName: string) => {
      const clientNameDisplay = (clientName || "").toString().trim();
      const comparableClientName = normalizeComparableText(clientNameDisplay);
      if (!comparableClientName) {
        throw new Error("Client name is required.");
      }
      if (!canRefreshClientManagerInCard) {
        throw new Error("Only owner/admin/client-service department head can refresh Client Manager.");
      }

      setRefreshingCardClientManagerKey(comparableClientName);
      try {
        await getClientManagers("full", {
          clientName: clientNameDisplay,
        });
        await forceRefresh();
        showToast({
          type: "success",
          message: "Client Manager обновлен и сохранен в базу.",
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to refresh Client Manager.";
        showToast({
          type: "error",
          message,
        });
        throw new Error(message);
      } finally {
        setRefreshingCardClientManagerKey("");
      }
    },
    [canRefreshClientManagerInCard, forceRefresh],
  );

  const refreshSingleClientPhone = useCallback(
    async (clientName: string) => {
      const clientNameDisplay = (clientName || "").toString().trim();
      const comparableClientName = normalizeComparableText(clientNameDisplay);
      if (!comparableClientName) {
        throw new Error("Client name is required.");
      }
      if (!canRefreshClientPhoneInCard) {
        throw new Error("Only owner/admin/client-service department head can refresh Phone.");
      }
      if (!activeRecord || normalizeComparableText(activeRecord.clientName) !== comparableClientName) {
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
          message: `Телефон обновлен: ${nextPhone}`,
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

  const refreshManager = useCallback(async () => {
    if (!canManageRefreshActions) {
      showToast({ type: "error", message: "Недостаточно прав для refresh manager." });
      return;
    }

    markFirstAction("refresh_manager_incremental");
    trackClientPayments3Event({ event: "refresh_manager_click", meta: { mode: "incremental" } });
    setIsManagersLoading(true);
    try {
      const payload = await startClientManagersRefreshBackgroundJob({
        activeOnly: true,
        noManagerOnly: true,
      });
      const totalClientsRaw = Number(payload?.job?.totalClients);
      const totalClients = Number.isFinite(totalClientsRaw) && totalClientsRaw > 0 ? totalClientsRaw : null;
      const detail = totalClients === null ? "" : ` (${totalClients} клиентов)`;
      const message =
        payload?.reused === true
          ? "Refresh Manager уже выполняется в фоне."
          : `Refresh Manager запущен в фоне${detail}.`;
      setManagersStatusNote(message);
      showToast({ type: "success", message });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось запустить Refresh Manager.";
      showToast({ type: "error", message });
    } finally {
      setIsManagersLoading(false);
    }
  }, [canManageRefreshActions, markFirstAction, trackClientPayments3Event]);

  const totalRefreshManager = useCallback(async () => {
    if (!canManageRefreshActions) {
      showToast({ type: "error", message: "Недостаточно прав для total refresh." });
      return;
    }

    markFirstAction("refresh_manager_total");
    trackClientPayments3Event({ event: "refresh_manager_click", meta: { mode: "full" } });
    setIsManagersLoading(true);
    try {
      const payload = await startClientManagersRefreshBackgroundJob({
        activeOnly: true,
        noManagerOnly: false,
      });
      const totalClientsRaw = Number(payload?.job?.totalClients);
      const totalClients = Number.isFinite(totalClientsRaw) && totalClientsRaw > 0 ? totalClientsRaw : null;
      const detail = totalClients === null ? "" : ` (${totalClients} клиентов)`;
      const message = payload?.reused === true ? "Total Refresh Manager уже выполняется в фоне." : `Total Refresh Manager запущен в фоне${detail}.`;
      setManagersStatusNote(message);
      showToast({ type: "success", message });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось запустить Total Refresh Manager.";
      showToast({ type: "error", message });
    } finally {
      setIsManagersLoading(false);
    }
  }, [canManageRefreshActions, markFirstAction, trackClientPayments3Event]);

  const refreshFilteredClientPhones = useCallback(async () => {
    if (!canRefreshClientPhoneInCard) {
      showToast({ type: "error", message: "Недостаточно прав для обновления телефонов." });
      return;
    }

    if (!filteredClientNamesForPhoneRefresh.length) {
      showToast({ type: "info", message: "Нет клиентов в текущем фильтре." });
      return;
    }

    markFirstAction("refresh_phones_filtered");
    trackClientPayments3Event({ event: "refresh_phones_click", meta: { filteredCount: filteredClientNamesForPhoneRefresh.length } });

    setIsPhonesRefreshLoading(true);
    try {
      let refreshedClientsCount = 0;
      let notFoundClientsCount = 0;
      let failedClientsCount = 0;
      for (const clientName of filteredClientNamesForPhoneRefresh) {
        try {
          const payload = await postGhlClientPhoneRefresh(clientName);
          const nextPhone = (payload?.phone || "").toString().trim();
          if (payload?.status === "found" && nextPhone) {
            refreshedClientsCount += 1;
          } else {
            notFoundClientsCount += 1;
          }
        } catch {
          failedClientsCount += 1;
        }
      }

      await forceRefresh();
      const summary = `Phones refresh завершен. Обновлено: ${refreshedClientsCount}. Не найдено: ${notFoundClientsCount}. Ошибок: ${failedClientsCount}.`;
      showToast({ type: failedClientsCount > 0 ? "info" : "success", message: summary });
      setManagersStatusNote(summary);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Не удалось обновить телефоны.";
      showToast({ type: "error", message });
    } finally {
      setIsPhonesRefreshLoading(false);
    }
  }, [canRefreshClientPhoneInCard, filteredClientNamesForPhoneRefresh, forceRefresh, markFirstAction, trackClientPayments3Event]);

  const handleRefreshData = useCallback(() => {
    markFirstAction("refresh_data");
    trackClientPayments3Event({ event: "refresh_data_click" });
    void forceRefresh();
  }, [forceRefresh, markFirstAction, trackClientPayments3Event]);

  const handleExportXls = useCallback(() => {
    markFirstAction("export_xls");
    trackClientPayments3Event({ event: "export_click", meta: { format: "xlsx", rows: filteredRecords.length } });
    exportRecordsToXls(filteredRecords);
  }, [filteredRecords, markFirstAction, trackClientPayments3Event]);

  const handleExportPdf = useCallback(() => {
    markFirstAction("export_pdf");
    trackClientPayments3Event({ event: "export_click", meta: { format: "pdf", rows: filteredRecords.length } });
    exportRecordsToPdf(filteredRecords);
  }, [filteredRecords, markFirstAction, trackClientPayments3Event]);

  const handleDeleteActiveRecord = useCallback(async () => {
    if (!activeRecord || !canDeleteActiveRecord) {
      return;
    }

    const safeClientName = (activeRecord.clientName || "").trim() || "этого клиента";
    const shouldDelete = window.confirm(`Удалить "${safeClientName}"? Yes/No`);
    if (!shouldDelete) {
      return;
    }

    const result = await deleteActiveRecord();
    if (result.ok) {
      showToast({ type: "success", message: "Клиент удален." });
      return;
    }

    showToast({ type: "error", message: result.error || "Не удалось удалить клиента." });
  }, [activeRecord, canDeleteActiveRecord, deleteActiveRecord]);

  useEffect(() => {
    if (!loadError) {
      return;
    }
    trackClientPayments3Event({ event: "load_error", meta: { message: loadError } });
  }, [loadError, trackClientPayments3Event]);

  useEffect(() => {
    if (!serverTotals.error) {
      return;
    }
    trackClientPayments3Event({ event: "totals_error", meta: { message: serverTotals.error } });
  }, [serverTotals.error, trackClientPayments3Event]);

  const tableColumns = useMemo<TableColumn<ScoredRow>[]>(() => {
    return tableColumnKeys.map((column) => {
      const columnKeyText = String(column);
      const isScoreColumn = columnKeyText === "score";
      const isClientManagerColumn = columnKeyText === "clientManager";
      const sortableColumn = column as keyof ClientRecord;
      const isSortable = !isScoreColumn && !isClientManagerColumn && sortableColumns.has(sortableColumn);
      const isActiveSort = !isScoreColumn && !isClientManagerColumn && sortState.key === sortableColumn;
      const headerLabel = resolveColumnLabel(column);

      return {
        key: String(column),
        label: isSortable ? (
          <button
            type="button"
            className="th-sort-btn cp3-focusable"
            onClick={() => {
              if (isSortable) {
                toggleSort(sortableColumn);
              }
            }}
          >
            <span className="th-sort-label">{headerLabel}</span>
            {isActiveSort ? <span>{sortState.direction === "asc" ? "↑" : "↓"}</span> : null}
          </button>
        ) : (
          <span className="th-sort-label">{headerLabel}</span>
        ),
        align: getColumnAlign(column),
        headerClassName: column === "clientName" ? "cp3-col-pinned" : undefined,
        className: column === "clientName" ? "cp3-col-pinned" : undefined,
        cell: (row) => renderCellValue(row, column),
      };
    });
  }, [sortState.direction, sortState.key, sortableColumns, tableColumnKeys, toggleSort]);

  const totalsSourceLabel = serverTotals.source === "server" ? "сервер (вся отфильтрованная выборка)" : "загруженные строки (fallback)";

  const periodLabel = OVERVIEW_PERIOD_OPTIONS_RU.find((item) => item.key === overviewPeriod)?.label || "Текущая неделя";
  const salesCents = Math.round(Number(overviewMetrics.sales || 0) * 100);
  const receivedCents = Math.round(Number(overviewMetrics.received || 0) * 100);
  const debtCents = Math.round(Number(overviewMetrics.debt || 0) * 100);

  return (
    <PageShell className="cp3-page">
      <PageHeader
        title="Client Payment 3"
        subtitle="Операционный дашборд: фильтры, таблица, totals и действия"
        actions={
          <Button size="sm" onClick={openCreateModal} disabled={!canManage}>
            Добавить клиента
          </Button>
        }
        meta={
          <div className="client-payments-page-header-meta">
            <span>Клиентов: {counters.totalCount}</span>
            <span>Filtered: {counters.filteredCount}</span>
            <span>Written Off: {counters.writtenOffCount}</span>
            <span>Fully Paid: {counters.fullyPaidCount}</span>
            <span>Overdue: {counters.overdueCount}</span>
          </div>
        }
      />

      <MetricCards
        periodLabel={periodLabel}
        period={overviewPeriod}
        periodOptions={OVERVIEW_PERIOD_OPTIONS_RU}
        salesCents={salesCents}
        receivedCents={receivedCents}
        debtCents={debtCents}
        totalsSourceLabel={serverTotals.loading ? "загрузка..." : totalsSourceLabel}
        totalsInvalidFieldsCount={serverTotals.invalidFieldsCount}
        totalsRowsCount={serverTotals.rowCount}
        onPeriodChange={setOverviewPeriod}
      />

      <ActiveFiltersBar chips={chips} onClearAll={clearAllFilters} />

      <div className="cp3-layout">
        <aside className={`cp3-filters ${filtersCollapsed ? "cp3-filters--collapsed" : ""}`.trim()}>
          <header className="cp3-filters__header">
            <h3 className="cp3-filters__title">Фильтры</h3>
            <Button
              variant="ghost"
              size="sm"
              type="button"
              onClick={() => setFiltersCollapsed(!filtersCollapsed)}
              aria-expanded={!filtersCollapsed}
              aria-label={filtersCollapsed ? "Развернуть фильтры" : "Свернуть фильтры"}
            >
              {filtersCollapsed ? "Развернуть" : "Свернуть"}
            </Button>
          </header>

          {!filtersCollapsed ? (
            <>
              <section className="cp3-filter-group" aria-label="Поиск">
                <p className="cp3-filter-group__title">Поиск</p>
                <Input
                  value={searchInputValue}
                  onChange={(event) => setSearchInputValue(event.target.value)}
                  placeholder="Поиск по имени, email, телефону или SSN"
                  autoComplete="off"
                  aria-label="Поиск по имени, email, телефону или SSN"
                />
              </section>

              <section className="cp3-filter-group" aria-label="Периоды">
                <p className="cp3-filter-group__title">Периоды</p>
                <div className="cp3-filter-row">
                  <label className="cp3-filter-field" htmlFor="cp3-created-from">
                    <span>Новый клиент от</span>
                    <DateInput
                      id="cp3-created-from"
                      value={filters.createdAtRange.from}
                      onChange={(value) => setDateRange("createdAtRange", "from", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                  <label className="cp3-filter-field" htmlFor="cp3-created-to">
                    <span>до</span>
                    <DateInput
                      id="cp3-created-to"
                      value={filters.createdAtRange.to}
                      onChange={(value) => setDateRange("createdAtRange", "to", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                </div>
                <div className="cp3-filter-row">
                  <label className="cp3-filter-field" htmlFor="cp3-payment-from">
                    <span>Платежи от</span>
                    <DateInput
                      id="cp3-payment-from"
                      value={filters.paymentDateRange.from}
                      onChange={(value) => setDateRange("paymentDateRange", "from", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                  <label className="cp3-filter-field" htmlFor="cp3-payment-to">
                    <span>до</span>
                    <DateInput
                      id="cp3-payment-to"
                      value={filters.paymentDateRange.to}
                      onChange={(value) => setDateRange("paymentDateRange", "to", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                </div>
              </section>

              <section className="cp3-filter-group" aria-label="Ответственные">
                <p className="cp3-filter-group__title">Ответственные</p>
                <label className="cp3-filter-field" htmlFor="cp3-closed-by">
                  <span>Closed By (Sales Manager)</span>
                  <Select
                    id="cp3-closed-by"
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
                </label>

                <label className="cp3-filter-field" htmlFor="cp3-client-manager">
                  <span>Client Manager</span>
                  <Select id="cp3-client-manager" value={managerFilter} onChange={(event) => setManagerFilter(event.target.value)}>
                    <option value={MANAGER_FILTER_ALL}>All</option>
                    {managerFilterOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </Select>
                </label>
              </section>

              <section className="cp3-filter-group" aria-label="Статусы">
                <p className="cp3-filter-group__title">Статусы</p>
                <label className="cp3-filter-field" htmlFor="cp3-status-filter">
                  <span>Статус</span>
                  <Select
                    id="cp3-status-filter"
                    value={filters.status}
                    onChange={(event) => updateFilter("status", event.target.value as ClientPaymentsFilters["status"])}
                  >
                    {STATUS_OPTIONS_RU.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.label}
                      </option>
                    ))}
                  </Select>
                </label>

                {filters.status === "overdue" ? (
                  <label className="cp3-filter-field" htmlFor="cp3-overdue-range">
                    <span>Диапазон просрочки</span>
                    <Select
                      id="cp3-overdue-range"
                      value={filters.overdueRange}
                      onChange={(event) => updateFilter("overdueRange", event.target.value as ClientPaymentsFilters["overdueRange"])}
                    >
                      {OVERDUE_RANGE_OPTIONS.map((option) => (
                        <option key={option.key || "all"} value={option.key}>
                          {option.label}
                        </option>
                      ))}
                    </Select>
                  </label>
                ) : null}

                <div className="cp3-filter-row">
                  <label className="cp3-filter-field" htmlFor="cp3-written-off-from">
                    <span>Written Off от</span>
                    <DateInput
                      id="cp3-written-off-from"
                      value={filters.writtenOffDateRange.from}
                      onChange={(value) => setDateRange("writtenOffDateRange", "from", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                  <label className="cp3-filter-field" htmlFor="cp3-written-off-to">
                    <span>до</span>
                    <DateInput
                      id="cp3-written-off-to"
                      value={filters.writtenOffDateRange.to}
                      onChange={(value) => setDateRange("writtenOffDateRange", "to", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                </div>

                <div className="cp3-filter-row">
                  <label className="cp3-filter-field" htmlFor="cp3-fully-paid-from">
                    <span>Fully Paid от</span>
                    <DateInput
                      id="cp3-fully-paid-from"
                      value={filters.fullyPaidDateRange.from}
                      onChange={(value) => setDateRange("fullyPaidDateRange", "from", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                  <label className="cp3-filter-field" htmlFor="cp3-fully-paid-to">
                    <span>до</span>
                    <DateInput
                      id="cp3-fully-paid-to"
                      value={filters.fullyPaidDateRange.to}
                      onChange={(value) => setDateRange("fullyPaidDateRange", "to", value)}
                      placeholder="MM/DD/YYYY"
                    />
                  </label>
                </div>
              </section>

              <Button type="button" variant="ghost" size="sm" onClick={clearAllFilters}>
                Сбросить фильтры
              </Button>
            </>
          ) : null}

          {filterOptionsError ? <p className="cp3-status cp3-status--warning">{filterOptionsError}</p> : null}
        </aside>

        <section className="cp3-main">
          <ToolbarMenu
            isRefreshing={isLoading}
            canManageRefreshActions={canManageRefreshActions}
            isManagerRefreshLoading={isManagersLoading}
            isPhonesRefreshLoading={isPhonesRefreshLoading}
            showAllPayments={showAllPayments}
            density={density}
            lastSyncedLabel={lastSyncedAt}
            onRefreshData={handleRefreshData}
            onExportXls={handleExportXls}
            onExportPdf={handleExportPdf}
            onToggleShowAllPayments={() => setShowAllPayments((prev) => !prev)}
            onRefreshManager={refreshManager}
            onTotalRefreshManager={totalRefreshManager}
            onRefreshPhones={refreshFilteredClientPhones}
            onDensityChange={setDensity}
          />

          <p className="cp3-status">{managersStatusNote}</p>
          {saveError ? (
            <p className="cp3-status cp3-status--error">
              Ошибка сохранения: {saveError}
              {saveRetryGiveUp ? " (Автоповторы остановлены)" : ""}
            </p>
          ) : null}
          {saveSuccessNotice ? <p className="cp3-status">{saveSuccessNotice}</p> : null}
          {hasUnsavedChanges ? <p className="cp3-status">Есть несохраненные изменения...</p> : null}
          {saveRetryCount > 0 && !saveRetryGiveUp ? (
            <p className="cp3-status">Автоповтор: {saveRetryCount} / {saveRetryMax}</p>
          ) : null}
          {serverTotals.error ? <p className="cp3-status cp3-status--warning">Totals fallback: {serverTotals.error}</p> : null}

          <article className="cp3-table-section" aria-label="Таблица клиентов">
            <header className="cp3-table-header">
              <h3 className="cp3-table-header__title">Таблица клиентов</h3>
              <p className="cp3-table-header__meta">
                Загружено {records.length}
                {typeof totalRecordsCount === "number" && totalRecordsCount > 0 ? ` из ${totalRecordsCount}` : ""}
                {isLoadingMoreRecords ? " • подгружаем..." : ""}
              </p>
            </header>

            <div className="cp3-table-surface">
              {isPageLoading ? <GridSkeleton columnCount={tableColumnKeys.length} /> : null}

              {!isPageLoading && loadError ? (
                <div className="cp3-error">
                  <ErrorState
                    title="Не удалось загрузить данные"
                    description={loadError}
                    actionLabel="Повторить"
                    onAction={() => void forceRefresh()}
                  />
                </div>
              ) : null}

              {!isPageLoading && !loadError && !scoredRows.length ? (
                <div className="cp3-empty">
                  <EmptyState
                    title="Нет записей по выбранным фильтрам"
                    description="Измените фильтры или добавьте нового клиента."
                  />
                </div>
              ) : null}

              {!isPageLoading && !loadError && scoredRows.length ? (
                <Table
                  className="cp3-table-wrap"
                  tableClassName={density === "regular" ? "cb-table--cp3-regular" : ""}
                  columns={tableColumns}
                  rows={scoredRows}
                  rowKey={(row) => row.record.id}
                  density="compact"
                  virtualizeRows
                  virtualRowHeight={density === "regular" ? 56 : 48}
                  virtualOverscan={10}
                  virtualThreshold={80}
                  onScrollNearEnd={() => {
                    if (hasMoreRecords && !isLoadingMoreRecords) {
                      void loadMoreRecords();
                    }
                  }}
                  scrollNearEndOffset={300}
                  onRowActivate={(row) => openRecordModal(row.record)}
                  footer={
                    <tr>
                      {tableColumnKeys.map((column) => {
                        if (column === "clientName") {
                          return (
                            <td key={column} className="cp3-col-pinned">
                              <strong>Итого (по загруженным)</strong>
                            </td>
                          );
                        }

                        if (column === "score") {
                          return <td key={column}>—</td>;
                        }

                        if (column === "clientManager") {
                          return <td key={column}>—</td>;
                        }

                        if (!isMoneyFieldKey(column)) {
                          if (column === "closedBy") {
                            return <td key={column}>{`${filteredRecords.length} клиентов`}</td>;
                          }
                          return <td key={column}>—</td>;
                        }

                        const cents = loadedTotalsByVisibleColumns.totalsCents[column] ?? 0;
                        const hasInvalid = loadedTotalsByVisibleColumns.invalidFieldsCount > 0;
                        return (
                          <td key={column} className="cb-table__cell--align-right">
                            <span
                              className={`cp3-money ${hasInvalid ? "cp3-money--invalid" : ""}`.trim()}
                              title={
                                hasInvalid
                                  ? "Некоторые значения в выборке некорректны. Показана сумма только по валидным данным."
                                  : undefined
                              }
                            >
                              {formatMoneyFromCents(cents)}
                            </span>
                          </td>
                        );
                      })}
                    </tr>
                  }
                />
              ) : null}
            </div>
          </article>
        </section>
      </div>

      <Modal
        open={modalState.open}
        title={
          modalState.mode === "create"
            ? "Создать клиента"
            : modalState.mode === "edit"
              ? "Редактирование клиента"
              : activeRecord?.clientName || "Карточка клиента"
        }
        onClose={requestCloseModal}
        footer={
          <div className="client-payments__modal-actions">
            <Button variant="secondary" size="sm" onClick={requestCloseModal}>
              Закрыть
            </Button>
            {isViewMode ? (
              <Button size="sm" onClick={startEditRecord}>
                Редактировать
              </Button>
            ) : null}
            {canDeleteActiveRecord ? (
              <Button type="button" variant="danger" size="sm" onClick={() => void handleDeleteActiveRecord()}>
                Delete
              </Button>
            ) : null}
            {!isViewMode ? (
              <Button size="sm" onClick={saveDraft} isLoading={isSaving}>
                Сохранить
              </Button>
            ) : null}
            {saveError ? (
              <Button type="button" variant="secondary" size="sm" onClick={retrySave}>
                Повторить сохранение
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
            isRefreshingClientManager={refreshingCardClientManagerKey === normalizeComparableText(activeRecord.clientName)}
            onRefreshClientManager={refreshSingleClientManager}
            canRefreshClientPhone={canRefreshClientPhoneInCard}
            isRefreshingClientPhone={refreshingCardClientPhoneKey === normalizeComparableText(activeRecord.clientName)}
            onRefreshClientPhone={refreshSingleClientPhone}
            canConfirmPendingQuickBooksPayments={canConfirmPendingQuickBooksPayments}
            onPaymentRowCleared={forceRefresh}
          />
        ) : null}

        {!isViewMode ? <RecordEditorForm draft={modalState.draft} onChange={updateDraftField} /> : null}
      </Modal>

      <Modal
        open={isDiscardConfirmOpen}
        title="Отменить изменения?"
        onClose={cancelDiscardModalClose}
        footer={
          <div className="client-payments__modal-actions">
            <Button type="button" variant="secondary" size="sm" onClick={cancelDiscardModalClose}>
              Назад
            </Button>
            <Button type="button" variant="danger" size="sm" onClick={discardDraftAndCloseModal}>
              Отменить изменения
            </Button>
          </div>
        }
      >
        <p>Есть несохраненные изменения. Закрыть карточку без сохранения?</p>
      </Modal>
    </PageShell>
  );
}

function resolveColumnLabel(column: keyof ClientRecord | "score" | "clientManager"): string {
  if (column in COLUMN_LABELS_RU) {
    return COLUMN_LABELS_RU[column as string] || String(column);
  }

  const match = String(column).match(PAYMENT_COLUMN_MATCH);
  if (match) {
    const paymentIndex = match[1];
    const isDate = Boolean(match[2]);
    return isDate ? `Платеж ${paymentIndex} дата` : `Платеж ${paymentIndex}`;
  }

  return String(column);
}

function getColumnAlign(column: keyof ClientRecord | "score" | "clientManager"): TableAlign {
  if (column === "score") {
    return "center";
  }

  if (column === "clientName" || column === "clientManager" || column === "closedBy") {
    return "left";
  }

  if (isMoneyFieldKey(column)) {
    return "right";
  }

  if (String(column).includes("Date") || column === "afterResult" || column === "writtenOff") {
    return "center";
  }

  return "left";
}

function renderCellValue(row: ScoredRow, column: keyof ClientRecord | "score" | "clientManager") {
  const record = row.record;

  if (column === "score") {
    return row.scoreValue === null ? "—" : <Badge tone={row.scoreTone}>{row.scoreValue}</Badge>;
  }

  if (column === "clientManager") {
    return row.clientManagerLabel || "—";
  }

  if (column === "clientName") {
    return (
      <div className="cp3-client-cell">
        <strong className="cp3-client-cell__name">{record.clientName || "Unnamed"}</strong>
        <span className="cp3-client-cell__sub">{record.companyName || "—"}</span>
        <StatusBadges record={record} />
      </div>
    );
  }

  if (isMoneyFieldKey(column)) {
    const rawValue = record[column];
    const cents = parseMoneyToCents(rawValue);
    const normalizedText = String(rawValue || "").trim();
    if (!normalizedText) {
      return "—";
    }
    if (cents === null) {
      return (
        <span className="cp3-money cp3-money--invalid" title="Данные некорректны">
          —
        </span>
      );
    }
    return <span className="cp3-money">{formatMoneyFromCents(cents)}</span>;
  }

  const rawValue = record[column] || "";
  if (String(column).includes("Date") || column === "dateOfCollection" || column === "dateWhenWrittenOff") {
    return formatDate(rawValue);
  }

  return rawValue || "—";
}

function normalizeComparableText(rawValue: unknown): string {
  return String(rawValue || "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function resolveClientManagerNamesFromRecord(record: ClientRecord): string[] {
  const rawValue = String(record?.clientManager || "").trim();
  if (!rawValue) {
    return [NO_MANAGER_LABEL];
  }

  const values = rawValue
    .split(/[|,;/]+/)
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  if (!values.length) {
    return [rawValue];
  }

  return [...new Set(values)];
}

function buildPaymentColumns(from: number, to: number): Array<keyof ClientRecord> {
  const columns: Array<keyof ClientRecord> = [];
  for (let index = from; index <= to; index += 1) {
    columns.push(`payment${index}` as keyof ClientRecord);
    columns.push(`payment${index}Date` as keyof ClientRecord);
  }
  return columns;
}

function isMoneyFieldKey(column: keyof ClientRecord | "score" | "clientManager"): boolean {
  if (column === "score" || column === "clientManager") {
    return false;
  }
  return (
    column === "contractTotals" ||
    column === "totalPayments" ||
    column === "futurePayments" ||
    column === "collection" ||
    PAYMENT_AMOUNT_COLUMN_MATCH.test(String(column))
  );
}

function addDateRangeChip(
  chips: ActiveFilterChip[],
  id: string,
  label: string,
  range: { from: string; to: string },
  onRemove: () => void,
): void {
  const from = String(range?.from || "").trim();
  const to = String(range?.to || "").trim();
  if (!from && !to) {
    return;
  }

  const valueLabel = `${from || "..."} - ${to || "..."}`;
  chips.push({
    id,
    label: `${label}: ${valueLabel}`,
    onRemove,
  });
}

function GridSkeleton({ columnCount }: { columnCount: number }) {
  return (
    <div role="status" aria-live="polite" aria-label="Загрузка таблицы">
      <table className="cb-table">
        <tbody>
          {Array.from({ length: 10 }).map((_, rowIndex) => (
            <tr key={`sk-${rowIndex}`}>
              {Array.from({ length: columnCount }).map((__, cellIndex) => (
                <td key={`sk-${rowIndex}-${cellIndex}`}>
                  <div className="loading-line" style={{ width: `${40 + ((rowIndex + cellIndex) % 5) * 10}%` }} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
