import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { MutableRefObject } from "react";

import { ApiError, getClients, getClientsPage, getSession, patchClients, putClients } from "@/shared/api";
import type { ClientRecord } from "@/shared/types/records";
import type { Session } from "@/shared/types/session";
import {
  calculateOverviewMetrics,
  calculateTableTotals,
  createEmptyRecord,
  filterRecords,
  formatDateTime,
  getClosedByOptions,
  normalizeDateForStorage,
  normalizeFormRecord,
  normalizeRecords,
  sortRecords,
  type ClientPaymentsFilters,
  type SortState,
} from "@/features/client-payments/domain/calculations";
import {
  FIELD_DEFINITIONS,
  REMOTE_SYNC_DEBOUNCE_MS,
  REMOTE_SYNC_MAX_RETRIES,
  REMOTE_SYNC_MAX_RETRY_DELAY_MS,
  REMOTE_SYNC_RETRY_MS,
  STATUS_FILTER_ALL,
  type OverviewPeriodKey,
} from "@/features/client-payments/domain/constants";
import {
  buildRecordsPatchOperations,
  resolveRecordsPatchEnabled,
  shouldFallbackToPutFromPatch,
} from "@/features/client-payments/domain/recordsPatch";
import {
  readClientPaymentsUiState,
  writeClientPaymentsUiState,
} from "@/shared/storage/uiState";
import {
  consumePendingOpenClientCardRequest,
  OPEN_CLIENT_CARD_EVENT_NAME,
  type OpenClientCardEventDetail,
} from "@/shared/lib/openClientCard";

interface ModalState {
  open: boolean;
  mode: "view" | "edit" | "create";
  recordId: string;
  draft: ClientRecord;
  dirty: boolean;
}

const INITIAL_FILTERS: ClientPaymentsFilters = {
  search: "",
  status: STATUS_FILTER_ALL,
  overdueRange: "",
  closedBy: "",
  createdAtRange: { from: "", to: "" },
  paymentDateRange: { from: "", to: "" },
  writtenOffDateRange: { from: "", to: "" },
  fullyPaidDateRange: { from: "", to: "" },
};

const INITIAL_MODAL_STATE: ModalState = {
  open: false,
  mode: "view",
  recordId: "",
  draft: createEmptyRecord(),
  dirty: false,
};

const normalizeRevisionTimestamp = (rawValue: string | null | undefined): string | null => {
  if (!rawValue) {
    return null;
  }

  const timestamp = Date.parse(rawValue);
  if (Number.isNaN(timestamp)) {
    return null;
  }

  return new Date(timestamp).toISOString();
};

interface UseClientPaymentsOptions {
  enabled?: boolean;
  pagination?: {
    enabled?: boolean;
    pageSize?: number;
  };
  buildClientsApiQuery?: (
    filters: ClientPaymentsFilters,
  ) => Record<string, string | number | boolean | null | undefined>;
}

export function useClientPayments(options: UseClientPaymentsOptions = {}) {
  const uiState = useMemo(() => readClientPaymentsUiState(), []);
  const paginationOptions = options.pagination;
  const buildClientsApiQuery = options.buildClientsApiQuery;
  const enabledOption = options.enabled;

  const [session, setSession] = useState<Session | null>(null);
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const hookEnabled = enabledOption !== false;
  const [isLoading, setIsLoading] = useState(hookEnabled);
  const [loadError, setLoadError] = useState("");

  const [filters, setFilters] = useState<ClientPaymentsFilters>(INITIAL_FILTERS);
  const [sortState, setSortState] = useState<SortState>({
    key: (uiState.sortKey as keyof ClientRecord) || "createdAt",
    direction: uiState.sortDirection,
  });
  const [overviewPeriod, setOverviewPeriod] = useState<OverviewPeriodKey>(
    uiState.selectedPeriod as OverviewPeriodKey,
  );
  const [filtersCollapsed, setFiltersCollapsed] = useState(uiState.filtersCollapsed);

  const [modalState, setModalState] = useState<ModalState>(INITIAL_MODAL_STATE);

  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState("");
  const [saveRetryCount, setSaveRetryCount] = useState(0);
  const [saveRetryGiveUp, setSaveRetryGiveUp] = useState(false);
  const [saveSuccessNotice, setSaveSuccessNotice] = useState("");
  const [lastSyncedAt, setLastSyncedAt] = useState("");
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [isDiscardConfirmOpen, setIsDiscardConfirmOpen] = useState(false);
  const [hasMoreRecords, setHasMoreRecords] = useState(false);
  const [isLoadingMoreRecords, setIsLoadingMoreRecords] = useState(false);
  const [totalRecordsCount, setTotalRecordsCount] = useState<number | null>(null);

  const recordsRef = useRef<ClientRecord[]>([]);
  const baselineRecordsRef = useRef<ClientRecord[]>([]);
  const baselineRef = useRef("");
  const recordsPatchEnabledRef = useRef<boolean>(resolveRecordsPatchEnabled(null));
  const initializedRef = useRef(false);
  const saveInFlightRef = useRef(false);
  const pendingSaveRef = useRef(false);
  const debounceTimerRef = useRef<number | null>(null);
  const retryTimerRef = useRef<number | null>(null);
  const retryAttemptRef = useRef(0);
  const saveSuccessTimerRef = useRef<number | null>(null);
  const lastSaveSuccessAtRef = useRef(0);
  const serverUpdatedAtRef = useRef<string | null>(null);
  const nextOffsetRef = useRef(0);

  const paginationEnabled = paginationOptions?.enabled === true;
  const pageSize = Math.max(1, Math.min(500, Math.trunc(paginationOptions?.pageSize || 100)));
  const clientsApiQuery = useMemo(() => {
    if (!paginationEnabled || typeof buildClientsApiQuery !== "function") {
      return {};
    }
    return normalizeClientsApiQuery(buildClientsApiQuery(filters));
  }, [buildClientsApiQuery, filters, paginationEnabled]);
  const clientsApiQueryKey = useMemo(() => serializeClientsApiQuery(clientsApiQuery), [clientsApiQuery]);

  const canManage = Boolean(session?.permissions?.manage_client_payments);

  const visibleRecords = useMemo(() => {
    const scoped = filterRecords(records, filters);
    return sortRecords(scoped, sortState);
  }, [filters, records, sortState]);

  const closedByOptions = useMemo(() => getClosedByOptions(records), [records]);

  const overviewMetrics = useMemo(() => calculateOverviewMetrics(records, overviewPeriod), [records, overviewPeriod]);
  const tableTotals = useMemo(() => calculateTableTotals(visibleRecords), [visibleRecords]);

  const activeRecord = useMemo(() => {
    if (!modalState.recordId) {
      return null;
    }

    return records.find((record) => record.id === modalState.recordId) || null;
  }, [modalState.recordId, records]);

  useEffect(() => {
    recordsRef.current = records;
  }, [records]);

  useEffect(() => {
    writeClientPaymentsUiState({
      filtersCollapsed,
      selectedPeriod: overviewPeriod,
      sortKey: sortState.key,
      sortDirection: sortState.direction,
    });
  }, [filtersCollapsed, overviewPeriod, sortState.direction, sortState.key]);

  const flushSave = useCallback(async () => {
    if (!initializedRef.current) {
      return;
    }

    const snapshot = recordsRef.current;
    const serialized = serializeRecords(snapshot);
    if (serialized === baselineRef.current) {
      setHasUnsavedChanges(false);
      setSaveError("");
      resetSaveRetryState(retryAttemptRef, setSaveRetryCount, setSaveRetryGiveUp);
      return;
    }

    if (saveInFlightRef.current) {
      pendingSaveRef.current = true;
      return;
    }

    saveInFlightRef.current = true;
    setIsSaving(true);

    try {
      let savePayload: { updatedAt?: string | null };
      const operations = buildRecordsPatchOperations(baselineRecordsRef.current, snapshot);
      const shouldTryPatchFirst = recordsPatchEnabledRef.current || paginationEnabled;
      if (shouldTryPatchFirst) {
        try {
          savePayload = await patchClients(operations, serverUpdatedAtRef.current);
          recordsPatchEnabledRef.current = true;
        } catch (error) {
          if (!shouldFallbackToPutFromPatch(error)) {
            throw error;
          }

          if (paginationEnabled) {
            throw new Error("PATCH API is unavailable. Saving is blocked in paged mode to prevent partial overwrite.");
          }

          recordsPatchEnabledRef.current = false;
          savePayload = await putClients(snapshot, serverUpdatedAtRef.current);
        }
      } else {
        if (paginationEnabled) {
          throw new Error("Saving is unavailable in paged mode because PATCH is disabled.");
        }
        savePayload = await putClients(snapshot, serverUpdatedAtRef.current);
      }

      baselineRef.current = serialized;
      baselineRecordsRef.current = cloneRecords(snapshot);
      setSaveError("");
      setHasUnsavedChanges(false);
      const nextUpdatedAt = normalizeRevisionTimestamp(savePayload.updatedAt) || new Date().toISOString();
      serverUpdatedAtRef.current = nextUpdatedAt;
      setLastSyncedAt(nextUpdatedAt);
      maybeShowSaveSuccess(setSaveSuccessNotice, saveSuccessTimerRef, lastSaveSuccessAtRef);
      resetSaveRetryState(retryAttemptRef, setSaveRetryCount, setSaveRetryGiveUp);
      clearRetryTimer(retryTimerRef);
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        clearRetryTimer(retryTimerRef);
        resetSaveRetryState(retryAttemptRef, setSaveRetryCount, setSaveRetryGiveUp);
        setSaveRetryGiveUp(true);
        setSaveSuccessNotice("");
        setSaveError("Save conflict: records were updated elsewhere. Click Refresh and retry.");
        return;
      }

      const message = extractErrorMessage(error, "Failed to save records.");
      setSaveSuccessNotice("");
      const nextRetryCount = retryAttemptRef.current + 1;
      if (nextRetryCount > REMOTE_SYNC_MAX_RETRIES) {
        setSaveRetryCount(REMOTE_SYNC_MAX_RETRIES);
        setSaveRetryGiveUp(true);
        setSaveError(`${message} Give up after ${REMOTE_SYNC_MAX_RETRIES} retries.`);
      } else {
        retryAttemptRef.current = nextRetryCount;
        setSaveRetryCount(nextRetryCount);
        setSaveRetryGiveUp(false);
        setSaveError(message);
        scheduleRetry(flushSave, retryTimerRef, getRetryDelayMs(nextRetryCount));
      }
    } finally {
      saveInFlightRef.current = false;
      setIsSaving(false);
      if (pendingSaveRef.current) {
        pendingSaveRef.current = false;
        scheduleDebouncedSave(flushSave, debounceTimerRef, 80);
      }
    }
  }, [paginationEnabled]);

  useEffect(() => {
    if (!initializedRef.current) {
      return;
    }

    const serialized = serializeRecords(records);
    const isDirty = serialized !== baselineRef.current;
    setHasUnsavedChanges(isDirty);

    if (isDirty) {
      setSaveSuccessNotice("");
      scheduleDebouncedSave(flushSave, debounceTimerRef, REMOTE_SYNC_DEBOUNCE_MS);
    }
  }, [flushSave, records]);

  useEffect(() => {
    function onBeforeUnload(event: BeforeUnloadEvent) {
      if (!hasUnsavedChanges) {
        return;
      }

      event.preventDefault();
      event.returnValue = "";
    }

    window.addEventListener("beforeunload", onBeforeUnload);
    return () => {
      window.removeEventListener("beforeunload", onBeforeUnload);
    };
  }, [hasUnsavedChanges]);

  useEffect(() => {
    return () => {
      clearDebounceTimer(debounceTimerRef);
      clearRetryTimer(retryTimerRef);
      clearSaveSuccessTimer(saveSuccessTimerRef);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function bootstrap() {
      if (!hookEnabled) {
        setIsLoading(false);
        return;
      }

      setIsLoading(true);
      setLoadError("");
      setHasMoreRecords(false);
      setIsLoadingMoreRecords(false);
      setTotalRecordsCount(null);
      nextOffsetRef.current = 0;

      try {
        const [sessionPayload, recordsPayload] = await Promise.all([
          getSession(),
          paginationEnabled ? getClientsPage(pageSize, 0, clientsApiQuery) : getClients(),
        ]);
        if (cancelled) {
          return;
        }

        const normalizedRecords = normalizeRecords(recordsPayload.records);
        recordsPatchEnabledRef.current = resolveRecordsPatchEnabled(sessionPayload);
        setSession(sessionPayload);
        setRecords(normalizedRecords);
        baselineRef.current = serializeRecords(normalizedRecords);
        baselineRecordsRef.current = cloneRecords(normalizedRecords);
        nextOffsetRef.current =
          typeof recordsPayload.nextOffset === "number"
            ? Math.max(0, recordsPayload.nextOffset)
            : normalizedRecords.length;
        setHasMoreRecords(Boolean(recordsPayload.hasMore));
        setTotalRecordsCount(
          typeof recordsPayload.total === "number" && Number.isFinite(recordsPayload.total)
            ? Math.max(normalizedRecords.length, recordsPayload.total)
            : null,
        );
        const nextUpdatedAt = normalizeRevisionTimestamp(recordsPayload.updatedAt);
        serverUpdatedAtRef.current = nextUpdatedAt;
        initializedRef.current = true;
        setLastSyncedAt(nextUpdatedAt || new Date().toISOString());
        setHasUnsavedChanges(false);
      } catch (error) {
        if (cancelled) {
          return;
        }

        setLoadError(extractErrorMessage(error, "Failed to load records."));
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void bootstrap();

    return () => {
      cancelled = true;
    };
  }, [clientsApiQuery, clientsApiQueryKey, hookEnabled, pageSize, paginationEnabled]);

  const updateFilter = useCallback(<Key extends keyof ClientPaymentsFilters>(key: Key, value: ClientPaymentsFilters[Key]) => {
    setFilters((prev) => ({
      ...prev,
      [key]: value,
    }));
  }, []);

  const setDateRange = useCallback(
    (key: "createdAtRange" | "paymentDateRange" | "writtenOffDateRange" | "fullyPaidDateRange", side: "from" | "to", value: string) => {
      setFilters((prev) => ({
        ...prev,
        [key]: {
          ...prev[key],
          [side]: value,
        },
      }));
    },
    [],
  );

  const toggleSort = useCallback((key: keyof ClientRecord) => {
    setSortState((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === "asc" ? "desc" : "asc",
        };
      }

      return {
        key,
        direction: "asc",
      };
    });
  }, []);

  const openCreateModal = useCallback(() => {
    setIsDiscardConfirmOpen(false);
    setModalState({
      open: true,
      mode: "create",
      recordId: "",
      draft: createEmptyRecord(),
      dirty: false,
    });
  }, []);

  const openRecordModal = useCallback((record: ClientRecord) => {
    setIsDiscardConfirmOpen(false);
    setModalState({
      open: true,
      mode: "view",
      recordId: record.id,
      draft: { ...record },
      dirty: false,
    });
  }, []);

  const openClientByName = useCallback(
    (rawClientName: string | undefined) => {
      const requestedName = normalizeComparableClientName(rawClientName || "");
      if (!requestedName || !records.length) {
        return;
      }

      const bestRecord = findRecordByComparableClientName(records, requestedName);
      if (!bestRecord) {
        return;
      }

      openRecordModal(bestRecord);
    },
    [openRecordModal, records],
  );

  useEffect(() => {
    if (!records.length) {
      return;
    }

    const pendingClientName = consumePendingOpenClientCardRequest();
    if (!pendingClientName) {
      return;
    }

    openClientByName(pendingClientName);
  }, [openClientByName, records.length]);

  useEffect(() => {
    function onAssistantOpenClient(event: Event) {
      const detail = (event as CustomEvent<OpenClientCardEventDetail>).detail;
      openClientByName(detail?.clientName);
    }

    window.addEventListener(OPEN_CLIENT_CARD_EVENT_NAME, onAssistantOpenClient as EventListener);
    return () => {
      window.removeEventListener(OPEN_CLIENT_CARD_EVENT_NAME, onAssistantOpenClient as EventListener);
    };
  }, [openClientByName]);

  const startEditRecord = useCallback(() => {
    if (!activeRecord) {
      return;
    }

    setModalState((prev) => ({
      ...prev,
      mode: "edit",
      draft: { ...activeRecord },
      dirty: false,
    }));
  }, [activeRecord]);

  const closeModalNow = useCallback(() => {
    setIsDiscardConfirmOpen(false);
    setModalState(INITIAL_MODAL_STATE);
  }, []);

  const requestCloseModal = useCallback(() => {
    if (!modalState.open) {
      return;
    }

    if (modalState.dirty) {
      setIsDiscardConfirmOpen(true);
      return;
    }

    closeModalNow();
  }, [closeModalNow, modalState.dirty, modalState.open]);

  const cancelDiscardModalClose = useCallback(() => {
    setIsDiscardConfirmOpen(false);
  }, []);

  const discardDraftAndCloseModal = useCallback(() => {
    closeModalNow();
  }, [closeModalNow]);

  const updateDraftField = useCallback((key: keyof ClientRecord, value: string) => {
    setModalState((prev) => ({
      ...prev,
      draft: {
        ...prev.draft,
        [key]: value,
      },
      dirty: prev.dirty || (prev.draft[key] || "") !== (value || ""),
    }));
  }, []);

  const saveDraft = useCallback(() => {
    const validationError = validateDraftAgainstLegacyRules(modalState.draft);
    if (validationError) {
      setSaveError(validationError);
      return;
    }

    const normalized = normalizeFormRecord(modalState.draft);
    setSaveError("");

    setRecords((prev) => {
      if (modalState.mode === "create") {
        return [{ ...normalized, id: normalized.id || createEmptyRecord().id, createdAt: new Date().toISOString() }, ...prev];
      }

      return prev.map((record) => (record.id === modalState.recordId ? { ...normalized, id: record.id, createdAt: record.createdAt } : record));
    });

    closeModalNow();
  }, [closeModalNow, modalState.draft, modalState.mode, modalState.recordId]);

  const retrySave = useCallback(() => {
    clearRetryTimer(retryTimerRef);
    resetSaveRetryState(retryAttemptRef, setSaveRetryCount, setSaveRetryGiveUp);
    setSaveError("");
    void flushSave();
  }, [flushSave]);

  const forceRefresh = useCallback(async () => {
    if (!hookEnabled) {
      return;
    }

    setLoadError("");
    setIsLoading(true);
    setHasMoreRecords(false);
    setIsLoadingMoreRecords(false);
    nextOffsetRef.current = 0;

    try {
      const recordsPayload = paginationEnabled ? await getClientsPage(pageSize, 0, clientsApiQuery) : await getClients();
      const normalized = normalizeRecords(recordsPayload.records);
      setRecords(normalized);
      baselineRef.current = serializeRecords(normalized);
      baselineRecordsRef.current = cloneRecords(normalized);
      nextOffsetRef.current =
        typeof recordsPayload.nextOffset === "number"
          ? Math.max(0, recordsPayload.nextOffset)
          : normalized.length;
      setHasMoreRecords(Boolean(recordsPayload.hasMore));
      setTotalRecordsCount(
        typeof recordsPayload.total === "number" && Number.isFinite(recordsPayload.total)
          ? Math.max(normalized.length, recordsPayload.total)
          : null,
      );
      const nextUpdatedAt = normalizeRevisionTimestamp(recordsPayload.updatedAt);
      serverUpdatedAtRef.current = nextUpdatedAt;
      setLastSyncedAt(nextUpdatedAt || new Date().toISOString());
      setHasUnsavedChanges(false);
      setSaveError("");
      setSaveSuccessNotice("");
    } catch (error) {
      setLoadError(extractErrorMessage(error, "Failed to refresh records."));
    } finally {
      setIsLoading(false);
    }
  }, [clientsApiQuery, hookEnabled, pageSize, paginationEnabled]);

  const loadMoreRecords = useCallback(async () => {
    if (!hookEnabled || !paginationEnabled || isLoading || isLoadingMoreRecords || !hasMoreRecords) {
      return;
    }

    setIsLoadingMoreRecords(true);
    setLoadError("");
    try {
      const recordsPayload = await getClientsPage(pageSize, nextOffsetRef.current, clientsApiQuery);
      const normalized = normalizeRecords(recordsPayload.records);
      if (!normalized.length) {
        setHasMoreRecords(Boolean(recordsPayload.hasMore));
        nextOffsetRef.current =
          typeof recordsPayload.nextOffset === "number"
            ? Math.max(nextOffsetRef.current, recordsPayload.nextOffset)
            : nextOffsetRef.current;
        return;
      }

      const nextBaseline = appendUniqueRecordsById(baselineRecordsRef.current, normalized);
      baselineRecordsRef.current = nextBaseline;
      baselineRef.current = serializeRecords(nextBaseline);

      setRecords((prev) => appendUniqueRecordsById(prev, normalized));
      nextOffsetRef.current =
        typeof recordsPayload.nextOffset === "number"
          ? Math.max(nextOffsetRef.current, recordsPayload.nextOffset)
          : nextOffsetRef.current + normalized.length;
      setHasMoreRecords(Boolean(recordsPayload.hasMore));
      setTotalRecordsCount((prev) => {
        const incomingTotal =
          typeof recordsPayload.total === "number" && Number.isFinite(recordsPayload.total)
            ? Math.max(recordsPayload.total, nextOffsetRef.current)
            : null;
        if (incomingTotal === null) {
          return prev;
        }
        if (prev === null) {
          return incomingTotal;
        }
        return Math.max(prev, incomingTotal);
      });
    } catch (error) {
      setLoadError(extractErrorMessage(error, "Failed to load more records."));
    } finally {
      setIsLoadingMoreRecords(false);
    }
  }, [clientsApiQuery, hasMoreRecords, hookEnabled, isLoading, isLoadingMoreRecords, pageSize, paginationEnabled]);

  return {
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
    tableTotals,
    closedByOptions,
    filtersCollapsed,
    isSaving,
    hasMoreRecords,
    isLoadingMoreRecords,
    totalRecordsCount,
    saveError,
    saveRetryCount,
    saveRetryMax: REMOTE_SYNC_MAX_RETRIES,
    saveRetryGiveUp,
    saveSuccessNotice,
    hasUnsavedChanges,
    lastSyncedAt: formatDateTime(lastSyncedAt),
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
    retrySave,
  };
}

function serializeRecords(records: ClientRecord[]): string {
  return JSON.stringify(records);
}

function cloneRecords(records: ClientRecord[]): ClientRecord[] {
  return records.map((record) => ({ ...record }));
}

function appendUniqueRecordsById(currentRecords: ClientRecord[], incomingRecords: ClientRecord[]): ClientRecord[] {
  if (!incomingRecords.length) {
    return currentRecords;
  }

  const existingIds = new Set(currentRecords.map((record) => record.id));
  const additions = incomingRecords.filter((record) => !existingIds.has(record.id));
  if (!additions.length) {
    return currentRecords;
  }

  return [...currentRecords, ...additions];
}

function scheduleDebouncedSave(
  callback: () => void,
  timerRef: MutableRefObject<number | null>,
  delay: number,
): void {
  clearDebounceTimer(timerRef);
  timerRef.current = window.setTimeout(() => {
    callback();
  }, delay);
}

function clearDebounceTimer(timerRef: MutableRefObject<number | null>): void {
  if (timerRef.current !== null) {
    window.clearTimeout(timerRef.current);
    timerRef.current = null;
  }
}

function scheduleRetry(
  callback: () => void,
  timerRef: MutableRefObject<number | null>,
  delayMs: number,
): void {
  if (timerRef.current !== null) {
    return;
  }

  timerRef.current = window.setTimeout(() => {
    timerRef.current = null;
    callback();
  }, delayMs);
}

function clearRetryTimer(timerRef: MutableRefObject<number | null>): void {
  if (timerRef.current !== null) {
    window.clearTimeout(timerRef.current);
    timerRef.current = null;
  }
}

function extractErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return fallback;
}

function getRetryDelayMs(retryCount: number): number {
  const exponent = Math.max(0, retryCount - 1);
  const delay = REMOTE_SYNC_RETRY_MS * Math.pow(2, exponent);
  return Math.min(delay, REMOTE_SYNC_MAX_RETRY_DELAY_MS);
}

function resetSaveRetryState(
  retryAttemptRef: MutableRefObject<number>,
  setSaveRetryCount: (value: number) => void,
  setSaveRetryGiveUp: (value: boolean) => void,
): void {
  retryAttemptRef.current = 0;
  setSaveRetryCount(0);
  setSaveRetryGiveUp(false);
}

function maybeShowSaveSuccess(
  setSaveSuccessNotice: (value: string) => void,
  timerRef: MutableRefObject<number | null>,
  lastShownAtRef: MutableRefObject<number>,
): void {
  const now = Date.now();
  if (now - lastShownAtRef.current < 2800) {
    return;
  }

  lastShownAtRef.current = now;
  setSaveSuccessNotice("Changes saved.");
  clearSaveSuccessTimer(timerRef);
  timerRef.current = window.setTimeout(() => {
    setSaveSuccessNotice("");
    timerRef.current = null;
  }, 1600);
}

function clearSaveSuccessTimer(timerRef: MutableRefObject<number | null>): void {
  if (timerRef.current !== null) {
    window.clearTimeout(timerRef.current);
    timerRef.current = null;
  }
}

function findRecordByComparableClientName(records: ClientRecord[], requestedName: string): ClientRecord | null {
  let exactMatch: ClientRecord | null = null;
  let exactTimestamp = 0;
  let fuzzyMatch: ClientRecord | null = null;
  let fuzzyTimestamp = 0;

  for (const record of records) {
    const comparableName = normalizeComparableClientName(record.clientName);
    if (!comparableName) {
      continue;
    }

    const createdAtTimestamp = parseCreatedAtTimestamp(record.createdAt);
    if (comparableName === requestedName) {
      if (!exactMatch || createdAtTimestamp > exactTimestamp) {
        exactMatch = record;
        exactTimestamp = createdAtTimestamp;
      }
      continue;
    }

    if (comparableName.includes(requestedName) || requestedName.includes(comparableName)) {
      if (!fuzzyMatch || createdAtTimestamp > fuzzyTimestamp) {
        fuzzyMatch = record;
        fuzzyTimestamp = createdAtTimestamp;
      }
    }
  }

  return exactMatch || fuzzyMatch;
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

function parseCreatedAtTimestamp(rawValue: string): number {
  const timestamp = Date.parse(rawValue || "");
  return Number.isNaN(timestamp) ? 0 : timestamp;
}

function validateDraftAgainstLegacyRules(record: ClientRecord): string {
  for (const field of FIELD_DEFINITIONS) {
    if (field.computed) {
      continue;
    }

    const value = (record[field.key] || "").toString().trim();

    if (field.required && !value) {
      return `Field "${field.label}" is required.`;
    }

    if (field.type === "date" && value) {
      const normalizedDate = normalizeDateForStorage(value);
      if (normalizedDate === null) {
        return `Field "${field.label}" must be in MM/DD/YYYY format.`;
      }
    }
  }

  return "";
}

function normalizeClientsApiQuery(
  query: Record<string, string | number | boolean | null | undefined>,
): Record<string, string | number | boolean> {
  const normalized: Record<string, string | number | boolean> = {};
  for (const [key, rawValue] of Object.entries(query || {})) {
    if (rawValue === null || rawValue === undefined) {
      continue;
    }
    if (typeof rawValue === "boolean") {
      normalized[key] = rawValue;
      continue;
    }
    if (typeof rawValue === "number" && Number.isFinite(rawValue)) {
      normalized[key] = rawValue;
      continue;
    }
    const value = String(rawValue).trim();
    if (!value) {
      continue;
    }
    normalized[key] = value;
  }
  return normalized;
}

function serializeClientsApiQuery(query: Record<string, string | number | boolean>): string {
  const entries = Object.entries(query || {}).sort(([left], [right]) => left.localeCompare(right));
  return JSON.stringify(entries);
}
