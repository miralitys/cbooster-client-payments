import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { MutableRefObject } from "react";

import { getRecords, getSession, putRecords } from "@/shared/api";
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
  readClientPaymentsUiState,
  writeClientPaymentsUiState,
} from "@/shared/storage/uiState";

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

export function useClientPayments() {
  const uiState = useMemo(() => readClientPaymentsUiState(), []);

  const [session, setSession] = useState<Session | null>(null);
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
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
  const [lastSyncedAt, setLastSyncedAt] = useState("");
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState(false);
  const [isDiscardConfirmOpen, setIsDiscardConfirmOpen] = useState(false);

  const recordsRef = useRef<ClientRecord[]>([]);
  const baselineRef = useRef("");
  const initializedRef = useRef(false);
  const saveInFlightRef = useRef(false);
  const pendingSaveRef = useRef(false);
  const debounceTimerRef = useRef<number | null>(null);
  const retryTimerRef = useRef<number | null>(null);
  const retryAttemptRef = useRef(0);

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
      await putRecords(snapshot);
      baselineRef.current = serialized;
      setSaveError("");
      setHasUnsavedChanges(false);
      setLastSyncedAt(new Date().toISOString());
      resetSaveRetryState(retryAttemptRef, setSaveRetryCount, setSaveRetryGiveUp);
      clearRetryTimer(retryTimerRef);
    } catch (error) {
      const message = extractErrorMessage(error, "Failed to save records.");
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
  }, []);

  useEffect(() => {
    if (!initializedRef.current) {
      return;
    }

    const serialized = serializeRecords(records);
    const isDirty = serialized !== baselineRef.current;
    setHasUnsavedChanges(isDirty);

    if (isDirty) {
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
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function bootstrap() {
      setIsLoading(true);
      setLoadError("");

      try {
        const [sessionPayload, recordsPayload] = await Promise.all([getSession(), getRecords()]);
        if (cancelled) {
          return;
        }

        const normalizedRecords = normalizeRecords(recordsPayload);
        setSession(sessionPayload);
        setRecords(normalizedRecords);
        baselineRef.current = serializeRecords(normalizedRecords);
        initializedRef.current = true;
        setLastSyncedAt(new Date().toISOString());
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
  }, []);

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

  const startEditRecord = useCallback(() => {
    if (!canManage || !activeRecord) {
      return;
    }

    setModalState((prev) => ({
      ...prev,
      mode: "edit",
      draft: { ...activeRecord },
      dirty: false,
    }));
  }, [activeRecord, canManage]);

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
    setLoadError("");
    setIsLoading(true);

    try {
      const recordsPayload = await getRecords();
      const normalized = normalizeRecords(recordsPayload);
      setRecords(normalized);
      baselineRef.current = serializeRecords(normalized);
      setLastSyncedAt(new Date().toISOString());
      setHasUnsavedChanges(false);
      setSaveError("");
    } catch (error) {
      setLoadError(extractErrorMessage(error, "Failed to refresh records."));
    } finally {
      setIsLoading(false);
    }
  }, []);

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
    saveError,
    saveRetryCount,
    saveRetryMax: REMOTE_SYNC_MAX_RETRIES,
    saveRetryGiveUp,
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
