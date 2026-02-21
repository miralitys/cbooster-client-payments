import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import { withStableRowKeys, type RowWithKey } from "@/shared/lib/stableRowKeys";
import {
  normalizeQuickBooksExpenseCategoryMap,
  normalizeQuickBooksExpenseCategoryFingerprintMap,
  normalizeQuickBooksExpenseCategoriesList,
  readQuickBooksExpenseCategoryFingerprintMap,
  readQuickBooksExpenseCategoryMap,
  readQuickBooksExpenseCategoriesList,
  writeQuickBooksExpenseCategoryFingerprintMap,
  writeQuickBooksExpenseCategoryMap,
  writeQuickBooksExpenseCategoriesList,
  type QuickBooksExpenseCategoryFingerprintMap,
  type QuickBooksExpenseCategoryMap,
} from "@/shared/storage/quickbooksExpenseCategories";
import {
  normalizeQuickBooksTransactionInsightMap,
  readQuickBooksTransactionInsightMap,
  writeQuickBooksTransactionInsightMap,
  type QuickBooksInsightCacheMap,
} from "@/shared/storage/quickbooksInsights";
import {
  createQuickBooksSyncJob,
  getQuickBooksOutgoingPayments,
  getQuickBooksPayments,
  getQuickBooksSyncJob,
  getQuickBooksTransactionInsight,
  getSession,
} from "@/shared/api";
import type { QuickBooksPaymentRow, QuickBooksSyncJob, QuickBooksSyncMeta } from "@/shared/types/quickbooks";
import {
  Button,
  EmptyState,
  ErrorState,
  Input,
  LoadingSkeleton,
  Modal,
  PageHeader,
  PageShell,
  Panel,
  Select,
  SegmentedControl,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const QUICKBOOKS_FROM_DATE = "2026-01-01";
const QUICKBOOKS_SYNC_POLL_INTERVAL_MS = 1200;
const QUICKBOOKS_SYNC_POLL_MAX_ATTEMPTS = 150;
const QUICKBOOKS_MONTH_OPTIONS = [
  { value: 1, label: "January" },
  { value: 2, label: "February" },
  { value: 3, label: "March" },
  { value: 4, label: "April" },
  { value: 5, label: "May" },
  { value: 6, label: "June" },
  { value: 7, label: "July" },
  { value: 8, label: "August" },
  { value: 9, label: "September" },
  { value: 10, label: "October" },
  { value: 11, label: "November" },
  { value: 12, label: "December" },
] as const;
const QUICKBOOKS_EXPENSE_DEFAULT_CATEGORIES = ["Marketing", "Salaries"] as const;
const QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL = "Uncategorized";
const QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE = "__none__";
const CURRENCY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

interface QuickBooksRange {
  from: string;
  to: string;
}

interface QuickBooksExpenseCategorySummaryRow {
  category: string;
  totalAmount: number;
}

interface LoadOptions {
  sync?: boolean;
  fullSync?: boolean;
  range?: QuickBooksRange;
}

const QUICKBOOKS_MONEY_FLOW_TABS = [
  {
    key: "incoming",
    label: "Incoming Money",
  },
  {
    key: "outgoing",
    label: "Outgoing Money",
  },
] as const;

type QuickBooksTab = (typeof QUICKBOOKS_MONEY_FLOW_TABS)[number]["key"];
type QuickBooksViewRow = RowWithKey<QuickBooksPaymentRow>;

export default function QuickBooksPage() {
  const todayDate = useMemo(() => new Date(), []);
  const minDateParts = useMemo(() => parseQuickBooksIsoDateParts(QUICKBOOKS_FROM_DATE), []);
  const minQuickBooksYear = minDateParts?.year ?? todayDate.getUTCFullYear();
  const minQuickBooksMonth = minDateParts?.month ?? 1;
  const currentYear = todayDate.getUTCFullYear();
  const currentMonth = todayDate.getUTCMonth() + 1;
  const [activeTab, setActiveTab] = useState<QuickBooksTab>("incoming");
  const [selectedYear, setSelectedYear] = useState(currentYear);
  const [selectedMonth, setSelectedMonth] = useState(currentMonth);
  const [canSync, setCanSync] = useState(false);
  const [incomingTransactions, setIncomingTransactions] = useState<QuickBooksViewRow[]>([]);
  const [outgoingTransactions, setOutgoingTransactions] = useState<QuickBooksViewRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [syncWarning, setSyncWarning] = useState("");
  const [statusText, setStatusText] = useState("Loading incoming transactions...");
  const [rangeText, setRangeText] = useState("");
  const [lastLoadPrefix, setLastLoadPrefix] = useState("");
  const [insightModalRow, setInsightModalRow] = useState<QuickBooksViewRow | null>(null);
  const [insightText, setInsightText] = useState("");
  const [insightError, setInsightError] = useState("");
  const [isInsightLoading, setIsInsightLoading] = useState(false);
  const [insightCache, setInsightCache] = useState<QuickBooksInsightCacheMap>(() => readQuickBooksTransactionInsightMap());

  const [search, setSearch] = useState("");
  const [refundOnly, setRefundOnly] = useState(false);
  const [uncategorizedOnly, setUncategorizedOnly] = useState(false);
  const [selectedOutgoingKeys, setSelectedOutgoingKeys] = useState<string[]>([]);
  const [bulkExpenseCategory, setBulkExpenseCategory] = useState("");
  const [savedExpenseCategories, setSavedExpenseCategories] = useState<string[]>(() =>
    buildQuickBooksExpenseCategoryOptions(readQuickBooksExpenseCategoriesList()),
  );
  const [expenseCategoryMap, setExpenseCategoryMap] = useState<QuickBooksExpenseCategoryMap>(() =>
    readQuickBooksExpenseCategoryMap(),
  );
  const [expenseCategoryFingerprintMap, setExpenseCategoryFingerprintMap] = useState<QuickBooksExpenseCategoryFingerprintMap>(
    () => readQuickBooksExpenseCategoryFingerprintMap(),
  );
  const incomingTransactionsRef = useRef<QuickBooksViewRow[]>([]);
  const outgoingTransactionsRef = useRef<QuickBooksViewRow[]>([]);
  const rowKeySequenceRef = useRef(0);
  const insightModalCacheKeyRef = useRef("");
  const insightPendingCacheKeysRef = useRef(new Set<string>());
  const allTransactions = activeTab === "incoming" ? incomingTransactions : outgoingTransactions;
  const showOnlyRefunds = activeTab === "incoming" && refundOnly;
  const showOnlyUncategorized = activeTab === "outgoing" && uncategorizedOnly;

  const filteredTransactions = useMemo(() => {
    const baseTransactions = filterTransactions(allTransactions, search, showOnlyRefunds);
    if (!showOnlyUncategorized) {
      return baseTransactions;
    }
    return baseTransactions.filter(
      (item) => !resolveQuickBooksExpenseCategoryForRow(item, expenseCategoryMap, expenseCategoryFingerprintMap),
    );
  }, [
    allTransactions,
    expenseCategoryFingerprintMap,
    expenseCategoryMap,
    search,
    showOnlyRefunds,
    showOnlyUncategorized,
  ]);
  const uncategorizedTransactionsCount = useMemo(() => {
    if (activeTab !== "outgoing") {
      return 0;
    }
    return allTransactions.filter(
      (item) => !resolveQuickBooksExpenseCategoryForRow(item, expenseCategoryMap, expenseCategoryFingerprintMap),
    ).length;
  }, [activeTab, allTransactions, expenseCategoryFingerprintMap, expenseCategoryMap]);
  const filteredOutgoingSelectionKeys = useMemo(() => {
    if (activeTab !== "outgoing") {
      return [];
    }
    return filteredTransactions.map((row) => resolveQuickBooksOutgoingSelectionKey(row)).filter(Boolean);
  }, [activeTab, filteredTransactions]);
  const selectedOutgoingKeySet = useMemo(() => new Set(selectedOutgoingKeys), [selectedOutgoingKeys]);
  const selectedFilteredCount = useMemo(
    () => filteredOutgoingSelectionKeys.filter((selectionKey) => selectedOutgoingKeySet.has(selectionKey)).length,
    [filteredOutgoingSelectionKeys, selectedOutgoingKeySet],
  );
  const allFilteredSelected = Boolean(filteredOutgoingSelectionKeys.length) && selectedFilteredCount === filteredOutgoingSelectionKeys.length;
  const canApplyBulkCategory =
    selectedOutgoingKeys.length > 0 && Boolean(sanitizeQuickBooksExpenseCategorySelectValue(bulkExpenseCategory));
  const yearOptions = useMemo(() => {
    const years: number[] = [];
    for (let year = currentYear; year >= minQuickBooksYear; year -= 1) {
      years.push(year);
    }
    return years;
  }, [currentYear, minQuickBooksYear]);
  const monthOptions = useMemo(() => {
    const minMonth = selectedYear === minQuickBooksYear ? minQuickBooksMonth : 1;
    const maxMonth = selectedYear === currentYear ? currentMonth : 12;
    return QUICKBOOKS_MONTH_OPTIONS.filter((monthOption) => monthOption.value >= minMonth && monthOption.value <= maxMonth);
  }, [currentMonth, currentYear, minQuickBooksMonth, minQuickBooksYear, selectedYear]);
  const selectedRange = useMemo(
    () => buildQuickBooksMonthlyRange(selectedYear, selectedMonth, todayDate),
    [selectedMonth, selectedYear, todayDate],
  );
  const expenseCategoryOptions = useMemo(
    () =>
      buildQuickBooksExpenseCategoryOptions(
        savedExpenseCategories,
        outgoingTransactions,
        expenseCategoryMap,
        expenseCategoryFingerprintMap,
      ),
    [expenseCategoryFingerprintMap, expenseCategoryMap, outgoingTransactions, savedExpenseCategories],
  );
  const expenseCategorySummaryRows = useMemo(
    () =>
      buildQuickBooksExpenseCategorySummaryRows(
        outgoingTransactions,
        expenseCategoryMap,
        expenseCategoryFingerprintMap,
        expenseCategoryOptions,
      ),
    [expenseCategoryFingerprintMap, expenseCategoryMap, expenseCategoryOptions, outgoingTransactions],
  );

  useEffect(() => {
    if (!monthOptions.length) {
      return;
    }
    const minAllowedMonth = monthOptions[0].value;
    const maxAllowedMonth = monthOptions[monthOptions.length - 1].value;
    if (selectedMonth < minAllowedMonth) {
      setSelectedMonth(minAllowedMonth);
      return;
    }
    if (selectedMonth > maxAllowedMonth) {
      setSelectedMonth(maxAllowedMonth);
    }
  }, [monthOptions, selectedMonth]);

  useEffect(() => {
    writeQuickBooksExpenseCategoryMap(expenseCategoryMap);
  }, [expenseCategoryMap]);

  useEffect(() => {
    writeQuickBooksExpenseCategoriesList(savedExpenseCategories);
  }, [savedExpenseCategories]);

  useEffect(() => {
    writeQuickBooksExpenseCategoryFingerprintMap(expenseCategoryFingerprintMap);
  }, [expenseCategoryFingerprintMap]);

  useEffect(() => {
    writeQuickBooksTransactionInsightMap(insightCache);
  }, [insightCache]);

  useEffect(() => {
    const outgoingKeys = new Set(outgoingTransactions.map((row) => resolveQuickBooksOutgoingSelectionKey(row)).filter(Boolean));
    setSelectedOutgoingKeys((previousKeys) => {
      const nextKeys = previousKeys.filter((selectionKey) => outgoingKeys.has(selectionKey));
      if (nextKeys.length === previousKeys.length) {
        return previousKeys;
      }
      return nextKeys;
    });
  }, [outgoingTransactions]);

  useEffect(() => {
    if (activeTab === "outgoing") {
      return;
    }
    setSelectedOutgoingKeys([]);
  }, [activeTab]);

  const setQuickBooksExpenseCategory = useCallback((row: QuickBooksViewRow, rawValue: string) => {
    const transactionId = sanitizeQuickBooksTransactionId(row?.transactionId);
    if (!transactionId) {
      return;
    }

    const normalizedValue = normalizeQuickBooksExpenseCategoryLabel(rawValue);
    const rowFingerprint = buildQuickBooksExpenseFingerprint(row);
    setExpenseCategoryMap((previousMap) => {
      const normalizedMap = normalizeQuickBooksExpenseCategoryMap(previousMap);
      if (!normalizedValue) {
        if (!(transactionId in normalizedMap)) {
          return previousMap;
        }
        const nextMap = { ...normalizedMap };
        delete nextMap[transactionId];
        return nextMap;
      }
      if (normalizedMap[transactionId] === normalizedValue) {
        return previousMap;
      }
      return {
        ...normalizedMap,
        [transactionId]: normalizedValue,
      };
    });

    if (normalizedValue && rowFingerprint) {
      setExpenseCategoryFingerprintMap((previousMap) => {
        const normalizedMap = normalizeQuickBooksExpenseCategoryFingerprintMap(previousMap);
        if (normalizedMap[rowFingerprint] === normalizedValue) {
          return previousMap;
        }
        return {
          ...normalizedMap,
          [rowFingerprint]: normalizedValue,
        };
      });
    }
  }, []);

  const addQuickBooksExpenseCategoryFromPrompt = useCallback((row: QuickBooksViewRow) => {
    const nextCategoryRaw = globalThis.window?.prompt("Add expense category", "") || "";
    const nextCategory = normalizeQuickBooksExpenseCategoryLabel(nextCategoryRaw);
    if (!nextCategory) {
      return;
    }

    setSavedExpenseCategories((previousCategories) =>
      prependQuickBooksExpenseCategory(buildQuickBooksExpenseCategoryOptions(previousCategories), nextCategory),
    );
    setQuickBooksExpenseCategory(row, nextCategory);
  }, [setQuickBooksExpenseCategory]);

  const addBulkQuickBooksExpenseCategoryFromPrompt = useCallback(() => {
    const nextCategoryRaw = globalThis.window?.prompt("Add expense category", "") || "";
    const nextCategory = normalizeQuickBooksExpenseCategoryLabel(nextCategoryRaw);
    if (!nextCategory) {
      return;
    }

    setSavedExpenseCategories((previousCategories) =>
      prependQuickBooksExpenseCategory(buildQuickBooksExpenseCategoryOptions(previousCategories), nextCategory),
    );
    setBulkExpenseCategory(nextCategory);
  }, []);

  const toggleOutgoingTransactionSelection = useCallback((row: QuickBooksViewRow) => {
    const selectionKey = resolveQuickBooksOutgoingSelectionKey(row);
    if (!selectionKey) {
      return;
    }

    setSelectedOutgoingKeys((previousKeys) => {
      if (previousKeys.includes(selectionKey)) {
        return previousKeys.filter((existingKey) => existingKey !== selectionKey);
      }
      return [...previousKeys, selectionKey];
    });
  }, []);

  const selectAllFilteredOutgoingTransactions = useCallback(() => {
    if (!filteredOutgoingSelectionKeys.length) {
      return;
    }
    setSelectedOutgoingKeys((previousKeys) => {
      const nextSet = new Set(previousKeys);
      for (const selectionKey of filteredOutgoingSelectionKeys) {
        nextSet.add(selectionKey);
      }
      return [...nextSet];
    });
  }, [filteredOutgoingSelectionKeys]);

  const clearFilteredOutgoingSelection = useCallback(() => {
    if (!filteredOutgoingSelectionKeys.length) {
      return;
    }
    const filteredSet = new Set(filteredOutgoingSelectionKeys);
    setSelectedOutgoingKeys((previousKeys) => previousKeys.filter((selectionKey) => !filteredSet.has(selectionKey)));
  }, [filteredOutgoingSelectionKeys]);

  const applyBulkQuickBooksExpenseCategory = useCallback(() => {
    const nextCategory = sanitizeQuickBooksExpenseCategorySelectValue(bulkExpenseCategory);
    if (!nextCategory || !selectedOutgoingKeys.length) {
      return;
    }

    const selectedSet = new Set(selectedOutgoingKeys);
    const targetRows = outgoingTransactions.filter((row) => selectedSet.has(resolveQuickBooksOutgoingSelectionKey(row)));
    if (!targetRows.length) {
      return;
    }

    setSavedExpenseCategories((previousCategories) =>
      prependQuickBooksExpenseCategory(buildQuickBooksExpenseCategoryOptions(previousCategories), nextCategory),
    );
    for (const row of targetRows) {
      setQuickBooksExpenseCategory(row, nextCategory);
    }
  }, [bulkExpenseCategory, outgoingTransactions, selectedOutgoingKeys, setQuickBooksExpenseCategory]);

  const fetchQuickBooksInsight = useCallback(async (
    row: QuickBooksViewRow,
    options: {
      showSuccessToast?: boolean;
      showErrorToast?: boolean;
    } = {},
  ) => {
    const cacheKey = buildQuickBooksInsightCacheKey(row);
    if (!cacheKey || insightPendingCacheKeysRef.current.has(cacheKey)) {
      return;
    }

    const showSuccessToast = options.showSuccessToast !== false;
    const showErrorToast = options.showErrorToast !== false;
    const isCurrentModalTarget = insightModalCacheKeyRef.current === cacheKey;
    if (isCurrentModalTarget) {
      setIsInsightLoading(true);
      setInsightError("");
    }
    insightPendingCacheKeysRef.current.add(cacheKey);

    try {
      const payload = await getQuickBooksTransactionInsight({
        companyName: formatQuickBooksPayeeLabel(row.clientName),
        amount: Number(row.paymentAmount) || 0,
        date: String(row.paymentDate || "").trim(),
        description: buildQuickBooksInsightDescription(row),
      });

      const nextInsight = String(payload?.insight || "").trim();
      if (!nextInsight) {
        throw new Error("GPT returned an empty response.");
      }

      setInsightCache((previousCache) =>
        normalizeQuickBooksTransactionInsightMap({
          ...normalizeQuickBooksTransactionInsightMap(previousCache),
          [cacheKey]: nextInsight,
        }),
      );
      if (insightModalCacheKeyRef.current === cacheKey) {
        setInsightText(nextInsight);
        setInsightError("");
      }
      if (showSuccessToast) {
        showToast({
          type: "success",
          message: "Insight received from GPT.",
          dedupeKey: "quickbooks-insight-success",
          cooldownMs: 2200,
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to get GPT insight.";
      if (insightModalCacheKeyRef.current === cacheKey) {
        setInsightError(message);
      }
      if (showErrorToast) {
        showToast({
          type: "error",
          message,
          dedupeKey: `quickbooks-insight-error-${message}`,
          cooldownMs: 2200,
        });
      }
    } finally {
      insightPendingCacheKeysRef.current.delete(cacheKey);
      if (insightModalCacheKeyRef.current === cacheKey) {
        setIsInsightLoading(false);
      }
    }
  }, []);

  const openInsightModal = useCallback((row: QuickBooksViewRow) => {
    const cacheKey = buildQuickBooksInsightCacheKey(row);
    const cachedInsight = cacheKey ? insightCache[cacheKey] : "";
    insightModalCacheKeyRef.current = cacheKey;

    setInsightModalRow(row);
    setInsightError("");
    if (cachedInsight) {
      setInsightText(cachedInsight);
      setIsInsightLoading(false);
      return;
    }

    setInsightText("");
    setIsInsightLoading(true);
    void fetchQuickBooksInsight(row, {
      showSuccessToast: false,
      showErrorToast: false,
    });
  }, [fetchQuickBooksInsight, insightCache]);

  const closeInsightModal = useCallback(() => {
    insightModalCacheKeyRef.current = "";
    setInsightModalRow(null);
    setInsightText("");
    setInsightError("");
    setIsInsightLoading(false);
  }, []);

  const askQuickBooksInsight = useCallback(async () => {
    if (!insightModalRow) {
      return;
    }

    await fetchQuickBooksInsight(insightModalRow, {
      showSuccessToast: true,
      showErrorToast: true,
    });
  }, [fetchQuickBooksInsight, insightModalRow]);

  const tableColumns = useMemo<TableColumn<QuickBooksViewRow>[]>(() => {
    if (activeTab === "outgoing") {
      return [
        {
          key: "selection",
          label: "",
          align: "center",
          cell: (item) => {
            const selectionKey = resolveQuickBooksOutgoingSelectionKey(item);
            return (
              <input
                type="checkbox"
                checked={Boolean(selectionKey && selectedOutgoingKeySet.has(selectionKey))}
                onChange={() => toggleOutgoingTransactionSelection(item)}
                className="quickbooks-row-select"
              />
            );
          },
        },
        {
          key: "paymentDate",
          label: "Date",
          align: "center",
          cell: (item) => formatDate(item.paymentDate),
        },
        {
          key: "expenseCategory",
          label: "Expense Category",
          align: "left",
          cell: (item) => (
            <div className="quickbooks-expense-category-control">
              <Select
                value={
                  resolveQuickBooksExpenseCategoryForRow(item, expenseCategoryMap, expenseCategoryFingerprintMap) ||
                  QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE
                }
                onChange={(event) => {
                  const nextCategory = sanitizeQuickBooksExpenseCategorySelectValue(event.target.value);
                  setQuickBooksExpenseCategory(item, nextCategory);
                }}
                className="quickbooks-expense-category-input"
              >
                <option value={QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE}>Not selected</option>
                {expenseCategoryOptions.map((categoryOption) => (
                  <option key={categoryOption} value={categoryOption}>
                    {categoryOption}
                  </option>
                ))}
              </Select>
              <Button
                type="button"
                variant="secondary"
                size="sm"
                className="quickbooks-expense-category-add"
                onClick={() => addQuickBooksExpenseCategoryFromPrompt(item)}
              >
                Add
              </Button>
            </div>
          ),
        },
        {
          key: "clientName",
          label: "Payee",
          align: "left",
          cell: (item) => (
            <div className="quickbooks-client-cell">
              <span>{formatQuickBooksPayeeLabel(item.clientName)}</span>
              <button
                type="button"
                className="quickbooks-info-button"
                onClick={() => openInsightModal(item)}
                aria-label={`Open transaction insight for ${formatQuickBooksPayeeLabel(item.clientName)}`}
              >
                i
              </button>
            </div>
          ),
        },
        {
          key: "paymentAmount",
          label: "Outgoing Amount",
          align: "right",
          cell: (item) => CURRENCY_FORMATTER.format(Number(item.paymentAmount) || 0),
        },
        {
          key: "description",
          label: "Description",
          align: "left",
          cell: (item) => formatQuickBooksOutgoingDescriptionLabel(item.description),
        },
      ];
    }

    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (item) => (
          <div className="quickbooks-client-cell">
            <span>{formatQuickBooksClientLabel(item.clientName, item.transactionType, Number(item.paymentAmount) || 0)}</span>
            <button
              type="button"
              className="quickbooks-info-button"
              onClick={() => openInsightModal(item)}
              aria-label={`Open transaction insight for ${formatQuickBooksClientLabel(item.clientName, item.transactionType, Number(item.paymentAmount) || 0)}`}
            >
              i
            </button>
          </div>
        ),
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
  }, [
    activeTab,
    addQuickBooksExpenseCategoryFromPrompt,
    expenseCategoryFingerprintMap,
    expenseCategoryMap,
    expenseCategoryOptions,
    selectedOutgoingKeySet,
    openInsightModal,
    setQuickBooksExpenseCategory,
    toggleOutgoingTransactionSelection,
  ]);

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
    const targetRange = options.range || selectedRange;
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
          from: targetRange.from,
          to: targetRange.to,
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
        from: targetRange.from,
        to: targetRange.to,
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
  }, [pollQuickBooksSyncJob, selectedRange]);

  const loadOutgoingQuickBooksPayments = useCallback(async (options: LoadOptions = {}) => {
    const targetRange = options.range || selectedRange;
    const previousItems = [...outgoingTransactionsRef.current];
    setIsLoading(true);
    setLoadError("");
    setSyncWarning("");
    setStatusText("Loading expense transactions from QuickBooks...");

    try {
      const payload = await getQuickBooksOutgoingPayments({
        from: targetRange.from,
        to: targetRange.to,
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
  }, [selectedRange]);

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
        showOnlyUncategorized ? uncategorizedTransactionsCount : allTransactions.length,
        filteredTransactions.length,
        search,
        showOnlyRefunds,
        showOnlyUncategorized,
        lastLoadPrefix,
        activeTab,
      ),
    );
  }, [
    activeTab,
    allTransactions.length,
    filteredTransactions.length,
    lastLoadPrefix,
    search,
    showOnlyRefunds,
    showOnlyUncategorized,
    uncategorizedTransactionsCount,
  ]);

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

  const transactionsContent = (
    <>
      {activeTab === "outgoing" ? (
        <div className="quickbooks-bulk-actions">
          <div className="quickbooks-bulk-actions__selection">
            <div className="quickbooks-bulk-actions__buttons">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={selectAllFilteredOutgoingTransactions}
                disabled={!filteredOutgoingSelectionKeys.length || allFilteredSelected}
              >
                Select all
              </Button>
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={clearFilteredOutgoingSelection}
                disabled={!selectedFilteredCount}
              >
                Clear selected
              </Button>
            </div>
            <span className="quickbooks-bulk-actions__meta">Selected: {selectedOutgoingKeys.length}</span>
          </div>
          <div className="quickbooks-bulk-actions__assign">
            <Select
              value={bulkExpenseCategory || QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE}
              onChange={(event) => setBulkExpenseCategory(event.target.value)}
              className="quickbooks-bulk-actions__select"
            >
              <option value={QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE}>Choose category</option>
              {expenseCategoryOptions.map((categoryOption) => (
                <option key={`bulk-${categoryOption}`} value={categoryOption}>
                  {categoryOption}
                </option>
              ))}
            </Select>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              className="quickbooks-bulk-actions__add"
              onClick={addBulkQuickBooksExpenseCategoryFromPrompt}
            >
              Add
            </Button>
            <Button
              type="button"
              size="sm"
              className="quickbooks-bulk-actions__apply"
              onClick={applyBulkQuickBooksExpenseCategory}
              disabled={!canApplyBulkCategory}
            >
              Apply to selected
            </Button>
          </div>
        </div>
      ) : null}

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
                ? showOnlyUncategorized
                  ? `No uncategorized expense transactions found for "${search.trim()}".`
                  : `No expense transactions found for "${search.trim()}".`
                : showOnlyUncategorized
                  ? "No uncategorized expense transactions found for the selected period."
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
    </>
  );

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
              <p className="search-label quickbooks-search-field__label">Money Flow</p>
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
            <div className="quickbooks-search-field quickbooks-period-field">
              <label htmlFor="quickbooks-month-select" className="search-label quickbooks-search-field__label">
                Month-Year
              </label>
              <div className="quickbooks-period-field__controls">
                <Select
                  id="quickbooks-month-select"
                  value={String(selectedMonth)}
                  onChange={(event) => {
                    const nextMonth = Number.parseInt(event.target.value, 10);
                    if (Number.isFinite(nextMonth)) {
                      setSelectedMonth(nextMonth);
                    }
                  }}
                  disabled={isLoading}
                >
                  {monthOptions.map((monthOption) => (
                    <option key={monthOption.value} value={monthOption.value}>
                      {monthOption.label}
                    </option>
                  ))}
                </Select>
                <Select
                  id="quickbooks-year-select"
                  value={String(selectedYear)}
                  onChange={(event) => {
                    const nextYear = Number.parseInt(event.target.value, 10);
                    if (Number.isFinite(nextYear)) {
                      setSelectedYear(nextYear);
                    }
                  }}
                  disabled={isLoading}
                >
                  {yearOptions.map((yearOption) => (
                    <option key={yearOption} value={yearOption}>
                      {yearOption}
                    </option>
                  ))}
                </Select>
              </div>
            </div>
            <div className="quickbooks-search-field quickbooks-search-field--query">
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
              <label htmlFor="quickbooks-refund-only" className="quickbooks-toggle-filter">
                <input
                  id="quickbooks-refund-only"
                  type="checkbox"
                  checked={refundOnly}
                  onChange={(event) => setRefundOnly(event.target.checked)}
                />
                <span>Only refunds</span>
              </label>
            ) : (
              <label htmlFor="quickbooks-uncategorized-only" className="quickbooks-toggle-filter">
                <input
                  id="quickbooks-uncategorized-only"
                  type="checkbox"
                  checked={uncategorizedOnly}
                  onChange={(event) => setUncategorizedOnly(event.target.checked)}
                />
                <span>Только без категории</span>
              </label>
            )}
          </div>
        }
      >
        {activeTab === "outgoing" ? (
          <div className="quickbooks-outgoing-layout">
            <aside className="quickbooks-outgoing-layout__sidebar">
              <div className="quickbooks-expense-summary">
                <p className="quickbooks-expense-summary__title">Expense Categories</p>
                <div className="quickbooks-expense-summary__rows">
                  {expenseCategorySummaryRows.map((summaryRow) => (
                    <div key={summaryRow.category} className="quickbooks-expense-summary__row">
                      <span>{summaryRow.category}</span>
                      <strong>{CURRENCY_FORMATTER.format(summaryRow.totalAmount)}</strong>
                    </div>
                  ))}
                </div>
              </div>
            </aside>
            <div className="quickbooks-outgoing-layout__main">
              {transactionsContent}
            </div>
          </div>
        ) : (
          transactionsContent
        )}
      </Panel>

      <Modal
        open={Boolean(insightModalRow)}
        title="Transaction Insight"
        onClose={closeInsightModal}
        footer={
          <div className="quickbooks-insight-actions">
            <Button type="button" variant="secondary" size="sm" onClick={closeInsightModal} disabled={isInsightLoading}>
              Close
            </Button>
            <Button type="button" size="sm" onClick={() => void askQuickBooksInsight()} isLoading={isInsightLoading}>
              Ask GPT
            </Button>
          </div>
        }
      >
        <div className="quickbooks-insight-summary">
          <p>
            <strong>Company:</strong> {insightModalRow ? formatQuickBooksPayeeLabel(insightModalRow.clientName) : "-"}
          </p>
          <p>
            <strong>Amount:</strong>{" "}
            {insightModalRow ? CURRENCY_FORMATTER.format(Number(insightModalRow.paymentAmount) || 0) : "-"}
          </p>
          <p>
            <strong>Date:</strong> {insightModalRow ? formatDate(insightModalRow.paymentDate) : "-"}
          </p>
        </div>

        {!insightText && !insightError ? (
          <p className="quickbooks-insight-empty">
            {isInsightLoading ? "Generating GPT insight..." : 'Click "Ask GPT" to generate a transaction explanation.'}
          </p>
        ) : null}
        {insightError ? <p className="quickbooks-insight-error">{insightError}</p> : null}
        {insightText ? <QuickBooksInsightFormattedView rawText={insightText} /> : null}
      </Modal>
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
    String(row.transactionId || "").trim(),
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

function resolveQuickBooksOutgoingSelectionKey(row: QuickBooksPaymentRow): string {
  const transactionId = sanitizeQuickBooksTransactionId(row?.transactionId);
  if (transactionId) {
    return `tx:${transactionId}`;
  }

  const viewRowKey = (row as { _rowKey?: unknown })?._rowKey;
  if (typeof viewRowKey === "string" && viewRowKey.trim()) {
    return `row:${viewRowKey.trim()}`;
  }

  const fallbackSignature = [
    String(row?.paymentDate || "").trim(),
    String(row?.clientName || "").trim(),
    String(row?.description || "").trim(),
    String(row?.paymentAmount ?? "").trim(),
  ]
    .join("|")
    .trim();
  return fallbackSignature ? `sig:${fallbackSignature}` : "";
}

function sanitizeQuickBooksTransactionId(rawValue: unknown): string {
  return String(rawValue || "").trim().slice(0, 180);
}

function normalizeQuickBooksExpenseCategoryLabel(rawValue: unknown): string {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "";
  }

  const lowered = value.toLocaleLowerCase("en-US");
  if (lowered === "маркетинг" || lowered === "marketing") {
    return "Marketing";
  }
  if (lowered === "зарплаты" || lowered === "зарплата" || lowered === "salary" || lowered === "salaries") {
    return "Salaries";
  }
  if (lowered === "без категории" || lowered === "uncategorized" || lowered === "no category") {
    return QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL;
  }

  return value.slice(0, 120);
}

function normalizeQuickBooksExpenseFingerprintValue(rawValue: unknown): string {
  return String(rawValue || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLocaleLowerCase("ru-RU")
    .slice(0, 200);
}

function buildQuickBooksExpenseFingerprint(row: QuickBooksPaymentRow): string {
  const normalizedPayee = normalizeQuickBooksExpenseFingerprintValue(row?.clientName);
  const normalizedDescription = normalizeQuickBooksExpenseFingerprintValue(row?.description);
  if (!normalizedPayee && !normalizedDescription) {
    return "";
  }
  return `${normalizedPayee}|${normalizedDescription}`;
}

function sanitizeQuickBooksExpenseCategorySelectValue(rawValue: unknown): string {
  const value = String(rawValue || "").trim();
  if (!value || value === QUICKBOOKS_EXPENSE_CATEGORY_EMPTY_VALUE) {
    return "";
  }
  return normalizeQuickBooksExpenseCategoryLabel(value);
}

function buildQuickBooksExpenseCategoryOptions(
  categories: string[] | null | undefined,
  rows: QuickBooksPaymentRow[] = [],
  categoryMap: QuickBooksExpenseCategoryMap = {},
  categoryFingerprintMap: QuickBooksExpenseCategoryFingerprintMap = {},
): string[] {
  const normalizedCategories = normalizeQuickBooksExpenseCategoriesList(categories || [])
    .map((category) => normalizeQuickBooksExpenseCategoryLabel(category))
    .filter(Boolean);
  const dedupedNormalizedCategories: string[] = [];
  const seen = new Set<string>();
  for (const category of normalizedCategories) {
    const key = category.toLocaleLowerCase("en-US");
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    dedupedNormalizedCategories.push(category);
  }

  const combined = [...dedupedNormalizedCategories];
  for (const defaultCategory of QUICKBOOKS_EXPENSE_DEFAULT_CATEGORIES) {
    const key = defaultCategory.toLocaleLowerCase("en-US");
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    combined.push(defaultCategory);
  }
  for (const row of Array.isArray(rows) ? rows : []) {
    const resolvedCategory = resolveQuickBooksExpenseCategoryForRow(row, categoryMap, categoryFingerprintMap);
    if (!resolvedCategory) {
      continue;
    }
    const key = resolvedCategory.toLocaleLowerCase("en-US");
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    combined.push(resolvedCategory);
  }
  return combined;
}

function prependQuickBooksExpenseCategory(categories: string[], category: string): string[] {
  const normalizedCategory = normalizeQuickBooksExpenseCategoryLabel(category);
  if (!normalizedCategory) {
    return categories;
  }
  const normalizedCategories = buildQuickBooksExpenseCategoryOptions(categories);
  const categoryKey = normalizedCategory.toLocaleLowerCase("en-US");
  const nextCategories = [normalizedCategory];
  for (const currentCategory of normalizedCategories) {
    if (currentCategory.toLocaleLowerCase("en-US") === categoryKey) {
      continue;
    }
    nextCategories.push(currentCategory);
  }
  return nextCategories;
}

function resolveQuickBooksExpenseCategoryForRow(
  row: QuickBooksPaymentRow,
  categoryMap: QuickBooksExpenseCategoryMap,
  categoryFingerprintMap: QuickBooksExpenseCategoryFingerprintMap = {},
): string {
  const transactionId = sanitizeQuickBooksTransactionId(row?.transactionId);
  if (transactionId) {
    const directCategory = normalizeQuickBooksExpenseCategoryLabel(categoryMap[transactionId] || "");
    if (directCategory) {
      return directCategory;
    }
  }

  const fingerprint = buildQuickBooksExpenseFingerprint(row);
  if (!fingerprint) {
    return "";
  }
  return normalizeQuickBooksExpenseCategoryLabel(categoryFingerprintMap[fingerprint] || "");
}

function buildQuickBooksExpenseCategorySummaryRows(
  rows: QuickBooksPaymentRow[],
  categoryMap: QuickBooksExpenseCategoryMap,
  categoryFingerprintMap: QuickBooksExpenseCategoryFingerprintMap,
  orderedCategories: string[],
): QuickBooksExpenseCategorySummaryRow[] {
  const totals = new Map<string, number>();
  for (const category of orderedCategories) {
    totals.set(category, 0);
  }

  for (const row of Array.isArray(rows) ? rows : []) {
    const amount = Math.abs(Number(row?.paymentAmount) || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    const resolvedCategory = resolveQuickBooksExpenseCategoryForRow(row, categoryMap, categoryFingerprintMap);
    const category = resolvedCategory || QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL;
    totals.set(category, (totals.get(category) || 0) + amount);
  }

  const extraCategories = [...totals.keys()]
    .filter((category) => !orderedCategories.includes(category) && category !== QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL)
    .sort((left, right) => left.localeCompare(right, "en-US"));
  const normalizedOrderedCategories = [...orderedCategories, ...extraCategories];
  if ((totals.get(QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL) || 0) > 0) {
    normalizedOrderedCategories.push(QUICKBOOKS_EXPENSE_UNCATEGORIZED_LABEL);
  }
  return normalizedOrderedCategories.map((category) => ({
    category,
    totalAmount: totals.get(category) || 0,
  }));
}

function buildFilterStatusMessage(
  totalCount: number,
  visibleCount: number,
  query: string,
  showOnlyRefunds: boolean,
  showOnlyUncategorized = false,
  prefix = "",
  tab: QuickBooksTab = "incoming",
): string {
  const normalizedQuery = query.trim();
  const normalizedPrefix = prefix.trim();

  if (tab === "outgoing") {
    const nounPhrase = showOnlyUncategorized ? "uncategorized expense transaction" : "expense transaction";
    let outgoingMessage = "";
    if (!normalizedQuery) {
      outgoingMessage = `Loaded ${totalCount} ${nounPhrase}${totalCount === 1 ? "" : "s"}.`;
    } else if (visibleCount === 0) {
      outgoingMessage = `No ${showOnlyUncategorized ? "uncategorized expense" : "expense"} transactions found for "${normalizedQuery}".`;
    } else {
      outgoingMessage = `Showing ${visibleCount} of ${totalCount} ${showOnlyUncategorized ? "uncategorized expense" : "expense"} transactions for "${normalizedQuery}".`;
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

function formatQuickBooksOutgoingDescriptionLabel(description: string | undefined): string {
  const normalizedValue = String(description || "").trim();
  return normalizedValue || "-";
}

function buildQuickBooksInsightDescription(item: QuickBooksPaymentRow): string {
  const details = [item?.description, item?.categoryName, item?.categoryDetails, item?.transactionType]
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  return details.join(" | ") || "-";
}

function buildQuickBooksInsightCacheKey(item: QuickBooksPaymentRow): string {
  const transactionId = sanitizeQuickBooksTransactionId(item?.transactionId);
  if (transactionId) {
    return `tx:${transactionId}`;
  }

  const signature = [
    String(item?.clientName || "").trim(),
    String(item?.paymentAmount ?? "").trim(),
    String(item?.paymentDate || "").trim(),
    String(item?.transactionType || "").trim(),
    String(item?.description || "").trim(),
  ]
    .join("|")
    .trim();
  return signature ? `sig:${signature}` : "";
}

type InsightSectionKey = "company" | "expense" | "categories" | "confidence" | "other";

interface ParsedQuickBooksInsight {
  company: string[];
  expense: string[];
  categories: string[];
  confidence: string;
  other: string[];
}

function QuickBooksInsightFormattedView({ rawText }: { rawText: string }) {
  const parsedInsight = parseQuickBooksInsight(rawText);
  const confidenceClassName = getQuickBooksConfidenceClassName(parsedInsight.confidence);

  return (
    <div className="quickbooks-insight-pretty">
      {parsedInsight.company.length ? (
        <section className="quickbooks-insight-card">
          <h4>Company Activity</h4>
          <p>{parsedInsight.company.join(" ")}</p>
        </section>
      ) : null}

      {parsedInsight.expense.length ? (
        <section className="quickbooks-insight-card">
          <h4>Most Likely Expense Type</h4>
          <p>{parsedInsight.expense.join(" ")}</p>
        </section>
      ) : null}

      {parsedInsight.categories.length ? (
        <section className="quickbooks-insight-card">
          <h4>Suggested QuickBooks Category</h4>
          <ol className="quickbooks-insight-list">
            {parsedInsight.categories.map((category, index) => (
              <li key={`${category}-${index}`}>{category}</li>
            ))}
          </ol>
        </section>
      ) : null}

      {parsedInsight.confidence ? (
        <section className="quickbooks-insight-card quickbooks-insight-card--confidence">
          <h4>Confidence Level</h4>
          <span className={`quickbooks-insight-confidence ${confidenceClassName}`.trim()}>{parsedInsight.confidence}</span>
        </section>
      ) : null}

      {parsedInsight.other.length ? (
        <section className="quickbooks-insight-card">
          <h4>Additional Notes</h4>
          <p>{parsedInsight.other.join(" ")}</p>
        </section>
      ) : null}
    </div>
  );
}

function parseQuickBooksInsight(rawText: string): ParsedQuickBooksInsight {
  const parsed: ParsedQuickBooksInsight = {
    company: [],
    expense: [],
    categories: [],
    confidence: "",
    other: [],
  };

  const normalizedLines = String(rawText || "")
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => cleanQuickBooksInsightLine(line))
    .filter(Boolean);

  let currentSection: InsightSectionKey = "other";

  for (const line of normalizedLines) {
    const sectionKey = resolveQuickBooksInsightSectionByHeader(line);
    if (sectionKey) {
      currentSection = sectionKey;
      continue;
    }

    if (currentSection === "categories") {
      const category = line.replace(/^\d+[.)]\s*/, "").trim();
      if (category) {
        parsed.categories.push(category);
      }
      continue;
    }

    if (currentSection === "confidence") {
      if (!parsed.confidence) {
        parsed.confidence = normalizeQuickBooksConfidenceLabel(line);
      }
      continue;
    }

    if (currentSection === "company") {
      parsed.company.push(line);
      continue;
    }

    if (currentSection === "expense") {
      parsed.expense.push(line);
      continue;
    }

    parsed.other.push(line);
  }

  if (!parsed.categories.length && parsed.other.length) {
    const extractedCategories = parsed.other
      .filter((line) => /^\d+[.)]\s*/.test(line))
      .map((line) => line.replace(/^\d+[.)]\s*/, "").trim())
      .filter(Boolean);
    if (extractedCategories.length) {
      parsed.categories = extractedCategories;
      parsed.other = parsed.other.filter((line) => !/^\d+[.)]\s*/.test(line));
    }
  }

  if (!parsed.confidence) {
    const sourceLine = [...parsed.other, ...parsed.expense, ...parsed.company].find((line) => /(low|medium|high)/i.test(line));
    if (sourceLine) {
      parsed.confidence = normalizeQuickBooksConfidenceLabel(sourceLine);
    }
  }

  if (!parsed.company.length && !parsed.expense.length && !parsed.categories.length && !parsed.confidence) {
    parsed.other = normalizedLines;
  }

  return parsed;
}

function cleanQuickBooksInsightLine(line: string): string {
  return String(line || "")
    .replace(/\*\*/g, "")
    .replace(/^[-•]\s*/, "")
    .replace(/[“”]/g, '"')
    .trim();
}

function resolveQuickBooksInsightSectionByHeader(line: string): InsightSectionKey | null {
  const normalized = cleanQuickBooksInsightLine(line).toLowerCase().replace(/[:\s]+$/g, "");
  if (!normalized) {
    return null;
  }

  if (normalized.startsWith("company activity")) {
    return "company";
  }
  if (normalized.startsWith("most likely expense type")) {
    return "expense";
  }
  if (normalized.startsWith("suggested quickbooks category")) {
    return "categories";
  }
  if (normalized.startsWith("confidence level")) {
    return "confidence";
  }

  return null;
}

function normalizeQuickBooksConfidenceLabel(value: string): string {
  const normalized = cleanQuickBooksInsightLine(value).toLowerCase();
  if (normalized.includes("high")) {
    return "High";
  }
  if (normalized.includes("medium")) {
    return "Medium";
  }
  if (normalized.includes("low")) {
    return "Low";
  }
  return cleanQuickBooksInsightLine(value);
}

function getQuickBooksConfidenceClassName(confidence: string): string {
  const normalized = String(confidence || "").toLowerCase();
  if (normalized === "high") {
    return "quickbooks-insight-confidence--high";
  }
  if (normalized === "medium") {
    return "quickbooks-insight-confidence--medium";
  }
  if (normalized === "low") {
    return "quickbooks-insight-confidence--low";
  }
  return "quickbooks-insight-confidence--neutral";
}

function parseQuickBooksIsoDateParts(value: string): { year: number; month: number; day: number } | null {
  const match = String(value || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return null;
  }
  const year = Number.parseInt(match[1], 10);
  const month = Number.parseInt(match[2], 10);
  const day = Number.parseInt(match[3], 10);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }
  return { year, month, day };
}

function buildQuickBooksMonthlyRange(year: number, month: number, todayDate: Date): QuickBooksRange {
  const normalizedYear = Number.isFinite(year) ? Math.max(1900, Math.trunc(year)) : todayDate.getUTCFullYear();
  const normalizedMonth = Number.isFinite(month) ? Math.min(12, Math.max(1, Math.trunc(month))) : todayDate.getUTCMonth() + 1;
  const from = `${normalizedYear}-${String(normalizedMonth).padStart(2, "0")}-01`;
  const endOfMonth = new Date(Date.UTC(normalizedYear, normalizedMonth, 0));
  const endOfMonthIso = formatDateForApi(endOfMonth);
  const todayIso = formatDateForApi(todayDate);
  const rawTo = endOfMonthIso <= todayIso ? endOfMonthIso : todayIso;
  return {
    from,
    to: rawTo >= from ? rawTo : from,
  };
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
