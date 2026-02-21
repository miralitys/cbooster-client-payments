export type QuickBooksInsightCacheMap = Record<string, string>;

const QUICKBOOKS_INSIGHT_STORAGE_KEY = "cbooster_quickbooks_transaction_insights_v1";
const QUICKBOOKS_INSIGHT_KEY_MAX_LENGTH = 360;
const QUICKBOOKS_INSIGHT_VALUE_MAX_LENGTH = 10000;
const QUICKBOOKS_INSIGHT_MAX_ENTRIES = 600;

export function readQuickBooksTransactionInsightMap(): QuickBooksInsightCacheMap {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const rawValue = window.localStorage.getItem(QUICKBOOKS_INSIGHT_STORAGE_KEY);
    if (!rawValue) {
      return {};
    }
    const parsed = JSON.parse(rawValue);
    return normalizeQuickBooksTransactionInsightMap(parsed);
  } catch {
    return {};
  }
}

export function writeQuickBooksTransactionInsightMap(map: QuickBooksInsightCacheMap): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeQuickBooksTransactionInsightMap(map);
  try {
    window.localStorage.setItem(QUICKBOOKS_INSIGHT_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // Ignore localStorage write errors.
  }
}

export function normalizeQuickBooksTransactionInsightMap(
  map: QuickBooksInsightCacheMap | null | undefined,
): QuickBooksInsightCacheMap {
  const source = map && typeof map === "object" ? map : {};
  const normalized: QuickBooksInsightCacheMap = {};
  let acceptedEntries = 0;

  for (const [rawKey, rawInsight] of Object.entries(source)) {
    if (acceptedEntries >= QUICKBOOKS_INSIGHT_MAX_ENTRIES) {
      break;
    }

    const key = sanitizeStorageText(rawKey, QUICKBOOKS_INSIGHT_KEY_MAX_LENGTH);
    const insight = sanitizeStorageText(rawInsight, QUICKBOOKS_INSIGHT_VALUE_MAX_LENGTH);
    if (!key || !insight) {
      continue;
    }

    normalized[key] = insight;
    acceptedEntries += 1;
  }

  return normalized;
}

function sanitizeStorageText(value: unknown, maxLength: number): string {
  if (typeof value !== "string") {
    return "";
  }

  const text = value.trim();
  if (!text) {
    return "";
  }

  return text.slice(0, Math.max(1, maxLength));
}
