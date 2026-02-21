export type QuickBooksExpenseCategoryMap = Record<string, string>;
export type QuickBooksExpenseCategoryFingerprintMap = Record<string, string>;

const QUICKBOOKS_EXPENSE_CATEGORIES_STORAGE_KEY = "cbooster_quickbooks_expense_categories_v1";
const QUICKBOOKS_EXPENSE_CATEGORIES_LIST_STORAGE_KEY = "cbooster_quickbooks_expense_categories_list_v1";
const QUICKBOOKS_EXPENSE_CATEGORIES_FINGERPRINT_STORAGE_KEY = "cbooster_quickbooks_expense_categories_fingerprint_v1";
const QUICKBOOKS_CATEGORY_MAX_LENGTH = 120;
const QUICKBOOKS_TRANSACTION_ID_MAX_LENGTH = 180;
const QUICKBOOKS_FINGERPRINT_MAX_LENGTH = 400;

export function readQuickBooksExpenseCategoryMap(): QuickBooksExpenseCategoryMap {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const rawValue = window.localStorage.getItem(QUICKBOOKS_EXPENSE_CATEGORIES_STORAGE_KEY);
    if (!rawValue) {
      return {};
    }
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    const result: QuickBooksExpenseCategoryMap = {};
    for (const [rawTransactionId, rawCategory] of Object.entries(parsed)) {
      const transactionId = sanitizeStorageText(rawTransactionId, QUICKBOOKS_TRANSACTION_ID_MAX_LENGTH);
      const category = sanitizeStorageText(rawCategory, QUICKBOOKS_CATEGORY_MAX_LENGTH);
      if (!transactionId || !category) {
        continue;
      }
      result[transactionId] = category;
    }
    return result;
  } catch {
    return {};
  }
}

export function writeQuickBooksExpenseCategoryMap(map: QuickBooksExpenseCategoryMap): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeQuickBooksExpenseCategoryMap(map);
  try {
    window.localStorage.setItem(QUICKBOOKS_EXPENSE_CATEGORIES_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // Ignore localStorage write errors.
  }
}

export function readQuickBooksExpenseCategoryFingerprintMap(): QuickBooksExpenseCategoryFingerprintMap {
  if (typeof window === "undefined") {
    return {};
  }

  try {
    const rawValue = window.localStorage.getItem(QUICKBOOKS_EXPENSE_CATEGORIES_FINGERPRINT_STORAGE_KEY);
    if (!rawValue) {
      return {};
    }
    const parsed = JSON.parse(rawValue);
    return normalizeQuickBooksExpenseCategoryFingerprintMap(parsed);
  } catch {
    return {};
  }
}

export function writeQuickBooksExpenseCategoryFingerprintMap(map: QuickBooksExpenseCategoryFingerprintMap): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeQuickBooksExpenseCategoryFingerprintMap(map);
  try {
    window.localStorage.setItem(QUICKBOOKS_EXPENSE_CATEGORIES_FINGERPRINT_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // Ignore localStorage write errors.
  }
}

export function readQuickBooksExpenseCategoriesList(): string[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const rawValue = window.localStorage.getItem(QUICKBOOKS_EXPENSE_CATEGORIES_LIST_STORAGE_KEY);
    if (!rawValue) {
      return [];
    }
    const parsed = JSON.parse(rawValue);
    return normalizeQuickBooksExpenseCategoriesList(parsed);
  } catch {
    return [];
  }
}

export function writeQuickBooksExpenseCategoriesList(categories: string[]): void {
  if (typeof window === "undefined") {
    return;
  }

  const normalized = normalizeQuickBooksExpenseCategoriesList(categories);
  try {
    window.localStorage.setItem(QUICKBOOKS_EXPENSE_CATEGORIES_LIST_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // Ignore localStorage write errors.
  }
}

export function normalizeQuickBooksExpenseCategoryMap(
  map: QuickBooksExpenseCategoryMap | null | undefined,
): QuickBooksExpenseCategoryMap {
  const source = map && typeof map === "object" ? map : {};
  const normalized: QuickBooksExpenseCategoryMap = {};
  for (const [rawTransactionId, rawCategory] of Object.entries(source)) {
    const transactionId = sanitizeStorageText(rawTransactionId, QUICKBOOKS_TRANSACTION_ID_MAX_LENGTH);
    const category = sanitizeStorageText(rawCategory, QUICKBOOKS_CATEGORY_MAX_LENGTH);
    if (!transactionId || !category) {
      continue;
    }
    normalized[transactionId] = category;
  }
  return normalized;
}

export function normalizeQuickBooksExpenseCategoryFingerprintMap(
  map: QuickBooksExpenseCategoryFingerprintMap | null | undefined,
): QuickBooksExpenseCategoryFingerprintMap {
  const source = map && typeof map === "object" ? map : {};
  const normalized: QuickBooksExpenseCategoryFingerprintMap = {};
  for (const [rawFingerprint, rawCategory] of Object.entries(source)) {
    const fingerprint = sanitizeStorageText(rawFingerprint, QUICKBOOKS_FINGERPRINT_MAX_LENGTH);
    const category = sanitizeStorageText(rawCategory, QUICKBOOKS_CATEGORY_MAX_LENGTH);
    if (!fingerprint || !category) {
      continue;
    }
    normalized[fingerprint] = category;
  }
  return normalized;
}

export function normalizeQuickBooksExpenseCategoriesList(categories: unknown): string[] {
  const source = Array.isArray(categories) ? categories : [];
  const normalized: string[] = [];
  const seen = new Set<string>();
  for (const rawCategory of source) {
    const category = sanitizeStorageText(rawCategory, QUICKBOOKS_CATEGORY_MAX_LENGTH);
    if (!category) {
      continue;
    }
    const key = category.toLocaleLowerCase("ru-RU");
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalized.push(category);
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
