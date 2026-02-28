const MONEY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const MAX_SAFE_CENTS = Number.MAX_SAFE_INTEGER;
const MONEY_NUMBER_PATTERN = /^[-+]?(?:\d+\.?\d*|\.\d+)$/;

export function parseMoneyToCents(rawValue: unknown): number | null {
  let value = String(rawValue ?? "").trim();
  if (!value) {
    return null;
  }

  value = value.replace(/[−–—]/g, "-");
  let isNegativeByParentheses = false;
  if (value.startsWith("(") && value.endsWith(")")) {
    isNegativeByParentheses = true;
    value = value.slice(1, -1).trim();
  }

  if (/[a-z]/i.test(value)) {
    return null;
  }

  value = value.replace(/[$,\s]/g, "");
  if (!value || !MONEY_NUMBER_PATTERN.test(value)) {
    return null;
  }

  let parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  if (isNegativeByParentheses) {
    parsed = -Math.abs(parsed);
  }

  const cents = Math.round(parsed * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(cents) > MAX_SAFE_CENTS) {
    return null;
  }

  return cents;
}

export function formatMoneyFromCents(
  cents: number | null | undefined,
  options: { fallback?: string } = {},
): string {
  if (typeof cents !== "number" || !Number.isFinite(cents)) {
    return options.fallback ?? "—";
  }

  const amount = cents / 100;
  const formatted = MONEY_FORMATTER.format(amount);
  return formatted.startsWith("-") ? `−${formatted.slice(1)}` : formatted;
}

export function isFiniteSafeCents(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && Number.isSafeInteger(value);
}
