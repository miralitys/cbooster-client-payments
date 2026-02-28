import { describe, expect, it } from "vitest";

import { formatMoneyFromCents, parseMoneyToCents } from "@/features/client-payments-3/domain/money";

describe("client-payments-3 money", () => {
  it("parses money to cents from multiple formats", () => {
    expect(parseMoneyToCents("$1,234.56")).toBe(123456);
    expect(parseMoneyToCents("(100.00)")).toBe(-10000);
    expect(parseMoneyToCents("−$5.25")).toBe(-525);
    expect(parseMoneyToCents("  ")).toBeNull();
    expect(parseMoneyToCents("abc")).toBeNull();
  });

  it("formats cents with leading minus before currency symbol", () => {
    expect(formatMoneyFromCents(123456)).toBe("$1,234.56");
    expect(formatMoneyFromCents(-123456)).toBe("−$1,234.56");
    expect(formatMoneyFromCents(null)).toBe("—");
  });
});
