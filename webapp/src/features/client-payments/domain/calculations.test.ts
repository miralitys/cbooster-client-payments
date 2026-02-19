import { describe, expect, it } from "vitest";

import type { ClientRecord } from "@/shared/types/records";
import {
  calculateOverviewMetrics,
  calculateTableTotals,
  createEmptyRecord,
  filterRecords,
  formatDate,
  formatMoney,
  getRecordStatusFlags,
  normalizeFormRecord,
  parseDateValue,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";

function makeRecord(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    ...patch,
  };
}

function formatUsDateFromUtcDaysAgo(daysAgo: number): string {
  const now = new Date();
  const utcDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - daysAgo));
  const month = String(utcDate.getUTCMonth() + 1).padStart(2, "0");
  const day = String(utcDate.getUTCDate()).padStart(2, "0");
  const year = String(utcDate.getUTCFullYear());
  return `${month}/${day}/${year}`;
}

describe("calculations", () => {
  it("computes futurePayments and totalPayments from payment fields", () => {
    const normalized = normalizeFormRecord(
      makeRecord({
        contractTotals: "1000",
        payment1: "200",
        payment2: "100",
      }),
    );

    expect(normalized.totalPayments).toBe("300");
    expect(normalized.futurePayments).toBe("700");
  });

  it("computes dateWhenFullyPaid when balance reaches zero on latest payment date", () => {
    const normalized = normalizeFormRecord(
      makeRecord({
        contractTotals: "100",
        payment1: "40",
        payment1Date: "02/01/2026",
        payment2: "60",
        payment2Date: "02/20/2026",
      }),
    );

    expect(normalized.futurePayments).toBe("0");
    expect(normalized.dateWhenFullyPaid).toBe("02/20/2026");
  });

  it("does not set dateWhenFullyPaid for written off records", () => {
    const normalized = normalizeFormRecord(
      makeRecord({
        contractTotals: "100",
        payment1: "100",
        payment1Date: "02/20/2026",
        writtenOff: "Yes",
      }),
    );

    expect(normalized.dateWhenFullyPaid).toBe("");
  });

  it("calculates overdue status and range for stale balances", () => {
    const record = normalizeFormRecord(
      makeRecord({
        contractTotals: "500",
        payment1: "100",
        payment1Date: formatUsDateFromUtcDaysAgo(10),
      }),
    );

    const status = getRecordStatusFlags(record);
    expect(status.isOverdue).toBe(true);
    expect(status.overdueRange).toBe("8-30");
    expect(status.isWrittenOff).toBe(false);
    expect(status.isFullyPaid).toBe(false);
  });

  it("computes table totals with mixed number formats", () => {
    const totals = calculateTableTotals([
      makeRecord({
        contractTotals: "$1,200.50",
        totalPayments: "300.25",
        futurePayments: "900.25",
        collection: "10",
      }),
      makeRecord({
        contractTotals: "(100)",
        totalPayments: "50",
        futurePayments: "-150",
        collection: "5.5",
      }),
    ]);

    expect(totals.contractTotals).toBeCloseTo(1100.5, 6);
    expect(totals.totalPayments).toBeCloseTo(350.25, 6);
    expect(totals.futurePayments).toBeCloseTo(750.25, 6);
    expect(totals.collection).toBeCloseTo(15.5, 6);
  });

  it("handles money parsing/formatting edge cases", () => {
    expect(parseMoneyValue("$1,234.56")).toBeCloseTo(1234.56, 6);
    expect(parseMoneyValue("(100.00)")).toBeCloseTo(-100, 6);
    expect(parseMoneyValue("  ")).toBeNull();
    expect(parseMoneyValue("abc")).toBeNull();
    expect(formatMoney(1234.5)).toBe("$1,234.50");
  });

  it("handles date parsing/formatting edge cases", () => {
    expect(parseDateValue("02/29/2024")).toBe(Date.UTC(2024, 1, 29));
    expect(parseDateValue("02/29/2025")).toBeNull();
    expect(parseDateValue("2026-02-18")).toBe(Date.UTC(2026, 1, 18));
    expect(parseDateValue("13/99/2026")).toBeNull();
    expect(formatDate("2026-02-18")).toBe("02/18/2026");
  });

  it("parses timezone timestamps into normalized UTC day", () => {
    expect(parseDateValue("2026-02-18T23:30:00-05:00")).toBe(Date.UTC(2026, 1, 19));
    expect(parseDateValue("2026-02-18T00:30:00+09:00")).toBe(Date.UTC(2026, 1, 17));
  });

  it("calculates overview metrics by selected period", () => {
    const today = formatUsDateFromUtcDaysAgo(0);
    const record = normalizeFormRecord(
      makeRecord({
        contractTotals: "1000",
        payment1: "250",
        payment1Date: today,
      }),
    );

    const metrics = calculateOverviewMetrics([record], "currentWeek");
    expect(metrics.sales).toBeGreaterThanOrEqual(1000);
    expect(metrics.received).toBeGreaterThanOrEqual(250);
    expect(metrics.debt).toBeGreaterThanOrEqual(750);
  });

  it("filters New Client range by Payment 1 Date (not createdAt)", () => {
    const records: ClientRecord[] = [
      makeRecord({
        id: "legacy-created-in-range",
        clientName: "Created In Range",
        createdAt: "2026-02-10T10:00:00.000Z",
        payment1Date: "01/15/2026",
      }),
      makeRecord({
        id: "payment-in-range",
        clientName: "Payment In Range",
        createdAt: "2026-03-10T10:00:00.000Z",
        payment1Date: "02/20/2026",
      }),
    ];

    const filtered = filterRecords(records, {
      search: "",
      status: "all",
      overdueRange: "",
      closedBy: "",
      createdAtRange: { from: "02/01/2026", to: "02/28/2026" },
      paymentDateRange: { from: "", to: "" },
      writtenOffDateRange: { from: "", to: "" },
      fullyPaidDateRange: { from: "", to: "" },
    });

    expect(filtered.map((item) => item.id)).toEqual(["payment-in-range"]);
  });
});
