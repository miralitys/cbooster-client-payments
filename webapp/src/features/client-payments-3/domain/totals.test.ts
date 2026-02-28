import { describe, expect, it } from "vitest";

import { createEmptyRecord } from "@/features/client-payments/domain/calculations";
import { calculateTotalsByFieldCents, getDefaultTotalsKeys } from "@/features/client-payments-3/domain/totals";
import type { ClientRecord } from "@/shared/types/records";

function record(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    ...patch,
  };
}

describe("client-payments-3 totals", () => {
  it("keeps totals finite and correct on mixed fixtures", () => {
    const rows = [
      record({
        contractTotals: "$1,000.00",
        totalPayments: "$250.00",
        futurePayments: "$750.00",
        collection: "$10.25",
      }),
      record({
        contractTotals: "200.25",
        totalPayments: "50",
        futurePayments: "150.25",
        collection: "",
      }),
      record({
        contractTotals: "bad-data",
        totalPayments: "(25.10)",
        futurePayments: "not-a-money",
        collection: "1e500",
      }),
    ];

    const result = calculateTotalsByFieldCents(rows, getDefaultTotalsKeys());

    expect(result.totalsCents.contractTotals).toBe(120025);
    expect(result.totalsCents.totalPayments).toBe(27490);
    expect(result.totalsCents.futurePayments).toBe(90025);
    expect(result.totalsCents.collection).toBe(1025);
    expect(result.invalidFieldsCount).toBe(3);

    for (const value of Object.values(result.totalsCents)) {
      expect(Number.isFinite(value)).toBe(true);
      expect(Number.isNaN(value)).toBe(false);
    }
  });
});
