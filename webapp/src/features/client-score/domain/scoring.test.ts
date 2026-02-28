import { describe, expect, it } from "vitest";

import { createEmptyRecord } from "@/features/client-payments/domain/calculations";
import { computeLegacyPaymentProbabilities, evaluateClientScore } from "@/features/client-score/domain/scoring";
import type { ClientRecord } from "@/shared/types/records";

function makeRecord(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    active: "Active",
    contractTotals: "1000",
    payment1: "200",
    payment1Date: "01/10/2026",
    ...patch,
  };
}

describe("evaluateClientScore", () => {
  it("returns no score for inactive clients", () => {
    const result = evaluateClientScore(
      makeRecord({
        active: "No",
      }),
      new Date("2026-02-19T12:00:00Z"),
    );

    expect(result.score).toBeNull();
    expect(result.displayScore).toBeNull();
    expect(result.explanation).toContain("Inactive");
  });

  it("returns no score for written off clients", () => {
    const result = evaluateClientScore(
      makeRecord({
        writtenOff: "Yes",
      }),
      new Date("2026-02-19T12:00:00Z"),
    );

    expect(result.score).toBeNull();
    expect(result.displayScore).toBeNull();
    expect(result.explanation).toContain("Written Off");
  });

  it("returns no score for after result clients", () => {
    const result = evaluateClientScore(
      makeRecord({
        afterResult: "Yes",
      }),
      new Date("2026-02-19T12:00:00Z"),
    );

    expect(result.score).toBeNull();
    expect(result.displayScore).toBeNull();
    expect(result.explanation).toContain("After Result");
  });

  it("returns no score for fully paid clients", () => {
    const result = evaluateClientScore(
      makeRecord({
        payment1: "1000",
      }),
      new Date("2026-02-19T12:00:00Z"),
    );

    expect(result.score).toBeNull();
    expect(result.displayScore).toBeNull();
    expect(result.explanation).toContain("Fully Paid");
  });
});

describe("computeLegacyPaymentProbabilities", () => {
  it("returns 0% across months when score is 0", () => {
    const result = computeLegacyPaymentProbabilities({
      contractTotal: 1000,
      paidTotal: 100,
      paidRatio: 0.1,
      paymentPace: 0.1,
      displayScore: 0,
      overdueDays: 0,
      openMilestones: 0,
      futurePayments: 900,
      writtenOff: false,
      balance: 900,
    });

    expect(result).toEqual({ p1: 0, p2: 0, p3: 0 });
  });
});
