import { describe, expect, it } from "vitest";

import { createEmptyRecord } from "@/features/client-payments/domain/calculations";
import { evaluateClientScore } from "@/features/client-score/domain/scoring";
import type { ClientRecord } from "@/shared/types/records";

function makeRecord(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    contractTotals: "1000",
    payment1: "200",
    payment1Date: "01/10/2026",
    ...patch,
  };
}

describe("evaluateClientScore", () => {
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
});
