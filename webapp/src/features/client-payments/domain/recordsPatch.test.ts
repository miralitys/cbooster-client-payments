import { describe, expect, it } from "vitest";

import { ApiError } from "@/shared/api/fetcher";
import { createEmptyRecord } from "@/features/client-payments/domain/calculations";
import {
  buildRecordsPatchOperations,
  shouldFallbackToPutFromPatch,
} from "@/features/client-payments/domain/recordsPatch";
import type { ClientRecord } from "@/shared/types/records";

function makeRecord(patch: Partial<ClientRecord>): ClientRecord {
  return {
    ...createEmptyRecord(),
    ...patch,
  };
}

describe("recordsPatch", () => {
  it("builds upsert/delete operations for create-edit-delete delta", () => {
    const previousRecords = [
      makeRecord({
        id: "alpha",
        createdAt: "2026-01-01T00:00:00.000Z",
        clientName: "Alpha",
        companyName: "Old Co",
      }),
      makeRecord({
        id: "beta",
        createdAt: "2026-01-02T00:00:00.000Z",
        clientName: "Beta",
      }),
    ];

    const nextRecords = [
      makeRecord({
        id: "alpha",
        createdAt: "2026-01-01T00:00:00.000Z",
        clientName: "Alpha",
        companyName: "New Co",
      }),
      makeRecord({
        id: "gamma",
        createdAt: "2026-01-03T00:00:00.000Z",
        clientName: "Gamma",
      }),
    ];

    const operations = buildRecordsPatchOperations(previousRecords, nextRecords);

    expect(operations).toEqual([
      {
        type: "upsert",
        id: "alpha",
        record: {
          companyName: "New Co",
        },
      },
      {
        type: "upsert",
        id: "gamma",
        record: nextRecords[1],
      },
      {
        type: "delete",
        id: "beta",
      },
    ]);
  });

  it("returns empty delta for identical records", () => {
    const records = [
      makeRecord({
        id: "same",
        createdAt: "2026-01-01T00:00:00.000Z",
        clientName: "No Change",
      }),
    ];

    expect(buildRecordsPatchOperations(records, records.map((item) => ({ ...item })))).toEqual([]);
  });

  it("detects unsupported patch errors for PUT fallback", () => {
    expect(shouldFallbackToPutFromPatch(new ApiError("API route not found", 404, "http_error"))).toBe(true);
    expect(shouldFallbackToPutFromPatch(new ApiError("Patch is disabled", 400, "records_patch_disabled"))).toBe(
      true,
    );
    expect(shouldFallbackToPutFromPatch(new ApiError("Conflict", 409, "records_conflict"))).toBe(false);
    expect(shouldFallbackToPutFromPatch(new Error("network"))).toBe(false);
  });
});
