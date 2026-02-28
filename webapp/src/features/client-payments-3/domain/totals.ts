import type { ClientRecord } from "@/shared/types/records";

import { parseMoneyToCents } from "@/features/client-payments-3/domain/money";

export interface TotalsByFieldResult {
  totalsCents: Record<string, number>;
  invalidFieldsCount: number;
}

export function calculateTotalsByFieldCents(
  records: ClientRecord[],
  fieldKeys: string[],
): TotalsByFieldResult {
  const totalsCents: Record<string, number> = {};
  let invalidFieldsCount = 0;

  for (const fieldKey of fieldKeys) {
    totalsCents[fieldKey] = 0;
  }

  for (const record of Array.isArray(records) ? records : []) {
    if (!record || typeof record !== "object") {
      continue;
    }

    for (const fieldKey of fieldKeys) {
      const rawValue = record[fieldKey as keyof ClientRecord];
      const normalizedText = String(rawValue ?? "").trim();
      if (!normalizedText) {
        continue;
      }

      const cents = parseMoneyToCents(rawValue);
      if (cents === null) {
        invalidFieldsCount += 1;
        continue;
      }

      totalsCents[fieldKey] += cents;
    }
  }

  return {
    totalsCents,
    invalidFieldsCount,
  };
}

export function getDefaultTotalsKeys(): string[] {
  return ["contractTotals", "totalPayments", "futurePayments", "collection"];
}
