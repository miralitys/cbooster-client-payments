import type { ClientRecord } from "@/shared/types/records";
import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { formatDate, formatMoney, parseMoneyValue } from "@/features/client-payments/domain/calculations";

interface RecordDetailsProps {
  record: ClientRecord;
}

export function RecordDetails({ record }: RecordDetailsProps) {
  return (
    <div className="record-details-grid">
      {FIELD_DEFINITIONS.map((field) => {
        const value = record[field.key] || "";
        if (!value && field.type !== "checkbox") {
          return null;
        }

        let displayValue = value;
        if (field.type === "checkbox") {
          displayValue = value ? "Yes" : "No";
        } else if (field.type === "date") {
          displayValue = formatDate(value);
        } else if (field.key === "contractTotals" || field.key === "totalPayments" || field.key === "futurePayments") {
          const amount = parseMoneyValue(value);
          displayValue = amount === null ? value : formatMoney(amount);
        }

        return (
          <div key={field.key} className="record-details-grid__item">
            <span className="record-details-grid__label">{field.label}</span>
            <strong className="record-details-grid__value">{displayValue || "-"}</strong>
          </div>
        );
      })}
    </div>
  );
}
