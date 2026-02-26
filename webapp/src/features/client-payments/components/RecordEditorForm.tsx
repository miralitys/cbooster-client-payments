import type { ChangeEvent } from "react";

import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { DateInput, Field, Input, Select, Textarea } from "@/shared/ui";
import type { ClientRecord } from "@/shared/types/records";

interface RecordEditorFormProps {
  draft: ClientRecord;
  onChange: (key: keyof ClientRecord, value: string) => void;
}

const ACTIVE_CLIENT_FIELD_KEY: keyof ClientRecord = "active";
const ACTIVE_CLIENT_SELECT_ACTIVE = "active";
const ACTIVE_CLIENT_SELECT_INACTIVE = "inactive";

export function RecordEditorForm({ draft, onChange }: RecordEditorFormProps) {
  function onInputChange(key: keyof ClientRecord, event: ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) {
    const target = event.target;
    if (target instanceof HTMLInputElement && target.type === "checkbox") {
      onChange(key, target.checked ? "Yes" : "");
      return;
    }

    onChange(key, target.value);
  }

  return (
    <div className="record-editor-form">
      {FIELD_DEFINITIONS.filter((field) => !field.computed || field.key === "futurePayments" || field.key === "totalPayments").map((field) => {
        const value = draft[field.key] || "";

        if (field.key === ACTIVE_CLIENT_FIELD_KEY) {
          const normalizedActiveValue = normalizeActiveClientSelectValue(value);
          return (
            <Field key={field.key} label={field.label} htmlFor={`field-${field.key}`}>
              <Select
                id={`field-${field.key}`}
                name={field.key}
                value={normalizedActiveValue}
                onChange={(event) => onChange(field.key, event.target.value === ACTIVE_CLIENT_SELECT_ACTIVE ? "Active" : "Inactive")}
              >
                <option value={ACTIVE_CLIENT_SELECT_ACTIVE}>Active</option>
                <option value={ACTIVE_CLIENT_SELECT_INACTIVE}>Inactive</option>
              </Select>
            </Field>
          );
        }

        if (field.type === "textarea") {
          return (
            <Field key={field.key} label={field.label} htmlFor={`field-${field.key}`}>
              <Textarea
                id={`field-${field.key}`}
                name={field.key}
                value={value}
                onChange={(event) => onInputChange(field.key, event)}
                rows={3}
              />
            </Field>
          );
        }

        if (field.type === "checkbox") {
          const normalizedValue = value.toLowerCase();
          const checked =
            normalizedValue === "yes" ||
            normalizedValue === "true" ||
            normalizedValue === "1" ||
            normalizedValue === "on" ||
            normalizedValue === "completed";
          return (
            <label key={field.key} className="cb-checkbox-row" htmlFor={`field-${field.key}`}>
              <input
                id={`field-${field.key}`}
                name={field.key}
                type="checkbox"
                checked={checked}
                onChange={(event) => onInputChange(field.key, event)}
              />
              <span>{field.label}</span>
            </label>
          );
        }

        if (field.type === "date") {
          return (
            <Field key={field.key} label={field.label} htmlFor={`field-${field.key}`}>
              <DateInput
                id={`field-${field.key}`}
                name={field.key}
                value={value}
                onChange={(nextValue) => onChange(field.key, nextValue)}
                readOnly={Boolean(field.computed)}
                placeholder="MM/DD/YYYY"
              />
            </Field>
          );
        }

        return (
          <Field key={field.key} label={field.label} htmlFor={`field-${field.key}`}>
            <Input
              id={`field-${field.key}`}
              name={field.key}
              type="text"
              value={value}
              onChange={(event) => onInputChange(field.key, event)}
              readOnly={Boolean(field.computed)}
              placeholder=""
            />
          </Field>
        );
      })}
    </div>
  );
}

function normalizeActiveClientSelectValue(rawValue: string): string {
  const normalized = (rawValue || "").toString().trim().toLowerCase();
  if (
    normalized === "yes" ||
    normalized === "true" ||
    normalized === "1" ||
    normalized === "on" ||
    normalized === "active"
  ) {
    return ACTIVE_CLIENT_SELECT_ACTIVE;
  }

  return ACTIVE_CLIENT_SELECT_INACTIVE;
}
