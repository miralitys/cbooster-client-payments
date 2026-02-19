import { useId, useRef } from "react";
import type { ChangeEvent, InputHTMLAttributes } from "react";

interface DateInputProps
  extends Omit<InputHTMLAttributes<HTMLInputElement>, "type" | "value" | "onChange"> {
  value: string;
  onChange: (value: string) => void;
  hasError?: boolean;
}

export function DateInput({
  className = "",
  value,
  onChange,
  hasError = false,
  disabled,
  readOnly,
  ...rest
}: DateInputProps) {
  const dateProxyRef = useRef<HTMLInputElement | null>(null);
  const generatedId = useId();
  const inputId = (rest.id || generatedId).toString();
  const proxyId = `${inputId}-proxy`;

  function onTextInputChange(event: ChangeEvent<HTMLInputElement>) {
    onChange(formatDateInput(event.target.value));
  }

  function onProxyChange(event: ChangeEvent<HTMLInputElement>) {
    onChange(formatIsoDateToUs(event.target.value));
  }

  function openDatePicker() {
    if (disabled || readOnly) {
      return;
    }

    const proxyInput = dateProxyRef.current;
    if (!proxyInput) {
      return;
    }

    proxyInput.focus({ preventScroll: true });
    if (typeof proxyInput.showPicker === "function") {
      proxyInput.showPicker();
      return;
    }

    proxyInput.click();
  }

  return (
    <div className="date-input-shell">
      <button
        type="button"
        className="date-picker-trigger"
        aria-label="Open calendar"
        onClick={openDatePicker}
        disabled={disabled || readOnly}
      >
        <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
          <path d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h1a3 3 0 0 1 3 3v11a4 4 0 0 1-4 4H6a4 4 0 0 1-4-4V7a3 3 0 0 1 3-3h1V3a1 1 0 0 1 1-1Zm13 9H4v7a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7ZM5 6a1 1 0 0 0-1 1v2h16V7a1 1 0 0 0-1-1H5Zm3 8a1 1 0 0 1 1 1v1H8v-1a1 1 0 0 1 1-1Zm4 0a1 1 0 0 1 1 1v1h-1v-1a1 1 0 0 1 1-1Zm4 0a1 1 0 0 1 1 1v1h-1v-1a1 1 0 0 1 1-1Z" />
        </svg>
      </button>

      <input
        {...rest}
        id={inputId}
        type="text"
        inputMode="numeric"
        className={`cb-input ${hasError ? "is-error" : ""} ${className}`.trim()}
        value={formatDateInput(value)}
        onChange={onTextInputChange}
        disabled={disabled}
        readOnly={readOnly}
      />

      <input
        id={proxyId}
        ref={dateProxyRef}
        type="date"
        className="date-picker-proxy"
        value={formatUsDateToIso(value)}
        onChange={onProxyChange}
        disabled={disabled || readOnly}
        tabIndex={-1}
        aria-hidden="true"
      />
    </div>
  );
}

function formatDateInput(rawValue: string): string {
  const digitsOnly = (rawValue || "").replace(/\D/g, "").slice(0, 8);
  if (digitsOnly.length <= 2) {
    return digitsOnly;
  }

  if (digitsOnly.length <= 4) {
    return `${digitsOnly.slice(0, 2)}/${digitsOnly.slice(2)}`;
  }

  return `${digitsOnly.slice(0, 2)}/${digitsOnly.slice(2, 4)}/${digitsOnly.slice(4)}`;
}

function formatIsoDateToUs(rawIsoDate: string): string {
  const match = (rawIsoDate || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) {
    return "";
  }

  return `${match[2]}/${match[3]}/${match[1]}`;
}

function formatUsDateToIso(rawUsDate: string): string {
  const match = (rawUsDate || "").match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) {
    return "";
  }

  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = Number(match[3]);
  if (!isValidDateParts(year, month, day)) {
    return "";
  }

  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function isValidDateParts(year: number, month: number, day: number): boolean {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}
