import type { ReactNode } from "react";

interface FieldProps {
  label: string;
  htmlFor?: string;
  hint?: string;
  error?: string;
  children: ReactNode;
}

export function Field({ label, htmlFor, hint, error, children }: FieldProps) {
  return (
    <label className="cb-field" htmlFor={htmlFor}>
      <span className="cb-field__label">{label}</span>
      {children}
      {error ? <span className="cb-field__error">{error}</span> : null}
      {!error && hint ? <span className="cb-field__hint">{hint}</span> : null}
    </label>
  );
}
