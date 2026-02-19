import type { SelectHTMLAttributes } from "react";

interface SelectProps extends SelectHTMLAttributes<HTMLSelectElement> {
  hasError?: boolean;
}

export function Select({ className = "", hasError = false, children, ...rest }: SelectProps) {
  return (
    <select className={`cb-select ${hasError ? "is-error" : ""} ${className}`.trim()} {...rest}>
      {children}
    </select>
  );
}
