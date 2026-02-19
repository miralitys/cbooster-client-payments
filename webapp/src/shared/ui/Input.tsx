import type { InputHTMLAttributes } from "react";

interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  hasError?: boolean;
}

export function Input({ className = "", hasError = false, ...rest }: InputProps) {
  return <input className={`cb-input ${hasError ? "is-error" : ""} ${className}`.trim()} {...rest} />;
}
