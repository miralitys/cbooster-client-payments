import type { TextareaHTMLAttributes } from "react";

interface TextareaProps extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  hasError?: boolean;
}

export function Textarea({ className = "", hasError = false, ...rest }: TextareaProps) {
  return <textarea className={`cb-textarea ${hasError ? "is-error" : ""} ${className}`.trim()} {...rest} />;
}
