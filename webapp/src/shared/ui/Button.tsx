import type { ButtonHTMLAttributes, ReactNode } from "react";

type ButtonVariant = "primary" | "secondary" | "danger" | "ghost";
type ButtonSize = "sm" | "md";

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  isLoading?: boolean;
  leadingIcon?: ReactNode;
}

export function Button({
  children,
  className = "",
  variant = "primary",
  size = "md",
  isLoading = false,
  leadingIcon,
  disabled,
  ...rest
}: ButtonProps) {
  const classes = [
    "cb-button",
    `cb-button--${variant}`,
    `cb-button--${size}`,
    className,
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <button className={classes} disabled={disabled || isLoading} {...rest}>
      {leadingIcon ? <span className="cb-button__icon">{leadingIcon}</span> : null}
      <span>{isLoading ? "Loading..." : children}</span>
    </button>
  );
}
