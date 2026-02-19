import type { ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

interface PageShellProps {
  children: ReactNode;
  className?: string;
}

export function PageShell({ children, className }: PageShellProps) {
  return <div className={cx("cb-page-shell", className)}>{children}</div>;
}
