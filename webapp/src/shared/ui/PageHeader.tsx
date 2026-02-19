import type { ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  meta?: ReactNode;
  className?: string;
}

export function PageHeader({ title, subtitle, actions, meta, className }: PageHeaderProps) {
  return (
    <section className={cx("section", "cb-page-header-panel", className)}>
      <div className="cb-page-header-panel__top">
        <div className="cb-page-header-panel__title-wrap">
          <h2 className="cb-page-header-panel__title">{title}</h2>
          {subtitle ? <p className="cb-page-header-panel__subtitle">{subtitle}</p> : null}
        </div>
        {actions ? <div className="cb-page-header-panel__actions">{actions}</div> : null}
      </div>
      {meta ? <div className="cb-page-header-panel__meta">{meta}</div> : null}
    </section>
  );
}
