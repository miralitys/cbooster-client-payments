import type { ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

interface PanelProps {
  title?: ReactNode;
  actions?: ReactNode;
  header?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}

export function Panel({ title, actions, header, children, className, bodyClassName }: PanelProps) {
  return (
    <section className={cx("section", "cb-panel", className)}>
      {header ? (
        <header className="cb-panel__header">{header}</header>
      ) : title || actions ? (
        <header className="cb-panel__header">
          <div className="cb-panel__title-wrap">{title ? <h2 className="section-heading">{title}</h2> : null}</div>
          {actions ? <div className="cb-panel__actions">{actions}</div> : null}
        </header>
      ) : null}
      <div className={cx("cb-panel__body", bodyClassName)}>{children}</div>
    </section>
  );
}
