import type { ReactNode } from "react";

import { cx } from "@/shared/lib/cx";

interface PageHeaderProps {
  title?: string;
  subtitle?: string;
  actions?: ReactNode;
  meta?: ReactNode;
  className?: string;
}

export function PageHeader({ title, subtitle, actions, meta, className }: PageHeaderProps) {
  const hasTitle = Boolean(title || subtitle);
  const hasActions = Boolean(actions);
  const hasTop = hasTitle || hasActions;
  const isCompactMetaRow = !hasTitle && hasActions && Boolean(meta);

  if (!hasTop && !meta) {
    return null;
  }

  return (
    <section className={cx("section", "cb-page-header-panel", className)}>
      {isCompactMetaRow ? (
        <div className="cb-page-header-panel__compact-row">
          <div className="cb-page-header-panel__compact-meta">{meta}</div>
          <div className="cb-page-header-panel__actions">{actions}</div>
        </div>
      ) : (
        <>
          {hasTop ? (
            <div className={cx("cb-page-header-panel__top", !hasTitle && hasActions && "cb-page-header-panel__top--actions-only")}>
              {hasTitle ? (
                <div className="cb-page-header-panel__title-wrap">
                  {title ? <h2 className="cb-page-header-panel__title">{title}</h2> : null}
                  {subtitle ? <p className="cb-page-header-panel__subtitle">{subtitle}</p> : null}
                </div>
              ) : null}
              {actions ? <div className="cb-page-header-panel__actions">{actions}</div> : null}
            </div>
          ) : null}
          {meta ? <div className="cb-page-header-panel__meta">{meta}</div> : null}
        </>
      )}
    </section>
  );
}
