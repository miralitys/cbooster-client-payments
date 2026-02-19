import type { ReactNode } from "react";

interface CardProps {
  title?: string;
  subtitle?: string;
  className?: string;
  children: ReactNode;
  actions?: ReactNode;
}

export function Card({ title, subtitle, className = "", children, actions }: CardProps) {
  return (
    <section className={`cb-card ${className}`.trim()}>
      {title || subtitle || actions ? (
        <header className="cb-card__header">
          <div>
            {title ? <h2 className="cb-card__title">{title}</h2> : null}
            {subtitle ? <p className="cb-card__subtitle">{subtitle}</p> : null}
          </div>
          {actions ? <div className="cb-card__actions">{actions}</div> : null}
        </header>
      ) : null}
      <div className="cb-card__body">{children}</div>
    </section>
  );
}
