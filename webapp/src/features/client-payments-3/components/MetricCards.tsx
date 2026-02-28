import { formatMoneyFromCents } from "@/features/client-payments-3/domain/money";
import type { OverviewPeriodKey } from "@/features/client-payments/domain/constants";
import { Select } from "@/shared/ui";

interface OverviewPeriodOption {
  key: OverviewPeriodKey;
  label: string;
}

interface MetricCardsProps {
  periodLabel: string;
  period: OverviewPeriodKey;
  periodOptions: OverviewPeriodOption[];
  salesCents: number;
  receivedCents: number;
  debtCents: number;
  totalsSourceLabel: string;
  totalsInvalidFieldsCount: number;
  totalsRowsCount: number;
  onPeriodChange: (period: OverviewPeriodKey) => void;
}

export function MetricCards({
  periodLabel,
  period,
  periodOptions,
  salesCents,
  receivedCents,
  debtCents,
  totalsSourceLabel,
  totalsInvalidFieldsCount,
  totalsRowsCount,
  onPeriodChange,
}: MetricCardsProps) {
  return (
    <section className="cp3-metrics" aria-label="Обзор метрик">
      <article className="cp3-card" aria-label="Продажи">
        <p className="cp3-card__title">Продажи</p>
        <p className="cp3-card__value">{formatMoneyFromCents(salesCents)}</p>
        <p className="cp3-card__meta">{periodLabel}</p>
      </article>
      <article className="cp3-card" aria-label="Получено">
        <p className="cp3-card__title">Получено</p>
        <p className="cp3-card__value">{formatMoneyFromCents(receivedCents)}</p>
        <p className="cp3-card__meta">{periodLabel}</p>
      </article>
      <article className="cp3-card" aria-label="Долг">
        <p className="cp3-card__title">Долг</p>
        <p className="cp3-card__value">{formatMoneyFromCents(debtCents)}</p>
        <p className="cp3-card__meta">На сегодня</p>
      </article>

      <article className="cp3-card cp3-card--wide" aria-label="Источник totals">
        <div className="cp3-card__header-row">
          <p className="cp3-card__title">Итого по выборке</p>
          <label className="cp3-card__period">
            <Select
              value={period}
              onChange={(event) => onPeriodChange((event.target.value as OverviewPeriodKey) || "currentWeek")}
              aria-label="Период метрик"
            >
              {periodOptions.map((option) => (
                <option key={option.key} value={option.key}>
                  {option.label}
                </option>
              ))}
            </Select>
          </label>
        </div>
        <p className="cp3-card__meta">Источник: {totalsSourceLabel}</p>
        <p className="cp3-card__meta">Строк в выборке: {totalsRowsCount}</p>
        {totalsInvalidFieldsCount > 0 ? (
          <p className="cp3-card__warning" role="status">
            Поля с некорректными суммами: {totalsInvalidFieldsCount}
          </p>
        ) : null}
      </article>
    </section>
  );
}
