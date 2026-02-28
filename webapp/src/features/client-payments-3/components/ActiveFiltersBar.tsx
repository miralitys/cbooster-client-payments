import { Button } from "@/shared/ui";

export interface ActiveFilterChip {
  id: string;
  label: string;
  onRemove: () => void;
}

interface ActiveFiltersBarProps {
  chips: ActiveFilterChip[];
  onClearAll: () => void;
}

export function ActiveFiltersBar({ chips, onClearAll }: ActiveFiltersBarProps) {
  if (!chips.length) {
    return null;
  }

  return (
    <section className="cp3-active-filters" aria-label="Активные фильтры">
      <div className="cp3-active-filters__chips">
        {chips.map((chip) => (
          <button
            key={chip.id}
            type="button"
            className="cp3-chip"
            onClick={chip.onRemove}
            aria-label={`Удалить фильтр: ${chip.label}`}
            title="Удалить фильтр"
          >
            <span>{chip.label}</span>
            <span aria-hidden="true">×</span>
          </button>
        ))}
      </div>
      <Button type="button" variant="ghost" size="sm" onClick={onClearAll}>
        Очистить всё
      </Button>
    </section>
  );
}
