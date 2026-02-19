interface SegmentedOption {
  key: string;
  label: string;
}

interface SegmentedControlProps {
  value: string;
  options: SegmentedOption[];
  onChange: (value: string) => void;
}

export function SegmentedControl({ value, options, onChange }: SegmentedControlProps) {
  return (
    <div className="cb-segmented" role="group">
      {options.map((option) => {
        const isActive = option.key === value;
        return (
          <button
            key={option.key}
            type="button"
            className={`cb-segmented__item ${isActive ? "is-active" : ""}`.trim()}
            onClick={() => onChange(option.key)}
            aria-pressed={isActive}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}
