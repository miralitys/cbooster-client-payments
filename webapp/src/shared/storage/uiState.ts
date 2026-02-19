export interface ClientPaymentsUiState {
  filtersCollapsed: boolean;
  selectedPeriod: string;
  sortKey: string;
  sortDirection: "asc" | "desc";
  tableDensity: "compact" | "comfortable";
}

const UI_STATE_KEY = "cbooster_react_ui_state_v1";

const DEFAULT_UI_STATE: ClientPaymentsUiState = {
  filtersCollapsed: false,
  selectedPeriod: "currentWeek",
  sortKey: "createdAt",
  sortDirection: "desc",
  tableDensity: "compact",
};

export function readClientPaymentsUiState(): ClientPaymentsUiState {
  if (typeof window === "undefined") {
    return DEFAULT_UI_STATE;
  }

  try {
    const raw = window.localStorage.getItem(UI_STATE_KEY);
    if (!raw) {
      return DEFAULT_UI_STATE;
    }

    const parsed = JSON.parse(raw) as Partial<ClientPaymentsUiState>;
    return {
      filtersCollapsed: Boolean(parsed.filtersCollapsed),
      selectedPeriod: normalizePeriod(parsed.selectedPeriod),
      sortKey: typeof parsed.sortKey === "string" ? parsed.sortKey : DEFAULT_UI_STATE.sortKey,
      sortDirection: parsed.sortDirection === "asc" ? "asc" : "desc",
      tableDensity: normalizeDensity(parsed.tableDensity),
    };
  } catch {
    return DEFAULT_UI_STATE;
  }
}

export function writeClientPaymentsUiState(nextState: ClientPaymentsUiState): void {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.setItem(
    UI_STATE_KEY,
    JSON.stringify({
      filtersCollapsed: Boolean(nextState.filtersCollapsed),
      selectedPeriod: normalizePeriod(nextState.selectedPeriod),
      sortKey: nextState.sortKey,
      sortDirection: nextState.sortDirection,
      tableDensity: normalizeDensity(nextState.tableDensity),
    }),
  );
}

function normalizePeriod(value: unknown): string {
  if (
    value === "currentWeek" ||
    value === "previousWeek" ||
    value === "currentMonth" ||
    value === "last30Days"
  ) {
    return value;
  }

  return DEFAULT_UI_STATE.selectedPeriod;
}

function normalizeDensity(value: unknown): "compact" | "comfortable" {
  return value === "comfortable" ? "comfortable" : "compact";
}
