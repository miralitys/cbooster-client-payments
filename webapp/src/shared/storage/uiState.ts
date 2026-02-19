export interface ClientPaymentsUiState {
  filtersCollapsed: boolean;
  selectedPeriod: string;
  sortKey: string;
  sortDirection: "asc" | "desc";
}

const UI_STATE_KEY = "cbooster_react_ui_state_v1";

const DEFAULT_UI_STATE: ClientPaymentsUiState = {
  filtersCollapsed: false,
  selectedPeriod: "currentWeek",
  sortKey: "createdAt",
  sortDirection: "desc",
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
