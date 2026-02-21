export type CustomDashboardWidgetKey =
  | "managerTasks"
  | "specialistTasks"
  | "salesReport"
  | "callsByManager";

export interface CustomDashboardWidgetVisibility {
  enabled: boolean;
  visibleNames: string[];
}

export interface CustomDashboardWidgetSettings {
  managerTasks: CustomDashboardWidgetVisibility;
  specialistTasks: CustomDashboardWidgetVisibility;
  salesReport: CustomDashboardWidgetVisibility;
  callsByManager: CustomDashboardWidgetVisibility;
}

export interface CustomDashboardUploadMeta {
  type: "tasks" | "contacts" | "calls";
  uploadedAt: string;
  uploadedBy: string;
  fileName: string;
  count: number;
  archiveKey: string;
}

export type CustomDashboardTasksSourceKind = "upload" | "ghl";

export interface CustomDashboardTasksSourceState {
  selected: CustomDashboardTasksSourceKind;
  options: CustomDashboardTasksSourceKind[];
  ghlConfigured: boolean;
  autoSyncEnabled: boolean;
  syncInFlight: boolean;
  lastAttemptedAt: string;
  lastSyncedAt: string;
  lastMode: "" | "delta" | "full";
  lastError: string;
  cursorUpdatedAt: string;
  stats: {
    contactsTotal: number;
    contactsProcessed: number;
    contactsDeleted: number;
    tasksTotal: number;
  };
}

export interface CustomDashboardManagerTaskRow {
  managerName: string;
  open: number;
  overdue: number;
  dueToday: number;
  oldestOverdueDays: number;
  completedYesterday: number;
}

export interface CustomDashboardManagerTasksWidget {
  enabled: boolean;
  visibleNames: string[];
  totals: {
    managers: number;
    tasks: number;
    open: number;
    overdue: number;
    dueToday: number;
    completedYesterday: number;
  };
  rows: CustomDashboardManagerTaskRow[];
}

export interface CustomDashboardTaskItem {
  id: string;
  title: string;
  managerName: string;
  specialistName: string;
  clientName: string;
  status: string;
  dueDate: string;
  createdAt: string;
  completedAt: string;
  isCompleted: boolean;
  isOverdue: boolean;
  isDueToday: boolean;
}

export interface CustomDashboardSpecialistTasksWidget {
  enabled: boolean;
  visibleNames: string[];
  specialistOptions: string[];
  selectedSpecialist: string;
  totals: {
    all: number;
    open: number;
    overdue: number;
    dueToday: number;
  };
  allTasks: CustomDashboardTaskItem[];
  overdueTasks: CustomDashboardTaskItem[];
  dueTodayTasks: CustomDashboardTaskItem[];
}

export interface CustomDashboardSalesMetrics {
  calls: number;
  answers: number;
  talks: number;
  interested: number;
  closedDeals: number;
  closedAmount: number;
}

export interface CustomDashboardSalesManagerRow extends CustomDashboardSalesMetrics {
  managerName: string;
}

export interface CustomDashboardSalesReportWidget {
  enabled: boolean;
  visibleNames: string[];
  periods: {
    today: CustomDashboardSalesMetrics;
    yesterday: CustomDashboardSalesMetrics;
    currentWeek: CustomDashboardSalesMetrics;
    currentMonth: CustomDashboardSalesMetrics;
  };
  managerBreakdown: CustomDashboardSalesManagerRow[];
}

export interface CustomDashboardCallsStatsRow {
  managerName: string;
  totalCalls: number;
  acceptedCalls: number;
  over30Sec: number;
}

export interface CustomDashboardMissedCallRow {
  id: string;
  managerName: string;
  clientName: string;
  phone: string;
  callAt: string;
  status: string;
  calledBack: boolean;
}

export interface CustomDashboardCallsWidget {
  enabled: boolean;
  visibleNames: string[];
  managerOptions: string[];
  stats: CustomDashboardCallsStatsRow[];
  missedCalls: CustomDashboardMissedCallRow[];
}

export interface CustomDashboardPayload {
  ok: boolean;
  moduleRole: "admin" | "user";
  canManage: boolean;
  activeUser: {
    username: string;
    displayName: string;
    isOwner: boolean;
  };
  widgets: CustomDashboardWidgetSettings;
  uploads: {
    tasks: CustomDashboardUploadMeta;
    tasksGhl: CustomDashboardUploadMeta;
    contacts: CustomDashboardUploadMeta;
    calls: CustomDashboardUploadMeta;
  };
  tasksSource: CustomDashboardTasksSourceState;
  options: {
    managerTasks: string[];
    specialistTasks: string[];
    salesReport: string[];
    callsByManager: string[];
  };
  managerTasks: CustomDashboardManagerTasksWidget;
  specialistTasks: CustomDashboardSpecialistTasksWidget;
  salesReport: CustomDashboardSalesReportWidget;
  callsByManager: CustomDashboardCallsWidget;
}

export interface CustomDashboardUserSettingsEntry {
  username: string;
  displayName: string;
  isOwner: boolean;
  moduleRole: "admin" | "user";
  widgets: CustomDashboardWidgetSettings;
}

export interface CustomDashboardUsersPayload {
  ok: boolean;
  users: CustomDashboardUserSettingsEntry[];
  options: {
    managerTasks: string[];
    specialistTasks: string[];
    salesReport: string[];
    callsByManager: string[];
  };
  tasksSource: CustomDashboardTasksSourceState;
  updatedAt?: string;
}

export interface CustomDashboardUsersSavePayload {
  users: Array<{
    username: string;
    widgets: CustomDashboardWidgetSettings;
  }>;
}

export interface CustomDashboardUploadResponse {
  ok: boolean;
  type: "tasks" | "contacts" | "calls";
  count: number;
  archiveKey: string;
  uploadedAt: string;
}

export interface CustomDashboardTasksSourceUpdateResponse {
  ok: boolean;
  tasksSource: CustomDashboardTasksSourceState;
}

export interface CustomDashboardTasksSyncResponse {
  ok: boolean;
  mode: "delta" | "full";
  uploadedAt: string;
  count: number;
  archiveKey: string;
  stats: {
    contactsTotal: number;
    contactsProcessed: number;
    contactsDeleted: number;
    tasksTotal: number;
  };
  tasksSource: CustomDashboardTasksSourceState;
}

export interface CustomDashboardTaskMovementRow {
  taskId: string;
  title: string;
  managerName: string;
  clientName: string;
  contactId: string;
  status: string;
  isCompleted: boolean;
  changeType: "created" | "completed" | "updated";
  createdAt: string;
  updatedAt: string;
  completedAt: string;
}

export interface CustomDashboardTaskMovementManagerRow {
  managerName: string;
  changed: number;
  created: number;
  completed: number;
  updated: number;
}

export interface CustomDashboardTaskMovementsResponse {
  ok: boolean;
  generatedAt: string;
  periodHours: number;
  since: string;
  scannedTasks: number;
  changedTasks: number;
  createdTasks: number;
  completedTasks: number;
  updatedTasks: number;
  totalPages: number;
  managers: number;
  contacts: number;
  rowsReturned: number;
  rowLimit: number;
  truncatedRows: boolean;
  rows: CustomDashboardTaskMovementRow[];
  managerSummary: CustomDashboardTaskMovementManagerRow[];
}
