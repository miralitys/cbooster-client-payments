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
    contacts: CustomDashboardUploadMeta;
    calls: CustomDashboardUploadMeta;
  };
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
