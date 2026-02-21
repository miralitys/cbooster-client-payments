import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";

import {
  getCustomDashboard,
  getCustomDashboardUsers,
  saveCustomDashboardUsers,
  uploadCustomDashboardFile,
} from "@/shared/api";
import { getCustomDashboardTaskMovements } from "@/shared/api/customDashboard";
import { showToast } from "@/shared/lib/toast";
import type {
  CustomDashboardCallsStatsRow,
  CustomDashboardMissedCallRow,
  CustomDashboardPayload,
  CustomDashboardSalesManagerRow,
  CustomDashboardSalesMetrics,
  CustomDashboardTaskMovementManagerRow,
  CustomDashboardTaskMovementsResponse,
  CustomDashboardTaskMovementRow,
  CustomDashboardTaskItem,
  CustomDashboardUploadMeta,
  CustomDashboardUserSettingsEntry,
  CustomDashboardUsersPayload,
  CustomDashboardWidgetKey,
  CustomDashboardWidgetSettings,
} from "@/shared/types/customDashboard";
import {
  Badge,
  Button,
  EmptyState,
  ErrorState,
  LoadingSkeleton,
  PageHeader,
  PageShell,
  Panel,
  Select,
  SegmentedControl,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const TASK_VIEW_OPTIONS = [
  { key: "totals", label: "Totals" },
  { key: "all", label: "All Tasks" },
  { key: "overdue", label: "Overdue" },
  { key: "dueToday", label: "Due Today" },
] as const;

const CALLS_VIEW_OPTIONS = [
  { key: "stats", label: "Statistics" },
  { key: "missed", label: "Missed Calls" },
] as const;

const SALES_PERIOD_LABELS: Record<keyof CustomDashboardPayload["salesReport"]["periods"], string> = {
  today: "Today",
  yesterday: "Yesterday",
  currentWeek: "This Week",
  currentMonth: "This Month",
};

const WIDGET_LABELS: Record<CustomDashboardWidgetKey, string> = {
  managerTasks: "Manager Tasks",
  specialistTasks: "Specialist Tasks",
  salesReport: "Sales Report",
  callsByManager: "Calls by Manager",
};

type SettingsTab = "dashboard" | "settings";
type TasksViewKey = (typeof TASK_VIEW_OPTIONS)[number]["key"];
type CallsViewKey = (typeof CALLS_VIEW_OPTIONS)[number]["key"];
type UploadType = "contacts" | "calls";

export default function CustomDashboardPage() {
  const location = useLocation();
  const navigate = useNavigate();

  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [dashboard, setDashboard] = useState<CustomDashboardPayload | null>(null);

  const [tasksView, setTasksView] = useState<TasksViewKey>("totals");
  const [callsView, setCallsView] = useState<CallsViewKey>("stats");
  const [selectedSpecialist, setSelectedSpecialist] = useState("");
  const [selectedCallsManager, setSelectedCallsManager] = useState("");

  const [uploadingType, setUploadingType] = useState<UploadType | "">("");
  const [taskMovements, setTaskMovements] = useState<CustomDashboardTaskMovementsResponse | null>(null);
  const [taskMovementsLoading, setTaskMovementsLoading] = useState(false);
  const [taskMovementsError, setTaskMovementsError] = useState("");
  const [selectedTaskMovementManager, setSelectedTaskMovementManager] = useState("");
  const taskMovementsLoadedRef = useRef(false);
  const contactsFileInputRef = useRef<HTMLInputElement | null>(null);
  const callsFileInputRef = useRef<HTMLInputElement | null>(null);

  const [usersLoading, setUsersLoading] = useState(false);
  const [usersError, setUsersError] = useState("");
  const [usersPayload, setUsersPayload] = useState<CustomDashboardUsersPayload | null>(null);
  const [usersDraft, setUsersDraft] = useState<Record<string, CustomDashboardWidgetSettings>>({});
  const [selectedUser, setSelectedUser] = useState("");
  const [isSavingUsers, setIsSavingUsers] = useState(false);

  const canManage = Boolean(dashboard?.canManage);

  const activeTab = useMemo<SettingsTab>(() => {
    const query = new URLSearchParams(location.search);
    const tab = (query.get("tab") || "").toLowerCase();
    if (canManage && tab === "settings") {
      return "settings";
    }
    return "dashboard";
  }, [canManage, location.search]);

  const setTab = useCallback(
    (nextTab: SettingsTab) => {
      const query = new URLSearchParams(location.search);
      if (nextTab === "settings") {
        query.set("tab", "settings");
      } else {
        query.delete("tab");
      }

      const nextSearch = query.toString();
      navigate(
        {
          pathname: location.pathname,
          search: nextSearch ? `?${nextSearch}` : "",
        },
        { replace: false },
      );
    },
    [location.pathname, location.search, navigate],
  );

  const loadDashboard = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const payload = await getCustomDashboard();
      setDashboard(payload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load custom dashboard.";
      setLoadError(message);
      setDashboard(null);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const loadUsersSettings = useCallback(async () => {
    if (!canManage) {
      return;
    }

    setUsersLoading(true);
    setUsersError("");
    try {
      const payload = await getCustomDashboardUsers();
      setUsersPayload(payload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load user settings.";
      setUsersError(message);
      setUsersPayload(null);
    } finally {
      setUsersLoading(false);
    }
  }, [canManage]);

  useEffect(() => {
    void loadDashboard();
  }, [loadDashboard]);

  useEffect(() => {
    if (activeTab === "settings" && canManage) {
      void loadUsersSettings();
    }
  }, [activeTab, canManage, loadUsersSettings]);

  useEffect(() => {
    const options = dashboard?.specialistTasks?.specialistOptions || [];
    const fallback = dashboard?.specialistTasks?.selectedSpecialist || options[0] || "";
    if (!options.length) {
      setSelectedSpecialist("");
      return;
    }

    setSelectedSpecialist((previous) => {
      if (previous && options.includes(previous)) {
        return previous;
      }
      return fallback;
    });
  }, [dashboard?.specialistTasks?.selectedSpecialist, dashboard?.specialistTasks?.specialistOptions]);

  useEffect(() => {
    const managerOptions = dashboard?.callsByManager?.managerOptions || [];
    if (!managerOptions.length) {
      setSelectedCallsManager("");
      return;
    }

    setSelectedCallsManager((previous) => {
      if (!previous) {
        return "";
      }
      if (managerOptions.includes(previous)) {
        return previous;
      }
      return "";
    });
  }, [dashboard?.callsByManager?.managerOptions]);

  useEffect(() => {
    const managerOptions = taskMovements?.managerSummary?.map((row) => row.managerName) || [];
    if (!managerOptions.length) {
      setSelectedTaskMovementManager("");
      return;
    }

    setSelectedTaskMovementManager((previous) => {
      if (!previous) {
        return "";
      }
      if (managerOptions.includes(previous)) {
        return previous;
      }
      return "";
    });
  }, [taskMovements?.managerSummary]);

  useEffect(() => {
    const users = usersPayload?.users || [];
    if (!users.length) {
      setUsersDraft({});
      setSelectedUser("");
      return;
    }

    const nextDraft: Record<string, CustomDashboardWidgetSettings> = {};
    for (const user of users) {
      nextDraft[user.username] = cloneWidgetSettings(user.widgets);
    }

    setUsersDraft(nextDraft);
    setSelectedUser((previous) => {
      if (previous && users.some((user) => user.username === previous)) {
        return previous;
      }
      return users[0]?.username || "";
    });
  }, [usersPayload]);

  const specialistTasksScoped = useMemo(() => {
    const allTasks = dashboard?.specialistTasks?.allTasks || [];
    if (!selectedSpecialist) {
      return allTasks;
    }

    return allTasks.filter(
      (task) => normalizeComparable(task.specialistName) === normalizeComparable(selectedSpecialist),
    );
  }, [dashboard?.specialistTasks?.allTasks, selectedSpecialist]);

  const specialistOverdueTasks = useMemo(
    () => specialistTasksScoped.filter((task) => task.isOverdue),
    [specialistTasksScoped],
  );

  const specialistDueTodayTasks = useMemo(
    () => specialistTasksScoped.filter((task) => task.isDueToday),
    [specialistTasksScoped],
  );

  const specialistTotals = useMemo(
    () => ({
      all: specialistTasksScoped.length,
      open: specialistTasksScoped.filter((task) => !task.isCompleted).length,
      overdue: specialistOverdueTasks.length,
      dueToday: specialistDueTodayTasks.length,
    }),
    [specialistDueTodayTasks.length, specialistOverdueTasks.length, specialistTasksScoped],
  );

  const callsStatsRows = useMemo(() => {
    const rows = dashboard?.callsByManager?.stats || [];
    if (!selectedCallsManager) {
      return rows;
    }

    return rows.filter(
      (row) => normalizeComparable(row.managerName) === normalizeComparable(selectedCallsManager),
    );
  }, [dashboard?.callsByManager?.stats, selectedCallsManager]);

  const missedCallsRows = useMemo(() => {
    const rows = dashboard?.callsByManager?.missedCalls || [];
    if (!selectedCallsManager) {
      return rows;
    }

    return rows.filter(
      (row) => normalizeComparable(row.managerName) === normalizeComparable(selectedCallsManager),
    );
  }, [dashboard?.callsByManager?.missedCalls, selectedCallsManager]);

  const taskMovementsManagerOptions = useMemo(
    () => taskMovements?.managerSummary.map((row) => row.managerName) || [],
    [taskMovements?.managerSummary],
  );

  const taskMovementsRows = useMemo(() => {
    const rows = taskMovements?.rows || [];
    if (!selectedTaskMovementManager) {
      return rows;
    }

    return rows.filter(
      (row) => normalizeComparable(row.managerName) === normalizeComparable(selectedTaskMovementManager),
    );
  }, [selectedTaskMovementManager, taskMovements?.rows]);

  const managerTasksColumns = useMemo<TableColumn<CustomDashboardPayload["managerTasks"]["rows"][number]>[]>(
    () => [
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName,
      },
      {
        key: "open",
        label: "Open",
        align: "center",
        cell: (row) => String(row.open),
      },
      {
        key: "overdue",
        label: "Overdue",
        align: "center",
        cell: (row) => String(row.overdue),
      },
      {
        key: "dueToday",
        label: "Due Today",
        align: "center",
        cell: (row) => String(row.dueToday),
      },
      {
        key: "oldestOverdueDays",
        label: "Oldest Overdue (days)",
        align: "center",
        cell: (row) => String(row.oldestOverdueDays),
      },
      {
        key: "completedYesterday",
        label: "Completed Yesterday",
        align: "center",
        cell: (row) => String(row.completedYesterday),
      },
    ],
    [],
  );

  const specialistTaskColumns = useMemo<TableColumn<CustomDashboardTaskItem>[]>(
    () => [
      {
        key: "title",
        label: "Task",
        align: "left",
        cell: (task) => (
          <div className="custom-dashboard-task-title">
            <strong>{task.title || "Task"}</strong>
            <span>{task.clientName || "-"}</span>
          </div>
        ),
      },
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (task) => task.managerName || "-",
      },
      {
        key: "status",
        label: "Status",
        align: "center",
        cell: (task) => {
          const tone: "success" | "warning" | "danger" | "neutral" = task.isCompleted
            ? "success"
            : task.isOverdue
              ? "danger"
              : task.isDueToday
                ? "warning"
                : "neutral";

          const text = task.isCompleted
            ? "Completed"
            : task.isOverdue
              ? "Overdue"
              : task.isDueToday
                ? "Due Today"
                : task.status || "Open";

          return <Badge tone={tone}>{text}</Badge>;
        },
      },
      {
        key: "dueDate",
        label: "Due Date",
        align: "center",
        cell: (task) => formatDateOrDash(task.dueDate),
      },
      {
        key: "completedAt",
        label: "Completed At",
        align: "center",
        cell: (task) => formatDateTimeOrDash(task.completedAt),
      },
    ],
    [],
  );

  const salesColumns = useMemo<TableColumn<CustomDashboardSalesManagerRow>[]>(
    () => [
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName,
      },
      {
        key: "calls",
        label: "Calls",
        align: "center",
        cell: (row) => String(row.calls),
      },
      {
        key: "answers",
        label: "Answers",
        align: "center",
        cell: (row) => String(row.answers),
      },
      {
        key: "talks",
        label: "Talks",
        align: "center",
        cell: (row) => String(row.talks),
      },
      {
        key: "interested",
        label: "Interested",
        align: "center",
        cell: (row) => String(row.interested),
      },
      {
        key: "closedDeals",
        label: "Closed Deals",
        align: "center",
        cell: (row) => String(row.closedDeals),
      },
      {
        key: "closedAmount",
        label: "Closed Amount",
        align: "right",
        cell: (row) => formatMoney(row.closedAmount),
      },
    ],
    [],
  );

  const callsStatsColumns = useMemo<TableColumn<CustomDashboardCallsStatsRow>[]>(
    () => [
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName,
      },
      {
        key: "totalCalls",
        label: "Total Calls",
        align: "center",
        cell: (row) => String(row.totalCalls),
      },
      {
        key: "acceptedCalls",
        label: "Accepted",
        align: "center",
        cell: (row) => String(row.acceptedCalls),
      },
      {
        key: "over30Sec",
        label: "> 30 sec",
        align: "center",
        cell: (row) => String(row.over30Sec),
      },
    ],
    [],
  );

  const missedCallsColumns = useMemo<TableColumn<CustomDashboardMissedCallRow>[]>(
    () => [
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName,
      },
      {
        key: "clientName",
        label: "Client",
        align: "left",
        cell: (row) => row.clientName || "-",
      },
      {
        key: "phone",
        label: "Phone",
        align: "left",
        cell: (row) => row.phone || "-",
      },
      {
        key: "callAt",
        label: "Date / Time",
        align: "center",
        cell: (row) => formatDateTimeOrDash(row.callAt),
      },
      {
        key: "status",
        label: "Status",
        align: "center",
        cell: (row) => row.status || "missed",
      },
      {
        key: "calledBack",
        label: "Called Back",
        align: "center",
        cell: (row) => <Badge tone={row.calledBack ? "success" : "danger"}>{row.calledBack ? "Yes" : "No"}</Badge>,
      },
    ],
    [],
  );

  const taskMovementsManagerColumns = useMemo<TableColumn<CustomDashboardTaskMovementManagerRow>[]>(
    () => [
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName,
      },
      {
        key: "changed",
        label: "Changed",
        align: "center",
        cell: (row) => String(row.changed),
      },
      {
        key: "created",
        label: "Created",
        align: "center",
        cell: (row) => String(row.created),
      },
      {
        key: "completed",
        label: "Completed",
        align: "center",
        cell: (row) => String(row.completed),
      },
      {
        key: "updated",
        label: "Updated",
        align: "center",
        cell: (row) => String(row.updated),
      },
    ],
    [],
  );

  const taskMovementsColumns = useMemo<TableColumn<CustomDashboardTaskMovementRow>[]>(
    () => [
      {
        key: "updatedAt",
        label: "Updated At",
        align: "center",
        cell: (row) => formatDateTimeOrDash(row.updatedAt),
      },
      {
        key: "managerName",
        label: "Manager",
        align: "left",
        cell: (row) => row.managerName || "Unassigned",
      },
      {
        key: "clientName",
        label: "Client",
        align: "left",
        cell: (row) => row.clientName || row.contactId || "-",
      },
      {
        key: "title",
        label: "Task",
        align: "left",
        cell: (row) => row.title || "Task",
      },
      {
        key: "changeType",
        label: "Change",
        align: "center",
        cell: (row) => {
          const tone: "success" | "warning" | "danger" | "neutral" =
            row.changeType === "created" ? "success" : row.changeType === "completed" ? "warning" : "neutral";
          const label =
            row.changeType === "created" ? "Created" : row.changeType === "completed" ? "Completed" : "Updated";
          return <Badge tone={tone}>{label}</Badge>;
        },
      },
      {
        key: "status",
        label: "Status",
        align: "center",
        cell: (row) => (row.status || "").trim() || (row.isCompleted ? "completed" : "open"),
      },
    ],
    [],
  );

  const selectedUserEntry = useMemo<CustomDashboardUserSettingsEntry | null>(() => {
    const users = usersPayload?.users || [];
    if (!selectedUser) {
      return null;
    }
    return users.find((user) => user.username === selectedUser) || null;
  }, [selectedUser, usersPayload?.users]);

  const selectedUserDraft = useMemo<CustomDashboardWidgetSettings | null>(() => {
    if (!selectedUser) {
      return null;
    }
    return usersDraft[selectedUser] || null;
  }, [selectedUser, usersDraft]);

  const uploadStateLabel = useMemo(() => {
    if (!uploadingType) {
      return "";
    }
    if (uploadingType === "contacts") {
      return "Uploading contacts file...";
    }
    return "Uploading calls file...";
  }, [uploadingType]);

  const refreshEverything = useCallback(async () => {
    await loadDashboard();
    if (canManage && activeTab === "settings") {
      await loadUsersSettings();
    }
  }, [activeTab, canManage, loadDashboard, loadUsersSettings]);

  const onUploadSelected = useCallback(
    async (type: UploadType, file: File) => {
      setUploadingType(type);
      try {
        const result = await uploadCustomDashboardFile(type, file);
        showToast({
          type: "success",
          message: `${toUploadTitle(type)} uploaded (${result.count} rows).`,
          dedupeKey: `custom-dashboard-upload-${type}-${result.uploadedAt}`,
          cooldownMs: 2500,
        });

        await refreshEverything();
      } catch (error) {
        const message = error instanceof Error ? error.message : "Upload failed.";
        showToast({
          type: "error",
          message,
          dedupeKey: `custom-dashboard-upload-error-${type}-${message}`,
          cooldownMs: 3000,
        });
      } finally {
        setUploadingType("");
      }
    },
    [refreshEverything],
  );

  const onLoadTaskMovements = useCallback(async (options: { refresh?: boolean; silent?: boolean } = {}) => {
    if (!canManage) {
      return;
    }

    const refresh = Boolean(options.refresh);
    const silent = Boolean(options.silent);
    setTaskMovementsLoading(true);
    setTaskMovementsError("");
    try {
      const payload = await getCustomDashboardTaskMovements(24, { refresh });
      setTaskMovements(payload);
      if (!silent) {
        showToast({
          type: "success",
          message: refresh
            ? `Refreshed from GoHighLevel: ${payload.changedTasks} changes.`
            : `Loaded 24h task movements: ${payload.changedTasks} changes.`,
          dedupeKey: `custom-dashboard-task-movements-${refresh ? "refresh" : "load"}-${payload.generatedAt}`,
          cooldownMs: 2500,
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load task movements.";
      setTaskMovementsError(message);
      if (!silent) {
        showToast({
          type: "error",
          message,
          dedupeKey: `custom-dashboard-task-movements-error-${message}`,
          cooldownMs: 3000,
        });
      }
    } finally {
      setTaskMovementsLoading(false);
    }
  }, [canManage]);

  useEffect(() => {
    if (!canManage || activeTab !== "dashboard") {
      return;
    }
    if (taskMovementsLoadedRef.current) {
      return;
    }
    taskMovementsLoadedRef.current = true;
    void onLoadTaskMovements({ silent: true, refresh: false });
  }, [activeTab, canManage, onLoadTaskMovements]);

  const onSaveUsersSettings = useCallback(async () => {
    if (!canManage || !usersPayload) {
      return;
    }

    setIsSavingUsers(true);
    setUsersError("");
    try {
      const usersToSave = usersPayload.users.map((user) => ({
        username: user.username,
        widgets: usersDraft[user.username] ? cloneWidgetSettings(usersDraft[user.username]) : cloneWidgetSettings(user.widgets),
      }));

      await saveCustomDashboardUsers({ users: usersToSave });
      showToast({
        type: "success",
        message: "User visibility settings saved.",
        dedupeKey: "custom-dashboard-users-saved",
        cooldownMs: 2500,
      });

      await loadUsersSettings();
      await loadDashboard();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to save user settings.";
      setUsersError(message);
      showToast({
        type: "error",
        message,
        dedupeKey: `custom-dashboard-users-save-error-${message}`,
        cooldownMs: 3000,
      });
    } finally {
      setIsSavingUsers(false);
    }
  }, [canManage, loadDashboard, loadUsersSettings, usersDraft, usersPayload]);

  function updateWidgetEnabled(widgetKey: CustomDashboardWidgetKey, enabled: boolean) {
    if (!selectedUser) {
      return;
    }

    setUsersDraft((previous) => {
      const current = previous[selectedUser];
      if (!current) {
        return previous;
      }

      return {
        ...previous,
        [selectedUser]: {
          ...current,
          [widgetKey]: {
            ...current[widgetKey],
            enabled,
          },
        },
      };
    });
  }

  function toggleVisibilityName(widgetKey: CustomDashboardWidgetKey, name: string) {
    if (!selectedUser) {
      return;
    }

    setUsersDraft((previous) => {
      const current = previous[selectedUser];
      if (!current) {
        return previous;
      }

      const currentWidget = current[widgetKey];
      const comparableName = normalizeComparable(name);
      const hasName = currentWidget.visibleNames.some((entry) => normalizeComparable(entry) === comparableName);
      const nextVisibleNames = hasName
        ? currentWidget.visibleNames.filter((entry) => normalizeComparable(entry) !== comparableName)
        : [...currentWidget.visibleNames, name];

      return {
        ...previous,
        [selectedUser]: {
          ...current,
          [widgetKey]: {
            ...currentWidget,
            visibleNames: nextVisibleNames,
          },
        },
      };
    });
  }

  return (
    <PageShell className="custom-dashboard-react-page">
      <PageHeader
        title="Custom Dashboard"
        subtitle="Operational center for sales and CRM"
        actions={
          <div className="custom-dashboard-header-actions">
            {canManage ? (
              <>
                <Button
                  type="button"
                  size="sm"
                  variant={activeTab === "dashboard" ? "primary" : "secondary"}
                  onClick={() => setTab("dashboard")}
                >
                  Dashboard
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant={activeTab === "settings" ? "primary" : "secondary"}
                  onClick={() => setTab("settings")}
                >
                  User Settings
                </Button>
              </>
            ) : null}
            <Button type="button" size="sm" variant="secondary" onClick={() => void refreshEverything()} disabled={isLoading}>
              Refresh
            </Button>
          </div>
        }
        meta={
          dashboard ? (
            <p className="custom-dashboard-meta">
              Signed in as <strong>{dashboard.activeUser.displayName || dashboard.activeUser.username || "User"}</strong> ({dashboard.moduleRole})
            </p>
          ) : null
        }
      />

      {isLoading ? <LoadingSkeleton rows={8} /> : null}

      {!isLoading && loadError ? (
        <ErrorState
          title="Failed to load custom dashboard"
          description={loadError}
          actionLabel="Retry"
          onAction={() => void loadDashboard()}
        />
      ) : null}

      {!isLoading && !loadError && dashboard ? (
        <>
          {activeTab === "dashboard" ? (
            <>
              {canManage ? (
                <Panel title="Uploads" className="custom-dashboard-uploads-panel">
                  <p className="dashboard-message">Upload data files for tasks, contacts and calls.</p>
                  <div className="custom-dashboard-tasks-source-card">
                    <div className="custom-dashboard-tasks-source-head">
                      <h3>Task Movements (Last 24 Hours)</h3>
                      <div className="custom-dashboard-tasks-source-actions">
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          onClick={() => void onLoadTaskMovements({ refresh: true })}
                          isLoading={taskMovementsLoading}
                          disabled={!dashboard.tasksSource.ghlConfigured || taskMovementsLoading}
                        >
                          {taskMovements ? "Refresh from GHL" : "Load 24h"}
                        </Button>
                      </div>
                    </div>
                    <p className="dashboard-message">
                      Auto refresh: daily at 10:00 PM America/Chicago.
                    </p>
                    <p className="dashboard-message">
                      Data is stored in our database. Manual refresh is optional.
                    </p>
                    {taskMovementsError ? <p className="dashboard-message error">{taskMovementsError}</p> : null}
                    {taskMovements ? (
                      <>
                        <p className="dashboard-message">
                          Generated: {formatDateTimeOrDash(taskMovements.generatedAt)}. Since: {formatDateTimeOrDash(taskMovements.since)}.
                          {" "}
                          Cached: {formatDateTimeOrDash(taskMovements.cachedAt || "")}.
                          {" "}
                          Source: {taskMovements.fromCache ? "cache" : "live"}.
                        </p>
                        {taskMovements.autoSync?.nextRunAt ? (
                          <p className="dashboard-message">Next auto refresh: {formatDateTimeOrDash(taskMovements.autoSync.nextRunAt)}.</p>
                        ) : null}
                        {taskMovements.autoSync?.lastError ? (
                          <p className="dashboard-message error">Last auto-sync error: {taskMovements.autoSync.lastError}</p>
                        ) : null}
                        <div className="custom-dashboard-kpi-row">
                          <KpiCard label="Changed" value={String(taskMovements.changedTasks)} />
                          <KpiCard label="Created" value={String(taskMovements.createdTasks)} />
                          <KpiCard label="Completed" value={String(taskMovements.completedTasks)} />
                          <KpiCard label="Updated" value={String(taskMovements.updatedTasks)} />
                          <KpiCard label="Managers" value={String(taskMovements.managers)} />
                          <KpiCard label="Contacts" value={String(taskMovements.contacts)} />
                        </div>
                        <p className="dashboard-message">
                          Scanned tasks: {taskMovements.scannedTasks}. Pages: {taskMovements.totalPages}. Returned rows:{" "}
                          {taskMovements.rowsReturned}
                          {taskMovements.truncatedRows ? ` (capped at ${taskMovements.rowLimit})` : ""}.
                        </p>
                        {taskMovements.managerSummary.length ? (
                          <Table
                            className="custom-dashboard-table-wrap"
                            columns={taskMovementsManagerColumns}
                            rows={taskMovements.managerSummary}
                            rowKey={(row) => row.managerName}
                            density="compact"
                          />
                        ) : (
                          <EmptyState title="No manager movement summary for selected period." />
                        )}
                        <div className="custom-dashboard-task-movements-filter">
                          <Select
                            value={selectedTaskMovementManager}
                            onChange={(event) => setSelectedTaskMovementManager(event.target.value)}
                            disabled={!taskMovementsManagerOptions.length || taskMovementsLoading}
                          >
                            <option value="">All managers</option>
                            {taskMovementsManagerOptions.map((managerName) => (
                              <option key={managerName} value={managerName}>
                                {managerName}
                              </option>
                            ))}
                          </Select>
                        </div>
                        {taskMovementsRows.length ? (
                          <Table
                            className="custom-dashboard-table-wrap"
                            columns={taskMovementsColumns}
                            rows={taskMovementsRows}
                            rowKey={(row, index) => `${row.taskId}-${index}`}
                            density="compact"
                          />
                        ) : (
                          <EmptyState title="No changed tasks for selected manager in last 24 hours." />
                        )}
                      </>
                    ) : null}
                  </div>
                  {uploadStateLabel ? <p className="dashboard-message">{uploadStateLabel}</p> : null}

                  <div className="custom-dashboard-upload-grid">
                    <UploadCard
                      title="Contacts"
                      meta={dashboard.uploads.contacts}
                      disabled={Boolean(uploadingType)}
                      loading={uploadingType === "contacts"}
                      onUploadClick={() => contactsFileInputRef.current?.click()}
                    />
                    <UploadCard
                      title="Calls"
                      meta={dashboard.uploads.calls}
                      disabled={Boolean(uploadingType)}
                      loading={uploadingType === "calls"}
                      onUploadClick={() => callsFileInputRef.current?.click()}
                    />
                  </div>

                  <input
                    ref={contactsFileInputRef}
                    type="file"
                    accept=".csv,.tsv,.txt"
                    className="custom-dashboard-upload-input"
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) {
                        void onUploadSelected("contacts", file);
                      }
                      event.currentTarget.value = "";
                    }}
                  />
                  <input
                    ref={callsFileInputRef}
                    type="file"
                    accept=".csv,.tsv,.txt"
                    className="custom-dashboard-upload-input"
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) {
                        void onUploadSelected("calls", file);
                      }
                      event.currentTarget.value = "";
                    }}
                  />
                </Panel>
              ) : null}

              <Panel title="Manager Tasks" className="custom-dashboard-widget-panel">
                {!dashboard.managerTasks.enabled ? (
                  <EmptyState title="Widget is disabled for your account." />
                ) : !dashboard.managerTasks.rows.length ? (
                  <EmptyState title="No manager tasks available." />
                ) : (
                  <>
                    <div className="custom-dashboard-kpi-row">
                      <KpiCard label="Managers" value={String(dashboard.managerTasks.totals.managers)} />
                      <KpiCard label="Tasks" value={String(dashboard.managerTasks.totals.tasks)} />
                      <KpiCard label="Open" value={String(dashboard.managerTasks.totals.open)} />
                      <KpiCard label="Overdue" value={String(dashboard.managerTasks.totals.overdue)} />
                      <KpiCard label="Due Today" value={String(dashboard.managerTasks.totals.dueToday)} />
                      <KpiCard
                        label="Completed Yesterday"
                        value={String(dashboard.managerTasks.totals.completedYesterday)}
                      />
                    </div>
                    <Table
                      className="custom-dashboard-table-wrap"
                      columns={managerTasksColumns}
                      rows={dashboard.managerTasks.rows}
                      rowKey={(row) => row.managerName}
                      density="compact"
                    />
                  </>
                )}
              </Panel>

              <Panel
                title="Specialist Tasks"
                className="custom-dashboard-widget-panel"
                actions={
                  dashboard.specialistTasks.enabled ? (
                    <div className="custom-dashboard-specialist-toolbar">
                      <Select
                        value={selectedSpecialist}
                        onChange={(event) => setSelectedSpecialist(event.target.value)}
                        disabled={!dashboard.specialistTasks.specialistOptions.length}
                      >
                        {dashboard.specialistTasks.specialistOptions.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </Select>
                    </div>
                  ) : undefined
                }
              >
                {!dashboard.specialistTasks.enabled ? (
                  <EmptyState title="Widget is disabled for your account." />
                ) : !dashboard.specialistTasks.specialistOptions.length ? (
                  <EmptyState title="No specialist tasks available." />
                ) : (
                  <>
                    <SegmentedControl
                      value={tasksView}
                      options={TASK_VIEW_OPTIONS.map((option) => ({
                        key: option.key,
                        label: option.label,
                      }))}
                      onChange={(value) => {
                        if (value === "totals" || value === "all" || value === "overdue" || value === "dueToday") {
                          setTasksView(value);
                        }
                      }}
                    />

                    {tasksView === "totals" ? (
                      <div className="custom-dashboard-kpi-row custom-dashboard-kpi-row--four">
                        <KpiCard label="All Tasks" value={String(specialistTotals.all)} />
                        <KpiCard label="Open" value={String(specialistTotals.open)} />
                        <KpiCard label="Overdue" value={String(specialistTotals.overdue)} />
                        <KpiCard label="Due Today" value={String(specialistTotals.dueToday)} />
                      </div>
                    ) : null}

                    {tasksView === "all" ? (
                      specialistTasksScoped.length ? (
                        <Table
                          className="custom-dashboard-table-wrap"
                          columns={specialistTaskColumns}
                          rows={specialistTasksScoped}
                          rowKey={(row, index) => `${row.id}-${index}`}
                          density="compact"
                        />
                      ) : (
                        <EmptyState title="No tasks for selected specialist." />
                      )
                    ) : null}

                    {tasksView === "overdue" ? (
                      specialistOverdueTasks.length ? (
                        <Table
                          className="custom-dashboard-table-wrap"
                          columns={specialistTaskColumns}
                          rows={specialistOverdueTasks}
                          rowKey={(row, index) => `${row.id}-${index}`}
                          density="compact"
                        />
                      ) : (
                        <EmptyState title="No overdue tasks for selected specialist." />
                      )
                    ) : null}

                    {tasksView === "dueToday" ? (
                      specialistDueTodayTasks.length ? (
                        <Table
                          className="custom-dashboard-table-wrap"
                          columns={specialistTaskColumns}
                          rows={specialistDueTodayTasks}
                          rowKey={(row, index) => `${row.id}-${index}`}
                          density="compact"
                        />
                      ) : (
                        <EmptyState title="No tasks due today for selected specialist." />
                      )
                    ) : null}
                  </>
                )}
              </Panel>

              <Panel title="Sales Report" className="custom-dashboard-widget-panel">
                {!dashboard.salesReport.enabled ? (
                  <EmptyState title="Widget is disabled for your account." />
                ) : (
                  <>
                    <div className="custom-dashboard-sales-period-grid">
                      {(
                        Object.entries(dashboard.salesReport.periods) as Array<
                          [keyof CustomDashboardPayload["salesReport"]["periods"], CustomDashboardSalesMetrics]
                        >
                      ).map(([periodKey, metrics]) => (
                        <div key={periodKey} className="custom-dashboard-sales-period-card">
                          <h3>{SALES_PERIOD_LABELS[periodKey]}</h3>
                          <div className="custom-dashboard-sales-period-metrics">
                            <MetricLine label="Calls" value={String(metrics.calls)} />
                            <MetricLine label="Answers" value={String(metrics.answers)} />
                            <MetricLine label="Talks" value={String(metrics.talks)} />
                            <MetricLine label="Interested" value={String(metrics.interested)} />
                            <MetricLine label="Closed" value={String(metrics.closedDeals)} />
                            <MetricLine label="Amount" value={formatMoney(metrics.closedAmount)} />
                          </div>
                        </div>
                      ))}
                    </div>

                    {dashboard.salesReport.managerBreakdown.length ? (
                      <Table
                        className="custom-dashboard-table-wrap"
                        columns={salesColumns}
                        rows={dashboard.salesReport.managerBreakdown}
                        rowKey={(row) => row.managerName}
                        density="compact"
                      />
                    ) : (
                      <EmptyState title="No sales rows for manager breakdown." />
                    )}
                  </>
                )}
              </Panel>

              <Panel
                title="Calls by Manager"
                className="custom-dashboard-widget-panel"
                actions={
                  dashboard.callsByManager.enabled ? (
                    <Select
                      value={selectedCallsManager}
                      onChange={(event) => setSelectedCallsManager(event.target.value)}
                    >
                      <option value="">All managers</option>
                      {dashboard.callsByManager.managerOptions.map((managerName) => (
                        <option key={managerName} value={managerName}>
                          {managerName}
                        </option>
                      ))}
                    </Select>
                  ) : undefined
                }
              >
                {!dashboard.callsByManager.enabled ? (
                  <EmptyState title="Widget is disabled for your account." />
                ) : (
                  <>
                    <SegmentedControl
                      value={callsView}
                      options={CALLS_VIEW_OPTIONS.map((option) => ({
                        key: option.key,
                        label: option.label,
                      }))}
                      onChange={(value) => {
                        if (value === "stats" || value === "missed") {
                          setCallsView(value);
                        }
                      }}
                    />

                    {callsView === "stats" ? (
                      callsStatsRows.length ? (
                        <Table
                          className="custom-dashboard-table-wrap"
                          columns={callsStatsColumns}
                          rows={callsStatsRows}
                          rowKey={(row) => row.managerName}
                          density="compact"
                        />
                      ) : (
                        <EmptyState title="No call statistics rows." />
                      )
                    ) : null}

                    {callsView === "missed" ? (
                      missedCallsRows.length ? (
                        <Table
                          className="custom-dashboard-table-wrap"
                          columns={missedCallsColumns}
                          rows={missedCallsRows}
                          rowKey={(row, index) => `${row.id}-${index}`}
                          density="compact"
                        />
                      ) : (
                        <EmptyState title="No missed incoming calls." />
                      )
                    ) : null}
                  </>
                )}
              </Panel>
            </>
          ) : null}

          {activeTab === "settings" && canManage ? (
            <Panel
              title="User Settings"
              className="custom-dashboard-settings-panel"
              actions={
                <div className="custom-dashboard-settings-actions">
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    onClick={() => void loadUsersSettings()}
                    disabled={usersLoading || isSavingUsers}
                  >
                    Reload
                  </Button>
                  <Button
                    type="button"
                    size="sm"
                    onClick={() => void onSaveUsersSettings()}
                    isLoading={isSavingUsers}
                    disabled={usersLoading || !selectedUser || isSavingUsers}
                  >
                    Save
                  </Button>
                </div>
              }
            >
              {usersLoading ? <LoadingSkeleton rows={6} /> : null}

              {!usersLoading && usersError ? (
                <ErrorState
                  title="Failed to load user settings"
                  description={usersError}
                  actionLabel="Retry"
                  onAction={() => void loadUsersSettings()}
                />
              ) : null}

              {!usersLoading && !usersError && usersPayload ? (
                <>
                  <div className="custom-dashboard-user-picker">
                    <label htmlFor="custom-dashboard-user-select" className="search-label">
                      Select User
                    </label>
                    <Select
                      id="custom-dashboard-user-select"
                      value={selectedUser}
                      onChange={(event) => setSelectedUser(event.target.value)}
                    >
                      {usersPayload.users.map((user) => (
                        <option key={user.username} value={user.username}>
                          {user.displayName || user.username} ({user.moduleRole})
                        </option>
                      ))}
                    </Select>
                  </div>

                  {selectedUserEntry && selectedUserDraft ? (
                    <div className="custom-dashboard-settings-grid">
                      {(
                        Object.keys(WIDGET_LABELS) as CustomDashboardWidgetKey[]
                      ).map((widgetKey) => {
                        const widgetState = selectedUserDraft[widgetKey];
                        const options = usersPayload.options[widgetKey] || [];
                        const visibleSet = new Set(
                          widgetState.visibleNames.map((name) => normalizeComparable(name)),
                        );

                        return (
                          <section key={`${selectedUser}-${widgetKey}`} className="custom-dashboard-widget-settings-card">
                            <header className="custom-dashboard-widget-settings-header">
                              <h3>{WIDGET_LABELS[widgetKey]}</h3>
                              <label className="cb-checkbox-row custom-dashboard-widget-enable">
                                <input
                                  type="checkbox"
                                  checked={widgetState.enabled}
                                  onChange={(event) => updateWidgetEnabled(widgetKey, event.target.checked)}
                                />
                                Enabled
                              </label>
                            </header>

                            {!options.length ? (
                              <p className="dashboard-message">No data-driven options yet. Upload files first.</p>
                            ) : (
                              <div className="custom-dashboard-visibility-list">
                                {options.map((name) => {
                                  const comparableName = normalizeComparable(name);
                                  return (
                                    <label
                                      key={`${widgetKey}-${name}`}
                                      className="cb-checkbox-row custom-dashboard-visibility-item"
                                    >
                                      <input
                                        type="checkbox"
                                        checked={visibleSet.has(comparableName)}
                                        onChange={() => toggleVisibilityName(widgetKey, name)}
                                        disabled={!widgetState.enabled}
                                      />
                                      {name}
                                    </label>
                                  );
                                })}
                              </div>
                            )}

                            <p className="dashboard-message">
                              {widgetState.visibleNames.length
                                ? `Selected: ${widgetState.visibleNames.join(", ")}`
                                : "Selected: All employees"}
                            </p>
                          </section>
                        );
                      })}
                    </div>
                  ) : (
                    <EmptyState title="Select user to edit widget visibility." />
                  )}
                </>
              ) : null}
            </Panel>
          ) : null}
        </>
      ) : null}
    </PageShell>
  );
}

interface UploadCardProps {
  title: string;
  meta: CustomDashboardUploadMeta;
  disabled: boolean;
  loading: boolean;
  onUploadClick: () => void;
  actionLabel?: string;
}

function UploadCard({ title, meta, disabled, loading, onUploadClick, actionLabel }: UploadCardProps) {
  return (
    <div className="custom-dashboard-upload-card">
      <h3>{title}</h3>
      <p className="dashboard-message">
        {meta.count ? `${meta.count} rows` : "No uploads yet"}
      </p>
      <p className="dashboard-message">File: {meta.fileName || "-"}</p>
      <p className="dashboard-message">Uploaded: {formatDateTimeOrDash(meta.uploadedAt)}</p>
      <Button
        type="button"
        size="sm"
        variant="secondary"
        onClick={onUploadClick}
        disabled={disabled}
        isLoading={loading}
      >
        {actionLabel || "Upload File"}
      </Button>
    </div>
  );
}

interface KpiCardProps {
  label: string;
  value: string;
}

function KpiCard({ label, value }: KpiCardProps) {
  return (
    <div className="custom-dashboard-kpi-card">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

interface MetricLineProps {
  label: string;
  value: string;
}

function MetricLine({ label, value }: MetricLineProps) {
  return (
    <p className="custom-dashboard-metric-line">
      <span>{label}</span>
      <strong>{value}</strong>
    </p>
  );
}

function normalizeComparable(value: string): string {
  return (value || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function cloneWidgetSettings(settings: CustomDashboardWidgetSettings): CustomDashboardWidgetSettings {
  return {
    managerTasks: {
      enabled: Boolean(settings.managerTasks?.enabled),
      visibleNames: [...(settings.managerTasks?.visibleNames || [])],
    },
    specialistTasks: {
      enabled: Boolean(settings.specialistTasks?.enabled),
      visibleNames: [...(settings.specialistTasks?.visibleNames || [])],
    },
    salesReport: {
      enabled: Boolean(settings.salesReport?.enabled),
      visibleNames: [...(settings.salesReport?.visibleNames || [])],
    },
    callsByManager: {
      enabled: Boolean(settings.callsByManager?.enabled),
      visibleNames: [...(settings.callsByManager?.visibleNames || [])],
    },
  };
}

function toUploadTitle(type: UploadType): string {
  return type === "contacts" ? "Contacts" : "Calls";
}

function formatDateTimeOrDash(rawValue: string): string {
  const value = (rawValue || "").trim();
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatDateOrDash(rawValue: string): string {
  const value = (rawValue || "").trim();
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function formatMoney(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number.isFinite(value) ? value : 0);
}
