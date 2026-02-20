import { useCallback, useEffect, useMemo, useState } from "react";

import { getGhlLeads, getSession } from "@/shared/api";
import type { GhlLeadRow, GhlLeadsSummary } from "@/shared/types/ghlLeads";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageShell, Panel, SegmentedControl, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

type RefreshMode = "none" | "incremental";
type LeadsFilter = "all" | "today" | "week" | "month";

const EMPTY_SUMMARY: GhlLeadsSummary = {
  total: 0,
  today: 0,
  week: 0,
  month: 0,
  timezone: "",
  generatedAt: "",
};
const DEFAULT_TIME_ZONE = "America/Chicago";
const FILTER_OPTIONS = [
  { key: "all", label: "All" },
  { key: "today", label: "Today" },
  { key: "week", label: "This Week" },
  { key: "month", label: "This Month" },
];
const WEEK_START_DAY = 1;
const WEEKDAY_INDEX_BY_LABEL: Record<string, number> = {
  sun: 0,
  mon: 1,
  tue: 2,
  wed: 3,
  thu: 4,
  fri: 5,
  sat: 6,
};
const SPREADSHEET_FORMULA_PREFIX = /^\s*[=+\-@]/;

export default function LeadsPage() {
  const [items, setItems] = useState<GhlLeadRow[]>([]);
  const [summary, setSummary] = useState<GhlLeadsSummary>(EMPTY_SUMMARY);
  const [pipelineName, setPipelineName] = useState("SALES 3 LINE");
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [canSync, setCanSync] = useState(false);
  const [currentMode, setCurrentMode] = useState<RefreshMode>("none");
  const [activeFilter, setActiveFilter] = useState<LeadsFilter>("all");
  const [lastSyncedCount, setLastSyncedCount] = useState(0);
  const activeTimeZone = summary.timezone || DEFAULT_TIME_ZONE;

  const visibleItems = useMemo(() => {
    return filterLeadsByDateWindow(items, activeFilter, activeTimeZone);
  }, [activeFilter, activeTimeZone, items]);

  const statusText = useMemo(() => {
    if (isLoading) {
      if (currentMode === "incremental") {
        return "Refreshing only new leads from GoHighLevel...";
      }
      return "Loading leads from local cache...";
    }

    if (loadError) {
      return loadError;
    }

    if (!summary.total) {
      return "No leads found in local cache. Press Refresh to sync new data.";
    }

    return `Showing ${visibleItems.length} of ${summary.total} leads. Today: ${summary.today}. This week: ${summary.week}. This month: ${summary.month}. Last sync added/updated: ${lastSyncedCount}.`;
  }, [
    currentMode,
    isLoading,
    lastSyncedCount,
    loadError,
    summary.month,
    summary.today,
    summary.total,
    summary.week,
    visibleItems.length,
  ]);

  const loadLeads = useCallback(async (mode: RefreshMode = "none") => {
    setIsLoading(true);
    setLoadError("");
    setCurrentMode(mode);

    try {
      const payload = await getGhlLeads(mode);
      const nextItems = Array.isArray(payload.items) ? payload.items : [];

      setItems(nextItems);
      setSummary(payload.summary || EMPTY_SUMMARY);
      setPipelineName((payload.pipeline?.name || "").toString().trim() || "SALES 3 LINE");
      setLastSyncedCount(Number(payload.refresh?.syncedLeadsCount) || 0);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load leads table.";
      setItems([]);
      setSummary(EMPTY_SUMMARY);
      setLastSyncedCount(0);
      setLoadError(message);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void getSession()
      .then((session) => {
        setCanSync(Boolean(session?.permissions?.sync_client_managers));
      })
      .catch(() => {
        setCanSync(false);
      });

    void loadLeads("none");
  }, [loadLeads]);

  const columns = useMemo<TableColumn<GhlLeadRow>[]>(() => {
    return [
      {
        key: "createdOn",
        label: "Created On",
        align: "left",
        cell: (item) => formatDateTime(item.createdOn),
      },
      {
        key: "opportunityName",
        label: "Lead",
        align: "left",
        cell: (item) => item.opportunityName || "-",
      },
      {
        key: "contactName",
        label: "Contact",
        align: "left",
        cell: (item) => item.contactName || "-",
      },
      {
        key: "stageName",
        label: "Stage",
        align: "left",
        cell: (item) => item.stageName || "-",
      },
      {
        key: "status",
        label: "Status",
        align: "left",
        cell: (item) => <Badge tone={statusToBadgeTone(item.status)}>{formatStatus(item.status)}</Badge>,
      },
      {
        key: "monetaryValue",
        label: "Amount",
        align: "right",
        cell: (item) => formatCurrency(item.monetaryValue),
      },
    ];
  }, []);

  const handleExportCsv = useCallback(() => {
    if (!visibleItems.length) {
      return;
    }
    exportLeadsToCsv(visibleItems, activeFilter);
  }, [activeFilter, visibleItems]);

  return (
    <PageShell className="leads-react-page">
      <Panel
        className="table-panel leads-react-table-panel"
        title={`Leads (${pipelineName})`}
        actions={
          <div className="leads-toolbar-react">
            <SegmentedControl
              value={activeFilter}
              options={FILTER_OPTIONS}
              onChange={(value) => setActiveFilter(value as LeadsFilter)}
            />
            <Button
              type="button"
              size="sm"
              variant="secondary"
              onClick={handleExportCsv}
              disabled={isLoading || !visibleItems.length}
            >
              Export CSV
            </Button>
            <Button
              type="button"
              size="sm"
              onClick={() => void loadLeads("incremental")}
              disabled={isLoading || !canSync}
              isLoading={isLoading && currentMode === "incremental"}
            >
              Refresh
            </Button>
          </div>
        }
      >
        {!loadError ? <p className="dashboard-message leads-status">{statusText}</p> : null}

        <div className="leads-summary-react" aria-live="polite">
          <SummaryCard title="Today" value={summary.today} />
          <SummaryCard title="This Week" value={summary.week} />
          <SummaryCard title="This Month" value={summary.month} />
        </div>

        {isLoading ? <LoadingSkeleton rows={8} /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load leads"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadLeads("none")}
          />
        ) : null}

        {!isLoading && !loadError && !visibleItems.length ? (
          <EmptyState title={items.length ? "No leads in selected period." : "No leads found."} />
        ) : null}

        {!isLoading && !loadError && visibleItems.length ? (
          <Table
            className="leads-react-table-wrap"
            columns={columns}
            rows={visibleItems}
            rowKey={(item, index) => item.leadId || `${item.createdOn}-${index}`}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function SummaryCard({ title, value }: { title: string; value: number }) {
  return (
    <section className="leads-summary-card">
      <p className="leads-summary-card__title">{title}</p>
      <p className="leads-summary-card__value">{Number.isFinite(value) ? value : 0}</p>
    </section>
  );
}

function formatDateTime(value: string): string {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function formatCurrency(rawValue: number): string {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) {
    return "-";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function statusToBadgeTone(statusValue: string): "neutral" | "success" | "warning" | "danger" | "info" {
  const status = (statusValue || "").toString().trim().toLowerCase();
  if (status.includes("won") || status.includes("success")) {
    return "success";
  }
  if (status.includes("lost") || status.includes("abandoned") || status.includes("fail")) {
    return "danger";
  }
  if (status.includes("open") || status.includes("active")) {
    return "info";
  }
  if (status.includes("pending")) {
    return "warning";
  }
  return "neutral";
}

function formatStatus(statusValue: string): string {
  const status = (statusValue || "").toString().trim();
  if (!status) {
    return "Unknown";
  }

  return status
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (symbol) => symbol.toUpperCase());
}

function filterLeadsByDateWindow(items: GhlLeadRow[], filter: LeadsFilter, timeZone: string): GhlLeadRow[] {
  if (filter === "all") {
    return items;
  }

  const boundaries = buildTimeBoundaries(timeZone, new Date());
  if (!boundaries) {
    return items;
  }

  return items.filter((item) => {
    const createdTimestamp = Date.parse(item.createdOn || "");
    if (!Number.isFinite(createdTimestamp)) {
      return false;
    }

    if (filter === "today") {
      return createdTimestamp >= boundaries.todayStart && createdTimestamp < boundaries.tomorrowStart;
    }

    if (filter === "week") {
      return createdTimestamp >= boundaries.weekStart && createdTimestamp < boundaries.tomorrowStart;
    }

    if (filter === "month") {
      return createdTimestamp >= boundaries.monthStart && createdTimestamp < boundaries.tomorrowStart;
    }

    return true;
  });
}

function buildTimeBoundaries(timeZone: string, dateValue = new Date()) {
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  const nowParts = getClockParts(timeZone, date);
  if (!nowParts) {
    return null;
  }

  const todayStart = buildUtcDateFromTimeZoneLocalParts(timeZone, nowParts.year, nowParts.month, nowParts.day, 0, 0).getTime();

  const tomorrow = addDaysToCalendarDate(nowParts.year, nowParts.month, nowParts.day, 1);
  const tomorrowStart = buildUtcDateFromTimeZoneLocalParts(timeZone, tomorrow.year, tomorrow.month, tomorrow.day, 0, 0).getTime();

  const monthStart = buildUtcDateFromTimeZoneLocalParts(timeZone, nowParts.year, nowParts.month, 1, 0, 0).getTime();

  const weekdayIndex = getWeekdayIndexForTimeZone(timeZone, date);
  const offsetToWeekStart = (weekdayIndex - WEEK_START_DAY + 7) % 7;
  const weekStartCalendar = addDaysToCalendarDate(nowParts.year, nowParts.month, nowParts.day, -offsetToWeekStart);
  const weekStart = buildUtcDateFromTimeZoneLocalParts(
    timeZone,
    weekStartCalendar.year,
    weekStartCalendar.month,
    weekStartCalendar.day,
    0,
    0,
  ).getTime();

  return {
    todayStart,
    tomorrowStart,
    weekStart,
    monthStart,
  };
}

function getClockParts(timeZone: string, dateValue = new Date()) {
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const values: Record<string, string> = {};
  for (const part of formatter.formatToParts(date)) {
    if (part.type !== "literal") {
      values[part.type] = part.value;
    }
  }

  const year = Number.parseInt(values.year || "", 10);
  const month = Number.parseInt(values.month || "", 10);
  const day = Number.parseInt(values.day || "", 10);

  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }

  return {
    year,
    month,
    day,
  };
}

function getWeekdayIndexForTimeZone(timeZone: string, dateValue = new Date()): number {
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  const label = new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "short",
  })
    .format(date)
    .slice(0, 3)
    .toLowerCase();
  return WEEKDAY_INDEX_BY_LABEL[label] ?? 0;
}

function getTimeZoneOffsetMinutes(timeZone: string, dateValue: Date): number {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    timeZoneName: "shortOffset",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(dateValue);
  const offsetPart = parts.find((part) => part.type === "timeZoneName");
  const value = (offsetPart?.value || "").toString();
  const match = value.match(/^GMT([+-]\d{1,2})(?::?(\d{2}))?$/i);
  if (!match) {
    return 0;
  }

  const hours = Number.parseInt(match[1], 10);
  const minutes = Number.parseInt(match[2] || "0", 10);
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) {
    return 0;
  }

  return hours * 60 + (hours >= 0 ? minutes : -minutes);
}

function buildUtcDateFromTimeZoneLocalParts(
  timeZone: string,
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
): Date {
  let utcTimestamp = Date.UTC(year, month - 1, day, hour, minute, 0, 0);

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const offsetMinutes = getTimeZoneOffsetMinutes(timeZone, new Date(utcTimestamp));
    const candidateTimestamp = Date.UTC(year, month - 1, day, hour, minute, 0, 0) - offsetMinutes * 60 * 1000;
    if (candidateTimestamp === utcTimestamp) {
      break;
    }
    utcTimestamp = candidateTimestamp;
  }

  return new Date(utcTimestamp);
}

function addDaysToCalendarDate(year: number, month: number, day: number, dayOffset: number) {
  const date = new Date(Date.UTC(year, month - 1, day + dayOffset, 12, 0, 0, 0));
  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}

function exportLeadsToCsv(rows: GhlLeadRow[], filter: LeadsFilter): void {
  const headers = ["Created On", "Lead", "Contact", "Stage", "Status", "Amount", "Pipeline", "Lead ID"];
  const lines = [headers.map(escapeCsvValue).join(",")];

  for (const row of rows) {
    lines.push(
      [
        formatDateTime(row.createdOn),
        row.opportunityName || "",
        row.contactName || "",
        row.stageName || "",
        formatStatus(row.status),
        normalizeNumberForCsv(row.monetaryValue),
        row.pipelineName || "",
        row.leadId || "",
      ]
        .map(escapeCsvValue)
        .join(","),
    );
  }

  const blob = new Blob([`\ufeff${lines.join("\n")}`], {
    type: "text/csv;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `leads-${filter}-${formatFileDate(new Date())}.csv`;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function escapeCsvValue(rawValue: string): string {
  const text = sanitizeSpreadsheetCell(rawValue);
  const escaped = text.replace(/"/g, '""');
  return `"${escaped}"`;
}

function sanitizeSpreadsheetCell(value: string): string {
  const text = (value || "").toString();
  if (!text) {
    return "";
  }

  if (SPREADSHEET_FORMULA_PREFIX.test(text)) {
    return `'${text}`;
  }

  return text;
}

function normalizeNumberForCsv(rawValue: number): string {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) {
    return "0";
  }
  return value.toFixed(2);
}

function formatFileDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  const hours = String(value.getHours()).padStart(2, "0");
  const minutes = String(value.getMinutes()).padStart(2, "0");
  return `${year}${month}${day}-${hours}${minutes}`;
}
