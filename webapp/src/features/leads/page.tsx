import { useCallback, useEffect, useMemo, useState } from "react";

import { getGhlLeads, getSession } from "@/shared/api";
import type { GhlLeadRow, GhlLeadsSummary } from "@/shared/types/ghlLeads";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const EMPTY_SUMMARY: GhlLeadsSummary = {
  total: 0,
  today: 0,
  week: 0,
  month: 0,
  timezone: "",
  generatedAt: "",
};
const SPREADSHEET_FORMULA_PREFIX = /^\s*[=+\-@]/;

export default function LeadsPage() {
  const [items, setItems] = useState<GhlLeadRow[]>([]);
  const [summary, setSummary] = useState<GhlLeadsSummary>(EMPTY_SUMMARY);
  const [pipelineName, setPipelineName] = useState("SALES 3 LINE");
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [canSync, setCanSync] = useState(false);
  const [lastSyncedCount, setLastSyncedCount] = useState(0);

  const statusText = useMemo(() => {
    if (isLoading) {
      return "Refreshing only today's leads from GoHighLevel...";
    }

    if (loadError) {
      return loadError;
    }

    if (!items.length) {
      return "Press Refresh to load today's leads.";
    }

    return `Loaded ${items.length} leads for today. Last sync added/updated: ${lastSyncedCount}.`;
  }, [isLoading, items.length, lastSyncedCount, loadError]);

  const loadLeads = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const payload = await getGhlLeads("incremental", {
        todayOnly: true,
      });
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
    setIsLoading(false);
    void getSession()
      .then((session) => {
        setCanSync(Boolean(session?.permissions?.sync_client_managers));
      })
      .catch(() => {
        setCanSync(false);
      });
  }, []);

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
        key: "leadType",
        label: "Lead Type",
        align: "left",
        cell: (item) => item.leadType || "-",
      },
      {
        key: "contactName",
        label: "Contact",
        align: "left",
        cell: (item) => item.contactName || "-",
      },
      {
        key: "source",
        label: "Source",
        align: "left",
        cell: (item) => item.source || "-",
      },
      {
        key: "pipelineName",
        label: "Pipeline",
        align: "left",
        cell: (item) => item.pipelineName || "-",
      },
      {
        key: "assignedTo",
        label: "Assigned",
        align: "left",
        cell: (item) => item.assignedTo || "-",
      },
      {
        key: "phone",
        label: "Phone",
        align: "left",
        cell: (item) => item.phone || "-",
      },
      {
        key: "email",
        label: "Email",
        align: "left",
        cell: (item) => item.email || "-",
      },
      {
        key: "stageName",
        label: "Stage",
        align: "left",
        cell: (item) => item.stageName || "-",
      },
      {
        key: "notes",
        label: "Notes",
        align: "left",
        cell: (item) => formatNotesPreview(item.notes),
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
    if (!items.length) {
      return;
    }
    exportLeadsToCsv(items, "today");
  }, [items]);

  return (
    <PageShell className="leads-react-page">
      <Panel
        className="table-panel leads-react-table-panel"
        title={`Leads (${pipelineName})`}
        actions={
          <div className="leads-toolbar-react">
            <Button
              type="button"
              size="sm"
              variant="secondary"
              onClick={handleExportCsv}
              disabled={isLoading || !items.length}
            >
              Export CSV
            </Button>
            <Button
              type="button"
              size="sm"
              onClick={() => void loadLeads()}
              disabled={isLoading || !canSync}
              isLoading={isLoading}
            >
              Refresh
            </Button>
          </div>
        }
      >
        {!loadError ? <p className="dashboard-message leads-status">{statusText}</p> : null}

        <div className="leads-summary-react" aria-live="polite">
          <SummaryCard title="Today" value={summary.today || items.length} />
        </div>

        {isLoading ? <LoadingSkeleton rows={8} /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load leads"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadLeads()}
          />
        ) : null}

        {!isLoading && !loadError && !items.length ? (
          <EmptyState title="Press Refresh to load today's leads." />
        ) : null}

        {!isLoading && !loadError && items.length ? (
          <Table
            className="leads-react-table-wrap"
            columns={columns}
            rows={items}
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

function exportLeadsToCsv(rows: GhlLeadRow[], filter: string): void {
  const headers = [
    "Created On",
    "Lead",
    "Lead Type",
    "Contact",
    "Source",
    "Pipeline",
    "Assigned",
    "Phone",
    "Email",
    "Stage",
    "Notes",
    "Status",
    "Amount",
    "Lead ID",
  ];
  const lines = [headers.map(escapeCsvValue).join(",")];

  for (const row of rows) {
    lines.push(
      [
        formatDateTime(row.createdOn),
        row.opportunityName || "",
        row.leadType || "",
        row.contactName || "",
        row.source || "",
        row.pipelineName || "",
        row.assignedTo || "",
        row.phone || "",
        row.email || "",
        row.stageName || "",
        row.notes || "",
        formatStatus(row.status),
        normalizeNumberForCsv(row.monetaryValue),
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

function formatNotesPreview(rawValue: string): string {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "-";
  }

  if (value.length <= 120) {
    return value;
  }

  return `${value.slice(0, 117)}...`;
}
