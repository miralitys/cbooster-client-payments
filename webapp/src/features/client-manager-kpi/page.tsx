import { useCallback, useEffect, useMemo, useState } from "react";

import { getClientManagerKpi } from "@/shared/api";
import type { ClientManagerKpiRow } from "@/shared/types/clientManagerKpi";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

export default function ClientManagerKpiPage() {
  const [rows, setRows] = useState<ClientManagerKpiRow[]>([]);
  const [month, setMonth] = useState("");
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [loadError, setLoadError] = useState("");

  const loadKpi = useCallback(async (isManualRefresh = false) => {
    if (isManualRefresh) {
      setIsRefreshing(true);
    } else {
      setIsLoading(true);
    }
    setLoadError("");

    try {
      const payload = await getClientManagerKpi();
      setRows(Array.isArray(payload.rows) ? payload.rows : []);
      setMonth(typeof payload.month === "string" ? payload.month : "");
      setUpdatedAt(typeof payload.updatedAt === "string" ? payload.updatedAt : null);
    } catch (error) {
      setRows([]);
      setLoadError(error instanceof Error ? error.message : "Failed to load KPI data.");
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  }, []);

  useEffect(() => {
    void loadKpi(false);
  }, [loadKpi]);

  const sortedRows = useMemo(() => {
    return rows.slice().sort((left, right) => {
      if (right.kpiPercent !== left.kpiPercent) {
        return right.kpiPercent - left.kpiPercent;
      }
      return left.managerName.localeCompare(right.managerName, "en", { sensitivity: "base" });
    });
  }, [rows]);

  const columns = useMemo<TableColumn<ClientManagerKpiRow>[]>(() => [
    {
      key: "manager",
      label: "MANAGER",
      cell: (row) => row.managerName || "No manager",
      className: "kpi-client-manager__manager-cell",
    },
    {
      key: "kpiPercent",
      label: "KPI %",
      align: "right",
      cell: (row) => `${row.kpiPercent.toFixed(2)}%`,
      className: "kpi-client-manager__percent-cell",
    },
    {
      key: "status",
      label: "RESULT",
      align: "center",
      cell: (row) => (
        <Badge tone={row.isKpiReached ? "success" : "danger"}>
          {row.isKpiReached ? "KPI done" : "KPI not done"}
        </Badge>
      ),
    },
    {
      key: "bonus",
      label: "BONUS",
      align: "right",
      cell: (row) => `$${row.bonusUsd.toFixed(0)}`,
    },
  ], []);

  const monthLabel = month ? formatMonthLabel(month) : "Current month";
  const updatedAtLabel = updatedAt ? formatDateTime(updatedAt) : "Not synced yet";

  return (
    <PageShell className="kpi-client-manager-page">
      <PageHeader
        title="KPI Client Manager"
        subtitle={`Month: ${monthLabel}`}
        meta={(
          <span className="client-payments-page-header-meta">
            Last sync: {updatedAtLabel}
          </span>
        )}
        actions={(
          <Button
            type="button"
            variant="secondary"
            onClick={() => void loadKpi(true)}
            disabled={isLoading || isRefreshing}
          >
            {isRefreshing ? "Refreshing..." : "Refresh"}
          </Button>
        )}
      />

      <Panel title="Client Manager KPI">
        <p className="dashboard-message">KPI % = clients with payment this month / clients not fully paid.</p>
        {isLoading ? (
          <LoadingSkeleton rows={8} />
        ) : loadError ? (
          <ErrorState
            title="Failed to load KPI data."
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadKpi(true)}
          />
        ) : (
          <Table
            columns={columns}
            rows={sortedRows}
            rowKey={(row) => row.managerName}
            emptyState={<EmptyState title="No KPI rows yet." description="No managers with active KPI base were found." />}
          />
        )}
      </Panel>
    </PageShell>
  );
}

function formatMonthLabel(rawMonth: string): string {
  const monthMatch = String(rawMonth || "").match(/^(\d{4})-(\d{2})$/);
  if (!monthMatch) {
    return rawMonth || "Current month";
  }
  const year = Number.parseInt(monthMatch[1], 10);
  const month = Number.parseInt(monthMatch[2], 10);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return rawMonth;
  }

  return new Intl.DateTimeFormat("en-US", { month: "long", year: "numeric" })
    .format(new Date(Date.UTC(year, month - 1, 1)));
}

function formatDateTime(rawValue: string): string {
  const date = new Date(rawValue);
  if (!Number.isFinite(date.getTime())) {
    return rawValue;
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
