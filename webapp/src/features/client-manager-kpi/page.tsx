import { Fragment, useCallback, useEffect, useMemo, useState } from "react";

import { getClientManagerKpi } from "@/shared/api";
import type { ClientManagerKpiClientRow, ClientManagerKpiRow } from "@/shared/types/clientManagerKpi";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel } from "@/shared/ui";

export default function ClientManagerKpiPage() {
  const [rows, setRows] = useState<ClientManagerKpiRow[]>([]);
  const [month, setMonth] = useState("");
  const [updatedAt, setUpdatedAt] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [expandedManagers, setExpandedManagers] = useState<Record<string, boolean>>({});

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

  const toggleManagerExpanded = useCallback((managerName: string) => {
    setExpandedManagers((prev) => {
      const current = Boolean(prev[managerName]);
      return {
        ...prev,
        [managerName]: !current,
      };
    });
  }, []);

  const sortedRows = useMemo(() => {
    return rows.slice().sort((left, right) => {
      if (right.kpiPercent !== left.kpiPercent) {
        return right.kpiPercent - left.kpiPercent;
      }
      return left.managerName.localeCompare(right.managerName, "en", { sensitivity: "base" });
    });
  }, [rows]);

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
        <p className="dashboard-message">
          KPI formula: clients with payment in current month / clients in KPI base (all not fully paid clients).
          Bonus rules: &lt;75% = $0, 76-80.99% = $150, 81%+ = $300.
        </p>
        {isLoading ? (
          <LoadingSkeleton rows={8} />
        ) : loadError ? (
          <ErrorState
            title="Failed to load KPI data."
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadKpi(true)}
          />
        ) : !sortedRows.length ? (
          <EmptyState title="No KPI rows yet." description="No managers with active KPI base were found." />
        ) : (
          <div className="cb-table-wrap kpi-client-manager-table-wrap">
            <table className="cb-table kpi-client-manager-table">
              <thead>
                <tr>
                  <th className="cb-table__head-cell cb-table__cell--align-left">MANAGER</th>
                  <th className="cb-table__head-cell cb-table__cell--align-right">KPI %</th>
                  <th className="cb-table__head-cell cb-table__cell--align-left">HOW CALCULATED</th>
                  <th className="cb-table__head-cell cb-table__cell--align-center">RESULT</th>
                  <th className="cb-table__head-cell cb-table__cell--align-right">BONUS</th>
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row) => {
                  const managerName = row.managerName || "No manager";
                  const isExpanded = Boolean(expandedManagers[managerName]);
                  return (
                    <Fragment key={managerName}>
                      <tr className="kpi-client-manager-summary-row">
                        <td className="cb-table__cell">
                          <button
                            type="button"
                            className="kpi-client-manager-name-button"
                            onClick={() => toggleManagerExpanded(managerName)}
                            aria-expanded={isExpanded}
                          >
                            {managerName}
                          </button>
                        </td>
                        <td className="cb-table__cell cb-table__cell--align-right">{row.kpiPercent.toFixed(2)}%</td>
                        <td className="cb-table__cell">
                          <strong>{row.calculationLabel}</strong>
                          <span className="kpi-client-manager-calc-hint">{row.calculationDescription}</span>
                        </td>
                        <td className="cb-table__cell cb-table__cell--align-center">
                          <Badge tone={row.isKpiReached ? "success" : "danger"}>
                            {row.isKpiReached ? "KPI done" : "KPI not done"}
                          </Badge>
                        </td>
                        <td className="cb-table__cell cb-table__cell--align-right">${row.bonusUsd.toFixed(0)}</td>
                      </tr>
                      {isExpanded ? (
                        <tr className="kpi-client-manager-details-row">
                          <td className="cb-table__cell" colSpan={5}>
                            <ManagerClientsDetails monthLabel={monthLabel} clients={row.clients || []} />
                          </td>
                        </tr>
                      ) : null}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    </PageShell>
  );
}

function ManagerClientsDetails(props: { monthLabel: string; clients: ClientManagerKpiClientRow[] }) {
  const { monthLabel, clients } = props;
  if (!Array.isArray(clients) || !clients.length) {
    return (
      <div className="kpi-client-manager-details-empty">
        <p className="dashboard-message">No clients in KPI base for this manager.</p>
      </div>
    );
  }

  return (
    <div className="kpi-client-manager-details">
      <p className="dashboard-message">Clients who should pay in {monthLabel}: paid or not paid this month.</p>
      <table className="cb-table cb-table--compact kpi-client-manager-details-table">
        <thead>
          <tr>
            <th className="cb-table__head-cell cb-table__cell--align-left">CLIENT</th>
            <th className="cb-table__head-cell cb-table__cell--align-center">SHOULD PAY THIS MONTH</th>
            <th className="cb-table__head-cell cb-table__cell--align-center">PAID THIS MONTH</th>
            <th className="cb-table__head-cell cb-table__cell--align-center">PAYMENT DATES</th>
            <th className="cb-table__head-cell cb-table__cell--align-right">PAYMENT TOTAL</th>
          </tr>
        </thead>
        <tbody>
          {clients.map((client) => (
            <tr key={buildClientDetailRowKey(client)}>
              <td className="cb-table__cell">{client.clientName || "Unknown client"}</td>
              <td className="cb-table__cell cb-table__cell--align-center">
                <Badge tone={client.shouldPayThisMonth ? "info" : "neutral"}>{client.shouldPayThisMonth ? "Yes" : "No"}</Badge>
              </td>
              <td className="cb-table__cell cb-table__cell--align-center">
                <Badge tone={client.paidThisMonth ? "success" : "danger"}>{client.paidThisMonth ? "Paid" : "Not paid"}</Badge>
              </td>
              <td className="cb-table__cell cb-table__cell--align-center">
                {formatPaymentDates(client.paymentDatesThisMonth)}
              </td>
              <td className="cb-table__cell cb-table__cell--align-right">
                {formatCurrency(client.totalPaidThisMonth)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function buildClientDetailRowKey(client: ClientManagerKpiClientRow): string {
  const byId = String(client?.clientId || "").trim();
  if (byId) {
    return byId;
  }
  return String(client?.clientName || "unknown-client");
}

function formatPaymentDates(dates: string[]): string {
  if (!Array.isArray(dates) || !dates.length) {
    return "-";
  }
  return dates.join(", ");
}

function formatCurrency(amount: number): string {
  if (!Number.isFinite(amount)) {
    return "$0.00";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
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
