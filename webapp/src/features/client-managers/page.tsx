import { useCallback, useEffect, useMemo, useState } from "react";

import { getClientManagers, getSession } from "@/shared/api";
import type { ClientManagerRow } from "@/shared/types/clientManagers";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

type RefreshMode = "none" | "incremental" | "full";

interface RefreshSummary {
  total: number;
  refreshed: number;
  mode: RefreshMode;
}

export default function ClientManagersPage() {
  const [items, setItems] = useState<ClientManagerRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [noManagerOnly, setNoManagerOnly] = useState(false);
  const [canSync, setCanSync] = useState(false);
  const [currentMode, setCurrentMode] = useState<RefreshMode>("none");
  const [refreshSummary, setRefreshSummary] = useState<RefreshSummary>({
    total: 0,
    refreshed: 0,
    mode: "none",
  });

  const filteredItems = useMemo(() => {
    if (!noManagerOnly) {
      return items;
    }
    return items.filter(isNoManagerItem);
  }, [items, noManagerOnly]);

  const statusText = useMemo(() => {
    if (isLoading) {
      if (currentMode === "full") {
        return "Running total refresh for all clients...";
      }
      if (currentMode === "incremental") {
        return "Refreshing only new clients...";
      }
      return "Loading saved client-manager data...";
    }

    if (loadError) {
      return loadError;
    }

    if (!refreshSummary.total) {
      return noManagerOnly ? "No manager filter is enabled. Press Refresh to load data." : "Press Refresh to load data.";
    }

    if (noManagerOnly) {
      return `Showing ${filteredItems.length} client${filteredItems.length === 1 ? "" : "s"} without manager out of ${refreshSummary.total}.`;
    }

    return `Loaded ${refreshSummary.total} client${refreshSummary.total === 1 ? "" : "s"}. Refreshed: ${refreshSummary.refreshed}.`;
  }, [currentMode, filteredItems.length, isLoading, loadError, noManagerOnly, refreshSummary.refreshed, refreshSummary.total]);

  const loadClientManagers = useCallback(async (mode: RefreshMode = "none") => {
    setIsLoading(true);
    setLoadError("");
    setCurrentMode(mode);

    try {
      const payload = await getClientManagers(mode);
      const nextItems = Array.isArray(payload.items) ? payload.items : [];
      const refreshed = Number.isFinite(payload?.refresh?.refreshedClientsCount)
        ? Number(payload.refresh?.refreshedClientsCount)
        : 0;

      setItems(nextItems);
      setRefreshSummary({
        total: nextItems.length,
        refreshed,
        mode,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load client-manager table.";
      setLoadError(message);
      setItems([]);
      setRefreshSummary({
        total: 0,
        refreshed: 0,
        mode,
      });
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

    void loadClientManagers("none");
  }, [loadClientManagers]);

  const tableColumns = useMemo<TableColumn<ClientManagerRow>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (item) => item.clientName || "-",
      },
      {
        key: "managersLabel",
        label: "Managers",
        align: "left",
        cell: (item) => item.managersLabel || "-",
      },
      {
        key: "matchedContacts",
        label: "Matched Contacts",
        align: "center",
        cell: (item) => String(Number(item.matchedContacts) || 0),
      },
      {
        key: "status",
        label: "Status",
        align: "left",
        cell: (item) => {
          const status = (item.status || "").toLowerCase();
          const tone = statusToBadgeTone(status);
          const label = formatStatusLabel(status);
          return <Badge tone={tone}>{label}</Badge>;
        },
      },
    ];
  }, []);

  return (
    <PageShell className="client-managers-react-page">
      <Panel
        className="table-panel client-managers-react-table-panel"
        title="Client - Manager Table"
        actions={
          <div className="client-managers-toolbar-react">
            <label htmlFor="client-managers-no-manager-only" className="cb-checkbox-row client-managers-no-manager-only">
              <input
                id="client-managers-no-manager-only"
                type="checkbox"
                checked={noManagerOnly}
                onChange={(event) => setNoManagerOnly(event.target.checked)}
              />
              No manager
            </label>
            <Button
              type="button"
              size="sm"
              onClick={() => void loadClientManagers("incremental")}
              disabled={isLoading || !canSync}
              isLoading={isLoading && currentMode === "incremental"}
            >
              Refresh
            </Button>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => void loadClientManagers("full")}
              disabled={isLoading || !canSync}
              isLoading={isLoading && currentMode === "full"}
            >
              Total Refresh
            </Button>
          </div>
        }
      >
        {!loadError ? <p className="dashboard-message client-managers-status">{statusText}</p> : null}

        {isLoading ? <LoadingSkeleton rows={8} /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load client-manager table"
            description={loadError}
            actionLabel={canSync ? "Retry" : undefined}
            onAction={canSync ? () => void loadClientManagers("none") : undefined}
          />
        ) : null}

        {!isLoading && !loadError && !filteredItems.length ? (
          <EmptyState title={noManagerOnly ? "No clients without manager." : "No clients found."} />
        ) : null}

        {!isLoading && !loadError && filteredItems.length ? (
          <Table
            className="client-managers-react-table-wrap"
            columns={tableColumns}
            rows={filteredItems}
            rowKey={(item, index) => `${item.clientName || "client"}-${index}`}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function isNoManagerItem(item: ClientManagerRow): boolean {
  const statusValue = (item?.status || "").toString().trim().toLowerCase();
  if (statusValue === "unassigned") {
    return true;
  }

  const managers = Array.isArray(item?.managers)
    ? item.managers.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  if (!managers.length) {
    return true;
  }

  const managersLabel = (item?.managersLabel || "").toString().trim().toLowerCase();
  return !managersLabel || managersLabel === "-" || managersLabel === "unassigned";
}

function statusToBadgeTone(status: string): "success" | "warning" | "danger" {
  if (status === "assigned") {
    return "success";
  }
  if (status === "unassigned") {
    return "warning";
  }
  return "danger";
}

function formatStatusLabel(status: string): string {
  if (status === "assigned") {
    return "Assigned";
  }
  if (status === "unassigned") {
    return "No manager";
  }
  if (status === "error") {
    return "Lookup error";
  }
  return "Unknown";
}
