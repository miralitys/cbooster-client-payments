import { useCallback, useEffect, useMemo, useState } from "react";

import { downloadGhlClientContract, getGhlClientContracts } from "@/shared/api";
import type { GhlClientContractRow } from "@/shared/types/ghlDocuments";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const DEFAULT_LIMIT = 25;

export default function GhlContractsPage() {
  const [items, setItems] = useState<GhlClientContractRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [statusText, setStatusText] = useState("Loading client list and searching signed PDF contracts in GoHighLevel...");
  const [downloadingMap, setDownloadingMap] = useState<Record<string, boolean>>({});

  const loadContracts = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");
    setStatusText("Loading client list and searching signed PDF contracts in GoHighLevel...");

    try {
      const payload = await getGhlClientContracts(DEFAULT_LIMIT);
      const nextItems = Array.isArray(payload.items) ? payload.items : [];
      const readyCount = nextItems.filter((item) => normalizeStatus(item.status) === "ready").length;

      setItems(nextItems);
      setStatusText(`Loaded ${nextItems.length} clients. Contracts ready for download: ${readyCount}.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load contract download table.";
      setItems([]);
      setLoadError(message);
      setStatusText(message);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadContracts();
  }, [loadContracts]);

  const handleDownload = useCallback(async (item: GhlClientContractRow) => {
    const status = normalizeStatus(item.status);
    if (status !== "ready") {
      return;
    }

    const rowKey = buildRowKey(item);
    setDownloadingMap((prev) => ({
      ...prev,
      [rowKey]: true,
    }));

    try {
      const { blob, fileName } = await downloadGhlClientContract(item.clientName, item.contactId);
      triggerBlobDownload(blob, ensurePdfFileName(fileName || item.contractTitle || item.clientName));
      setStatusText(`Downloaded PDF contract for "${item.clientName}".`);
    } catch (error) {
      const message = error instanceof Error ? error.message : `Failed to download contract for "${item.clientName}".`;
      setStatusText(message);
    } finally {
      setDownloadingMap((prev) => {
        const next = { ...prev };
        delete next[rowKey];
        return next;
      });
    }
  }, []);

  const columns = useMemo<TableColumn<GhlClientContractRow>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (item) => item.clientName || "-",
      },
      {
        key: "contactName",
        label: "Matched Contact",
        align: "left",
        cell: (item) => `${item.contactName || "-"} (${Math.max(0, Number(item.matchedContacts) || 0)})`,
      },
      {
        key: "contract",
        label: "Contract",
        align: "left",
        cell: (item) => (
          <div className="ghl-contracts-contract-cell">
            <span className="ghl-contracts-contract-title">{item.contractTitle || "-"}</span>
            {item.contractUrl ? (
              <a href={item.contractUrl} target="_blank" rel="noopener noreferrer" className="ghl-contracts-open-link">
                Open in GHL
              </a>
            ) : null}
          </div>
        ),
      },
      {
        key: "source",
        label: "Source",
        align: "left",
        cell: (item) => item.source || "-",
      },
      {
        key: "status",
        label: "Status",
        align: "left",
        cell: (item) => <Badge tone={statusToBadgeTone(item.status)}>{formatStatus(item.status)}</Badge>,
      },
      {
        key: "actions",
        label: "Download",
        align: "left",
        cell: (item) => {
          const rowKey = buildRowKey(item);
          const canDownload = normalizeStatus(item.status) === "ready";
          return (
            <div className="ghl-contracts-actions">
              <Button
                type="button"
                size="sm"
                onClick={() => void handleDownload(item)}
                isLoading={Boolean(downloadingMap[rowKey])}
                disabled={!canDownload}
              >
                Download PDF
              </Button>
            </div>
          );
        },
      },
    ];
  }, [downloadingMap, handleDownload]);

  return (
    <PageShell className="ghl-contracts-react-page">
      <Panel
        className="table-panel ghl-contracts-react-table-panel"
        title="GoHighLevel Client Contracts (PDF Download)"
        actions={
          <Button
            type="button"
            size="sm"
            onClick={() => void loadContracts()}
            isLoading={isLoading}
            disabled={isLoading}
          >
            Refresh
          </Button>
        }
      >
        {!loadError ? <p className="dashboard-message ghl-contracts-status">{statusText}</p> : null}

        {isLoading ? <LoadingSkeleton rows={8} /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load contract download table"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadContracts()}
          />
        ) : null}

        {!isLoading && !loadError && !items.length ? <EmptyState title="No clients found." /> : null}

        {!isLoading && !loadError && items.length ? (
          <Table
            className="ghl-contracts-react-table-wrap"
            columns={columns}
            rows={items}
            rowKey={(item) => buildRowKey(item)}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function buildRowKey(item: GhlClientContractRow): string {
  return `${item.clientName || "client"}::${item.contactId || "-"}`;
}

function triggerBlobDownload(blob: Blob, fileName: string) {
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = fileName;
  anchor.rel = "noopener";
  document.body.append(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(objectUrl);
}

function ensurePdfFileName(fileName: string): string {
  const normalized = (fileName || "").toString().trim() || "contract";
  return normalized.toLowerCase().endsWith(".pdf") ? normalized : `${normalized}.pdf`;
}

function normalizeStatus(status: string): "ready" | "no_contact" | "no_contract" | "error" {
  const normalized = (status || "").toString().trim().toLowerCase();
  if (normalized === "ready") {
    return "ready";
  }
  if (normalized === "no_contact") {
    return "no_contact";
  }
  if (normalized === "no_contract") {
    return "no_contract";
  }
  return "error";
}

function statusToBadgeTone(status: string): "success" | "info" | "warning" | "danger" {
  const normalized = normalizeStatus(status);
  if (normalized === "ready") {
    return "success";
  }
  if (normalized === "no_contact" || normalized === "no_contract") {
    return "warning";
  }
  return "danger";
}

function formatStatus(status: string): string {
  const normalized = normalizeStatus(status);
  if (normalized === "ready") {
    return "Ready to download";
  }
  if (normalized === "no_contact") {
    return "Contact not found";
  }
  if (normalized === "no_contract") {
    return "Contract not found";
  }
  if (normalized === "error") {
    return "Lookup error";
  }
  return "Unknown";
}
