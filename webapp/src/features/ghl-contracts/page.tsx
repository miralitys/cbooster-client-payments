import { useCallback, useEffect, useMemo, useState } from "react";

import { getGhlClientDocuments } from "@/shared/api";
import type { GhlClientDocument, GhlClientDocumentsRow } from "@/shared/types/ghlDocuments";
import { Badge, Button, EmptyState, ErrorState, LoadingSkeleton, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const DEFAULT_LIMIT = 10;

export default function GhlContractsPage() {
  const [items, setItems] = useState<GhlClientDocumentsRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [statusText, setStatusText] = useState("Loading first 10 clients from database and all documents from GoHighLevel...");

  const loadDocuments = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");
    setStatusText("Loading first 10 clients from database and all documents from GoHighLevel...");

    try {
      const payload = await getGhlClientDocuments(DEFAULT_LIMIT);
      const nextItems = Array.isArray(payload.items) ? payload.items : [];
      const clientsWithDocuments = nextItems.filter((item) => (item.status || "").toLowerCase() === "found").length;
      const totalDocuments = nextItems.reduce((sum, item) => sum + (Number(item.documentsCount) || 0), 0);

      setItems(nextItems);
      setStatusText(
        `Loaded ${nextItems.length} clients. Found ${totalDocuments} documents for ${clientsWithDocuments} clients.`,
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load client document table.";
      setItems([]);
      setLoadError(message);
      setStatusText(message);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadDocuments();
  }, [loadDocuments]);

  const columns = useMemo<TableColumn<GhlClientDocumentsRow>[]>(() => {
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
        cell: (item) => `${item.contactName || "-"} (${Number(item.matchedContacts) || 0})`,
      },
      {
        key: "documents",
        label: "Documents",
        align: "left",
        cell: (item) => <DocumentsCell documents={item.documents} fallback={item.contractTitle || "-"} />,
      },
      {
        key: "status",
        label: "Status",
        align: "left",
        cell: (item) => <Badge tone={statusToBadgeTone((item.status || "").toLowerCase())}>{formatStatus(item.status)}</Badge>,
      },
    ];
  }, []);

  return (
    <PageShell className="ghl-documents-react-page">
      <PageHeader
        title="GHL Documents Test"
        subtitle="Separate test page. First 10 clients from your database with read-only GoHighLevel lookup and all found documents."
      />

      <Panel
        className="table-panel ghl-documents-react-table-panel"
        title="Client - Documents Table"
        actions={
          <Button
            type="button"
            size="sm"
            onClick={() => void loadDocuments()}
            isLoading={isLoading}
            disabled={isLoading}
          >
            Refresh
          </Button>
        }
      >
        {!loadError ? <p className="dashboard-message ghl-documents-status">{statusText}</p> : null}

        {isLoading ? <LoadingSkeleton rows={8} /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load GHL documents"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadDocuments()}
          />
        ) : null}

        {!isLoading && !loadError && !items.length ? <EmptyState title="No clients found." /> : null}

        {!isLoading && !loadError && items.length ? (
          <Table
            className="ghl-documents-react-table-wrap"
            columns={columns}
            rows={items}
            rowKey={(item, index) => `${item.clientName || "client"}-${index}`}
            density="compact"
          />
        ) : null}
      </Panel>
    </PageShell>
  );
}

function DocumentsCell({ documents, fallback }: { documents: GhlClientDocument[]; fallback: string }) {
  const items = Array.isArray(documents) ? documents : [];
  if (!items.length) {
    return <span>{fallback || "-"}</span>;
  }

  return (
    <ul className="ghl-documents-list">
      {items.map((documentItem, index) => {
        const title = (documentItem?.title || "Document").toString();
        const url = (documentItem?.url || "").toString().trim();
        const details = [documentItem?.source, documentItem?.isContractMatch ? "contract" : "", documentItem?.snippet]
          .map((value) => String(value || "").trim())
          .filter(Boolean)
          .join(" | ");

        return (
          <li key={`${title}-${index}`} className="ghl-documents-list__item">
            {url ? (
              <a href={url} target="_blank" rel="noopener noreferrer" className="ghl-documents-link">
                {title}
              </a>
            ) : (
              <span className="ghl-documents-link ghl-documents-link--plain">{title}</span>
            )}
            {details ? <span className="ghl-documents-meta">{details}</span> : null}
          </li>
        );
      })}
    </ul>
  );
}

function statusToBadgeTone(status: string): "success" | "info" | "warning" | "danger" {
  if (status === "found") {
    return "success";
  }
  if (status === "possible") {
    return "info";
  }
  if (status === "not_found") {
    return "warning";
  }
  return "danger";
}

function formatStatus(status: string): string {
  const normalized = (status || "").toString().toLowerCase();
  if (normalized === "found") {
    return "Documents found";
  }
  if (normalized === "possible") {
    return "Possible match";
  }
  if (normalized === "not_found") {
    return "Not found";
  }
  if (normalized === "error") {
    return "Lookup error";
  }
  return "Unknown";
}
