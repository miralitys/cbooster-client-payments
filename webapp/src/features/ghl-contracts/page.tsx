import { type FormEvent, useMemo, useState } from "react";

import { getGhlContractPdf } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { GhlContractPdfRequest, GhlContractPdfResult } from "@/shared/types/ghlContractPdf";
import { Badge, Button, EmptyState, Field, Input, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface GhlContractTextFormState {
  clientName: string;
  locationId: string;
}

interface GhlContractTextHistoryRow extends GhlContractPdfResult {
  id: string;
}

const HISTORY_MAX_ROWS = 20;
export default function GhlContractsPage() {
  const [form, setForm] = useState<GhlContractTextFormState>({
    clientName: "",
    locationId: "",
  });
  const [isLoading, setIsLoading] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [statusMessage, setStatusMessage] = useState("Ready to load contract PDF from GoHighLevel API.");
  const [latestResult, setLatestResult] = useState<GhlContractPdfResult | null>(null);
  const [historyRows, setHistoryRows] = useState<GhlContractTextHistoryRow[]>([]);

  const historyColumns = useMemo<TableColumn<GhlContractTextHistoryRow>[]>(() => {
    return [
      {
        key: "client",
        label: "Client",
        align: "left",
        cell: (row) => (
          <div>
            <strong>{row.clientName || "Unnamed client"}</strong>
            <div className="react-user-footnote">{row.contactName || "-"}</div>
          </div>
        ),
      },
      {
        key: "status",
        label: "Status",
        align: "center",
        cell: (row) => <Badge tone={row.status === "ok" ? "success" : "warning"}>{row.status}</Badge>,
      },
      {
        key: "size",
        label: "PDF Size",
        align: "center",
        cell: (row) => formatSizeBytes(row.sizeBytes),
      },
      {
        key: "fetchedAt",
        label: "Checked At",
        align: "center",
        cell: (row) => formatDateTime(row.fetchedAt),
      },
    ];
  }, []);

  async function executeContractTextRequest(request: GhlContractPdfRequest) {
    setIsLoading(true);
    setStatusMessage("Fetching contract PDF from GoHighLevel API...");
    try {
      const payload = await getGhlContractPdf(request);

      const result = payload?.result;
      if (!result?.fileBase64) {
        throw new Error("GoHighLevel returned an empty contract PDF result.");
      }

      setLatestResult(result);
      setHistoryRows((previous) => {
        const nextItem: GhlContractTextHistoryRow = {
          ...result,
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        };
        return [nextItem, ...previous].slice(0, HISTORY_MAX_ROWS);
      });
      setStatusMessage(`Last extraction completed at ${formatDateTime(result.fetchedAt)}.`);
      showToast({
        type: "success",
        message: "Contract PDF loaded.",
        dedupeKey: "ghl-contract-pdf-success",
        cooldownMs: 2200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load GoHighLevel contract PDF.";
      setSubmitError(message);
      setStatusMessage("GoHighLevel contract PDF request failed.");
      showToast({
        type: "error",
        message,
        dedupeKey: `ghl-contract-pdf-error-${message}`,
        cooldownMs: 2200,
      });
    } finally {
      setIsLoading(false);
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError("");

    const validationError = validateGhlContractTextForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    const request: GhlContractPdfRequest = {
      clientName: form.clientName.trim(),
      locationId: form.locationId.trim() || undefined,
    };
    await executeContractTextRequest(request);
  }

  return (
    <PageShell className="ghl-contracts-react-page">
      <PageHeader
        title="GoHighLevel Contract PDF"
        subtitle="API-based contract PDF extraction"
        meta={
          <>
            <p className={`dashboard-message ${submitError ? "error" : ""}`.trim()}>{submitError || statusMessage}</p>
            <p className="react-user-footnote">
              Uses server credentials: `GHL_API_KEY` and `GHL_LOCATION_ID`. You can override location ID below.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Extract Contract PDF">
        <form className="ghl-contracts-form" onSubmit={handleSubmit}>
          <Field label="Client Name" htmlFor="ghl-contract-client-name">
            <Input
              id="ghl-contract-client-name"
              autoComplete="off"
              value={form.clientName}
              onChange={(event) => setForm((previous) => ({ ...previous, clientName: event.target.value }))}
              placeholder="Vladyslav Novosiadlyi"
              hasError={Boolean(submitError) && !form.clientName.trim()}
            />
          </Field>

          <Field label="Location ID (optional)" htmlFor="ghl-contract-location-id">
            <Input
              id="ghl-contract-location-id"
              autoComplete="off"
              value={form.locationId}
              onChange={(event) => setForm((previous) => ({ ...previous, locationId: event.target.value }))}
              placeholder="XTqqycBohnAAVy4uneZR"
            />
          </Field>

          <div className="ghl-contracts-actions">
            <Button type="submit" size="sm" isLoading={isLoading}>
              Get Contract PDF
            </Button>
          </div>
        </form>
      </Panel>

      <Panel className="table-panel" title="Latest PDF Result">
        {!latestResult ? (
          <EmptyState title="No contract PDF loaded yet." description="Run an extraction to see the PDF here." />
        ) : (
          <div className="ghl-contracts-result">
            <div className="ghl-contracts-summary">
              <div>
                <p className="react-user-footnote">Client</p>
                <p className="ghl-contracts-summary__value">{latestResult.clientName || "Unnamed client"}</p>
              </div>
              <div>
                <p className="react-user-footnote">Status</p>
                <Badge tone={latestResult.status === "ok" ? "success" : "warning"}>{latestResult.status}</Badge>
              </div>
              <div>
                <p className="react-user-footnote">PDF Size</p>
                <p className="ghl-contracts-summary__value">{formatSizeBytes(latestResult.sizeBytes)}</p>
              </div>
            </div>

            <div className="ghl-contracts-meta">
              <p className="react-user-footnote">
                Checked at: {formatDateTime(latestResult.fetchedAt)} ({latestResult.elapsedMs} ms)
              </p>
              <p className="react-user-footnote">
                Source: {latestResult.source || "-"}
              </p>
              {latestResult.note ? <p className="react-user-footnote">{latestResult.note}</p> : null}
              {latestResult.fileName ? <p className="react-user-footnote">File: {latestResult.fileName}</p> : null}
              {latestResult.contractUrl ? (
                <p className="react-user-footnote">
                  Contract URL:
                  {" "}
                  <a href={latestResult.contractUrl} target="_blank" rel="noreferrer">
                    {latestResult.contractUrl}
                  </a>
                </p>
              ) : null}
            </div>

            <div className="ghl-contracts-text">
              <iframe
                className="ghl-contracts-pdf"
                title="Contract PDF"
                src={`data:${latestResult.mimeType || "application/pdf"};base64,${latestResult.fileBase64}`}
              />
            </div>
          </div>
        )}
      </Panel>

      <Panel className="table-panel" title="Recent Extractions">
        <Table
          columns={historyColumns}
          rows={historyRows}
          rowKey={(row) => row.id}
          className="ghl-contracts-history-wrap"
          emptyState="No extraction history in this browser session yet."
          density="compact"
        />
      </Panel>
    </PageShell>
  );
}

function validateGhlContractTextForm(form: GhlContractTextFormState): string {
  if (!form.clientName.trim()) {
    return "Client name is required.";
  }

  return "";
}

function formatSizeBytes(value: number): string {
  const size = Number.isFinite(value) ? Number(value) : 0;
  if (size <= 0) {
    return "0 B";
  }
  if (size < 1024) {
    return `${size} B`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  return `${(size / (1024 * 1024)).toFixed(2)} MB`;
}

function formatDateTime(rawValue: string): string {
  const date = new Date(rawValue);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
