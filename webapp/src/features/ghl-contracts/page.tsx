import { type FormEvent, useMemo, useState } from "react";

import { getGhlContractText } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { GhlContractTextRequest, GhlContractTextResult } from "@/shared/types/ghlContractText";
import { Badge, Button, EmptyState, Field, Input, PageHeader, PageShell, Panel, Table, Textarea } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface GhlContractTextFormState {
  clientName: string;
  locationId: string;
}

interface GhlContractTextHistoryRow extends GhlContractTextResult {
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
  const [statusMessage, setStatusMessage] = useState("Ready to extract contract text from GoHighLevel API.");
  const [latestResult, setLatestResult] = useState<GhlContractTextResult | null>(null);
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
        key: "length",
        label: "Text Length",
        align: "center",
        cell: (row) => `${Math.max(0, Number(row.textLength) || 0)} chars`,
      },
      {
        key: "fetchedAt",
        label: "Checked At",
        align: "center",
        cell: (row) => formatDateTime(row.fetchedAt),
      },
    ];
  }, []);

  async function executeContractTextRequest(request: GhlContractTextRequest) {
    setIsLoading(true);
    setStatusMessage("Fetching contract text from GoHighLevel API...");
    try {
      const payload = await getGhlContractText(request);

      const result = payload?.result;
      if (!result?.contractText) {
        throw new Error("GoHighLevel returned an empty contract text result.");
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
        message: "Contract text extracted.",
        dedupeKey: "ghl-contract-text-success",
        cooldownMs: 2200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to extract GoHighLevel contract text.";
      setSubmitError(message);
      setStatusMessage("GoHighLevel contract text request failed.");
      showToast({
        type: "error",
        message,
        dedupeKey: `ghl-contract-text-error-${message}`,
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

    const request: GhlContractTextRequest = {
      clientName: form.clientName.trim(),
      locationId: form.locationId.trim() || undefined,
    };
    await executeContractTextRequest(request);
  }

  return (
    <PageShell className="ghl-contracts-react-page">
      <PageHeader
        title="GoHighLevel Contract Text"
        subtitle="API-based contract text extraction"
        meta={
          <>
            <p className={`dashboard-message ${submitError ? "error" : ""}`.trim()}>{submitError || statusMessage}</p>
            <p className="react-user-footnote">
              Uses server credentials: `GHL_API_KEY` and `GHL_LOCATION_ID`. You can override location ID below.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Extract Contract Text">
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
              Get Contract Text
            </Button>
          </div>
        </form>
      </Panel>

      <Panel className="table-panel" title="Latest Result">
        {!latestResult ? (
          <EmptyState title="No contract text loaded yet." description="Run an extraction to see contract text here." />
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
                <p className="react-user-footnote">Text Length</p>
                <p className="ghl-contracts-summary__value">{Math.max(0, Number(latestResult.textLength) || 0)} chars</p>
              </div>
            </div>

            <div className="ghl-contracts-meta">
              <p className="react-user-footnote">
                Checked at: {formatDateTime(latestResult.fetchedAt)} ({latestResult.elapsedMs} ms)
              </p>
              <p className="react-user-footnote">
                Source: {latestResult.source || "-"}
                {latestResult.fallbackMode && latestResult.fallbackMode !== "none" ? ` (${latestResult.fallbackMode})` : ""}
              </p>
              {latestResult.note ? <p className="react-user-footnote">{latestResult.note}</p> : null}
              {latestResult.dashboardUrl ? (
                <p className="react-user-footnote">
                  Dashboard URL:
                  {" "}
                  <a href={latestResult.dashboardUrl} target="_blank" rel="noreferrer">
                    {latestResult.dashboardUrl}
                  </a>
                </p>
              ) : null}
            </div>

            <div className="ghl-contracts-text">
              <Textarea
                className="ghl-contracts-textarea"
                readOnly
                value={latestResult.contractText || ""}
                rows={18}
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
