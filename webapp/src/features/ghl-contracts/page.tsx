import { type FormEvent, useMemo, useState, useEffect } from "react";

import { getGhlContractTerms, getGhlContractTermsRecent } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { GhlContractTermsRequest, GhlContractTermsResult } from "@/shared/types/ghlContractTerms";
import { Badge, Button, EmptyState, Field, Input, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface GhlContractTextFormState {
  clientName: string;
  locationId: string;
}

interface GhlContractTextHistoryRow extends GhlContractTermsResult {
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
  const [statusMessage, setStatusMessage] = useState("Ready to load contract terms from GoHighLevel API.");
  const [latestResult, setLatestResult] = useState<GhlContractTermsResult | null>(null);
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
        key: "status",
        label: "Status",
        align: "center",
        cell: (row) => {
          const tone = row.status === "completed" ? "success" : "warning";
          return <Badge tone={tone}>{row.status || "-"}</Badge>;
        },
      },
      {
        key: "fetchedAt",
        label: "Checked At",
        align: "center",
        cell: (row) => formatDateTime(row.fetchedAt),
      },
    ];
  }, []);

  useEffect(() => {
    void loadRecent();
  }, []);

  async function loadRecent() {
    try {
      const payload = await getGhlContractTermsRecent(20);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setHistoryRows(items.map((item) => ({ ...item, id: item.id || `${Date.now()}-${Math.random()}` })));
    } catch {
      // Recent extractions are optional.
    }
  }

  async function executeContractTextRequest(request: GhlContractTermsRequest) {
    setIsLoading(true);
    setStatusMessage("Fetching contract terms from GoHighLevel API...");
    try {
      const payload = await getGhlContractTerms(request);

      const result = payload?.result;
      if (!result?.terms?.length) {
        throw new Error("GoHighLevel returned empty contract terms.");
      }

      setLatestResult(result);
      setHistoryRows((previous) => {
        const nextItem: GhlContractTextHistoryRow = {
          ...result,
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        };
        return [nextItem, ...previous].slice(0, HISTORY_MAX_ROWS);
      });
      void loadRecent();
      setStatusMessage(`Last extraction completed at ${formatDateTime(result.fetchedAt)}.`);
      showToast({
        type: "success",
        message: "Contract terms loaded.",
        dedupeKey: "ghl-contract-terms-success",
        cooldownMs: 2200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load GoHighLevel contract terms.";
      setSubmitError(message);
      setStatusMessage("GoHighLevel contract terms request failed.");
      showToast({
        type: "error",
        message,
        dedupeKey: `ghl-contract-terms-error-${message}`,
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

    const request: GhlContractTermsRequest = {
      clientName: form.clientName.trim(),
      locationId: form.locationId.trim() || undefined,
    };
    await executeContractTextRequest(request);
  }

  return (
    <PageShell className="ghl-contracts-react-page">
      <PageHeader
        title="GoHighLevel Contract Terms"
        subtitle="API-based contract terms lookup"
        meta={
          <>
            <p className={`dashboard-message ${submitError ? "error" : ""}`.trim()}>{submitError || statusMessage}</p>
            <p className="react-user-footnote">
              Uses server credentials: `GHL_API_KEY` and `GHL_LOCATION_ID`. You can override location ID below.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Extract Contract Terms">
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
              Get Contract Terms
            </Button>
          </div>
        </form>
      </Panel>

      <Panel className="table-panel" title="Latest Contract Terms">
        {!latestResult ? (
          <EmptyState title="No contract terms loaded yet." description="Run an extraction to see the terms here." />
        ) : (
          <div className="ghl-contracts-result">
            <div className="ghl-contracts-summary">
              <div>
                <p className="react-user-footnote">Client</p>
                <p className="ghl-contracts-summary__value">{latestResult.clientName || "Unnamed client"}</p>
              </div>
              <div>
                <p className="react-user-footnote">Status</p>
                <Badge tone={latestResult.status === "completed" ? "success" : "warning"}>
                  {latestResult.status || "unknown"}
                </Badge>
              </div>
              <div>
                <p className="react-user-footnote">Last Update</p>
                <p className="ghl-contracts-summary__value">{formatDateTime(latestResult.updatedAt)}</p>
              </div>
            </div>

            <div className="ghl-contracts-meta">
              <p className="react-user-footnote">
                Checked at: {formatDateTime(latestResult.fetchedAt)} ({latestResult.elapsedMs} ms)
              </p>
              <p className="react-user-footnote">
                Document ID: {latestResult.documentId || "-"}
              </p>
              {latestResult.note ? <p className="react-user-footnote">{latestResult.note}</p> : null}
            </div>

            <div className="ghl-contracts-text">
              {latestResult.status === "not_found" ? (
                <div className="ghl-contracts-terms">
                  <h3 className="ghl-contracts-terms__title">Contract not found</h3>
                  <p>No contract terms were found for this client in GoHighLevel.</p>
                </div>
              ) : (
                <div className="ghl-contracts-terms">
                  <h3 className="ghl-contracts-terms__title">{latestResult.documentName}</h3>
                  <p><strong>Status:</strong> {latestResult.status}</p>
                  <p><strong>Last update:</strong> {latestResult.updatedAt}</p>
                  <p><strong>Contact name in GHL:</strong> {latestResult.contactName}</p>
                  <p><strong>Email:</strong> {latestResult.contactEmail}</p>

                  <p className="ghl-contracts-terms__section">Date of signature: {latestResult.signedAt || "-"}</p>

                  <h4>Contact details from the contract:</h4>
                  <p>{latestResult.contactDetails.fullName}</p>
                  <p><strong>Phone number:</strong> {latestResult.contactDetails.phone}</p>
                  <p><strong>Email address:</strong> {latestResult.contactDetails.email}</p>
                  <p><strong>Address:</strong> {latestResult.contactDetails.address}</p>

                  <h4>Credit monitoring service:</h4>
                  <p>{latestResult.contactDetails.monitoringService}</p>
                  <p>{latestResult.contactDetails.monitoringEmail}</p>
                  <p>{latestResult.contactDetails.monitoringPassword}</p>
                  <p><strong>Secret:</strong> {latestResult.contactDetails.monitoringSecret}</p>
                  <p><strong>SSN:</strong> {latestResult.contactDetails.ssn}</p>
                  <p><strong>Date of birth:</strong> {latestResult.contactDetails.dob}</p>

                  <h4>Contract terms:</h4>
                  <ol>
                    {latestResult.terms.map((term) => (
                      <li key={term}>{term}</li>
                    ))}
                  </ol>

                  <h4>Payments under the contract:</h4>
                  <ol>
                    {latestResult.payments.map((payment) => (
                      <li key={`${payment.dueDate}-${payment.amount}`}>
                        {payment.dueDate} {payment.amount}
                      </li>
                    ))}
                  </ol>

                  {latestResult.signatureDataUrl ? (
                    <>
                      <h4>Signature</h4>
                      <img className="ghl-contracts-signature" src={latestResult.signatureDataUrl} alt="Signature" />
                    </>
                  ) : null}
                </div>
              )}
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
