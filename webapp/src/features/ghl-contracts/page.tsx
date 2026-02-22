import { type FormEvent, useMemo, useState } from "react";

import { getGhlContractText } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { GhlContractTextResult } from "@/shared/types/ghlContractText";
import { Badge, Button, EmptyState, Field, Input, PageHeader, PageShell, Panel, Table, Textarea } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface GhlContractTextFormState {
  clientName: string;
  login: string;
  password: string;
  mfaCode: string;
  locationId: string;
}

interface GhlContractTextHistoryRow extends GhlContractTextResult {
  id: string;
}

const HISTORY_MAX_ROWS = 20;

export default function GhlContractsPage() {
  const [form, setForm] = useState<GhlContractTextFormState>({
    clientName: "",
    login: "",
    password: "",
    mfaCode: "",
    locationId: "",
  });
  const [isLoading, setIsLoading] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [statusMessage, setStatusMessage] = useState("Ready to extract contract text from GoHighLevel.");
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

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError("");

    const validationError = validateGhlContractTextForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    setIsLoading(true);
    setStatusMessage("Logging in to GoHighLevel and extracting contract text...");

    try {
      const payload = await getGhlContractText({
        clientName: form.clientName.trim(),
        login: form.login.trim() || undefined,
        password: form.password || undefined,
        mfaCode: form.mfaCode.trim() || undefined,
        locationId: form.locationId.trim() || undefined,
      });

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
      setForm((previous) => ({
        ...previous,
        password: "",
        mfaCode: "",
      }));
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

  return (
    <PageShell className="ghl-contracts-react-page">
      <PageHeader
        title="GoHighLevel Contract Text"
        subtitle="Admin login-based contract text extraction"
        meta={
          <>
            <p className={`dashboard-message ${submitError ? "error" : ""}`.trim()}>{submitError || statusMessage}</p>
            <p className="react-user-footnote">
              You can leave login/password empty to use server env vars: `GHL_ADMIN_LOGIN` and `GHL_ADMIN_PASSWORD`.
              Use `GHL_ADMIN_MFA_CODE` only if you rotate it per request.
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

          <Field label="Admin Login (optional)" htmlFor="ghl-contract-login">
            <Input
              id="ghl-contract-login"
              autoComplete="username"
              value={form.login}
              onChange={(event) => setForm((previous) => ({ ...previous, login: event.target.value }))}
              placeholder="admin@company.com"
            />
          </Field>

          <Field label="Admin Password (optional)" htmlFor="ghl-contract-password">
            <Input
              id="ghl-contract-password"
              type="password"
              autoComplete="current-password"
              value={form.password}
              onChange={(event) => setForm((previous) => ({ ...previous, password: event.target.value }))}
              placeholder="********"
            />
          </Field>

          <Field label="MFA Code (optional)" htmlFor="ghl-contract-mfa-code">
            <Input
              id="ghl-contract-mfa-code"
              autoComplete="one-time-code"
              inputMode="numeric"
              value={form.mfaCode}
              onChange={(event) => setForm((previous) => ({ ...previous, mfaCode: event.target.value }))}
              placeholder="123456"
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

  const hasLogin = Boolean(form.login.trim());
  const hasPassword = Boolean(form.password.trim());
  if (hasLogin !== hasPassword) {
    return "Provide both admin login and password, or leave both empty to use server env credentials.";
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
