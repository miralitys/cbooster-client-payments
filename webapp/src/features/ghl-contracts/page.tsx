import { type FormEvent, useMemo, useState } from "react";

import { getGhlContractText } from "@/shared/api";
import { ApiError } from "@/shared/api/fetcher";
import { showToast } from "@/shared/lib/toast";
import type { GhlContractTextRequest, GhlContractTextResult } from "@/shared/types/ghlContractText";
import { Badge, Button, EmptyState, Field, Input, Modal, PageHeader, PageShell, Panel, Table, Textarea } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface GhlContractTextFormState {
  clientName: string;
  login: string;
  password: string;
  locationId: string;
}

interface GhlContractTextHistoryRow extends GhlContractTextResult {
  id: string;
}

const HISTORY_MAX_ROWS = 20;
const GHL_MFA_ERROR_CODES = new Set([
  "ghl_mfa_required",
  "ghl_mfa_invalid_code",
  "ghl_mfa_field_not_found",
  "ghl_mfa_submit_unavailable",
  "ghl_mfa_session_expired",
  "ghl_mfa_session_busy",
  "ghl_mfa_code_required",
]);

export default function GhlContractsPage() {
  const [form, setForm] = useState<GhlContractTextFormState>({
    clientName: "",
    login: "",
    password: "",
    locationId: "",
  });
  const [isLoading, setIsLoading] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const [statusMessage, setStatusMessage] = useState("Ready to extract contract text from GoHighLevel.");
  const [latestResult, setLatestResult] = useState<GhlContractTextResult | null>(null);
  const [historyRows, setHistoryRows] = useState<GhlContractTextHistoryRow[]>([]);
  const [isMfaDialogOpen, setIsMfaDialogOpen] = useState(false);
  const [mfaCode, setMfaCode] = useState("");
  const [mfaError, setMfaError] = useState("");
  const [pendingMfaRequest, setPendingMfaRequest] = useState<GhlContractTextRequest | null>(null);

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

  async function executeContractTextRequest(request: GhlContractTextRequest, mode: "initial" | "mfa" = "initial") {
    setIsLoading(true);
    setStatusMessage(
      mode === "mfa"
        ? "Verifying MFA code and extracting contract text..."
        : "Logging in to GoHighLevel and extracting contract text...",
    );
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
      setForm((previous) => ({
        ...previous,
        password: "",
      }));
      setMfaCode("");
      setMfaError("");
      setIsMfaDialogOpen(false);
      setPendingMfaRequest(null);
      showToast({
        type: "success",
        message: "Contract text extracted.",
        dedupeKey: "ghl-contract-text-success",
        cooldownMs: 2200,
      });
    } catch (error) {
      if (error instanceof ApiError && error.status === 409 && GHL_MFA_ERROR_CODES.has(error.code)) {
        const mfaSessionId = extractMfaSessionIdFromApiError(error) || request.mfaSessionId || "";
        setPendingMfaRequest({
          clientName: request.clientName,
          login: request.login,
          password: request.password,
          mfaSessionId: mfaSessionId || undefined,
          locationId: request.locationId,
        });
        setSubmitError("");
        setIsMfaDialogOpen(true);
        setStatusMessage("GoHighLevel requested MFA code. Enter it in the verification window.");
        setMfaError(
          error.code === "ghl_mfa_invalid_code" ? "The code is invalid or expired. Enter a fresh code." : error.message,
        );
        showToast({
          type: "info",
          message: "Enter the one-time code from email in the MFA window.",
          dedupeKey: "ghl-contract-text-mfa-required",
          cooldownMs: 1800,
        });
        return;
      }

      const message = error instanceof Error ? error.message : "Failed to extract GoHighLevel contract text.";
      if (mode === "mfa") {
        setMfaError(message);
      } else {
        setSubmitError(message);
      }
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
    setMfaError("");

    const validationError = validateGhlContractTextForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    const request: GhlContractTextRequest = {
      clientName: form.clientName.trim(),
      login: form.login.trim() || undefined,
      password: form.password || undefined,
      locationId: form.locationId.trim() || undefined,
    };
    setPendingMfaRequest(request);
    await executeContractTextRequest(request, "initial");
  }

  async function handleMfaSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError("");
    const normalizedCode = mfaCode.trim();
    if (!normalizedCode) {
      setMfaError("MFA code is required.");
      return;
    }
    if (!pendingMfaRequest) {
      setMfaError("Login session for MFA verification has expired. Run request again.");
      return;
    }
    const activeMfaSessionId = pendingMfaRequest.mfaSessionId?.trim();
    if (!activeMfaSessionId) {
      setMfaError("MFA verification session was not found. Start extraction again.");
      return;
    }

    setMfaError("");
    await executeContractTextRequest(
      {
        ...pendingMfaRequest,
        mfaCode: normalizedCode,
        mfaSessionId: activeMfaSessionId,
      },
      "mfa",
    );
  }

  function closeMfaDialog() {
    setIsMfaDialogOpen(false);
    setMfaError("");
    setMfaCode("");
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

      <Modal
        open={isMfaDialogOpen}
        title="GoHighLevel Verification Code"
        onClose={closeMfaDialog}
        footer={
          <>
            <Button type="button" variant="secondary" onClick={closeMfaDialog}>
              Cancel
            </Button>
            <Button type="submit" form="ghl-contract-mfa-form" size="sm" isLoading={isLoading}>
              Verify and Continue
            </Button>
          </>
        }
      >
        <form id="ghl-contract-mfa-form" onSubmit={handleMfaSubmit}>
          <p className="react-user-footnote">
            Enter the one-time code sent to your GoHighLevel admin email, then continue extraction.
          </p>
          <Field label="MFA Code" htmlFor="ghl-contract-mfa-dialog-code">
            <Input
              id="ghl-contract-mfa-dialog-code"
              autoComplete="one-time-code"
              inputMode="numeric"
              value={mfaCode}
              onChange={(event) => setMfaCode(event.target.value)}
              placeholder="123456"
              hasError={Boolean(mfaError)}
            />
          </Field>
          {mfaError ? <p className="dashboard-message error">{mfaError}</p> : null}
        </form>
      </Modal>
    </PageShell>
  );
}

function extractMfaSessionIdFromApiError(error: ApiError): string {
  const payload = error.payload;
  if (!payload || typeof payload !== "object") {
    return "";
  }
  const rawSessionId = (payload as Record<string, unknown>).mfaSessionId;
  return typeof rawSessionId === "string" ? rawSessionId.trim() : "";
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
