import { type FormEvent, useEffect, useMemo, useState } from "react";

import { getIdentityIqCreditScore } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { IdentityIqCreditScoreResult } from "@/shared/types/identityIq";
import { Badge, Button, EmptyState, Field, Input, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface IdentityIqFormState {
  clientName: string;
  email: string;
  password: string;
  ssnLast4: string;
}

interface IdentityIqHistoryRow extends IdentityIqCreditScoreResult {
  id: string;
}

const HISTORY_MAX_ROWS = 20;
const BUREAU_ORDER = ["TransUnion", "Equifax", "Experian"] as const;
const LOADING_STATUS_MESSAGES = [
  "Connecting to IdentityIQ...",
  "Submitting client credentials...",
  "Passing security verification...",
  "Opening member dashboard...",
  "Reading credit score signals...",
  "Preparing final score result...",
] as const;

export default function IdentityIqScorePage() {
  const [form, setForm] = useState<IdentityIqFormState>({
    clientName: "",
    email: "",
    password: "",
    ssnLast4: "",
  });
  const [isLoading, setIsLoading] = useState(false);
  const [statusMessage, setStatusMessage] = useState("Ready to request score from IdentityIQ.");
  const [submitError, setSubmitError] = useState("");
  const [loadingStatusIndex, setLoadingStatusIndex] = useState(0);
  const [latestResult, setLatestResult] = useState<IdentityIqCreditScoreResult | null>(null);
  const [historyRows, setHistoryRows] = useState<IdentityIqHistoryRow[]>([]);

  useEffect(() => {
    if (!isLoading) {
      setLoadingStatusIndex(0);
      return;
    }

    setLoadingStatusIndex(0);
    const timerId = window.setInterval(() => {
      setLoadingStatusIndex((previous) => (previous + 1) % LOADING_STATUS_MESSAGES.length);
    }, 2000);

    return () => {
      window.clearInterval(timerId);
    };
  }, [isLoading]);

  const visibleStatusMessage = useMemo(() => {
    if (submitError) {
      return submitError;
    }
    if (isLoading) {
      return LOADING_STATUS_MESSAGES[loadingStatusIndex] || LOADING_STATUS_MESSAGES[0];
    }
    return statusMessage;
  }, [isLoading, loadingStatusIndex, statusMessage, submitError]);

  const bureauRows = useMemo(() => {
    const source = Array.isArray(latestResult?.bureauScores) ? latestResult.bureauScores : [];
    const scoreByBureau = new Map<string, number>();
    for (const item of source) {
      const bureau = item?.bureau?.trim();
      if (!bureau || !Number.isFinite(item?.score) || scoreByBureau.has(bureau)) {
        continue;
      }
      scoreByBureau.set(bureau, Number(item.score));
    }
    return BUREAU_ORDER.map((bureau) => ({
      id: `${bureau}-${scoreByBureau.get(bureau) ?? "na"}`,
      bureau,
      score: scoreByBureau.get(bureau) ?? null,
    }));
  }, [latestResult]);

  const hasMissingBureauScores = useMemo(
    () => bureauRows.some((row) => !Number.isFinite(row.score)),
    [bureauRows],
  );

  const bureauColumns = useMemo<TableColumn<BureauRow>[]>(() => {
    return [
      {
        key: "bureau",
        label: "Credit Bureau",
        align: "left",
        cell: (row) => row.bureau,
      },
      {
        key: "score",
        label: "Score",
        align: "center",
        cell: (row) => <Badge tone={Number.isFinite(row.score) ? "info" : "warning"}>{formatScore(row.score)}</Badge>,
      },
    ];
  }, []);

  const historyColumns = useMemo<TableColumn<IdentityIqHistoryRow>[]>(() => {
    return [
      {
        key: "client",
        label: "Client",
        align: "left",
        cell: (row) => (
          <div>
            <strong>{row.clientName || "Unnamed client"}</strong>
            <div className="react-user-footnote">{row.emailMasked || "-"}</div>
          </div>
        ),
      },
      {
        key: "score",
        label: "Score",
        align: "center",
        cell: (row) => <Badge tone={row.status === "ok" ? "success" : "warning"}>{formatScore(row.score)}</Badge>,
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

    const validationError = validateIdentityIqForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    setIsLoading(true);
    setStatusMessage("Starting IdentityIQ check...");

    try {
      const payload = await getIdentityIqCreditScore({
        clientName: form.clientName.trim(),
        email: form.email.trim(),
        password: form.password,
        ssnLast4: form.ssnLast4.trim(),
      });

      const result = payload?.result;
      if (!result) {
        throw new Error("IdentityIQ returned an empty result.");
      }

      setLatestResult(result);
      setHistoryRows((previous) => {
        const nextItem: IdentityIqHistoryRow = {
          ...result,
          id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        };
        return [nextItem, ...previous].slice(0, HISTORY_MAX_ROWS);
      });
      setStatusMessage(`Last check completed at ${formatDateTime(result.fetchedAt)}.`);
      setForm((previous) => ({
        ...previous,
        password: "",
        ssnLast4: "",
      }));
      showToast({
        type: "success",
        message: "IdentityIQ score loaded.",
        dedupeKey: "identityiq-load-success",
        cooldownMs: 2200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load IdentityIQ score.";
      setSubmitError(message);
      setStatusMessage("IdentityIQ request failed.");
      showToast({
        type: "error",
        message,
        dedupeKey: `identityiq-load-error-${message}`,
        cooldownMs: 2200,
      });
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <PageShell className="identityiq-score-react-page">
      <PageHeader
        title="IdentityIQ Credit Score"
        subtitle="Secure login-based score read for each client"
        meta={
          <>
            <p className={`dashboard-message ${submitError ? "error" : ""} ${isLoading ? "is-live" : ""}`.trim()}>
              {visibleStatusMessage}
            </p>
            <p className="react-user-footnote">
              Credentials are used only for the live request. Password and SSN4 are cleared from the form after successful check.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Check Client">
        <form className="identityiq-score-form" onSubmit={handleSubmit}>
          <div className="identityiq-score-form__fields">
            <Field label="Client Name (optional)" htmlFor="identityiq-client-name">
              <Input
                id="identityiq-client-name"
                autoComplete="off"
                value={form.clientName}
                onChange={(event) => setForm((previous) => ({ ...previous, clientName: event.target.value }))}
                placeholder="Oleksandr Savras"
              />
            </Field>

            <Field label="Email" htmlFor="identityiq-email">
              <Input
                id="identityiq-email"
                type="email"
                autoComplete="username"
                value={form.email}
                onChange={(event) => setForm((previous) => ({ ...previous, email: event.target.value }))}
                placeholder="client@example.com"
                hasError={Boolean(submitError) && !form.email.trim()}
              />
            </Field>

            <Field label="Password" htmlFor="identityiq-password">
              <Input
                id="identityiq-password"
                type="password"
                autoComplete="current-password"
                value={form.password}
                onChange={(event) => setForm((previous) => ({ ...previous, password: event.target.value }))}
                placeholder="********"
                hasError={Boolean(submitError) && !form.password.trim()}
              />
            </Field>

            <Field label="SSN Last 4" htmlFor="identityiq-ssn4" hint="Exactly 4 digits">
              <Input
                id="identityiq-ssn4"
                inputMode="numeric"
                autoComplete="off"
                value={form.ssnLast4}
                onChange={(event) =>
                  setForm((previous) => ({
                    ...previous,
                    ssnLast4: event.target.value.replace(/\D/g, "").slice(0, 4),
                  }))
                }
                placeholder="9205"
                hasError={Boolean(submitError) && !/^\d{4}$/.test(form.ssnLast4)}
              />
            </Field>
          </div>

          <div className="identityiq-score-actions">
            <div className="identityiq-score-actions__copy">
              <p className="identityiq-score-actions__title">Run live check</p>
              <p className="identityiq-score-actions__hint">Credentials are used for one request only.</p>
            </div>
            <Button type="submit" size="md" isLoading={isLoading}>
              Get Credit Score
            </Button>
          </div>
        </form>
      </Panel>

      <Panel className="table-panel" title="Latest Result">
        {!latestResult ? (
          <EmptyState title="No score loaded yet." description="Run a client check to see score details here." />
        ) : (
          <div className="identityiq-score-result">
            <div className="identityiq-score-summary">
              <div>
                <p className="react-user-footnote">Client</p>
                <p className="identityiq-score-summary__value">{latestResult.clientName || "Unnamed client"}</p>
              </div>
              <div>
                <p className="react-user-footnote">Overall Score</p>
                <p className="identityiq-score-summary__value">{formatScore(latestResult.score)}</p>
              </div>
              <div>
                <p className="react-user-footnote">Status</p>
                <Badge tone={latestResult.status === "ok" ? "success" : "warning"}>{latestResult.status}</Badge>
              </div>
            </div>

            <div className="identityiq-score-meta">
              <p className="react-user-footnote">
                Checked at: {formatDateTime(latestResult.fetchedAt)} ({latestResult.elapsedMs} ms)
              </p>
              {latestResult.dashboardUrl ? (
                <p className="react-user-footnote">
                  Dashboard URL:
                  {" "}
                  <a href={latestResult.dashboardUrl} target="_blank" rel="noreferrer">
                    {latestResult.dashboardUrl}
                  </a>
                </p>
              ) : null}
              {latestResult.note ? <p className="react-user-footnote">{latestResult.note}</p> : null}
            </div>

            <div className="identityiq-score-bureaus">
              <Table
                columns={bureauColumns}
                rows={bureauRows}
                rowKey={(row) => row.id}
                className="identityiq-score-table-wrap"
                density="compact"
              />
              {hasMissingBureauScores ? (
                <p className="react-user-footnote">
                  One or more bureau scores were not found in the IdentityIQ response for this check.
                </p>
              ) : null}
            </div>

            {latestResult.snippets.length ? (
              <div className="identityiq-score-snippets">
                <p className="react-user-footnote">Matched score snippets:</p>
                <ul>
                  {latestResult.snippets.map((snippet, index) => (
                    <li key={`${snippet}-${index}`}>{snippet}</li>
                  ))}
                </ul>
              </div>
            ) : null}
          </div>
        )}
      </Panel>

      <Panel className="table-panel" title="Recent Checks">
        <Table
          columns={historyColumns}
          rows={historyRows}
          rowKey={(row) => row.id}
          className="identityiq-score-table-wrap"
          emptyState="No checks run in this browser session yet."
          density="compact"
        />
      </Panel>
    </PageShell>
  );
}

interface BureauRow {
  id: string;
  bureau: string;
  score: number | null;
}

function validateIdentityIqForm(form: IdentityIqFormState): string {
  const email = form.email.trim();
  if (!email || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) {
    return "Valid client email is required.";
  }

  if (!form.password.trim()) {
    return "Client password is required.";
  }

  if (!/^\d{4}$/.test(form.ssnLast4.trim())) {
    return "SSN last 4 must contain exactly 4 digits.";
  }

  return "";
}

function formatScore(score: number | null | undefined): string {
  return Number.isFinite(score) ? String(score) : "N/A";
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
