import { type FormEvent, useEffect, useMemo, useState } from "react";

import { getIdentityIqCreditScore } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { IdentityIqBureauScore, IdentityIqCreditScoreResult } from "@/shared/types/identityIq";
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
const HISTORY_STORAGE_KEY = "identityiq-score-history-v1";

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
  const [latestResult, setLatestResult] = useState<IdentityIqCreditScoreResult | null>(null);
  const [historyRows, setHistoryRows] = useState<IdentityIqHistoryRow[]>([]);
  const [selectedHistoryId, setSelectedHistoryId] = useState("");
  const [isHistoryHydrated, setIsHistoryHydrated] = useState(false);

  const bureauRows = useMemo(() => {
    const source = Array.isArray(latestResult?.bureauScores) ? latestResult.bureauScores : [];
    return source.map((item, index) => ({
      id: `${item.bureau}-${item.score}-${index}`,
      bureau: item.bureau,
      score: item.score,
    }));
  }, [latestResult]);

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
        cell: (row) => <Badge tone="info">{row.score}</Badge>,
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
        key: "bureaus",
        label: "Bureaus",
        align: "center",
        cell: (row) => {
          const bureauScores = getOrderedBureauScores(row.bureauScores);
          if (!bureauScores.length) {
            return "-";
          }

          return (
            <div className="identityiq-history-badges">
              {bureauScores.map((item) => (
                <Badge key={`${row.id}-${item.bureau}`} tone="info">
                  {item.bureau}: {formatScore(item.score)}
                </Badge>
              ))}
            </div>
          );
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
    const restoredRows = loadIdentityIqHistory();
    if (restoredRows.length) {
      setHistoryRows(restoredRows);
      setLatestResult(restoredRows[0]);
      setSelectedHistoryId(restoredRows[0].id);
      setStatusMessage(`Loaded ${restoredRows.length} saved checks from this browser.`);
    }
    setIsHistoryHydrated(true);
  }, []);

  useEffect(() => {
    if (!isHistoryHydrated) {
      return;
    }
    saveIdentityIqHistory(historyRows);
  }, [historyRows, isHistoryHydrated]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitError("");

    const validationError = validateIdentityIqForm(form);
    if (validationError) {
      setSubmitError(validationError);
      return;
    }

    setIsLoading(true);
    setStatusMessage("Logging in to IdentityIQ and reading the score...");

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

      const nextItem: IdentityIqHistoryRow = {
        ...result,
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      };
      setLatestResult(nextItem);
      setSelectedHistoryId(nextItem.id);
      setHistoryRows((previous) => [nextItem, ...previous].slice(0, HISTORY_MAX_ROWS));
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
            <p className={`dashboard-message ${submitError ? "error" : ""}`.trim()}>{submitError || statusMessage}</p>
            <p className="react-user-footnote">
              Credentials are used only for the live request. Password and SSN4 are cleared from the form after successful check.
            </p>
          </>
        }
      />

      <Panel className="table-panel" title="Check Client">
        <form
          className="identityiq-score-form"
          onSubmit={handleSubmit}
          autoComplete="off"
          data-form-type="other"
          data-lpignore="true"
          data-1p-ignore="true"
        >
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
              name="identityiq_client_email_live_only"
              type="email"
              autoComplete="off"
              data-lpignore="true"
              data-1p-ignore="true"
              spellCheck={false}
              value={form.email}
              onChange={(event) => setForm((previous) => ({ ...previous, email: event.target.value }))}
              placeholder="client@example.com"
              hasError={Boolean(submitError) && !form.email.trim()}
            />
          </Field>

          <Field label="Password" htmlFor="identityiq-password">
            <Input
              id="identityiq-password"
              name="identityiq_client_password_live_only"
              type="password"
              autoComplete="new-password"
              data-lpignore="true"
              data-1p-ignore="true"
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

          <div className="identityiq-score-actions">
            <Button type="submit" size="sm" isLoading={isLoading}>
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

            {bureauRows.length ? (
              <div className="identityiq-score-bureaus">
                <Table
                  columns={bureauColumns}
                  rows={bureauRows}
                  rowKey={(row) => row.id}
                  className="identityiq-score-table-wrap"
                  density="compact"
                />
              </div>
            ) : (
              <EmptyState title="No bureau-specific score blocks were found." />
            )}

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
          onRowActivate={(row) => {
            setLatestResult(row);
            setSelectedHistoryId(row.id);
            setSubmitError("");
            setStatusMessage(`Loaded check from history at ${formatDateTime(row.fetchedAt)}.`);
          }}
          rowClassName={(row) => (row.id === selectedHistoryId ? "identityiq-history-row--selected" : undefined)}
        />
      </Panel>
    </PageShell>
  );
}

interface BureauRow extends IdentityIqBureauScore {
  id: string;
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

function getOrderedBureauScores(bureauScores: IdentityIqBureauScore[]): IdentityIqBureauScore[] {
  const source = Array.isArray(bureauScores) ? bureauScores : [];
  if (!source.length) {
    return [];
  }

  const scoreByBureau = new Map<string, number>();
  for (const item of source) {
    const normalized = normalizeBureauName(item.bureau);
    if (!normalized || !Number.isFinite(item.score)) {
      continue;
    }
    scoreByBureau.set(normalized, item.score);
  }

  return ["TransUnion", "Equifax", "Experian"]
    .filter((bureau) => scoreByBureau.has(bureau))
    .map((bureau) => ({
      bureau,
      score: scoreByBureau.get(bureau) ?? 0,
    }));
}

function normalizeBureauName(rawValue: string | null | undefined): string {
  const value = typeof rawValue === "string" ? rawValue.trim().toLowerCase() : "";
  if (!value) {
    return "";
  }

  if (value.includes("trans") || value.includes("tu")) {
    return "TransUnion";
  }

  if (value.includes("equifax") || value === "eq") {
    return "Equifax";
  }

  if (value.includes("experian") || value === "ex") {
    return "Experian";
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

function loadIdentityIqHistory(): IdentityIqHistoryRow[] {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const serialized = window.localStorage.getItem(HISTORY_STORAGE_KEY);
    if (!serialized) {
      return [];
    }

    const parsed = JSON.parse(serialized) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed.filter(isIdentityIqHistoryRow).slice(0, HISTORY_MAX_ROWS);
  } catch {
    return [];
  }
}

function saveIdentityIqHistory(rows: IdentityIqHistoryRow[]): void {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(rows.slice(0, HISTORY_MAX_ROWS)));
  } catch {
    // Ignore localStorage write errors (quota or private mode restrictions).
  }
}

function isIdentityIqHistoryRow(value: unknown): value is IdentityIqHistoryRow {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<IdentityIqHistoryRow>;
  if (
    typeof candidate.id !== "string"
    || typeof candidate.status !== "string"
    || typeof candidate.provider !== "string"
    || typeof candidate.emailMasked !== "string"
    || typeof candidate.fetchedAt !== "string"
    || typeof candidate.elapsedMs !== "number"
    || !(typeof candidate.score === "number" || candidate.score === null)
    || !Array.isArray(candidate.bureauScores)
    || !Array.isArray(candidate.snippets)
  ) {
    return false;
  }

  return candidate.bureauScores.every((item) => isIdentityIqBureauScore(item));
}

function isIdentityIqBureauScore(value: unknown): value is IdentityIqBureauScore {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = value as Partial<IdentityIqBureauScore>;
  return typeof candidate.bureau === "string" && typeof candidate.score === "number";
}
