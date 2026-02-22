import { type CSSProperties, type FormEvent, useEffect, useMemo, useState } from "react";

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

interface BureauRow {
  id: string;
  bureau: string;
  score: number | null;
}

const HISTORY_MAX_ROWS = 20;
const HISTORY_STORAGE_KEY = "identityiq-score-history-v1";
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
  const [selectedHistoryId, setSelectedHistoryId] = useState("");
  const [isHistoryHydrated, setIsHistoryHydrated] = useState(false);

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
    if (isLoading) {
      return LOADING_STATUS_MESSAGES[loadingStatusIndex] || LOADING_STATUS_MESSAGES[0];
    }
    if (submitError) {
      return submitError;
    }
    return statusMessage;
  }, [isLoading, loadingStatusIndex, statusMessage, submitError]);

  const bureauRows = useMemo<BureauRow[]>(() => {
    const orderedScores = resolveOrderedBureauScores(latestResult?.bureauScores);
    return orderedScores.map((item) => ({
      id: `${item.bureau}-${item.score ?? "na"}`,
      bureau: item.bureau,
      score: item.score,
    }));
  }, [latestResult]);

  const historyColumns = useMemo<TableColumn<IdentityIqHistoryRow>[]>(() => {
    return [
      {
        key: "client",
        label: "Client",
        align: "left",
        className: "identityiq-history-col-client",
        headerClassName: "identityiq-history-col-client",
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
        align: "left",
        className: "identityiq-history-col-bureaus",
        headerClassName: "identityiq-history-col-bureaus",
        cell: (row) => {
          const orderedScores = resolveOrderedBureauScores(row.bureauScores);
          return (
            <div className="identityiq-history-bureaus">
              {orderedScores.map((item) => (
                <Badge key={`${row.id}-${item.bureau}`} tone={Number.isFinite(item.score) ? "info" : "warning"}>
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
        align: "right",
        className: "identityiq-history-col-time",
        headerClassName: "identityiq-history-col-time",
        cell: (row) => formatDateTime(row.fetchedAt),
      },
    ];
  }, []);

  const hasMissingBureauScores = useMemo(
    () => bureauRows.some((row) => !Number.isFinite(row.score)),
    [bureauRows],
  );

  const bureauLoadSummary = useMemo(() => {
    const loadedCount = bureauRows.filter((row) => Number.isFinite(row.score)).length;
    return `${loadedCount}/${bureauRows.length} bureaus loaded`;
  }, [bureauRows]);

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
          </div>

          <div className="identityiq-score-actions">
            <div className="identityiq-score-actions__copy">
              <p className="identityiq-score-actions__title">Run live check</p>
              <p className="identityiq-score-actions__hint">Credentials are used for one request only.</p>
            </div>
            <div className="identityiq-score-actions__controls">
              <p className={`identityiq-score-actions__status ${isLoading ? "is-live" : ""} ${submitError ? "error" : ""}`.trim()}>
                {visibleStatusMessage}
              </p>
              <Button type="submit" size="md" isLoading={isLoading}>
                Get Credit Score
              </Button>
            </div>
          </div>
        </form>
      </Panel>

      <Panel className="table-panel" title="Latest Result">
        {!latestResult ? (
          <EmptyState title="No score loaded yet." description="Run a client check to see score details here." />
        ) : (
          <div className="identityiq-score-result">
            <div className="identityiq-latest-hero">
              <div className="identityiq-latest-hero__main">
                <div className="identityiq-latest-hero__status-row">
                  <Badge tone={latestResult.status === "ok" ? "success" : "warning"}>{formatResultStatus(latestResult.status)}</Badge>
                  <span className="react-user-footnote">{bureauLoadSummary}</span>
                </div>
                <div>
                  <p className="react-user-footnote">Client</p>
                  <p className="identityiq-score-summary__value">{latestResult.clientName || "Unnamed client"}</p>
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
              </div>
            </div>

            <div className="identityiq-bureau-grid">
              {bureauRows.map((item) => {
                const visual = getScoreVisual(item.score);
                const barStyle = {
                  "--identityiq-score-ratio": `${visual.progress}%`,
                } as CSSProperties;
                return (
                  <article key={item.id} className={`identityiq-bureau-card tone-${visual.tone}`}>
                    <p className="identityiq-bureau-card__name">{item.bureau}</p>
                    <p className="identityiq-bureau-card__score">{formatScore(item.score)}</p>
                    <p className="identityiq-bureau-card__tier">{visual.label}</p>
                    <div className="identityiq-bureau-card__bar" style={barStyle} />
                  </article>
                );
              })}
            </div>

            {hasMissingBureauScores ? (
              <p className="react-user-footnote">
                One or more bureau scores were not found in the IdentityIQ response for this check.
              </p>
            ) : null}

            {latestResult.snippets.length ? (
              <details className="identityiq-score-snippets">
                <summary>Matched score snippets ({latestResult.snippets.length})</summary>
                <ul>
                  {latestResult.snippets.map((snippet, index) => (
                    <li key={`${snippet}-${index}`}>{snippet}</li>
                  ))}
                </ul>
              </details>
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
          tableClassName="identityiq-history-table"
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

function resolveOrderedBureauScores(
  source: IdentityIqCreditScoreResult["bureauScores"] | null | undefined,
): Array<{ bureau: string; score: number | null }> {
  const scoreByBureau = new Map<string, number>();

  for (const item of Array.isArray(source) ? source : []) {
    const bureau = normalizeBureauName(item?.bureau);
    const score = Number.isFinite(item?.score) ? Number(item.score) : null;
    if (!bureau || score === null || scoreByBureau.has(bureau)) {
      continue;
    }
    scoreByBureau.set(bureau, score);
  }

  return BUREAU_ORDER.map((bureau) => ({
    bureau,
    score: scoreByBureau.get(bureau) ?? null,
  }));
}

function normalizeBureauName(rawValue: string | null | undefined): string {
  const value = typeof rawValue === "string" ? rawValue.trim().toLowerCase() : "";
  if (!value) {
    return "";
  }

  if (value.includes("trans") || value === "tu") {
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

function formatResultStatus(status: string): string {
  const normalized = (status || "").toString().trim().toLowerCase();
  if (!normalized) {
    return "Unknown";
  }
  if (normalized === "ok") {
    return "Complete";
  }
  if (normalized === "partial") {
    return "Partial";
  }
  return normalized[0].toUpperCase() + normalized.slice(1);
}

function getScoreVisual(score: number | null): {
  label: string;
  tone: "empty" | "poor" | "fair" | "good" | "very-good" | "excellent";
  progress: number;
} {
  if (!Number.isFinite(score)) {
    return {
      label: "Not found",
      tone: "empty",
      progress: 0,
    };
  }

  const boundedScore = Math.min(850, Math.max(300, Number(score)));
  const progress = Math.round(((boundedScore - 300) / (850 - 300)) * 100);
  if (boundedScore < 580) {
    return { label: "Poor", tone: "poor", progress };
  }
  if (boundedScore < 670) {
    return { label: "Fair", tone: "fair", progress };
  }
  if (boundedScore < 740) {
    return { label: "Good", tone: "good", progress };
  }
  if (boundedScore < 800) {
    return { label: "Very good", tone: "very-good", progress };
  }
  return { label: "Excellent", tone: "excellent", progress };
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
