import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { showToast } from "@/shared/lib/toast";
import { requestOpenClientCard } from "@/shared/lib/openClientCard";
import { withStableRowKeys, type RowWithKey } from "@/shared/lib/stableRowKeys";
import {
  confirmQuickBooksRecentPayment,
  approveModerationSubmission,
  getModerationSubmissionFiles,
  getModerationSubmissions,
  getQuickBooksPayments,
  getRecords,
  getSession,
  rejectModerationSubmission,
} from "@/shared/api";
import {
  calculateOverviewMetrics,
  formatDate,
  formatDateTime,
  formatKpiMoney,
} from "@/features/client-payments/domain/calculations";
import type { OverviewPeriodKey } from "@/features/client-payments/domain/constants";
import type { ModerationSubmission, ModerationSubmissionFile } from "@/shared/types/moderation";
import type { ClientRecord } from "@/shared/types/records";
import type { QuickBooksPaymentRow } from "@/shared/types/quickbooks";
import {
  Button,
  EmptyState,
  ErrorState,
  LoadingSkeleton,
  Modal,
  PageHeader,
  PageShell,
  Panel,
  SegmentedControl,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const QUICKBOOKS_DASHBOARD_TIME_ZONE = "America/Chicago";
const CURRENCY_FORMATTER = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const OVERVIEW_PERIOD_OPTIONS: Array<{ key: OverviewPeriodKey; label: string }> = [
  { key: "currentWeek", label: "Current Week" },
  { key: "previousWeek", label: "Previous Week" },
  { key: "currentMonth", label: "Current Month" },
  { key: "last30Days", label: "Last 30 Days" },
];
const MODERATION_PAGE_LIMIT = 200;
const MODERATION_CLIENT_DETAIL_FIELDS: Array<{ key: string; label: string }> = [
  { key: "clientName", label: "Client Name" },
  { key: "closedBy", label: "Closed By" },
  { key: "leadSource", label: "Lead Source" },
  { key: "companyName", label: "Company Name" },
  { key: "serviceType", label: "Service Type" },
  { key: "contractTotals", label: "Contract Totals" },
  { key: "payment1", label: "Payment 1" },
  { key: "payment1Date", label: "Payment 1 Date" },
  { key: "futurePayment", label: "Future Payment" },
  { key: "identityIq", label: "Identity IQ" },
  { key: "ssn", label: "SSN" },
  { key: "clientPhoneNumber", label: "Client Phone Number" },
  { key: "clientEmailAddress", label: "Client Email Address" },
  { key: "notes", label: "Notes" },
  { key: "afterResult", label: "After Result" },
  { key: "writtenOff", label: "Written Off" },
];
const SHOWN_CLIENT_FIELDS = new Set([...MODERATION_CLIENT_DETAIL_FIELDS.map((field) => field.key), "id", "createdAt"]);

type DashboardQuickBooksViewRow = RowWithKey<QuickBooksPaymentRow>;

function normalizeModerationNextCursor(value: unknown): string | null {
  const cursor = String(value || "").trim();
  return cursor ? cursor : null;
}

function mergeModerationSubmissions(
  previousItems: ModerationSubmission[],
  nextItems: ModerationSubmission[],
): ModerationSubmission[] {
  if (!previousItems.length) {
    return nextItems;
  }
  if (!nextItems.length) {
    return previousItems;
  }

  const seen = new Set(previousItems.map((item) => item.id));
  const merged = [...previousItems];
  for (const item of nextItems) {
    if (seen.has(item.id)) {
      continue;
    }
    seen.add(item.id);
    merged.push(item);
  }
  return merged;
}

export default function DashboardPage() {
  const [sessionCanReview, setSessionCanReview] = useState(false);
  const [sessionCanConfirmQuickBooksPayments, setSessionCanConfirmQuickBooksPayments] = useState(false);

  const [overviewPeriod, setOverviewPeriod] = useState<OverviewPeriodKey>("currentWeek");
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [recordsLoading, setRecordsLoading] = useState(true);
  const [recordsError, setRecordsError] = useState("");

  const [submissions, setSubmissions] = useState<ModerationSubmission[]>([]);
  const [submissionsLoading, setSubmissionsLoading] = useState(true);
  const [submissionsLoadingMore, setSubmissionsLoadingMore] = useState(false);
  const [submissionsError, setSubmissionsError] = useState("");
  const [submissionsLoadMoreError, setSubmissionsLoadMoreError] = useState("");
  const [submissionsHasMore, setSubmissionsHasMore] = useState(false);
  const [submissionsNextCursor, setSubmissionsNextCursor] = useState<string | null>(null);
  const [submissionsUpdatedAt, setSubmissionsUpdatedAt] = useState("");
  const [submissionsRefreshFailed, setSubmissionsRefreshFailed] = useState(false);

  const [todayPayments, setTodayPayments] = useState<DashboardQuickBooksViewRow[]>([]);
  const [todayPaymentsLoading, setTodayPaymentsLoading] = useState(true);
  const [todayPaymentsError, setTodayPaymentsError] = useState("");
  const [confirmingPaymentIds, setConfirmingPaymentIds] = useState<Record<string, boolean>>({});

  const [activeSubmission, setActiveSubmission] = useState<ModerationSubmission | null>(null);
  const [submissionFiles, setSubmissionFiles] = useState<ModerationSubmissionFile[]>([]);
  const [submissionFilesLoading, setSubmissionFilesLoading] = useState(false);
  const [submissionFilesError, setSubmissionFilesError] = useState("");
  const [approvalChecked, setApprovalChecked] = useState(false);
  const [isModerationActionRunning, setIsModerationActionRunning] = useState(false);

  const activeFilesRequestRef = useRef(0);
  const submissionsRequestRef = useRef(0);
  const todayPaymentsRef = useRef<DashboardQuickBooksViewRow[]>([]);
  const todayPaymentsKeySequenceRef = useRef(0);

  const overviewMetrics = useMemo(
    () => calculateOverviewMetrics(records, overviewPeriod),
    [overviewPeriod, records],
  );
  const todayQuickBooksKpi = useMemo(() => {
    if (!todayPayments.length) {
      return "$0.00";
    }
    const total = todayPayments.reduce((sum, item) => sum + (Number(item.paymentAmount) || 0), 0);
    return CURRENCY_FORMATTER.format(total);
  }, [todayPayments]);

  const todayLabel = useMemo(
    () => `${formatDateForApiInChicago(new Date()).replace(/-/g, "/")} (Chicago time)`,
    [],
  );

  const showDashboardMessage = useCallback((text: string, tone: "success" | "error" | "info" = "info") => {
    const message = String(text || "").trim();
    if (!message) {
      return;
    }

    showToast({
      type: tone,
      message,
      dedupeKey: `dashboard-${tone}-${message}`,
      cooldownMs: 3000,
    });
  }, []);

  const openSubmissionModal = useCallback((submission: ModerationSubmission) => {
    setActiveSubmission(submission);
    setApprovalChecked(false);
    setSubmissionFiles([]);
    setSubmissionFilesError("");
  }, []);

  const closeSubmissionModal = useCallback(() => {
    setActiveSubmission(null);
    setApprovalChecked(false);
    setSubmissionFiles([]);
    setSubmissionFilesError("");
    activeFilesRequestRef.current += 1;
  }, []);

  const loadOverview = useCallback(async () => {
    setRecordsLoading(true);
    setRecordsError("");
    try {
      const payload = await getRecords();
      const items = Array.isArray(payload.records) ? payload.records : [];
      setRecords(items);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load overview data.";
      setRecords([]);
      setRecordsError(message);
      showDashboardMessage(message, "error");
    } finally {
      setRecordsLoading(false);
    }
  }, [showDashboardMessage]);

  const loadPendingSubmissions = useCallback(
    async (options?: { append?: boolean; cursor?: string | null }) => {
      const append = options?.append === true;
      const cursor = append ? normalizeModerationNextCursor(options?.cursor) : null;
      const requestId = submissionsRequestRef.current + 1;
      submissionsRequestRef.current = requestId;

      if (append) {
        setSubmissionsLoadingMore(true);
        setSubmissionsLoadMoreError("");
      } else {
        setSubmissionsLoading(true);
        setSubmissionsLoadingMore(false);
        setSubmissionsError("");
        setSubmissionsLoadMoreError("");
        setSubmissionsHasMore(false);
        setSubmissionsNextCursor(null);
      }

      try {
        const payload = await getModerationSubmissions("pending", MODERATION_PAGE_LIMIT, cursor);
        if (requestId !== submissionsRequestRef.current) {
          return;
        }

        const items = Array.isArray(payload.items) ? payload.items : [];
        const nextCursor = normalizeModerationNextCursor(payload.nextCursor);
        const hasMore = payload.hasMore === true && Boolean(nextCursor);

        if (append) {
          setSubmissions((previous) => mergeModerationSubmissions(previous, items));
        } else {
          setSubmissions(items);
          setSubmissionsError("");
        }

        setSubmissionsHasMore(hasMore);
        setSubmissionsNextCursor(hasMore ? nextCursor : null);
        setSubmissionsRefreshFailed(false);
        setSubmissionsUpdatedAt(
          new Date().toLocaleTimeString("en-US", {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
          }),
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to load submissions.";
        if (requestId !== submissionsRequestRef.current) {
          return;
        }

        if (append) {
          setSubmissionsLoadMoreError(message);
          showDashboardMessage(message, "error");
          return;
        }

        setSubmissions([]);
        setSubmissionsError(message);
        setSubmissionsRefreshFailed(true);
        setSubmissionsUpdatedAt("");
        setSubmissionsHasMore(false);
        setSubmissionsNextCursor(null);
        showDashboardMessage(message, "error");
      } finally {
        if (requestId === submissionsRequestRef.current) {
          if (append) {
            setSubmissionsLoadingMore(false);
          } else {
            setSubmissionsLoading(false);
          }
        }
      }
    },
    [showDashboardMessage],
  );

  const loadTodayQuickBooksPayments = useCallback(async () => {
    setTodayPaymentsLoading(true);
    setTodayPaymentsError("");
    try {
      const todayIso = formatDateForApiInChicago(new Date());
      const payload = await getQuickBooksPayments({
        from: todayIso,
        to: todayIso,
      });
      const items = Array.isArray(payload.items) ? payload.items : [];
      setTodayPayments(
        withStableRowKeys(items, todayPaymentsRef.current, todayPaymentsKeySequenceRef, {
          prefix: "dashboard-payment",
          signature: quickBooksRowSignature,
        }),
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load QuickBooks payments.";
      setTodayPayments([]);
      setTodayPaymentsError(message);
      showDashboardMessage(message, "error");
    } finally {
      setTodayPaymentsLoading(false);
    }
  }, [showDashboardMessage]);

  const reloadDashboard = useCallback(async () => {
    await Promise.all([loadOverview(), loadPendingSubmissions(), loadTodayQuickBooksPayments()]);
  }, [loadOverview, loadPendingSubmissions, loadTodayQuickBooksPayments]);

  const loadMorePendingSubmissions = useCallback(async () => {
    if (!submissionsHasMore || !submissionsNextCursor || submissionsLoading || submissionsLoadingMore) {
      return;
    }

    await loadPendingSubmissions({
      append: true,
      cursor: submissionsNextCursor,
    });
  }, [
    loadPendingSubmissions,
    submissionsHasMore,
    submissionsNextCursor,
    submissionsLoading,
    submissionsLoadingMore,
  ]);

  useEffect(() => {
    void getSession()
      .then((payload) => {
        setSessionCanReview(Boolean(payload?.permissions?.review_moderation));
        setSessionCanConfirmQuickBooksPayments(Boolean(payload?.permissions?.sync_quickbooks));
      })
      .catch(() => {
        setSessionCanReview(false);
        setSessionCanConfirmQuickBooksPayments(false);
      });
    void reloadDashboard();
  }, [reloadDashboard]);

  useEffect(() => {
    if (!activeSubmission?.id) {
      return;
    }

    const requestId = activeFilesRequestRef.current + 1;
    activeFilesRequestRef.current = requestId;
    setSubmissionFilesLoading(true);
    setSubmissionFilesError("");

    void getModerationSubmissionFiles(activeSubmission.id)
      .then((payload) => {
        if (requestId !== activeFilesRequestRef.current) {
          return;
        }
        setSubmissionFiles(Array.isArray(payload.items) ? payload.items : []);
      })
      .catch((error) => {
        if (requestId !== activeFilesRequestRef.current) {
          return;
        }
        setSubmissionFiles([]);
        setSubmissionFilesError(error instanceof Error ? error.message : "Failed to load attachments.");
      })
      .finally(() => {
        if (requestId === activeFilesRequestRef.current) {
          setSubmissionFilesLoading(false);
        }
      });
  }, [activeSubmission?.id]);

  useEffect(() => {
    todayPaymentsRef.current = todayPayments;
  }, [todayPayments]);

  async function runModerationAction(action: "approve" | "reject") {
    if (!activeSubmission || isModerationActionRunning) {
      return;
    }

    if (action === "approve" && !approvalChecked) {
      showDashboardMessage('Check "Add to main database" or click "Delete".', "error");
      return;
    }

    if (action === "reject") {
      const shouldDelete = window.confirm("Delete this submission from the moderation queue?");
      if (!shouldDelete) {
        return;
      }
    }

    setIsModerationActionRunning(true);
    try {
      if (action === "approve") {
        await approveModerationSubmission(activeSubmission.id);
        showDashboardMessage("Client added to the main database.", "success");
      } else {
        await rejectModerationSubmission(activeSubmission.id);
        showDashboardMessage("Submission removed from the moderation queue.", "success");
      }
      closeSubmissionModal();
      await reloadDashboard();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to apply moderation action.";
      showDashboardMessage(message, "error");
    } finally {
      setIsModerationActionRunning(false);
    }
  }

  const runQuickBooksPaymentConfirm = useCallback(
    async (payment: DashboardQuickBooksViewRow) => {
      if (!sessionCanConfirmQuickBooksPayments) {
        showDashboardMessage("You do not have permission to confirm payments.", "error");
        return;
      }

      const transactionId = String(payment.transactionId || "").trim();
      if (!transactionId) {
        showDashboardMessage("Payment confirmation is unavailable for this row.", "error");
        return;
      }

      if (payment.matchedConfirmed) {
        return;
      }

      const confirmed = window.confirm("Confirm payment? Yes/No");
      if (!confirmed) {
        return;
      }

      setConfirmingPaymentIds((previous) => ({
        ...previous,
        [transactionId]: true,
      }));

      try {
        await confirmQuickBooksRecentPayment({
          transactionId,
          transactionType: payment.transactionType || "payment",
        });

        setTodayPayments((previousRows) =>
          previousRows.map((row) =>
            String(row.transactionId || "").trim() === transactionId
              ? {
                  ...row,
                  matchedConfirmed: true,
                }
              : row,
          ),
        );
        showDashboardMessage("Payment confirmed.", "success");
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to confirm payment.";
        showDashboardMessage(message, "error");
      } finally {
        setConfirmingPaymentIds((previous) => {
          const next = { ...previous };
          delete next[transactionId];
          return next;
        });
      }
    },
    [sessionCanConfirmQuickBooksPayments, showDashboardMessage],
  );

  const submissionsColumns = useMemo<TableColumn<ModerationSubmission>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (submission) => getClientField(submission, "clientName") || "Unnamed",
      },
      {
        key: "companyName",
        label: "Company",
        align: "left",
        cell: (submission) => getClientField(submission, "companyName") || "-",
      },
      {
        key: "closedBy",
        label: "Closed By",
        align: "left",
        cell: (submission) => getClientField(submission, "closedBy") || formatSubmittedBy(submission.submittedBy),
      },
      {
        key: "submittedAt",
        label: "Submitted",
        align: "center",
        cell: (submission) => formatDateTime(submission.submittedAt || ""),
      },
      {
        key: "actions",
        label: "",
        align: "right",
        cell: (submission) => (
          <Button
            type="button"
            variant="secondary"
            size="sm"
            onClick={(event) => {
              event.stopPropagation();
              openSubmissionModal(submission);
            }}
          >
            Open
          </Button>
        ),
      },
    ];
  }, [openSubmissionModal]);

  const todayPaymentsColumns = useMemo<TableColumn<DashboardQuickBooksViewRow>[]>(() => {
    return [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        cell: (item) => {
          const label = formatQuickBooksClientLabel(item.clientName, item.transactionType, Number(item.paymentAmount) || 0);
          const normalizedClientName = String(item.clientName || "").trim();
          if (!normalizedClientName) {
            return label;
          }

          return (
            <button
              type="button"
              className="payments-today-client-link"
              onClick={() =>
                requestOpenClientCard(normalizedClientName, {
                  fallbackHref: "/app/client-payments",
                })
              }
              aria-label={`Open client card for ${normalizedClientName}`}
            >
              {label}
            </button>
          );
        },
      },
      {
        key: "paymentAmount",
        label: "Amount",
        align: "right",
        cell: (item) => CURRENCY_FORMATTER.format(Number(item.paymentAmount) || 0),
      },
      {
        key: "paymentDate",
        label: "Date",
        align: "center",
        cell: (item) => {
          const transactionId = String(item.transactionId || "").trim();
          const isConfirming = Boolean(transactionId && confirmingPaymentIds[transactionId]);
          const matchedRecordId = String(item.matchedRecordId || "").trim();
          const canConfirm =
            sessionCanConfirmQuickBooksPayments &&
            Boolean(transactionId) &&
            Boolean(matchedRecordId) &&
            !item.matchedConfirmed;

          return (
            <span className="payments-today-date-cell">
              <span>{formatDate(item.paymentDate || "")}</span>
              {canConfirm ? (
                <button
                  type="button"
                  className="payments-today-confirm-btn"
                  disabled={isConfirming}
                  onClick={(event) => {
                    event.stopPropagation();
                    void runQuickBooksPaymentConfirm(item);
                  }}
                  aria-label={`Confirm payment for ${String(item.clientName || "").trim() || "client"}`}
                  title="Confirm payment"
                >
                  {isConfirming ? "..." : "✓"}
                </button>
              ) : null}
            </span>
          );
        },
      },
    ];
  }, [confirmingPaymentIds, runQuickBooksPaymentConfirm, sessionCanConfirmQuickBooksPayments]);

  const isReloading = submissionsLoading || recordsLoading || todayPaymentsLoading;

  return (
    <PageShell className="dashboard-react-page">
      <PageHeader
        actions={
          <Button
            type="button"
            size="sm"
            onClick={() => void reloadDashboard()}
            isLoading={isReloading}
            aria-label="Refresh dashboard data"
          >
            Refresh
          </Button>
        }
        meta={
          <div className="table-panel-sync-row">
            <p className={`table-panel-updated-at ${submissionsRefreshFailed ? "error" : ""}`.trim()}>
              {submissionsUpdatedAt
                ? `Moderation updated ${submissionsUpdatedAt}`
                : submissionsRefreshFailed
                  ? "Last moderation refresh failed."
                  : ""}
            </p>
          </div>
        }
      />

      <Panel
        className="period-dashboard-shell-react"
        header={
          <div
            id="toggle-overview-panel"
            className="period-dashboard-shell__header period-dashboard-shell__header--toggle"
            aria-controls="react-dashboard-overview-content"
            aria-expanded
          >
            <div className="period-dashboard-shell__header-main">
              <h2 className="period-dashboard-shell__heading-text">Overview</h2>
            </div>
            <div className="period-dashboard-shell__header-actions">
              <SegmentedControl
                value={overviewPeriod}
                options={OVERVIEW_PERIOD_OPTIONS}
                onChange={(value) => setOverviewPeriod(value as OverviewPeriodKey)}
              />
            </div>
          </div>
        }
      >
        <div id="react-dashboard-overview-content" className="overview-cards">
          <article className="overview-card" aria-label="Total received by period">
            <h3 className="overview-card__title">Total Received</h3>
            <p className="overview-card__value">{formatKpiMoney(overviewMetrics.received)}</p>
            <p className="overview-card__context">{labelByPeriod(overviewPeriod)}</p>
          </article>
          <article className="overview-card" aria-label="Total sales by period">
            <h3 className="overview-card__title">Total Sales</h3>
            <p className="overview-card__value">{formatKpiMoney(overviewMetrics.sales)}</p>
            <p className="overview-card__context">{labelByPeriod(overviewPeriod)}</p>
          </article>
          <article className="overview-card" aria-label="Total debt">
            <h3 className="overview-card__title">Total Debt</h3>
            <p className="overview-card__value">{formatKpiMoney(overviewMetrics.debt)}</p>
            <p className="overview-card__context">As of today</p>
          </article>
        </div>
        {recordsLoading ? <LoadingSkeleton rows={3} /> : null}
        {!recordsLoading && recordsError ? (
          <ErrorState
            title="Failed to load overview data"
            description={recordsError}
            actionLabel="Retry"
            onAction={() => void loadOverview()}
          />
        ) : null}
      </Panel>

      <Panel className="table-panel moderation-table-panel" title="New Clients from Mini App">
        {submissionsLoading ? <LoadingSkeleton rows={5} /> : null}
        {!submissionsLoading && submissionsError ? (
          <ErrorState
            title="Failed to load moderation submissions"
            description={submissionsError}
            actionLabel="Retry"
            onAction={() => void loadPendingSubmissions()}
          />
        ) : null}
        {!submissionsLoading && !submissionsError && !submissions.length ? (
          <EmptyState title="No new clients pending moderation." />
        ) : null}

        {!submissionsLoading && !submissionsError && submissions.length ? (
          <>
            <Table
              className="moderation-table-wrap mini-clients-table-wrap"
              columns={submissionsColumns}
              rows={submissions}
              rowKey={(submission) => submission.id}
              onRowActivate={(submission) => openSubmissionModal(submission)}
              density="compact"
            />

            {(submissionsHasMore || submissionsLoadingMore || submissionsLoadMoreError) ? (
              <div className="moderation-pagination-row">
                {submissionsLoadMoreError ? (
                  <p className="moderation-pagination-error">{submissionsLoadMoreError}</p>
                ) : null}
                {submissionsHasMore ? (
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    onClick={() => void loadMorePendingSubmissions()}
                    isLoading={submissionsLoadingMore}
                    disabled={submissionsLoadingMore || submissionsLoading}
                  >
                    Load more
                  </Button>
                ) : null}
              </div>
            ) : null}
          </>
        ) : null}
      </Panel>

      <Panel className="table-panel payments-today-panel" title="New Payment Today">
        <p className={`payments-today-kpi ${todayPaymentsError ? "error" : !todayPayments.length ? "is-muted" : ""}`.trim()}>
          {todayPaymentsError
            ? todayPaymentsError
            : todayPayments.length
              ? `${todayQuickBooksKpi} - ${todayPayments.length} QuickBooks payment${todayPayments.length === 1 ? "" : "s"}`
              : "No QuickBooks payments recorded today."}
        </p>
        <p className="payments-today-date">{todayLabel}</p>

        {todayPaymentsLoading ? <LoadingSkeleton rows={4} /> : null}
        {!todayPaymentsLoading && !todayPaymentsError && !todayPayments.length ? (
          <EmptyState title="No QuickBooks payments recorded for today." />
        ) : null}

        {!todayPaymentsLoading && todayPayments.length ? (
          <Table
            className="payments-today-table-wrap"
            columns={todayPaymentsColumns}
            rows={todayPayments}
            rowKey={(item) => item._rowKey}
            density="compact"
          />
        ) : null}
      </Panel>

      <Modal
        open={Boolean(activeSubmission)}
        title="Client Review"
        onClose={closeSubmissionModal}
        footer={
          <div className="client-payments__modal-actions">
            <label className="cb-checkbox-row moderation-approval-label">
              <input
                id="approval-checkbox"
                type="checkbox"
                checked={approvalChecked}
                onChange={(event) => setApprovalChecked(event.target.checked)}
              />
              <span>Add to main database</span>
            </label>
            <div className="moderation-modal-actions">
              <Button
                id="apply-moderation-button"
                type="button"
                size="sm"
                onClick={() => void runModerationAction("approve")}
                isLoading={isModerationActionRunning}
                disabled={isModerationActionRunning || !sessionCanReview}
              >
                Apply
              </Button>
              <Button
                id="delete-moderation-button"
                type="button"
                variant="danger"
                size="sm"
                onClick={() => void runModerationAction("reject")}
                isLoading={isModerationActionRunning}
                disabled={isModerationActionRunning || !sessionCanReview}
              >
                Delete
              </Button>
              <Button type="button" variant="secondary" size="sm" onClick={closeSubmissionModal}>
                Close
              </Button>
            </div>
          </div>
        }
      >
        {activeSubmission ? (
          <>
            <div className="record-details-grid submission-modal__details">
              {MODERATION_CLIENT_DETAIL_FIELDS.map((field) => (
                <Detail key={field.key} label={field.label} value={getClientField(activeSubmission, field.key)} />
              ))}
              <Detail label="Submitted By" value={formatSubmittedBy(activeSubmission.submittedBy)} />
              {Object.entries(activeSubmission.client || {})
                .filter(([key, value]) => !SHOWN_CLIENT_FIELDS.has(key) && formatClientFieldValue(value).length > 0)
                .map(([key, value]) => (
                  <Detail key={key} label={key} value={formatClientFieldValue(value)} />
                ))}
            </div>

            <section className="submission-files" hidden={false}>
              <h4 className="submission-files__title">Attachments</h4>
              {submissionFilesLoading ? <LoadingSkeleton rows={2} /> : null}
              {!submissionFilesLoading && submissionFilesError ? (
                <ErrorState title="Failed to load attachments" description={submissionFilesError} />
              ) : null}
              {!submissionFilesLoading && !submissionFilesError && !submissionFiles.length ? (
                <p className="submission-file-empty">No attachments.</p>
              ) : null}
              {!submissionFilesLoading && submissionFiles.length ? (
                <div className="submission-files__list">
                  {submissionFiles.map((file) => (
                    <div key={file.id} className="submission-file-row">
                      <div className="submission-file-info">
                        <div className="submission-file-name">{file.fileName || "attachment"}</div>
                        <div className="submission-file-meta">{`${formatFileSize(file.sizeBytes)} · ${file.mimeType || "application/octet-stream"}`}</div>
                      </div>
                      <div className="submission-file-actions">
                        {file.canPreview && file.previewUrl ? (
                          <a className="cb-button cb-button--secondary cb-button--sm" href={file.previewUrl} target="_blank" rel="noreferrer">
                            Preview
                          </a>
                        ) : null}
                        {file.downloadUrl ? (
                          <a
                            className="cb-button cb-button--secondary cb-button--sm"
                            href={file.downloadUrl}
                            download={file.fileName || "attachment"}
                          >
                            Download
                          </a>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>
              ) : null}
            </section>
          </>
        ) : null}
      </Modal>
    </PageShell>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="record-details-grid__item">
      <span className="record-details-grid__label">{label}</span>
      <span className="record-details-grid__value">{value || "-"}</span>
    </div>
  );
}

function getClientField(submission: ModerationSubmission, key: string): string {
  const clientValue = getObjectFieldValue(submission?.client, key);
  if (clientValue) {
    return clientValue;
  }
  return getObjectFieldValue(submission?.miniData, key);
}

function getObjectFieldValue(source: unknown, key: string): string {
  if (!source || typeof source !== "object") {
    return "";
  }
  return formatClientFieldValue((source as Record<string, unknown>)[key]);
}

function formatClientFieldValue(rawValue: unknown): string {
  if (rawValue === null || rawValue === undefined) {
    return "";
  }
  if (typeof rawValue === "boolean") {
    return rawValue ? "Yes" : "No";
  }
  return String(rawValue).trim();
}

function formatSubmittedBy(submittedBy: ModerationSubmission["submittedBy"]): string {
  if (!submittedBy || typeof submittedBy !== "object") {
    return "-";
  }
  const username = String(submittedBy.username || "").trim();
  if (username) {
    return `@${username}`;
  }
  const firstName = String(submittedBy.first_name || "").trim();
  const lastName = String(submittedBy.last_name || "").trim();
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    return fullName;
  }
  const userId = String(submittedBy.id || "").trim();
  return userId ? `tg:${userId}` : "-";
}

function formatFileSize(value: number): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }
  const kilobyte = 1024;
  const megabyte = 1024 * 1024;
  if (bytes >= megabyte) {
    return `${(bytes / megabyte).toFixed(bytes >= 10 * megabyte ? 0 : 1)} MB`;
  }
  if (bytes >= kilobyte) {
    return `${Math.round(bytes / kilobyte)} KB`;
  }
  return `${bytes} B`;
}

function formatDateForApiInChicago(dateValue: Date): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: QUICKBOOKS_DASHBOARD_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(dateValue);
  const values: Record<string, string> = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      values[part.type] = part.value;
    }
  }
  const year = (values.year || "").trim();
  const month = (values.month || "").trim().padStart(2, "0");
  const day = (values.day || "").trim().padStart(2, "0");
  if (year && month && day) {
    return `${year}-${month}-${day}`;
  }
  return new Date(dateValue).toISOString().slice(0, 10);
}

function formatQuickBooksClientLabel(clientName: string, transactionType: string, amount: number): string {
  const normalizedName = String(clientName || "Unknown client").trim() || "Unknown client";
  const normalizedType = String(transactionType || "").trim().toLowerCase();
  if (normalizedType === "refund") {
    return `${normalizedName} (Refund)`;
  }
  if (normalizedType === "creditmemo" || (normalizedType === "payment" && amount < 0)) {
    return `${normalizedName} (Write-off)`;
  }
  return normalizedName;
}

function quickBooksRowSignature(row: QuickBooksPaymentRow): string {
  return [
    String(row.clientName || "").trim(),
    String(row.clientPhone || "").trim(),
    String(row.clientEmail || "").trim(),
    String(row.paymentDate || "").trim(),
    String(row.paymentAmount ?? "").trim(),
    String(row.transactionType || "").trim(),
  ].join("|");
}

function labelByPeriod(period: OverviewPeriodKey): string {
  const found = OVERVIEW_PERIOD_OPTIONS.find((item) => item.key === period);
  return found?.label || "Current Week";
}
