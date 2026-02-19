import { useMemo } from "react";

import {
  formatDate,
  formatKpiMoney,
  formatMoney,
  getRecordStatusFlags,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import {
  OVERDUE_RANGE_OPTIONS,
  OVERVIEW_PERIOD_OPTIONS,
  STATUS_FILTER_OVERDUE,
  STATUS_FILTER_OPTIONS,
  TABLE_COLUMNS,
  type OverviewPeriodKey,
} from "@/features/client-payments/domain/constants";
import { exportRecordsToPdf, exportRecordsToXls } from "@/features/client-payments/domain/export";
import { RecordDetails } from "@/features/client-payments/components/RecordDetails";
import { RecordEditorForm } from "@/features/client-payments/components/RecordEditorForm";
import { StatusBadges } from "@/features/client-payments/components/StatusBadges";
import { useClientPayments } from "@/features/client-payments/hooks/useClientPayments";
import type { ClientRecord } from "@/shared/types/records";
import {
  Badge,
  Button,
  DateInput,
  EmptyState,
  ErrorState,
  Input,
  LoadingSkeleton,
  Modal,
  SegmentedControl,
  Select,
  Toast,
} from "@/shared/ui";

const COLUMN_LABELS: Record<string, string> = {
  clientName: "Client Name",
  closedBy: "Closed By",
  companyName: "Company",
  serviceType: "Service",
  contractTotals: "Contract",
  totalPayments: "Paid",
  futurePayments: "Balance",
  afterResult: "After Result",
  writtenOff: "Written Off",
  dateWhenFullyPaid: "Fully Paid Date",
  createdAt: "Created",
};

function getOverviewContextLabel(period: OverviewPeriodKey): string {
  const found = OVERVIEW_PERIOD_OPTIONS.find((option) => option.key === period);
  return found?.label || "Current Week";
}

export default function ClientPaymentsPage() {
  const {
    session,
    canManage,
    isLoading,
    loadError,
    records,
    visibleRecords,
    filters,
    sortState,
    overviewPeriod,
    overviewMetrics,
    tableTotals,
    closedByOptions,
    filtersCollapsed,
    isSaving,
    saveError,
    saveRetryCount,
    saveRetryMax,
    saveRetryGiveUp,
    hasUnsavedChanges,
    lastSyncedAt,
    modalState,
    isDiscardConfirmOpen,
    activeRecord,
    updateFilter,
    setDateRange,
    setOverviewPeriod,
    setFiltersCollapsed,
    toggleSort,
    forceRefresh,
    openCreateModal,
    openRecordModal,
    startEditRecord,
    requestCloseModal,
    cancelDiscardModalClose,
    discardDraftAndCloseModal,
    updateDraftField,
    saveDraft,
    retrySave,
  } = useClientPayments();

  const sortableColumns = new Set<keyof ClientRecord>(
    TABLE_COLUMNS.filter((column) => column !== "afterResult" && column !== "writtenOff"),
  );

  const isViewMode = modalState.mode === "view";

  const counters = useMemo(() => {
    let writtenOffCount = 0;
    let fullyPaidCount = 0;
    let overdueCount = 0;

    for (const record of visibleRecords) {
      const status = getRecordStatusFlags(record);
      if (status.isWrittenOff) {
        writtenOffCount += 1;
      }
      if (status.isFullyPaid) {
        fullyPaidCount += 1;
      }
      if (status.isOverdue) {
        overdueCount += 1;
      }
    }

    return {
      totalCount: records.length,
      filteredCount: visibleRecords.length,
      writtenOffCount,
      fullyPaidCount,
      overdueCount,
    };
  }, [records.length, visibleRecords]);

  function clearAllFilters() {
    updateFilter("search", "");
    updateFilter("closedBy", "");
    updateFilter("status", "all");
    updateFilter("overdueRange", "");
    setDateRange("createdAtRange", "from", "");
    setDateRange("createdAtRange", "to", "");
    setDateRange("paymentDateRange", "from", "");
    setDateRange("paymentDateRange", "to", "");
    setDateRange("writtenOffDateRange", "from", "");
    setDateRange("writtenOffDateRange", "to", "");
    setDateRange("fullyPaidDateRange", "from", "");
    setDateRange("fullyPaidDateRange", "to", "");
  }

  return (
    <div className="dashboard-home">
      <section className="section page-stats-bar" aria-live="polite">
        <div className="page-stats-bar__row">
          <div className="page-header__stats">
            <span className="stat-chip">
              <span className="stat-chip__label">Clients:</span>
              <span className="stat-chip__value">{counters.totalCount}</span>
            </span>
            <span className="stat-chip">
              <span className="stat-chip__label">Filtered:</span>
              <span className="stat-chip__value">{counters.filteredCount}</span>
            </span>
            <span className="stat-chip">
              <span className="stat-chip__label">Written Off:</span>
              <span className="stat-chip__value">{counters.writtenOffCount}</span>
            </span>
            <span className="stat-chip">
              <span className="stat-chip__label">Fully Paid:</span>
              <span className="stat-chip__value">{counters.fullyPaidCount}</span>
            </span>
            <span className="stat-chip">
              <span className="stat-chip__label">Overdue:</span>
              <span className="stat-chip__value">{counters.overdueCount}</span>
            </span>
          </div>
          <p className="table-panel-updated-at">Last sync: {lastSyncedAt}</p>
        </div>
        {saveError ? (
          <div className="save-error-stack">
            <Toast kind="error" message={saveError} />
            {saveRetryGiveUp ? (
              <Button type="button" variant="secondary" size="sm" onClick={retrySave}>
                Retry Save
              </Button>
            ) : null}
            {!saveRetryGiveUp && saveRetryCount > 0 ? (
              <span className="react-user-footnote">
                Retry {saveRetryCount}/{saveRetryMax}
              </span>
            ) : null}
          </div>
        ) : null}
        {!saveError && hasUnsavedChanges ? <Toast kind="info" message="Unsaved changes are syncing..." /> : null}
        {!hasUnsavedChanges && !saveError && isSaving ? <Toast kind="info" message="Saving changes..." /> : null}
      </section>

      <div className={`grid dashboard-grid-react ${filtersCollapsed ? "is-filters-collapsed" : ""}`.trim()}>
        <section className="section filters-panel" aria-label="Filtering options">
          <div className="filters-panel__header">
            <h2 className="section-heading">Filters</h2>
            <button
              type="button"
              className="filters-toggle-btn"
              aria-label={filtersCollapsed ? "Expand filters panel" : "Collapse filters panel"}
              aria-expanded={!filtersCollapsed}
              onClick={() => setFiltersCollapsed(!filtersCollapsed)}
            >
              <span className="filters-toggle-icon" aria-hidden="true">
                {filtersCollapsed ? "›" : "‹"}
              </span>
            </button>
          </div>

          {!filtersCollapsed ? (
            <div className="filters-panel__content">
              <label className="search-label" htmlFor="records-search-input">
                Search by client or company
              </label>
              <div className="search-row">
                <Input
                  id="records-search-input"
                  value={filters.search}
                  onChange={(event) => updateFilter("search", event.target.value)}
                  placeholder="For example: John Smith or ACME Logistics"
                />
                <Button type="button" variant="secondary" size="sm" onClick={clearAllFilters}>
                  Reset
                </Button>
              </div>

              <div className="filters-grid-react">
                <div className="filter-field">
                  <label htmlFor="created-from-input">New Client From</label>
                  <DateInput
                    id="created-from-input"
                    value={filters.createdAtRange.from}
                    onChange={(nextValue) => setDateRange("createdAtRange", "from", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="created-to-input">To</label>
                  <DateInput
                    id="created-to-input"
                    value={filters.createdAtRange.to}
                    onChange={(nextValue) => setDateRange("createdAtRange", "to", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="payments-from-input">Payments From</label>
                  <DateInput
                    id="payments-from-input"
                    value={filters.paymentDateRange.from}
                    onChange={(nextValue) => setDateRange("paymentDateRange", "from", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field">
                  <label htmlFor="payments-to-input">To</label>
                  <DateInput
                    id="payments-to-input"
                    value={filters.paymentDateRange.to}
                    onChange={(nextValue) => setDateRange("paymentDateRange", "to", nextValue)}
                    placeholder="MM/DD/YYYY"
                  />
                </div>
                <div className="filter-field filter-field--full">
                  <label htmlFor="closed-by-filter-select">Closed By</label>
                  <Select
                    id="closed-by-filter-select"
                    value={filters.closedBy}
                    onChange={(event) => updateFilter("closedBy", event.target.value)}
                  >
                    <option value="">All</option>
                    {closedByOptions.map((option) => (
                      <option key={option} value={option}>
                        {option}
                      </option>
                    ))}
                  </Select>
                </div>
              </div>

              <SegmentedControl
                value={filters.status}
                options={STATUS_FILTER_OPTIONS}
                onChange={(value) => updateFilter("status", value as typeof filters.status)}
              />

              {filters.status === STATUS_FILTER_OVERDUE ? (
                <SegmentedControl
                  value={filters.overdueRange}
                  options={OVERDUE_RANGE_OPTIONS}
                  onChange={(value) => updateFilter("overdueRange", value as typeof filters.overdueRange)}
                />
              ) : null}

              {filters.status === "written-off" ? (
                <div className="written-off-date-filter-react">
                  <div className="filter-field">
                    <label htmlFor="written-off-from-input">Written Off Date From</label>
                    <DateInput
                      id="written-off-from-input"
                      value={filters.writtenOffDateRange.from}
                      onChange={(nextValue) => setDateRange("writtenOffDateRange", "from", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                  <div className="filter-field">
                    <label htmlFor="written-off-to-input">To</label>
                    <DateInput
                      id="written-off-to-input"
                      value={filters.writtenOffDateRange.to}
                      onChange={(nextValue) => setDateRange("writtenOffDateRange", "to", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                </div>
              ) : null}

              {filters.status === "fully-paid" ? (
                <div className="written-off-date-filter-react">
                  <div className="filter-field">
                    <label htmlFor="fully-paid-from-input">Fully Paid Date From</label>
                    <DateInput
                      id="fully-paid-from-input"
                      value={filters.fullyPaidDateRange.from}
                      onChange={(nextValue) => setDateRange("fullyPaidDateRange", "from", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                  <div className="filter-field">
                    <label htmlFor="fully-paid-to-input">To</label>
                    <DateInput
                      id="fully-paid-to-input"
                      value={filters.fullyPaidDateRange.to}
                      onChange={(nextValue) => setDateRange("fullyPaidDateRange", "to", nextValue)}
                      placeholder="MM/DD/YYYY"
                    />
                  </div>
                </div>
              ) : null}
            </div>
          ) : null}
        </section>

        <section className="section period-dashboard-shell-react" aria-label="Period financial dashboard">
          <div className="period-dashboard-shell__header">
            <div className="period-dashboard-shell__header-main">
              <h2 className="period-dashboard-shell__heading-text">Overview</h2>
            </div>
            <div className="period-dashboard-shell__header-actions">
              <SegmentedControl
                value={overviewPeriod}
                options={OVERVIEW_PERIOD_OPTIONS.map((option) => ({ key: option.key, label: option.label }))}
                onChange={(value) => setOverviewPeriod(value as OverviewPeriodKey)}
              />
            </div>
          </div>

          <div className="overview-cards">
            <article className="overview-card">
              <p className="overview-card__title">Sales</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.sales)}</p>
              <p className="overview-card__context">{getOverviewContextLabel(overviewPeriod)}</p>
            </article>
            <article className="overview-card">
              <p className="overview-card__title">Received</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.received)}</p>
              <p className="overview-card__context">{getOverviewContextLabel(overviewPeriod)}</p>
            </article>
            <article className="overview-card">
              <p className="overview-card__title">Debt</p>
              <p className="overview-card__value">{formatKpiMoney(overviewMetrics.debt)}</p>
              <p className="overview-card__context">As of today</p>
            </article>
          </div>
        </section>

        <section className="section table-panel" aria-label="Client records table">
          <header className="table-panel__header">
            <h2 className="section-heading">Records</h2>
            <div className="table-panel__actions">
              <Button variant="secondary" size="sm" onClick={() => void forceRefresh()} isLoading={isLoading}>
                Refresh
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToXls(visibleRecords)}>
                Export XLS
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToPdf(visibleRecords)}>
                Export PDF
              </Button>
              {canManage ? (
                <Button size="sm" onClick={openCreateModal}>
                  Add Client
                </Button>
              ) : null}
            </div>
          </header>

          {isLoading ? <LoadingSkeleton rows={8} /> : null}
          {!isLoading && loadError ? (
            <ErrorState
              title="Failed to load records"
              description={loadError}
              actionLabel="Retry"
              onAction={() => void forceRefresh()}
            />
          ) : null}
          {!isLoading && !loadError && !visibleRecords.length ? (
            <EmptyState title="No records found" description="Adjust filters or add a new client." />
          ) : null}

          {!isLoading && !loadError && visibleRecords.length ? (
            <div className="table-wrap">
              <table className="cb-table" id="payments-table">
                <thead>
                  <tr>
                    {TABLE_COLUMNS.map((column) => {
                      const isSortable = sortableColumns.has(column);
                      const isActive = sortState.key === column;

                      return (
                        <th key={column}>
                          {isSortable ? (
                            <button
                              type="button"
                              className={`th-sort-btn ${isActive ? "is-active" : ""}`.trim()}
                              onClick={() => toggleSort(column)}
                            >
                              <span className="th-sort-label">{COLUMN_LABELS[column] || column}</span>
                              {isActive ? (
                                <span className="th-sort-indicator">{sortState.direction === "asc" ? "↑" : "↓"}</span>
                              ) : null}
                            </button>
                          ) : (
                            <span className="th-sort-label">{COLUMN_LABELS[column] || column}</span>
                          )}
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {visibleRecords.map((record) => {
                    const contractValue = parseMoneyValue(record.contractTotals) || 0;
                    const paidValue = parseMoneyValue(record.totalPayments) || 0;
                    const balanceValue = parseMoneyValue(record.futurePayments) || 0;

                    return (
                      <tr key={record.id} className="is-clickable" onClick={() => openRecordModal(record)}>
                        <td>
                          <strong>{record.clientName || "Unnamed"}</strong>
                          <StatusBadges record={record} />
                        </td>
                        <td>{record.closedBy || "-"}</td>
                        <td>{record.companyName || "-"}</td>
                        <td>{record.serviceType || "-"}</td>
                        <td>{formatMoney(contractValue)}</td>
                        <td>{formatMoney(paidValue)}</td>
                        <td>{formatMoney(balanceValue)}</td>
                        <td>{record.afterResult ? "Yes" : "No"}</td>
                        <td>{record.writtenOff ? "Yes" : "No"}</td>
                        <td>{formatDate(record.dateWhenFullyPaid)}</td>
                        <td>{formatDate(record.createdAt)}</td>
                      </tr>
                    );
                  })}
                </tbody>
                <tfoot>
                  <tr>
                    <td colSpan={4}>
                      <strong>Totals</strong>
                    </td>
                    <td>{formatMoney(tableTotals.contractTotals)}</td>
                    <td>{formatMoney(tableTotals.totalPayments)}</td>
                    <td>{formatMoney(tableTotals.futurePayments)}</td>
                    <td colSpan={4}>{formatMoney(tableTotals.collection)} collection</td>
                  </tr>
                </tfoot>
              </table>
            </div>
          ) : null}
        </section>
      </div>

      <Modal
        open={modalState.open}
        title={
          modalState.mode === "create"
            ? "Create Client"
            : modalState.mode === "edit"
              ? "Edit Client"
              : activeRecord?.clientName || "Client Details"
        }
        onClose={requestCloseModal}
        footer={
          <div className="client-payments__modal-actions">
            <Button variant="secondary" size="sm" onClick={requestCloseModal}>
              Close
            </Button>
            {isViewMode && canManage ? (
              <Button size="sm" onClick={startEditRecord}>
                Edit
              </Button>
            ) : null}
            {!isViewMode ? (
              <Button size="sm" onClick={saveDraft}>
                Save
              </Button>
            ) : null}
          </div>
        }
      >
        {isViewMode && activeRecord ? <RecordDetails record={activeRecord} /> : null}
        {!isViewMode ? <RecordEditorForm draft={modalState.draft} onChange={updateDraftField} /> : null}
      </Modal>

      <Modal
        open={isDiscardConfirmOpen}
        title="Discard Changes?"
        onClose={cancelDiscardModalClose}
        footer={
          <div className="client-payments__modal-actions">
            <Button type="button" variant="secondary" size="sm" onClick={cancelDiscardModalClose}>
              Cancel
            </Button>
            <Button type="button" variant="danger" size="sm" onClick={discardDraftAndCloseModal}>
              Discard
            </Button>
          </div>
        }
      >
        <p>You have unsaved changes. Discard?</p>
      </Modal>

      {hasUnsavedChanges ? <Badge tone="warning">Unsaved changes pending sync</Badge> : null}
      {session?.user?.displayName ? <p className="react-user-footnote">Signed in as: {session.user.displayName}</p> : null}
    </div>
  );
}
