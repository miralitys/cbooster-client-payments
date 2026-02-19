import { useEffect, useMemo } from "react";

import { showToast } from "@/shared/lib/toast";
import {
  formatDate,
  formatKpiMoney,
  formatMoney,
  getRecordStatusFlags,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import {
  PAYMENT_DATE_FIELDS,
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
  Button,
  DateInput,
  EmptyState,
  ErrorState,
  Input,
  Modal,
  PageHeader,
  PageShell,
  Panel,
  SegmentedControl,
  Select,
  Table,
} from "@/shared/ui";
import type { TableAlign, TableColumn } from "@/shared/ui/Table";

const COLUMN_LABELS: Record<string, string> = {
  clientName: "Client Name",
  closedBy: "Closed By",
  companyName: "Company",
  serviceType: "Service",
  contractTotals: "Contract",
  totalPayments: "Paid",
  payment1Date: "Payment 1 Date",
  payment2Date: "Payment 2 Date",
  payment3Date: "Payment 3 Date",
  payment4Date: "Payment 4 Date",
  payment5Date: "Payment 5 Date",
  payment6Date: "Payment 6 Date",
  payment7Date: "Payment 7 Date",
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
    saveSuccessNotice,
    hasUnsavedChanges,
    lastSyncedAt,
    modalState,
    isDiscardConfirmOpen,
    activeRecord,
    updateFilter,
    setDateRange,
    setOverviewPeriod,
    setFiltersCollapsed,
    tableDensity,
    setTableDensity,
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

  const sortableColumns = useMemo(
    () =>
      new Set<keyof ClientRecord>(
        TABLE_COLUMNS.filter((column) => column !== "afterResult" && column !== "writtenOff"),
      ),
    [],
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

  const totalsFooterLayout = useMemo(() => {
    const contractIndex = TABLE_COLUMNS.indexOf("contractTotals");
    const paidIndex = TABLE_COLUMNS.indexOf("totalPayments");
    const balanceIndex = TABLE_COLUMNS.indexOf("futurePayments");

    return {
      leadingSpan: Math.max(1, contractIndex),
      middleSpan: Math.max(0, balanceIndex - paidIndex - 1),
      trailingSpan: Math.max(1, TABLE_COLUMNS.length - balanceIndex - 1),
    };
  }, []);

  const tableColumns = useMemo<TableColumn<ClientRecord>[]>(() => {
    return TABLE_COLUMNS.map((column) => {
      const isSortable = sortableColumns.has(column);
      const isActive = sortState.key === column;

      return {
        key: column,
        label: isSortable ? (
          <button
            type="button"
            className={`th-sort-btn ${isActive ? "is-active" : ""}`.trim()}
            onClick={() => toggleSort(column)}
          >
            <span className="th-sort-label">{COLUMN_LABELS[column] || column}</span>
            {isActive ? <span className="th-sort-indicator">{sortState.direction === "asc" ? "↑" : "↓"}</span> : null}
          </button>
        ) : (
          <span className="th-sort-label">{COLUMN_LABELS[column] || column}</span>
        ),
        align: getColumnAlign(column),
        cell: (record) => {
          const contractValue = parseMoneyValue(record.contractTotals) || 0;
          const paidValue = parseMoneyValue(record.totalPayments) || 0;
          const balanceValue = parseMoneyValue(record.futurePayments) || 0;

          switch (column) {
            case "clientName":
              return (
                <>
                  <strong>{record.clientName || "Unnamed"}</strong>
                  <StatusBadges record={record} />
                </>
              );
            case "closedBy":
              return record.closedBy || "-";
            case "companyName":
              return record.companyName || "-";
            case "serviceType":
              return record.serviceType || "-";
            case "contractTotals":
              return formatMoney(contractValue);
            case "totalPayments":
              return formatMoney(paidValue);
            case "payment1Date":
            case "payment2Date":
            case "payment3Date":
            case "payment4Date":
            case "payment5Date":
            case "payment6Date":
            case "payment7Date":
              return formatDate((record[column] || "").toString());
            case "futurePayments":
              return formatMoney(balanceValue);
            case "afterResult":
              return record.afterResult ? "Yes" : "No";
            case "writtenOff":
              return record.writtenOff ? "Yes" : "No";
            case "dateWhenFullyPaid":
              return formatDate(record.dateWhenFullyPaid);
            case "createdAt":
              return formatDate(record.createdAt);
            default:
              return "";
          }
        },
      };
    });
  }, [sortState.direction, sortState.key, sortableColumns, toggleSort]);

  useEffect(() => {
    if (!saveError) {
      return;
    }

    showToast({
      type: "error",
      message: saveError,
      dedupeKey: `client-payments-save-error-${saveError}`,
      cooldownMs: 3000,
      action: saveRetryGiveUp
        ? {
            label: "Retry",
            onClick: retrySave,
          }
        : undefined,
      durationMs: saveRetryGiveUp ? 6000 : 4200,
    });
  }, [retrySave, saveError, saveRetryGiveUp]);

  useEffect(() => {
    if (!saveSuccessNotice || saveError || hasUnsavedChanges || isSaving) {
      return;
    }

    showToast({
      type: "success",
      message: saveSuccessNotice,
      dedupeKey: "client-payments-save-success",
      cooldownMs: 3200,
      durationMs: 2200,
    });
  }, [hasUnsavedChanges, isSaving, saveError, saveSuccessNotice]);

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
    <PageShell className="client-payments-react-page">
      <PageHeader
        title="Client Payments"
        subtitle="Revenue tracking and collections"
        actions={
          canManage ? (
            <Button size="sm" onClick={openCreateModal}>
              Add Client
            </Button>
          ) : null
        }
        meta={
          <div className="client-payments-page-header-meta">
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
            <div className="table-panel-sync-row">
              <p className="table-panel-updated-at">Last sync: {lastSyncedAt}</p>
              {isSaving ? (
                <span className="cb-badge cb-badge--info cb-badge--with-spinner">
                  <span className="cb-inline-spinner" aria-hidden="true" />
                  Saving...
                </span>
              ) : hasUnsavedChanges ? (
                <span className="cb-badge cb-badge--warning">Unsaved changes</span>
              ) : null}
              {!saveRetryGiveUp && saveRetryCount > 0 ? (
                <span className="react-user-footnote">
                  Retry {saveRetryCount}/{saveRetryMax}
                </span>
              ) : null}
              {saveRetryGiveUp ? (
                <Button type="button" variant="secondary" size="sm" onClick={retrySave}>
                  Retry Save
                </Button>
              ) : null}
            </div>
          </div>
        }
      />

      <div className={`grid dashboard-grid-react ${filtersCollapsed ? "is-filters-collapsed" : ""}`.trim()}>
        <Panel
          className="filters-panel"
          title="Filters"
          actions={
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
          }
        >
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
        </Panel>

        <Panel
          className="period-dashboard-shell-react"
          title="Overview"
          actions={
            <SegmentedControl
              value={overviewPeriod}
              options={OVERVIEW_PERIOD_OPTIONS.map((option) => ({ key: option.key, label: option.label }))}
              onChange={(value) => setOverviewPeriod(value as OverviewPeriodKey)}
            />
          }
        >
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
        </Panel>

        <Panel
          className="table-panel"
          title="Records"
          actions={
            <div className="table-panel__actions">
              <SegmentedControl
                value={tableDensity}
                options={[
                  { key: "compact", label: "Compact" },
                  { key: "comfortable", label: "Comfortable" },
                ]}
                onChange={(value) => setTableDensity(value as "compact" | "comfortable")}
              />
              <Button variant="secondary" size="sm" onClick={() => void forceRefresh()} isLoading={isLoading}>
                Refresh
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToXls(visibleRecords)}>
                Export XLS
              </Button>
              <Button variant="secondary" size="sm" onClick={() => exportRecordsToPdf(visibleRecords)}>
                Export PDF
              </Button>
            </div>
          }
        >
          {isLoading ? <TableLoadingSkeleton tableDensity={tableDensity} /> : null}
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
            <Table
              className="table-wrap"
              columns={tableColumns}
              rows={visibleRecords}
              rowKey={(record) => record.id}
              density={tableDensity}
              onRowActivate={(record) => openRecordModal(record)}
              footer={
                <tr>
                  <td colSpan={totalsFooterLayout.leadingSpan}>
                    <strong>Totals</strong>
                  </td>
                  <td className="cb-table__cell--align-right">{formatMoney(tableTotals.contractTotals)}</td>
                  <td className="cb-table__cell--align-right">{formatMoney(tableTotals.totalPayments)}</td>
                  {totalsFooterLayout.middleSpan > 0 ? <td colSpan={totalsFooterLayout.middleSpan} /> : null}
                  <td className="cb-table__cell--align-right">{formatMoney(tableTotals.futurePayments)}</td>
                  <td colSpan={totalsFooterLayout.trailingSpan}>{formatMoney(tableTotals.collection)} collection</td>
                </tr>
              }
            />
          ) : null}
        </Panel>
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

      {session?.user?.displayName ? <p className="react-user-footnote">Signed in as: {session.user.displayName}</p> : null}
    </PageShell>
  );
}

function getColumnAlign(column: keyof ClientRecord): TableAlign {
  if (
    column === "createdAt" ||
    column === "dateWhenFullyPaid" ||
    column === "afterResult" ||
    column === "writtenOff" ||
    PAYMENT_DATE_FIELDS.includes(column)
  ) {
    return "center";
  }

  if (column === "contractTotals" || column === "totalPayments" || column === "futurePayments") {
    return "right";
  }

  return "left";
}

function TableLoadingSkeleton({ tableDensity }: { tableDensity: "compact" | "comfortable" }) {
  const columnCount = TABLE_COLUMNS.length;

  return (
    <div className="table-wrap table-wrap--loading">
      <table className={`cb-table cb-table--${tableDensity}`.trim()}>
        <thead>
          <tr>
            {Array.from({ length: columnCount }).map((_, index) => (
              <th key={index}>
                <span className="cb-table-skeleton__head" />
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 8 }).map((_, rowIndex) => (
            <tr key={rowIndex}>
              {Array.from({ length: columnCount }).map((_, cellIndex) => (
                <td key={cellIndex}>
                  <span className="cb-table-skeleton__cell" />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
