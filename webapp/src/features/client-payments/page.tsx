import { useMemo } from "react";

import {
  calculateTableTotals,
  formatDate,
  formatKpiMoney,
  formatMoney,
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
  Card,
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

export default function ClientPaymentsPage() {
  const {
    canManage,
    isLoading,
    loadError,
    records,
    visibleRecords,
    filters,
    sortState,
    overviewPeriod,
    overviewMetrics,
    closedByOptions,
    filtersCollapsed,
    isSaving,
    saveError,
    hasUnsavedChanges,
    lastSyncedAt,
    modalState,
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
    closeModal,
    updateDraftField,
    saveDraft,
    retrySave,
  } = useClientPayments();

  const visibleTotals = useMemo(() => calculateTableTotals(visibleRecords), [visibleRecords]);

  const sortableColumns = new Set<keyof ClientRecord>(
    TABLE_COLUMNS.filter((column) => column !== "afterResult" && column !== "writtenOff"),
  );

  const isViewMode = modalState.mode === "view";

  return (
    <div className="client-payments-page">
      <Card
        title="Client Payments"
        subtitle={`Last sync: ${lastSyncedAt || "-"}`}
        actions={
          <div className="client-payments__header-actions">
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
        }
      >
        {saveError ? <Toast kind="error" message={saveError} onClose={retrySave} /> : null}
        {!saveError && hasUnsavedChanges ? <Toast kind="info" message="Unsaved changes are syncing..." /> : null}
        {!hasUnsavedChanges && !saveError && isSaving ? <Toast kind="info" message="Saving changes..." /> : null}
      </Card>

      <Card title="Overview" subtitle="Financial totals by selected period">
        <SegmentedControl
          value={overviewPeriod}
          options={OVERVIEW_PERIOD_OPTIONS.map((option) => ({ key: option.key, label: option.label }))}
          onChange={(value) => setOverviewPeriod(value as OverviewPeriodKey)}
        />
        <div className="client-payments__kpi-grid">
          <div className="client-payments__kpi-card">
            <p>Sales</p>
            <strong>{formatKpiMoney(overviewMetrics.sales)}</strong>
          </div>
          <div className="client-payments__kpi-card">
            <p>Received</p>
            <strong>{formatKpiMoney(overviewMetrics.received)}</strong>
          </div>
          <div className="client-payments__kpi-card client-payments__kpi-card--debt">
            <p>Debt</p>
            <strong>{formatKpiMoney(overviewMetrics.debt)}</strong>
          </div>
        </div>
      </Card>

      <Card
        title="Filters"
        subtitle="Records always load from server. LocalStorage stores UI controls only."
        actions={
          <Button variant="ghost" size="sm" onClick={() => setFiltersCollapsed(!filtersCollapsed)}>
            {filtersCollapsed ? "Expand" : "Collapse"}
          </Button>
        }
      >
        {!filtersCollapsed ? (
          <div className="client-payments__filters-grid">
            <Input
              value={filters.search}
              onChange={(event) => updateFilter("search", event.target.value)}
              placeholder="Search by name, company, service"
            />

            <Select value={filters.closedBy} onChange={(event) => updateFilter("closedBy", event.target.value)}>
              <option value="">All Closed By</option>
              {closedByOptions.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </Select>

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

            <div className="client-payments__date-grid">
              <Input
                value={filters.createdAtRange.from}
                onChange={(event) => setDateRange("createdAtRange", "from", event.target.value)}
                placeholder="Created from (MM/DD/YYYY)"
              />
              <Input
                value={filters.createdAtRange.to}
                onChange={(event) => setDateRange("createdAtRange", "to", event.target.value)}
                placeholder="Created to (MM/DD/YYYY)"
              />
              <Input
                value={filters.paymentDateRange.from}
                onChange={(event) => setDateRange("paymentDateRange", "from", event.target.value)}
                placeholder="Payments from"
              />
              <Input
                value={filters.paymentDateRange.to}
                onChange={(event) => setDateRange("paymentDateRange", "to", event.target.value)}
                placeholder="Payments to"
              />
              <Input
                value={filters.writtenOffDateRange.from}
                onChange={(event) => setDateRange("writtenOffDateRange", "from", event.target.value)}
                placeholder="Written off from"
              />
              <Input
                value={filters.writtenOffDateRange.to}
                onChange={(event) => setDateRange("writtenOffDateRange", "to", event.target.value)}
                placeholder="Written off to"
              />
              <Input
                value={filters.fullyPaidDateRange.from}
                onChange={(event) => setDateRange("fullyPaidDateRange", "from", event.target.value)}
                placeholder="Fully paid from"
              />
              <Input
                value={filters.fullyPaidDateRange.to}
                onChange={(event) => setDateRange("fullyPaidDateRange", "to", event.target.value)}
                placeholder="Fully paid to"
              />
            </div>
          </div>
        ) : null}
      </Card>

      <Card title={`Records (${visibleRecords.length})`} subtitle={`Total in DB: ${records.length}`}>
        {isLoading ? <LoadingSkeleton rows={8} /> : null}
        {!isLoading && loadError ? (
          <ErrorState title="Failed to load records" description={loadError} actionLabel="Retry" onAction={() => void forceRefresh()} />
        ) : null}
        {!isLoading && !loadError && !visibleRecords.length ? (
          <EmptyState title="No records found" description="Adjust filters or add a new client." />
        ) : null}

        {!isLoading && !loadError && visibleRecords.length ? (
          <div className="cb-table-wrap">
            <table className="cb-table">
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
                            className={`client-payments__sort-btn ${isActive ? "is-active" : ""}`.trim()}
                            onClick={() => toggleSort(column)}
                          >
                            {COLUMN_LABELS[column] || column}
                            {isActive ? <span>{sortState.direction === "asc" ? "↑" : "↓"}</span> : null}
                          </button>
                        ) : (
                          <span>{COLUMN_LABELS[column] || column}</span>
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
                  <td>{formatMoney(visibleTotals.contractTotals)}</td>
                  <td>{formatMoney(visibleTotals.totalPayments)}</td>
                  <td>{formatMoney(visibleTotals.futurePayments)}</td>
                  <td colSpan={4}>{formatMoney(visibleTotals.collection)} collection</td>
                </tr>
              </tfoot>
            </table>
          </div>
        ) : null}
      </Card>

      <Modal
        open={modalState.open}
        title={
          modalState.mode === "create"
            ? "Create Client"
            : modalState.mode === "edit"
              ? "Edit Client"
              : activeRecord?.clientName || "Client Details"
        }
        onClose={closeModal}
        footer={
          <div className="client-payments__modal-actions">
            <Button variant="secondary" size="sm" onClick={closeModal}>
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

      {hasUnsavedChanges ? <Badge tone="warning">Unsaved changes pending sync</Badge> : null}
    </div>
  );
}
