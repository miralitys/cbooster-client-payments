import { useCallback, useEffect, useMemo, useState } from "react";

import { PAYMENT_PAIRS } from "@/features/client-payments/domain/constants";
import {
  formatDate,
  formatMoney,
  normalizeDateForStorage,
  parseDateValue,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { RecordDetails } from "@/features/client-payments/components/RecordDetails";
import { getClients, getQuickBooksPayments, patchClients } from "@/shared/api";
import { showToast } from "@/shared/lib/toast";
import type { QuickBooksPaymentRow } from "@/shared/types/quickbooks";
import type { ClientRecord } from "@/shared/types/records";
import { Button, EmptyState, ErrorState, Input, LoadingSkeleton, Modal, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const MATCH_FROM_DATE = "2026-01-01";
const MATCH_FROM_TIMESTAMP = parseDateValue(MATCH_FROM_DATE) ?? 0;
const CLIENT_MATCH_CONFIRMATIONS_STORAGE_KEY = "client-match-confirmed-v1";
const NAME_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });

interface PaymentPair {
  date: string;
  amount: number | null;
}

interface DbPaymentPair extends PaymentPair {
  recordId: string;
  paymentAmountKey: keyof ClientRecord;
  paymentDateKey: keyof ClientRecord;
}

interface ClientMatchRow {
  id: string;
  clientName: string;
  quickBooksPayments: PaymentPair[];
  databasePayments: DbPaymentPair[];
}

interface ClientMatchSummary {
  quickBooksPaymentsCount: number;
  quickBooksClientsCount: number;
  databaseMatchedClientsCount: number;
  rangeFrom: string;
  rangeTo: string;
}

type EditableDbFieldType = "date" | "amount";

interface EditingCellState {
  rowId: string;
  slotIndex: number;
  fieldType: EditableDbFieldType;
  value: string;
  isSaving: boolean;
  error: string;
}

const EMPTY_SUMMARY: ClientMatchSummary = {
  quickBooksPaymentsCount: 0,
  quickBooksClientsCount: 0,
  databaseMatchedClientsCount: 0,
  rangeFrom: MATCH_FROM_DATE,
  rangeTo: MATCH_FROM_DATE,
};

export default function ClientMatchPage() {
  const [rows, setRows] = useState<ClientMatchRow[]>([]);
  const [clientRecords, setClientRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [summary, setSummary] = useState<ClientMatchSummary>(EMPTY_SUMMARY);
  const [clientsUpdatedAt, setClientsUpdatedAt] = useState<string | null>(null);
  const [editingCell, setEditingCell] = useState<EditingCellState | null>(null);
  const [selectedClientRecord, setSelectedClientRecord] = useState<ClientRecord | null>(null);
  const [confirmedClientIds, setConfirmedClientIds] = useState<string[]>(() => readConfirmedClientIds());

  const loadMatches = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const to = formatDateForApi(new Date());
      const [quickBooksPayload, clientsPayload] = await Promise.all([
        getQuickBooksPayments({ from: MATCH_FROM_DATE, to }),
        getClients(),
      ]);

      const quickBooksItems = Array.isArray(quickBooksPayload?.items) ? quickBooksPayload.items : [];
      const clientRecords = Array.isArray(clientsPayload?.records) ? clientsPayload.records : [];
      setClientRecords(clientRecords);
      setClientsUpdatedAt(typeof clientsPayload?.updatedAt === "string" ? clientsPayload.updatedAt : null);

      const quickBooksByClient = groupQuickBooksPaymentsByClientName(quickBooksItems);
      const databaseByClient = groupDatabasePaymentsByClientName(clientRecords);

      const nextRows: ClientMatchRow[] = [...quickBooksByClient.entries()]
        .map(([clientKey, quickBooksMatch]) => {
          const databasePayments = databaseByClient.get(clientKey) || [];
          return {
            id: clientKey,
            clientName: quickBooksMatch.clientName,
            quickBooksPayments: quickBooksMatch.payments,
            databasePayments,
          };
        })
        .sort((left, right) => NAME_SORTER.compare(left.clientName, right.clientName));

      setRows(nextRows);
      setSummary({
        quickBooksPaymentsCount: quickBooksItems.length,
        quickBooksClientsCount: nextRows.length,
        databaseMatchedClientsCount: nextRows.filter((row) => row.databasePayments.length > 0).length,
        rangeFrom: MATCH_FROM_DATE,
        rangeTo: to,
      });
    } catch (error) {
      setRows([]);
      setClientRecords([]);
      setSummary({
        ...EMPTY_SUMMARY,
        rangeTo: formatDateForApi(new Date()),
      });
      setLoadError(error instanceof Error ? error.message : "Failed to load Client Match.");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadMatches();
  }, [loadMatches]);

  const maxPaymentColumns = useMemo(() => {
    const maxColumns = rows.reduce((max, row) => {
      return Math.max(max, row.quickBooksPayments.length, row.databasePayments.length);
    }, 0);
    return Math.max(1, maxColumns);
  }, [rows]);
  const confirmedClientIdSet = useMemo(() => new Set(confirmedClientIds), [confirmedClientIds]);

  useEffect(() => {
    writeConfirmedClientIds(confirmedClientIds);
  }, [confirmedClientIds]);

  const confirmClientRow = useCallback((row: ClientMatchRow) => {
    if (confirmedClientIdSet.has(row.id)) {
      return;
    }

    const confirmed = globalThis.window?.confirm("Подтверждаешь?") ?? false;
    if (!confirmed) {
      return;
    }

    setConfirmedClientIds((previous) => {
      if (previous.includes(row.id)) {
        return previous;
      }
      return [...previous, row.id];
    });

    showToast({
      type: "success",
      message: `Client ${row.clientName} confirmed.`,
      dedupeKey: `client-match-confirm-${row.id}`,
      cooldownMs: 800,
    });
  }, [confirmedClientIdSet]);

  const openClientCardByRow = useCallback((row: ClientMatchRow) => {
    const matchedRecords = clientRecords.filter((record) => normalizeClientName(record.clientName) === row.id);
    if (!matchedRecords.length) {
      showToast({
        type: "error",
        message: "Client card not found in Client Payments DB.",
        dedupeKey: `client-match-open-card-not-found-${row.id}`,
        cooldownMs: 1000,
      });
      return;
    }

    const bestRecord = matchedRecords.reduce((latest, current) => {
      return parseCreatedAtTimestamp(current.createdAt) > parseCreatedAtTimestamp(latest.createdAt) ? current : latest;
    });
    setSelectedClientRecord(bestRecord);
  }, [clientRecords]);

  const beginEditCell = useCallback((rowId: string, slotIndex: number, fieldType: EditableDbFieldType, initialValue: string) => {
    setEditingCell({
      rowId,
      slotIndex,
      fieldType,
      value: initialValue,
      isSaving: false,
      error: "",
    });
  }, []);

  const cancelEditCell = useCallback(() => {
    setEditingCell(null);
  }, []);

  const updateEditingCellValue = useCallback((value: string) => {
    setEditingCell((previous) => {
      if (!previous || previous.isSaving) {
        return previous;
      }
      return {
        ...previous,
        value,
        error: "",
      };
    });
  }, []);

  const saveEditingCell = useCallback(async () => {
    if (!editingCell || editingCell.isSaving) {
      return;
    }

    const targetRow = rows.find((row) => row.id === editingCell.rowId);
    const targetPayment = targetRow?.databasePayments[editingCell.slotIndex];
    if (!targetRow || !targetPayment) {
      setEditingCell(null);
      return;
    }

    const trimmedValue = editingCell.value.trim();
    let nextStoredValue = "";
    let nextAmountValue: number | null = targetPayment.amount;

    if (editingCell.fieldType === "date") {
      const normalizedDate = normalizeDateForStorage(trimmedValue);
      if (normalizedDate === null) {
        setEditingCell((previous) => (previous ? { ...previous, error: "Invalid date format. Use MM/DD/YYYY." } : previous));
        return;
      }
      nextStoredValue = normalizedDate;
    } else if (!trimmedValue) {
      nextStoredValue = "";
      nextAmountValue = null;
    } else {
      const parsedAmount = parseMoneyValue(trimmedValue);
      if (parsedAmount === null) {
        setEditingCell((previous) => (previous ? { ...previous, error: "Invalid amount." } : previous));
        return;
      }
      nextStoredValue = parsedAmount.toFixed(2);
      nextAmountValue = parsedAmount;
    }

    const previousStoredValue =
      editingCell.fieldType === "date"
        ? String(targetPayment.date || "").trim()
        : targetPayment.amount === null || targetPayment.amount === undefined
          ? ""
          : targetPayment.amount.toFixed(2);

    if (nextStoredValue === previousStoredValue) {
      setEditingCell(null);
      return;
    }

    const patchRecord: Partial<ClientRecord> = {};
    const recordPatchKey = editingCell.fieldType === "date" ? targetPayment.paymentDateKey : targetPayment.paymentAmountKey;
    (patchRecord as Record<string, string>)[recordPatchKey] = nextStoredValue;

    setEditingCell((previous) => (previous ? { ...previous, isSaving: true, error: "" } : previous));

    try {
      const patchPayload = await patchClients(
        [
          {
            type: "upsert",
            id: targetPayment.recordId,
            record: patchRecord,
          },
        ],
        clientsUpdatedAt,
      );

      setClientsUpdatedAt(typeof patchPayload?.updatedAt === "string" ? patchPayload.updatedAt : clientsUpdatedAt);

      setRows((previousRows) =>
        previousRows.map((row) => {
          if (row.id !== editingCell.rowId) {
            return row;
          }

          return {
            ...row,
            databasePayments: row.databasePayments.map((payment, paymentIndex) => {
              if (paymentIndex !== editingCell.slotIndex) {
                return payment;
              }

              if (editingCell.fieldType === "date") {
                return {
                  ...payment,
                  date: nextStoredValue,
                };
              }

              return {
                ...payment,
                amount: nextAmountValue,
              };
            }),
          };
        }),
      );

      setEditingCell(null);
      showToast({
        type: "success",
        message: "Client payment updated in DB.",
        dedupeKey: "client-match-save-success",
        cooldownMs: 1200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to update Client DB payment.";
      setEditingCell((previous) => (previous ? { ...previous, isSaving: false, error: message } : previous));
      showToast({
        type: "error",
        message,
        dedupeKey: `client-match-save-error-${message}`,
        cooldownMs: 1500,
      });
    }
  }, [clientsUpdatedAt, editingCell, rows]);

  const isEditingCellTarget = useCallback((rowId: string, slotIndex: number, fieldType: EditableDbFieldType) => {
    return Boolean(
      editingCell && editingCell.rowId === rowId && editingCell.slotIndex === slotIndex && editingCell.fieldType === fieldType,
    );
  }, [editingCell]);

  const renderEditControls = useCallback(() => {
    if (!editingCell) {
      return null;
    }

    return (
      <>
        <div className="client-match-edit-actions">
          <button
            type="button"
            className="client-match-edit-action client-match-edit-action--confirm"
            onClick={() => void saveEditingCell()}
            disabled={editingCell.isSaving}
            aria-label="Confirm update"
          >
            ✓
          </button>
          <button
            type="button"
            className="client-match-edit-action client-match-edit-action--cancel"
            onClick={cancelEditCell}
            disabled={editingCell.isSaving}
            aria-label="Cancel update"
          >
            ✕
          </button>
        </div>
        {editingCell.error ? <p className="client-match-edit-error">{editingCell.error}</p> : null}
      </>
    );
  }, [cancelEditCell, editingCell, saveEditingCell]);

  const renderDbDateCell = useCallback((row: ClientMatchRow, slotIndex: number, isMatched: boolean) => {
    const dbPayment = row.databasePayments[slotIndex];
    if (!dbPayment) {
      return renderMatchDateCell("", false);
    }

    if (!isEditingCellTarget(row.id, slotIndex, "date")) {
      return (
        <button
          type="button"
          className={resolveCellButtonClassName(isMatched)}
          onClick={() => beginEditCell(row.id, slotIndex, "date", buildEditableDateValue(dbPayment.date))}
          aria-label={`Edit DB date for ${row.clientName}`}
        >
          {formatMatchDate(dbPayment.date)}
        </button>
      );
    }

    return (
      <div className="client-match-edit-cell">
        <Input
          value={editingCell?.value || ""}
          hasError={Boolean(editingCell?.error)}
          className="client-match-edit-input"
          placeholder="MM/DD/YYYY"
          onChange={(event) => updateEditingCellValue(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              void saveEditingCell();
            } else if (event.key === "Escape") {
              event.preventDefault();
              cancelEditCell();
            }
          }}
        />
        {renderEditControls()}
      </div>
    );
  }, [beginEditCell, cancelEditCell, editingCell?.error, editingCell?.value, isEditingCellTarget, renderEditControls, saveEditingCell, updateEditingCellValue]);

  const renderDbAmountCell = useCallback((row: ClientMatchRow, slotIndex: number, isMatched: boolean) => {
    const dbPayment = row.databasePayments[slotIndex];
    if (!dbPayment) {
      return renderMatchAmountCell(null, false);
    }

    if (!isEditingCellTarget(row.id, slotIndex, "amount")) {
      return (
        <button
          type="button"
          className={resolveCellButtonClassName(isMatched)}
          onClick={() => beginEditCell(row.id, slotIndex, "amount", buildEditableAmountValue(dbPayment.amount))}
          aria-label={`Edit DB amount for ${row.clientName}`}
        >
          {formatMatchAmount(dbPayment.amount)}
        </button>
      );
    }

    return (
      <div className="client-match-edit-cell">
        <Input
          value={editingCell?.value || ""}
          hasError={Boolean(editingCell?.error)}
          className="client-match-edit-input"
          placeholder="0.00"
          onChange={(event) => updateEditingCellValue(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              void saveEditingCell();
            } else if (event.key === "Escape") {
              event.preventDefault();
              cancelEditCell();
            }
          }}
        />
        {renderEditControls()}
      </div>
    );
  }, [beginEditCell, cancelEditCell, editingCell?.error, editingCell?.value, isEditingCellTarget, renderEditControls, saveEditingCell, updateEditingCellValue]);

  const tableColumns = useMemo<TableColumn<ClientMatchRow>[]>(() => {
    const columns: TableColumn<ClientMatchRow>[] = [
      {
        key: "clientName",
        label: "Client Name",
        align: "left",
        className: "client-match-column-client",
        headerClassName: "client-match-column-client",
        cell: (row) => {
          const isConfirmed = confirmedClientIdSet.has(row.id);
          return (
            <div className={`client-match-client-name ${isConfirmed ? "is-confirmed" : ""}`.trim()}>
              <input
                type="checkbox"
                checked={isConfirmed}
                onChange={(event) => {
                  if (event.target.checked) {
                    confirmClientRow(row);
                  }
                }}
                disabled={isConfirmed}
                aria-label={`Confirm ${row.clientName}`}
              />
              <button
                type="button"
                className="client-match-client-link"
                onClick={() => openClientCardByRow(row)}
                aria-label={`Open client card for ${row.clientName}`}
              >
                {row.clientName}
              </button>
            </div>
          );
        },
      },
    ];

    for (let index = 0; index < maxPaymentColumns; index += 1) {
      const slot = index + 1;
      columns.push(
        {
          key: `qbDate_${slot}`,
          label: `QB Date ${slot}`,
          align: "center",
          cell: (row) => {
            const qbPayment = row.quickBooksPayments[index];
            const dbPayment = row.databasePayments[index];
            const isMatched = isDateMatched(qbPayment?.date, dbPayment?.date);
            return renderMatchDateCell(qbPayment?.date || "", isMatched);
          },
        },
        {
          key: `qbAmount_${slot}`,
          label: `QB Amount ${slot}`,
          align: "right",
          cell: (row) => {
            const qbPayment = row.quickBooksPayments[index];
            const dbPayment = row.databasePayments[index];
            const isMatched = isAmountMatched(qbPayment?.amount, dbPayment?.amount);
            return renderMatchAmountCell(qbPayment?.amount, isMatched);
          },
        },
        {
          key: `dbDate_${slot}`,
          label: `DB Date ${slot}`,
          align: "center",
          cell: (row) => {
            const qbPayment = row.quickBooksPayments[index];
            const dbPayment = row.databasePayments[index];
            const isMatched = isDateMatched(qbPayment?.date, dbPayment?.date);
            return renderDbDateCell(row, index, isMatched);
          },
        },
        {
          key: `dbAmount_${slot}`,
          label: `DB Amount ${slot}`,
          align: "right",
          cell: (row) => {
            const qbPayment = row.quickBooksPayments[index];
            const dbPayment = row.databasePayments[index];
            const isMatched = isAmountMatched(qbPayment?.amount, dbPayment?.amount);
            return renderDbAmountCell(row, index, isMatched);
          },
        },
      );
    }

    return columns;
  }, [confirmClientRow, confirmedClientIdSet, maxPaymentColumns, openClientCardByRow, renderDbAmountCell, renderDbDateCell]);

  const headerMeta = (
    <div className="client-match-meta">
      <span>QuickBooks payments: {summary.quickBooksPaymentsCount}</span>
      <span>QuickBooks clients: {summary.quickBooksClientsCount}</span>
      <span>Matched in DB: {summary.databaseMatchedClientsCount}</span>
      <span>Confirmed: {confirmedClientIds.filter((id) => rows.some((row) => row.id === id)).length}</span>
      <span>
        Range: {summary.rangeFrom} -&gt; {summary.rangeTo}
      </span>
    </div>
  );

  return (
    <PageShell className="client-match-page">
      <PageHeader
        title="Client Match"
        subtitle="Temporary comparison of QuickBooks payments against Client Payment DB"
        meta={headerMeta}
        actions={(
          <Button type="button" onClick={() => void loadMatches()} disabled={isLoading}>
            {isLoading ? "Refreshing..." : "Refresh"}
          </Button>
        )}
      />

      <Panel title="QuickBooks Clients (from 2026-01-01)">
        {isLoading ? <LoadingSkeleton rows={8} /> : null}
        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load Client Match"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadMatches()}
          />
        ) : null}
        {!isLoading && !loadError && !rows.length ? (
          <EmptyState title="No QuickBooks payments found" description="No payments were returned for the selected range." />
        ) : null}
        {!isLoading && !loadError && rows.length ? (
          <Table
            columns={tableColumns}
            rows={rows}
            rowKey={(row) => row.id}
            className="client-match-table-wrap"
            tableClassName="client-match-table"
          />
        ) : null}
      </Panel>

      <Modal
        open={Boolean(selectedClientRecord)}
        title={selectedClientRecord?.clientName || "Client Details"}
        onClose={() => setSelectedClientRecord(null)}
        footer={(
          <div className="client-payments__modal-actions">
            <Button type="button" variant="secondary" size="sm" onClick={() => setSelectedClientRecord(null)}>
              Close
            </Button>
          </div>
        )}
      >
        {selectedClientRecord ? <RecordDetails record={selectedClientRecord} /> : null}
      </Modal>
    </PageShell>
  );
}

function normalizeClientName(rawValue: unknown): string {
  return String(rawValue || "")
    .toLowerCase()
    .replace(/\[wo\]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function groupQuickBooksPaymentsByClientName(items: QuickBooksPaymentRow[]): Map<string, { clientName: string; payments: PaymentPair[] }> {
  const grouped = new Map<string, { clientName: string; payments: PaymentPair[] }>();

  for (const item of items) {
    const paymentDate = String(item?.paymentDate || "").trim();
    const paymentDateTimestamp = parseDateValue(paymentDate);
    if (paymentDateTimestamp !== null && paymentDateTimestamp < MATCH_FROM_TIMESTAMP) {
      continue;
    }

    const clientName = String(item?.clientName || "").trim();
    const normalizedClientName = normalizeClientName(clientName);
    if (!normalizedClientName) {
      continue;
    }

    const existing = grouped.get(normalizedClientName);
    if (!existing) {
      grouped.set(normalizedClientName, {
        clientName,
        payments: [
          {
            date: paymentDate,
            amount: normalizeQuickBooksAmount(item?.paymentAmount),
          },
        ],
      });
      continue;
    }

    existing.payments.push({
      date: paymentDate,
      amount: normalizeQuickBooksAmount(item?.paymentAmount),
    });
  }

  for (const [, value] of grouped) {
    value.payments.sort(comparePaymentPairs);
  }

  return grouped;
}

function groupDatabasePaymentsByClientName(records: ClientRecord[]): Map<string, DbPaymentPair[]> {
  const grouped = new Map<string, DbPaymentPair[]>();

  for (const record of records) {
    const normalizedClientName = normalizeClientName(record?.clientName);
    if (!normalizedClientName) {
      continue;
    }

    const payments = grouped.get(normalizedClientName) || [];
    for (const [paymentKey, paymentDateKey] of PAYMENT_PAIRS) {
      const amount = parseMoneyValue(record[paymentKey]);
      const date = String(record[paymentDateKey] || "").trim();
      const paymentDateTimestamp = parseDateValue(date);
      if (paymentDateTimestamp !== null && paymentDateTimestamp < MATCH_FROM_TIMESTAMP) {
        continue;
      }

      if (amount === null && !date) {
        continue;
      }

      payments.push({
        date,
        amount,
        recordId: String(record.id || "").trim(),
        paymentAmountKey: paymentKey,
        paymentDateKey,
      });
    }

    grouped.set(normalizedClientName, payments);
  }

  for (const [, payments] of grouped) {
    payments.sort(comparePaymentPairs);
  }

  return grouped;
}

function comparePaymentPairs(left: PaymentPair, right: PaymentPair): number {
  const leftDate = parseDateValue(left.date);
  const rightDate = parseDateValue(right.date);

  if (leftDate !== null && rightDate !== null && leftDate !== rightDate) {
    return leftDate - rightDate;
  }

  if (leftDate !== null && rightDate === null) {
    return -1;
  }

  if (leftDate === null && rightDate !== null) {
    return 1;
  }

  const leftAmount = left.amount ?? Number.NEGATIVE_INFINITY;
  const rightAmount = right.amount ?? Number.NEGATIVE_INFINITY;
  if (leftAmount !== rightAmount) {
    return leftAmount - rightAmount;
  }

  return 0;
}

function normalizeQuickBooksAmount(value: unknown): number | null {
  const numberValue = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function formatMatchDate(rawValue: string): string {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "-";
  }
  return formatDate(value);
}

function formatMatchAmount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return formatMoney(value);
}

function renderMatchDateCell(rawValue: string, isMatched: boolean) {
  return (
    <span className={resolveMatchCellClassName(isMatched)}>
      {formatMatchDate(rawValue)}
    </span>
  );
}

function renderMatchAmountCell(value: number | null | undefined, isMatched: boolean) {
  return (
    <span className={resolveMatchCellClassName(isMatched)}>
      {formatMatchAmount(value)}
    </span>
  );
}

function resolveMatchCellClassName(isMatched: boolean): string {
  return isMatched ? "client-match-cell client-match-cell--matched" : "client-match-cell";
}

function resolveCellButtonClassName(isMatched: boolean): string {
  return isMatched
    ? "client-match-cell-button client-match-cell-button--matched"
    : "client-match-cell-button";
}

function isDateMatched(leftRaw: string | undefined, rightRaw: string | undefined): boolean {
  const left = parseDateValue(leftRaw || "");
  const right = parseDateValue(rightRaw || "");
  return left !== null && right !== null && left === right;
}

function isAmountMatched(leftRaw: number | null | undefined, rightRaw: number | null | undefined): boolean {
  if (leftRaw === null || leftRaw === undefined || !Number.isFinite(leftRaw)) {
    return false;
  }

  if (rightRaw === null || rightRaw === undefined || !Number.isFinite(rightRaw)) {
    return false;
  }

  return Math.abs(leftRaw - rightRaw) <= 0.005;
}

function buildEditableDateValue(rawDate: string): string {
  const normalizedDate = normalizeDateForStorage(rawDate);
  if (normalizedDate === null) {
    return String(rawDate || "").trim();
  }
  return normalizedDate;
}

function buildEditableAmountValue(rawAmount: number | null | undefined): string {
  if (rawAmount === null || rawAmount === undefined || !Number.isFinite(rawAmount)) {
    return "";
  }
  return rawAmount.toFixed(2);
}

function readConfirmedClientIds(): string[] {
  try {
    const rawValue = globalThis.window?.localStorage?.getItem(CLIENT_MATCH_CONFIRMATIONS_STORAGE_KEY) || "";
    if (!rawValue) {
      return [];
    }
    const parsedValue: unknown = JSON.parse(rawValue);
    if (!Array.isArray(parsedValue)) {
      return [];
    }

    return parsedValue
      .map((item) => String(item || "").trim())
      .filter((item, index, array) => Boolean(item) && array.indexOf(item) === index);
  } catch {
    return [];
  }
}

function writeConfirmedClientIds(clientIds: string[]): void {
  try {
    const normalizedClientIds = clientIds
      .map((item) => String(item || "").trim())
      .filter((item, index, array) => Boolean(item) && array.indexOf(item) === index);
    globalThis.window?.localStorage?.setItem(CLIENT_MATCH_CONFIRMATIONS_STORAGE_KEY, JSON.stringify(normalizedClientIds));
  } catch {
    // Ignore localStorage write errors for this temporary page.
  }
}

function formatDateForApi(value: Date): string {
  const year = String(value.getUTCFullYear());
  const month = String(value.getUTCMonth() + 1).padStart(2, "0");
  const day = String(value.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function parseCreatedAtTimestamp(rawValue: string): number {
  const timestamp = Date.parse(rawValue || "");
  return Number.isNaN(timestamp) ? 0 : timestamp;
}
