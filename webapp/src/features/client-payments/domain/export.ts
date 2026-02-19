import { formatDate, formatMoney, getRecordStatusFlags } from "@/features/client-payments/domain/calculations";
import { TABLE_COLUMNS } from "@/features/client-payments/domain/constants";
import type { ClientRecord } from "@/shared/types/records";

const COLUMN_LABELS: Record<string, string> = {
  clientName: "Client Name",
  closedBy: "Closed By",
  companyName: "Company",
  serviceType: "Service",
  contractTotals: "Contract",
  totalPayments: "Total Paid",
  futurePayments: "Balance",
  afterResult: "After Result",
  writtenOff: "Written Off",
  dateWhenFullyPaid: "Fully Paid Date",
  createdAt: "Created",
};

export function exportRecordsToXls(records: ClientRecord[]): void {
  const html = buildExportHtml(records, "Client Payments Export");
  const blob = new Blob([`\ufeff${html}`], {
    type: "application/vnd.ms-excel;charset=utf-8",
  });

  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `client-payments-${formatFileDate(new Date())}.xls`;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

export function exportRecordsToPdf(records: ClientRecord[]): void {
  const html = buildExportHtml(records, "Client Payments Export");
  const printWindow = window.open("", "_blank", "noopener,noreferrer");
  if (!printWindow) {
    return;
  }

  printWindow.document.write(`<!doctype html><html><head><meta charset="utf-8" /><title>Client Payments</title><style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #0f172a; }
    h1 { font-size: 18px; margin: 0 0 12px; }
    p { color: #475569; margin: 0 0 14px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { border: 1px solid #cbd5e1; padding: 6px 8px; font-size: 12px; text-align: left; }
    th { background: #f1f5f9; }
  </style></head><body>${html}</body></html>`);
  printWindow.document.close();
  printWindow.focus();
  printWindow.print();
}

function buildExportHtml(records: ClientRecord[], title: string): string {
  const headers = TABLE_COLUMNS.map((key) => COLUMN_LABELS[key] || key);
  const rows = records.map((record) => TABLE_COLUMNS.map((key) => formatCell(record, key)));

  return [
    `<h1>${escapeHtml(title)}</h1>`,
    `<p>Exported: ${escapeHtml(new Date().toLocaleString("en-US"))}</p>`,
    "<table>",
    `<thead><tr>${headers.map((header) => `<th>${escapeHtml(header)}</th>`).join("")}</tr></thead>`,
    `<tbody>${rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("")}</tbody>`,
    "</table>",
  ].join("");
}

function formatCell(record: ClientRecord, key: keyof ClientRecord): string {
  if (key === "afterResult" || key === "writtenOff") {
    return record[key] ? "Yes" : "";
  }

  if (key === "createdAt") {
    return formatDate(record[key]);
  }

  if (key === "dateWhenFullyPaid") {
    return formatDate(record[key]);
  }

  if (key === "contractTotals" || key === "totalPayments" || key === "futurePayments") {
    const amount = Number(record[key] || 0);
    return Number.isFinite(amount) ? formatMoney(amount) : record[key];
  }

  if (key === "clientName") {
    const status = getRecordStatusFlags(record);
    if (status.isWrittenOff) {
      return `${record.clientName} [Written Off]`;
    }

    if (status.isOverdue) {
      return `${record.clientName} [Overdue ${status.overdueRange}]`;
    }
  }

  return (record[key] || "").toString();
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function formatFileDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  const hours = String(value.getHours()).padStart(2, "0");
  const minutes = String(value.getMinutes()).padStart(2, "0");
  return `${year}${month}${day}-${hours}${minutes}`;
}
