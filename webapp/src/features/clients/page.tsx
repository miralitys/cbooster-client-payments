import { useCallback, useEffect, useMemo, useState } from "react";

import { StatusBadges } from "@/features/client-payments/components/StatusBadges";
import {
  formatDate,
  formatMoney,
  normalizeRecords,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { getRecords } from "@/shared/api";
import type { ClientRecord } from "@/shared/types/records";
import { Button, EmptyState, ErrorState, Input, LoadingSkeleton, Modal, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const TEXT_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });

const SEARCH_FIELDS: Array<keyof ClientRecord> = [
  "clientName",
  "clientPhoneNumber",
  "clientEmailAddress",
  "companyName",
  "serviceType",
  "closedBy",
];

const MONEY_FIELDS = new Set<keyof ClientRecord>([
  "contractTotals",
  "totalPayments",
  "futurePayments",
  "collection",
  "payment1",
  "payment2",
  "payment3",
  "payment4",
  "payment5",
  "payment6",
  "payment7",
]);

interface ClientDetailItem {
  key: string;
  label: string;
  value: string;
}

export default function ClientsPage() {
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [search, setSearch] = useState("");
  const [selectedRecordId, setSelectedRecordId] = useState("");

  const loadClients = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");

    try {
      const payload = await getRecords();
      const normalizedRecords = normalizeRecords(Array.isArray(payload.records) ? payload.records : []);
      setRecords(normalizedRecords);
    } catch (error) {
      setRecords([]);
      setLoadError(error instanceof Error ? error.message : "Failed to load clients from Client Payments.");
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadClients();
  }, [loadClients]);

  const filteredRecords = useMemo(() => {
    const query = normalizeSearchTerm(search);
    const scopedRecords = query
      ? records.filter((record) =>
          SEARCH_FIELDS.some((field) => normalizeSearchTerm(record[field]).includes(query)),
        )
      : records;

    return [...scopedRecords].sort((left, right) => {
      const nameCompare = TEXT_SORTER.compare((left.clientName || "").trim(), (right.clientName || "").trim());
      if (nameCompare !== 0) {
        return nameCompare;
      }

      return resolveCreatedAtTimestamp(right) - resolveCreatedAtTimestamp(left);
    });
  }, [records, search]);

  const selectedRecord = useMemo(
    () => records.find((record) => record.id === selectedRecordId) || null,
    [records, selectedRecordId],
  );

  const detailItems = useMemo<ClientDetailItem[]>(() => {
    if (!selectedRecord) {
      return [];
    }

    const details = FIELD_DEFINITIONS.map((field) => ({
      key: field.key,
      label: field.label,
      value: formatDetailValue(selectedRecord, field.key, field.type),
    }));

    return [
      {
        key: "createdAt",
        label: "Created At",
        value: formatDate(selectedRecord.createdAt),
      },
      ...details,
    ];
  }, [selectedRecord]);

  const totalContractAmount = useMemo(
    () => filteredRecords.reduce((sum, record) => sum + (parseMoneyValue(record.contractTotals) || 0), 0),
    [filteredRecords],
  );

  const totalBalanceAmount = useMemo(
    () => filteredRecords.reduce((sum, record) => sum + (parseMoneyValue(record.futurePayments) || 0), 0),
    [filteredRecords],
  );

  const statusMessage = useMemo(() => {
    if (isLoading) {
      return "Loading clients from Client Payments...";
    }

    if (loadError) {
      return loadError;
    }

    if (!records.length) {
      return "No clients found in Client Payments.";
    }

    if (search.trim() && !filteredRecords.length) {
      return "No clients match the current search.";
    }

    if (search.trim()) {
      return `Showing ${filteredRecords.length} of ${records.length} clients from Client Payments.`;
    }

    return `Loaded ${records.length} clients from Client Payments.`;
  }, [filteredRecords.length, isLoading, loadError, records.length, search]);

  const columns = useMemo<TableColumn<ClientRecord>[]>(() => {
    return [
      {
        key: "client",
        label: "Client",
        align: "left",
        className: "clients-column-client",
        headerClassName: "clients-column-client",
        cell: (record) => (
          <div className="client-name-cell">
            <strong>{record.clientName || "Unnamed client"}</strong>
            <div className="react-user-footnote">{record.serviceType || "No service type"}</div>
            <StatusBadges record={record} />
          </div>
        ),
      },
      {
        key: "contact",
        label: "Contact",
        align: "left",
        className: "clients-column-contact",
        headerClassName: "clients-column-contact",
        cell: (record) => (
          <div className="clients-contact-cell">
            <span>{record.clientPhoneNumber || "-"}</span>
            <span className="react-user-footnote">{record.clientEmailAddress || "-"}</span>
          </div>
        ),
      },
      {
        key: "company",
        label: "Company",
        align: "left",
        cell: (record) => record.companyName || "-",
      },
      {
        key: "closedBy",
        label: "Closed By",
        align: "left",
        cell: (record) => record.closedBy || "-",
      },
      {
        key: "contractTotals",
        label: "Contract",
        align: "right",
        cell: (record) => formatMoneyCell(record.contractTotals),
      },
      {
        key: "totalPayments",
        label: "Paid",
        align: "right",
        cell: (record) => formatMoneyCell(record.totalPayments),
      },
      {
        key: "futurePayments",
        label: "Balance",
        align: "right",
        cell: (record) => formatMoneyCell(record.futurePayments),
      },
      {
        key: "createdAt",
        label: "Created",
        align: "left",
        cell: (record) => formatDate(record.createdAt),
      },
      {
        key: "details",
        label: "",
        align: "right",
        cell: (record) => (
          <Button type="button" variant="ghost" size="sm" onClick={() => setSelectedRecordId(record.id)}>
            Open
          </Button>
        ),
      },
    ];
  }, []);

  return (
    <PageShell className="clients-react-page">
      <PageHeader
        title="Clients"
        subtitle="All client profiles sourced from Client Payments"
        actions={
          <Button type="button" variant="secondary" onClick={() => void loadClients()} isLoading={isLoading}>
            Refresh
          </Button>
        }
        meta={
          <>
            <p className={`dashboard-message ${loadError ? "error" : ""}`.trim()}>{statusMessage}</p>
            <div className="page-header__stats">
              <span className="stat-chip">
                <span className="stat-chip__label">Clients:</span>
                <span className="stat-chip__value">{filteredRecords.length}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Contract Total:</span>
                <span className="stat-chip__value">{formatMoney(totalContractAmount)}</span>
              </span>
              <span className="stat-chip">
                <span className="stat-chip__label">Balance:</span>
                <span className="stat-chip__value">{formatMoney(totalBalanceAmount)}</span>
              </span>
            </div>
          </>
        }
      />

      <Panel title="Clients Database">
        <div>
          <label className="search-label" htmlFor="clients-search-input">
            Search Clients
          </label>
          <div className="search-row">
            <Input
              id="clients-search-input"
              placeholder="Name, phone, email, company, service, or closer"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              autoComplete="off"
            />
            <Button type="button" variant="ghost" onClick={() => setSearch("")} disabled={!search.trim()}>
              Clear
            </Button>
          </div>
        </div>

        {isLoading ? <LoadingSkeleton rows={6} /> : null}
        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load clients"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadClients()}
          />
        ) : null}
        {!isLoading && !loadError ? (
          <Table
            columns={columns}
            rows={filteredRecords}
            rowKey={(record, index) => record.id || `client-${index}`}
            onRowClick={(record) => setSelectedRecordId(record.id)}
            emptyState={
              <EmptyState title="No clients found" description="Try changing search filters or refresh Client Payments data." />
            }
          />
        ) : null}
      </Panel>

      <Modal
        open={Boolean(selectedRecord)}
        title={selectedRecord?.clientName || "Client Details"}
        onClose={() => setSelectedRecordId("")}
        footer={
          <Button type="button" variant="secondary" onClick={() => setSelectedRecordId("")}>
            Close
          </Button>
        }
      >
        <div className="record-details-grid record-details-grid--requested">
          {detailItems.map((item) => (
            <div key={item.key} className="record-details-grid__item">
              <span className="record-details-grid__label">{item.label}</span>
              <strong className="record-details-grid__value">{item.value}</strong>
            </div>
          ))}
        </div>
      </Modal>
    </PageShell>
  );
}

function normalizeSearchTerm(value: unknown): string {
  return (value || "").toString().trim().toLowerCase();
}

function resolveCreatedAtTimestamp(record: ClientRecord): number {
  const parsed = Date.parse(record.createdAt || "");
  if (Number.isNaN(parsed)) {
    return 0;
  }
  return parsed;
}

function formatDetailValue(
  record: ClientRecord,
  key: keyof ClientRecord,
  fieldType: "text" | "textarea" | "checkbox" | "date",
): string {
  if (fieldType === "checkbox") {
    return isTruthyField(record[key]) ? "Yes" : "No";
  }

  if (fieldType === "date") {
    return formatDate(record[key]);
  }

  if (MONEY_FIELDS.has(key)) {
    return formatMoneyCell(record[key]);
  }

  const value = (record[key] || "").toString().trim();
  return value || "-";
}

function formatMoneyCell(rawValue: string): string {
  const amount = parseMoneyValue(rawValue);
  if (amount === null) {
    return "-";
  }
  return formatMoney(amount);
}

function isTruthyField(rawValue: string): boolean {
  const normalized = (rawValue || "").toString().trim().toLowerCase();
  return normalized === "true" || normalized === "yes" || normalized === "1" || normalized === "on";
}
