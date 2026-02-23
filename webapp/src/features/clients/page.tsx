import { useCallback, useEffect, useMemo, useState } from "react";

import { StatusBadges } from "@/features/client-payments/components/StatusBadges";
import {
  formatDate,
  formatMoney,
  normalizeRecords,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { FIELD_DEFINITIONS } from "@/features/client-payments/domain/constants";
import { evaluateClientScore, type ClientScoreResult } from "@/features/client-score/domain/scoring";
import { getClientManagers, getRecords } from "@/shared/api";
import type { ClientManagerRow } from "@/shared/types/clientManagers";
import type { ClientRecord } from "@/shared/types/records";
import { Badge, Button, EmptyState, ErrorState, Input, LoadingSkeleton, Modal, PageHeader, PageShell, Panel, Table } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const TEXT_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });
const NO_MANAGER_LABEL = "No manager";

const SEARCH_FIELDS: Array<keyof ClientRecord> = [
  "clientName",
  "closedBy",
  "notes",
  "clientPhoneNumber",
  "clientEmailAddress",
  "companyName",
  "serviceType",
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
  const [clientManagersByClientName, setClientManagersByClientName] = useState<Map<string, string>>(new Map());

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

  const loadClientManagers = useCallback(async () => {
    try {
      const payload = await getClientManagers("none");
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setClientManagersByClientName(buildClientManagersLookup(items));
    } catch {
      setClientManagersByClientName(new Map());
    }
  }, []);

  useEffect(() => {
    void loadClients();
  }, [loadClients]);

  useEffect(() => {
    void loadClientManagers();
  }, [loadClientManagers]);

  const scoreByRecordId = useMemo(() => {
    const asOfDate = new Date();
    const scores = new Map<string, ClientScoreResult>();
    for (const record of records) {
      scores.set(record.id, evaluateClientScore(record, asOfDate));
    }
    return scores;
  }, [records]);

  const filteredRecords = useMemo(() => {
    const query = normalizeSearchTerm(search);
    const scopedRecords = query
      ? records.filter((record) => {
          const managerLabel = resolveClientManagerLabel(record, clientManagersByClientName);
          const searchable = [
            ...SEARCH_FIELDS.map((field) => (record[field] || "").toString()),
            managerLabel,
          ].join(" ");
          return normalizeSearchTerm(searchable).includes(query);
        })
      : records;

    return [...scopedRecords].sort((left, right) => {
      const nameCompare = TEXT_SORTER.compare((left.clientName || "").trim(), (right.clientName || "").trim());
      if (nameCompare !== 0) {
        return nameCompare;
      }

      return resolveCreatedAtTimestamp(right) - resolveCreatedAtTimestamp(left);
    });
  }, [clientManagersByClientName, records, search]);

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
        key: "clientName",
        label: "Client (Last Name, First Name)",
        align: "left",
        className: "clients-column-client",
        headerClassName: "clients-column-client",
        cell: (record) => (
          <div className="client-name-cell">
            <strong>{record.clientName || "Unnamed client"}</strong>
          </div>
        ),
      },
      {
        key: "score",
        label: "Score",
        align: "center",
        cell: (record) => {
          const scoreMeta = resolveScoreDisplay(scoreByRecordId.get(record.id));
          return <Badge tone={scoreMeta.tone}>{scoreMeta.value}</Badge>;
        },
      },
      {
        key: "closedBy",
        label: "Closed By",
        align: "left",
        cell: (record) => record.closedBy || "-",
      },
      {
        key: "clientManager",
        label: "Client Manager",
        align: "left",
        cell: (record) => resolveClientManagerLabel(record, clientManagersByClientName),
      },
      {
        key: "status",
        label: "Status",
        align: "left",
        cell: (record) => <StatusBadges record={record} />,
      },
      {
        key: "contractSigned",
        label: "Contract Signed",
        align: "center",
        cell: (record) => {
          const isSigned = resolveContractSigned(record);
          return <Badge tone={isSigned ? "success" : "warning"}>{isSigned ? "Yes" : "No"}</Badge>;
        },
      },
      {
        key: "contractTotals",
        label: "Total Contract",
        align: "right",
        cell: (record) => formatMoneyCell(record.contractTotals),
      },
      {
        key: "futurePayments",
        label: "Remaining",
        align: "right",
        cell: (record) => formatMoneyCell(record.futurePayments),
      },
      {
        key: "totalPayments",
        label: "Paid",
        align: "right",
        cell: (record) => formatMoneyCell(record.totalPayments),
      },
      {
        key: "notes",
        label: "Notes",
        align: "left",
        cell: (record) => (record.notes || "").trim() || "-",
      },
    ];
  }, [clientManagersByClientName, scoreByRecordId]);

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
              placeholder="Client, manager, phone, email, notes, company"
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

function buildClientManagersLookup(rows: ClientManagerRow[]): Map<string, string> {
  const map = new Map<string, string>();

  for (const row of rows) {
    const key = normalizeComparableClientName((row?.clientName || "").toString());
    if (!key) {
      continue;
    }

    const nextLabel = resolveManagersLabel(row);
    if (!nextLabel) {
      continue;
    }

    const current = map.get(key);
    if (!current || current === NO_MANAGER_LABEL) {
      map.set(key, nextLabel);
      continue;
    }

    if (nextLabel === NO_MANAGER_LABEL) {
      continue;
    }

    const merged = [...new Set([...splitClientManagerLabel(current), ...splitClientManagerLabel(nextLabel)])]
      .filter((name) => name !== NO_MANAGER_LABEL)
      .join(", ");
    map.set(key, merged || NO_MANAGER_LABEL);
  }

  return map;
}

function resolveManagersLabel(row: ClientManagerRow): string {
  const managers = Array.isArray(row?.managers)
    ? row.managers.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  if (managers.length) {
    return [...new Set(managers)].join(", ");
  }

  const managersLabel = (row?.managersLabel || "").toString().trim();
  if (!managersLabel || managersLabel === "-" || managersLabel.toLowerCase() === "unassigned") {
    return NO_MANAGER_LABEL;
  }

  return managersLabel;
}

function splitClientManagerLabel(rawLabel: string): string[] {
  const names = rawLabel
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (!names.length) {
    return [NO_MANAGER_LABEL];
  }

  return [...new Set(names)];
}

function resolveClientManagerLabel(record: ClientRecord, lookup: Map<string, string>): string {
  const key = normalizeComparableClientName(record.clientName);
  if (key) {
    const label = lookup.get(key);
    if (label) {
      return label;
    }
  }

  const fallbackCandidates = [
    getOptionalRecordText(record, "manager"),
    getOptionalRecordText(record, "assignedManager"),
    getOptionalRecordText(record, "clientManager"),
    getOptionalRecordText(record, "managerName"),
  ];

  for (const candidate of fallbackCandidates) {
    if (candidate) {
      return candidate;
    }
  }

  return NO_MANAGER_LABEL;
}

function normalizeComparableClientName(rawValue: string): string {
  return (rawValue || "")
    .toString()
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function getOptionalRecordText(record: ClientRecord, key: string): string {
  const rawValue = (record as unknown as Record<string, unknown>)[key];
  if (typeof rawValue !== "string") {
    return "";
  }
  return rawValue.trim();
}

function resolveScoreDisplay(score: ClientScoreResult | undefined): {
  value: string;
  tone: "neutral" | "success" | "info" | "warning" | "danger";
} {
  if (!score || score.displayScore === null) {
    return { value: "N/A", tone: "neutral" };
  }

  return {
    value: String(score.displayScore),
    tone: score.tone,
  };
}

function resolveContractSigned(record: ClientRecord): boolean {
  const rawContractSignedValues: unknown[] = [
    getOptionalUnknownRecordValue(record, "contractSigned"),
    getOptionalUnknownRecordValue(record, "isContractSigned"),
    getOptionalUnknownRecordValue(record, "signedContract"),
    getOptionalUnknownRecordValue(record, "contractIsSigned"),
    getOptionalUnknownRecordValue(record, "contract_sign"),
    getOptionalUnknownRecordValue(record, "contractStatus"),
  ];

  for (const rawValue of rawContractSignedValues) {
    const parsed = parseOptionalBoolean(rawValue);
    if (parsed !== null) {
      return parsed;
    }
  }

  const totalContract = parseMoneyValue(record.contractTotals);
  return totalContract !== null && totalContract > 0;
}

function getOptionalUnknownRecordValue(record: ClientRecord, key: string): unknown {
  return (record as unknown as Record<string, unknown>)[key];
}

function parseOptionalBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") {
    return value;
  }

  const normalized = (value || "").toString().trim().toLowerCase();
  if (!normalized) {
    return null;
  }

  if (["true", "yes", "1", "on", "signed", "active", "completed"].includes(normalized)) {
    return true;
  }

  if (["false", "no", "0", "off", "unsigned", "not signed", "pending"].includes(normalized)) {
    return false;
  }

  return null;
}
