import { useCallback, useEffect, useMemo, useState } from "react";

import { RecordDetails } from "@/features/client-payments/components/RecordDetails";
import {
  formatDate,
  formatMoney,
  getRecordStatusFlags,
  normalizeRecords,
  parseDateValue,
  parseMoneyValue,
} from "@/features/client-payments/domain/calculations";
import { evaluateClientScore, type ClientScoreResult } from "@/features/client-score/domain/scoring";
import { patchRecords, getClientManagers, getRecords } from "@/shared/api";
import type { ClientManagerRow } from "@/shared/types/clientManagers";
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
  PageHeader,
  PageShell,
  Panel,
  Select,
  Table,
} from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

const TEXT_SORTER = new Intl.Collator("en-US", { sensitivity: "base", numeric: true });
const NO_MANAGER_LABEL = "No manager";
const SALES_FILTER_ALL = "__all_sales__";
const SALES_FILTER_UNASSIGNED = "__unassigned_sales__";
const MANAGER_FILTER_ALL = "__all_managers__";
const MANAGER_FILTER_UNASSIGNED = "__unassigned_managers__";

type ClientsStatusFilter = "all" | "new" | "active" | "overdue" | "written-off" | "fully-paid" | "after-result";
type ContractSignedFilter = "all" | "signed" | "unsigned";

const STATUS_FILTER_OPTIONS: Array<{ key: ClientsStatusFilter; label: string }> = [
  { key: "all", label: "All Statuses" },
  { key: "new", label: "New" },
  { key: "active", label: "Active" },
  { key: "overdue", label: "Overdue" },
  { key: "written-off", label: "Written Off" },
  { key: "fully-paid", label: "Fully Paid" },
  { key: "after-result", label: "After Result" },
];

interface ManagerFilterOptions {
  managers: string[];
  hasUnassigned: boolean;
}

interface SalesFilterOptions {
  sales: string[];
  hasUnassigned: boolean;
}

const SEARCH_FIELDS: Array<keyof ClientRecord> = [
  "clientName",
  "closedBy",
  "notes",
  "clientPhoneNumber",
  "clientEmailAddress",
  "companyName",
  "serviceType",
];

export default function ClientsPage() {
  const [records, setRecords] = useState<ClientRecord[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [search, setSearch] = useState("");
  const [isMoreFiltersOpen, setIsMoreFiltersOpen] = useState(false);
  const [hideWrittenOffByDefault, setHideWrittenOffByDefault] = useState(true);
  const [salesFilter, setSalesFilter] = useState(SALES_FILTER_ALL);
  const [clientManagerFilter, setClientManagerFilter] = useState(MANAGER_FILTER_ALL);
  const [statusFilter, setStatusFilter] = useState<ClientsStatusFilter>("all");
  const [contractSignedFilter, setContractSignedFilter] = useState<ContractSignedFilter>("all");
  const [contractDateFrom, setContractDateFrom] = useState("");
  const [contractDateTo, setContractDateTo] = useState("");
  const [selectedRecordId, setSelectedRecordId] = useState("");
  const [recordsUpdatedAt, setRecordsUpdatedAt] = useState<string | null>(null);
  const [isSavingClientFlags, setIsSavingClientFlags] = useState(false);
  const [saveClientFlagsError, setSaveClientFlagsError] = useState("");
  const [clientManagersByClientName, setClientManagersByClientName] = useState<Map<string, string>>(new Map());

  const loadClients = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");
    setSaveClientFlagsError("");

    try {
      const payload = await getRecords();
      const normalizedRecords = normalizeRecords(Array.isArray(payload.records) ? payload.records : []);
      setRecords(normalizedRecords);
      setRecordsUpdatedAt(normalizeRevisionTimestamp(payload.updatedAt));
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

  const salesFilterOptions = useMemo<SalesFilterOptions>(() => {
    let hasUnassigned = false;
    const uniqueByComparable = new Map<string, string>();

    for (const record of records) {
      const salesName = (record.closedBy || "").trim();
      if (!salesName) {
        hasUnassigned = true;
        continue;
      }

      const comparable = normalizeComparableClientName(salesName);
      if (!comparable || uniqueByComparable.has(comparable)) {
        continue;
      }
      uniqueByComparable.set(comparable, salesName);
    }

    return {
      sales: [...uniqueByComparable.values()].sort((left, right) => TEXT_SORTER.compare(left, right)),
      hasUnassigned,
    };
  }, [records]);

  const managerFilterOptions = useMemo<ManagerFilterOptions>(() => {
    let hasUnassigned = false;
    const uniqueByComparable = new Map<string, string>();

    for (const record of records) {
      const managerNames = splitClientManagerLabel(resolveClientManagerLabel(record, clientManagersByClientName));
      for (const managerName of managerNames) {
        if (managerName === NO_MANAGER_LABEL) {
          hasUnassigned = true;
          continue;
        }

        const comparable = normalizeComparableClientName(managerName);
        if (!comparable || uniqueByComparable.has(comparable)) {
          continue;
        }
        uniqueByComparable.set(comparable, managerName);
      }
    }

    return {
      managers: [...uniqueByComparable.values()].sort((left, right) => TEXT_SORTER.compare(left, right)),
      hasUnassigned,
    };
  }, [clientManagersByClientName, records]);

  useEffect(() => {
    if (salesFilter === SALES_FILTER_ALL) {
      return;
    }

    if (salesFilter === SALES_FILTER_UNASSIGNED) {
      if (salesFilterOptions.hasUnassigned) {
        return;
      }
      setSalesFilter(SALES_FILTER_ALL);
      return;
    }

    if (salesFilterOptions.sales.includes(salesFilter)) {
      return;
    }

    setSalesFilter(SALES_FILTER_ALL);
  }, [salesFilter, salesFilterOptions]);

  useEffect(() => {
    if (clientManagerFilter === MANAGER_FILTER_ALL) {
      return;
    }

    if (clientManagerFilter === MANAGER_FILTER_UNASSIGNED) {
      if (managerFilterOptions.hasUnassigned) {
        return;
      }
      setClientManagerFilter(MANAGER_FILTER_ALL);
      return;
    }

    if (managerFilterOptions.managers.includes(clientManagerFilter)) {
      return;
    }

    setClientManagerFilter(MANAGER_FILTER_ALL);
  }, [clientManagerFilter, managerFilterOptions]);

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
    const selectedSalesComparable = normalizeComparableClientName(salesFilter);
    const selectedManagerComparable = normalizeComparableClientName(clientManagerFilter);
    const contractDateFromTimestamp = parseDateValue(contractDateFrom);
    const contractDateToTimestamp = parseDateValue(contractDateTo);
    const hasContractDateFilter = Boolean(contractDateFrom.trim() || contractDateTo.trim());

    const scopedRecords = records.filter((record) => {
      const managerLabel = resolveClientManagerLabel(record, clientManagersByClientName);
      const managerNames = splitClientManagerLabel(managerLabel);
      const contractDateTimestamp = parseDateValue(record.payment1Date);
      const isContractSigned = resolveContractSigned(record);

      if (query) {
        const searchable = [...SEARCH_FIELDS.map((field) => (record[field] || "").toString()), managerLabel].join(" ");
        if (!normalizeSearchTerm(searchable).includes(query)) {
          return false;
        }
      }

      if (salesFilter === SALES_FILTER_UNASSIGNED) {
        if ((record.closedBy || "").trim()) {
          return false;
        }
      } else if (salesFilter !== SALES_FILTER_ALL) {
        if (normalizeComparableClientName(record.closedBy || "") !== selectedSalesComparable) {
          return false;
        }
      }

      if (clientManagerFilter === MANAGER_FILTER_UNASSIGNED) {
        if (!managerNames.includes(NO_MANAGER_LABEL)) {
          return false;
        }
      } else if (clientManagerFilter !== MANAGER_FILTER_ALL) {
        if (!managerNames.some((managerName) => normalizeComparableClientName(managerName) === selectedManagerComparable)) {
          return false;
        }
      }

      if (hideWrittenOffByDefault && getRecordStatusFlags(record).isWrittenOff) {
        return false;
      }

      if (!matchesStatusFilter(record, statusFilter, isContractSigned)) {
        return false;
      }

      if (contractSignedFilter === "signed" && !isContractSigned) {
        return false;
      }
      if (contractSignedFilter === "unsigned" && isContractSigned) {
        return false;
      }

      if (hasContractDateFilter) {
        if (contractDateTimestamp === null) {
          return false;
        }

        if (contractDateFromTimestamp !== null && contractDateTimestamp < contractDateFromTimestamp) {
          return false;
        }

        if (contractDateToTimestamp !== null && contractDateTimestamp > contractDateToTimestamp) {
          return false;
        }
      }

      return true;
    });

    return [...scopedRecords].sort((left, right) => {
      const nameCompare = TEXT_SORTER.compare((left.clientName || "").trim(), (right.clientName || "").trim());
      if (nameCompare !== 0) {
        return nameCompare;
      }

      return resolveCreatedAtTimestamp(right) - resolveCreatedAtTimestamp(left);
    });
  }, [
    clientManagerFilter,
    clientManagersByClientName,
    contractDateFrom,
    contractDateTo,
    contractSignedFilter,
    hideWrittenOffByDefault,
    records,
    salesFilter,
    search,
    statusFilter,
  ]);

  const selectedRecord = useMemo(
    () => records.find((record) => record.id === selectedRecordId) || null,
    [records, selectedRecordId],
  );

  const selectedContractSigned = selectedRecord ? resolveContractSigned(selectedRecord) : false;
  const selectedStartedInWork = selectedRecord ? resolveStartedInWork(selectedRecord) : false;

  useEffect(() => {
    setSaveClientFlagsError("");
  }, [selectedRecordId]);

  const updateSelectedRecordFlags = useCallback(
    async (changes: Pick<ClientRecord, "contractSigned" | "startedInWork">) => {
      if (!selectedRecord) {
        return;
      }

      setIsSavingClientFlags(true);
      setSaveClientFlagsError("");

      try {
        const payload = await patchRecords(
          [
            {
              type: "upsert",
              id: selectedRecord.id,
              record: changes,
            },
          ],
          recordsUpdatedAt,
        );

        setRecords((previous) =>
          previous.map((record) => (record.id === selectedRecord.id ? { ...record, ...changes } : record)),
        );
        setRecordsUpdatedAt(normalizeRevisionTimestamp(payload.updatedAt) || recordsUpdatedAt);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Failed to update client status.";
        setSaveClientFlagsError(message);
        await loadClients();
      } finally {
        setIsSavingClientFlags(false);
      }
    },
    [loadClients, recordsUpdatedAt, selectedRecord],
  );

  const totalContractAmount = useMemo(
    () => filteredRecords.reduce((sum, record) => sum + (parseMoneyValue(record.contractTotals) || 0), 0),
    [filteredRecords],
  );

  const totalBalanceAmount = useMemo(
    () => filteredRecords.reduce((sum, record) => sum + (parseMoneyValue(record.futurePayments) || 0), 0),
    [filteredRecords],
  );

  const hasActiveStructuredFilters = useMemo(() => {
    return (
      salesFilter !== SALES_FILTER_ALL ||
      clientManagerFilter !== MANAGER_FILTER_ALL ||
      statusFilter !== "all" ||
      contractSignedFilter !== "all" ||
      !hideWrittenOffByDefault ||
      Boolean(contractDateFrom.trim()) ||
      Boolean(contractDateTo.trim())
    );
  }, [clientManagerFilter, contractDateFrom, contractDateTo, contractSignedFilter, hideWrittenOffByDefault, salesFilter, statusFilter]);

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
        cell: (record) => {
          const statusBadge = resolvePrimaryStatusBadge(record);
          return <Badge tone={statusBadge.tone}>{statusBadge.label}</Badge>;
        },
      },
      {
        key: "startedInWork",
        label: "Запуск",
        align: "center",
        cell: (record) => {
          const startedInWork = resolveStartedInWork(record);
          return (
            <Badge tone={startedInWork ? "success" : "warning"}>
              {startedInWork ? "Запущен в работу" : "Не запущен в работу"}
            </Badge>
          );
        },
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
        key: "contractDate",
        label: "Contract Date",
        align: "center",
        cell: (record) => formatDate(record.payment1Date),
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
        actions={
          <Button type="button" variant="secondary" onClick={() => void loadClients()} isLoading={isLoading}>
            Refresh
          </Button>
        }
        meta={
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
            <Button
              type="button"
              variant={isMoreFiltersOpen ? "secondary" : "ghost"}
              onClick={() => setIsMoreFiltersOpen((previous) => !previous)}
              aria-expanded={isMoreFiltersOpen}
              aria-controls="clients-advanced-filters"
            >
              More
            </Button>
          </div>
        </div>

        {isMoreFiltersOpen ? (
          <div id="clients-advanced-filters">
            <div className="filters-grid-react">
              <div className="filter-field filter-field--full">
                <label className="cb-checkbox-row" htmlFor="clients-hide-writeoff-checkbox">
                  <input
                    id="clients-hide-writeoff-checkbox"
                    type="checkbox"
                    checked={hideWrittenOffByDefault}
                    onChange={(event) => setHideWrittenOffByDefault(event.target.checked)}
                  />
                  Show all clients except write-off
                </label>
              </div>

              <div className="filter-field">
                <label htmlFor="clients-contract-date-from-input">Contract Date From</label>
                <DateInput
                  id="clients-contract-date-from-input"
                  value={contractDateFrom}
                  onChange={setContractDateFrom}
                  placeholder="MM/DD/YYYY"
                />
              </div>

              <div className="filter-field">
                <label htmlFor="clients-contract-date-to-input">Contract Date To</label>
                <DateInput
                  id="clients-contract-date-to-input"
                  value={contractDateTo}
                  onChange={setContractDateTo}
                  placeholder="MM/DD/YYYY"
                />
              </div>

              <div className="filter-field">
                <label htmlFor="clients-sales-filter-select">Sales (Closed By)</label>
                <Select
                  id="clients-sales-filter-select"
                  value={salesFilter}
                  onChange={(event) => setSalesFilter(event.target.value)}
                >
                  <option value={SALES_FILTER_ALL}>All Sales</option>
                  {salesFilterOptions.hasUnassigned ? <option value={SALES_FILTER_UNASSIGNED}>Unassigned</option> : null}
                  {salesFilterOptions.sales.map((salesName) => (
                    <option key={salesName} value={salesName}>
                      {salesName}
                    </option>
                  ))}
                </Select>
              </div>

              <div className="filter-field">
                <label htmlFor="clients-manager-filter-select">Client Manager</label>
                <Select
                  id="clients-manager-filter-select"
                  value={clientManagerFilter}
                  onChange={(event) => setClientManagerFilter(event.target.value)}
                >
                  <option value={MANAGER_FILTER_ALL}>All Managers</option>
                  {managerFilterOptions.hasUnassigned ? <option value={MANAGER_FILTER_UNASSIGNED}>Unassigned</option> : null}
                  {managerFilterOptions.managers.map((managerName) => (
                    <option key={managerName} value={managerName}>
                      {managerName}
                    </option>
                  ))}
                </Select>
              </div>

              <div className="filter-field">
                <label htmlFor="clients-status-filter-select">Status</label>
                <Select
                  id="clients-status-filter-select"
                  value={statusFilter}
                  onChange={(event) => setStatusFilter(event.target.value as ClientsStatusFilter)}
                >
                  {STATUS_FILTER_OPTIONS.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label}
                    </option>
                  ))}
                </Select>
              </div>

              <div className="filter-field">
                <label htmlFor="clients-contract-signed-filter-select">Contract Signed</label>
                <Select
                  id="clients-contract-signed-filter-select"
                  value={contractSignedFilter}
                  onChange={(event) => setContractSignedFilter(event.target.value as ContractSignedFilter)}
                >
                  <option value="all">All</option>
                  <option value="signed">Signed</option>
                  <option value="unsigned">Not Signed</option>
                </Select>
              </div>
            </div>

            <div className="cb-page-header-toolbar">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => {
                  setSalesFilter(SALES_FILTER_ALL);
                  setClientManagerFilter(MANAGER_FILTER_ALL);
                  setStatusFilter("all");
                  setContractSignedFilter("all");
                  setHideWrittenOffByDefault(true);
                  setContractDateFrom("");
                  setContractDateTo("");
                }}
                disabled={!hasActiveStructuredFilters}
              >
                Reset Filters
              </Button>
            </div>
          </div>
        ) : null}

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
        {selectedRecord ? (
          <div className="clients-record-flags">
            <label className="cb-checkbox-row" htmlFor="clients-contract-signed-checkbox">
              <input
                id="clients-contract-signed-checkbox"
                type="checkbox"
                checked={selectedContractSigned}
                disabled={isSavingClientFlags}
                onChange={(event) =>
                  void updateSelectedRecordFlags({
                    contractSigned: toStatusCheckboxValue(event.target.checked),
                    startedInWork: toStatusCheckboxValue(selectedStartedInWork),
                  })
                }
              />
              Контракт подписан
            </label>

            <label className="cb-checkbox-row" htmlFor="clients-started-in-work-checkbox">
              <input
                id="clients-started-in-work-checkbox"
                type="checkbox"
                checked={selectedStartedInWork}
                disabled={isSavingClientFlags}
                onChange={(event) =>
                  void updateSelectedRecordFlags({
                    contractSigned: toStatusCheckboxValue(selectedContractSigned),
                    startedInWork: toStatusCheckboxValue(event.target.checked),
                  })
                }
              />
              {selectedStartedInWork ? "Запущен" : "Не запущен в работу"}
            </label>

            {saveClientFlagsError ? <p className="dashboard-message error">{saveClientFlagsError}</p> : null}
          </div>
        ) : null}
        {selectedRecord ? <RecordDetails record={selectedRecord} /> : null}
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

function formatMoneyCell(rawValue: string): string {
  const amount = parseMoneyValue(rawValue);
  if (amount === null) {
    return "-";
  }
  return formatMoney(amount);
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

function matchesStatusFilter(record: ClientRecord, statusFilter: ClientsStatusFilter, isContractSigned: boolean): boolean {
  if (statusFilter === "all") {
    return true;
  }

  if (statusFilter === "new") {
    return !isContractSigned;
  }

  if (!isContractSigned) {
    return false;
  }

  const status = getRecordStatusFlags(record);
  if (statusFilter === "written-off") {
    return status.isWrittenOff;
  }
  if (statusFilter === "fully-paid") {
    return status.isFullyPaid;
  }
  if (statusFilter === "overdue") {
    return status.isOverdue;
  }
  if (statusFilter === "after-result") {
    return status.isAfterResult;
  }

  return !status.isAfterResult && !status.isWrittenOff && !status.isFullyPaid && !status.isOverdue;
}

function resolvePrimaryStatusBadge(record: ClientRecord): {
  label: string;
  tone: "neutral" | "success" | "info" | "warning" | "danger";
} {
  const isContractSigned = resolveContractSigned(record);
  if (!isContractSigned) {
    return {
      label: "New",
      tone: "info",
    };
  }

  const status = getRecordStatusFlags(record);
  if (status.isWrittenOff) {
    return {
      label: "Written Off",
      tone: "danger",
    };
  }
  if (status.isFullyPaid) {
    return {
      label: "Fully Paid",
      tone: "success",
    };
  }
  if (status.isOverdue) {
    return {
      label: `Overdue ${status.overdueRange}`,
      tone: "warning",
    };
  }
  if (status.isAfterResult) {
    return {
      label: "After Result",
      tone: "info",
    };
  }

  return {
    label: "Active",
    tone: "neutral",
  };
}

function resolveContractSigned(record: ClientRecord): boolean {
  const directFieldValue = parseOptionalBoolean(record.contractSigned);
  if (directFieldValue !== null) {
    return directFieldValue;
  }

  const rawContractSignedValues: unknown[] = [
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

function resolveStartedInWork(record: ClientRecord): boolean {
  const directFieldValue = parseOptionalBoolean(record.startedInWork);
  if (directFieldValue !== null) {
    return directFieldValue;
  }

  const rawStartedInWorkValues: unknown[] = [
    getOptionalUnknownRecordValue(record, "inWork"),
    getOptionalUnknownRecordValue(record, "isInWork"),
    getOptionalUnknownRecordValue(record, "startedWork"),
    getOptionalUnknownRecordValue(record, "workStarted"),
    getOptionalUnknownRecordValue(record, "launchedInWork"),
  ];

  for (const rawValue of rawStartedInWorkValues) {
    const parsed = parseOptionalBoolean(rawValue);
    if (parsed !== null) {
      return parsed;
    }
  }

  return false;
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

function toStatusCheckboxValue(value: boolean): string {
  return value ? "Yes" : "No";
}

function normalizeRevisionTimestamp(rawValue: string | null | undefined): string | null {
  if (!rawValue) {
    return null;
  }

  const timestamp = Date.parse(rawValue);
  if (Number.isNaN(timestamp)) {
    return null;
  }

  return new Date(timestamp).toISOString();
}
