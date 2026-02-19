export type ClientManagerStatus = "assigned" | "unassigned" | "error" | string;

export interface ClientManagerRow {
  clientName: string;
  managers: string[];
  managersLabel: string;
  matchedContacts: number;
  status: ClientManagerStatus;
  error?: string;
  updatedAt?: string | null;
}

export interface ClientManagersRefreshMeta {
  mode: "none" | "incremental" | "full" | string;
  performed: boolean;
  refreshedClientsCount: number;
  refreshedRowsWritten: number;
  deletedStaleRowsCount: number;
}

export interface ClientManagersPayload {
  ok: boolean;
  count: number;
  items: ClientManagerRow[];
  source?: string;
  updatedAt?: string | null;
  refresh?: ClientManagersRefreshMeta;
}
