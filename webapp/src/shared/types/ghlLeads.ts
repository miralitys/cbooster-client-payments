export interface GhlLeadRow {
  leadId: string;
  contactId: string;
  contactName: string;
  opportunityName: string;
  pipelineId: string;
  pipelineName: string;
  stageId: string;
  stageName: string;
  status: string;
  monetaryValue: number;
  source: string;
  createdOn: string;
  ghlUpdatedAt: string;
  updatedAt?: string | null;
}

export interface GhlLeadsSummary {
  total: number;
  today: number;
  week: number;
  month: number;
  timezone?: string;
  generatedAt?: string;
}

export interface GhlLeadsRefreshMeta {
  mode: "none" | "incremental" | "full" | string;
  performed: boolean;
  pagesFetched: number;
  leadsFetched: number;
  skippedByCutoff: number;
  syncedLeadsCount: number;
  writtenRows: number;
  incrementalCutoff: string | null;
}

export interface GhlLeadsPipelineMeta {
  id: string;
  name: string;
}

export interface GhlLeadsPayload {
  ok: boolean;
  count: number;
  items: GhlLeadRow[];
  summary: GhlLeadsSummary;
  source?: string;
  pipeline?: GhlLeadsPipelineMeta;
  refresh?: GhlLeadsRefreshMeta;
}
