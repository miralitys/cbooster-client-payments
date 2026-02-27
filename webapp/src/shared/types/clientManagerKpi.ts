export interface ClientManagerKpiRow {
  managerName: string;
  totalClients: number;
  fullyPaidClients: number;
  kpiBaseClients: number;
  clientsPaidThisMonth: number;
  kpiPercent: number;
  bonusUsd: number;
  isKpiReached: boolean;
}

export interface ClientManagerKpiPayload {
  month: string;
  rows: ClientManagerKpiRow[];
  updatedAt?: string | null;
}
