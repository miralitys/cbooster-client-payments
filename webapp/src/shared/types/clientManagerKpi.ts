export interface ClientManagerKpiClientRow {
  clientId: string;
  clientName: string;
  shouldPayThisMonth: boolean;
  paidThisMonth: boolean;
  paymentDatesThisMonth: string[];
  paymentAmountsThisMonth: number[];
  totalPaidThisMonth: number;
  reason: string;
}

export interface ClientManagerKpiRow {
  managerName: string;
  totalClients: number;
  fullyPaidClients: number;
  kpiBaseClients: number;
  clientsPaidThisMonth: number;
  kpiPercent: number;
  bonusUsd: number;
  isKpiReached: boolean;
  calculationLabel: string;
  calculationDescription: string;
  clients: ClientManagerKpiClientRow[];
}

export interface ClientManagerKpiPayload {
  month: string;
  rows: ClientManagerKpiRow[];
  updatedAt?: string | null;
}
