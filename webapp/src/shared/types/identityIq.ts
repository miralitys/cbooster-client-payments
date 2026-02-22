export interface IdentityIqCreditScoreRequest {
  clientName?: string;
  email: string;
  password: string;
  ssnLast4: string;
}

export interface IdentityIqBureauScore {
  bureau: string;
  score: number;
}

export interface IdentityIqCreditScoreResult {
  provider: string;
  status: "ok" | "partial" | string;
  clientName?: string;
  emailMasked: string;
  score: number | null;
  bureauScores: IdentityIqBureauScore[];
  snippets: string[];
  dashboardUrl: string;
  fetchedAt: string;
  elapsedMs: number;
  note?: string;
}

export interface IdentityIqCreditScorePayload {
  ok: boolean;
  result: IdentityIqCreditScoreResult;
}
