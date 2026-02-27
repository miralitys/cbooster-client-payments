import type { ClientRecord } from "@/shared/types/records";

export interface ClientHealthPayload {
  records: ClientRecord[];
  updatedAt?: string | null;
  limit?: number;
  safeMode?: boolean;
  source?: string;
  sampleMode?: string;
}
