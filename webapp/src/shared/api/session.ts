import type { Session } from "@/shared/types/session";
import { apiRequest } from "@/shared/api/fetcher";

export async function getSession(): Promise<Session> {
  return apiRequest<Session>("/api/auth/session");
}
