import { apiRequest } from "@/shared/api/fetcher";
import type {
  AccessModelPayload,
  UpsertUserPayload,
  UpsertUserResponse,
  UsersPayload,
} from "@/shared/types/accessControl";

export async function getAccessModel(): Promise<AccessModelPayload> {
  return apiRequest<AccessModelPayload>("/api/auth/access-model");
}

export async function listAccessUsers(): Promise<UsersPayload> {
  return apiRequest<UsersPayload>("/api/auth/users");
}

export async function createAccessUser(payload: UpsertUserPayload): Promise<UpsertUserResponse> {
  return apiRequest<UpsertUserResponse>("/api/auth/users", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function updateAccessUser(username: string, payload: UpsertUserPayload): Promise<UpsertUserResponse> {
  return apiRequest<UpsertUserResponse>(`/api/auth/users/${encodeURIComponent(username)}`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
