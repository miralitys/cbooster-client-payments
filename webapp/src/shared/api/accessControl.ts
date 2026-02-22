import { apiRequest } from "@/shared/api/fetcher";
import type {
  AssistantReviewListPayload,
  AssistantReviewUpdatePayload,
  AssistantReviewUpdateResponse,
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

export async function deleteAccessUser(username: string): Promise<UpsertUserResponse> {
  return apiRequest<UpsertUserResponse>(`/api/auth/users/${encodeURIComponent(username)}`, {
    method: "DELETE",
  });
}

export async function listAssistantReviews(limit = 60, offset = 0): Promise<AssistantReviewListPayload> {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(1, Math.min(Math.floor(limit), 200)) : 60;
  const normalizedOffset = Number.isFinite(offset) ? Math.max(0, Math.floor(offset)) : 0;
  const query = new URLSearchParams({
    limit: String(normalizedLimit),
    offset: String(normalizedOffset),
  });

  return apiRequest<AssistantReviewListPayload>(`/api/assistant/reviews?${query.toString()}`);
}

export async function updateAssistantReview(
  reviewId: number,
  payload: AssistantReviewUpdatePayload,
): Promise<AssistantReviewUpdateResponse> {
  return apiRequest<AssistantReviewUpdateResponse>(`/api/assistant/reviews/${encodeURIComponent(String(reviewId))}`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}
