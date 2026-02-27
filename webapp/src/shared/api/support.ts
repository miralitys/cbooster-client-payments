import { apiRequest } from "@/shared/api/fetcher";
import type {
  SupportRequestPayload,
  SupportRequestsPayload,
  SupportReportPayload,
  SupportReport,
  SupportRequest,
} from "@/shared/types/support";

export async function getSupportRequests(params: Record<string, string | string[] | undefined> = {}): Promise<SupportRequest[]> {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (!value) {
      continue;
    }
    if (Array.isArray(value)) {
      if (value.length) {
        search.set(key, value.join(","));
      }
      continue;
    }
    search.set(key, value);
  }
  const suffix = search.toString() ? `?${search.toString()}` : "";
  const payload = await apiRequest<SupportRequestsPayload>(`/api/support/requests${suffix}`);
  return Array.isArray(payload.items) ? payload.items : [];
}

export async function getSupportRequest(id: string): Promise<SupportRequest> {
  const payload = await apiRequest<SupportRequestPayload>(`/api/support/requests/${encodeURIComponent(id)}`);
  return payload.item;
}

export async function createSupportRequest(formData: FormData): Promise<SupportRequest> {
  const payload = await apiRequest<SupportRequestPayload>("/api/support/requests", {
    method: "POST",
    body: formData,
  });
  return payload.item;
}

export async function updateSupportRequest(id: string, payload: Record<string, unknown>): Promise<SupportRequest> {
  const response = await apiRequest<SupportRequestPayload>(`/api/support/requests/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  return response.item;
}

export async function moveSupportRequest(
  id: string,
  payload: Record<string, unknown>,
  attachments: File[] = [],
): Promise<SupportRequest> {
  const formData = new FormData();
  for (const [key, value] of Object.entries(payload)) {
    if (value === undefined || value === null) {
      continue;
    }
    formData.append(key, String(value));
  }
  for (const file of attachments) {
    formData.append("attachments", file);
  }

  const response = await apiRequest<SupportRequestPayload>(`/api/support/requests/${encodeURIComponent(id)}/actions/move-to`, {
    method: "POST",
    body: formData,
  });
  return response.item;
}

export async function addSupportAttachments(id: string, attachments: File[]): Promise<void> {
  const formData = new FormData();
  for (const file of attachments) {
    formData.append("attachments", file);
  }
  await apiRequest(`/api/support/requests/${encodeURIComponent(id)}/attachments`, {
    method: "POST",
    body: formData,
  });
}

export async function addSupportComment(id: string, comment: string): Promise<void> {
  await apiRequest(`/api/support/requests/${encodeURIComponent(id)}/comments`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ comment }),
  });
}

export async function getSupportReports(params: Record<string, string | string[] | undefined> = {}): Promise<SupportReport> {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (!value) {
      continue;
    }
    if (Array.isArray(value)) {
      if (value.length) {
        search.set(key, value.join(","));
      }
      continue;
    }
    search.set(key, value);
  }
  const suffix = search.toString() ? `?${search.toString()}` : "";
  const payload = await apiRequest<SupportReportPayload>(`/api/support/reports${suffix}`);
  return payload.report;
}

export function getSupportAttachmentDownloadUrl(id: string): string {
  return `/api/support/attachments/${encodeURIComponent(id)}`;
}
