import { apiRequest } from "@/shared/api/fetcher";
import type {
  CustomDashboardPayload,
  CustomDashboardTasksSourceKind,
  CustomDashboardTasksSourceUpdateResponse,
  CustomDashboardTasksSyncResponse,
  CustomDashboardUploadResponse,
  CustomDashboardUsersPayload,
  CustomDashboardUsersSavePayload,
} from "@/shared/types/customDashboard";

export async function getCustomDashboard(): Promise<CustomDashboardPayload> {
  return apiRequest<CustomDashboardPayload>("/api/custom-dashboard");
}

export async function getCustomDashboardUsers(): Promise<CustomDashboardUsersPayload> {
  return apiRequest<CustomDashboardUsersPayload>("/api/custom-dashboard/users");
}

export async function saveCustomDashboardUsers(payload: CustomDashboardUsersSavePayload): Promise<{ ok: boolean; updatedAt?: string }> {
  return apiRequest<{ ok: boolean; updatedAt?: string }>("/api/custom-dashboard/users", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function uploadCustomDashboardFile(
  type: "tasks" | "contacts" | "calls",
  file: File,
): Promise<CustomDashboardUploadResponse> {
  const formData = new FormData();
  formData.set("type", type);
  formData.set("file", file);

  return apiRequest<CustomDashboardUploadResponse>("/api/custom-dashboard/upload", {
    method: "POST",
    body: formData,
  });
}

export async function updateCustomDashboardTasksSource(
  source: CustomDashboardTasksSourceKind,
): Promise<CustomDashboardTasksSourceUpdateResponse> {
  return apiRequest<CustomDashboardTasksSourceUpdateResponse>("/api/custom-dashboard/tasks-source", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ source }),
  });
}

export async function syncCustomDashboardTasks(mode: "delta" | "full"): Promise<CustomDashboardTasksSyncResponse> {
  return apiRequest<CustomDashboardTasksSyncResponse>("/api/custom-dashboard/tasks-sync", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ mode }),
  });
}
