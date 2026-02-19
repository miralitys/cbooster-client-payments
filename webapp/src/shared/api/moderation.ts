import { apiRequest } from "@/shared/api/fetcher";
import type {
  ModerationActionPayload,
  ModerationSubmissionFilesPayload,
  ModerationSubmissionListPayload,
} from "@/shared/types/moderation";

export async function getModerationSubmissions(status = "pending", limit = 200): Promise<ModerationSubmissionListPayload> {
  const query = new URLSearchParams({
    status,
    limit: String(limit),
  });
  return apiRequest<ModerationSubmissionListPayload>(`/api/moderation/submissions?${query.toString()}`);
}

export async function getModerationSubmissionFiles(submissionId: string): Promise<ModerationSubmissionFilesPayload> {
  return apiRequest<ModerationSubmissionFilesPayload>(
    `/api/moderation/submissions/${encodeURIComponent(submissionId)}/files`,
  );
}

export async function approveModerationSubmission(submissionId: string): Promise<ModerationActionPayload> {
  return apiRequest<ModerationActionPayload>(
    `/api/moderation/submissions/${encodeURIComponent(submissionId)}/approve`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    },
  );
}

export async function rejectModerationSubmission(submissionId: string): Promise<ModerationActionPayload> {
  return apiRequest<ModerationActionPayload>(
    `/api/moderation/submissions/${encodeURIComponent(submissionId)}/reject`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({}),
    },
  );
}
