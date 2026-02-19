import type { ClientRecord } from "@/shared/types/records";

export interface TelegramSubmittedBy {
  id?: string | number;
  username?: string;
  first_name?: string;
  last_name?: string;
}

export interface ModerationSubmission {
  id: string;
  status: string;
  submittedAt: string;
  reviewedAt?: string;
  reviewedBy?: string;
  reviewNote?: string;
  submittedBy?: TelegramSubmittedBy;
  client: Partial<ClientRecord>;
  attachmentsCount?: number;
}

export interface ModerationSubmissionListPayload {
  status: string;
  items: ModerationSubmission[];
}

export interface ModerationActionPayload {
  ok: boolean;
  item?: ModerationSubmission;
}

export interface ModerationSubmissionFile {
  id: string;
  fileName: string;
  mimeType: string;
  sizeBytes: number;
  canPreview: boolean;
  previewUrl: string;
  downloadUrl: string;
}

export interface ModerationSubmissionFilesPayload {
  ok: boolean;
  items: ModerationSubmissionFile[];
}
