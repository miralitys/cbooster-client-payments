export type SupportPriority = "low" | "normal" | "urgent" | "critical";
export type SupportStatus =
  | "new"
  | "review"
  | "in_progress"
  | "needs_revision"
  | "done"
  | "rejected"
  | "withdrawn";

export interface SupportAttachment {
  id: string;
  requestId: string;
  fileName: string;
  mimeType: string;
  sizeBytes: number;
  storageProvider: string;
  storageKey: string;
  storageUrl: string;
  uploadedBy: string;
  uploadedByDisplayName: string;
  uploadedAt: string | null;
}

export interface SupportHistoryItem {
  id: string;
  requestId: string;
  action: string;
  actorUsername: string;
  actorDisplayName: string;
  payload: Record<string, unknown>;
  createdAt: string | null;
}

export interface SupportRequest {
  id: string;
  title: string;
  description: string;
  priority: SupportPriority;
  urgencyReason: string;
  desiredDueDate: string | null;
  status: SupportStatus;
  createdBy: string;
  createdByDisplayName: string;
  createdByDepartmentId: string;
  createdByDepartmentName: string;
  createdByRoleId: string;
  createdByRoleName: string;
  isFromHead: boolean;
  assignedTo: string;
  assignedToDisplayName: string;
  createdAt: string | null;
  updatedAt: string | null;
  timeInProgressStart: string | null;
  timeDoneAt: string | null;
  lastNeedsRevisionReason: string;
  lastRejectedReason: string;
  lastNeedsRevisionAt: string | null;
  lastRejectedAt: string | null;
  withdrawnAt: string | null;
  attachments?: SupportAttachment[];
  history?: SupportHistoryItem[];
}

export interface SupportRequestsPayload {
  ok: boolean;
  items: SupportRequest[];
}

export interface SupportRequestPayload {
  ok: boolean;
  item: SupportRequest;
}

export interface SupportReportItem {
  id: string;
  title: string;
  priority: SupportPriority;
  author: string;
  authorUsername: string;
  assignedTo: string;
  assignedToUsername: string;
  hours: number;
  doneAt: string | null;
}

export interface SupportReport {
  totalHours: number;
  averageHours: number;
  items: SupportReportItem[];
  recent: SupportReportItem[];
}

export interface SupportReportPayload {
  ok: boolean;
  report: SupportReport;
  period: string;
}
