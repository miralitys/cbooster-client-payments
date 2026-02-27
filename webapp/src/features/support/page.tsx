import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent } from "react";

import {
  addSupportAttachments,
  addSupportComment,
  createSupportRequest,
  getSupportAttachmentDownloadUrl,
  getSupportReports,
  getSupportRequest,
  getSupportRequests,
  moveSupportRequest,
  updateSupportRequest,
} from "@/shared/api/support";
import { getAccessModel } from "@/shared/api/accessControl";
import { getSession } from "@/shared/api/session";
import { isDepartmentHeadSession, isOwnerOrAdminSession } from "@/shared/lib/access";
import { showToast } from "@/shared/lib/toast";
import type { SupportAttachment, SupportReport, SupportRequest } from "@/shared/types/support";
import {
  Button,
  Card,
  EmptyState,
  Input,
  Modal,
  PageHeader,
  PageShell,
  Panel,
  Select,
  Textarea,
} from "@/shared/ui";

const PRIORITY_OPTIONS = [
  { value: "low", label: "Not urgent" },
  { value: "urgent", label: "Urgent" },
  { value: "critical", label: "Very urgent" },
];

const STATUS_LABELS: Record<string, string> = {
  new: "New",
  review: "Review",
  in_progress: "In Progress",
  needs_revision: "Needs revision",
  done: "Done",
  rejected: "Rejected",
  withdrawn: "Withdrawn",
};

const STATUS_COLUMNS = [
  { key: "review", label: "Review" },
  { key: "in_progress", label: "In Progress" },
  { key: "needs_revision", label: "Needs Revision" },
  { key: "done", label: "Done" },
];

const DATE_TIME_FORMATTER = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

function formatDateTime(value: string | null): string {
  if (!value) {
    return "-";
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    return "-";
  }
  return DATE_TIME_FORMATTER.format(new Date(timestamp));
}

function resolvePriorityLabel(priority: string): string {
  const option = PRIORITY_OPTIONS.find((item) => item.value === priority);
  return option?.label || priority || "-";
}

function buildFormData(fields: Record<string, string | undefined>, attachments: File[]): FormData {
  const form = new FormData();
  for (const [key, value] of Object.entries(fields)) {
    if (value === undefined || value === null) {
      continue;
    }
    form.append(key, value);
  }
  for (const file of attachments) {
    form.append("attachments", file);
  }
  return form;
}

function isEditableStatus(status: string): boolean {
  return status === "new" || status === "review";
}

export default function SupportPage() {
  const [sessionLoaded, setSessionLoaded] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [isHead, setIsHead] = useState(false);
  const [requests, setRequests] = useState<SupportRequest[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [selectedRequest, setSelectedRequest] = useState<SupportRequest | null>(null);
  const [isOverlayOpen, setIsOverlayOpen] = useState(false);
  const [assignableUsers, setAssignableUsers] = useState<Array<{ username: string; displayName: string }>>([]);
  const [reports, setReports] = useState<SupportReport | null>(null);
  const [reportPeriod, setReportPeriod] = useState("week");
  const [reportPriority, setReportPriority] = useState("");
  const [reportAssignedTo, setReportAssignedTo] = useState("");
  const [reportAuthor, setReportAuthor] = useState("");

  const [formState, setFormState] = useState({
    title: "",
    description: "",
    priority: "low",
    urgencyReason: "",
    desiredDueDate: "",
  });
  const [formAttachments, setFormAttachments] = useState<File[]>([]);
  const [formError, setFormError] = useState("");
  const [formSubmitting, setFormSubmitting] = useState(false);

  const [editModalOpen, setEditModalOpen] = useState(false);
  const [editDraft, setEditDraft] = useState({
    title: "",
    description: "",
    priority: "low",
    urgencyReason: "",
    desiredDueDate: "",
  });
  const [editError, setEditError] = useState("");
  const [editSaving, setEditSaving] = useState(false);

  const [moveModalOpen, setMoveModalOpen] = useState(false);
  const [moveTargetStatus, setMoveTargetStatus] = useState("");
  const [moveReason, setMoveReason] = useState("");
  const [moveAttachments, setMoveAttachments] = useState<File[]>([]);
  const [moveSaving, setMoveSaving] = useState(false);

  const [commentText, setCommentText] = useState("");
  const [commentSaving, setCommentSaving] = useState(false);
  const [attachmentUploading, setAttachmentUploading] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const moveFileInputRef = useRef<HTMLInputElement | null>(null);
  const overlayFileInputRef = useRef<HTMLInputElement | null>(null);
  const supportEventRef = useRef<EventSource | null>(null);
  const closeMoveModal = useCallback(() => setMoveModalOpen(false), []);
  const closeEditModal = useCallback(() => setEditModalOpen(false), []);
  const closeOverlay = useCallback(() => setIsOverlayOpen(false), []);

  const loadRequests = useCallback(async () => {
    setIsRefreshing(true);
    try {
      const items = await getSupportRequests();
      setRequests(items);
    } catch {
      showToast({ type: "error", message: "Failed to load support requests." });
    } finally {
      setIsRefreshing(false);
    }
  }, []);

  const loadReports = useCallback(async () => {
    if (!isAdmin) {
      return;
    }
    try {
      const report = await getSupportReports({
        period: reportPeriod,
        priority: reportPriority || undefined,
        assigned_to: reportAssignedTo || undefined,
        created_by: reportAuthor || undefined,
      });
      setReports(report);
    } catch {
      showToast({ type: "error", message: "Failed to load reports." });
    }
  }, [isAdmin, reportAuthor, reportAssignedTo, reportPeriod, reportPriority]);

  useEffect(() => {
    let active = true;
    void getSession()
      .then((payload) => {
        if (!active) {
          return;
        }
        setIsAdmin(isOwnerOrAdminSession(payload));
        setIsHead(isDepartmentHeadSession(payload));
        setSessionLoaded(true);
      })
      .catch(() => {
        if (!active) {
          return;
        }
        setSessionLoaded(true);
      });
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    if (!sessionLoaded) {
      return;
    }
    setIsLoading(true);
    void loadRequests().finally(() => setIsLoading(false));
  }, [loadRequests, sessionLoaded]);

  useEffect(() => {
    if (!isAdmin) {
      return;
    }
    void getAccessModel()
      .then((payload) => {
        const users = Array.isArray(payload?.accessModel?.users) ? payload.accessModel.users : [];
        setAssignableUsers(
          users.map((user) => ({
            username: user.username,
            displayName: user.displayName || user.username,
          })),
        );
      })
      .catch(() => setAssignableUsers([]));
  }, [isAdmin]);

  useEffect(() => {
    if (!sessionLoaded) {
      return;
    }
    if (supportEventRef.current) {
      supportEventRef.current.close();
    }
    const source = new EventSource("/api/support/stream");
    supportEventRef.current = source;
    source.addEventListener("support", () => {
      void loadRequests();
    });
    return () => {
      source.close();
    };
  }, [loadRequests, sessionLoaded]);

  useEffect(() => {
    void loadReports();
  }, [loadReports]);

  const adminColumns = useMemo(() => {
    const grouped = new Map<string, SupportRequest[]>();
    for (const column of STATUS_COLUMNS) {
      grouped.set(column.key, []);
    }
    for (const request of requests) {
      if (grouped.has(request.status)) {
        grouped.get(request.status)?.push(request);
      }
    }
    return grouped;
  }, [requests]);

  const handleFormSubmit = useCallback(
    async (event: FormEvent) => {
      event.preventDefault();
      setFormError("");
      if (!formState.title.trim()) {
        setFormError("Title is required.");
        return;
      }
      if (!formState.description.trim()) {
        setFormError("Description is required.");
        return;
      }
      if (!formState.desiredDueDate) {
        setFormError("Desired due date is required.");
        return;
      }
      if ((formState.priority === "urgent" || formState.priority === "critical") && !formState.urgencyReason.trim()) {
        setFormError("Urgency reason is required.");
        return;
      }

      setFormSubmitting(true);
      try {
        const payload = buildFormData(
          {
            title: formState.title,
            description: formState.description,
            priority: formState.priority,
            urgency_reason: formState.urgencyReason,
            desired_due_date: formState.desiredDueDate,
          },
          formAttachments,
        );
        await createSupportRequest(payload);
        setFormState({
          title: "",
          description: "",
          priority: "low",
          urgencyReason: "",
          desiredDueDate: "",
        });
        setFormAttachments([]);
        if (fileInputRef.current) {
          fileInputRef.current.value = "";
        }
        await loadRequests();
        showToast({ type: "success", message: "Support request created." });
      } catch (error) {
        setFormError("Failed to create support request.");
      } finally {
        setFormSubmitting(false);
      }
    },
    [formAttachments, formState, loadRequests],
  );

  const openEditModal = useCallback((request: SupportRequest) => {
    setEditDraft({
      title: request.title,
      description: request.description,
      priority: request.priority,
      urgencyReason: request.urgencyReason || "",
      desiredDueDate: request.desiredDueDate ? request.desiredDueDate.slice(0, 16) : "",
    });
    setSelectedRequest(request);
    setEditModalOpen(true);
    setEditError("");
  }, []);

  const saveEdit = useCallback(async () => {
    if (!selectedRequest) {
      return;
    }
    setEditSaving(true);
    setEditError("");
    try {
      const updated = await updateSupportRequest(selectedRequest.id, {
        title: editDraft.title,
        description: editDraft.description,
        priority: editDraft.priority,
        urgency_reason: editDraft.urgencyReason,
        desired_due_date: editDraft.desiredDueDate,
      });
      setSelectedRequest(updated);
      setEditModalOpen(false);
      await loadRequests();
      showToast({ type: "success", message: "Request updated." });
    } catch {
      setEditError("Failed to update request.");
    } finally {
      setEditSaving(false);
    }
  }, [editDraft, loadRequests, selectedRequest]);

  const withdrawRequest = useCallback(
    async (request: SupportRequest) => {
      try {
        await moveSupportRequest(request.id, { status: "withdrawn" });
        await loadRequests();
        showToast({ type: "success", message: "Request withdrawn." });
      } catch {
        showToast({ type: "error", message: "Failed to withdraw request." });
      }
    },
    [loadRequests],
  );

  const openMoveModal = useCallback((request: SupportRequest, status: string) => {
    setSelectedRequest(request);
    setMoveTargetStatus(status);
    setMoveReason("");
    setMoveAttachments([]);
    setMoveModalOpen(true);
  }, []);

  const handleMoveSubmit = useCallback(async () => {
    if (!selectedRequest || !moveTargetStatus) {
      return;
    }
    if ((moveTargetStatus === "needs_revision" || moveTargetStatus === "rejected") && !moveReason.trim()) {
      showToast({ type: "error", message: "Reason is required." });
      return;
    }
    setMoveSaving(true);
    try {
      const updated = await moveSupportRequest(
        selectedRequest.id,
        {
          status: moveTargetStatus,
          reason: moveReason,
        },
        moveAttachments,
      );
      setSelectedRequest(updated);
      setMoveModalOpen(false);
      await loadRequests();
      showToast({ type: "success", message: "Status updated." });
    } catch {
      showToast({ type: "error", message: "Failed to update status." });
    } finally {
      setMoveSaving(false);
    }
  }, [loadRequests, moveAttachments, moveReason, moveTargetStatus, selectedRequest]);

  const openOverlay = useCallback(async (request: SupportRequest) => {
    try {
      const full = await getSupportRequest(request.id);
      setSelectedRequest(full);
      setIsOverlayOpen(true);
    } catch {
      showToast({ type: "error", message: "Failed to load request details." });
    }
  }, []);

  const saveComment = useCallback(async () => {
    if (!selectedRequest || !commentText.trim()) {
      return;
    }
    setCommentSaving(true);
    try {
      await addSupportComment(selectedRequest.id, commentText.trim());
      const refreshed = await getSupportRequest(selectedRequest.id);
      setSelectedRequest(refreshed);
      setCommentText("");
      showToast({ type: "success", message: "Comment added." });
    } catch {
      showToast({ type: "error", message: "Failed to add comment." });
    } finally {
      setCommentSaving(false);
    }
  }, [commentText, selectedRequest]);

  const uploadOverlayAttachments = useCallback(
    async (files: File[]) => {
      if (!selectedRequest || !files.length) {
        return;
      }
      setAttachmentUploading(true);
      try {
        await addSupportAttachments(selectedRequest.id, files);
        const refreshed = await getSupportRequest(selectedRequest.id);
        setSelectedRequest(refreshed);
        showToast({ type: "success", message: "Attachments uploaded." });
      } catch {
        showToast({ type: "error", message: "Failed to upload attachments." });
      } finally {
        setAttachmentUploading(false);
      }
    },
    [selectedRequest],
  );

  if (!sessionLoaded) {
    return (
      <PageShell className="support-page">
        <PageHeader title="Support" />
        <Panel>
          <EmptyState title="Loading session..." />
        </Panel>
      </PageShell>
    );
  }

  return (
    <PageShell className="support-page">
      <PageHeader
        title="Support"
        actions={(
          <Button variant="secondary" onClick={() => void loadRequests()} isLoading={isRefreshing}>
            Refresh
          </Button>
        )}
      />

      {isAdmin ? (
        <div className="support-admin-grid">
          <Panel title="Support Funnel">
            <div className="support-kanban">
              {STATUS_COLUMNS.map((column) => {
                const columnItems = adminColumns.get(column.key) || [];
                return (
                  <div
                    key={column.key}
                    className="support-kanban__column"
                    onDragOver={(event) => event.preventDefault()}
                    onDrop={(event) => {
                      event.preventDefault();
                      const requestId = event.dataTransfer.getData("text/plain");
                      const request = requests.find((item) => item.id === requestId);
                      if (!request) {
                        return;
                      }
                      if (column.key === "needs_revision") {
                        openMoveModal(request, "needs_revision");
                        return;
                      }
                      void moveSupportRequest(request.id, { status: column.key }).then(async (updated) => {
                        setSelectedRequest(updated);
                        await loadRequests();
                      }).catch(() => {
                        showToast({ type: "error", message: "Failed to move request." });
                      });
                    }}
                  >
                    <div className="support-kanban__column-header">
                      <h4>{column.label}</h4>
                      <span>{columnItems.length}</span>
                    </div>
                    <div className="support-kanban__cards">
                      {columnItems.map((item) => (
                        <div
                          key={item.id}
                          className={`support-card support-card--${item.priority}`}
                          draggable
                          onDragStart={(event) => event.dataTransfer.setData("text/plain", item.id)}
                          onClick={() => void openOverlay(item)}
                        >
                          <div className="support-card__title">{item.title}</div>
                          <div className="support-card__meta">
                            <span>{item.createdByDisplayName || item.createdBy}</span>
                            <span>{formatDateTime(item.createdAt)}</span>
                          </div>
                          <div className="support-card__badges">
                            <span className="support-card__priority">{resolvePriorityLabel(item.priority)}</span>
                            {item.isFromHead ? <span className="support-card__head-badge">From Head</span> : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          </Panel>

          <Panel title="Reports">
            <div className="support-reports">
              <div className="support-reports__filters">
                <Select value={reportPeriod} onChange={(event) => setReportPeriod(event.target.value)}>
                  <option value="day">Day</option>
                  <option value="week">Week</option>
                  <option value="month">Month</option>
                </Select>
                <Select value={reportPriority} onChange={(event) => setReportPriority(event.target.value)}>
                  <option value="">All priorities</option>
                  {PRIORITY_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </Select>
                <Select value={reportAssignedTo} onChange={(event) => setReportAssignedTo(event.target.value)}>
                  <option value="">All assignees</option>
                  {assignableUsers.map((user) => (
                    <option key={user.username} value={user.username}>
                      {user.displayName}
                    </option>
                  ))}
                </Select>
                <Select value={reportAuthor} onChange={(event) => setReportAuthor(event.target.value)}>
                  <option value="">All authors</option>
                  {assignableUsers.map((user) => (
                    <option key={user.username} value={user.username}>
                      {user.displayName}
                    </option>
                  ))}
                </Select>
                <Button variant="secondary" onClick={() => void loadReports()}>
                  Refresh
                </Button>
              </div>

              <div className="support-reports__summary">
                <div>
                  <span className="support-reports__label">Total Hours</span>
                  <span className="support-reports__value">{reports ? reports.totalHours.toFixed(2) : "-"}</span>
                </div>
                <div>
                  <span className="support-reports__label">Average Hours</span>
                  <span className="support-reports__value">{reports ? reports.averageHours.toFixed(2) : "-"}</span>
                </div>
              </div>

              <div className="support-reports__table">
                <div className="support-reports__row support-reports__row--head">
                  <span>ID</span>
                  <span>Title</span>
                  <span>Author</span>
                  <span>Priority</span>
                  <span>Hours</span>
                  <span>Done</span>
                </div>
                {(reports?.items || []).map((item) => (
                  <div key={item.id} className="support-reports__row">
                    <span>{item.id.slice(0, 8)}</span>
                    <span>{item.title}</span>
                    <span>{item.author}</span>
                    <span>{resolvePriorityLabel(item.priority)}</span>
                    <span>{item.hours.toFixed(2)}</span>
                    <span>{formatDateTime(item.doneAt)}</span>
                  </div>
                ))}
              </div>

              <details className="support-reports__recent">
                <summary>Show recently completed</summary>
                <div className="support-reports__table">
                  {(reports?.recent || []).map((item) => (
                    <div key={item.id} className="support-reports__row">
                      <span>{item.id.slice(0, 8)}</span>
                      <span>{item.title}</span>
                      <span>{item.author}</span>
                      <span>{resolvePriorityLabel(item.priority)}</span>
                      <span>{item.hours.toFixed(2)}</span>
                      <span>{formatDateTime(item.doneAt)}</span>
                    </div>
                  ))}
                </div>
              </details>
            </div>
          </Panel>
        </div>
      ) : (
        <div className="support-user-grid">
          <Panel title="Create request">
            <form className="support-form" onSubmit={handleFormSubmit}>
              <Input
                value={formState.title}
                onChange={(event) => setFormState((prev) => ({ ...prev, title: event.target.value }))}
                placeholder="Title"
              />
              <Textarea
                value={formState.description}
                onChange={(event) => setFormState((prev) => ({ ...prev, description: event.target.value }))}
                placeholder="Description"
              />
              <div className="support-form__row">
                <Select
                  value={formState.priority}
                  onChange={(event) => setFormState((prev) => ({ ...prev, priority: event.target.value }))}
                >
                  {PRIORITY_OPTIONS.filter((option) => (option.value === "critical" ? isHead : true)).map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </Select>
                <Input
                  type="datetime-local"
                  value={formState.desiredDueDate}
                  onChange={(event) => setFormState((prev) => ({ ...prev, desiredDueDate: event.target.value }))}
                />
              </div>
              {(formState.priority === "urgent" || formState.priority === "critical") ? (
                <Textarea
                  value={formState.urgencyReason}
                  onChange={(event) => setFormState((prev) => ({ ...prev, urgencyReason: event.target.value }))}
                  placeholder="Explain urgency"
                />
              ) : null}
              <div className="support-file-row">
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={() => fileInputRef.current?.click()}
                >
                  Attach files
                </Button>
                <span className="support-file-count">
                  {formAttachments.length ? `${formAttachments.length} file(s) selected` : "No files selected"}
                </span>
                <input
                  ref={fileInputRef}
                  type="file"
                  multiple
                  onChange={(event) => setFormAttachments(Array.from(event.target.files || []))}
                  hidden
                />
              </div>
              {formError ? <div className="support-form__error">{formError}</div> : null}
              <Button type="submit" isLoading={formSubmitting}>
                Submit
              </Button>
            </form>
          </Panel>

          <Panel title="My requests">
            {isLoading ? (
              <EmptyState title="Loading..." />
            ) : requests.length === 0 ? (
              <EmptyState title="No requests yet" />
            ) : (
              <div className="support-list">
                {requests.map((request) => (
                  <Card key={request.id} className={`support-list-card support-card--${request.priority}`}>
                    <div className="support-list-card__row">
                      <div>
                        <h4>{request.title}</h4>
                        <p>{request.description}</p>
                      </div>
                      <div className="support-list-card__meta">
                        <span>{STATUS_LABELS[request.status] || request.status}</span>
                        <span>{resolvePriorityLabel(request.priority)}</span>
                        <span>{formatDateTime(request.desiredDueDate)}</span>
                      </div>
                    </div>
                    <div className="support-list-card__actions">
                      <Button variant="secondary" onClick={() => void openOverlay(request)}>
                        View
                      </Button>
                      {isEditableStatus(request.status) ? (
                        <Button variant="secondary" onClick={() => openEditModal(request)}>
                          Edit
                        </Button>
                      ) : null}
                      {isEditableStatus(request.status) ? (
                        <Button variant="ghost" onClick={() => void withdrawRequest(request)}>
                          Withdraw
                        </Button>
                      ) : null}
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </Panel>
        </div>
      )}

      {moveModalOpen ? (
        <Modal
          open={moveModalOpen}
          title={moveTargetStatus === "rejected" ? "Reason for rejection" : "Reason for revision"}
          onClose={closeMoveModal}
        >
            <div className="support-modal">
              <Textarea
                value={moveReason}
                onChange={(event) => setMoveReason(event.target.value)}
                placeholder={
                  moveTargetStatus === "rejected"
                    ? "Reason for rejection"
                    : "Reason for revision"
                }
              />
              <div className="support-file-row">
              <Button
                type="button"
                variant="secondary"
                size="sm"
                onClick={() => moveFileInputRef.current?.click()}
              >
                Attach files
              </Button>
              <span className="support-file-count">
                {moveAttachments.length ? `${moveAttachments.length} file(s) selected` : "No files selected"}
              </span>
              <input
                ref={moveFileInputRef}
                type="file"
                multiple
                onChange={(event) => setMoveAttachments(Array.from(event.target.files || []))}
                hidden
              />
            </div>
            <Button onClick={handleMoveSubmit} isLoading={moveSaving}>
              Confirm
            </Button>
          </div>
        </Modal>
      ) : null}

      {editModalOpen ? (
        <Modal open={editModalOpen} title="Edit request" onClose={closeEditModal}>
          <div className="support-modal">
            <Input
              value={editDraft.title}
              onChange={(event) => setEditDraft((prev) => ({ ...prev, title: event.target.value }))}
              placeholder="Title"
            />
            <Textarea
              value={editDraft.description}
              onChange={(event) => setEditDraft((prev) => ({ ...prev, description: event.target.value }))}
              placeholder="Description"
            />
            <Select value={editDraft.priority} onChange={(event) => setEditDraft((prev) => ({ ...prev, priority: event.target.value }))}>
              {PRIORITY_OPTIONS.filter((option) => (option.value === "critical" ? isHead : true)).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </Select>
            {(editDraft.priority === "urgent" || editDraft.priority === "critical") ? (
              <Textarea
                value={editDraft.urgencyReason}
                onChange={(event) => setEditDraft((prev) => ({ ...prev, urgencyReason: event.target.value }))}
                placeholder="Explain urgency"
              />
            ) : null}
            <Input
              type="datetime-local"
              value={editDraft.desiredDueDate}
              onChange={(event) => setEditDraft((prev) => ({ ...prev, desiredDueDate: event.target.value }))}
            />
            {editError ? <div className="support-form__error">{editError}</div> : null}
            <Button onClick={saveEdit} isLoading={editSaving}>
              Save
            </Button>
          </div>
        </Modal>
      ) : null}

      {isOverlayOpen && selectedRequest ? (
        <Modal open={isOverlayOpen} title="Support Request" onClose={closeOverlay}>
          <div className="support-overlay">
            <div className="support-overlay__header">
              <h3>{selectedRequest.title}</h3>
              {selectedRequest.isFromHead ? <span className="support-card__head-badge">From Head</span> : null}
            </div>
            <p>{selectedRequest.description}</p>
            <div className="support-overlay__meta">
              <span>Status: {STATUS_LABELS[selectedRequest.status] || selectedRequest.status}</span>
              <span>Priority: {resolvePriorityLabel(selectedRequest.priority)}</span>
              <span>Created: {formatDateTime(selectedRequest.createdAt)}</span>
              <span>Due: {formatDateTime(selectedRequest.desiredDueDate)}</span>
            </div>

            {isAdmin ? (
              <div className="support-overlay__admin">
                <Select
                  value={selectedRequest.status}
                  onChange={(event) => {
                    const nextStatus = event.target.value;
                    if (nextStatus === "needs_revision" || nextStatus === "rejected") {
                      openMoveModal(selectedRequest, nextStatus);
                      return;
                    }
                    void moveSupportRequest(selectedRequest.id, { status: nextStatus }).then(async (updated) => {
                      setSelectedRequest(updated);
                      await loadRequests();
                    }).catch(() => {
                      showToast({ type: "error", message: "Failed to update status." });
                    });
                  }}
                >
                  {Object.entries(STATUS_LABELS).map(([value, label]) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </Select>
                <Select
                  value={selectedRequest.assignedTo}
                  onChange={(event) => {
                    const username = event.target.value;
                    const displayName = assignableUsers.find((user) => user.username === username)?.displayName || "";
                    void moveSupportRequest(selectedRequest.id, { status: selectedRequest.status, assigned_to: username, assigned_to_display_name: displayName }).then(async (updated) => {
                      setSelectedRequest(updated);
                      await loadRequests();
                    }).catch(() => {
                      showToast({ type: "error", message: "Failed to assign request." });
                    });
                  }}
                >
                  <option value="">Unassigned</option>
                  {assignableUsers.map((user) => (
                    <option key={user.username} value={user.username}>
                      {user.displayName}
                    </option>
                  ))}
                </Select>
              </div>
            ) : null}

            <div className="support-overlay__attachments">
              <h4>Attachments</h4>
              {(selectedRequest.attachments || []).length === 0 ? (
                <p>No attachments</p>
              ) : (
                <ul>
                  {(selectedRequest.attachments || []).map((attachment: SupportAttachment) => (
                    <li key={attachment.id}>
                      <a href={attachment.storageUrl || getSupportAttachmentDownloadUrl(attachment.id)} target="_blank" rel="noreferrer">
                        {attachment.fileName}
                      </a>
                    </li>
                  ))}
                </ul>
              )}
              <div className="support-file-row">
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={() => overlayFileInputRef.current?.click()}
                  disabled={attachmentUploading}
                >
                  Attach files
                </Button>
                <span className="support-file-count">
                  {attachmentUploading ? "Uploading..." : " "}
                </span>
                <input
                  ref={overlayFileInputRef}
                  type="file"
                  multiple
                  disabled={attachmentUploading}
                  onChange={(event) => void uploadOverlayAttachments(Array.from(event.target.files || []))}
                  hidden
                />
              </div>
            </div>

            <div className="support-overlay__comments">
              <h4>Comments</h4>
              <div className="support-overlay__history">
                {(selectedRequest.history || []).map((entry) => (
                  <div key={entry.id} className="support-overlay__history-item">
                    <div className="support-overlay__history-meta">
                      <span>{entry.actorDisplayName || entry.actorUsername}</span>
                      <span>{formatDateTime(entry.createdAt)}</span>
                    </div>
                    {entry.action === "comment" ? <p>{String(entry.payload?.comment || "")}</p> : null}
                    {entry.action === "status_change" ? (
                      <div>
                        <p>
                          Status: {String(entry.payload?.from || "")} → {String(entry.payload?.to || "")}
                        </p>
                        {entry.payload?.reason ? (
                          <p className="support-overlay__history-reason">
                            Reason: {String(entry.payload.reason)}
                          </p>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
              <Textarea value={commentText} onChange={(event) => setCommentText(event.target.value)} placeholder="Add a comment" />
              <Button onClick={() => void saveComment()} isLoading={commentSaving}>
                Add Comment
              </Button>
            </div>
          </div>
        </Modal>
      ) : null}
    </PageShell>
  );
}
