"use strict";

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const {
  buildAttachmentStorageKey,
  buildAttachmentStorageUrl,
  resolveAttachmentStoragePath,
  sanitizeAttachmentStorageSegment,
} = require("../../../attachments-storage-utils");

const PRIORITY_LOW = "low";
const PRIORITY_NORMAL = "normal";
const PRIORITY_URGENT = "urgent";
const PRIORITY_CRITICAL = "critical";

const STATUS_NEW = "new";
const STATUS_REVIEW = "review";
const STATUS_IN_PROGRESS = "in_progress";
const STATUS_NEEDS_REVISION = "needs_revision";
const STATUS_DONE = "done";
const STATUS_REJECTED = "rejected";
const STATUS_WITHDRAWN = "withdrawn";

const DEFAULT_EDITABLE_STATUSES = new Set([STATUS_NEW, STATUS_REVIEW]);

function baseSanitize(value, maxLength = 4000) {
  return String(value ?? "").trim().slice(0, maxLength);
}

function normalizePriorityValue(rawValue, sanitize = baseSanitize) {
  const normalized = sanitize(rawValue, 80).toLowerCase().replace(/\s+/g, " ").trim();
  if (!normalized) {
    return PRIORITY_NORMAL;
  }
  if (normalized === "low" || normalized === "не срочно" || normalized === "несрочно") {
    return PRIORITY_LOW;
  }
  if (normalized === "normal" || normalized === "обычно") {
    return PRIORITY_NORMAL;
  }
  if (normalized === "urgent" || normalized === "срочно") {
    return PRIORITY_URGENT;
  }
  if (normalized === "critical" || normalized === "очень срочно" || normalized === "оченьсрочно") {
    return PRIORITY_CRITICAL;
  }
  return PRIORITY_NORMAL;
}

function normalizeStatusValue(rawValue, sanitize = baseSanitize) {
  const normalized = sanitize(rawValue, 80).toLowerCase().trim();
  if (
    normalized === STATUS_NEW ||
    normalized === STATUS_REVIEW ||
    normalized === STATUS_IN_PROGRESS ||
    normalized === STATUS_NEEDS_REVISION ||
    normalized === STATUS_DONE ||
    normalized === STATUS_REJECTED ||
    normalized === STATUS_WITHDRAWN
  ) {
    return normalized;
  }
  return STATUS_REVIEW;
}

function createSupportService(dependencies = {}) {
  const {
    supportRepo,
    notificationsService,
    sanitizeTextValue,
    listWebAuthUsers,
    getWebAuthUserByUsername,
    normalizeWebAuthDepartmentId,
    normalizeWebAuthRoleId,
    isWebAuthOwnerOrAdminProfile,
    webAuthRoleDepartmentHead,
    attachmentsStorageRoot,
    attachmentsStoragePublicBaseUrl,
    attachmentsUploadTmpDir,
    supportAttachmentsMaxBytes,
    supportAttachmentsMaxCount,
    supportAllowedAttachmentMimeTypes,
    supportSupabaseUrl,
    supportSupabaseServiceRoleKey,
    supportSupabaseBucket,
    supportSupabasePublicBaseUrl,
    logWarn,
  } = dependencies;

  if (!supportRepo) {
    throw new Error("createSupportService requires supportRepo.");
  }

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  const warn = typeof logWarn === "function" ? logWarn : () => {};
  const roleDepartmentHead = webAuthRoleDepartmentHead || "department_head";
  const maxAttachmentsCount = Number.isFinite(supportAttachmentsMaxCount) ? supportAttachmentsMaxCount : 12;
  const maxAttachmentsBytes =
    Number.isFinite(supportAttachmentsMaxBytes) && supportAttachmentsMaxBytes > 0
      ? supportAttachmentsMaxBytes
      : 25 * 1024 * 1024;
  const allowedAttachmentMimeTypes = Array.isArray(supportAllowedAttachmentMimeTypes)
    ? supportAllowedAttachmentMimeTypes
    : [
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "image/png",
        "image/jpeg",
      ];

  const supabaseConfigured =
    Boolean(supportSupabaseUrl && supportSupabaseServiceRoleKey && supportSupabaseBucket);

  function normalizeUsername(rawValue) {
    return sanitize(rawValue, 200).toLowerCase();
  }

  const normalizePriority = (rawValue) => normalizePriorityValue(rawValue, sanitize);
  const normalizeStatus = (rawValue) => normalizeStatusValue(rawValue, sanitize);

  function isDepartmentHeadProfile(profile) {
    if (!profile || typeof profile !== "object") {
      return false;
    }
    const departmentId = normalizeWebAuthDepartmentId?.(profile.departmentId) || "";
    const roleId = normalizeWebAuthRoleId?.(profile.roleId, departmentId) || profile.roleId || "";
    return roleId === roleDepartmentHead;
  }

  function isAdminProfile(profile) {
    if (typeof isWebAuthOwnerOrAdminProfile === "function") {
      return isWebAuthOwnerOrAdminProfile(profile);
    }
    return Boolean(profile?.isOwner);
  }

  function getProfileDepartmentId(profile) {
    return normalizeWebAuthDepartmentId?.(profile?.departmentId) || sanitize(profile?.departmentId, 120).toLowerCase();
  }

  function getAllowedUsernames(profile) {
    if (isAdminProfile(profile)) {
      return null;
    }
    const username = normalizeUsername(profile?.username || profile?.user?.username);
    if (!username) {
      return [];
    }
    if (isDepartmentHeadProfile(profile)) {
      const departmentId = getProfileDepartmentId(profile);
      const users = typeof listWebAuthUsers === "function" ? listWebAuthUsers() : [];
      return users
        .filter((user) => normalizeWebAuthDepartmentId?.(user.departmentId) === departmentId)
        .map((user) => normalizeUsername(user.username))
        .filter(Boolean);
    }
    return [username];
  }

  function canUseCriticalPriority(profile) {
    return isDepartmentHeadProfile(profile) || isAdminProfile(profile);
  }

  function normalizeDueDate(rawValue) {
    const value = sanitize(rawValue, 120);
    if (!value) {
      return null;
    }
    const timestamp = Date.parse(value);
    if (!Number.isFinite(timestamp)) {
      return null;
    }
    return new Date(timestamp).toISOString();
  }

  function mapSupportRequestRow(row) {
    if (!row || typeof row !== "object") {
      return null;
    }
    return {
      id: sanitize(row.id, 180),
      title: sanitize(row.title, 300),
      description: sanitize(row.description, 6000),
      priority: sanitize(row.priority, 80),
      urgencyReason: sanitize(row.urgency_reason, 2000),
      desiredDueDate: row.desired_due_date ? new Date(row.desired_due_date).toISOString() : null,
      status: sanitize(row.status, 80),
      createdBy: sanitize(row.created_by, 200),
      createdByDisplayName: sanitize(row.created_by_display_name, 220),
      createdByDepartmentId: sanitize(row.created_by_department_id, 120),
      createdByDepartmentName: sanitize(row.created_by_department_name, 220),
      createdByRoleId: sanitize(row.created_by_role_id, 120),
      createdByRoleName: sanitize(row.created_by_role_name, 180),
      isFromHead: Boolean(row.is_from_head),
      assignedTo: sanitize(row.assigned_to, 200),
      assignedToDisplayName: sanitize(row.assigned_to_display_name, 220),
      createdAt: row.created_at ? new Date(row.created_at).toISOString() : null,
      updatedAt: row.updated_at ? new Date(row.updated_at).toISOString() : null,
      timeInProgressStart: row.time_in_progress_start ? new Date(row.time_in_progress_start).toISOString() : null,
      timeDoneAt: row.time_done_at ? new Date(row.time_done_at).toISOString() : null,
      lastNeedsRevisionReason: sanitize(row.last_needs_revision_reason, 2000),
      lastRejectedReason: sanitize(row.last_rejected_reason, 2000),
      lastNeedsRevisionAt: row.last_needs_revision_at ? new Date(row.last_needs_revision_at).toISOString() : null,
      lastRejectedAt: row.last_rejected_at ? new Date(row.last_rejected_at).toISOString() : null,
      withdrawnAt: row.withdrawn_at ? new Date(row.withdrawn_at).toISOString() : null,
    };
  }

  function mapSupportAttachmentRow(row) {
    if (!row || typeof row !== "object") {
      return null;
    }
    return {
      id: sanitize(row.id, 180),
      requestId: sanitize(row.request_id, 180),
      fileName: sanitize(row.file_name, 400),
      mimeType: sanitize(row.mime_type, 120),
      sizeBytes: Number(row.size_bytes) || 0,
      storageProvider: sanitize(row.storage_provider, 40),
      storageKey: sanitize(row.storage_key, 400),
      storageUrl: sanitize(row.storage_url, 1200),
      uploadedBy: sanitize(row.uploaded_by, 200),
      uploadedByDisplayName: sanitize(row.uploaded_by_display_name, 220),
      uploadedAt: row.uploaded_at ? new Date(row.uploaded_at).toISOString() : null,
    };
  }

  function mapSupportHistoryRow(row) {
    if (!row || typeof row !== "object") {
      return null;
    }
    const payload = row.payload && typeof row.payload === "object" ? row.payload : {};
    return {
      id: sanitize(row.id, 180),
      requestId: sanitize(row.request_id, 180),
      action: sanitize(row.action, 80),
      actorUsername: sanitize(row.actor_username, 200),
      actorDisplayName: sanitize(row.actor_display_name, 220),
      payload,
      createdAt: row.created_at ? new Date(row.created_at).toISOString() : null,
    };
  }

  async function listRequestsForProfile(profile, filters = {}) {
    const allowedUsernames = getAllowedUsernames(profile);
    const rows = await supportRepo.listSupportRequests({
      allowedUsernames,
      statuses: Array.isArray(filters.statuses) ? filters.statuses : [],
      priorities: Array.isArray(filters.priorities) ? filters.priorities : [],
      assignedTo: filters.assignedTo,
      createdBy: filters.createdBy,
      limit: filters.limit,
      offset: filters.offset,
    });
    return rows.map(mapSupportRequestRow).filter(Boolean);
  }

  async function getRequestDetails(profile, requestId) {
    const row = await supportRepo.getSupportRequestById(requestId);
    if (!row) {
      return null;
    }
    const mapped = mapSupportRequestRow(row);
    if (!mapped) {
      return null;
    }
    if (!canProfileViewRequest(profile, mapped)) {
      return null;
    }
    const [attachmentsRows, historyRows] = await Promise.all([
      supportRepo.listSupportAttachments(mapped.id),
      supportRepo.listSupportHistory(mapped.id),
    ]);
    const attachments = attachmentsRows.map(mapSupportAttachmentRow).filter(Boolean);
    const history = historyRows.map(mapSupportHistoryRow).filter(Boolean);
    return { ...mapped, attachments, history };
  }

  function canProfileViewRequest(profile, request) {
    if (isAdminProfile(profile)) {
      return true;
    }
    const username = normalizeUsername(profile?.username || profile?.user?.username);
    if (!username) {
      return false;
    }
    if (request.createdBy && normalizeUsername(request.createdBy) === username) {
      return true;
    }
    if (isDepartmentHeadProfile(profile)) {
      const dept = getProfileDepartmentId(profile);
      return normalizeWebAuthDepartmentId?.(request.createdByDepartmentId) === dept;
    }
    return false;
  }

  function canEditRequest(profile, request) {
    if (!request) {
      return false;
    }
    if (isAdminProfile(profile)) {
      return true;
    }
    if (!DEFAULT_EDITABLE_STATUSES.has(request.status)) {
      return false;
    }
    const username = normalizeUsername(profile?.username || profile?.user?.username);
    if (!username) {
      return false;
    }
    if (request.createdBy && normalizeUsername(request.createdBy) === username) {
      return true;
    }
    if (isDepartmentHeadProfile(profile)) {
      const dept = getProfileDepartmentId(profile);
      return normalizeWebAuthDepartmentId?.(request.createdByDepartmentId) === dept;
    }
    return false;
  }

  function canWithdrawRequest(profile, request) {
    if (!request) {
      return false;
    }
    if (!DEFAULT_EDITABLE_STATUSES.has(request.status)) {
      return false;
    }
    const username = normalizeUsername(profile?.username || profile?.user?.username);
    if (!username) {
      return false;
    }
    return request.createdBy && normalizeUsername(request.createdBy) === username;
  }

  function canAdminMoveStatus(profile) {
    return isAdminProfile(profile);
  }

  async function createSupportHistoryEntry({
    requestId,
    action,
    actorUsername,
    actorDisplayName,
    payload,
  }) {
    const entry = {
      id: crypto.randomUUID(),
      requestId,
      action,
      actorUsername,
      actorDisplayName,
      payload: payload || {},
      createdAt: new Date().toISOString(),
    };
    await supportRepo.insertSupportHistory(entry);
  }

  function getUserDisplayName(username) {
    const profile = typeof getWebAuthUserByUsername === "function" ? getWebAuthUserByUsername(username) : null;
    return sanitize(profile?.displayName, 220) || username;
  }

  function getDepartmentHeadForUser(userProfile) {
    const departmentId = normalizeWebAuthDepartmentId?.(userProfile?.departmentId) || "";
    if (!departmentId || typeof listWebAuthUsers !== "function") {
      return null;
    }
    const users = listWebAuthUsers();
    return users.find((user) => {
      const deptId = normalizeWebAuthDepartmentId?.(user.departmentId) || "";
      const roleId = normalizeWebAuthRoleId?.(user.roleId, deptId) || user.roleId || "";
      return deptId === departmentId && roleId === roleDepartmentHead;
    }) || null;
  }

  async function notifyAdminsOnCreate(request) {
    if (!notificationsService || typeof notificationsService.createNotifications !== "function") {
      return;
    }
    if (typeof listWebAuthUsers !== "function") {
      return;
    }
    const users = listWebAuthUsers();
    const adminRecipients = users.filter((user) => isAdminProfile(user));
    if (!adminRecipients.length) {
      return;
    }
    const entries = adminRecipients.map((user) => ({
      id: crypto.randomUUID(),
      recipientUsername: user.username,
      type: "support",
      title: "New support request",
      message: request.title || "Support request created.",
      tone: "info",
      clientName: "",
      linkHref: "/app/support",
      linkLabel: "Open",
      payload: {
        requestId: request.id,
        priority: request.priority,
      },
      createdAt: new Date().toISOString(),
    }));

    try {
      await notificationsService.createNotifications(entries);
    } catch (error) {
      warn(
        `[support notifications] Failed to notify admins: ${sanitize(error?.message, 320) || "unknown error"}`,
      );
    }
  }

  async function notifyAuthorOnStatus(request, status, reason) {
    if (!notificationsService || typeof notificationsService.createNotifications !== "function") {
      return;
    }
    const recipientUsername = normalizeUsername(request.createdBy);
    if (!recipientUsername) {
      return;
    }
    const tone =
      status === STATUS_DONE ? "success" : status === STATUS_NEEDS_REVISION ? "warning" : status === STATUS_REJECTED ? "error" : "info";
    const title =
      status === STATUS_DONE
        ? "Support request completed"
        : status === STATUS_NEEDS_REVISION
          ? "Support request needs revision"
          : status === STATUS_REJECTED
            ? "Support request rejected"
            : "Support request updated";
    const message = reason ? `${title}: ${reason}` : title;
    const entries = [
      {
        id: crypto.randomUUID(),
        recipientUsername,
        type: "support",
        title,
        message,
        tone,
        clientName: "",
        linkHref: "/app/support",
        linkLabel: "Open",
        payload: {
          requestId: request.id,
          status,
        },
        createdAt: new Date().toISOString(),
      },
    ];

    const authorProfile = typeof getWebAuthUserByUsername === "function" ? getWebAuthUserByUsername(recipientUsername) : null;
    const headProfile = getDepartmentHeadForUser(authorProfile || {});
    if (headProfile && normalizeUsername(headProfile.username) !== recipientUsername) {
      entries.push({
        id: crypto.randomUUID(),
        recipientUsername: headProfile.username,
        type: "support",
        title,
        message,
        tone,
        clientName: "",
        linkHref: "/app/support",
        linkLabel: "Open",
        payload: {
          requestId: request.id,
          status,
        },
        createdAt: new Date().toISOString(),
      });
    }

    try {
      await notificationsService.createNotifications(entries);
    } catch (error) {
      warn(
        `[support notifications] Failed to notify author: ${sanitize(error?.message, 320) || "unknown error"}`,
      );
    }
  }

  function resolveStorageKey(requestId, attachmentId, originalName) {
    const safeRequestId = sanitizeAttachmentStorageSegment(requestId, "request");
    const safeName = sanitizeAttachmentStorageSegment(originalName, "attachment");
    return buildAttachmentStorageKey({
      submissionId: `support-${safeRequestId}`,
      fileId: attachmentId,
      fileName: safeName,
    });
  }

  async function storeAttachmentFile(requestId, file, uploaderProfile) {
    if (!file) {
      return null;
    }
    const attachmentId = crypto.randomUUID();
    const fileName = sanitize(file.originalname, 300) || `attachment-${attachmentId}`;
    const mimeType = sanitize(file.mimetype, 120) || "application/octet-stream";
    const sizeBytes = Number(file.size) || 0;
    const storageKey = resolveStorageKey(requestId, attachmentId, fileName);

    let storageProvider = "bytea";
    let storageUrl = "";
    let contentBuffer = null;

    const tempPath = file.path || "";

    try {
      if (supabaseConfigured) {
        const buffer = await fs.promises.readFile(tempPath);
        const targetUrl = `${supportSupabaseUrl.replace(/\/+$/, "")}/storage/v1/object/${supportSupabaseBucket}/${storageKey}`;
        const response = await fetch(targetUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${supportSupabaseServiceRoleKey}`,
            apikey: supportSupabaseServiceRoleKey,
            "Content-Type": mimeType,
            "x-upsert": "true",
          },
          body: buffer,
        });
        if (!response.ok) {
          throw new Error(`Supabase upload failed: ${response.status}`);
        }
        const baseUrl =
          supportSupabasePublicBaseUrl ||
          `${supportSupabaseUrl.replace(/\/+$/, "")}/storage/v1/object/public`;
        storageUrl = `${baseUrl.replace(/\/+$/, "")}/${supportSupabaseBucket}/${storageKey}`;
        storageProvider = "supabase";
      } else if (attachmentsStorageRoot) {
        const storagePath = resolveAttachmentStoragePath(attachmentsStorageRoot, storageKey);
        if (!storagePath) {
          throw new Error("Failed to resolve local storage path.");
        }
        await fs.promises.mkdir(path.dirname(storagePath), { recursive: true });
        await fs.promises.copyFile(tempPath, storagePath);
        storageUrl = buildAttachmentStorageUrl(attachmentsStoragePublicBaseUrl, storageKey);
        storageProvider = "local_fs";
      } else {
        contentBuffer = await fs.promises.readFile(tempPath);
        storageProvider = "bytea";
      }
    } finally {
      if (tempPath) {
        fs.promises.unlink(tempPath).catch(() => {});
      }
    }

    const uploadedBy = normalizeUsername(uploaderProfile?.username || uploaderProfile?.user?.username);
    const uploadedByDisplayName =
      sanitize(uploaderProfile?.displayName, 220) ||
      sanitize(uploaderProfile?.user?.displayName, 220) ||
      getUserDisplayName(uploadedBy);

    await supportRepo.insertSupportAttachment({
      id: attachmentId,
      requestId,
      fileName,
      mimeType,
      sizeBytes,
      content: contentBuffer,
      storageProvider,
      storageKey,
      storageUrl,
      uploadedBy,
      uploadedByDisplayName,
      uploadedAt: new Date().toISOString(),
    });

    return {
      id: attachmentId,
      requestId,
      fileName,
      mimeType,
      sizeBytes,
      storageProvider,
      storageKey,
      storageUrl,
      uploadedBy,
      uploadedByDisplayName,
      uploadedAt: new Date().toISOString(),
    };
  }

  function validateAttachments(files = []) {
    const normalized = Array.isArray(files) ? files : [];
    if (normalized.length > maxAttachmentsCount) {
      return { ok: false, error: `Too many attachments (max ${maxAttachmentsCount}).` };
    }
    let totalBytes = 0;
    for (const file of normalized) {
      const size = Number(file?.size) || 0;
      totalBytes += size;
      const mime = sanitize(file?.mimetype, 120);
      if (!allowedAttachmentMimeTypes.includes(mime)) {
        return { ok: false, error: `Unsupported attachment type: ${mime || "unknown"}.` };
      }
    }
    if (totalBytes > maxAttachmentsBytes) {
      return { ok: false, error: "Attachments exceed total size limit." };
    }
    return { ok: true };
  }

  async function createRequest(profile, payload, files = []) {
    const title = sanitize(payload?.title, 300);
    const description = sanitize(payload?.description, 6000);
    const priority = normalizePriority(payload?.priority);
    const desiredDueDate = normalizeDueDate(payload?.desired_due_date || payload?.desiredDueDate);
    const urgencyReason = sanitize(payload?.urgency_reason || payload?.urgencyReason, 2000);

    if (!title) {
      return { ok: false, status: 400, error: "Title is required." };
    }
    if (!description) {
      return { ok: false, status: 400, error: "Description is required." };
    }
    if (!desiredDueDate) {
      return { ok: false, status: 400, error: "Desired due date is required." };
    }
    if ((priority === PRIORITY_URGENT || priority === PRIORITY_CRITICAL) && !urgencyReason) {
      return { ok: false, status: 400, error: "Urgency reason is required." };
    }
    if (priority === PRIORITY_CRITICAL && !canUseCriticalPriority(profile)) {
      return { ok: false, status: 403, error: "Only Department Head can set very urgent priority." };
    }

    const validation = validateAttachments(files);
    if (!validation.ok) {
      return { ok: false, status: 400, error: validation.error };
    }

    const username = normalizeUsername(profile?.username || profile?.user?.username);
    const displayName = sanitize(profile?.displayName, 220) || sanitize(profile?.user?.displayName, 220) || username;
    if (!username) {
      return { ok: false, status: 401, error: "Unauthorized." };
    }

    const departmentId = normalizeWebAuthDepartmentId?.(profile?.departmentId) || "";
    const roleId = normalizeWebAuthRoleId?.(profile?.roleId, departmentId) || profile?.roleId || "";
    const nowIso = new Date().toISOString();

    const requestId = crypto.randomUUID();
    await supportRepo.insertSupportRequest({
      id: requestId,
      title,
      description,
      priority,
      urgencyReason,
      desiredDueDate,
      status: STATUS_REVIEW,
      createdBy: username,
      createdByDisplayName: displayName,
      createdByDepartmentId: departmentId,
      createdByDepartmentName: sanitize(profile?.departmentName, 220),
      createdByRoleId: roleId,
      createdByRoleName: sanitize(profile?.roleName, 180),
      isFromHead: isDepartmentHeadProfile(profile),
      assignedTo: "",
      assignedToDisplayName: "",
      createdAt: nowIso,
      updatedAt: nowIso,
      timeInProgressStart: null,
      timeDoneAt: null,
      lastNeedsRevisionReason: "",
      lastRejectedReason: "",
      lastNeedsRevisionAt: null,
      lastRejectedAt: null,
      withdrawnAt: null,
    });

    await createSupportHistoryEntry({
      requestId,
      action: "create",
      actorUsername: username,
      actorDisplayName: displayName,
      payload: {
        title,
        description,
        priority,
        desiredDueDate,
        urgencyReason,
      },
    });

    const attachments = [];
    for (const file of files) {
      const stored = await storeAttachmentFile(requestId, file, profile);
      if (stored) {
        attachments.push(stored);
      }
    }

    const request = await getRequestDetails(profile, requestId);
    if (request) {
      await notifyAdminsOnCreate(request);
    }

    return { ok: true, status: 201, request, attachments };
  }

  async function updateRequest(profile, requestId, payload) {
    const existingRow = await supportRepo.getSupportRequestById(requestId);
    const existing = mapSupportRequestRow(existingRow);
    if (!existing) {
      return { ok: false, status: 404, error: "Request not found." };
    }
    if (!canEditRequest(profile, existing)) {
      return { ok: false, status: 403, error: "Access denied." };
    }

    const updates = {};
    const before = {};
    const after = {};

    if (payload?.title !== undefined) {
      const nextTitle = sanitize(payload.title, 300);
      if (!nextTitle) {
        return { ok: false, status: 400, error: "Title is required." };
      }
      updates.title = nextTitle;
      before.title = existing.title;
      after.title = nextTitle;
    }

    if (payload?.description !== undefined) {
      const nextDescription = sanitize(payload.description, 6000);
      if (!nextDescription) {
        return { ok: false, status: 400, error: "Description is required." };
      }
      updates.description = nextDescription;
      before.description = existing.description;
      after.description = nextDescription;
    }

    if (payload?.priority !== undefined) {
      const nextPriority = normalizePriority(payload.priority);
      if (nextPriority === PRIORITY_CRITICAL && !canUseCriticalPriority(profile)) {
        return { ok: false, status: 403, error: "Only Department Head can set very urgent priority." };
      }
      updates.priority = nextPriority;
      before.priority = existing.priority;
      after.priority = nextPriority;
    }

    if (payload?.urgency_reason !== undefined || payload?.urgencyReason !== undefined) {
      const nextUrgency = sanitize(payload?.urgency_reason ?? payload?.urgencyReason, 2000);
      if ((updates.priority === PRIORITY_URGENT || updates.priority === PRIORITY_CRITICAL || existing.priority === PRIORITY_URGENT || existing.priority === PRIORITY_CRITICAL) && !nextUrgency) {
        return { ok: false, status: 400, error: "Urgency reason is required." };
      }
      updates.urgency_reason = nextUrgency;
      before.urgencyReason = existing.urgencyReason;
      after.urgencyReason = nextUrgency;
    }

    if (payload?.desired_due_date !== undefined || payload?.desiredDueDate !== undefined) {
      const nextDue = normalizeDueDate(payload?.desired_due_date ?? payload?.desiredDueDate);
      if (!nextDue) {
        return { ok: false, status: 400, error: "Desired due date is required." };
      }
      updates.desired_due_date = nextDue;
      before.desiredDueDate = existing.desiredDueDate;
      after.desiredDueDate = nextDue;
    }

    updates.updated_at = new Date().toISOString();
    await supportRepo.updateSupportRequest(existing.id, updates);

    await createSupportHistoryEntry({
      requestId: existing.id,
      action: "update",
      actorUsername: normalizeUsername(profile?.username || profile?.user?.username),
      actorDisplayName: sanitize(profile?.displayName, 220) || sanitize(profile?.user?.displayName, 220),
      payload: {
        before,
        after,
      },
    });

    return { ok: true, status: 200, request: await getRequestDetails(profile, existing.id) };
  }

  async function moveRequestStatus(profile, requestId, payload, files = []) {
    const existingRow = await supportRepo.getSupportRequestById(requestId);
    const existing = mapSupportRequestRow(existingRow);
    if (!existing) {
      return { ok: false, status: 404, error: "Request not found." };
    }

    const targetStatus = normalizeStatus(payload?.status);
    const reason = sanitize(payload?.reason, 2000);

    if (targetStatus === STATUS_WITHDRAWN && !canWithdrawRequest(profile, existing)) {
      return { ok: false, status: 403, error: "Withdraw is not allowed." };
    }
    if (targetStatus !== STATUS_WITHDRAWN && !canAdminMoveStatus(profile)) {
      return { ok: false, status: 403, error: "Access denied." };
    }

    if ((targetStatus === STATUS_NEEDS_REVISION || targetStatus === STATUS_REJECTED) && !reason) {
      return { ok: false, status: 400, error: "Reason is required." };
    }

    const validation = validateAttachments(files);
    if (!validation.ok) {
      return { ok: false, status: 400, error: validation.error };
    }

    const updates = {
      status: targetStatus,
      updated_at: new Date().toISOString(),
    };

    if (targetStatus === STATUS_IN_PROGRESS && !existing.timeInProgressStart) {
      updates.time_in_progress_start = new Date().toISOString();
    }

    if (targetStatus === STATUS_DONE && !existing.timeDoneAt) {
      updates.time_done_at = new Date().toISOString();
    }

    if (targetStatus === STATUS_NEEDS_REVISION) {
      updates.last_needs_revision_reason = reason;
      updates.last_needs_revision_at = new Date().toISOString();
    }

    if (targetStatus === STATUS_REJECTED) {
      updates.last_rejected_reason = reason;
      updates.last_rejected_at = new Date().toISOString();
    }

    if (targetStatus === STATUS_WITHDRAWN) {
      updates.withdrawn_at = new Date().toISOString();
    }

    if (payload?.assigned_to !== undefined) {
      updates.assigned_to = sanitize(payload.assigned_to, 200);
      updates.assigned_to_display_name = sanitize(payload.assigned_to_display_name, 220) || getUserDisplayName(updates.assigned_to);
    }

    await supportRepo.updateSupportRequest(existing.id, updates);

    const attachments = [];
    for (const file of files) {
      const stored = await storeAttachmentFile(existing.id, file, profile);
      if (stored) {
        attachments.push(stored);
      }
    }

    await createSupportHistoryEntry({
      requestId: existing.id,
      action: "status_change",
      actorUsername: normalizeUsername(profile?.username || profile?.user?.username),
      actorDisplayName: sanitize(profile?.displayName, 220) || sanitize(profile?.user?.displayName, 220),
      payload: {
        from: existing.status,
        to: targetStatus,
        reason,
        attachments: attachments.map((item) => item.id),
      },
    });

    if (targetStatus === STATUS_NEEDS_REVISION || targetStatus === STATUS_REJECTED || targetStatus === STATUS_DONE) {
      await notifyAuthorOnStatus(existing, targetStatus, reason);
    }

    return { ok: true, status: 200, request: await getRequestDetails(profile, existing.id) };
  }

  async function addRequestAttachment(profile, requestId, files = []) {
    const existingRow = await supportRepo.getSupportRequestById(requestId);
    const existing = mapSupportRequestRow(existingRow);
    if (!existing) {
      return { ok: false, status: 404, error: "Request not found." };
    }
    if (!canProfileViewRequest(profile, existing)) {
      return { ok: false, status: 403, error: "Access denied." };
    }

    const validation = validateAttachments(files);
    if (!validation.ok) {
      return { ok: false, status: 400, error: validation.error };
    }

    const attachments = [];
    for (const file of files) {
      const stored = await storeAttachmentFile(existing.id, file, profile);
      if (stored) {
        attachments.push(stored);
      }
    }

    if (attachments.length) {
      await createSupportHistoryEntry({
        requestId: existing.id,
        action: "attachment_add",
        actorUsername: normalizeUsername(profile?.username || profile?.user?.username),
        actorDisplayName: sanitize(profile?.displayName, 220) || sanitize(profile?.user?.displayName, 220),
        payload: {
          attachments: attachments.map((item) => item.id),
        },
      });
    }

    return { ok: true, status: 200, attachments };
  }

  async function addComment(profile, requestId, comment) {
    const existingRow = await supportRepo.getSupportRequestById(requestId);
    const existing = mapSupportRequestRow(existingRow);
    if (!existing) {
      return { ok: false, status: 404, error: "Request not found." };
    }
    if (!canProfileViewRequest(profile, existing)) {
      return { ok: false, status: 403, error: "Access denied." };
    }
    const normalizedComment = sanitize(comment, 4000);
    if (!normalizedComment) {
      return { ok: false, status: 400, error: "Comment is required." };
    }

    await createSupportHistoryEntry({
      requestId: existing.id,
      action: "comment",
      actorUsername: normalizeUsername(profile?.username || profile?.user?.username),
      actorDisplayName: sanitize(profile?.displayName, 220) || sanitize(profile?.user?.displayName, 220),
      payload: {
        comment: normalizedComment,
      },
    });

    return { ok: true, status: 200 };
  }

  async function getReports(profile, filters = {}) {
    if (!isAdminProfile(profile)) {
      return { ok: false, status: 403, error: "Access denied." };
    }
    const rows = await supportRepo.listSupportRequestsForReport(filters);
    const items = [];
    const recent = [];
    const now = Date.now();
    let totalHours = 0;

    for (const row of rows) {
      if (!row?.time_in_progress_start || !row?.time_done_at) {
        continue;
      }
      const start = new Date(row.time_in_progress_start).getTime();
      const done = new Date(row.time_done_at).getTime();
      if (!Number.isFinite(start) || !Number.isFinite(done) || done < start) {
        continue;
      }
      const hours = (done - start) / (1000 * 60 * 60);
      const entry = {
        id: sanitize(row.id, 180),
        title: sanitize(row.title, 300),
        priority: sanitize(row.priority, 80),
        author: sanitize(row.created_by_display_name, 220) || sanitize(row.created_by, 200),
        authorUsername: sanitize(row.created_by, 200),
        assignedTo: sanitize(row.assigned_to_display_name, 220) || sanitize(row.assigned_to, 200),
        assignedToUsername: sanitize(row.assigned_to, 200),
        hours,
        doneAt: row.time_done_at ? new Date(row.time_done_at).toISOString() : null,
      };

      const ageMs = now - done;
      if (ageMs < 60 * 60 * 1000) {
        recent.push(entry);
      } else {
        items.push(entry);
        totalHours += hours;
      }
    }

    const avgHours = items.length ? totalHours / items.length : 0;
    return {
      ok: true,
      status: 200,
      report: {
        totalHours,
        averageHours: avgHours,
        items,
        recent,
      },
    };
  }

  async function getAttachmentDownload(profile, attachmentId) {
    const row = await supportRepo.getSupportAttachmentById(attachmentId);
    if (!row) {
      return { ok: false, status: 404, error: "Attachment not found." };
    }
    const requestRow = await supportRepo.getSupportRequestById(row.request_id);
    const request = mapSupportRequestRow(requestRow);
    if (!request || !canProfileViewRequest(profile, request)) {
      return { ok: false, status: 403, error: "Access denied." };
    }
    if (row.storage_provider === "local_fs" && !row.content && attachmentsStorageRoot && row.storage_key) {
      const storagePath = resolveAttachmentStoragePath(attachmentsStorageRoot, row.storage_key);
      if (storagePath) {
        try {
          row.content = await fs.promises.readFile(storagePath);
        } catch {
          // Ignore read errors, controller will fall back to storage_url if present.
        }
      }
    }

    return { ok: true, status: 200, attachment: row };
  }

  return {
    listRequestsForProfile,
    getRequestDetails,
    createRequest,
    updateRequest,
    moveRequestStatus,
    addRequestAttachment,
    addComment,
    getReports,
    getAttachmentDownload,
    mapSupportRequestRow,
    mapSupportAttachmentRow,
    mapSupportHistoryRow,
    normalizePriority,
    normalizeStatus,
  };
}

module.exports = {
  createSupportService,
  normalizePriority: normalizePriorityValue,
  PRIORITY_LOW,
  PRIORITY_NORMAL,
  PRIORITY_URGENT,
  PRIORITY_CRITICAL,
  STATUS_NEW,
  STATUS_REVIEW,
  STATUS_IN_PROGRESS,
  STATUS_NEEDS_REVISION,
  STATUS_DONE,
  STATUS_REJECTED,
  STATUS_WITHDRAWN,
};
