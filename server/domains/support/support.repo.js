"use strict";

function createSupportRepo(dependencies = {}) {
  const { db, ensureDatabaseReady, tables, helpers } = dependencies;
  const query =
    typeof db?.query === "function"
      ? db.query
      : async () => {
          throw new Error("Support repository query function is not configured.");
        };

  const supportRequestsTable = tables?.supportRequestsTable;
  const supportAttachmentsTable = tables?.supportAttachmentsTable;
  const supportHistoryTable = tables?.supportHistoryTable;
  const sanitizeTextValue =
    typeof helpers?.sanitizeTextValue === "function"
      ? helpers.sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  if (typeof ensureDatabaseReady !== "function") {
    throw new Error("createSupportRepo requires ensureDatabaseReady().");
  }
  if (!supportRequestsTable || !supportAttachmentsTable || !supportHistoryTable) {
    throw new Error("createSupportRepo requires support tables.");
  }

  function normalizeLimit(rawValue) {
    const parsed = Number.parseInt(String(rawValue ?? ""), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return 200;
    }
    return Math.min(Math.max(parsed, 1), 1000);
  }

  function normalizeOffset(rawValue) {
    const parsed = Number.parseInt(String(rawValue ?? ""), 10);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return 0;
    }
    return parsed;
  }

  async function listSupportRequests(filters = {}) {
    await ensureDatabaseReady();
    const clauses = [];
    const values = [];
    let index = 1;

    if (Array.isArray(filters.allowedUsernames) && filters.allowedUsernames.length) {
      clauses.push(`created_by = ANY($${index}::text[])`);
      values.push(filters.allowedUsernames);
      index += 1;
    }

    if (Array.isArray(filters.statuses) && filters.statuses.length) {
      clauses.push(`status = ANY($${index}::text[])`);
      values.push(filters.statuses);
      index += 1;
    }

    if (Array.isArray(filters.priorities) && filters.priorities.length) {
      clauses.push(`priority = ANY($${index}::text[])`);
      values.push(filters.priorities);
      index += 1;
    }

    if (filters.assignedTo) {
      clauses.push(`assigned_to = $${index}`);
      values.push(sanitizeTextValue(filters.assignedTo, 200));
      index += 1;
    }

    if (filters.createdBy) {
      clauses.push(`created_by = $${index}`);
      values.push(sanitizeTextValue(filters.createdBy, 200));
      index += 1;
    }

    const limit = normalizeLimit(filters.limit);
    const offset = normalizeOffset(filters.offset);
    values.push(limit, offset);

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const result = await query(
      `
        SELECT
          id,
          title,
          description,
          priority,
          urgency_reason,
          desired_due_date,
          status,
          created_by,
          created_by_display_name,
          created_by_department_id,
          created_by_department_name,
          created_by_role_id,
          created_by_role_name,
          is_from_head,
          assigned_to,
          assigned_to_display_name,
          created_at,
          updated_at,
          time_in_progress_start,
          time_done_at,
          last_needs_revision_reason,
          last_rejected_reason,
          last_needs_revision_at,
          last_rejected_at,
          withdrawn_at
        FROM ${supportRequestsTable}
        ${whereClause}
        ORDER BY created_at DESC, id DESC
        LIMIT $${index} OFFSET $${index + 1}
      `,
      values,
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  async function getSupportRequestById(id) {
    const normalizedId = sanitizeTextValue(id, 180);
    if (!normalizedId) {
      return null;
    }
    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          id,
          title,
          description,
          priority,
          urgency_reason,
          desired_due_date,
          status,
          created_by,
          created_by_display_name,
          created_by_department_id,
          created_by_department_name,
          created_by_role_id,
          created_by_role_name,
          is_from_head,
          assigned_to,
          assigned_to_display_name,
          created_at,
          updated_at,
          time_in_progress_start,
          time_done_at,
          last_needs_revision_reason,
          last_rejected_reason,
          last_needs_revision_at,
          last_rejected_at,
          withdrawn_at
        FROM ${supportRequestsTable}
        WHERE id = $1
        LIMIT 1
      `,
      [normalizedId],
    );
    return result?.rows?.[0] || null;
  }

  async function insertSupportRequest(payload) {
    await ensureDatabaseReady();
    const result = await query(
      `
        INSERT INTO ${supportRequestsTable} (
          id,
          title,
          description,
          priority,
          urgency_reason,
          desired_due_date,
          status,
          created_by,
          created_by_display_name,
          created_by_department_id,
          created_by_department_name,
          created_by_role_id,
          created_by_role_name,
          is_from_head,
          assigned_to,
          assigned_to_display_name,
          created_at,
          updated_at,
          time_in_progress_start,
          time_done_at,
          last_needs_revision_reason,
          last_rejected_reason,
          last_needs_revision_at,
          last_rejected_at,
          withdrawn_at
        )
        VALUES (
          $1, $2, $3, $4, $5, $6, $7,
          $8, $9, $10, $11, $12, $13, $14,
          $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
        )
        RETURNING id
      `,
      [
        payload.id,
        payload.title,
        payload.description,
        payload.priority,
        payload.urgencyReason,
        payload.desiredDueDate,
        payload.status,
        payload.createdBy,
        payload.createdByDisplayName,
        payload.createdByDepartmentId,
        payload.createdByDepartmentName,
        payload.createdByRoleId,
        payload.createdByRoleName,
        payload.isFromHead,
        payload.assignedTo,
        payload.assignedToDisplayName,
        payload.createdAt,
        payload.updatedAt,
        payload.timeInProgressStart,
        payload.timeDoneAt,
        payload.lastNeedsRevisionReason,
        payload.lastRejectedReason,
        payload.lastNeedsRevisionAt,
        payload.lastRejectedAt,
        payload.withdrawnAt,
      ],
    );
    return result?.rows?.[0] || null;
  }

  async function updateSupportRequest(id, updates = {}) {
    const normalizedId = sanitizeTextValue(id, 180);
    if (!normalizedId) {
      return null;
    }
    const allowedFields = [
      "title",
      "description",
      "priority",
      "urgency_reason",
      "desired_due_date",
      "status",
      "assigned_to",
      "assigned_to_display_name",
      "updated_at",
      "time_in_progress_start",
      "time_done_at",
      "last_needs_revision_reason",
      "last_rejected_reason",
      "last_needs_revision_at",
      "last_rejected_at",
      "withdrawn_at",
    ];

    const assignments = [];
    const values = [];
    let index = 1;

    for (const field of allowedFields) {
      if (!Object.prototype.hasOwnProperty.call(updates, field)) {
        continue;
      }
      assignments.push(`${field} = $${index}`);
      values.push(updates[field]);
      index += 1;
    }

    if (!assignments.length) {
      return null;
    }

    values.push(normalizedId);
    await ensureDatabaseReady();
    const result = await query(
      `
        UPDATE ${supportRequestsTable}
        SET ${assignments.join(", ")}
        WHERE id = $${index}
        RETURNING id
      `,
      values,
    );
    return result?.rows?.[0] || null;
  }

  async function insertSupportHistory(entry) {
    await ensureDatabaseReady();
    const result = await query(
      `
        INSERT INTO ${supportHistoryTable} (
          id,
          request_id,
          action,
          actor_username,
          actor_display_name,
          payload,
          created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        RETURNING id
      `,
      [
        entry.id,
        entry.requestId,
        entry.action,
        entry.actorUsername,
        entry.actorDisplayName,
        JSON.stringify(entry.payload || {}),
        entry.createdAt,
      ],
    );
    return result?.rows?.[0] || null;
  }

  async function listSupportHistory(requestId) {
    const normalizedId = sanitizeTextValue(requestId, 180);
    if (!normalizedId) {
      return [];
    }
    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          id,
          request_id,
          action,
          actor_username,
          actor_display_name,
          payload,
          created_at
        FROM ${supportHistoryTable}
        WHERE request_id = $1
        ORDER BY created_at DESC, id DESC
      `,
      [normalizedId],
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  async function insertSupportAttachment(entry) {
    await ensureDatabaseReady();
    const result = await query(
      `
        INSERT INTO ${supportAttachmentsTable} (
          id,
          request_id,
          file_name,
          mime_type,
          size_bytes,
          content,
          storage_provider,
          storage_key,
          storage_url,
          uploaded_by,
          uploaded_by_display_name,
          uploaded_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        RETURNING id
      `,
      [
        entry.id,
        entry.requestId,
        entry.fileName,
        entry.mimeType,
        entry.sizeBytes,
        entry.content,
        entry.storageProvider,
        entry.storageKey,
        entry.storageUrl,
        entry.uploadedBy,
        entry.uploadedByDisplayName,
        entry.uploadedAt,
      ],
    );
    return result?.rows?.[0] || null;
  }

  async function listSupportAttachments(requestId) {
    const normalizedId = sanitizeTextValue(requestId, 180);
    if (!normalizedId) {
      return [];
    }
    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          id,
          request_id,
          file_name,
          mime_type,
          size_bytes,
          storage_provider,
          storage_key,
          storage_url,
          uploaded_by,
          uploaded_by_display_name,
          uploaded_at
        FROM ${supportAttachmentsTable}
        WHERE request_id = $1
        ORDER BY uploaded_at DESC, id DESC
      `,
      [normalizedId],
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  async function getSupportAttachmentById(id) {
    const normalizedId = sanitizeTextValue(id, 180);
    if (!normalizedId) {
      return null;
    }
    await ensureDatabaseReady();
    const result = await query(
      `
        SELECT
          id,
          request_id,
          file_name,
          mime_type,
          size_bytes,
          content,
          storage_provider,
          storage_key,
          storage_url,
          uploaded_by,
          uploaded_by_display_name,
          uploaded_at
        FROM ${supportAttachmentsTable}
        WHERE id = $1
        LIMIT 1
      `,
      [normalizedId],
    );
    return result?.rows?.[0] || null;
  }

  async function listSupportRequestsForReport(filters = {}) {
    await ensureDatabaseReady();
    const clauses = ["status = 'done'"];
    const values = [];
    let index = 1;

    if (filters.from) {
      clauses.push(`time_done_at >= $${index}`);
      values.push(filters.from);
      index += 1;
    }

    if (filters.to) {
      clauses.push(`time_done_at <= $${index}`);
      values.push(filters.to);
      index += 1;
    }

    if (Array.isArray(filters.priorities) && filters.priorities.length) {
      clauses.push(`priority = ANY($${index}::text[])`);
      values.push(filters.priorities);
      index += 1;
    }

    if (filters.assignedTo) {
      clauses.push(`assigned_to = $${index}`);
      values.push(sanitizeTextValue(filters.assignedTo, 200));
      index += 1;
    }

    if (filters.createdBy) {
      clauses.push(`created_by = $${index}`);
      values.push(sanitizeTextValue(filters.createdBy, 200));
      index += 1;
    }

    const whereClause = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
    const result = await query(
      `
        SELECT
          id,
          title,
          priority,
          created_by,
          created_by_display_name,
          assigned_to,
          assigned_to_display_name,
          time_in_progress_start,
          time_done_at,
          created_at
        FROM ${supportRequestsTable}
        ${whereClause}
        ORDER BY time_done_at DESC, id DESC
      `,
      values,
    );
    return Array.isArray(result?.rows) ? result.rows : [];
  }

  return {
    listSupportRequests,
    getSupportRequestById,
    insertSupportRequest,
    updateSupportRequest,
    insertSupportHistory,
    listSupportHistory,
    insertSupportAttachment,
    listSupportAttachments,
    getSupportAttachmentById,
    listSupportRequestsForReport,
  };
}

module.exports = {
  createSupportRepo,
};
