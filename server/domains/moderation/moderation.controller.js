"use strict";

function createModerationController(dependencies = {}) {
  const {
    hasDatabase,
    listModerationSubmissions,
    paginationV2Enabled,
    listPendingSubmissionFiles,
    isPreviewableAttachmentMimeType,
    getPendingSubmissionFile,
    normalizeAttachmentMimeType,
    buildContentDisposition,
    setNoStorePrivateApiHeaders,
    setAttachmentResponseSecurityHeaders,
    sanitizeTextValue,
    reviewClientSubmission,
    getReviewerIdentity,
    resolveDbHttpStatus,
    buildPublicErrorPayload,
  } = dependencies;

  function hasDatabaseConfigured() {
    if (typeof hasDatabase === "function") {
      return hasDatabase();
    }
    return Boolean(hasDatabase);
  }

  async function handleModerationSubmissionsGet(req, res) {
    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    try {
      const result = await listModerationSubmissions({
        status: req.query.status,
        limit: req.query.limit,
        cursor: req.query.cursor,
        paginationV2: paginationV2Enabled,
      });

      if (result.error) {
        res.status(400).json({
          error: result.error,
        });
        return;
      }

      const responsePayload = {
        status: result.status,
        items: result.items,
      };

      if (paginationV2Enabled) {
        responsePayload.hasMore = Boolean(result.hasMore);
        responsePayload.nextCursor = result.nextCursor || null;
      }

      res.json(responsePayload);
    } catch (error) {
      console.error("GET /api/moderation/submissions failed:", error);
      res
        .status(resolveDbHttpStatus(error))
        .json(buildPublicErrorPayload(error, "Failed to load moderation submissions"));
    }
  }

  async function handleModerationSubmissionFilesGet(req, res) {
    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    try {
      const filesResult = await listPendingSubmissionFiles(req.params.id);
      if (!filesResult.ok) {
        res.status(filesResult.status).json({
          error: filesResult.error,
        });
        return;
      }

      const basePath = `/api/moderation/submissions/${encodeURIComponent(filesResult.submissionId)}/files`;
      const items = filesResult.items.map((file) => {
        const canPreview = isPreviewableAttachmentMimeType(file.mimeType);
        return {
          ...file,
          canPreview,
          previewUrl: canPreview ? `${basePath}/${encodeURIComponent(file.id)}?inline=1` : "",
          downloadUrl: `${basePath}/${encodeURIComponent(file.id)}`,
        };
      });

      res.json({
        ok: true,
        items,
      });
    } catch (error) {
      console.error("GET /api/moderation/submissions/:id/files failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load submission files"));
    }
  }

  async function handleModerationSubmissionFileGet(req, res) {
    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    try {
      const fileResult = await getPendingSubmissionFile(req.params.id, req.params.fileId);
      if (!fileResult.ok) {
        res.status(fileResult.status).json({
          error: fileResult.error,
        });
        return;
      }

      const file = fileResult.file;
      const mimeType = normalizeAttachmentMimeType(file.mimeType);
      const inlineRequested = sanitizeTextValue(req.query.inline, 10) === "1";
      const isInline = inlineRequested && isPreviewableAttachmentMimeType(mimeType);
      res.setHeader("Content-Type", mimeType);
      res.setHeader("Content-Length", String(file.content.length));
      res.setHeader(
        "Content-Disposition",
        buildContentDisposition(isInline ? "inline" : "attachment", file.fileName),
      );
      setNoStorePrivateApiHeaders(res);
      setAttachmentResponseSecurityHeaders(res, {
        isInline,
      });
      res.send(file.content);
    } catch (error) {
      console.error("GET /api/moderation/submissions/:id/files/:fileId failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load file"));
    }
  }

  async function handleModerationApprovePost(req, res) {
    try {
      const result = await reviewClientSubmission(
        req.params.id,
        "approved",
        getReviewerIdentity(req),
        req.body?.reviewNote,
      );

      if (!result.ok) {
        res.status(result.status).json({
          error: result.error,
        });
        return;
      }

      res.json({
        ok: true,
        item: result.item,
      });
    } catch (error) {
      console.error("POST /api/moderation/submissions/:id/approve failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to approve submission"));
    }
  }

  async function handleModerationRejectPost(req, res) {
    try {
      const result = await reviewClientSubmission(
        req.params.id,
        "rejected",
        getReviewerIdentity(req),
        req.body?.reviewNote,
      );

      if (!result.ok) {
        res.status(result.status).json({
          error: result.error,
        });
        return;
      }

      res.json({
        ok: true,
        item: result.item,
      });
    } catch (error) {
      console.error("POST /api/moderation/submissions/:id/reject failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to reject submission"));
    }
  }

  return {
    handleModerationSubmissionsGet,
    handleModerationSubmissionFilesGet,
    handleModerationSubmissionFileGet,
    handleModerationApprovePost,
    handleModerationRejectPost,
  };
}

module.exports = {
  createModerationController,
};
