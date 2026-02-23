"use strict";

function createMiniController(dependencies = {}) {
  const {
    enforceMiniRateLimit,
    rateLimitProfileApiMiniAccess,
    rateLimitProfileApiMiniWrite,
    verifyTelegramInitData,
    sanitizeTextValue,
    createMiniUploadToken,
    miniUploadTokenTtlSec,
    telegramInitDataWriteTtlSec,
    miniClientAttachmentsConfig,
    resolveMiniIdempotencyKeyFromRequest,
    isMultipartRequest,
    resolveMiniUploadTokenFromRequest,
    parseMiniUploadToken,
    respondMiniRequestEarlyAndClose,
    resolveRequestContentLengthBytes,
    miniMultipartMaxContentLengthBytes,
    hasDatabase,
    withMiniUploadParseSlot,
    parseMiniMultipartRequest,
    parseMiniClientPayload,
    createRecordFromMiniPayload,
    buildMiniSubmissionAttachments,
    cleanupTemporaryUploadFiles,
    reserveMiniWriteIdempotency,
    reserveMiniWriteInitDataReplayKey,
    buildMiniWriteInitDataReplayKey,
    resolveMiniWriteInitDataReplayExpiresAtMs,
    queueClientSubmission,
    commitMiniWriteIdempotencySuccess,
    enqueueMiniSubmissionTelegramNotification,
    resolveDbHttpStatus,
    buildPublicErrorPayload,
    releaseMiniWriteIdempotencyReservation,
    releaseMiniWriteInitDataReplayKeyReservation,
    cleanupTemporaryAttachmentFiles,
    miniHtmlPath,
  } = dependencies;

  function hasDatabaseConfigured() {
    if (typeof hasDatabase === "function") {
      return hasDatabase();
    }
    return Boolean(hasDatabase);
  }

  async function handleMiniAccessPost(req, res) {
    if (
      !(await enforceMiniRateLimit(req, res, {
        scope: "api.mini.access",
        ipProfile: {
          windowMs: rateLimitProfileApiMiniAccess.windowMs,
          maxHits: rateLimitProfileApiMiniAccess.maxHitsIp,
          blockMs: rateLimitProfileApiMiniAccess.blockMs,
        },
        message: "Mini App access check limit reached. Please wait before retrying.",
        code: "mini_access_rate_limited",
      }))
    ) {
      return;
    }

    const authResult = await verifyTelegramInitData(req.body?.initData);
    if (!authResult.ok) {
      res.status(authResult.status).json({
        error: authResult.error,
      });
      return;
    }

    if (
      !(await enforceMiniRateLimit(req, res, {
        scope: "api.mini.access",
        userProfile: {
          windowMs: rateLimitProfileApiMiniAccess.windowMs,
          maxHits: rateLimitProfileApiMiniAccess.maxHitsUser,
          blockMs: rateLimitProfileApiMiniAccess.blockMs,
        },
        username: sanitizeTextValue(authResult.user?.id, 50),
        message: "Mini App access check limit reached. Please wait before retrying.",
        code: "mini_access_rate_limited",
      }))
    ) {
      return;
    }

    const uploadToken = createMiniUploadToken(authResult.user);
    res.json({
      ok: true,
      user: {
        id: sanitizeTextValue(authResult.user?.id, 50),
        username: sanitizeTextValue(authResult.user?.username, 120),
      },
      uploadToken: uploadToken.token,
      uploadTokenExpiresAt: uploadToken.expiresAtMs ? new Date(uploadToken.expiresAtMs).toISOString() : null,
      uploadTokenTtlSec: miniUploadTokenTtlSec,
      writeInitDataTtlSec: telegramInitDataWriteTtlSec,
      miniConfig: {
        attachments: miniClientAttachmentsConfig,
      },
    });
  }

  async function handleMiniClientsPost(req, res) {
    const multipartRequest = isMultipartRequest(req);
    const idempotencyKeyResult = resolveMiniIdempotencyKeyFromRequest(req);
    if (!idempotencyKeyResult.ok) {
      res.status(idempotencyKeyResult.status || 400).json({
        error: idempotencyKeyResult.error || "Invalid Idempotency-Key header.",
        code: idempotencyKeyResult.code || "mini_idempotency_key_invalid",
      });
      return;
    }

    const idempotencyKey = idempotencyKeyResult.key;
    let parsedUploadToken = null;
    if (
      !(await enforceMiniRateLimit(req, res, {
        scope: "api.mini.write",
        ipProfile: {
          windowMs: rateLimitProfileApiMiniWrite.windowMs,
          maxHits: rateLimitProfileApiMiniWrite.maxHitsIp,
          blockMs: rateLimitProfileApiMiniWrite.blockMs,
        },
        message: "Mini App write limit reached. Please wait before retrying.",
        code: "mini_write_rate_limited",
      }))
    ) {
      return;
    }

    if (multipartRequest) {
      const uploadTokenRaw = resolveMiniUploadTokenFromRequest(req);
      if (!uploadTokenRaw) {
        respondMiniRequestEarlyAndClose(req, res, 401, {
          error: "Missing upload token. Reopen Mini App.",
          code: "mini_upload_token_missing",
        });
        return;
      }

      parsedUploadToken = parseMiniUploadToken(uploadTokenRaw);
      if (!parsedUploadToken.ok) {
        respondMiniRequestEarlyAndClose(req, res, parsedUploadToken.status || 401, {
          error: parsedUploadToken.error || "Upload token is invalid. Reopen Mini App.",
          code: parsedUploadToken.code || "mini_upload_token_invalid",
        });
        return;
      }

      if (
        !(await enforceMiniRateLimit(req, res, {
          scope: "api.mini.write",
          userProfile: {
            windowMs: rateLimitProfileApiMiniWrite.windowMs,
            maxHits: rateLimitProfileApiMiniWrite.maxHitsUser,
            blockMs: rateLimitProfileApiMiniWrite.blockMs,
          },
          username: parsedUploadToken.userId,
          message: "Mini App write limit reached. Please wait before retrying.",
          code: "mini_write_rate_limited",
        }))
      ) {
        return;
      }

      const contentLengthBytes = resolveRequestContentLengthBytes(req);
      if (Number.isFinite(contentLengthBytes) && contentLengthBytes > miniMultipartMaxContentLengthBytes) {
        respondMiniRequestEarlyAndClose(req, res, 413, {
          error: "Attachment payload is too large.",
          code: "mini_multipart_too_large",
        });
        return;
      }
    }

    if (!hasDatabaseConfigured()) {
      res.status(503).json({
        error: "Database is not configured. Add DATABASE_URL in Render environment variables.",
      });
      return;
    }

    try {
      if (multipartRequest) {
        await withMiniUploadParseSlot(() => parseMiniMultipartRequest(req, res));
      }
    } catch (error) {
      res.status(error.httpStatus || 400).json({
        error: sanitizeTextValue(error?.message, 500) || "Failed to process file uploads.",
      });
      return;
    }

    const parsedPayload = parseMiniClientPayload(req);
    if (parsedPayload.error) {
      res.status(parsedPayload.status || 400).json({
        error: parsedPayload.error,
      });
      return;
    }

    const authResult = await verifyTelegramInitData(parsedPayload.initData, {
      ttlSec: telegramInitDataWriteTtlSec,
    });
    if (!authResult.ok) {
      res.status(authResult.status).json({
        error: authResult.error,
      });
      return;
    }

    const authenticatedUserId = sanitizeTextValue(authResult.user?.id, 50);
    if (
      !parsedUploadToken &&
      !(await enforceMiniRateLimit(req, res, {
        scope: "api.mini.write",
        userProfile: {
          windowMs: rateLimitProfileApiMiniWrite.windowMs,
          maxHits: rateLimitProfileApiMiniWrite.maxHitsUser,
          blockMs: rateLimitProfileApiMiniWrite.blockMs,
        },
        username: authenticatedUserId,
        message: "Mini App write limit reached. Please wait before retrying.",
        code: "mini_write_rate_limited",
      }))
    ) {
      return;
    }

    if (parsedUploadToken && (!authenticatedUserId || authenticatedUserId !== parsedUploadToken.userId)) {
      res.status(401).json({
        error: "Upload token user mismatch. Reopen Mini App.",
        code: "mini_upload_token_user_mismatch",
      });
      return;
    }

    const creationResult = createRecordFromMiniPayload(parsedPayload.client);
    if (!creationResult.record) {
      res.status(400).json({
        error: creationResult.error || "Invalid client payload.",
      });
      return;
    }

    const attachmentsResult = await buildMiniSubmissionAttachments(req.files);
    if (attachmentsResult.error) {
      await cleanupTemporaryUploadFiles(req.files);
      res.status(attachmentsResult.status || 400).json({
        error: attachmentsResult.error,
      });
      return;
    }

    let writeReplayReservation = null;
    let writeReplayReservationShouldPersist = false;
    let writeIdempotencyReservation = null;
    try {
      const idempotencyReservationResult = await reserveMiniWriteIdempotency(authenticatedUserId, idempotencyKey);
      if (!idempotencyReservationResult.ok) {
        if (idempotencyReservationResult.replayed) {
          res.setHeader("Idempotency-Replayed", "true");
          res.status(idempotencyReservationResult.status || 201).json(
            idempotencyReservationResult.body && typeof idempotencyReservationResult.body === "object"
              ? idempotencyReservationResult.body
              : { ok: true },
          );
          return;
        }

        res.status(idempotencyReservationResult.status || 409).json({
          error: idempotencyReservationResult.error || "Duplicate request.",
          code: idempotencyReservationResult.code || "mini_idempotency_conflict",
        });
        return;
      }
      writeIdempotencyReservation = idempotencyReservationResult.reservation;

      const replayReservationResult = await reserveMiniWriteInitDataReplayKey(
        buildMiniWriteInitDataReplayKey(authResult),
        resolveMiniWriteInitDataReplayExpiresAtMs(authResult),
      );
      if (!replayReservationResult.ok) {
        res.status(replayReservationResult.status || 409).json({
          error: replayReservationResult.error || "Duplicate request.",
          code: replayReservationResult.code || "mini_init_data_replay",
        });
        return;
      }
      writeReplayReservation = replayReservationResult.reservation;

      const submission = await queueClientSubmission(
        creationResult.record,
        authResult.user,
        creationResult.miniData,
        attachmentsResult.attachments,
      );

      const successResponsePayload = {
        ok: true,
        status: submission.status,
        submissionId: submission.id,
        submittedAt: submission.submittedAt,
        attachmentsCount: submission.attachmentsCount || 0,
      };
      await commitMiniWriteIdempotencySuccess(writeIdempotencyReservation, 201, successResponsePayload);
      writeReplayReservationShouldPersist = true;
      res.status(201).json(successResponsePayload);

      enqueueMiniSubmissionTelegramNotification(
        creationResult.record,
        creationResult.miniData,
        submission,
        authResult.user,
        attachmentsResult.attachments,
      );
    } catch (error) {
      console.error("POST /api/mini/clients failed:", error);
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to submit client"));
    } finally {
      await releaseMiniWriteIdempotencyReservation(writeIdempotencyReservation);
      await releaseMiniWriteInitDataReplayKeyReservation(
        writeReplayReservation,
        writeReplayReservationShouldPersist,
      );
      await cleanupTemporaryAttachmentFiles(attachmentsResult.attachments || []);
      await cleanupTemporaryUploadFiles(req.files);
    }
  }

  function handleMiniPageGet(_req, res) {
    res.sendFile(miniHtmlPath);
  }

  return {
    handleMiniAccessPost,
    handleMiniClientsPost,
    handleMiniPageGet,
  };
}

module.exports = {
  createMiniController,
};
