"use strict";

function createSupportController(dependencies = {}) {
  const {
    supportService,
    buildPublicErrorPayload,
    resolveDbHttpStatus,
    supportEventBus,
    sanitizeTextValue,
  } = dependencies;

  if (!supportService) {
    throw new Error("createSupportController requires supportService.");
  }

  const sanitize =
    typeof sanitizeTextValue === "function"
      ? sanitizeTextValue
      : (value, maxLength = 4000) => String(value ?? "").trim().slice(0, maxLength);

  function emitEvent(type, payload) {
    if (!supportEventBus || typeof supportEventBus.emit !== "function") {
      return;
    }
    supportEventBus.emit("support-event", {
      type,
      payload,
      at: new Date().toISOString(),
    });
  }

  async function handleSupportRequestsGet(req, res) {
    try {
      const statuses = sanitize(req.query?.status, 400)
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
      const priorities = sanitize(req.query?.priority, 400)
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);

      const result = await supportService.listRequestsForProfile(req.webAuthProfile, {
        statuses,
        priorities,
        assignedTo: sanitize(req.query?.assigned_to, 200),
        createdBy: sanitize(req.query?.created_by, 200),
        limit: req.query?.limit,
        offset: req.query?.offset,
      });
      res.json({
        ok: true,
        items: result,
      });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load support requests"));
    }
  }

  async function handleSupportRequestGet(req, res) {
    try {
      const requestId = sanitize(req.params?.id, 180);
      const result = await supportService.getRequestDetails(req.webAuthProfile, requestId);
      if (!result) {
        res.status(404).json({ error: "Support request not found." });
        return;
      }
      res.json({ ok: true, item: result });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load support request"));
    }
  }

  async function handleSupportRequestPost(req, res) {
    try {
      const result = await supportService.createRequest(req.webAuthProfile, req.body, req.files || []);
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      emitEvent("created", { requestId: result.request?.id || "" });
      res.status(201).json({ ok: true, item: result.request });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to create support request"));
    }
  }

  async function handleSupportRequestPatch(req, res) {
    try {
      const requestId = sanitize(req.params?.id, 180);
      const result = await supportService.updateRequest(req.webAuthProfile, requestId, req.body || {});
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      emitEvent("updated", { requestId });
      res.json({ ok: true, item: result.request });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to update support request"));
    }
  }

  async function handleSupportRequestMoveToPost(req, res) {
    try {
      const requestId = sanitize(req.params?.id, 180);
      const result = await supportService.moveRequestStatus(req.webAuthProfile, requestId, req.body || {}, req.files || []);
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      const targetStatus = sanitize(req.body?.status, 80);
      if (targetStatus) {
        emitEvent("status", { requestId, status: targetStatus });
      } else {
        emitEvent("status", { requestId });
      }
      res.json({ ok: true, item: result.request });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to update support status"));
    }
  }

  async function handleSupportRequestAttachmentsPost(req, res) {
    try {
      const requestId = sanitize(req.params?.id, 180);
      const result = await supportService.addRequestAttachment(req.webAuthProfile, requestId, req.files || []);
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      emitEvent("attachments", { requestId });
      res.json({ ok: true, items: result.attachments || [] });
    } catch (error) {
      res
        .status(resolveDbHttpStatus(error))
        .json(buildPublicErrorPayload(error, "Failed to upload support attachments"));
    }
  }

  async function handleSupportRequestCommentPost(req, res) {
    try {
      const requestId = sanitize(req.params?.id, 180);
      const result = await supportService.addComment(req.webAuthProfile, requestId, req.body?.comment);
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      emitEvent("comment", { requestId });
      res.json({ ok: true });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to add comment"));
    }
  }

  async function handleSupportReportsGet(req, res) {
    try {
      const period = sanitize(req.query?.period, 40) || "week";
      const now = new Date();
      const end = new Date(now);
      let start = new Date(now);

      if (period === "day") {
        start.setHours(0, 0, 0, 0);
      } else if (period === "month") {
        start.setDate(1);
        start.setHours(0, 0, 0, 0);
      } else {
        const day = start.getDay();
        const diff = (day + 6) % 7;
        start.setDate(start.getDate() - diff);
        start.setHours(0, 0, 0, 0);
      }

      const filters = {
        from: start.toISOString(),
        to: end.toISOString(),
        priorities: sanitize(req.query?.priority, 400)
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean),
        assignedTo: sanitize(req.query?.assigned_to, 200),
        createdBy: sanitize(req.query?.created_by, 200),
      };

      const result = await supportService.getReports(req.webAuthProfile, filters);
      if (!result.ok) {
        res.status(result.status || 400).json({ error: result.error });
        return;
      }
      res.json({ ok: true, report: result.report, period });
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to load support reports"));
    }
  }

  async function handleSupportAttachmentGet(req, res) {
    try {
      const attachmentId = sanitize(req.params?.id, 180);
      const result = await supportService.getAttachmentDownload(req.webAuthProfile, attachmentId);
      if (!result.ok) {
        res.status(result.status || 404).json({ error: result.error });
        return;
      }

      const attachment = result.attachment;
      if (attachment.storage_url) {
        res.redirect(302, attachment.storage_url);
        return;
      }

      const content = attachment.content;
      if (!content) {
        res.status(404).json({ error: "Attachment content not available." });
        return;
      }

      res
        .status(200)
        .type(attachment.mime_type || "application/octet-stream")
        .setHeader(
          "Content-Disposition",
          `attachment; filename="${sanitize(attachment.file_name, 260) || "attachment"}"`,
        )
        .send(content);
    } catch (error) {
      res.status(resolveDbHttpStatus(error)).json(buildPublicErrorPayload(error, "Failed to download attachment"));
    }
  }

  function handleSupportStreamGet(req, res) {
    if (!supportEventBus || typeof supportEventBus.on !== "function") {
      res.status(503).json({ error: "Support stream is unavailable." });
      return;
    }

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write("event: support\n");
    res.write(`data: ${JSON.stringify({ type: "connected", at: new Date().toISOString() })}\n\n`);

    const listener = (event) => {
      res.write("event: support\n");
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    };
    supportEventBus.on("support-event", listener);

    req.on("close", () => {
      supportEventBus.removeListener("support-event", listener);
    });
  }

  return {
    handleSupportRequestsGet,
    handleSupportRequestGet,
    handleSupportRequestPost,
    handleSupportRequestPatch,
    handleSupportRequestMoveToPost,
    handleSupportRequestAttachmentsPost,
    handleSupportRequestCommentPost,
    handleSupportReportsGet,
    handleSupportAttachmentGet,
    handleSupportStreamGet,
  };
}

module.exports = {
  createSupportController,
};
