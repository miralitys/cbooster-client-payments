"use strict";

const submissionsListElement = document.querySelector("#submissions-list");
const messageElement = document.querySelector("#moderation-message");
const metaElement = document.querySelector("#moderation-meta");
const statusFilterElement = document.querySelector("#status-filter");
const refreshButton = document.querySelector("#refresh-button");

let isLoading = false;

statusFilterElement?.addEventListener("change", () => {
  void loadSubmissions();
});

refreshButton?.addEventListener("click", () => {
  void loadSubmissions();
});

void loadSubmissions();

async function loadSubmissions() {
  if (isLoading) {
    return;
  }

  isLoading = true;
  setMessage("", "");
  setMeta("Loading submissions...");
  renderLoading();

  try {
    const status = (statusFilterElement?.value || "pending").trim();
    const response = await fetch(`/api/moderation/submissions?status=${encodeURIComponent(status)}&limit=200`, {
      headers: {
        Accept: "application/json",
      },
    });

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.error || `Request failed (${response.status})`);
    }

    const items = Array.isArray(body.items) ? body.items : [];
    const statusLabel = (body.status || status || "pending").toString();
    setMeta(`${items.length} submission(s), filter: ${statusLabel}`);
    renderSubmissions(items);
  } catch (error) {
    setMeta("");
    renderEmpty("Failed to load moderation submissions.");
    setMessage(error.message || "Failed to load submissions.", "error");
  } finally {
    isLoading = false;
  }
}

function renderLoading() {
  if (!submissionsListElement) {
    return;
  }

  submissionsListElement.replaceChildren(createEmptyBlock("Loading..."));
}

function renderEmpty(text) {
  if (!submissionsListElement) {
    return;
  }

  submissionsListElement.replaceChildren(createEmptyBlock(text));
}

function createEmptyBlock(text) {
  const block = document.createElement("div");
  block.className = "empty";
  block.textContent = text;
  return block;
}

function renderSubmissions(items) {
  if (!submissionsListElement) {
    return;
  }

  if (!items.length) {
    renderEmpty("No submissions for selected filter.");
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const item of items) {
    fragment.append(createSubmissionCard(item));
  }

  submissionsListElement.replaceChildren(fragment);
}

function createSubmissionCard(item) {
  const card = document.createElement("article");
  card.className = "card";
  card.dataset.submissionId = item.id || "";

  const top = document.createElement("div");
  top.className = "card-top";

  const title = document.createElement("h2");
  title.className = "card-title";
  title.textContent = resolveClientName(item);

  const statusChip = document.createElement("span");
  statusChip.className = `status-chip status-${normalizeStatus(item.status)}`;
  statusChip.textContent = normalizeStatus(item.status);

  top.append(title, statusChip);
  card.append(top);

  const details = document.createElement("div");
  details.className = "details";
  appendDetail(details, "Company", getClientValue(item, "companyName"));
  appendDetail(details, "Closed By", getClientValue(item, "closedBy"));
  appendDetail(details, "Service", getClientValue(item, "serviceType"));
  appendDetail(details, "Contract", getClientValue(item, "contractTotals"));
  appendDetail(details, "Payment 1", getClientValue(item, "payment1"));
  appendDetail(details, "Payment 1 Date", getClientValue(item, "payment1Date"));
  appendDetail(details, "Notes", getClientValue(item, "notes"));
  appendDetail(details, "Submitted", formatDateTime(item.submittedAt));
  appendDetail(details, "By", formatSubmittedBy(item.submittedBy));
  appendDetail(details, "Reviewed", formatDateTime(item.reviewedAt));
  appendDetail(details, "Reviewed By", item.reviewedBy || "-");
  appendDetail(details, "Review Note", item.reviewNote || "-");
  card.append(details);

  if (normalizeStatus(item.status) === "pending") {
    const noteInput = document.createElement("input");
    noteInput.className = "review-note";
    noteInput.placeholder = "Optional moderation note";

    const actions = document.createElement("div");
    actions.className = "actions";

    const approveButton = document.createElement("button");
    approveButton.type = "button";
    approveButton.className = "button-success";
    approveButton.textContent = "Approve";
    approveButton.addEventListener("click", async () => {
      await reviewSubmission(item.id, "approve", noteInput.value);
    });

    const rejectButton = document.createElement("button");
    rejectButton.type = "button";
    rejectButton.className = "button-danger";
    rejectButton.textContent = "Reject";
    rejectButton.addEventListener("click", async () => {
      await reviewSubmission(item.id, "reject", noteInput.value);
    });

    actions.append(approveButton, rejectButton);
    card.append(noteInput, actions);
  }

  return card;
}

function appendDetail(container, key, value) {
  const row = document.createElement("div");
  row.className = "detail-row";

  const keyElement = document.createElement("span");
  keyElement.className = "detail-key";
  keyElement.textContent = `${key}:`;

  const valueElement = document.createElement("span");
  valueElement.className = "detail-value";
  valueElement.textContent = (value || "-").toString();

  row.append(keyElement, valueElement);
  container.append(row);
}

function resolveClientName(item) {
  const name = getClientValue(item, "clientName");
  return name || "Unnamed client";
}

function getClientValue(item, key) {
  const client = item?.client;
  return (client && typeof client === "object" ? client[key] : "") || "";
}

function normalizeStatus(rawStatus) {
  const status = (rawStatus || "").toString().trim().toLowerCase();
  if (status === "approved" || status === "rejected" || status === "pending") {
    return status;
  }
  return "pending";
}

function formatSubmittedBy(submittedBy) {
  if (!submittedBy || typeof submittedBy !== "object") {
    return "-";
  }

  const username = (submittedBy.username || "").toString().trim();
  if (username) {
    return `@${username}`;
  }

  const firstName = (submittedBy.first_name || "").toString().trim();
  const lastName = (submittedBy.last_name || "").toString().trim();
  const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
  if (fullName) {
    return fullName;
  }

  const userId = (submittedBy.id || "").toString().trim();
  return userId ? `tg:${userId}` : "-";
}

function formatDateTime(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "-";
  }

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }

  return new Date(timestamp).toLocaleString();
}

async function reviewSubmission(submissionId, action, reviewNote) {
  const id = (submissionId || "").toString().trim();
  if (!id) {
    return;
  }

  const endpointAction = action === "reject" ? "reject" : "approve";
  setMessage("", "");

  try {
    const response = await fetch(
      `/api/moderation/submissions/${encodeURIComponent(id)}/${endpointAction}`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          reviewNote: (reviewNote || "").toString().trim(),
        }),
      },
    );

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.error || `Request failed (${response.status})`);
    }

    setMessage(
      endpointAction === "approve" ? "Submission approved." : "Submission rejected.",
      "success",
    );
    await loadSubmissions();
  } catch (error) {
    setMessage(error.message || "Moderation action failed.", "error");
  }
}

function setMeta(text) {
  if (!metaElement) {
    return;
  }

  metaElement.textContent = text;
}

function setMessage(text, tone) {
  if (!messageElement) {
    return;
  }

  messageElement.textContent = text;
  messageElement.className = `message ${tone || ""}`.trim();
}
