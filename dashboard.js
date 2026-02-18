"use strict";

const MONEY_FIELDS = ["payment1", "payment2", "payment3", "payment4", "payment5", "payment6", "payment7"];

const topbarMenuToggle = document.querySelector("#topbar-menu-toggle");
const topbarMenu = document.querySelector("#topbar-menu");
const refreshSubmissionsButton = document.querySelector("#refresh-submissions-button");
const dashboardMessage = document.querySelector("#dashboard-message");
const overviewSalesValue = document.querySelector("#overview-sales-value");
const overviewReceivedValue = document.querySelector("#overview-received-value");
const overviewDebtValue = document.querySelector("#overview-debt-value");
const submissionsTableBody = document.querySelector("#submissions-table-body");
const submissionModal = document.querySelector("#submission-modal");
const submissionModalDetails = document.querySelector("#submission-modal-details");
const approvalCheckbox = document.querySelector("#approval-checkbox");
const applyModerationButton = document.querySelector("#apply-moderation-button");
const closeModalControls = [...document.querySelectorAll("[data-close-modal]")];

const moneyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

let pendingSubmissions = [];
let activeSubmission = null;
let isModerationActionRunning = false;

initializeMenu();
initializeModal();
void reloadDashboard();

async function reloadDashboard() {
  await Promise.all([loadOverviewData(), loadPendingSubmissions()]);
}

function initializeMenu() {
  if (!topbarMenuToggle || !topbarMenu) {
    return;
  }

  topbarMenuToggle.addEventListener("click", (event) => {
    event.stopPropagation();
    const shouldOpen = topbarMenu.hidden;
    topbarMenu.hidden = !shouldOpen;
    topbarMenuToggle.setAttribute("aria-expanded", String(shouldOpen));
  });

  document.addEventListener("click", (event) => {
    if (topbarMenu.hidden) {
      return;
    }

    if (topbarMenu.contains(event.target) || topbarMenuToggle.contains(event.target)) {
      return;
    }

    topbarMenu.hidden = true;
    topbarMenuToggle.setAttribute("aria-expanded", "false");
  });

  refreshSubmissionsButton?.addEventListener("click", async () => {
    await reloadDashboard();
  });
}

function initializeModal() {
  for (const control of closeModalControls) {
    control.addEventListener("click", () => {
      setModalVisibility(false);
    });
  }

  applyModerationButton?.addEventListener("click", async () => {
    if (!activeSubmission || isModerationActionRunning) {
      return;
    }

    isModerationActionRunning = true;
    applyModerationButton.disabled = true;

    try {
      if (approvalCheckbox?.checked) {
        await reviewSubmission(activeSubmission.id, "approve");
        showMessage("Клиент добавлен в общую базу.", "success");
      } else {
        await reviewSubmission(activeSubmission.id, "reject");
        showMessage("Клиент отклонен и не добавлен в базу.", "success");
      }

      setModalVisibility(false);
      await reloadDashboard();
    } catch (error) {
      showMessage(error.message || "Не удалось применить решение модерации.", "error");
    } finally {
      isModerationActionRunning = false;
      applyModerationButton.disabled = false;
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && submissionModal && !submissionModal.hidden) {
      setModalVisibility(false);
    }
  });
}

async function loadOverviewData() {
  try {
    const response = await fetch("/api/records", {
      headers: {
        Accept: "application/json",
      },
    });

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.error || `Failed to load records (${response.status})`);
    }

    const records = Array.isArray(body.records) ? body.records : [];
    const metrics = calculateOverviewMetrics(records);
    overviewSalesValue.textContent = moneyFormatter.format(metrics.totalSales);
    overviewReceivedValue.textContent = moneyFormatter.format(metrics.totalReceived);
    overviewDebtValue.textContent = moneyFormatter.format(metrics.totalDebt);
  } catch (error) {
    overviewSalesValue.textContent = "$0.00";
    overviewReceivedValue.textContent = "$0.00";
    overviewDebtValue.textContent = "$0.00";
    showMessage(error.message || "Не удалось загрузить данные overview.", "error");
  }
}

function calculateOverviewMetrics(records) {
  let totalSales = 0;
  let totalReceived = 0;
  let totalDebt = 0;

  for (const record of records) {
    const contractTotal = parseMoneyValue(record?.contractTotals) ?? 0;
    const paid = MONEY_FIELDS.reduce((sum, key) => sum + (parseMoneyValue(record?.[key]) ?? 0), 0);
    const writtenOff = isCheckboxEnabled(record?.writtenOff);
    const debt = writtenOff ? 0 : contractTotal - paid;

    totalSales += contractTotal;
    totalReceived += paid;
    totalDebt += debt;
  }

  return { totalSales, totalReceived, totalDebt };
}

function parseMoneyValue(rawValue) {
  const value = (rawValue ?? "").toString().trim();
  if (!value) {
    return null;
  }

  const normalized = value
    .replace(/[−–—]/g, "-")
    .replace(/\(([^)]+)\)/g, "-$1")
    .replace(/[^0-9.-]/g, "");
  if (!normalized || normalized === "-" || normalized === "." || normalized === "-.") {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function isCheckboxEnabled(rawValue) {
  const value = (rawValue ?? "").toString().trim().toLowerCase();
  return value === "yes" || value === "true" || value === "1";
}

async function loadPendingSubmissions() {
  try {
    const response = await fetch("/api/moderation/submissions?status=pending&limit=200", {
      headers: {
        Accept: "application/json",
      },
    });

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.error || `Failed to load submissions (${response.status})`);
    }

    pendingSubmissions = Array.isArray(body.items) ? body.items : [];
    renderPendingSubmissions();
  } catch (error) {
    pendingSubmissions = [];
    renderPendingSubmissions(error.message || "Не удалось загрузить заявки.");
  }
}

function renderPendingSubmissions(errorText = "") {
  if (!submissionsTableBody) {
    return;
  }

  if (!pendingSubmissions.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 5;
    cell.className = "empty-state";
    cell.textContent = errorText || "Новых клиентов на модерации нет.";
    row.append(cell);
    submissionsTableBody.replaceChildren(row);
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const submission of pendingSubmissions) {
    const row = document.createElement("tr");

    appendCell(row, getClientField(submission, "clientName") || "Unnamed");
    appendCell(row, getClientField(submission, "companyName") || "-");
    appendCell(row, getClientField(submission, "closedBy") || formatSubmittedBy(submission.submittedBy));
    appendCell(row, formatDateTime(submission.submittedAt));

    const actionCell = document.createElement("td");
    const openButton = document.createElement("button");
    openButton.type = "button";
    openButton.className = "open-submission-button";
    openButton.textContent = "Открыть";
    openButton.addEventListener("click", () => {
      openSubmissionModal(submission.id);
    });
    actionCell.append(openButton);
    row.append(actionCell);

    fragment.append(row);
  }

  submissionsTableBody.replaceChildren(fragment);
}

function appendCell(row, value) {
  const cell = document.createElement("td");
  cell.textContent = value;
  row.append(cell);
}

function openSubmissionModal(submissionId) {
  const submission = pendingSubmissions.find((item) => item.id === submissionId);
  if (!submission || !submissionModalDetails) {
    return;
  }

  activeSubmission = submission;
  approvalCheckbox.checked = false;

  const fragment = document.createDocumentFragment();
  appendDetail(fragment, "Client Name", getClientField(submission, "clientName"));
  appendDetail(fragment, "Closed By", getClientField(submission, "closedBy"));
  appendDetail(fragment, "Company Name", getClientField(submission, "companyName"));
  appendDetail(fragment, "Service Type", getClientField(submission, "serviceType"));
  appendDetail(fragment, "Contract Totals", getClientField(submission, "contractTotals"));
  appendDetail(fragment, "Payment 1", getClientField(submission, "payment1"));
  appendDetail(fragment, "Payment 1 Date", getClientField(submission, "payment1Date"));
  appendDetail(fragment, "Notes", getClientField(submission, "notes"));
  appendDetail(fragment, "After Result", getClientField(submission, "afterResult"));
  appendDetail(fragment, "Written Off", getClientField(submission, "writtenOff"));
  appendDetail(fragment, "Submitted At", formatDateTime(submission.submittedAt));
  appendDetail(fragment, "Submitted By", formatSubmittedBy(submission.submittedBy));

  const allClientFields = submission.client && typeof submission.client === "object" ? submission.client : {};
  const alreadyShown = new Set([
    "clientName",
    "closedBy",
    "companyName",
    "serviceType",
    "contractTotals",
    "payment1",
    "payment1Date",
    "notes",
    "afterResult",
    "writtenOff",
  ]);
  for (const [key, value] of Object.entries(allClientFields)) {
    if (alreadyShown.has(key)) {
      continue;
    }

    const textValue = (value ?? "").toString().trim();
    if (textValue) {
      appendDetail(fragment, key, textValue);
    }
  }

  submissionModalDetails.replaceChildren(fragment);
  setModalVisibility(true);
}

function appendDetail(fragment, label, value) {
  const row = document.createElement("div");
  row.className = "detail-row";

  const labelElement = document.createElement("div");
  labelElement.className = "detail-label";
  labelElement.textContent = `${label}:`;

  const valueElement = document.createElement("div");
  valueElement.className = "detail-value";
  valueElement.textContent = (value || "-").toString();

  row.append(labelElement, valueElement);
  fragment.append(row);
}

function setModalVisibility(isVisible) {
  if (!submissionModal) {
    return;
  }

  submissionModal.hidden = !isVisible;
  document.body.style.overflow = isVisible ? "hidden" : "";
  if (!isVisible) {
    activeSubmission = null;
  }
}

async function reviewSubmission(submissionId, action) {
  const endpoint = action === "approve" ? "approve" : "reject";
  const response = await fetch(`/api/moderation/submissions/${encodeURIComponent(submissionId)}/${endpoint}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify({}),
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(body.error || `Moderation request failed (${response.status})`);
  }
}

function getClientField(submission, key) {
  const client = submission?.client;
  if (!client || typeof client !== "object") {
    return "";
  }

  return (client[key] ?? "").toString().trim();
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

function showMessage(text, tone) {
  if (!dashboardMessage) {
    return;
  }

  dashboardMessage.textContent = text || "";
  dashboardMessage.className = `dashboard-message ${tone || ""}`.trim();
}
