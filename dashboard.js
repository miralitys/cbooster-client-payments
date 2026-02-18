"use strict";

const DAY_IN_MS = 24 * 60 * 60 * 1000;
const ZERO_TOLERANCE = 0.005;
const PAYMENT_PAIRS = [
  ["payment1", "payment1Date"],
  ["payment2", "payment2Date"],
  ["payment3", "payment3Date"],
  ["payment4", "payment4Date"],
  ["payment5", "payment5Date"],
  ["payment6", "payment6Date"],
  ["payment7", "payment7Date"],
];
const OVERVIEW_PERIOD_DEFAULT = "currentWeek";
const OVERVIEW_PERIOD_KEYS = {
  currentWeek: "Current Week",
  previousWeek: "Previous Week",
  currentMonth: "Current Month",
  last30Days: "Last 30 Days",
};

const topbarMenuToggle = document.querySelector("#topbar-menu-toggle");
const topbarMenu = document.querySelector("#topbar-menu");
const refreshSubmissionsButton = document.querySelector("#refresh-submissions-button");
const dashboardMessage = document.querySelector("#dashboard-message");

const overviewPanel = document.querySelector(".period-dashboard-shell");
const overviewContent = document.querySelector("#overview-content");
const toggleOverviewPanelButton = document.querySelector("#toggle-overview-panel");
const overviewCollapsedSummary = document.querySelector("#overview-collapsed-summary");
const overviewSummaryDebt = document.querySelector("#overview-summary-debt");
const overviewSummarySales = document.querySelector("#overview-summary-sales");
const overviewSummaryReceived = document.querySelector("#overview-summary-received");
const overviewSalesValue = document.querySelector("#overview-sales-value");
const overviewDebtValue = document.querySelector("#overview-debt-value");
const overviewReceivedValue = document.querySelector("#overview-received-value");
const overviewSalesContext = document.querySelector("#overview-sales-context");
const overviewReceivedContext = document.querySelector("#overview-received-context");
const overviewPeriodButtons = [...document.querySelectorAll(".overview-period-toggle")];

const submissionsTableBody = document.querySelector("#submissions-table-body");
const submissionModal = document.querySelector("#submission-modal");
const submissionModalDetails = document.querySelector("#submission-modal-details");
const submissionModalFiles = document.querySelector("#submission-modal-files");
const submissionModalFilesList = document.querySelector("#submission-modal-files-list");
const approvalCheckbox = document.querySelector("#approval-checkbox");
const applyModerationButton = document.querySelector("#apply-moderation-button");
const deleteModerationButton = document.querySelector("#delete-moderation-button");
const closeModalControls = [...document.querySelectorAll("[data-close-modal]")];

const kpiMoneyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

let pendingSubmissions = [];
let activeSubmission = null;
let isModerationActionRunning = false;
let activeOverviewPeriod = OVERVIEW_PERIOD_DEFAULT;
let cachedOverviewRecords = [];
let activeSubmissionFilesRequestId = 0;

initializeMenu();
initializeOverviewPeriodButtons();
initializeOverviewPanelToggle();
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

function initializeOverviewPeriodButtons() {
  if (!overviewPeriodButtons.length) {
    return;
  }

  for (const button of overviewPeriodButtons) {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      const nextPeriod = (button.dataset.overviewPeriod || "").trim();
      if (!Object.prototype.hasOwnProperty.call(OVERVIEW_PERIOD_KEYS, nextPeriod)) {
        return;
      }

      activeOverviewPeriod = nextPeriod;
      syncOverviewPeriodButtons();
      updatePeriodDashboard(cachedOverviewRecords);
    });
  }

  syncOverviewPeriodButtons();
}

function syncOverviewPeriodButtons() {
  for (const button of overviewPeriodButtons) {
    const periodKey = (button.dataset.overviewPeriod || "").trim();
    const isActive = periodKey === activeOverviewPeriod;

    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  }
}

function initializeOverviewPanelToggle() {
  if (!overviewPanel || !toggleOverviewPanelButton || !overviewContent) {
    return;
  }

  setOverviewPanelCollapsed(false);

  toggleOverviewPanelButton.addEventListener("click", (event) => {
    if (event.target.closest(".overview-segmented")) {
      return;
    }

    const isCollapsed = overviewPanel.classList.contains("is-collapsed");
    setOverviewPanelCollapsed(!isCollapsed);
  });

  toggleOverviewPanelButton.addEventListener("keydown", (event) => {
    if (event.target.closest(".overview-segmented")) {
      return;
    }

    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }

    event.preventDefault();
    const isCollapsed = overviewPanel.classList.contains("is-collapsed");
    setOverviewPanelCollapsed(!isCollapsed);
  });
}

function setOverviewPanelCollapsed(isCollapsed) {
  if (!overviewPanel || !toggleOverviewPanelButton || !overviewContent) {
    return;
  }

  overviewPanel.classList.toggle("is-collapsed", isCollapsed);
  overviewContent.hidden = isCollapsed;

  if (overviewCollapsedSummary) {
    overviewCollapsedSummary.hidden = !isCollapsed;
    overviewCollapsedSummary.setAttribute("aria-hidden", String(!isCollapsed));
  }

  toggleOverviewPanelButton.setAttribute("aria-expanded", String(!isCollapsed));
  toggleOverviewPanelButton.setAttribute("aria-label", isCollapsed ? "Expand overview" : "Collapse overview");
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

    if (!approvalCheckbox?.checked) {
      showMessage('Check "Add to main database" or click "Delete".', "error");
      return;
    }

    try {
      await runModerationAction("approve");
      showMessage("Client added to the main database.", "success");
    } catch (error) {
      showMessage(error.message || "Failed to apply moderation decision.", "error");
    }
  });

  deleteModerationButton?.addEventListener("click", async () => {
    if (!activeSubmission || isModerationActionRunning) {
      return;
    }

    const shouldDelete = window.confirm("Delete this submission from the moderation queue?");
    if (!shouldDelete) {
      return;
    }

    try {
      await runModerationAction("reject");
      showMessage("Submission removed from the moderation queue.", "success");
    } catch (error) {
      showMessage(error.message || "Failed to delete submission.", "error");
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && submissionModal && !submissionModal.hidden) {
      setModalVisibility(false);
    }
  });
}

async function runModerationAction(action) {
  if (!activeSubmission || isModerationActionRunning) {
    return;
  }

  isModerationActionRunning = true;
  if (applyModerationButton) {
    applyModerationButton.disabled = true;
  }
  if (deleteModerationButton) {
    deleteModerationButton.disabled = true;
  }

  try {
    await reviewSubmission(activeSubmission.id, action);
    setModalVisibility(false);
    await reloadDashboard();
  } finally {
    isModerationActionRunning = false;
    if (applyModerationButton) {
      applyModerationButton.disabled = false;
    }
    if (deleteModerationButton) {
      deleteModerationButton.disabled = false;
    }
  }
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
      throw new Error(body.error || body.details || `Failed to load records (${response.status})`);
    }

    const records = Array.isArray(body.records) ? body.records : [];
    cachedOverviewRecords = records;
    updatePeriodDashboard(records);
  } catch (error) {
    cachedOverviewRecords = [];
    updatePeriodDashboard([]);
    showMessage(error.message || "Failed to load overview data.", "error");
  }
}

function updatePeriodDashboard(recordsToMeasure = []) {
  const ranges = getPeriodDashboardRanges();
  const metricsByPeriod = {
    currentWeek: calculatePeriodMetrics(recordsToMeasure, ranges.currentWeek),
    previousWeek: calculatePeriodMetrics(recordsToMeasure, ranges.previousWeek),
    currentMonth: calculatePeriodMetrics(recordsToMeasure, ranges.currentMonth),
    last30Days: calculatePeriodMetrics(recordsToMeasure, ranges.last30Days),
  };

  const selectedMetrics = metricsByPeriod[activeOverviewPeriod] || metricsByPeriod[OVERVIEW_PERIOD_DEFAULT];
  const selectedPeriodLabel = OVERVIEW_PERIOD_KEYS[activeOverviewPeriod] || OVERVIEW_PERIOD_KEYS[OVERVIEW_PERIOD_DEFAULT];

  if (overviewSalesValue) {
    overviewSalesValue.textContent = formatKpiCurrency(selectedMetrics?.sales ?? 0);
  }

  if (overviewSalesContext) {
    overviewSalesContext.textContent = selectedPeriodLabel;
  }

  if (overviewReceivedValue) {
    overviewReceivedValue.textContent = formatKpiCurrency(selectedMetrics?.received ?? 0);
  }

  if (overviewReceivedContext) {
    overviewReceivedContext.textContent = selectedPeriodLabel;
  }

  const overallDebt = calculateOverallDebt(recordsToMeasure);
  if (overviewDebtValue) {
    overviewDebtValue.textContent = formatKpiCurrency(overallDebt);
  }

  if (overviewSummaryDebt) {
    overviewSummaryDebt.textContent = formatKpiCurrency(overallDebt);
  }

  if (overviewSummarySales) {
    overviewSummarySales.textContent = formatKpiCurrency(metricsByPeriod.currentWeek?.sales ?? 0);
  }

  if (overviewSummaryReceived) {
    overviewSummaryReceived.textContent = formatKpiCurrency(metricsByPeriod.currentWeek?.received ?? 0);
  }
}

function formatKpiCurrency(value) {
  const parsed = Number(value);
  return kpiMoneyFormatter.format(Number.isFinite(parsed) ? parsed : 0);
}

function getPeriodDashboardRanges() {
  const todayUtcStart = getCurrentUtcDayStart();
  const todayUtcDate = new Date(todayUtcStart);
  const currentWeekStart = getCurrentWeekStartUtc(todayUtcStart);
  const previousWeekStart = currentWeekStart - 7 * DAY_IN_MS;
  const previousWeekEnd = currentWeekStart - DAY_IN_MS;
  const currentMonthStart = Date.UTC(todayUtcDate.getUTCFullYear(), todayUtcDate.getUTCMonth(), 1);
  const last30DaysStart = todayUtcStart - 29 * DAY_IN_MS;

  return {
    currentWeek: {
      from: currentWeekStart,
      to: todayUtcStart,
    },
    previousWeek: {
      from: previousWeekStart,
      to: previousWeekEnd,
    },
    currentMonth: {
      from: currentMonthStart,
      to: todayUtcStart,
    },
    last30Days: {
      from: last30DaysStart,
      to: todayUtcStart,
    },
  };
}

function getCurrentUtcDayStart() {
  const now = new Date();
  return Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
}

function getCurrentWeekStartUtc(dayUtcStart) {
  const dayOfWeek = new Date(dayUtcStart).getUTCDay();
  const mondayOffset = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  return dayUtcStart - mondayOffset * DAY_IN_MS;
}

function isTimestampWithinInclusiveRange(timestamp, fromTimestamp, toTimestamp) {
  if (timestamp === null) {
    return false;
  }

  if (fromTimestamp !== null && timestamp < fromTimestamp) {
    return false;
  }

  if (toTimestamp !== null && timestamp > toTimestamp) {
    return false;
  }

  return true;
}

function calculatePeriodMetrics(recordsToMeasure, range) {
  let sales = 0;
  let received = 0;

  for (const record of recordsToMeasure) {
    const firstPaymentDate = parseDateValue(record?.payment1Date);
    const isSaleInRange = isTimestampWithinInclusiveRange(firstPaymentDate, range.from, range.to);

    if (isSaleInRange) {
      const contractAmount = parseMoneyValue(record?.contractTotals);
      if (contractAmount !== null) {
        sales += contractAmount;
      }
    }

    for (const [paymentFieldKey, paymentDateFieldKey] of PAYMENT_PAIRS) {
      const paymentDate = parseDateValue(record?.[paymentDateFieldKey]);
      if (!isTimestampWithinInclusiveRange(paymentDate, range.from, range.to)) {
        continue;
      }

      const paymentAmount = parseMoneyValue(record?.[paymentFieldKey]);
      if (paymentAmount !== null) {
        received += paymentAmount;
      }
    }
  }

  return {
    sales,
    received,
  };
}

function calculateOverallDebt(recordsToMeasure) {
  let debt = 0;

  for (const record of recordsToMeasure) {
    const futureAmount = computeFuturePaymentsAmount(record);
    if (futureAmount !== null && futureAmount > ZERO_TOLERANCE) {
      debt += futureAmount;
    }
  }

  return debt;
}

function computeFuturePaymentsAmount(record) {
  if (isRecordWrittenOff(record)) {
    return 0;
  }

  const contractTotal = parseMoneyValue(record?.contractTotals);
  if (contractTotal === null) {
    return null;
  }

  let paidTotal = 0;
  for (const [paymentFieldKey] of PAYMENT_PAIRS) {
    paidTotal += parseMoneyValue(record?.[paymentFieldKey]) ?? 0;
  }

  return contractTotal - paidTotal;
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

function parseDateValue(rawValue) {
  const value = (rawValue ?? "").toString().trim();
  if (!value) {
    return null;
  }

  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }
    return null;
  }

  const usMatch = value.match(/^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2}|\d{4})$/);
  if (usMatch) {
    const month = Number(usMatch[1]);
    const day = Number(usMatch[2]);
    let year = Number(usMatch[3]);
    if (usMatch[3].length === 2) {
      year += 2000;
    }

    if (isValidDateParts(year, month, day)) {
      return Date.UTC(year, month - 1, day);
    }
    return null;
  }

  return null;
}

function isValidDateParts(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) {
    return false;
  }

  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return false;
  }

  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

function isRecordWrittenOff(record) {
  return isCheckboxEnabled(record?.writtenOff);
}

function isCheckboxEnabled(rawValue) {
  const value = (rawValue ?? "").toString().trim().toLowerCase();
  return value === "yes" || value === "true" || value === "1" || value === "on";
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
      throw new Error(body.error || body.details || `Failed to load submissions (${response.status})`);
    }

    pendingSubmissions = Array.isArray(body.items) ? body.items : [];
    renderPendingSubmissions();
  } catch (error) {
    pendingSubmissions = [];
    renderPendingSubmissions(error.message || "Failed to load submissions.");
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
    cell.textContent = errorText || "No new clients pending moderation.";
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
    openButton.textContent = "Open";
    openButton.addEventListener("click", () => {
      void openSubmissionModal(submission.id);
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

async function openSubmissionModal(submissionId) {
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
    "id",
    "createdAt",
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
  const requestId = ++activeSubmissionFilesRequestId;
  renderSubmissionFilesLoading();
  setModalVisibility(true);
  await loadSubmissionFiles(submission.id, requestId);
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
  document.body.classList.toggle("modal-open", isVisible);
  document.body.style.overflow = isVisible ? "hidden" : "";
  if (!isVisible) {
    activeSubmissionFilesRequestId += 1;
    activeSubmission = null;
    if (submissionModalFiles) {
      submissionModalFiles.hidden = true;
    }
    if (submissionModalFilesList) {
      submissionModalFilesList.replaceChildren();
    }
  }
}

async function loadSubmissionFiles(submissionId, requestId) {
  if (!submissionModalFilesList) {
    return;
  }

  try {
    const response = await fetch(`/api/moderation/submissions/${encodeURIComponent(submissionId)}/files`, {
      headers: {
        Accept: "application/json",
      },
    });

    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(body.error || body.details || `Failed to load attachments (${response.status})`);
    }

    if (requestId !== activeSubmissionFilesRequestId) {
      return;
    }

    const items = Array.isArray(body.items) ? body.items : [];
    renderSubmissionFiles(items);
  } catch (error) {
    if (requestId !== activeSubmissionFilesRequestId) {
      return;
    }

    renderSubmissionFiles([], error.message || "Failed to load attachments.");
  }
}

function renderSubmissionFilesLoading() {
  if (!submissionModalFiles || !submissionModalFilesList) {
    return;
  }

  submissionModalFiles.hidden = false;
  const line = document.createElement("p");
  line.className = "submission-file-empty";
  line.textContent = "Loading attachments...";
  submissionModalFilesList.replaceChildren(line);
}

function renderSubmissionFiles(items, errorText = "") {
  if (!submissionModalFiles || !submissionModalFilesList) {
    return;
  }

  const normalizedItems = Array.isArray(items) ? items : [];
  if (!normalizedItems.length) {
    submissionModalFiles.hidden = false;
    const line = document.createElement("p");
    line.className = "submission-file-empty";
    line.textContent = errorText || "No attachments.";
    submissionModalFilesList.replaceChildren(line);
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const item of normalizedItems) {
    const row = document.createElement("div");
    row.className = "submission-file-row";

    const info = document.createElement("div");
    info.className = "submission-file-info";

    const name = document.createElement("div");
    name.className = "submission-file-name";
    name.textContent = (item?.fileName || "attachment").toString();

    const meta = document.createElement("div");
    meta.className = "submission-file-meta";
    const fileType = (item?.mimeType || "application/octet-stream").toString();
    meta.textContent = `${formatFileSize(item?.sizeBytes)} · ${fileType}`;
    info.append(name, meta);

    const actions = document.createElement("div");
    actions.className = "submission-file-actions";

    if (item?.canPreview && item?.previewUrl) {
      const previewLink = document.createElement("a");
      previewLink.className = "btn btn-secondary";
      previewLink.href = item.previewUrl;
      previewLink.target = "_blank";
      previewLink.rel = "noopener noreferrer";
      previewLink.textContent = "Preview";
      actions.append(previewLink);
    }

    if (item?.downloadUrl) {
      const downloadLink = document.createElement("a");
      downloadLink.className = "btn btn-secondary";
      downloadLink.href = item.downloadUrl;
      downloadLink.textContent = "Download";
      downloadLink.setAttribute("download", (item?.fileName || "attachment").toString());
      actions.append(downloadLink);
    }

    row.append(info, actions);
    fragment.append(row);
  }

  submissionModalFiles.hidden = false;
  submissionModalFilesList.replaceChildren(fragment);
}

function formatFileSize(value) {
  const bytes = Number.parseInt(value, 10);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "0 B";
  }

  const KB = 1024;
  const MB = 1024 * 1024;
  if (bytes >= MB) {
    return `${(bytes / MB).toFixed(bytes >= 10 * MB ? 0 : 1)} MB`;
  }

  if (bytes >= KB) {
    return `${Math.round(bytes / KB)} KB`;
  }

  return `${bytes} B`;
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
    throw new Error(body.error || body.details || `Moderation request failed (${response.status})`);
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
