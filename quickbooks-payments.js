"use strict";

const QUICKBOOKS_RECENT_DAYS = 3;
const QUICKBOOKS_PAYMENTS_ENDPOINT = `/api/quickbooks/payments/recent?days=${QUICKBOOKS_RECENT_DAYS}`;
const LOGIN_PATH = "/login";

const refreshButton = document.querySelector("#refresh-button");
const statusElement = document.querySelector("#status");
const rangeElement = document.querySelector("#range");
const tableBody = document.querySelector("#payments-body");

const usdFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

refreshButton?.addEventListener("click", () => {
  void loadRecentQuickBooksPayments();
});

void loadRecentQuickBooksPayments();

async function loadRecentQuickBooksPayments() {
  setLoadingState(true);
  setStatus("Loading payments...", false);

  try {
    const response = await fetch(QUICKBOOKS_PAYMENTS_ENDPOINT, {
      headers: {
        Accept: "application/json",
      },
    });

    if (response.status === 401) {
      redirectToLogin();
      return;
    }

    const payload = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(payload.error || `Failed to load payments (${response.status})`);
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    renderPayments(items);
    renderRange(payload.range);
    setStatus(`Loaded ${items.length} payment${items.length === 1 ? "" : "s"}.`, false);
  } catch (error) {
    renderPayments([]);
    renderRange(null);
    setStatus(error.message || "Failed to load payments.", true);
  } finally {
    setLoadingState(false);
  }
}

function renderPayments(items) {
  if (!tableBody) {
    return;
  }

  if (!items.length) {
    const row = document.createElement("tr");
    row.className = "quickbooks-table__empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 3;
    cell.textContent = "No payments found for the selected period.";
    row.append(cell);
    tableBody.replaceChildren(row);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const item of items) {
    const row = document.createElement("tr");

    const clientNameCell = document.createElement("td");
    clientNameCell.textContent = (item?.clientName || "Unknown client").toString();

    const paymentAmountCell = document.createElement("td");
    paymentAmountCell.className = "amount";
    paymentAmountCell.textContent = formatUsd(item?.paymentAmount);

    const paymentDateCell = document.createElement("td");
    paymentDateCell.textContent = formatDate(item?.paymentDate);

    row.append(clientNameCell, paymentAmountCell, paymentDateCell);
    fragment.append(row);
  }

  tableBody.replaceChildren(fragment);
}

function renderRange(range) {
  if (!rangeElement) {
    return;
  }

  const from = (range?.from || "").toString().trim();
  const to = (range?.to || "").toString().trim();
  if (!from || !to) {
    rangeElement.textContent = "";
    return;
  }

  rangeElement.textContent = `Range: ${from} -> ${to}`;
}

function setStatus(message, isError) {
  if (!statusElement) {
    return;
  }

  statusElement.textContent = message || "";
  statusElement.classList.toggle("error", Boolean(isError));
}

function setLoadingState(isLoading) {
  if (!refreshButton) {
    return;
  }

  refreshButton.disabled = isLoading;
}

function formatUsd(value) {
  const parsed = Number(value);
  return usdFormatter.format(Number.isFinite(parsed) ? parsed : 0);
}

function formatDate(rawValue) {
  const value = (rawValue || "").toString().trim();
  if (!value) {
    return "-";
  }

  const plainDateMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (plainDateMatch) {
    const month = plainDateMatch[2];
    const day = plainDateMatch[3];
    const year = plainDateMatch[1];
    return `${month}/${day}/${year}`;
  }

  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    return value;
  }

  return new Date(timestamp).toLocaleDateString("en-US");
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}
