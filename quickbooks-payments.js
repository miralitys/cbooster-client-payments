"use strict";

const QUICKBOOKS_FROM_DATE = "2026-01-01";
const LOGIN_PATH = "/login";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_LOGOUT_PATH = "/logout";

const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLogoutActionButton = document.querySelector("#account-logout-action");
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

let currentAuthUser = "";

initializeAccountMenu();
initializeAuthSession();

refreshButton?.addEventListener("click", () => {
  void loadRecentQuickBooksPayments();
});

void loadRecentQuickBooksPayments();

async function loadRecentQuickBooksPayments() {
  setLoadingState(true);
  setStatus("Loading payments...", false);

  try {
    const response = await fetch(buildQuickBooksPaymentsEndpoint(), {
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

function buildQuickBooksPaymentsEndpoint() {
  const todayIso = formatDateForApi(new Date());
  const query = new URLSearchParams({
    from: QUICKBOOKS_FROM_DATE,
    to: todayIso,
  });
  return `/api/quickbooks/payments/recent?${query.toString()}`;
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

function formatDateForApi(value) {
  const date = value instanceof Date ? value : new Date(value);
  const year = String(date.getUTCFullYear());
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}

function initializeAccountMenu() {
  if (!accountMenu || !accountMenuToggleButton || !accountMenuPanel) {
    return;
  }

  accountMenuToggleButton.addEventListener("click", (event) => {
    event.stopPropagation();
    const isOpen = accountMenu.classList.contains("is-open");
    setAccountMenuOpen(!isOpen);
  });

  accountLogoutActionButton?.addEventListener("click", () => {
    setAccountMenuOpen(false);
    signOutCurrentUser();
  });

  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Node)) {
      return;
    }

    if (!accountMenu.contains(event.target)) {
      setAccountMenuOpen(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setAccountMenuOpen(false);
    }
  });
}

function initializeAuthSession() {
  currentAuthUser = "";
  syncAuthUi();
  void hydrateAuthSessionFromServer();
}

function setAccountMenuOpen(isOpen) {
  if (!accountMenu || !accountMenuToggleButton || !accountMenuPanel) {
    return;
  }

  accountMenu.classList.toggle("is-open", isOpen);
  accountMenuPanel.hidden = !isOpen;
  accountMenuToggleButton.setAttribute("aria-expanded", String(isOpen));
  accountMenuToggleButton.setAttribute("aria-label", isOpen ? "Close account menu" : "Open account menu");
}

function syncAuthUi() {
  if (accountMenuUser) {
    accountMenuUser.textContent = currentAuthUser ? `User: ${currentAuthUser}` : "User: -";
  }
}

function signOutCurrentUser() {
  window.location.href = AUTH_LOGOUT_PATH;
}

async function hydrateAuthSessionFromServer() {
  try {
    const response = await fetch(AUTH_SESSION_ENDPOINT, {
      headers: {
        Accept: "application/json",
      },
    });

    if (response.status === 401) {
      redirectToLogin();
      return;
    }

    if (!response.ok) {
      return;
    }

    const payload = await response.json().catch(() => null);
    const username = (payload?.user?.username || "").toString().trim();
    currentAuthUser = username || "";
    syncAuthUi();
  } catch {
    // Keep default placeholder.
  }
}
