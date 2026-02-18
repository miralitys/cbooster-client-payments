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
const clientSearchInput = document.querySelector("#client-search");
const refundOnlyCheckbox = document.querySelector("#refund-only");
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
let allTransactions = [];
let lastLoadPrefix = "";

initializeAccountMenu();
initializeAuthSession();

refreshButton?.addEventListener("click", () => {
  void loadRecentQuickBooksPayments({ sync: true });
});

clientSearchInput?.addEventListener("input", () => {
  applyTransactionsFilter();
});

refundOnlyCheckbox?.addEventListener("change", () => {
  applyTransactionsFilter();
});

void loadRecentQuickBooksPayments();

async function loadRecentQuickBooksPayments(options = {}) {
  const shouldSync = Boolean(options?.sync);
  const previousItems = [...allTransactions];
  setLoadingState(true);
  setStatus(shouldSync ? "Refreshing from QuickBooks..." : "Loading saved transactions...", false);

  try {
    const response = await fetch(buildQuickBooksPaymentsEndpoint(shouldSync), {
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
      throw new Error(payload.error || `Failed to load transactions (${response.status})`);
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    allTransactions = items;
    lastLoadPrefix = buildLoadPrefixFromPayload(payload, shouldSync);
    applyTransactionsFilter();
    renderRange(payload.range);
  } catch (error) {
    if (!previousItems.length) {
      allTransactions = [];
      renderPayments([]);
      renderRange(null);
    } else {
      allTransactions = previousItems;
      applyTransactionsFilter();
    }
    setStatus(error.message || "Failed to load transactions.", true);
  } finally {
    setLoadingState(false);
  }
}

function buildQuickBooksPaymentsEndpoint(sync) {
  const todayIso = formatDateForApi(new Date());
  const query = new URLSearchParams({
    from: QUICKBOOKS_FROM_DATE,
    to: todayIso,
  });
  if (sync) {
    query.set("sync", "1");
  }
  return `/api/quickbooks/payments/recent?${query.toString()}`;
}

function applyTransactionsFilter() {
  const query = (clientSearchInput?.value || "").toString().trim();
  const showOnlyRefunds = Boolean(refundOnlyCheckbox?.checked);
  const filteredItems = filterTransactions(allTransactions, query, showOnlyRefunds);
  renderPayments(filteredItems, query, showOnlyRefunds);
  setStatus(
    buildFilterStatusMessage(allTransactions.length, filteredItems.length, query, showOnlyRefunds, lastLoadPrefix),
    false,
  );
}

function filterTransactions(items, query, showOnlyRefunds) {
  const normalizedQuery = query.toLowerCase();

  return items.filter((item) => {
    if (showOnlyRefunds && (item?.transactionType || "").toString().toLowerCase() !== "refund") {
      return false;
    }

    if (!normalizedQuery) {
      return true;
    }

    const clientName = (item?.clientName || "").toString().toLowerCase();
    return clientName.includes(normalizedQuery);
  });
}

function buildFilterStatusMessage(totalCount, visibleCount, query, showOnlyRefunds, prefix = "") {
  const normalizedQuery = query.toString().trim();
  const normalizedPrefix = prefix.toString().trim();

  let mainMessage = "";
  if (!normalizedQuery) {
    if (showOnlyRefunds) {
      mainMessage = `Showing ${visibleCount} refund${visibleCount === 1 ? "" : "s"}.`;
    } else {
      mainMessage = `Loaded ${totalCount} transaction${totalCount === 1 ? "" : "s"}.`;
    }
  } else if (visibleCount === 0) {
    mainMessage = showOnlyRefunds
      ? `No refunds found for "${normalizedQuery}".`
      : `No transactions found for "${normalizedQuery}".`;
  } else {
    mainMessage = showOnlyRefunds
      ? `Showing ${visibleCount} refund${visibleCount === 1 ? "" : "s"} for "${normalizedQuery}".`
      : `Showing ${visibleCount} of ${totalCount} transactions for "${normalizedQuery}".`;
  }

  if (!normalizedPrefix) {
    return mainMessage;
  }

  return `${normalizedPrefix} ${mainMessage}`;
}

function buildLoadPrefixFromPayload(payload, syncRequested) {
  if (!syncRequested) {
    return "Saved data:";
  }

  const syncMeta = payload?.sync && typeof payload.sync === "object" ? payload.sync : null;
  if (!syncMeta?.requested) {
    return "Saved data:";
  }

  const insertedCount = Number.parseInt(syncMeta.insertedCount, 10);
  if (Number.isFinite(insertedCount) && insertedCount > 0) {
    return `Refresh: +${insertedCount} new.`;
  }

  return "Refresh: no new.";
}

function renderPayments(items, query = "", showOnlyRefunds = false) {
  if (!tableBody) {
    return;
  }

  if (!items.length) {
    const row = document.createElement("tr");
    row.className = "quickbooks-table__empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 3;
    const normalizedQuery = query.toString().trim();
    if (normalizedQuery) {
      cell.textContent = showOnlyRefunds
        ? `No refunds found for "${normalizedQuery}".`
        : `No transactions found for "${normalizedQuery}".`;
    } else {
      cell.textContent = showOnlyRefunds ? "No refunds found for the selected period." : "No transactions found for the selected period.";
    }
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
  if (refreshButton) {
    refreshButton.disabled = isLoading;
  }

  if (clientSearchInput) {
    clientSearchInput.disabled = isLoading;
  }

  if (refundOnlyCheckbox) {
    refundOnlyCheckbox.disabled = isLoading;
  }
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
