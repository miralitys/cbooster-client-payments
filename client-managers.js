"use strict";

const CLIENT_MANAGER_ENDPOINT = "/api/ghl/client-managers";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_LOGOUT_PATH = "/logout";
const AUTH_LOGIN_PATH = "/login";

const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLogoutActionButton = document.querySelector("#account-logout-action");

const refreshButton = document.querySelector("#refresh-client-managers");
const statusElement = document.querySelector("#client-manager-status");
const tableBody = document.querySelector("#client-manager-table-body");

let currentAuthUser = "";

initializeAccountMenu();
initializeAuthSession();

refreshButton?.addEventListener("click", () => {
  void loadClientManagers();
});

void loadClientManagers();

async function loadClientManagers() {
  setLoadingState(true);
  setStatus("Loading client-manager table...", false);

  try {
    const response = await fetch(CLIENT_MANAGER_ENDPOINT, {
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
      throw new Error(payload.error || `Failed to load data (${response.status})`);
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    renderRows(items);
    setStatus(`Loaded ${items.length} client${items.length === 1 ? "" : "s"}.`, false);
  } catch (error) {
    renderRows([]);
    setStatus(error.message || "Failed to load client-manager table.", true);
  } finally {
    setLoadingState(false);
  }
}

function renderRows(items) {
  if (!tableBody) {
    return;
  }

  if (!items.length) {
    const row = document.createElement("tr");
    row.className = "client-manager-table__empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = "No clients found.";
    row.append(cell);
    tableBody.replaceChildren(row);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const item of items) {
    const row = document.createElement("tr");
    row.className = "client-manager-row";

    const clientCell = document.createElement("td");
    clientCell.textContent = (item?.clientName || "-").toString();

    const managerCell = document.createElement("td");
    managerCell.textContent = (item?.managersLabel || "-").toString();

    const matchedContactsCell = document.createElement("td");
    const matchedContacts = Number.parseInt(item?.matchedContacts, 10);
    matchedContactsCell.textContent = Number.isFinite(matchedContacts) ? String(matchedContacts) : "0";

    const statusCell = document.createElement("td");
    const statusValue = (item?.status || "").toString().toLowerCase();
    statusCell.textContent = formatStatusLabel(statusValue);
    statusCell.className = `client-manager-status-chip client-manager-status-chip--${statusValue || "unknown"}`;

    if (statusValue === "error" && item?.error) {
      statusCell.title = (item.error || "").toString();
    }

    row.append(clientCell, managerCell, matchedContactsCell, statusCell);
    fragment.append(row);
  }

  tableBody.replaceChildren(fragment);
}

function formatStatusLabel(statusValue) {
  if (statusValue === "assigned") {
    return "Assigned";
  }

  if (statusValue === "unassigned") {
    return "No manager";
  }

  if (statusValue === "error") {
    return "Lookup error";
  }

  return "Unknown";
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
    // Keep placeholder.
  }
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${AUTH_LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}
