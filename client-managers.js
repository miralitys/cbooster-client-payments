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
const ownerOnlyMenuItems = [...document.querySelectorAll('[data-owner-only="true"]')];

const refreshButton = document.querySelector("#refresh-client-managers");
const totalRefreshButton = document.querySelector("#total-refresh-client-managers");
const noManagerOnlyCheckbox = document.querySelector("#client-managers-no-manager-only");
const statusElement = document.querySelector("#client-manager-status");
const tableBody = document.querySelector("#client-manager-table-body");

let currentAuthUser = "";
let currentAuthLabel = "";
let currentAuthIsOwner = false;
let allClientManagerItems = [];

initializeAccountMenu();
initializeAuthSession();

refreshButton?.addEventListener("click", () => {
  void loadClientManagers("incremental");
});

totalRefreshButton?.addEventListener("click", () => {
  void loadClientManagers("full");
});

noManagerOnlyCheckbox?.addEventListener("change", () => {
  const visibleItems = applyRowsFilter();
  const totalItems = allClientManagerItems.length;
  const isNoManagerOnly = Boolean(noManagerOnlyCheckbox?.checked);
  if (!totalItems) {
    setStatus(
      isNoManagerOnly ? "No manager filter is enabled. Press Refresh to load data." : "Press Refresh to load data.",
      false,
    );
    return;
  }

  if (isNoManagerOnly) {
    setStatus(
      `Showing ${visibleItems.length} client${visibleItems.length === 1 ? "" : "s"} without manager out of ${totalItems}.`,
      false,
    );
    return;
  }

  setStatus(`Showing all ${totalItems} client${totalItems === 1 ? "" : "s"}.`, false);
});

async function loadClientManagers(refreshMode = "none") {
  setLoadingState(true);
  setStatus(
    refreshMode === "full"
      ? "Running total refresh for all clients..."
      : "Refreshing only new clients...",
    false,
  );

  try {
    const endpoint = buildClientManagerEndpoint(refreshMode);
    const response = await fetch(endpoint, {
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
    allClientManagerItems = items;
    const visibleItems = applyRowsFilter();
    const isNoManagerOnly = Boolean(noManagerOnlyCheckbox?.checked);
    const refreshedCount = Number.parseInt(payload?.refresh?.refreshedClientsCount, 10);
    const refreshedLabel = Number.isFinite(refreshedCount) ? refreshedCount : 0;
    const filterLabel = isNoManagerOnly
      ? ` Showing ${visibleItems.length} without manager out of ${items.length}.`
      : "";
    setStatus(
      `Loaded ${items.length} client${items.length === 1 ? "" : "s"}. Refreshed: ${refreshedLabel}.${filterLabel}`,
      false,
    );
  } catch (error) {
    allClientManagerItems = [];
    applyRowsFilter();
    setStatus(error.message || "Failed to load client-manager table.", true);
  } finally {
    setLoadingState(false);
  }
}

function applyRowsFilter() {
  const items = Array.isArray(allClientManagerItems) ? allClientManagerItems : [];
  const showNoManagerOnly = Boolean(noManagerOnlyCheckbox?.checked);
  const filteredItems = showNoManagerOnly ? items.filter(isNoManagerItem) : items;
  const emptyText = showNoManagerOnly ? "No clients without manager." : "No clients found.";
  renderRows(filteredItems, emptyText);
  return filteredItems;
}

function isNoManagerItem(item) {
  const statusValue = (item?.status || "").toString().trim().toLowerCase();
  if (statusValue === "unassigned") {
    return true;
  }

  const managers = Array.isArray(item?.managers)
    ? item.managers.map((value) => (value || "").toString().trim()).filter(Boolean)
    : [];
  if (!managers.length) {
    return true;
  }

  const managersLabel = (item?.managersLabel || "").toString().trim().toLowerCase();
  return !managersLabel || managersLabel === "-" || managersLabel === "unassigned";
}

function buildClientManagerEndpoint(refreshMode) {
  const url = new URL(CLIENT_MANAGER_ENDPOINT, window.location.origin);
  if (refreshMode === "incremental" || refreshMode === "full") {
    url.searchParams.set("refresh", refreshMode);
  }
  return `${url.pathname}${url.search}`;
}

function renderRows(items, emptyText = "No clients found.") {
  if (!tableBody) {
    return;
  }

  if (!items.length) {
    const row = document.createElement("tr");
    row.className = "client-manager-table__empty-row";
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = emptyText;
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

  if (totalRefreshButton) {
    totalRefreshButton.disabled = isLoading;
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
  currentAuthLabel = "";
  currentAuthIsOwner = false;
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
    if (!currentAuthUser) {
      accountMenuUser.textContent = "User: -";
    } else {
      accountMenuUser.textContent = currentAuthLabel
        ? `User: ${currentAuthUser} (${currentAuthLabel})`
        : `User: ${currentAuthUser}`;
    }
  }

  syncOwnerOnlyMenuItems(currentAuthIsOwner);
}

function syncOwnerOnlyMenuItems(isOwner) {
  for (const item of ownerOnlyMenuItems) {
    item.hidden = !isOwner;
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
    const roleName = (payload?.user?.roleName || "").toString().trim();
    const departmentName = (payload?.user?.departmentName || "").toString().trim();
    const isOwner = Boolean(payload?.user?.isOwner);
    currentAuthUser = username || "";
    currentAuthLabel = buildAuthLabel(roleName, departmentName);
    currentAuthIsOwner = isOwner;
    syncAuthUi();
  } catch {
    // Keep placeholder.
  }
}

function buildAuthLabel(roleName, departmentName) {
  const normalizedRoleName = roleName.toString().trim();
  const normalizedDepartmentName = departmentName.toString().trim();
  if (normalizedRoleName && normalizedDepartmentName) {
    return `${normalizedRoleName} | ${normalizedDepartmentName}`;
  }
  return normalizedRoleName || normalizedDepartmentName || "";
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${AUTH_LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}
