"use strict";

const CLIENT_CONTRACTS_ENDPOINT = "/api/ghl/client-contracts";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_LOGOUT_PATH = "/logout";
const AUTH_LOGIN_PATH = "/login";
const DEFAULT_LIMIT = 10;

const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLogoutActionButton = document.querySelector("#account-logout-action");
const ownerOnlyMenuItems = [...document.querySelectorAll('[data-owner-only="true"]')];

const refreshButton = document.querySelector("#refresh-client-contracts");
const statusElement = document.querySelector("#client-contract-status");
const tableBody = document.querySelector("#client-contract-table-body");

let currentAuthUser = "";
let currentAuthLabel = "";
let currentAuthIsOwner = false;
let allItems = [];

initializeAccountMenu();
initializeAuthSession();

refreshButton?.addEventListener("click", () => {
  void loadClientContracts();
});

void loadClientContracts();

async function loadClientContracts() {
  setLoadingState(true);
  setStatus("Loading first 10 clients from database and all documents from GoHighLevel...", false);

  try {
    const endpoint = buildClientContractsEndpoint();
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
    allItems = items;
    renderRows(allItems);

    const clientsWithDocuments = allItems.filter((item) => (item?.status || "").toString().toLowerCase() === "found").length;
    const totalDocuments = allItems.reduce((total, item) => {
      const documents = Array.isArray(item?.documents) ? item.documents.length : 0;
      if (Number.isFinite(item?.documentsCount)) {
        return total + Number(item.documentsCount);
      }
      return total + documents;
    }, 0);
    setStatus(`Loaded ${allItems.length} clients. Found ${totalDocuments} documents for ${clientsWithDocuments} clients.`, false);
  } catch (error) {
    allItems = [];
    renderRows([]);
    setStatus(error.message || "Failed to load client document table.", true);
  } finally {
    setLoadingState(false);
  }
}

function buildClientContractsEndpoint() {
  const url = new URL(CLIENT_CONTRACTS_ENDPOINT, window.location.origin);
  url.searchParams.set("limit", String(DEFAULT_LIMIT));
  return `${url.pathname}${url.search}`;
}

function renderRows(items) {
  if (!tableBody) {
    return;
  }

  if (!Array.isArray(items) || !items.length) {
    const row = document.createElement("tr");
    row.className = "client-contract-table__empty-row";
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
    row.className = "client-contract-row";

    const clientCell = document.createElement("td");
    clientCell.textContent = (item?.clientName || "-").toString();

    const contactCell = document.createElement("td");
    const contactName = (item?.contactName || "-").toString();
    const matchedContacts = Number.parseInt(item?.matchedContacts, 10);
    const matchedContactsLabel = Number.isFinite(matchedContacts) ? matchedContacts : 0;
    contactCell.textContent = `${contactName} (${matchedContactsLabel})`;

    const contractCell = document.createElement("td");
    contractCell.className = "client-contract-contract-cell";
    const documents = Array.isArray(item?.documents) ? item.documents : [];
    if (!documents.length) {
      contractCell.textContent = (item?.contractTitle || "-").toString() || "-";
    } else {
      const list = document.createElement("ul");
      list.className = "client-contract-documents-list";

      for (const documentItem of documents) {
        const listItem = document.createElement("li");
        listItem.className = "client-contract-document-item";

        const documentTitle = (documentItem?.title || "Document").toString();
        const documentUrl = (documentItem?.url || "").toString().trim();
        if (documentUrl) {
          const documentLink = document.createElement("a");
          documentLink.href = documentUrl;
          documentLink.target = "_blank";
          documentLink.rel = "noopener noreferrer";
          documentLink.className = "client-contract-link";
          documentLink.textContent = documentTitle;
          listItem.append(documentLink);
        } else {
          const documentLabel = document.createElement("span");
          documentLabel.className = "client-contract-document-label";
          documentLabel.textContent = documentTitle;
          listItem.append(documentLabel);
        }

        const metaParts = [];
        const sourceValue = (documentItem?.source || "").toString().trim();
        if (sourceValue) {
          metaParts.push(sourceValue);
        }

        if (documentItem?.isContractMatch) {
          metaParts.push("contract");
        }

        const noteValue = (documentItem?.snippet || "").toString().trim();
        if (noteValue) {
          metaParts.push(noteValue);
        }

        if (metaParts.length) {
          const sourceHint = document.createElement("span");
          sourceHint.className = "client-contract-source";
          sourceHint.textContent = metaParts.join(" | ");
          listItem.append(sourceHint);
        }

        list.append(listItem);
      }

      contractCell.append(list);
    }

    const statusCell = document.createElement("td");
    const statusValue = (item?.status || "").toString().toLowerCase();
    statusCell.textContent = formatStatusLabel(statusValue);
    statusCell.className = `client-contract-status-chip client-contract-status-chip--${statusValue || "unknown"}`;

    if (statusValue === "error" && item?.error) {
      statusCell.title = (item.error || "").toString();
    }

    row.append(clientCell, contactCell, contractCell, statusCell);
    fragment.append(row);
  }

  tableBody.replaceChildren(fragment);
}

function formatStatusLabel(statusValue) {
  if (statusValue === "found") {
    return "Documents found";
  }

  if (statusValue === "possible") {
    return "Possible match";
  }

  if (statusValue === "not_found") {
    return "Not found";
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
    // Keep placeholder user label.
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
