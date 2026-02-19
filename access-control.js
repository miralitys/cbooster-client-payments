"use strict";

const LOGIN_PATH = "/login";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_ACCESS_MODEL_ENDPOINT = "/api/auth/access-model";
const AUTH_LOGOUT_PATH = "/logout";

const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLogoutActionButton = document.querySelector("#account-logout-action");
const statusElement = document.querySelector("#access-control-status");
const currentUserElement = document.querySelector("#access-control-current-user");
const departmentsElement = document.querySelector("#access-control-departments");

let currentAuthUser = "";
let currentAuthLabel = "";

initializeAccountMenu();
initializeAuthSession();
void loadAccessModel();

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

function signOutCurrentUser() {
  window.location.href = AUTH_LOGOUT_PATH;
}

function syncAuthUi() {
  if (!accountMenuUser) {
    return;
  }

  if (!currentAuthUser) {
    accountMenuUser.textContent = "User: -";
    return;
  }

  accountMenuUser.textContent = currentAuthLabel
    ? `User: ${currentAuthUser} (${currentAuthLabel})`
    : `User: ${currentAuthUser}`;
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
    currentAuthUser = username || "";
    currentAuthLabel = buildUserLabel(roleName, departmentName);
    syncAuthUi();
  } catch {
    // Keep default placeholder.
  }
}

async function loadAccessModel() {
  setStatus("Loading access model...", false);

  try {
    const response = await fetch(AUTH_ACCESS_MODEL_ENDPOINT, {
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
      throw new Error(payload.error || `Failed to load access model (${response.status})`);
    }

    renderCurrentUserCard(payload.user, payload.permissions);
    renderDepartments(payload?.accessModel?.departments);
    setStatus("Access model loaded.", false);
  } catch (error) {
    renderCurrentUserCard(null, null);
    renderDepartments([]);
    setStatus(error.message || "Failed to load access model.", true);
  }
}

function renderCurrentUserCard(user, permissions) {
  if (!currentUserElement) {
    return;
  }

  const username = (user?.username || "").toString().trim();
  const displayName = (user?.displayName || "").toString().trim();
  const roleName = (user?.roleName || "").toString().trim() || "-";
  const departmentName = (user?.departmentName || "").toString().trim() || "-";
  const isOwner = Boolean(user?.isOwner);
  const permissionsCount = permissions && typeof permissions === "object"
    ? Object.values(permissions).filter((value) => Boolean(value)).length
    : 0;

  const lines = [
    { label: "Username", value: username || "-" },
    { label: "Display Name", value: displayName || "-" },
    { label: "Role", value: roleName },
    { label: "Department", value: departmentName },
    { label: "Access Level", value: isOwner ? "Owner (full access)" : "Department access" },
    { label: "Enabled Permissions", value: String(permissionsCount) },
  ];

  const fragment = document.createDocumentFragment();
  for (const line of lines) {
    const row = document.createElement("div");
    row.className = "access-control-current-user__row";

    const label = document.createElement("span");
    label.className = "access-control-current-user__label";
    label.textContent = `${line.label}:`;

    const value = document.createElement("span");
    value.className = "access-control-current-user__value";
    value.textContent = line.value;

    row.append(label, value);
    fragment.append(row);
  }

  currentUserElement.replaceChildren(fragment);
}

function renderDepartments(departments) {
  if (!departmentsElement) {
    return;
  }

  const items = Array.isArray(departments) ? departments : [];
  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "access-control-empty";
    empty.textContent = "No department access data.";
    departmentsElement.replaceChildren(empty);
    return;
  }

  const fragment = document.createDocumentFragment();

  for (const department of items) {
    const card = document.createElement("article");
    card.className = "access-control-department-card";

    const title = document.createElement("h3");
    title.className = "access-control-department-card__title";
    title.textContent = (department?.name || "Department").toString();
    card.append(title);

    const table = document.createElement("table");
    table.className = "access-control-department-card__table";

    const tableHead = document.createElement("thead");
    const headRow = document.createElement("tr");
    const roleHead = document.createElement("th");
    roleHead.textContent = "Role";
    const membersHead = document.createElement("th");
    membersHead.textContent = "Assigned Users";
    headRow.append(roleHead, membersHead);
    tableHead.append(headRow);
    table.append(tableHead);

    const tableBody = document.createElement("tbody");
    const roles = Array.isArray(department?.roles) ? department.roles : [];
    for (const role of roles) {
      const row = document.createElement("tr");

      const roleCell = document.createElement("td");
      roleCell.textContent = (role?.name || "-").toString();

      const membersCell = document.createElement("td");
      const members = Array.isArray(role?.members) ? role.members : [];
      if (!members.length) {
        membersCell.textContent = "Unassigned";
      } else {
        membersCell.textContent = members
          .map((member) => {
            const displayName = (member?.displayName || "").toString().trim();
            const username = (member?.username || "").toString().trim();
            return displayName || username || "-";
          })
          .join(", ");
      }

      row.append(roleCell, membersCell);
      tableBody.append(row);
    }

    table.append(tableBody);
    card.append(table);
    fragment.append(card);
  }

  departmentsElement.replaceChildren(fragment);
}

function buildUserLabel(roleName, departmentName) {
  const normalizedRoleName = roleName.toString().trim();
  const normalizedDepartmentName = departmentName.toString().trim();
  if (normalizedRoleName && normalizedDepartmentName) {
    return `${normalizedRoleName} | ${normalizedDepartmentName}`;
  }
  return normalizedRoleName || normalizedDepartmentName || "";
}

function setStatus(message, isError) {
  if (!statusElement) {
    return;
  }

  statusElement.textContent = message || "";
  statusElement.classList.toggle("error", Boolean(isError));
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}
