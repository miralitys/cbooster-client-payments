"use strict";

const LOGIN_PATH = "/login";
const AUTH_SESSION_ENDPOINT = "/api/auth/session";
const AUTH_ACCESS_MODEL_ENDPOINT = "/api/auth/access-model";
const AUTH_CREATE_USER_ENDPOINT = "/api/auth/users";
const AUTH_LOGOUT_PATH = "/logout";

const ROLE_OPTIONS_BY_DEPARTMENT = {
  accounting: [
    { id: "department_head", name: "Department Head" },
    { id: "manager", name: "Manager" },
  ],
  client_service: [
    { id: "department_head", name: "Department Head" },
    { id: "middle_manager", name: "Middle Manager" },
    { id: "manager", name: "Manager" },
  ],
  sales: [
    { id: "department_head", name: "Department Head" },
    { id: "manager", name: "Manager" },
  ],
  collection: [
    { id: "department_head", name: "Department Head" },
    { id: "manager", name: "Manager" },
  ],
};

const accountMenu = document.querySelector("#account-menu");
const accountMenuToggleButton = document.querySelector("#account-menu-toggle");
const accountMenuPanel = document.querySelector("#account-menu-panel");
const accountMenuUser = document.querySelector("#account-menu-user");
const accountLogoutActionButton = document.querySelector("#account-logout-action");
const ownerOnlyMenuItems = [...document.querySelectorAll('[data-owner-only="true"]')];

const statusElement = document.querySelector("#user-registration-status");
const formElement = document.querySelector("#user-registration-form");
const usernameInput = document.querySelector("#new-user-username");
const passwordInput = document.querySelector("#new-user-password");
const displayNameInput = document.querySelector("#new-user-display-name");
const departmentSelect = document.querySelector("#new-user-department");
const roleSelect = document.querySelector("#new-user-role");
const teamField = document.querySelector("#new-user-team-field");
const teamInput = document.querySelector("#new-user-team");
const createUserButton = document.querySelector("#create-user-button");
const usersTableBody = document.querySelector("#user-registration-users-body");

let currentAuthUser = "";
let currentAuthLabel = "";
let currentAuthIsOwner = false;

initializeAccountMenu();
initializeAuthSession();
initializeForm();
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
  currentAuthIsOwner = false;
  syncAuthUi();
  void hydrateAuthSessionFromServer();
}

function initializeForm() {
  renderRoleOptions(departmentSelect?.value || "accounting");
  syncTeamFieldVisibility();

  departmentSelect?.addEventListener("change", () => {
    renderRoleOptions(departmentSelect?.value || "");
  });

  roleSelect?.addEventListener("change", () => {
    syncTeamFieldVisibility();
  });

  formElement?.addEventListener("submit", async (event) => {
    event.preventDefault();
    await createUser();
  });
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
    currentAuthLabel = buildUserLabel(roleName, departmentName);
    currentAuthIsOwner = isOwner;
    syncAuthUi();
  } catch {
    // Keep default placeholder.
  }
}

async function loadAccessModel() {
  setStatus("Loading users...", false);

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
      throw new Error(payload.error || `Failed to load users (${response.status})`);
    }

    const users = Array.isArray(payload?.accessModel?.users) ? payload.accessModel.users : [];
    renderUsers(users);
    setStatus(`Loaded ${users.length} user${users.length === 1 ? "" : "s"}.`, false);
  } catch (error) {
    renderUsers([]);
    setStatus(error.message || "Failed to load users.", true);
  }
}

async function createUser() {
  const username = (usernameInput?.value || "").toString().trim();
  const password = (passwordInput?.value || "").toString();
  const displayName = (displayNameInput?.value || "").toString().trim();
  const departmentId = (departmentSelect?.value || "").toString().trim();
  const roleId = (roleSelect?.value || "").toString().trim();
  const teamUsernames = (teamInput?.value || "")
    .toString()
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);

  setLoadingState(true);
  setStatus("Creating user...", false);

  try {
    const response = await fetch(AUTH_CREATE_USER_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        username,
        password,
        displayName,
        departmentId,
        roleId,
        teamUsernames,
      }),
    });

    if (response.status === 401) {
      redirectToLogin();
      return;
    }

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || `Failed to create user (${response.status})`);
    }

    if (passwordInput) {
      passwordInput.value = "";
    }
    if (usernameInput) {
      usernameInput.value = "";
    }
    if (displayNameInput) {
      displayNameInput.value = "";
    }
    if (teamInput) {
      teamInput.value = "";
    }

    setStatus(`User "${payload?.item?.username || username}" created.`, false);
    await loadAccessModel();
  } catch (error) {
    setStatus(error.message || "Failed to create user.", true);
  } finally {
    setLoadingState(false);
  }
}

function renderRoleOptions(departmentId) {
  if (!roleSelect) {
    return;
  }

  const options = ROLE_OPTIONS_BY_DEPARTMENT[departmentId] || ROLE_OPTIONS_BY_DEPARTMENT.accounting;
  const previousValue = (roleSelect.value || "").toString();
  const fragment = document.createDocumentFragment();

  for (const option of options) {
    const element = document.createElement("option");
    element.value = option.id;
    element.textContent = option.name;
    fragment.append(element);
  }

  roleSelect.replaceChildren(fragment);
  if (options.some((option) => option.id === previousValue)) {
    roleSelect.value = previousValue;
  }

  syncTeamFieldVisibility();
}

function syncTeamFieldVisibility() {
  const isMiddleManager = (roleSelect?.value || "").toString().trim() === "middle_manager";
  if (teamField) {
    teamField.hidden = !isMiddleManager;
  }

  if (!isMiddleManager && teamInput) {
    teamInput.value = "";
  }
}

function renderUsers(users) {
  if (!usersTableBody) {
    return;
  }

  const items = Array.isArray(users) ? users : [];
  if (!items.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 5;
    cell.className = "user-registration-users-table__empty-cell";
    cell.textContent = "No users found.";
    row.append(cell);
    usersTableBody.replaceChildren(row);
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const item of items) {
    const row = document.createElement("tr");
    appendCell(row, (item?.username || "-").toString());
    appendCell(row, (item?.displayName || "-").toString());
    appendCell(row, (item?.roleName || "-").toString());
    appendCell(row, (item?.departmentName || "-").toString());
    appendCell(row, item?.isOwner ? "Yes" : "No");
    fragment.append(row);
  }

  usersTableBody.replaceChildren(fragment);
}

function appendCell(row, value) {
  const cell = document.createElement("td");
  cell.textContent = value;
  row.append(cell);
}

function setLoadingState(isLoading) {
  if (createUserButton) {
    createUserButton.disabled = isLoading;
  }
  if (usernameInput) {
    usernameInput.disabled = isLoading;
  }
  if (passwordInput) {
    passwordInput.disabled = isLoading;
  }
  if (displayNameInput) {
    displayNameInput.disabled = isLoading;
  }
  if (departmentSelect) {
    departmentSelect.disabled = isLoading;
  }
  if (roleSelect) {
    roleSelect.disabled = isLoading;
  }
  if (teamInput) {
    teamInput.disabled = isLoading;
  }
}

function setStatus(message, isError) {
  if (!statusElement) {
    return;
  }

  statusElement.textContent = message || "";
  statusElement.classList.toggle("error", Boolean(isError));
}

function buildUserLabel(roleName, departmentName) {
  const normalizedRoleName = roleName.toString().trim();
  const normalizedDepartmentName = departmentName.toString().trim();
  if (normalizedRoleName && normalizedDepartmentName) {
    return `${normalizedRoleName} | ${normalizedDepartmentName}`;
  }
  return normalizedRoleName || normalizedDepartmentName || "";
}

function redirectToLogin() {
  const nextPath = `${window.location.pathname || "/"}${window.location.search || ""}`;
  window.location.href = `${LOGIN_PATH}?next=${encodeURIComponent(nextPath)}`;
}
