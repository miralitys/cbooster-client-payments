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
const statusElement = document.querySelector("#access-control-status");
const currentUserElement = document.querySelector("#access-control-current-user");
const departmentsElement = document.querySelector("#access-control-departments");
const addUserButton = document.querySelector("#access-control-add-user-button");
const registrationPanel = document.querySelector("#access-control-registration-panel");
const closeUserFormButton = document.querySelector("#access-control-close-user-form-button");
const registrationStatusElement = document.querySelector("#access-control-registration-status");
const registrationFormElement = document.querySelector("#access-control-registration-form");
const usernameInput = document.querySelector("#access-control-new-user-username");
const passwordInput = document.querySelector("#access-control-new-user-password");
const displayNameInput = document.querySelector("#access-control-new-user-display-name");
const departmentSelect = document.querySelector("#access-control-new-user-department");
const roleSelect = document.querySelector("#access-control-new-user-role");
const teamField = document.querySelector("#access-control-new-user-team-field");
const teamInput = document.querySelector("#access-control-new-user-team");
const createUserButton = document.querySelector("#access-control-create-user-button");
const usersTableBody = document.querySelector("#access-control-users-body");

let currentAuthUser = "";
let currentAuthLabel = "";
let currentAuthIsOwner = false;
let currentAuthCanManageAccess = false;

initializeAccountMenu();
initializeAuthSession();
initializeRegistrationForm();
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
  currentAuthCanManageAccess = false;
  syncAuthUi();
  void hydrateAuthSessionFromServer();
}

function initializeRegistrationForm() {
  renderRoleOptions(departmentSelect?.value || "accounting");
  syncTeamFieldVisibility();
  setRegistrationPanelOpen(false);

  departmentSelect?.addEventListener("change", () => {
    renderRoleOptions(departmentSelect?.value || "");
  });

  roleSelect?.addEventListener("change", () => {
    syncTeamFieldVisibility();
  });

  addUserButton?.addEventListener("click", () => {
    const isCurrentlyHidden = registrationPanel?.hidden !== false;
    setRegistrationPanelOpen(isCurrentlyHidden);
  });

  closeUserFormButton?.addEventListener("click", () => {
    setRegistrationPanelOpen(false);
  });

  registrationFormElement?.addEventListener("submit", async (event) => {
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
  syncRegistrationAvailability();
}

function syncOwnerOnlyMenuItems(isOwner) {
  for (const item of ownerOnlyMenuItems) {
    item.hidden = !isOwner;
  }
}

function syncRegistrationAvailability() {
  if (addUserButton) {
    addUserButton.hidden = !currentAuthCanManageAccess;
  }

  if (!currentAuthCanManageAccess) {
    setRegistrationPanelOpen(false);
  }
}

function setRegistrationPanelOpen(isOpen) {
  if (!registrationPanel) {
    return;
  }

  const shouldOpen = Boolean(isOpen) && currentAuthCanManageAccess;
  registrationPanel.hidden = !shouldOpen;
  if (addUserButton) {
    addUserButton.textContent = shouldOpen ? "Hide User Registration" : "Add New User";
    addUserButton.setAttribute("aria-expanded", String(shouldOpen));
  }

  if (shouldOpen) {
    usernameInput?.focus();
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
    const canManageAccess = Boolean(payload?.permissions?.manage_access_control) || isOwner;
    currentAuthUser = username || "";
    currentAuthLabel = buildUserLabel(roleName, departmentName);
    currentAuthIsOwner = isOwner;
    currentAuthCanManageAccess = canManageAccess;
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

    currentAuthCanManageAccess = Boolean(payload?.permissions?.manage_access_control) || Boolean(payload?.user?.isOwner);
    syncAuthUi();
    renderCurrentUserCard(payload.user, payload.permissions);
    renderDepartments(payload?.accessModel?.departments);
    renderUsers(payload?.accessModel?.users);
    setStatus("Access model loaded.", false);
  } catch (error) {
    renderCurrentUserCard(null, null);
    renderDepartments([]);
    renderUsers([]);
    setStatus(error.message || "Failed to load access model.", true);
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
  setRegistrationStatus("Creating user...", false);

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

    setRegistrationStatus(`User "${payload?.item?.username || username}" created.`, false);
    await loadAccessModel();
    setRegistrationPanelOpen(false);
  } catch (error) {
    setRegistrationStatus(error.message || "Failed to create user.", true);
  } finally {
    setLoadingState(false);
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
  const permissionsCount =
    permissions && typeof permissions === "object" ? Object.values(permissions).filter((value) => Boolean(value)).length : 0;

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

function renderUsers(users) {
  if (!usersTableBody) {
    return;
  }

  if (!currentAuthCanManageAccess) {
    usersTableBody.replaceChildren();
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

function setRegistrationStatus(message, isError) {
  if (!registrationStatusElement) {
    return;
  }

  registrationStatusElement.textContent = message || "";
  registrationStatusElement.classList.toggle("error", Boolean(isError));
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
