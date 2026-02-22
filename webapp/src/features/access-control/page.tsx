import { useCallback, useEffect, useMemo, useState } from "react";
import QRCode from "qrcode";

import { createAccessUser, deleteAccessUser, getAccessModel, listAssistantReviews, updateAccessUser, updateAssistantReview } from "@/shared/api";
import type {
  AssistantReviewItem,
  AccessControlDepartment,
  AccessControlDepartmentRole,
  AccessControlModel,
  UpsertUserPayload,
} from "@/shared/types/accessControl";
import type { AuthUser, PermissionMap } from "@/shared/types/session";
import { Button, EmptyState, ErrorState, Field, Input, Modal, PageHeader, PageShell, Panel, Select, Table, Textarea } from "@/shared/ui";
import type { TableColumn } from "@/shared/ui";

interface UserFormState {
  username: string;
  password: string;
  displayName: string;
  departmentId: string;
  roleId: string;
  teamUsernames: string;
  totpSecret: string;
  totpEnabled: boolean;
}

const EMPTY_FORM: UserFormState = {
  username: "",
  password: "",
  displayName: "",
  departmentId: "",
  roleId: "",
  teamUsernames: "",
  totpSecret: "",
  totpEnabled: false,
};

interface AssistantReviewDraft {
  correctedReply: string;
  correctionNote: string;
}

const ASSISTANT_REVIEW_LIMIT = 80;
const DEFAULT_TOTP_ISSUER = "Credit Booster";
const DEFAULT_TOTP_PERIOD_SEC = 30;
const DEFAULT_TOTP_DIGITS = 6;

export default function AccessControlPage() {
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState("");
  const [statusText, setStatusText] = useState("Loading access model...");

  const [model, setModel] = useState<AccessControlModel | null>(null);
  const [currentUser, setCurrentUser] = useState<AuthUser | null>(null);
  const [permissions, setPermissions] = useState<PermissionMap>({});
  const [canManageAccess, setCanManageAccess] = useState(false);

  const [registrationOpen, setRegistrationOpen] = useState(false);
  const [createForm, setCreateForm] = useState<UserFormState>(EMPTY_FORM);
  const [createStatusText, setCreateStatusText] = useState(
    "Fill out the form to create a new user. If password is empty, a temporary password will be generated and shown once.",
  );
  const [createStatusError, setCreateStatusError] = useState(false);
  const [createdTemporaryPassword, setCreatedTemporaryPassword] = useState("");
  const [isCreating, setIsCreating] = useState(false);

  const [editingOriginalUsername, setEditingOriginalUsername] = useState("");
  const [editForm, setEditForm] = useState<UserFormState>(EMPTY_FORM);
  const [editStatusText, setEditStatusText] = useState("Update user data and click Save Changes.");
  const [editStatusError, setEditStatusError] = useState(false);
  const [isUpdating, setIsUpdating] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);

  const [assistantReviews, setAssistantReviews] = useState<AssistantReviewItem[]>([]);
  const [assistantReviewsTotal, setAssistantReviewsTotal] = useState(0);
  const [assistantReviewsLoading, setAssistantReviewsLoading] = useState(false);
  const [assistantReviewsError, setAssistantReviewsError] = useState("");
  const [assistantReviewsStatusText, setAssistantReviewsStatusText] = useState("Owner review queue is empty.");
  const [assistantReviewDrafts, setAssistantReviewDrafts] = useState<Record<string, AssistantReviewDraft>>({});
  const [assistantReviewSavingIds, setAssistantReviewSavingIds] = useState<Record<string, boolean>>({});
  const [expandedAssistantReviewId, setExpandedAssistantReviewId] = useState<number | null>(null);

  const departments = useMemo(() => model?.departments || [], [model]);
  const users = useMemo(() => model?.users || [], [model]);
  const rolesByDepartment = useMemo(() => {
    const mapping = new Map<string, AccessControlDepartmentRole[]>();
    for (const department of departments) {
      mapping.set(department.id, Array.isArray(department.roles) ? department.roles : []);
    }
    return mapping;
  }, [departments]);

  const createRoleOptions = useMemo(
    () => rolesByDepartment.get(createForm.departmentId) || [],
    [createForm.departmentId, rolesByDepartment],
  );
  const editRoleOptions = useMemo(
    () => rolesByDepartment.get(editForm.departmentId) || [],
    [editForm.departmentId, rolesByDepartment],
  );
  const totpIssuer = useMemo(
    () => sanitizeTotpIssuer(model?.totp?.issuer) || DEFAULT_TOTP_ISSUER,
    [model?.totp?.issuer],
  );
  const totpPeriodSec = useMemo(
    () => normalizeTotpPeriodSeconds(model?.totp?.periodSec),
    [model?.totp?.periodSec],
  );
  const totpDigits = useMemo(
    () => normalizeTotpDigits(model?.totp?.digits),
    [model?.totp?.digits],
  );

  const editingUser = useMemo(() => {
    const username = editingOriginalUsername.trim();
    if (!username) {
      return null;
    }
    return users.find((item) => item.username === username) || null;
  }, [editingOriginalUsername, users]);

  const loadAssistantReviewQueue = useCallback(async () => {
    setAssistantReviewsLoading(true);
    setAssistantReviewsError("");
    setAssistantReviewsStatusText("Loading assistant review queue...");

    try {
      const payload = await listAssistantReviews(ASSISTANT_REVIEW_LIMIT, 0);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setAssistantReviews(items);
      setAssistantReviewsTotal(Number.isFinite(payload?.total) ? payload.total : items.length);
      setAssistantReviewDrafts(() => {
        const next: Record<string, AssistantReviewDraft> = {};
        for (const item of items) {
          next[String(item.id)] = {
            correctedReply: item.correctedReply || "",
            correctionNote: item.correctionNote || "",
          };
        }
        return next;
      });
      setExpandedAssistantReviewId((previous) => {
        if (previous !== null && items.some((item) => item.id === previous)) {
          return previous;
        }
        return null;
      });
      if (!items.length) {
        setAssistantReviewsStatusText("No assistant questions yet.");
      } else {
        setAssistantReviewsStatusText(`Loaded ${items.length} of ${payload.total} assistant questions.`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load assistant review queue.";
      setAssistantReviews([]);
      setAssistantReviewsTotal(0);
      setAssistantReviewsError(message);
      setAssistantReviewsStatusText(message);
      setExpandedAssistantReviewId(null);
    } finally {
      setAssistantReviewsLoading(false);
    }
  }, []);

  const loadAccessModel = useCallback(async () => {
    setIsLoading(true);
    setLoadError("");
    setStatusText("Loading access model...");

    try {
      const payload = await getAccessModel();
      const accessModel = payload.accessModel;
      const canManage = Boolean(payload?.permissions?.manage_access_control) || Boolean(payload?.user?.isOwner);
      const nextDepartments = Array.isArray(accessModel?.departments) ? accessModel.departments : [];

      setModel(accessModel);
      setCurrentUser(payload.user || null);
      setPermissions(payload.permissions || {});
      setCanManageAccess(canManage);
      setCreateForm((previous) => ensureFormDefaults(previous, nextDepartments));
      setStatusText("Access model loaded.");
      if (payload?.user?.isOwner) {
        await loadAssistantReviewQueue();
      } else {
        setAssistantReviews([]);
        setAssistantReviewsTotal(0);
        setAssistantReviewsLoading(false);
        setAssistantReviewsError("");
        setAssistantReviewsStatusText("Owner review queue is available only for owner accounts.");
        setAssistantReviewDrafts({});
        setAssistantReviewSavingIds({});
        setExpandedAssistantReviewId(null);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to load access model.";
      setModel(null);
      setCurrentUser(null);
      setPermissions({});
      setCanManageAccess(false);
      setLoadError(message);
      setStatusText(message);
      setAssistantReviews([]);
      setAssistantReviewsTotal(0);
      setAssistantReviewsLoading(false);
      setAssistantReviewsError("");
      setAssistantReviewsStatusText("Owner review queue is unavailable until access model is loaded.");
      setAssistantReviewDrafts({});
      setAssistantReviewSavingIds({});
      setExpandedAssistantReviewId(null);
    } finally {
      setIsLoading(false);
    }
  }, [loadAssistantReviewQueue]);

  useEffect(() => {
    void loadAccessModel();
  }, [loadAccessModel]);

  const openEditModal = useCallback(
    (user: AuthUser) => {
      if (!canManageAccess || user.isOwner) {
        return;
      }

      const next = ensureFormDefaults(
        {
          username: user.username || "",
          password: "",
          displayName: user.displayName || user.username || "",
          departmentId: user.departmentId || "",
          roleId: user.roleId || "",
          teamUsernames: (user.teamUsernames || []).join(", "),
          totpSecret: "",
          totpEnabled: Boolean(user.totpEnabled),
        },
        departments,
      );

      setEditingOriginalUsername(user.username);
      setEditForm(next);
      setEditStatusText(`Editing "${user.displayName || user.username}".`);
      setEditStatusError(false);
    },
    [canManageAccess, departments],
  );

  const userColumns = useMemo<TableColumn<AuthUser>[]>(() => {
    return [
      {
        key: "username",
        label: "Username",
        align: "left",
        cell: (item) => item.username || "-",
      },
      {
        key: "displayName",
        label: "Display Name",
        align: "left",
        cell: (item) => {
          const display = item.displayName || item.username || "-";
          if (!canManageAccess || item.isOwner) {
            return display;
          }

          return (
            <button
              type="button"
              className="access-control-user-link"
              onClick={() => {
                openEditModal(item);
              }}
            >
              {display}
            </button>
          );
        },
      },
      {
        key: "roleName",
        label: "Role",
        align: "left",
        cell: (item) => item.roleName || "-",
      },
      {
        key: "departmentName",
        label: "Department",
        align: "left",
        cell: (item) => item.departmentName || "-",
      },
      {
        key: "isOwner",
        label: "Owner",
        align: "center",
        cell: (item) => (item.isOwner ? "Yes" : "No"),
      },
      {
        key: "totpEnabled",
        label: "2FA",
        align: "center",
        cell: (item) => (item.totpEnabled ? "Enabled" : "Off"),
      },
    ];
  }, [canManageAccess, openEditModal]);

  function closeEditModal() {
    setEditingOriginalUsername("");
    setEditForm(EMPTY_FORM);
    setEditStatusText("Update user data and click Save Changes.");
    setEditStatusError(false);
    setIsUpdating(false);
    setIsDeleting(false);
  }

  function onCreateDepartmentChange(departmentId: string) {
    const roleOptions = rolesByDepartment.get(departmentId) || [];
    setCreateForm((previous) => ({
      ...previous,
      departmentId,
      roleId: roleOptions.some((role) => role.id === previous.roleId) ? previous.roleId : roleOptions[0]?.id || "",
      teamUsernames: "",
    }));
  }

  function onEditDepartmentChange(departmentId: string) {
    const roleOptions = rolesByDepartment.get(departmentId) || [];
    setEditForm((previous) => ({
      ...previous,
      departmentId,
      roleId: roleOptions.some((role) => role.id === previous.roleId) ? previous.roleId : roleOptions[0]?.id || "",
      teamUsernames: "",
    }));
  }

  async function submitCreateUser(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canManageAccess) {
      return;
    }

    if (!createForm.displayName.trim()) {
      setCreateStatusText("Display Name is required.");
      setCreateStatusError(true);
      return;
    }
    if (createForm.totpEnabled && !normalizeTotpSecretValue(createForm.totpSecret)) {
      setCreateStatusText("TOTP secret is required when 2FA is enabled.");
      setCreateStatusError(true);
      return;
    }

    setIsCreating(true);
    setCreateStatusText("Creating user...");
    setCreateStatusError(false);
    setCreatedTemporaryPassword("");

    try {
      const payload = buildUpsertPayload(createForm);
      const response = await createAccessUser(payload);
      const createdUsername = response?.item?.username || payload.username || createForm.displayName;
      const temporaryPassword = String(response?.temporaryPassword || "").trim();
      if (temporaryPassword) {
        setCreateStatusText(`User "${createdUsername}" created. Temporary password is shown below (copy and share once).`);
        setCreatedTemporaryPassword(temporaryPassword);
      } else {
        setCreateStatusText(`User "${createdUsername}" created.`);
        setCreatedTemporaryPassword("");
      }
      setCreateStatusError(false);
      setCreateForm((previous) =>
        ensureFormDefaults(
          {
            ...EMPTY_FORM,
            departmentId: previous.departmentId,
            roleId: previous.roleId,
          },
          departments,
        ),
      );
      setRegistrationOpen(Boolean(temporaryPassword));
      await loadAccessModel();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to create user.";
      setCreateStatusText(message);
      setCreateStatusError(true);
      setCreatedTemporaryPassword("");
    } finally {
      setIsCreating(false);
    }
  }

  async function copyCreatedTemporaryPassword() {
    const password = createdTemporaryPassword.trim();
    if (!password) {
      return;
    }

    try {
      if (!globalThis.navigator?.clipboard?.writeText) {
        throw new Error("Clipboard access is not available.");
      }
      await globalThis.navigator.clipboard.writeText(password);
      setCreateStatusText("Temporary password copied to clipboard.");
      setCreateStatusError(false);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to copy temporary password.";
      setCreateStatusText(message);
      setCreateStatusError(true);
    }
  }

  async function submitUpdateUser(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canManageAccess || !editingOriginalUsername) {
      return;
    }

    if (!editForm.displayName.trim()) {
      setEditStatusText("Display Name is required.");
      setEditStatusError(true);
      return;
    }
    if (editForm.totpEnabled && !normalizeTotpSecretValue(editForm.totpSecret)) {
      setEditStatusText("TOTP secret is required when 2FA is enabled.");
      setEditStatusError(true);
      return;
    }

    setIsUpdating(true);
    setEditStatusText("Saving changes...");
    setEditStatusError(false);

    try {
      const payload = buildUpsertPayload(editForm);
      const response = await updateAccessUser(editingOriginalUsername, payload);
      const updatedName = response?.item?.displayName || response?.item?.username || editForm.displayName;
      setEditStatusText(`User "${updatedName}" updated.`);
      setEditStatusError(false);
      await loadAccessModel();
      closeEditModal();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to update user.";
      setEditStatusText(message);
      setEditStatusError(true);
    } finally {
      setIsUpdating(false);
    }
  }

  async function submitDeleteUser() {
    if (!canDeleteEditingUser || !editingOriginalUsername || !editingUser) {
      return;
    }

    const targetDisplayName = editingUser.displayName || editingUser.username || editingOriginalUsername;
    const shouldDelete = window.confirm(`Delete user "${targetDisplayName}"? This action cannot be undone.`);
    if (!shouldDelete) {
      return;
    }

    setIsDeleting(true);
    setEditStatusText(`Deleting "${targetDisplayName}"...`);
    setEditStatusError(false);

    try {
      const response = await deleteAccessUser(editingOriginalUsername);
      const deletedName = response?.item?.displayName || response?.item?.username || targetDisplayName;
      closeEditModal();
      setStatusText(`User "${deletedName}" deleted.`);
      await loadAccessModel();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to delete user.";
      setEditStatusText(message);
      setEditStatusError(true);
    } finally {
      setIsDeleting(false);
    }
  }

  const enabledPermissionsCount = useMemo(() => {
    return Object.values(permissions || {}).filter(Boolean).length;
  }, [permissions]);

  const isOwnerUser = Boolean(currentUser?.isOwner);
  const isAdminUser = Boolean(permissions?.manage_access_control) && !isOwnerUser;
  const canDeleteUsers = isOwnerUser || isAdminUser;
  const isEditingCurrentUser =
    Boolean(editingOriginalUsername) &&
    normalizeUsername(editingOriginalUsername) === normalizeUsername(currentUser?.username || "");
  const canDeleteEditingUser = Boolean(canDeleteUsers && editingUser && !editingUser.isOwner && !isEditingCurrentUser);
  const isEditBusy = isUpdating || isDeleting;

  const getAssistantReviewDraft = useCallback(
    (item: AssistantReviewItem): AssistantReviewDraft => {
      return (
        assistantReviewDrafts[String(item.id)] || {
          correctedReply: item.correctedReply || "",
          correctionNote: item.correctionNote || "",
        }
      );
    },
    [assistantReviewDrafts],
  );

  function updateAssistantReviewDraftField(
    reviewId: number,
    field: keyof AssistantReviewDraft,
    value: string,
  ) {
    const key = String(reviewId);
    setAssistantReviewDrafts((previous) => ({
      ...previous,
      [key]: {
        correctedReply: previous[key]?.correctedReply ?? "",
        correctionNote: previous[key]?.correctionNote ?? "",
        [field]: value,
      },
    }));
  }

  function toggleAssistantReviewItem(reviewId: number) {
    setExpandedAssistantReviewId((previous) => (previous === reviewId ? null : reviewId));
  }

  async function submitAssistantReviewCorrection(item: AssistantReviewItem) {
    if (!isOwnerUser) {
      return;
    }

    const reviewId = item.id;
    const draft = getAssistantReviewDraft(item);
    const correctedReply = draft.correctedReply.trim();
    const correctionNote = draft.correctionNote.trim();

    setAssistantReviewSavingIds((previous) => ({ ...previous, [String(reviewId)]: true }));
    setAssistantReviewsError("");
    setAssistantReviewsStatusText(`Saving correction for review #${reviewId}...`);

    try {
      const response = await updateAssistantReview(reviewId, {
        correctedReply,
        correctionNote,
      });
      const updatedItem = response.item;
      setAssistantReviews((previous) =>
        previous.map((review) => (review.id === updatedItem.id ? updatedItem : review)),
      );
      setAssistantReviewDrafts((previous) => ({
        ...previous,
        [String(updatedItem.id)]: {
          correctedReply: updatedItem.correctedReply || "",
          correctionNote: updatedItem.correctionNote || "",
        },
      }));
      setAssistantReviewsStatusText(`Correction saved for review #${reviewId}.`);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to save correction.";
      setAssistantReviewsError(message);
      setAssistantReviewsStatusText(message);
    } finally {
      setAssistantReviewSavingIds((previous) => {
        const next = { ...previous };
        delete next[String(reviewId)];
        return next;
      });
    }
  }

  return (
    <PageShell className="access-control-react-page">
      <PageHeader
        actions={
          canManageAccess ? (
            <Button
              type="button"
              size="sm"
              onClick={() =>
                setRegistrationOpen((previous) => {
                  const nextOpen = !previous;
                  if (!nextOpen) {
                    setCreatedTemporaryPassword("");
                  }
                  return nextOpen;
                })
              }
            >
              {registrationOpen ? "Hide User Registration" : "Add New User"}
            </Button>
          ) : undefined
        }
      />

      <Panel title="Current Access">
        <p className={`dashboard-message access-control-status-react ${loadError ? "error" : ""}`.trim()}>
          {statusText}
        </p>

        {isLoading ? <EmptyState title="Loading access data..." /> : null}

        {!isLoading && loadError ? (
          <ErrorState
            title="Failed to load access model"
            description={loadError}
            actionLabel="Retry"
            onAction={() => void loadAccessModel()}
          />
        ) : null}

        {!isLoading && !loadError ? (
          <div className="access-control-current-grid-react">
            <CurrentAccessLine label="Username" value={currentUser?.username || "-"} />
            <CurrentAccessLine label="Display Name" value={currentUser?.displayName || "-"} />
            <CurrentAccessLine label="Role" value={currentUser?.roleName || "-"} />
            <CurrentAccessLine label="Department" value={currentUser?.departmentName || "-"} />
            <CurrentAccessLine
              label="Access Level"
              value={currentUser?.isOwner ? "Owner (full access)" : "Department access"}
            />
            <CurrentAccessLine label="2FA (Authenticator)" value={currentUser?.totpEnabled ? "Enabled" : "Off"} />
            <CurrentAccessLine label="Enabled Permissions" value={String(enabledPermissionsCount)} />
          </div>
        ) : null}
      </Panel>

      {registrationOpen && canManageAccess ? (
        <Panel title="User Registration">
          <p className={`dashboard-message ${createStatusError ? "error" : ""}`.trim()}>{createStatusText}</p>
          {createdTemporaryPassword ? (
            <div className="access-control-temp-password-react">
              <p className="access-control-temp-password-label-react">One-time temporary password</p>
              <code className="access-control-temp-password-value-react">{createdTemporaryPassword}</code>
              <Button type="button" size="sm" variant="secondary" onClick={() => void copyCreatedTemporaryPassword()}>
                Copy Password
              </Button>
            </div>
          ) : null}

          <form className="access-control-form-react" onSubmit={(event) => void submitCreateUser(event)}>
            <Field label="Username / Email (optional)" htmlFor="access-create-username">
              <Input
                id="access-create-username"
                value={createForm.username}
                onChange={(event) => setCreateForm((previous) => ({ ...previous, username: event.target.value }))}
                placeholder="Optional: user email or login"
                disabled={isCreating}
              />
            </Field>

            <Field label="Password (optional, min 8 chars)" htmlFor="access-create-password">
              <Input
                id="access-create-password"
                type="password"
                value={createForm.password}
                onChange={(event) => setCreateForm((previous) => ({ ...previous, password: event.target.value }))}
                placeholder="Leave empty to auto-generate temporary password"
                disabled={isCreating}
              />
            </Field>

            <Field label="2FA (Authenticator)" htmlFor="access-create-totp-enabled">
              <Select
                id="access-create-totp-enabled"
                value={createForm.totpEnabled ? "enabled" : "disabled"}
                onChange={(event) => {
                  const enabled = event.target.value === "enabled";
                  setCreateForm((previous) => ({
                    ...previous,
                    totpEnabled: enabled,
                    totpSecret: enabled ? previous.totpSecret : "",
                  }));
                }}
                disabled={isCreating}
              >
                <option value="disabled">Off</option>
                <option value="enabled">Enabled</option>
              </Select>
            </Field>

            {createForm.totpEnabled ? (
              <>
                <Field
                  label="TOTP Secret (Base32)"
                  htmlFor="access-create-totp-secret"
                  hint="Scan the QR below or paste your own secret."
                >
                  <Input
                    id="access-create-totp-secret"
                    value={createForm.totpSecret}
                    onChange={(event) => setCreateForm((previous) => ({ ...previous, totpSecret: event.target.value }))}
                    placeholder="Example: JBSWY3DPEHPK3PXP"
                    disabled={isCreating}
                  />
                </Field>
                <div className="access-control-totp-actions-react">
                  <Button
                    type="button"
                    size="sm"
                    variant="secondary"
                    onClick={() =>
                      setCreateForm((previous) => ({
                        ...previous,
                        totpSecret: generateTotpSecretValue(),
                      }))
                    }
                    disabled={isCreating}
                  >
                    Generate Secret
                  </Button>
                </div>
                <TotpQrPreview
                  title="Authenticator QR Preview"
                  username={createForm.username.trim() || createForm.displayName.trim() || "new-user"}
                  secret={createForm.totpSecret}
                  issuer={totpIssuer}
                  periodSec={totpPeriodSec}
                  digits={totpDigits}
                />
              </>
            ) : null}

            <Field label="Display Name" htmlFor="access-create-display-name">
              <Input
                id="access-create-display-name"
                value={createForm.displayName}
                onChange={(event) => setCreateForm((previous) => ({ ...previous, displayName: event.target.value }))}
                placeholder="Required"
                required
                disabled={isCreating}
              />
            </Field>

            <Field label="Department" htmlFor="access-create-department">
              <Select
                id="access-create-department"
                value={createForm.departmentId}
                onChange={(event) => onCreateDepartmentChange(event.target.value)}
                disabled={isCreating}
              >
                {departments.map((department) => (
                  <option key={department.id} value={department.id}>
                    {department.name}
                  </option>
                ))}
              </Select>
            </Field>

            <Field label="Role" htmlFor="access-create-role">
              <Select
                id="access-create-role"
                value={createForm.roleId}
                onChange={(event) => {
                  const nextRole = event.target.value;
                  setCreateForm((previous) => ({
                    ...previous,
                    roleId: nextRole,
                    teamUsernames: nextRole === "middle_manager" ? previous.teamUsernames : "",
                  }));
                }}
                disabled={isCreating}
              >
                {createRoleOptions.map((role) => (
                  <option key={role.id} value={role.id}>
                    {role.name}
                  </option>
                ))}
              </Select>
            </Field>

            {createForm.roleId === "middle_manager" ? (
              <Field label="Team Usernames (comma separated, for Middle Manager)" htmlFor="access-create-team">
                <Input
                  id="access-create-team"
                  value={createForm.teamUsernames}
                  onChange={(event) => setCreateForm((previous) => ({ ...previous, teamUsernames: event.target.value }))}
                  disabled={isCreating}
                />
              </Field>
            ) : null}

            <div className="access-control-form-actions-react">
              <Button type="submit" isLoading={isCreating} disabled={isCreating}>
                Create User
              </Button>
            </div>
          </form>

          <h3 className="subsection-heading">Current Users</h3>

          {!users.length ? (
            <EmptyState title="No users found." />
          ) : (
            <Table
              className="access-control-users-table-wrap-react"
              columns={userColumns}
              rows={users}
              rowKey={(item) => item.username}
              density="compact"
            />
          )}
        </Panel>
      ) : null}

      <Panel title="Departments">
        {!departments.length ? (
          <EmptyState title="No department access data." />
        ) : (
          <div className="access-control-departments-grid-react">
            {departments.map((department) => (
              <DepartmentCard key={department.id} department={department} />
            ))}
          </div>
        )}
      </Panel>

      {isOwnerUser ? (
        <Panel
          title="Assistant Review Queue (Owner)"
          actions={
            <Button
              type="button"
              size="sm"
              variant="secondary"
              onClick={() => void loadAssistantReviewQueue()}
              disabled={assistantReviewsLoading}
            >
              Refresh
            </Button>
          }
        >
          <p className={`dashboard-message ${assistantReviewsError ? "error" : ""}`.trim()}>{assistantReviewsStatusText}</p>

          {assistantReviewsLoading ? <EmptyState title="Loading assistant questions..." /> : null}

          {!assistantReviewsLoading && assistantReviewsError ? (
            <ErrorState
              title="Failed to load assistant review queue"
              description={assistantReviewsError}
              actionLabel="Retry"
              onAction={() => void loadAssistantReviewQueue()}
            />
          ) : null}

          {!assistantReviewsLoading && !assistantReviewsError && !assistantReviews.length ? (
            <EmptyState title="No assistant questions yet." />
          ) : null}

          {!assistantReviewsLoading && !assistantReviewsError && assistantReviews.length ? (
            <div className="access-control-assistant-review-list-react">
              {assistantReviews.map((item) => {
                const draft = getAssistantReviewDraft(item);
                const itemSaving = Boolean(assistantReviewSavingIds[String(item.id)]);
                const isExpanded = expandedAssistantReviewId === item.id;
                const askerName = item.askedByDisplayName || "-";
                const askerUsername = item.askedByUsername || "-";

                return (
                  <article key={`assistant-review-${item.id}`} className="access-control-assistant-review-card-react">
                    <button
                      type="button"
                      className={`access-control-assistant-review-summary-react ${isExpanded ? "is-expanded" : ""}`.trim()}
                      onClick={() => toggleAssistantReviewItem(item.id)}
                    >
                      <span className="access-control-assistant-review-summary-cell-react">
                        <strong>Date:</strong> {formatDateTime(item.askedAt)}
                      </span>
                      <span className="access-control-assistant-review-summary-cell-react">
                        <strong>Entered By:</strong> {askerName}
                      </span>
                      <span className="access-control-assistant-review-summary-cell-react">
                        <strong>Username:</strong> {askerUsername}
                      </span>
                    </button>

                    {isExpanded ? (
                      <div className="access-control-assistant-review-expanded-react">
                        <div className="access-control-assistant-review-meta-react">
                          <span>#{item.id}</span>
                          <span>{item.mode === "voice" ? "Voice" : "Text"}</span>
                          <span>{item.provider || "-"}</span>
                          <span>{item.recordsUsed} records</span>
                        </div>

                        <div className="access-control-assistant-review-block-react">
                          <h4>Question</h4>
                          <p>{item.question || "-"}</p>
                        </div>

                        <div className="access-control-assistant-review-block-react">
                          <h4>Assistant Answer</h4>
                          <p>{item.assistantReply || "-"}</p>
                        </div>

                        <Field label="Owner Corrected Answer" htmlFor={`assistant-review-corrected-${item.id}`}>
                          <Textarea
                            id={`assistant-review-corrected-${item.id}`}
                            rows={4}
                            value={draft.correctedReply}
                            onChange={(event) => updateAssistantReviewDraftField(item.id, "correctedReply", event.target.value)}
                            placeholder="Write the correct answer that assistant should have returned..."
                            disabled={itemSaving}
                          />
                        </Field>

                        <Field label="Correction Note (optional)" htmlFor={`assistant-review-note-${item.id}`}>
                          <Textarea
                            id={`assistant-review-note-${item.id}`}
                            rows={2}
                            value={draft.correctionNote}
                            onChange={(event) => updateAssistantReviewDraftField(item.id, "correctionNote", event.target.value)}
                            placeholder="Optional explanation for future tuning..."
                            disabled={itemSaving}
                          />
                        </Field>

                        <div className="access-control-assistant-review-actions-react">
                          <Button
                            type="button"
                            size="sm"
                            onClick={() => void submitAssistantReviewCorrection(item)}
                            isLoading={itemSaving}
                            disabled={itemSaving}
                          >
                            Save Correction
                          </Button>
                          <span className="access-control-assistant-review-updated-react">
                            {item.correctedAt
                              ? `Last correction: ${formatDateTime(item.correctedAt)} by ${item.correctedBy || "-"}`
                              : "No correction yet"}
                          </span>
                        </div>
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </div>
          ) : null}

          {!assistantReviewsLoading && !assistantReviewsError && assistantReviews.length ? (
            <p className="access-control-assistant-review-total-react">
              Showing {assistantReviews.length} of {assistantReviewsTotal} latest assistant questions.
            </p>
          ) : null}
        </Panel>
      ) : null}

      <Modal
        open={Boolean(editingOriginalUsername && canManageAccess && editingUser)}
        title="Edit User"
        onClose={closeEditModal}
        footer={
          <div className="access-control-form-actions-react access-control-form-actions-react--split">
            {canDeleteEditingUser ? (
              <Button
                type="button"
                variant="danger"
                onClick={() => void submitDeleteUser()}
                isLoading={isDeleting}
                disabled={isEditBusy}
              >
                Delete User
              </Button>
            ) : (
              <span />
            )}
            <div className="access-control-form-actions-group-react">
              <Button type="button" variant="secondary" onClick={closeEditModal} disabled={isEditBusy}>
                Cancel
              </Button>
              <Button type="submit" form="access-control-edit-user-form" isLoading={isUpdating} disabled={isEditBusy}>
                Save Changes
              </Button>
            </div>
          </div>
        }
      >
        <form id="access-control-edit-user-form" className="access-control-form-react" onSubmit={(event) => void submitUpdateUser(event)}>
          <p className={`dashboard-message ${editStatusError ? "error" : ""}`.trim()}>{editStatusText}</p>

          <Field label="Username / Email (optional)" htmlFor="access-edit-username">
            <Input
              id="access-edit-username"
              value={editForm.username}
              onChange={(event) => setEditForm((previous) => ({ ...previous, username: event.target.value }))}
              placeholder="Optional: user email or login"
              disabled={isEditBusy}
            />
          </Field>

          <Field label="New Password (optional)" htmlFor="access-edit-password">
            <Input
              id="access-edit-password"
              type="password"
              value={editForm.password}
              onChange={(event) => setEditForm((previous) => ({ ...previous, password: event.target.value }))}
              placeholder="Leave empty to keep current password"
              disabled={isEditBusy}
            />
          </Field>

          <Field label="2FA (Authenticator)" htmlFor="access-edit-totp-enabled">
            <Select
              id="access-edit-totp-enabled"
              value={editForm.totpEnabled ? "enabled" : "disabled"}
              onChange={(event) => {
                const enabled = event.target.value === "enabled";
                  setEditForm((previous) => ({
                    ...previous,
                    totpEnabled: enabled,
                    totpSecret: enabled ? previous.totpSecret : "",
                  }));
                }}
                disabled={isEditBusy}
              >
                <option value="disabled">Off</option>
                <option value="enabled">Enabled</option>
              </Select>
            </Field>

          {editForm.totpEnabled ? (
            <>
              <Field
                label="TOTP Secret (Base32)"
                htmlFor="access-edit-totp-secret"
                hint={
                  editingUser?.totpEnabled
                    ? "Current secret is hidden. Generate and save a new secret to rotate this user 2FA key."
                    : "Scan the QR below or paste your own secret."
                }
              >
                <Input
                  id="access-edit-totp-secret"
                  value={editForm.totpSecret}
                  onChange={(event) => setEditForm((previous) => ({ ...previous, totpSecret: event.target.value }))}
                  placeholder="Example: JBSWY3DPEHPK3PXP"
                  disabled={isEditBusy}
                />
              </Field>
              <div className="access-control-totp-actions-react">
                <Button
                  type="button"
                  size="sm"
                  variant="secondary"
                  onClick={() =>
                    setEditForm((previous) => ({
                      ...previous,
                      totpSecret: generateTotpSecretValue(),
                    }))
                  }
                  disabled={isEditBusy}
                >
                  Generate Secret
                </Button>
              </div>
              <TotpQrPreview
                title="Authenticator QR Preview"
                username={editForm.username.trim() || editingOriginalUsername || editForm.displayName.trim() || "user"}
                secret={editForm.totpSecret}
                issuer={totpIssuer}
                periodSec={totpPeriodSec}
                digits={totpDigits}
              />
            </>
          ) : null}

          <Field label="Display Name" htmlFor="access-edit-display-name">
            <Input
              id="access-edit-display-name"
              value={editForm.displayName}
              onChange={(event) => setEditForm((previous) => ({ ...previous, displayName: event.target.value }))}
              required
              disabled={isEditBusy}
            />
          </Field>

          <Field label="Department" htmlFor="access-edit-department">
            <Select
              id="access-edit-department"
              value={editForm.departmentId}
              onChange={(event) => onEditDepartmentChange(event.target.value)}
              disabled={isEditBusy}
            >
              {departments.map((department) => (
                <option key={department.id} value={department.id}>
                  {department.name}
                </option>
              ))}
            </Select>
          </Field>

          <Field label="Role" htmlFor="access-edit-role">
            <Select
              id="access-edit-role"
              value={editForm.roleId}
              onChange={(event) => {
                const nextRole = event.target.value;
                  setEditForm((previous) => ({
                    ...previous,
                    roleId: nextRole,
                    teamUsernames: nextRole === "middle_manager" ? previous.teamUsernames : "",
                  }));
                }}
                disabled={isEditBusy}
              >
              {editRoleOptions.map((role) => (
                <option key={role.id} value={role.id}>
                  {role.name}
                </option>
              ))}
            </Select>
          </Field>

          {editForm.roleId === "middle_manager" ? (
            <Field label="Team Usernames (comma separated, for Middle Manager)" htmlFor="access-edit-team">
              <Input
                id="access-edit-team"
                value={editForm.teamUsernames}
                onChange={(event) => setEditForm((previous) => ({ ...previous, teamUsernames: event.target.value }))}
                disabled={isEditBusy}
              />
            </Field>
          ) : null}
        </form>
      </Modal>
    </PageShell>
  );
}

function DepartmentCard({ department }: { department: AccessControlDepartment }) {
  const rows = Array.isArray(department.roles) ? department.roles : [];
  return (
    <article className="access-control-department-card-react">
      <h3 className="access-control-department-card-react__title">{department.name}</h3>
      <table className="access-control-department-card-react__table">
        <thead>
          <tr>
            <th>Role</th>
            <th>Assigned Users</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((role) => (
            <tr key={`${department.id}-${role.id}`}>
              <td>
                <span className="access-control-role-pill-react">{role.name}</span>
              </td>
              <td className="access-control-members-cell-react">
                {role.members.length ? (
                  <div className="access-control-members-list-react">
                    {role.members.map((member) => (
                      <span key={`${role.id}-${member.username}`} className="access-control-member-chip-react" title={member.username}>
                        {member.displayName || member.username}
                      </span>
                    ))}
                  </div>
                ) : (
                  <span className="access-control-members-empty-react">Unassigned</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </article>
  );
}

function CurrentAccessLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="access-control-current-row-react">
      <span className="access-control-current-label-react">{label}:</span>
      <span className="access-control-current-value-react">{value}</span>
    </div>
  );
}

function TotpQrPreview({
  title,
  username,
  secret,
  issuer,
  periodSec,
  digits,
}: {
  title: string;
  username: string;
  secret: string;
  issuer: string;
  periodSec: number;
  digits: number;
}) {
  const [qrState, setQrState] = useState({
    uri: "",
    dataUrl: "",
    error: "",
  });
  const normalizedSecret = useMemo(() => normalizeTotpSecretValue(secret), [secret]);
  const normalizedUsername = useMemo(() => sanitizeTotpLabelText(username), [username]);
  const normalizedIssuer = useMemo(() => sanitizeTotpIssuer(issuer), [issuer]);
  const normalizedPeriodSec = useMemo(() => normalizeTotpPeriodSeconds(periodSec), [periodSec]);
  const normalizedDigits = useMemo(() => normalizeTotpDigits(digits), [digits]);
  const otpauthUri = useMemo(() => {
    if (!normalizedSecret) {
      return "";
    }

    return buildTotpSetupUri({
      username: normalizedUsername || "user",
      secret: normalizedSecret,
      issuer: normalizedIssuer || DEFAULT_TOTP_ISSUER,
      periodSec: normalizedPeriodSec,
      digits: normalizedDigits,
    });
  }, [normalizedSecret, normalizedUsername, normalizedIssuer, normalizedPeriodSec, normalizedDigits]);

  useEffect(() => {
    let canceled = false;

    if (!otpauthUri) {
      return;
    }

    void QRCode.toDataURL(otpauthUri, {
      errorCorrectionLevel: "M",
      margin: 1,
      width: 220,
    })
      .then((dataUrl: string) => {
        if (canceled) {
          return;
        }
        setQrState({
          uri: otpauthUri,
          dataUrl,
          error: "",
        });
      })
      .catch(() => {
        if (canceled) {
          return;
        }
        setQrState({
          uri: otpauthUri,
          dataUrl: "",
          error: "Failed to generate QR code.",
        });
      });

    return () => {
      canceled = true;
    };
  }, [otpauthUri]);
  const qrDataUrl = qrState.uri === otpauthUri ? qrState.dataUrl : "";
  const qrError = qrState.uri === otpauthUri ? qrState.error : "";

  return (
    <div className="access-control-totp-preview-react">
      <p className="access-control-totp-preview-title-react">{title}</p>
      {!normalizedSecret ? (
        <p className="access-control-totp-preview-hint-react">
          Enter or generate a secret to render QR.
        </p>
      ) : (
        <>
          {qrDataUrl ? (
            <img
              className="access-control-totp-preview-image-react"
              src={qrDataUrl}
              alt="Authenticator setup QR"
            />
          ) : null}
          {qrError ? (
            <p className="dashboard-message error">{qrError}</p>
          ) : null}
          {normalizedUsername ? (
            <p className="access-control-totp-preview-hint-react">
              Account label: {normalizedUsername}
            </p>
          ) : null}
          <p className="access-control-totp-uri-label-react">otpauth URI</p>
          <Input
            className="access-control-totp-uri-input-react"
            value={otpauthUri}
            readOnly
            onFocus={(event) => {
              event.currentTarget.select();
            }}
          />
        </>
      )}
    </div>
  );
}

function formatDateTime(rawValue: string | null): string {
  if (!rawValue) {
    return "-";
  }

  const parsed = Date.parse(rawValue);
  if (Number.isNaN(parsed)) {
    return rawValue;
  }

  return new Date(parsed).toLocaleString();
}

function normalizeUsername(value: string): string {
  return value.toString().trim().toLowerCase();
}

function ensureFormDefaults(form: UserFormState, departments: AccessControlDepartment[]): UserFormState {
  const normalizedDepartments = Array.isArray(departments) ? departments : [];
  if (!normalizedDepartments.length) {
    return {
      ...form,
      departmentId: "",
      roleId: "",
    };
  }

  const departmentId = normalizedDepartments.some((department) => department.id === form.departmentId)
    ? form.departmentId
    : normalizedDepartments[0].id;
  const roleOptions = normalizedDepartments.find((department) => department.id === departmentId)?.roles || [];
  const roleId = roleOptions.some((role) => role.id === form.roleId)
    ? form.roleId
    : roleOptions[0]?.id || "";

  return {
    ...form,
    departmentId,
    roleId,
    teamUsernames: roleId === "middle_manager" ? form.teamUsernames : "",
  };
}

function buildUpsertPayload(form: UserFormState): UpsertUserPayload {
  const payload: UpsertUserPayload = {
    displayName: form.displayName.trim(),
    departmentId: form.departmentId,
    roleId: form.roleId,
    teamUsernames: form.roleId === "middle_manager" ? parseTeamUsernames(form.teamUsernames) : [],
    totpEnabled: Boolean(form.totpEnabled),
  };

  const username = form.username.trim();
  if (username) {
    payload.username = username;
  }

  const password = form.password.trim();
  if (password) {
    payload.password = password;
  }

  const totpSecret = normalizeTotpSecretValue(form.totpSecret);
  if (form.totpEnabled) {
    if (totpSecret) {
      payload.totpSecret = totpSecret;
    }
  } else {
    payload.totpSecret = "";
  }

  return payload;
}

function parseTeamUsernames(rawValue: string): string[] {
  return String(rawValue || "")
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeTotpSecretValue(rawValue: string): string {
  return String(rawValue || "")
    .toUpperCase()
    .replace(/[\s-]+/g, "")
    .replace(/=+$/g, "")
    .replace(/[^A-Z2-7]/g, "")
    .slice(0, 200);
}

function sanitizeTotpLabelText(rawValue: string): string {
  return String(rawValue || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 180);
}

function sanitizeTotpIssuer(rawValue: string | undefined): string {
  return String(rawValue || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 120);
}

function normalizeTotpPeriodSeconds(rawValue: number | undefined): number {
  const parsed = Number.parseInt(String(rawValue || ""), 10);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_TOTP_PERIOD_SEC;
  }
  return Math.min(Math.max(parsed, 15), 120);
}

function normalizeTotpDigits(rawValue: number | undefined): number {
  const parsed = Number.parseInt(String(rawValue || ""), 10);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_TOTP_DIGITS;
  }
  return Math.min(Math.max(parsed, 6), 8);
}

function buildTotpSetupUri({
  username,
  secret,
  issuer,
  periodSec,
  digits,
}: {
  username: string;
  secret: string;
  issuer: string;
  periodSec: number;
  digits: number;
}): string {
  const label = `${issuer}:${username}`;
  const query = new URLSearchParams({
    secret,
    issuer,
    algorithm: "SHA1",
    digits: String(digits),
    period: String(periodSec),
  });
  return `otpauth://totp/${encodeURIComponent(label)}?${query.toString()}`;
}

function generateTotpSecretValue(sizeBytes = 20): string {
  const bytes = createRandomByteArray(sizeBytes);
  if (!bytes.length) {
    return "";
  }

  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = 0;
  let value = 0;
  let output = "";

  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }

  if (bits > 0) {
    output += alphabet[(value << (5 - bits)) & 31];
  }

  return output.slice(0, 80);
}

function createRandomByteArray(sizeBytes: number): Uint8Array {
  const length = Math.min(Math.max(Number.parseInt(String(sizeBytes), 10) || 20, 10), 64);
  const bytes = new Uint8Array(length);

  if (typeof window !== "undefined" && window.crypto && typeof window.crypto.getRandomValues === "function") {
    window.crypto.getRandomValues(bytes);
    return bytes;
  }

  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Math.floor(Math.random() * 256);
  }

  return bytes;
}
