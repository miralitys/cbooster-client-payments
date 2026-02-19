import { useCallback, useEffect, useMemo, useState } from "react";

import { createAccessUser, getAccessModel, listAssistantReviews, updateAccessUser, updateAssistantReview } from "@/shared/api";
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
}

const EMPTY_FORM: UserFormState = {
  username: "",
  password: "",
  displayName: "",
  departmentId: "",
  roleId: "",
  teamUsernames: "",
};

interface AssistantReviewDraft {
  correctedReply: string;
  correctionNote: string;
}

const ASSISTANT_REVIEW_LIMIT = 80;

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
    "Fill out the form to create a new user. Username and password are optional.",
  );
  const [createStatusError, setCreateStatusError] = useState(false);
  const [isCreating, setIsCreating] = useState(false);

  const [editingOriginalUsername, setEditingOriginalUsername] = useState("");
  const [editForm, setEditForm] = useState<UserFormState>(EMPTY_FORM);
  const [editStatusText, setEditStatusText] = useState("Update user data and click Save Changes.");
  const [editStatusError, setEditStatusError] = useState(false);
  const [isUpdating, setIsUpdating] = useState(false);

  const [assistantReviews, setAssistantReviews] = useState<AssistantReviewItem[]>([]);
  const [assistantReviewsTotal, setAssistantReviewsTotal] = useState(0);
  const [assistantReviewsLoading, setAssistantReviewsLoading] = useState(false);
  const [assistantReviewsError, setAssistantReviewsError] = useState("");
  const [assistantReviewsStatusText, setAssistantReviewsStatusText] = useState("Owner review queue is empty.");
  const [assistantReviewDrafts, setAssistantReviewDrafts] = useState<Record<string, AssistantReviewDraft>>({});
  const [assistantReviewSavingIds, setAssistantReviewSavingIds] = useState<Record<string, boolean>>({});

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
    } finally {
      setIsLoading(false);
    }
  }, [loadAssistantReviewQueue]);

  useEffect(() => {
    void loadAccessModel();
  }, [loadAccessModel]);

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
    ];
  }, [canManageAccess]);

  function openEditModal(user: AuthUser) {
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
      },
      departments,
    );

    setEditingOriginalUsername(user.username);
    setEditForm(next);
    setEditStatusText(`Editing "${user.displayName || user.username}".`);
    setEditStatusError(false);
  }

  function closeEditModal() {
    setEditingOriginalUsername("");
    setEditForm(EMPTY_FORM);
    setEditStatusText("Update user data and click Save Changes.");
    setEditStatusError(false);
    setIsUpdating(false);
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

    setIsCreating(true);
    setCreateStatusText("Creating user...");
    setCreateStatusError(false);

    try {
      const payload = buildUpsertPayload(createForm);
      const response = await createAccessUser(payload);
      const createdUsername = response?.item?.username || payload.username || createForm.displayName;
      setCreateStatusText(`User "${createdUsername}" created.`);
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
      setRegistrationOpen(false);
      await loadAccessModel();
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to create user.";
      setCreateStatusText(message);
      setCreateStatusError(true);
    } finally {
      setIsCreating(false);
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

  const enabledPermissionsCount = useMemo(() => {
    return Object.values(permissions || {}).filter(Boolean).length;
  }, [permissions]);

  const isOwnerUser = Boolean(currentUser?.isOwner);

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
              onClick={() => setRegistrationOpen((previous) => !previous)}
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
            <CurrentAccessLine label="Enabled Permissions" value={String(enabledPermissionsCount)} />
          </div>
        ) : null}
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
                return (
                  <article key={`assistant-review-${item.id}`} className="access-control-assistant-review-card-react">
                    <header className="access-control-assistant-review-meta-react">
                      <span>#{item.id}</span>
                      <span>{formatDateTime(item.askedAt)}</span>
                      <span>{item.askedByDisplayName || item.askedByUsername || "-"}</span>
                      <span>{item.mode === "voice" ? "Voice" : "Text"}</span>
                      <span>{item.provider || "-"}</span>
                      <span>{item.recordsUsed} records</span>
                    </header>

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

      {registrationOpen && canManageAccess ? (
        <Panel title="User Registration">
          <p className={`dashboard-message ${createStatusError ? "error" : ""}`.trim()}>{createStatusText}</p>

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

            <Field label="Password (optional)" htmlFor="access-create-password">
              <Input
                id="access-create-password"
                type="password"
                value={createForm.password}
                onChange={(event) => setCreateForm((previous) => ({ ...previous, password: event.target.value }))}
                placeholder="Optional: at least 8 characters"
                disabled={isCreating}
              />
            </Field>

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

      <Modal
        open={Boolean(editingOriginalUsername && canManageAccess && editingUser)}
        title="Edit User"
        onClose={closeEditModal}
        footer={
          <div className="access-control-form-actions-react access-control-form-actions-react--end">
            <Button type="button" variant="secondary" onClick={closeEditModal} disabled={isUpdating}>
              Cancel
            </Button>
            <Button type="submit" form="access-control-edit-user-form" isLoading={isUpdating} disabled={isUpdating}>
              Save Changes
            </Button>
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
              disabled={isUpdating}
            />
          </Field>

          <Field label="New Password (optional)" htmlFor="access-edit-password">
            <Input
              id="access-edit-password"
              type="password"
              value={editForm.password}
              onChange={(event) => setEditForm((previous) => ({ ...previous, password: event.target.value }))}
              placeholder="Leave empty to keep current password"
              disabled={isUpdating}
            />
          </Field>

          <Field label="Display Name" htmlFor="access-edit-display-name">
            <Input
              id="access-edit-display-name"
              value={editForm.displayName}
              onChange={(event) => setEditForm((previous) => ({ ...previous, displayName: event.target.value }))}
              required
              disabled={isUpdating}
            />
          </Field>

          <Field label="Department" htmlFor="access-edit-department">
            <Select
              id="access-edit-department"
              value={editForm.departmentId}
              onChange={(event) => onEditDepartmentChange(event.target.value)}
              disabled={isUpdating}
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
              disabled={isUpdating}
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
                disabled={isUpdating}
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
  };

  const username = form.username.trim();
  if (username) {
    payload.username = username;
  }

  const password = form.password.trim();
  if (password) {
    payload.password = password;
  }

  return payload;
}

function parseTeamUsernames(rawValue: string): string[] {
  return String(rawValue || "")
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}
