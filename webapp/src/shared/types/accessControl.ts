import type { AuthUser, PermissionMap } from "@/shared/types/session";

export interface AccessControlRoleOption {
  id: string;
  name: string;
}

export interface AccessControlRoleMember {
  username: string;
  displayName: string;
  roleId: string;
  roleName: string;
}

export interface AccessControlDepartmentRole {
  id: string;
  name: string;
  members: AccessControlRoleMember[];
}

export interface AccessControlDepartment {
  id: string;
  name: string;
  roles: AccessControlDepartmentRole[];
}

export interface AccessControlTotpSettings {
  issuer: string;
  periodSec: number;
  digits: number;
}

export interface AccessControlModel {
  ownerUsername: string;
  totp?: AccessControlTotpSettings;
  roles: AccessControlRoleOption[];
  departments: AccessControlDepartment[];
  users: AuthUser[];
}

export interface AccessModelPayload {
  ok: boolean;
  user: AuthUser;
  permissions: PermissionMap;
  accessModel: AccessControlModel;
}

export interface UsersPayload {
  ok: boolean;
  count: number;
  items: AuthUser[];
}

export interface UpsertUserPayload {
  username?: string;
  password?: string;
  displayName: string;
  departmentId: string;
  roleId: string;
  teamUsernames?: string[];
  totpSecret?: string;
  totpEnabled?: boolean;
}

export interface UpsertUserResponse {
  ok: boolean;
  item: AuthUser;
  temporaryPassword?: string;
}

export interface AssistantReviewItem {
  id: number;
  askedAt: string | null;
  askedByUsername: string;
  askedByDisplayName: string;
  mode: "text" | "voice" | "gpt";
  question: string;
  assistantReply: string;
  provider: string;
  recordsUsed: number;
  correctedReply: string;
  correctionNote: string;
  correctedBy: string;
  correctedAt: string | null;
}

export interface AssistantReviewListPayload {
  ok: boolean;
  total: number;
  count: number;
  limit: number;
  offset: number;
  items: AssistantReviewItem[];
}

export interface AssistantReviewUpdatePayload {
  correctedReply?: string;
  correctionNote?: string;
  markCorrect?: boolean;
}

export interface AssistantReviewUpdateResponse {
  ok: boolean;
  item: AssistantReviewItem;
}
