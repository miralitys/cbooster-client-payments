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

export interface AccessControlModel {
  ownerUsername: string;
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
}

export interface UpsertUserResponse {
  ok: boolean;
  item: AuthUser;
}
