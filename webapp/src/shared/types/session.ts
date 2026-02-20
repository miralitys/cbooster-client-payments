export type PermissionMap = Record<string, boolean>;

export interface AuthUser {
  username: string;
  displayName: string;
  roleId: string;
  roleName: string;
  departmentId: string;
  departmentName: string;
  isOwner: boolean;
  teamUsernames: string[];
  mustChangePassword?: boolean;
}

export interface Session {
  ok: boolean;
  user: AuthUser;
  permissions: PermissionMap;
}
