import type { Session } from "@/shared/types/session";

export function isOwnerOrAdminSession(session: Session | null | undefined): boolean {
  return Boolean(session?.user?.isOwner || session?.permissions?.manage_access_control);
}

function normalizeSessionIdentity(rawValue: string | null | undefined): string {
  return (rawValue || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

export function isClientServiceDepartmentHeadSession(session: Session | null | undefined): boolean {
  const roleId = normalizeSessionIdentity(session?.user?.roleId);
  const departmentId = normalizeSessionIdentity(session?.user?.departmentId);
  return roleId === "department_head" && departmentId === "client_service";
}

export function isAccountingDepartmentSession(session: Session | null | undefined): boolean {
  const departmentId = normalizeSessionIdentity(session?.user?.departmentId);
  return departmentId === "accounting";
}

export function canViewClientMatchSession(session: Session | null | undefined): boolean {
  return isOwnerOrAdminSession(session) || isAccountingDepartmentSession(session);
}

export function canRefreshClientManagerFromGhlSession(session: Session | null | undefined): boolean {
  return isOwnerOrAdminSession(session) || isClientServiceDepartmentHeadSession(session);
}

export function canRefreshClientPhoneFromGhlSession(session: Session | null | undefined): boolean {
  return canRefreshClientManagerFromGhlSession(session);
}
