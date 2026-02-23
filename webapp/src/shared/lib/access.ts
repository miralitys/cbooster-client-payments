import type { Session } from "@/shared/types/session";

export function isOwnerOrAdminSession(session: Session | null | undefined): boolean {
  return Boolean(session?.user?.isOwner || session?.permissions?.manage_access_control);
}
