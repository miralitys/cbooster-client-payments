export type AppNotificationTone = "info" | "success" | "warning" | "error";

export interface AppNotificationLink {
  href: string;
  label: string;
}

export interface AppNotification {
  id: string;
  title: string;
  message?: string;
  tone: AppNotificationTone;
  createdAt: string;
  read: boolean;
  clientName?: string;
  link?: AppNotificationLink;
}

export interface AppNotificationPayload {
  title: string;
  message?: string;
  tone?: AppNotificationTone;
  clientName?: string;
  link?: Partial<AppNotificationLink>;
}
