export { ApiError, apiRequest } from "@/shared/api/fetcher";
export { getSession } from "@/shared/api/session";
export {
  getNotificationsFeed,
  getNotificationsPushPublicKey,
  markAllNotificationsRead,
  markNotificationRead,
  subscribeNotificationsPush,
  unsubscribeNotificationsPush,
} from "@/shared/api/notifications";
export {
  getSupportRequests,
  getSupportRequest,
  createSupportRequest,
  updateSupportRequest,
  moveSupportRequest,
  addSupportAttachments,
  addSupportComment,
  getSupportReports,
  getSupportAttachmentDownloadUrl,
} from "@/shared/api/support";
export { getRecords, patchRecords, putRecords } from "@/shared/api/records";
export { getClientHealth } from "@/shared/api/clientHealth";
export { getClients, getClientsPage, getClientFilterOptions, patchClients, putClients } from "@/shared/api/clients";
export { queueAssistantSessionContextResetBeacon, resetAssistantSessionContext, sendAssistantMessage } from "@/shared/api/assistant";
export {
  approveModerationSubmission,
  getModerationSubmissionFiles,
  getModerationSubmissions,
  rejectModerationSubmission,
} from "@/shared/api/moderation";
export {
  confirmQuickBooksRecentPayment,
  createQuickBooksSyncJob,
  getQuickBooksPendingConfirmations,
  getQuickBooksOutgoingPayments,
  getQuickBooksPayments,
  getQuickBooksSyncJob,
  getQuickBooksTransactionInsight,
} from "@/shared/api/quickbooks";
export { getClientManagers, startClientManagersRefreshBackgroundJob } from "@/shared/api/clientManagers";
export { getGhlLeads } from "@/shared/api/ghlLeads";
export { getGhlClientBasicNote } from "@/shared/api/ghlNotes";
export {
  getGhlClientCommunications,
  postGhlClientCommunicationNormalizeTranscripts,
  postGhlClientCommunicationTranscript,
} from "@/shared/api/ghlCommunications";
export { createAccessUser, deleteAccessUser, getAccessModel, listAccessUsers, updateAccessUser } from "@/shared/api/accessControl";
export { listAssistantReviews, updateAssistantReview } from "@/shared/api/accessControl";
export {
  getCustomDashboard,
  getCustomDashboardUsers,
  saveCustomDashboardUsers,
  syncCustomDashboardCalls,
  syncCustomDashboardTasks,
  updateCustomDashboardTasksSource,
  uploadCustomDashboardFile,
} from "@/shared/api/customDashboard";
export { getIdentityIqCreditScore } from "@/shared/api/identityIq";
export { getGhlContractText } from "@/shared/api/ghlContractText";
export { getGhlContractPdf } from "@/shared/api/ghlContractPdf";
export { getGhlContractTerms, getGhlContractTermsRecent, getGhlContractTermsCache } from "@/shared/api/ghlContractTerms";
export { postGhlClientPhoneRefresh } from "@/shared/api/ghlClientPhone";
