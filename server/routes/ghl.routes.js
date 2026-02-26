"use strict";

function registerGhlRoutes(context) {
  const {
    app,
    requireWebPermission,
    permissionKeys,
    handlers,
  } = context;

  app.post(
    "/api/ghl/contract-text",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlContractTextPost,
  );

  app.get(
    "/api/ghl/leads",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlLeadsGet,
  );

  app.post(
    "/api/ghl/leads/refresh",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlLeadsRefreshPost,
  );

  app.get(
    "/api/ghl/client-managers",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientManagersGet,
  );

  app.post(
    "/api/ghl/client-managers/refresh",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientManagersRefreshPost,
  );

  app.post(
    "/api/ghl/client-managers/refresh/background",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientManagersRefreshBackgroundPost,
  );

  app.get(
    "/api/ghl/client-managers/refresh/background-jobs/:jobId",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientManagersRefreshBackgroundJobGet,
  );

  app.post(
    "/api/ghl/client-phone/refresh",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientPhoneRefreshPost,
  );

  app.post("/api/ghl/client-contracts/archive", handlers.handleGhlClientContractsArchivePost);

  app.get(
    "/api/ghl/client-contracts",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientContractsGet,
  );

  app.get(
    "/api/ghl/client-contracts/download",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientContractsDownloadGet,
  );

  app.get(
    "/api/ghl/client-contracts/text",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS),
    handlers.handleGhlClientContractsTextGet,
  );

  app.get(
    "/api/ghl/client-basic-notes/refresh-all",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleGhlClientBasicNotesRefreshAllGet,
  );

  app.post(
    "/api/ghl/client-basic-notes/refresh-all",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_CLIENT_PAYMENTS),
    handlers.handleGhlClientBasicNotesRefreshAllPost,
  );

  app.get(
    "/api/ghl/client-basic-notes/missing",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientBasicNotesMissingGet,
  );

  app.get(
    "/api/ghl/client-basic-note",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientBasicNoteGet,
  );

  app.post(
    "/api/ghl/client-basic-note/refresh",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientBasicNoteRefreshPost,
  );

  app.get(
    "/api/ghl/client-communications",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientCommunicationsGet,
  );

  app.get(
    "/api/ghl/client-communications/recording",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientCommunicationsRecordingGet,
  );

  app.post(
    "/api/ghl/client-communications/transcript",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientCommunicationsTranscriptPost,
  );

  app.post(
    "/api/ghl/client-communications/normalize-transcripts",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS),
    handlers.handleGhlClientCommunicationsNormalizeTranscriptsPost,
  );
}

module.exports = {
  registerGhlRoutes,
};
