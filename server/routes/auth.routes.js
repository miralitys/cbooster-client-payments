"use strict";

function registerAuthPublicRoutes(context) {
  const {
    app,
    requireWebApiCsrf,
    handlers,
  } = context;

  app.get("/login", handlers.handleWebLoginPage);
  app.post("/login", handlers.handleWebLoginSubmit);

  app.post("/api/auth/login", handlers.handleApiAuthLogin);
  app.post("/api/auth/logout", requireWebApiCsrf, handlers.handleApiAuthLogout);
  app.post("/api/mobile/auth/login", handlers.handleApiAuthLogin);
  app.post("/api/mobile/auth/logout", requireWebApiCsrf, handlers.handleApiAuthLogout);

  app.get("/logout", handlers.handleWebLogout);
  app.post("/logout", handlers.handleWebLogout);
}

function registerAuthProtectedRoutes(context) {
  const {
    app,
    requireWebPermission,
    requireOwnerOrAdminAccess,
    permissionKeys,
    handlers,
  } = context;

  app.get("/first-password", handlers.handleWebFirstPasswordPage);
  app.post("/first-password", handlers.handleWebFirstPasswordSubmit);

  app.post("/api/auth/first-password", handlers.handleApiAuthFirstPassword);
  app.post("/api/mobile/auth/first-password", handlers.handleApiAuthFirstPassword);

  app.get("/api/auth/session", handlers.handleApiAuthSession);
  app.get("/api/mobile/auth/session", handlers.handleApiAuthSession);

  app.get(
    "/api/auth/access-model",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleApiAuthAccessModel,
  );

  app.get(
    "/api/auth/users",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleApiAuthUsersList,
  );
  app.post(
    "/api/auth/users",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleApiAuthUsersCreate,
  );
  app.put(
    "/api/auth/users/:username",
    requireWebPermission(permissionKeys.WEB_AUTH_PERMISSION_MANAGE_ACCESS_CONTROL),
    handlers.handleApiAuthUsersUpdate,
  );
  app.delete(
    "/api/auth/users/:username",
    requireOwnerOrAdminAccess(),
    handlers.handleApiAuthUsersDelete,
  );
}

module.exports = {
  registerAuthPublicRoutes,
  registerAuthProtectedRoutes,
};
