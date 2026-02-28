"use strict";

function registerAuthPublicRoutes(context) {
  const {
    app,
    requireWebApiCsrf,
    requireAuthStateChangeProtection,
    handlers,
  } = context;

  app.get("/login", handlers.handleWebLoginPage);
  app.post("/login", requireAuthStateChangeProtection, handlers.handleWebLoginSubmit);

  app.post("/api/auth/login", requireAuthStateChangeProtection, handlers.handleApiAuthLogin);
  app.post("/api/auth/logout", requireAuthStateChangeProtection, requireWebApiCsrf, handlers.handleApiAuthLogout);
  app.post("/api/mobile/auth/login", handlers.handleApiAuthLogin);
  app.post("/api/mobile/auth/logout", requireWebApiCsrf, handlers.handleApiAuthLogout);

  app.get("/.well-known/security.txt", handlers.handleSecurityTxtGet);
  app.get("/logout", handlers.handleWebLogoutMethodNotAllowed);
  app.post("/logout", requireAuthStateChangeProtection, handlers.handleWebLogout);
}

function registerAuthProtectedRoutes(context) {
  const {
    app,
    requireWebPermission,
    requireOwnerOrAdminAccess,
    requireStrictOwnerOrAdminAccess,
    permissionKeys,
    handlers,
  } = context;
  const requireUsersManageAccess =
    typeof requireStrictOwnerOrAdminAccess === "function"
      ? requireStrictOwnerOrAdminAccess("Owner or admin access is required.")
      : requireOwnerOrAdminAccess("Owner or admin access is required.");

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
    requireUsersManageAccess,
    handlers.handleApiAuthUsersList,
  );
  app.post(
    "/api/auth/users",
    requireUsersManageAccess,
    handlers.handleApiAuthUsersCreate,
  );
  app.put(
    "/api/auth/users/:username",
    requireUsersManageAccess,
    handlers.handleApiAuthUsersUpdate,
  );
  app.delete(
    "/api/auth/users/:username",
    requireUsersManageAccess,
    handlers.handleApiAuthUsersDelete,
  );
}

module.exports = {
  registerAuthPublicRoutes,
  registerAuthProtectedRoutes,
};
