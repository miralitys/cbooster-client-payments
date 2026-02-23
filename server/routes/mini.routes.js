"use strict";

function registerMiniRoutes(context) {
  const {
    app,
    handlers,
  } = context;

  app.post("/api/mini/access", handlers.handleMiniAccessPost);
  app.post("/api/mini/clients", handlers.handleMiniClientsPost);

  app.get("/mini", handlers.handleMiniPageGet);
  app.get("/mini.html", handlers.handleMiniPageGet);
}

module.exports = {
  registerMiniRoutes,
};
