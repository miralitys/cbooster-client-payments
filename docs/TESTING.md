# Testing Notes

## `tests/server-bootstrap.test.js`

Purpose:
- verifies `server.js` can be imported in `NODE_ENV=test`,
- confirms the module exports `app` and `startServer`,
- confirms there is no accidental auto-listen during import-only bootstrap.

Stability hardening:
- the test now spawns a child process with deterministic env (no DB/external integrations),
- sets `SERVER_AUTOSTART_IN_TEST=false` explicitly,
- uses an explicit success exit (`process.exit(0)`) after export checks,
- uses a larger timeout window to avoid cross-suite CPU contention.

This keeps the test focused on bootstrap/export behavior and removes flakiness from unrelated background handles or ambient env state.
