# DB Access Rules

## Source of truth
All database operations must go through the shared DB wrapper layer:

- `server/shared/db/pool.js` for pool initialization only.
- `server/shared/db/query.js` for query wrapper and logging.
- `server/shared/db/tx.js` for transaction wrapper.
- `server/shared/db/errors.js` for DB error mapping.

Direct `pg` usage is allowed only in `server/shared/db/pool.js`.

## Where SQL is allowed
SQL statements are allowed only in:

- `server/domains/**/**.repo.js`
- `server/shared/db/**`

## Where SQL is forbidden
SQL statements are forbidden in:

- `server-legacy.js`
- `server/routes/**`
- `server/domains/**/**.controller.js`
- `server/domains/**/**.service.js`

## Required flow
Use this flow for all backend DB behavior:

`route -> controller -> service -> repo -> shared db query/tx`

## Adding a new query
If you need a new DB query:

1. Add or extend a function in the relevant repo file.
2. Call that repo function from service/controller.
3. Do not embed SQL directly into route handlers, controllers, services, or `server-legacy.js`.

## Why this matters
Keeping SQL isolated in repos and shared wrappers reduces regression risk during refactors, centralizes logging/observability for DB calls, and keeps behavior testable with clearer module boundaries.
