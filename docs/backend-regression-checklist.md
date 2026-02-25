# Backend Regression Checklist (Phase 0 Baseline)

Use this checklist after each backend refactor step.

## Automated

1. Build web app:
```bash
npm run build
```

2. Run backend smoke baseline:
```bash
node --test tests/api-baseline-smoke.test.js
```

3. (Optional full backend suite):
```bash
npm run test:backend
```

## Manual Sanity (local)

1. Start server locally.
2. `GET /api/health` (без ключа) returns `200` with minimal `{ ok: true }` and no detailed DB fields.
3. Open `/app/client-payments` as unauth user:
   - must **not** return `404`.
   - expected redirect/login behavior is preserved.
4. `GET /api/records` without auth:
   - must return `401/302/403` (not `500`).
5. Log in as owner/admin and check:
   - `/api/auth/session`
   - `/api/records`
   - `/api/ghl/client-basic-note?clientName=...`
   - `/api/ghl/client-communications?clientName=...`
   - `/api/quickbooks/payments/recent`
   - `/api/ghl/leads?range=today`
   - `/api/moderation/submissions?status=pending&limit=5`
6. Confirm each endpoint returns JSON envelope (or binary for recording proxy) and expected status family.
7. Confirm no unexpected `500` in server logs.

## Refactor Guardrails

1. Keep API paths/methods stable.
2. Keep response envelope keys stable.
3. Keep RBAC middleware on same routes.
4. Do not add write operations to external integrations (GHL/QuickBooks).
