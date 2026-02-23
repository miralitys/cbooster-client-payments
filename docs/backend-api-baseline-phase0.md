# Backend Phase 0 Baseline (API Contracts)

This document freezes the **current baseline contracts** for critical backend APIs before server refactoring.

Scope:
- `records`
- `auth/session`
- `ghl basic-note`
- `ghl communications`
- `quickbooks`
- `leads`
- `moderation`

Rules for refactor phases:
- Do not change route paths.
- Do not change HTTP methods.
- Do not change response envelope keys.
- Do not change status-code behavior for existing scenarios.

## Authentication Baseline

Most `/api/*` routes in scope require a web-auth session cookie.

Unauthenticated behavior baseline:
- JSON API routes: `401/403` (or `302/303` if redirect flow is active).
- App pages under `/app/*`: `302/303` to `/login` (or HTML response with login page depending on runtime redirect handling).

## Contract Baseline by Endpoint

## 1) Health

- Method/Path: `GET /api/health`
- Auth: no auth required

200 (healthy):
```json
{
  "ok": true,
  "status": "healthy"
}
```

503 (unhealthy / db unavailable):
```json
{
  "ok": false,
  "status": "unhealthy"
}
```

## 2) Auth Session

- Method/Path: `GET /api/auth/session`
- Auth: session cookie expected

200:
```json
{
  "ok": true,
  "user": {
    "username": "owner@example.com"
  },
  "permissions": {}
}
```

## 3) Records

- Method/Path: `GET /api/records`
- Auth: `WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS`

200:
```json
{
  "records": [],
  "updatedAt": "2026-02-22T18:00:00.000Z"
}
```

503 (db not configured):
```json
{
  "error": "Database is not configured. Add DATABASE_URL in Render environment variables."
}
```

## 4) GHL Basic Note

- Method/Path: `GET /api/ghl/client-basic-note?clientName=...`
- Auth: `WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS`

200 (cache read path):
```json
{
  "ok": true,
  "clientName": "Some Client",
  "status": "found",
  "fromCache": true
}
```

403/503 error envelope:
```json
{
  "error": "..."
}
```

## 5) GHL Communications

- Method/Path: `GET /api/ghl/client-communications?clientName=...`
- Auth: `WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS`

200:
```json
{
  "ok": true,
  "items": []
}
```

503 (ghl not configured) or 403 (scope mismatch):
```json
{
  "error": "..."
}
```

## 6) GHL Communications Recording Proxy

- Method/Path: `GET /api/ghl/client-communications/recording?clientName=...&messageId=...`
- Auth: `WEB_AUTH_PERMISSION_VIEW_CLIENT_PAYMENTS`

400 (missing params):
```json
{
  "error": "Query parameters `clientName` and `messageId` are required."
}
```

200:
- Binary audio payload
- `Content-Type: audio/*`
- `Content-Disposition: inline; filename="ghl-recording-..."`

## 7) QuickBooks Recent Payments

- Method/Path: `GET /api/quickbooks/payments/recent`
- Auth: `WEB_AUTH_PERMISSION_VIEW_QUICKBOOKS`

200:
```json
{
  "ok": true,
  "range": { "from": "2026-02-01", "to": "2026-02-22" },
  "count": 0,
  "items": [],
  "source": "quickbooks_live"
}
```

400/503 envelope:
```json
{
  "error": "..."
}
```

## 8) GHL Leads

- Method/Path: `GET /api/ghl/leads?range=today`
- Auth: `WEB_AUTH_PERMISSION_VIEW_CLIENT_MANAGERS`

200:
```json
{
  "ok": true,
  "items": [],
  "count": 0
}
```

400/503 envelope:
```json
{
  "error": "..."
}
```

## 9) Moderation Submissions

- Method/Path: `GET /api/moderation/submissions?status=pending&limit=5`
- Auth: `WEB_AUTH_PERMISSION_VIEW_MODERATION`

200:
```json
{
  "status": "pending",
  "items": []
}
```

400/503 envelope:
```json
{
  "error": "..."
}
```

## Smoke Test Source of Truth

Automated baseline smoke checks are implemented in:
- `tests/api-baseline-smoke.test.js`

Run:
```bash
node --test tests/api-baseline-smoke.test.js
```
