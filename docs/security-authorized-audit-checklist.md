# Authorized Security Audit Checklist (AuthZ + Payments + Integrations)

Use this checklist for the credentialed phase that cannot be validated from anonymous external testing.

## Preconditions

- Use dedicated test accounts: `owner`, `admin`, `manager`, `read-only`.
- Use only test data (no production PII).
- Run in staging first, then replay in production-safe read-only mode.

## 1) AuthZ / IDOR / BOLA

- Verify list/detail/update/delete endpoints enforce tenant and role boundaries.
- For each resource (`records`, `payments`, `clients`, `users`):
  - User A tries to access User B resource by ID.
  - User A tries to patch status/amount/owner fields outside permission scope.
  - Manager vs admin vs owner checks for every write endpoint.
- Expected:
  - Cross-tenant or cross-owner access is `403/404`.
  - No partial data leakage in denied responses.

## 2) Payment Business Logic

- Validate invariants on write endpoints:
  - `totalPayments <= contractTotals`
  - no negative values unless explicitly allowed by business rules.
  - no status transitions that bypass required checks.
- Replay/race checks:
  - duplicate submit with same payload/idempotency key.
  - quick repeated status/amount updates from two sessions.
- Expected:
  - deterministic conflict handling (`409` or idempotent replay),
  - no double-apply, no balance underflow/overflow.

## 3) Webhook / Import / SSRF Safety

- Validate every URL-fetch/import path rejects:
  - internal addresses (`127.0.0.1`, `10.0.0.0/8`, `169.254.169.254`, `*.internal`),
  - non-HTTP schemes (`file://`, `gopher://`, `ftp://`),
  - open redirects to blocked destinations.
- Expected:
  - blocked requests return safe `4xx`,
  - logs include reason code without sensitive internals.

## 4) File Upload / Download Handling

- Validate file type, extension, MIME, magic-bytes checks.
- Validate path traversal resistance on download endpoints.
- Validate access control on file IDs (no cross-user access).
- Expected:
  - dangerous files blocked,
  - file download only for authorized principals.

## 5) Evidence to Capture

- Request/response pairs (sanitized),
- exact endpoint + payload class,
- role/account used,
- expected vs actual result,
- fix recommendation and retest result.
