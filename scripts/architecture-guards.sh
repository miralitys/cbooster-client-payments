#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

EXIT_CODE=0
IGNORE_FILE="scripts/architecture-guards.ignore"

echo "[guard] Architecture guardrails"

fail() {
  local message="$1"
  echo "FAIL: ${message}"
  EXIT_CODE=1
}

warn() {
  local message="$1"
  echo "WARN: ${message}"
}

pass() {
  local message="$1"
  echo "PASS: ${message}"
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

declare -a IGNORE_GLOBS=()
declare -a IGNORE_REGEX=()

if [[ -f "${IGNORE_FILE}" ]]; then
  while IFS= read -r rawLine || [[ -n "${rawLine}" ]]; do
    localLine="$(trim "${rawLine}")"
    [[ -z "${localLine}" ]] && continue
    [[ "${localLine}" == \#* ]] && continue
    if [[ "${localLine}" == regex:* ]]; then
      IGNORE_REGEX+=("${localLine#regex:}")
    else
      IGNORE_GLOBS+=("${localLine}")
    fi
  done < "${IGNORE_FILE}"
fi

is_ignored_match() {
  local matchLine="$1"
  local matchFile="${matchLine%%:*}"
  local rule=""
  for rule in "${IGNORE_GLOBS[@]-}"; do
    [[ -z "${rule}" ]] && continue
    if [[ "${matchFile}" == ${rule} ]]; then
      return 0
    fi
  done
  for rule in "${IGNORE_REGEX[@]-}"; do
    [[ -z "${rule}" ]] && continue
    if [[ "${matchLine}" =~ ${rule} ]]; then
      return 0
    fi
  done
  return 1
}

apply_ignore_rules() {
  local rawMatches="$1"
  local filtered=()
  local line=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    [[ -z "${line}" ]] && continue
    if is_ignored_match "${line}"; then
      continue
    fi
    filtered+=("${line}")
  done <<< "${rawMatches}"
  if [[ "${#filtered[@]}" -gt 0 ]]; then
    printf '%s\n' "${filtered[@]}"
  fi
}

declare -a BACKEND_PATHS=(
  "server-legacy.js"
  "server.js"
  "server"
  "custom-dashboard-module.js"
  "attachments-storage-utils.js"
  "client-records-v2-utils.js"
  "records-patch-utils.js"
  "assistant-session-scope-identity-utils.js"
)
declare -a BACKEND_FILE_GLOBS=(
  "--glob" "*.js"
  "--glob" "!docs/**"
  "--glob" "!tests/**"
  "--glob" "!**/dist/**"
  "--glob" "!coverage/**"
)

echo "[guard] Rule 1: no pool.query/client.query in server-legacy.js"
LEGACY_QUERY_MATCHES="$(rg -n --no-heading -e '\bpool\.query\(' -e '\bclient\.query\(' server-legacy.js || true)"
LEGACY_QUERY_MATCHES="$(apply_ignore_rules "${LEGACY_QUERY_MATCHES}")"
if [[ -n "${LEGACY_QUERY_MATCHES}" ]]; then
  fail "Direct DB calls found in server-legacy.js (use repo + shared db wrapper)."
  echo "${LEGACY_QUERY_MATCHES}"
else
  pass "No direct pool.query/client.query in server-legacy.js"
fi

echo "[guard] Rule 2: pg import allowed only in server/shared/db/pool.js"
ALLOWED_PG_IMPORT_FILE="server/shared/db/pool.js"
PG_IMPORT_MATCHES="$(rg -n --no-heading "${BACKEND_FILE_GLOBS[@]}" -e 'require\(["'"'"']pg["'"'"']\)' -e 'from ["'"'"']pg["'"'"']' "${BACKEND_PATHS[@]}" || true)"
PG_IMPORT_MATCHES="$(apply_ignore_rules "${PG_IMPORT_MATCHES}")"
if [[ -n "${PG_IMPORT_MATCHES}" ]]; then
  INVALID_PG_IMPORTS="$(echo "${PG_IMPORT_MATCHES}" | awk -F: -v allowed="${ALLOWED_PG_IMPORT_FILE}" '$1 != allowed')"
  if [[ -n "${INVALID_PG_IMPORTS}" ]]; then
    fail "pg import detected outside ${ALLOWED_PG_IMPORT_FILE}"
    echo "${INVALID_PG_IMPORTS}"
  else
    pass "pg import is restricted to ${ALLOWED_PG_IMPORT_FILE}"
  fi
else
  pass "No pg imports detected in backend paths"
fi

SQL_PATTERN='\b(SELECT|INSERT|UPDATE|DELETE)\s'
SQL_CONTEXT_PATTERN='(^\s*(SELECT|INSERT|UPDATE|DELETE)\s+|["'"'"'`]\s*(SELECT|INSERT|UPDATE|DELETE)\s+)'

echo "[guard] Rule 3a: SQL forbidden in routes/controllers/services"
FORBIDDEN_SQL_MATCHES_NON_LEGACY="$(
  rg -n --no-heading -i "${BACKEND_FILE_GLOBS[@]}" -e "${SQL_CONTEXT_PATTERN}" \
    server/routes server/domains || true
)"
FORBIDDEN_SQL_MATCHES_NON_LEGACY="$(
  echo "${FORBIDDEN_SQL_MATCHES_NON_LEGACY}" | awk -F: '{
    file=$1
    if (file ~ /^server\/routes\/.*\.js$/) { print; next }
    if (file ~ /^server\/domains\/.*\.controller\.js$/) { print; next }
    if (file ~ /^server\/domains\/.*\.service\.js$/) { print; next }
  }'
)"
FORBIDDEN_SQL_MATCHES_NON_LEGACY="$(apply_ignore_rules "${FORBIDDEN_SQL_MATCHES_NON_LEGACY}")"
if [[ -n "${FORBIDDEN_SQL_MATCHES_NON_LEGACY}" ]]; then
  fail "SQL found in forbidden routes/controller/service files. Move SQL to repo."
  echo "${FORBIDDEN_SQL_MATCHES_NON_LEGACY}"
else
  pass "No SQL in routes/controllers/services"
fi

echo "[guard] Rule 3b: SQL in server-legacy.js is informational during migration"
LEGACY_SQL_MATCHES="$(rg -n --no-heading -i "${BACKEND_FILE_GLOBS[@]}" -e "${SQL_CONTEXT_PATTERN}" server-legacy.js || true)"
LEGACY_SQL_MATCHES="$(apply_ignore_rules "${LEGACY_SQL_MATCHES}")"
LEGACY_SQL_COUNT="$(printf '%s' "${LEGACY_SQL_MATCHES}" | awk 'NF{count+=1} END{print count+0}')"
if (( LEGACY_SQL_COUNT > 0 )); then
  warn "server-legacy.js still contains SQL markers (${LEGACY_SQL_COUNT}). Migration is in progress."
else
  pass "No SQL markers found in server-legacy.js"
fi

echo "[guard] Rule 3c: SQL outside repo/shared is warned"
ALL_BACKEND_SQL_MATCHES="$(rg -n --no-heading -i "${BACKEND_FILE_GLOBS[@]}" -e "${SQL_CONTEXT_PATTERN}" "${BACKEND_PATHS[@]}" || true)"
ALL_BACKEND_SQL_MATCHES="$(apply_ignore_rules "${ALL_BACKEND_SQL_MATCHES}")"
if [[ -n "${ALL_BACKEND_SQL_MATCHES}" ]]; then
  NON_REPO_SQL="$(echo "${ALL_BACKEND_SQL_MATCHES}" | awk -F: '{
    file=$1
    if (file ~ /^server\/domains\/.*\.repo\.js$/) next
    if (file ~ /^server\/shared\/db\//) next
    if (file == "server-legacy.js") next
    print
  }')"
  if [[ -n "${NON_REPO_SQL}" ]]; then
    warn "SQL detected outside repo/shared files (review manually)."
    echo "${NON_REPO_SQL}"
  else
    pass "SQL appears only in repo/shared files"
  fi
else
  pass "No SQL keywords detected in backend paths"
fi

if [[ "${EXIT_CODE}" -ne 0 ]]; then
  echo "[guard] FAILED"
  exit "${EXIT_CODE}"
fi

echo "[guard] PASSED"
