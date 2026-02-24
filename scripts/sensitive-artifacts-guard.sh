#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[guard] Sensitive artifacts guardrails"

declare -a VIOLATIONS=()

is_allowed_env_template() {
  local file_name="$1"
  case "$file_name" in
    .env.example|.env.sample|.env.template)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

while IFS= read -r -d '' tracked_file; do
  case "$tracked_file" in
    backups|backups/*|coverage|coverage/*|.codex-temp|.codex-temp/*)
      VIOLATIONS+=("$tracked_file :: blocked artifact directory")
      continue
      ;;
  esac

  if [[ "$tracked_file" =~ \.(tar|tgz|zip|7z|dump|bak)$ ]] || [[ "$tracked_file" =~ \.tar\.gz$ ]]; then
    VIOLATIONS+=("$tracked_file :: archive/dump artifact must not be tracked")
    continue
  fi

  if [[ "$tracked_file" =~ (^|/)\.env($|\.|_) ]] || [[ "$tracked_file" =~ (^|/)\.envrc$ ]]; then
    file_name="$(basename "$tracked_file")"
    if ! is_allowed_env_template "$file_name"; then
      VIOLATIONS+=("$tracked_file :: env-like sensitive file must not be tracked")
      continue
    fi
  fi

done < <(git ls-files -z)

if (( ${#VIOLATIONS[@]} > 0 )); then
  echo "FAIL: sensitive artifacts detected in tracked files"
  printf '%s\n' "${VIOLATIONS[@]}"
  exit 1
fi

echo "PASS: no blocked sensitive artifacts in tracked files"
