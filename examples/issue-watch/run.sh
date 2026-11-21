#!/usr/bin/env bash
# Submit the Router's charter once, then follow the resident Router's stream.
#
#   export DEEPSEEK_API_KEY=...            # or rely on kian's provider config
#   export KIAN_GITHUB_TOKEN="$(gh auth token)"
#   ./run.sh                                # dry-run: drafts never post
#   DRY_RUN=0 ./run.sh                      # let the worker post comments
set -euo pipefail
cd "$(dirname "$0")"

: "${KIAN_GITHUB_TOKEN:=$(gh auth token 2>/dev/null || true)}"
export KIAN_GITHUB_TOKEN
: "${DRY_RUN:=1}"
export DRY_RUN

SID=00000000-0000-4000-8000-00000000c060
BASE=${KIAN_URL:-http://127.0.0.1:7460}

curl -s "$BASE/rpc" -H 'content-type: application/json' \
  -d "$(jq -cn --arg s "$SID" --arg c "$(cat charter.md)" \
        '{jsonrpc:"2.0", id:1, method:"submit",
          params:{session:$s, content:$c, idempotency_key:"charter-v1"}}')"
echo

curl -sN "$BASE/sessions/$SID/events"
