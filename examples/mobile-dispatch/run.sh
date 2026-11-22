#!/usr/bin/env bash
# Submit the Router's charter once, then follow the resident Router's stream.
#
#   export DEEPSEEK_API_KEY=...
#   export KIAN_IMESSAGE_BRIDGE_TOKEN=...
#   ./run.sh
#
# The real interaction happens in Messages: text the bot's handle. This
# script only primes the session and tails its events.
set -euo pipefail
cd "$(dirname "$0")"

: "${KIAN_IMESSAGE_BRIDGE_TOKEN:?the bridge token is env-only (design/runbook-imessage.md §4)}"
export KIAN_IMESSAGE_BRIDGE_TOKEN

SID=00000000-0000-4000-8000-00000000c161
BASE=${KIAN_URL:-http://127.0.0.1:7461}

curl -s "$BASE/rpc" -H 'content-type: application/json' \
  -d "$(jq -cn --arg s "$SID" --arg c "$(cat charter.md)" \
        '{jsonrpc:"2.0", id:1, method:"submit",
          params:{session:$s, content:$c, idempotency_key:"charter-v1"}}')"
echo

curl -sN "$BASE/sessions/$SID/events"
