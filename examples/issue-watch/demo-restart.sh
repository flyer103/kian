#!/usr/bin/env bash
# demo-restart.sh — the story on a real workload (kian examples/issue-watch v2).
#
# Boots the issue-watch, lets at least one poll flow, kill -9's the
# server mid-watch, reboots it on the SAME sqlite event store, and
# shows the SAME Router session resuming from the event stream: seq
# keeps growing (never resets), the passthrough registry / outbox /
# speak tracker / poll cursor are all rebuilt or re-derived from the
# streams, and already-admitted github events dedupe by event id (the
# boot-derived since cursor means the source is not re-delivered).
#
# Options (env):
#   KEEP_STATE=1 do not wipe.state first (default: fresh demo)
#   CREATE_ISSUE=1 also create a throwaway issue on flyer103/kian before
#                  the kill, so the post-restart poll sees real new work
#                  (closes the issue afterwards)

set -euo pipefail
cd "$(dirname "$0")"

export NO_PROXY="127.0.0.1,localhost"
export no_proxy="127.0.0.1,localhost"
export DRY_RUN="${DRY_RUN:-1}"
INTERVAL_MS="$(sed -n 's/^ *interval_ms: *//p' config.yaml | head -1)"
INTERVAL_S=$(( INTERVAL_MS / 1000 ))

KIAN_BIN="${KIAN_BIN:-../../target/debug/kian}"
[[ -x "$KIAN_BIN" ]] || { echo "demo-restart.sh: build first: cargo build --workspace" >&2; exit 1; }
[[ -f ~/.hermes/.env ]] || { echo "demo-restart.sh: ~/.hermes/.env missing" >&2; exit 1; }
set -a; source ~/.hermes/.env; set +a
[[ -n "${DEEPSEEK_API_KEY:-}" ]] || { echo "demo-restart.sh: DEEPSEEK_API_KEY absent" >&2; exit 1; }
# The github poll + send token (env-only; gh keyring fallback).
if [[ -z "${KIAN_GITHUB_TOKEN:-}" ]]; then
  KIAN_GITHUB_TOKEN="$(gh auth token 2>/dev/null || true)"
  [[ -n "$KIAN_GITHUB_TOKEN" ]] && export KIAN_GITHUB_TOKEN
fi
[[ -n "${KIAN_GITHUB_TOKEN:-}" ]] || { echo "demo-restart.sh: KIAN_GITHUB_TOKEN absent (gh auth token failed)" >&2; exit 1; }

ROUTER_SESSION="$(sed -n 's/^ *session_id: *"\(.*\)"/\1/p' config.yaml | head -1)"

if [[ "${KEEP_STATE:-0}" != "1" ]]; then
  rm -rf .state
fi
mkdir -p .state/tools
: > .state/serve.log

boot() {
  : >> .state/serve.log
  "$KIAN_BIN" serve --config config.yaml >> .state/serve.log 2>&1 &
  SERVE_PID=$!
  echo "$SERVE_PID" > .state/serve.pid
  for _ in $(seq 1 100); do
    curl -s -o /dev/null --noproxy '*' "http://127.0.0.1:7460/metrics" && return 0
    kill -0 "$SERVE_PID" 2>/dev/null || { tail -20 .state/serve.log >&2; exit 1; }
    sleep 0.2
  done
  echo "demo-restart.sh: serve did not become ready" >&2; exit 1
}

submit_charter() {
  CHARTER="$(awk '/^ISSUE-WATCH CHARTER/{found=1} found' triage-prompt.md)"
  JSON_CHARTER="$(python3 - "$CHARTER" <<'PY'
import json, sys
print(json.dumps(sys.argv[1]))
PY
)"
  curl -s --noproxy '*' "http://127.0.0.1:7460/rpc" \
    -H 'Content-Type: application/json' \
    -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"submit\",\"params\":{\"session\":\"$ROUTER_SESSION\",\"content\":$JSON_CHARTER,\"idempotency_key\":\"issue-watch:charter\"}}"
  echo
}

count_events() {
  sqlite3 .state/kian.db "select count(*) from events where session='$ROUTER_SESSION'"
}

echo "== demo-restart: fresh boot =="
boot
echo "-- submitting the charter (idempotent)"
submit_charter

if [[ "${CREATE_ISSUE:-0}" == "1" ]]; then
  echo "-- creating a throwaway issue on flyer103/kian"
  ISSUE_URL="$(gh issue create -R flyer103/kian \
    -t "demo-restart throwaway: watch must survive kill -9" \
    -b "Created by examples/issue-watch/demo-restart.sh to exercise the restart demo. Close after the run.")"
  echo "   $ISSUE_URL"
fi

echo "-- waiting for the first tick (+${INTERVAL_S}s) and the poll worker to run"
sleep $(( INTERVAL_S + 20 ))

BEFORE="$(count_events)"
echo "-- Router stream length before the kill: $BEFORE events"

echo "-- kill -9 the server (no signal handling, no flush: a true crash)"
kill -9 "$SERVE_PID"
sleep 1
if kill -0 "$SERVE_PID" 2>/dev/null; then
  echo "demo-restart.sh: kill -9 failed?" >&2; exit 1
fi
echo "-- rebooting on the same event store"
boot
echo "-- resubmitting the charter (must dedupe: already-committed row)"
submit_charter

echo "-- waiting for the next tick (+${INTERVAL_S}s) post-restart"
sleep $(( INTERVAL_S + 20 ))

AFTER="$(count_events)"
echo "-- Router stream length after the restart: $AFTER events"
if [[ "$AFTER" -gt "$BEFORE" ]]; then
  echo "-- SAME session resumed: seq kept growing ($BEFORE -> $AFTER), no reset"
else
  echo "-- WARNING: stream did not grow ($BEFORE -> $AFTER); inspect .state/serve.log"
fi

if [[ "${CREATE_ISSUE:-0}" == "1" ]]; then
  echo "-- closing the throwaway issue"
  gh issue close "$ISSUE_URL" -R flyer103/kian -c "closed by demo-restart.sh" >/dev/null
fi

echo "== demo-restart: audit proof =="
"$KIAN_BIN" tickets --db .state/kian.db || true
"$KIAN_BIN" session inspect "$ROUTER_SESSION" --db .state/kian.db || true
echo "-- recent Router events (kind + seq):"
sqlite3 .state/kian.db \
  "select seq || '  ' || kind from events where session='$ROUTER_SESSION' order by seq desc limit 12" | tail -r
echo "-- tick inputs consumed (input rows are per session; tick dedupe shows as one row per tick):"
sqlite3 .state/kian.db \
  "select id, consumed_by_seq is not null from inputs where session='$ROUTER_SESSION' order by admitted_at desc limit 8"

echo "-- stopping the demo server"
kill "$SERVE_PID" 2>/dev/null || true
wait "$SERVE_PID" 2>/dev/null || true
echo "demo-restart: done. Full audit: $KIAN_BIN session replay $ROUTER_SESSION --db .state/kian.db"
