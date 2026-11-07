#!/usr/bin/env bash
# demo-rearm.sh — the acceptance run (examples/issue-watch).
#
# Two acts, both against the real DeepSeek endpoint, real gh (dry-run),
# and the real sqlite event store:
#
#   ACT A — forced soft failure. A fault-injecting proxy (flaky-proxy.py)
#   kills the SSE stream of the first post-bootstrap router judge turn
#   (HTTP 200, then the stream dies). The Router records
#   turn/end{outcome:"soft_failure"} and ends session/ended{failed} —
#   honestly. We kill -9 the server mid-watch and reboot on the same
#   store: the NEXT timer tick re-opens the SAME pinned session (a
#   failed end projects idle) and judging resumes there. No successor.
#
#   ACT B — the real terminal death. We boot on a copy of the live
#   run's event store (.state/kian.db), whose pinned router session
#   ...c060 ended session/ended{completed} at seq 211 (the lifetime
#   iteration budget exhausted: iteration 32 hit the cap after tick 15)
#   — the exact frozen state exists for. On boot the re-arm
#   walk arms the deterministic successor uuid5(c060,
#   "kian:resident:re-arm"); the successor's first input is the
#   [re-arm] cause prompt; ticks 1..15 dedupe against the frozen
#   stream's committed rows and tick 16 is judged on the successor.
#   No operator action.
#
# Options (env): DRY_RUN (default 1), KEEP_STATE=1 keep.state-rearm-*.
#
# Run from anywhere; artifacts land in.state-rearm-{a,b}/.

set -euo pipefail
cd "$(dirname "$0")"

export NO_PROXY="127.0.0.1,localhost"
export no_proxy="127.0.0.1,localhost"
export DRY_RUN="${DRY_RUN:-1}"

KIAN_BIN="${KIAN_BIN:-../../target/debug/kian}"
[[ -x "$KIAN_BIN" ]] || { echo "build first: cargo build --workspace" >&2; exit 1; }
[[ -f ~/.hermes/.env ]] || { echo "~/.hermes/.env missing" >&2; exit 1; }
set -a; source ~/.hermes/.env; set +a
[[ -n "${DEEPSEEK_API_KEY:-}" ]] || { echo "DEEPSEEK_API_KEY absent" >&2; exit 1; }

PINNED_A="00000000-0000-4000-8000-00000000c062"
PINNED_B="$(sed -n 's/^ *session_id: *"\(.*\)"/\1/p' config.yaml | head -1)"
SUCCESSOR_B="$(UUID="$PINNED_B" python3 -c 'import os,uuid;print(uuid.uuid5(uuid.UUID(os.environ["UUID"]),"kian:resident:re-arm"))')"
INTERVAL_MS=5000
INTERVAL_S=5

cleanup() {
  [[ -n "${SERVE_PID:-}" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null || true
}
trap cleanup EXIT

SQL() { sqlite3 "$1" "$2"; }

mk_config() { # mk_config <out> <sqlite> <pinned> <base_url> <interval_ms>
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import re, sys
out, sqlite, pinned, base, interval = sys.argv[1:6]
cfg = open("config.yaml").read()
cfg = cfg.replace('"./.state/kian.db"', f'"{sqlite}"')
cfg = re.sub(r'session_id: "[^"]+"', f'session_id: "{pinned}"', cfg)
cfg = re.sub(r'interval_ms: \d+', f'interval_ms: {interval}', cfg)
cfg = re.sub(r'base_url: "[^"]+"', f'base_url: "{base}"', cfg)
# The demo's residents must outlive the stock lifetime budget (32) —
# that budget is exactly what froze the router (§) and it
# would end the re-armed successor mid-demo too.
cfg = re.sub(r'max_iterations: \d+', 'max_iterations: 500', cfg)
open(out, "w").write(cfg)
PY
}

boot() { # boot <config> <state-dir>
  : >> "$2/serve.log"
  RUST_LOG=info "$KIAN_BIN" serve --config "$1" >> "$2/serve.log" 2>&1 &
  SERVE_PID=$!
  echo "$SERVE_PID" > "$2/serve.pid"
  for _ in $(seq 1 150); do
    curl -s -o /dev/null --noproxy '*' "http://127.0.0.1:7460/metrics" && return 0
    kill -0 "$SERVE_PID" 2>/dev/null || { tail -20 "$2/serve.log" >&2; exit 1; }
    sleep 0.2
  done
  echo "serve did not become ready" >&2; exit 1
}

stop_serve() {
  [[ -n "${SERVE_PID:-}" ]] && kill "$SERVE_PID" 2>/dev/null || true
  [[ -n "${SERVE_PID:-}" ]] && wait "$SERVE_PID" 2>/dev/null || true
  SERVE_PID=""
}

submit_charter() { # submit_charter <session>
  CHARTER="$(awk '/^ISSUE-WATCH CHARTER/{found=1} found' triage-prompt.md)"
  python3 - "$CHARTER" "$1" <<'PY' | curl -s --noproxy '*' "http://127.0.0.1:7460/rpc" \
      -H 'Content-Type: application/json' -d @- >/dev/null
import json, sys
print(json.dumps({"jsonrpc":"2.0","id":1,"method":"submit","params":{
  "session":sys.argv[2],"content":sys.argv[1],"idempotency_key":"issue-watch:charter"}}))
PY
}

wait_sql() { # wait_sql <db> <query> <desc> [timeout_s]
  local db="$1" q="$2" desc="$3" tmo="${4:-120}" out=""
  for _ in $(seq 1 $((tmo * 2))); do
    out="$(SQL "$db" "$q")"
    [[ -n "$out" ]] && { echo "$out"; return 0; }
    sleep 0.5
  done
  echo "TIMEOUT waiting for: $desc" >&2; return 1
}

echo "==================================================================="
echo "ACT A — forced soft failure, kill -9, reboot: SAME session resumes"
echo "==================================================================="

rm -rf .state-rearm-a
mkdir -p .state-rearm-a/tools
mk_config config-rearm-a.yaml "./.state-rearm-a/kian.db" "$PINNED_A" "http://127.0.0.1:7461" "$INTERVAL_MS"

echo "-- starting the fault-injecting proxy (kills router judge #2's stream)"
PORT=7461 KILL_ON=2 python3 flaky-proxy.py > .state-rearm-a/proxy.log 2>&1 &
PROXY_PID=$!
sleep 0.5

echo "-- boot #1 (through the proxy) and charter"
boot config-rearm-a.yaml .state-rearm-a
submit_charter "$PINNED_A"

DB_A=.state-rearm-a/kian.db
echo "-- waiting for the honest soft failure (session/ended{failed} on $PINNED_A)"
FAILED_SEQ="$(wait_sql "$DB_A" \
  "select seq from events where session='$PINNED_A' and kind='session/ended' and envelope like '%\"failed\"%' limit 1" \
  "session/ended{failed}")"
echo "   session/ended{failed} at seq $FAILED_SEQ"
SOFT="$(SQL "$DB_A" "select count(*) from events where session='$PINNED_A' and kind='turn/end' and envelope like '%soft_failure%'")"
echo "   turn/end{soft_failure} events: $SOFT"

echo "-- kill -9 the server mid-watch (true crash, no flush)"
kill -9 "$SERVE_PID"; wait "$SERVE_PID" 2>/dev/null || true; SERVE_PID=""
kill "$PROXY_PID" 2>/dev/null || true

echo "-- reboot DIRECT against DeepSeek on the same store (no proxy, no failure)"
mk_config config-rearm-a2.yaml "./.state-rearm-a/kian.db" "$PINNED_A" "https://api.deepseek.com" "$INTERVAL_MS"
boot config-rearm-a2.yaml .state-rearm-a
submit_charter "$PINNED_A"   # idempotent: must dedupe

echo "-- waiting for the next tick to be judged on the SAME pinned session"
TURN_AFTER="$(wait_sql "$DB_A" \
  "select seq from events where session='$PINNED_A' and kind='turn/start' and seq > $FAILED_SEQ limit 1" \
  "a judge turn after the failed end" 180)"
echo "   a judge turn ran at seq $TURN_AFTER (> failed end at $FAILED_SEQ) — the SAME session re-opened"
GREEN_A="$(wait_sql "$DB_A" \
  "select seq from events where session='$PINNED_A' and kind='turn/end' and envelope like '%final_answer%' and seq > $FAILED_SEQ limit 1" \
  "a completed (final_answer) judge turn after the failed end" 300)"
echo "   a judge turn COMPLETED (final_answer) at seq $GREEN_A — the watch is judging again, honestly"
SUCC_A="$(UUID="$PINNED_A" python3 -c 'import os,uuid;print(uuid.uuid5(uuid.UUID(os.environ["UUID"]),"kian:resident:re-arm"))')"
SUCC_A_EVENTS="$(SQL "$DB_A" "select count(*) from events where session='$SUCC_A'")"
echo "   successor-of-pinned events: $SUCC_A_EVENTS (expect 0 — a soft failure is not a succession)"
echo "-- proxy log tail (the kill):"
tail -4 .state-rearm-a/proxy.log || true
stop_serve

echo
echo "==================================================================="
echo "ACT B — the real frozen store: successor re-arm without operator action"
echo "==================================================================="

rm -rf .state-rearm-b
mkdir -p .state-rearm-b/tools
if [[ -f .state/kian.db ]]; then
  cp .state/kian.db .state-rearm-b/kian.db
  [[ -f .state/kian.db-wal ]] && cp .state/kian.db-wal .state-rearm-b/kian.db-wal
  [[ -f .state/kian.db-shm ]] && cp .state/kian.db-shm .state-rearm-b/kian.db-shm
else
  echo "frozen store .state/kian.db not found — ACT B needs it" >&2; exit 1
fi
DB_B=.state-rearm-b/kian.db
PINNED_TAIL="$(SQL "$DB_B" "select count(*) from events where session='$PINNED_B'")"
PINNED_END="$(SQL "$DB_B" "select kind from events where session='$PINNED_B' order by seq desc limit 1")"
echo "-- frozen predecessor: $PINNED_B, $PINNED_TAIL events, tail event: $PINNED_END"
echo "-- expected successor: $SUCCESSOR_B"

mk_config config-rearm-b.yaml "./.state-rearm-b/kian.db" "$PINNED_B" "https://api.deepseek.com" "$INTERVAL_MS"
boot config-rearm-b.yaml .state-rearm-b

echo "-- boot log evidence of the re-arm:"
grep -m2 "re-armed onto a successor session" .state-rearm-b/serve.log || echo "   (log line missing!)"

PROMPT_SEQ="$(wait_sql "$DB_B" \
  "select seq from events where session='$SUCCESSOR_B' and kind='input/admitted' and envelope like '%[re-arm]%' limit 1" \
  "the [re-arm] cause prompt on the successor" 60)"
echo "   [re-arm] prompt admitted at seq $PROMPT_SEQ"

echo "-- waiting for tick 16 (ticks 1..15 dedupe against the frozen stream) to be judged on the successor"
TICK16="$(wait_sql "$DB_B" \
  "select seq from events where session='$SUCCESSOR_B' and kind='input/admitted' and envelope like '%tick-16%' limit 1" \
  "tick-16 admitted on the successor" 240)"
echo "   tick-16 admitted at seq $TICK16"
TURN16="$(wait_sql "$DB_B" \
  "select seq from events where session='$SUCCESSOR_B' and kind='turn/start' and seq > $PROMPT_SEQ limit 1" \
  "a successor judge turn after the re-arm prompt" 240)"
echo "   successor judge turn ran at seq $TURN16 (after the re-arm prompt at $PROMPT_SEQ)"
PINNED_AFTER="$(SQL "$DB_B" "select count(*) from events where session='$PINNED_B'")"
echo "   frozen predecessor untouched: $PINNED_AFTER events (was $PINNED_TAIL)"
GREEN_B="$(SQL "$DB_B" "select count(*) from events where session='$SUCCESSOR_B' and kind='turn/end' and envelope like '%final_answer%'")"
echo "   completed (final_answer) judge turns on the successor: $GREEN_B"
SUCC_TAIL="$(SQL "$DB_B" "select kind from events where session='$SUCCESSOR_B' order by seq desc limit 1")"
echo "   successor tail event: $SUCC_TAIL (still running if not session/ended)"

echo "-- successor stream head (the observable cause):"
SQL "$DB_B" "select seq || '  ' || kind from events where session='$SUCCESSOR_B' order by seq limit 8"
echo "-- first input content (truncated):"
SQL "$DB_B" "select substr(envelope, instr(envelope,'\"content\":\"'), 220) from events where session='$SUCCESSOR_B' and kind='input/admitted' order by seq limit 1"

stop_serve
echo
echo "demo-rearm: done. Audit: $KIAN_BIN session inspect $SUCCESSOR_B --db .state-rearm-b/kian.db"
