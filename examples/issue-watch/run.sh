#!/usr/bin/env bash
# run.sh — boot the issue-watch agent (kian examples/issue-watch, card C60).
#
# What it does:
#   1. exports NO_PROXY (localhost e2e gotcha) and DRY_RUN (default 1:
#      drafts only; DRY_RUN=0 is the only way a real comment is posted)
#   2. sources ~/.hermes/.env for DEEPSEEK_API_KEY (presence-checked,
#      never printed)
#   3. boots `kian serve` on 127.0.0.1:7460 with ./config.yaml
#   4. submits the charter (triage-prompt.md) to the fixed Router session,
#      idempotently — safe to re-run after any restart
#   5. prints how to inspect the durable audit streams
#
# Stop it with Ctrl-C (SIGINT) or kill the printed pid.

set -euo pipefail
cd "$(dirname "$0")"

export NO_PROXY="127.0.0.1,localhost"
export no_proxy="127.0.0.1,localhost"
export DRY_RUN="${DRY_RUN:-1}"

KIAN_BIN="${KIAN_BIN:-../../target/debug/kian}"
if [[ ! -x "$KIAN_BIN" ]]; then
  echo "run.sh: kian binary not found at $KIAN_BIN" >&2
  echo "run.sh: build it first: cargo build --workspace" >&2
  exit 1
fi

if [[ ! -f ~/.hermes/.env ]]; then
  echo "run.sh: ~/.hermes/.env not found (DEEPSEEK_API_KEY source)" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source ~/.hermes/.env
set +a
if [[ -z "${DEEPSEEK_API_KEY:-}" ]]; then
  echo "run.sh: DEEPSEEK_API_KEY absent after sourcing ~/.hermes/.env" >&2
  exit 1
fi

mkdir -p .state/tools
: > .state/serve.log

# The Router session id (kept in one place: config.yaml).
ROUTER_SESSION="$(sed -n 's/^ *session_id: *"\(.*\)"/\1/p' config.yaml | head -1)"
if [[ -z "$ROUTER_SESSION" ]]; then
  echo "run.sh: could not read superagent.router.session_id from config.yaml" >&2
  exit 1
fi

echo "run.sh: booting kian serve (dry_run=$DRY_RUN, log: .state/serve.log)"
"$KIAN_BIN" serve --config config.yaml >> .state/serve.log 2>&1 &
SERVE_PID=$!
echo "$SERVE_PID" > .state/serve.pid

cleanup() {
  if kill -0 "$SERVE_PID" 2>/dev/null; then
    kill "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

for _ in $(seq 1 100); do
  if curl -s -o /dev/null --noproxy '*' "http://127.0.0.1:7460/metrics"; then
    break
  fi
  if ! kill -0 "$SERVE_PID" 2>/dev/null; then
    echo "run.sh: serve died during startup:" >&2
    tail -20 .state/serve.log >&2
    exit 1
  fi
  sleep 0.2
done

# The charter: everything from the ISSUE-WATCH CHARTER marker to EOF of
# triage-prompt.md, submitted once per session (idempotency key makes
# re-submission after a restart a dedupe no-op).
CHARTER="$(awk '/^ISSUE-WATCH CHARTER/{found=1} found' triage-prompt.md)"
JSON_CHARTER="$(python3 - "$CHARTER" <<'PY'
import json, sys
print(json.dumps(sys.argv[1]))
PY
)"

echo "run.sh: submitting the charter to Router session $ROUTER_SESSION"
curl -s --noproxy '*' "http://127.0.0.1:7460/rpc" \
  -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"submit\",\"params\":{\"session\":\"$ROUTER_SESSION\",\"content\":$JSON_CHARTER,\"idempotency_key\":\"issue-watch:charter\"}}"
echo

cat <<EOF

run.sh: watch is live.
  - timer: one tick every 30s (topic "issues")
  - flow:  tick -> Router -> poll worker (gh issue list) -> new issues
           -> draft worker (severity + decision + reply draft)
  - dry-run: DRY_RUN=$DRY_RUN (drafts land in the audit stream only)

Inspect the durable audit (from this directory):
  $KIAN_BIN session inspect $ROUTER_SESSION --db .state/kian.db
  $KIAN_BIN session replay   $ROUTER_SESSION --db .state/kian.db
  $KIAN_BIN session export   $ROUTER_SESSION --db .state/kian.db --out audit.json
Worker sessions (fresh uuids per thread) are listed by:
  sqlite3 .state/kian.db 'select distinct session from events order by 1'
Live server log: tail -f .state/serve.log
Stop: kill \$(cat .state/serve.pid)

run.sh: following the serve log (Ctrl-C stops the watch)...
EOF

wait "$SERVE_PID"
