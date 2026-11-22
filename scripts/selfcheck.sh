#!/usr/bin/env bash
# The four gates (AGENTS.md Commands), in the shape CI runs them.
#
# This is the pre-push body: run it before a push so a red GitHub run is
# the exception, not the first evidence. `KIAN_SELFCHECK=quick` runs only
# fmt + clippy; `KIAN_SELFCHECK=off` skips everything (a one-off escape,
# never a silent default change: CI still runs the full gates).
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

mode="${KIAN_SELFCHECK:-full}"
case "$mode" in
  full | quick | off) ;;
  *)
    echo "selfcheck: unknown KIAN_SELFCHECK='$mode' (use full | quick | off)" >&2
    exit 2
    ;;
esac

if [ "$mode" = "off" ]; then
  echo "selfcheck: skipped (KIAN_SELFCHECK=off)"
  exit 0
fi

echo "== cargo fmt --all -- --check =="
cargo fmt --all -- --check

echo "== cargo clippy --workspace --all-targets -- -D warnings =="
cargo clippy --workspace --all-targets -- -D warnings

if [ "$mode" = "quick" ]; then
  echo "selfcheck: fmt + clippy green (quick mode; tests skipped)"
  exit 0
fi

echo "== cargo test --workspace -- --test-threads=1 =="
# Serialized like CI: two concurrent e2e suites on a small runner starve
# their short waits and fail on timing, not on semantics.
NO_PROXY=127.0.0.1,localhost cargo test --workspace -- --test-threads=1

echo "selfcheck: fmt + clippy + tests green"
