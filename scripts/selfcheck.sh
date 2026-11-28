#!/usr/bin/env bash
# The gates CI runs, before the push instead of after.
#
# Default (scoped): run only what the outgoing commits can break.
#   - docs-only diff            -> docs_map
#   - crate change              -> workspace build (dependents compile)
#                                  + that crate's clippy and serialized tests
#   - workspace-level change    -> everything (any Cargo.toml/Cargo.lock,
#                                  kian-core, kian-store, kian-orchestrator)
# KIAN_SELFCHECK=full forces everything, quick runs fmt + clippy only, off
# skips the hook. CI still runs the full gates for every code change either way.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

mode="${KIAN_SELFCHECK:-scoped}"
case "$mode" in
  scoped | full | quick | off) ;;
  *)
    echo "selfcheck: unknown KIAN_SELFCHECK='$mode' (use scoped | full | quick | off)" >&2
    exit 2
    ;;
esac

if [ "$mode" = "off" ]; then
  echo "selfcheck: skipped (KIAN_SELFCHECK=off)"
  exit 0
fi

run_fmt() {
  echo "== cargo fmt --all -- --check =="
  cargo fmt --all -- --check
}

run_full() {
  run_fmt
  echo "== cargo build --workspace --locked =="
  cargo build --workspace --locked
  echo "== cargo clippy --workspace --all-targets --locked -- -D warnings =="
  cargo clippy --workspace --all-targets --locked -- -D warnings
  echo "== cargo test --workspace --locked -- --test-threads=1 =="
  # Serialized like CI: two concurrent e2e suites starve their short waits and
  # fail on timing, not on semantics.
  NO_PROXY=127.0.0.1,localhost cargo test --workspace --locked -- --test-threads=1
}

if [ "$mode" = "full" ] || [ -t 0 ]; then
  if [ "$mode" = "scoped" ]; then
    echo "selfcheck: no push refs on stdin (manual run); running the full gates"
  fi
  run_full
  echo "selfcheck: fmt + build + clippy + tests green"
  exit 0
fi

if [ "$mode" = "quick" ]; then
  run_fmt
  echo "== cargo clippy --workspace --all-targets --locked -- -D warnings =="
  cargo clippy --workspace --all-targets --locked -- -D warnings
  echo "selfcheck: fmt + clippy green (quick mode; tests skipped)"
  exit 0
fi

work=$(mktemp -d "${TMPDIR:-/tmp}/kian-selfcheck.XXXXXX")
trap 'rm -rf "$work"' EXIT
changed="$work/changed"
: >"$changed"

empty_tree=$(git hash-object -t tree /dev/null)
refs=0
while read -r local_ref local_sha remote_ref remote_sha; do
  [ -n "${local_ref:-}" ] || continue
  refs=$((refs + 1))
  case "$local_ref" in
    refs/tags/*)
      echo "selfcheck: tag $local_ref skipped (tag pushes are not gated by CI either)"
      continue
      ;;
  esac
  case "$local_sha" in
    0000000000000000000000000000000000000000)
      echo "selfcheck: deletion of $remote_ref skipped"
      continue
      ;;
  esac
  base="$remote_sha"
  case "$base" in
    0000000000000000000000000000000000000000)
      base=$(git merge-base "$local_sha" origin/main 2>/dev/null || true)
      [ -n "$base" ] || base="$empty_tree"
      ;;
  esac
  if [ "$base" != "$empty_tree" ] && ! git rev-parse --verify --quiet "$base^{commit}" >/dev/null; then
    base="$empty_tree"
  fi
  git diff --name-only "$base" "$local_sha" >>"$changed"
done

if [ "$refs" -eq 0 ]; then
  echo "selfcheck: nothing to push"
  exit 0
fi

sort -u "$changed" -o "$changed"
if [ ! -s "$changed" ]; then
  echo "selfcheck: no files differ from the remote"
  exit 0
fi

workspace=0
docs=0
pkgs=""
while IFS= read -r path; do
  case "$path" in
    *.md | docs/* | design/* | site/*)
      docs=1
      ;;
    Cargo.toml | Cargo.lock | rust-toolchain* | .cargo/*)
      workspace=1
      ;;
    crates/kian-core/* | crates/kian-store/* | crates/kian-orchestrator/*)
      workspace=1
      ;;
    crates/*/*)
      crate="${path#crates/}"
      pkgs="$pkgs ${crate%%/*}"
      ;;
    *)
      : # not compiled: docker/, contrib/, .github/, ...
      ;;
  esac
done <"$changed"

if [ "$workspace" -eq 1 ]; then
  echo "selfcheck: workspace-level change; running the full gates"
  run_full
  echo "selfcheck: fmt + build + clippy + tests green"
  exit 0
fi

# shellcheck disable=SC2086 # $pkgs is a space-separated list by design
pkgs=$(printf '%s\n' $pkgs | sed '/^$/d' | sort -u | tr '\n' ' ')
pkg_args=""
for p in $pkgs; do
  pkg_args="$pkg_args -p $p"
done

run_fmt

if [ "$docs" -eq 1 ]; then
  echo "== cargo test --locked -p kian-cli --test docs_map =="
  cargo test --locked -p kian-cli --test docs_map
fi

if [ -n "$pkg_args" ]; then
  echo "== cargo build --workspace --locked (dependents still compile) =="
  cargo build --workspace --locked
  echo "== cargo clippy --locked$pkg_args --all-targets -- -D warnings =="
  # shellcheck disable=SC2086 # $pkg_args is a -p list by design
  cargo clippy --locked $pkg_args --all-targets -- -D warnings
  echo "== cargo test --locked$pkg_args -- --test-threads=1 =="
  # shellcheck disable=SC2086 # $pkg_args is a -p list by design
  NO_PROXY=127.0.0.1,localhost cargo test --locked $pkg_args -- --test-threads=1
fi

if [ -z "$pkg_args" ] && [ "$docs" -eq 0 ]; then
  echo "selfcheck: no compiled file changed; fmt only"
fi
echo "selfcheck: scoped gates green"
