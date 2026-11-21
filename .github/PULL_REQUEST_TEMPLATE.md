## What this changes

<!-- One paragraph. Name the owning doc if the change has semantics: SPEC for scope, DESIGN for the
     blueprint, EVENT-MODEL for event bytes, API-CONTRACT for wire shapes. -->

## Why

<!-- The problem, not the patch. Link the issue or ADR if one exists. -->

## Contract impact

- [ ] No event / wire / config / prompt-bytes change
- [ ] Event kinds or payloads (EVENT-MODEL.md updated first)
- [ ] Wire shapes (API-CONTRACT.md updated first)
- [ ] Config keys or CLI surface
- [ ] Prompt or tool-schema bytes (cache stability — call it out explicitly)

## Verification

<!-- State what you actually ran, and at which level. "Unit tests pass" is not
     "the e2e passes"; say which suites ran and which env-gated ones did not. -->

- [ ] `cargo test --workspace`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test -p kian-cli --test docs_map` (docs or link changes)
- [ ] Env-gated suite run locally: <!-- name it, or say "skipped, needs KIAN_*" -->

## Honesty check

- [ ] Nothing is mapped onto `completed` that was aborted, crashed or empty
- [ ] No prior context is rewritten, reordered or reformatted
- [ ] New state is projected from the event stream, not stored beside it
- [ ] Any new refusal or degraded path names itself in an error the operator can act on
