# issue-watch — an agent that triages your repository's issues

A resident Router watches one GitHub repository and turns each new issue into a triage draft. It is
the smallest shape that exercises the bundle end to end: poll intake, two-level admission, Thread
delegation, a Mandate-bounded worker, a speak-rule, and the outbox.

```
GitHub poll ──► admission classes ──► Router (resident session)
                                        │  spawn_worker (Mandate)
                                        ▼
                                     triager worker ──► draft
                                        │
                                   speak-rule (result-ready) ──► outbox ──► comment
```

## Run it

```bash
export DEEPSEEK_API_KEY=...                       # the model slot
export KIAN_GITHUB_TOKEN="$(gh auth token)"       # poll + send

kian serve --config kian.yaml                     # terminal 1
./run.sh                                          # terminal 2: submit the charter, follow the stream
```

Open an issue on `flyer103/kian-runtime` (or change `github.poll.repo` in `kian.yaml` to your own repo).
Within one poll interval the Router judges it and a worker drafts a comment.

`DRY_RUN` is fail-closed: the worker only posts when `DRY_RUN=0` is exported. Everything else is a
draft you can read with `kian tickets` and `kian session inspect`.

## What to look at

| Surface | What it shows |
|---|---|
| `kian tickets` | the stage board: which worker is waiting for what |
| `kian session inspect <id>` | one session's events, turns, requests |
| `kian session replay <id>` | the derived context is byte-identical to what was sent |
| `kian cache-stats --json` | prefix reuse across turns; `unexpected_prefix_change` must stay zero |
| `kian workers` | the endpoint catalog (empty here — this example runs everything locally) |

## Boundaries

- One repository, one Router, one worker template. It is a demo of the shape, not a triage policy:
  edit `charter.md` and `mandate.scopes.triager` for your own rules.
- `DRY_RUN=0` posts a real comment as the token's user. Read the mandate once before you do that.
- The Router is a single resident session by design; throughput on this shape is one judgement per
  turn. That is enough at team scale and it is the trade-off the bundle documents.
