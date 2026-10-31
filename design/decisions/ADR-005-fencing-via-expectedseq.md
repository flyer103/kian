# ADR-005: Safety = fencing token (epoch + expectedSeq CAS); lease is only liveness

- **Status**: proposed
- **Date**: 2026-10-31
- **Context**: Under managed multi-replica deployment, a drain executor may partition or hang
  rather than crash cleanly. A scheme relying solely on lease-TTL detection will double-write
  (split-brain) during the window where "the old holder is still writing". Feishu design doc,
  formal conclusion #2.
- **Decision**: A claim issues a monotonically increasing epoch; all of a drain's EventStore
  writes carry it; the store validates the epoch via `expectedSeq` CAS (stale-epoch writes are
  always rejected) → NoSplitBrainWrite. The lease (claim TTL + heartbeat) serves only as the
  liveness mechanism triggering recoverable detection — it carries no safety responsibility. The
  session end write + state transition = one atomic append (NoHalfTerminated).
- **Alternatives considered**:
  1. Lease TTL + optimistic-lock version only — safety depends on detection timeliness; within
     the partition window, the old holder's writes can be accepted.
  2. A distributed lock service (etcd/zk) — introduces a new infrastructure dependency, and
     fencing still needs tokens (Martin Kleppmann's classic conclusion: locks don't solve
     fencing; tokens do).
- **Consequences**: The store write path gains one CAS validation (a single statement;
  performance is negligible); correctness is decoupled from deployment form (single-machine /
  multi-replica).
- **Rollback Trigger**: None — rolling back means abandoning NoSplitBrainWrite.
- **Cost Impact**: Zero (a sqlite transaction / a single postgres statement suffices).
