# ADR-0006: Durable channels and state backend

- Status: `planning`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, storage maintainers
- Depends on: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md)
- Leads to: [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0004](ADR-0004-multi-sink-commit.md), [ADR-0007](ADR-0007-backpressure-and-high-load.md)
- Refined by: [ADR-0010](ADR-0010-platform-durable-yield-channel.md)

## Problem

Selective recovery requires replayable channel data and versioned state, but
persisting every item synchronously can make throughput unacceptable. Retention,
compaction, partition ownership, and state migration are unresolved.

## Theory

Append-only segmented channels with batched durability, stable logical IDs, and
checkpoint-based retention can support replay without an external broker.
Incremental state snapshots can reduce checkpoint cost.

## Agreement for protocol epoch 2

ADR-0010 selects the platform-owned append-only partition log, immutable
ObjectRef envelopes, per-consumer progress, and fenced delivery ownership. The
following physical backend choices still require benchmark and failure evidence:

- local write-ahead segments replicated to durable object storage;
- database-backed metadata plus object-backed channel segments;
- full versus incremental state snapshots;
- platform-owned data-plane tables versus segmented files for hot records;
- retention watermark derived from the oldest restorable checkpoint and every
  active consumer cursor.

The SQLite stores used by `progress` ADR-0002, ADR-0003, and ADR-0010 are
provisional executable models, not a selection of the production backend. Those
ADRs may accumulate protocol evidence while this ADR remains `planning`; none can
become `accepted` until this backend decision is resolved and its production
criteria pass.

Sprint 2 adds a provisional partitioned SQLite channel in
[barrier_protocol.py](../prototypes/barrier_protocol.py). It persists data and
barrier records in one sequence, rejects gaps and conflicting stable-record
claims, enforces increasing checkpoint barriers, and fences an obsolete writer
after lease expiry and generation takeover. Lease expiry uses a durable monotonic
store clock read inside the guarded write statement; renewal cannot shorten the
existing deadline or revive an expired generation. Clock advancement belongs to
the trusted control plane. Raw deletion is disabled until the reference-safe
retention protocol in ST-042 exists. The subprocess matrix abruptly exits at
three named offsets for data append, barrier append, takeover,
alignment/snapshot, buffered data, restore, and combined decision, then reopens
the durable component. This is protocol evidence only; it does not select SQLite
or any other production backend, so this ADR remains `planning`.

## Stories

- As a developer, I want one channel API for memory, local test, and production
  durable backends.
- As a user, I want retention cost and recovery point objective to be explicit.
- As a system, I want to retain every byte needed by the oldest selectable
  checkpoint.

## Acceptance criteria

- As a system, I can recover after process and host loss without missing an
  acknowledged record.
- As a system, I got an identical retry for the same immutable envelope and a
  conflict for changed payload or lineage; client-computed output equivalence is
  outside this storage guarantee.
- As a user, I can configure durability, retention, and recovery objectives.
- As a developer, I can migrate compatible state schemas and receive a clear
  rejection for incompatible ones.

## Required experiments

- Crash during segment append, flush, upload, metadata publication, and deletion.
- Sustained throughput, tail latency, checkpoint time, and recovery time at
  increasing state and channel sizes.
- Retention under a stalled branch and a failed consumer.

