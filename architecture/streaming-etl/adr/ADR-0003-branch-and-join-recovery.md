# ADR-0003: Selective recovery through branches and joins

- Status: `progress`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0006](ADR-0006-durable-channels-and-state.md)
- Affects: [ADR-0007](ADR-0007-backpressure-and-high-load.md)

## Problem

When branch `B` fails, restarting the entire graph wastes work. Restarting only
`B` is also incorrect if a downstream join already consumed records from both
`B` and a healthy branch `C`.

## Theory

Restore `B` and its dependent tail while leaving `C` live, provided every input
to the restored region can replay data from the same checkpoint. At a join, this
means replaying retained output from healthy branches as well as recomputed
output from failed branches.

## Proposed agreement

```text
             ┌─ B ─┐
source ─ A ──┤     ├─ Join ─ sink
             └─ C ─┘
```

If `B` fails, the runtime restores `B`, `Join`, and the dependent tail. `A` and
`C` remain active. `B` replays retained `A → B` input; `Join` restores its state
and rereads post-checkpoint output from both `B` and `C`. New records may buffer
until the restored region catches up; [ADR-0007](ADR-0007-backpressure-and-high-load.md)
sets capacity limits.

Join operators forward checkpoint `N` only after receiving barrier `N` on every
active input. An aligned implementation pauses inputs that arrive early. A later
ADR may introduce unaligned checkpoints after channel-state snapshotting is
proven.

Stable logical record IDs make replayed channel appends idempotent. Retention
cannot delete channel segments needed by the oldest recoverable checkpoint.

## Stories

- As a developer, I want a failed branch to recover without restarting healthy
  upstream work.
- As a user, I want joins to preserve correct state after any single operator
  failure.
- As a system, I want to expand recovery upstream when a required replay boundary
  is unavailable.

## Acceptance criteria

- As a system, I can restore a failed branch and all dependent operators while an
  independent healthy branch remains live.
- As a system, I can replay every join input from the same completed checkpoint.
- As a system, I got the same platform-managed channel records and managed state
  as a failure-free execution when the client determinism assumption holds.
- As a user, I can see the selected recovery region and why it expanded.

## Prototype evidence

The production-backend implementation cycle is tracked in
[ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md). It adds
routed intermediate barriers, durable join mailboxes, and a PostgreSQL recovery
region planner. The earlier prototype evidence below remains historical;
The complete DS-031a…d gate subsequently passed 93/93 frozen-source tests and
independent Principal review on 2026-10-25, earning 75%. Policy-driven expansion
requires verified retained bytes; unsupported reconstruction is recorded in
[ADR-0093](ADR-0093-implicit-regeneration-of-missing-durable-output.md).

`test_failed_branch_and_tail_restore_from_completed_checkpoint` in
[test_protocols.py](../prototypes/test_protocols.py) restores `B` and a join while
`C` stays at its newer cursor. Durable, idempotent edge messages let the join
replay both post-checkpoint inputs and produce the same total as the failure-free
execution.

`test_replay_identity_conflict_is_rejected` proves that reusing a logical record
ID with a different payload is a protocol error rather than silent deduplication.
`test_non_contiguous_edge_sequence_is_rejected` protects the prototype's current
single-partition contiguous-cursor assumption. Production partition cursors and
lineage remain unimplemented in that first model.

Sprint 2 adds [barrier_protocol.py](../prototypes/barrier_protocol.py). `DATA` and
`BARRIER` share the same ordered partition log. The aligned-join test delivers an
early barrier and a post-barrier record on one input, continues pre-barrier data
on the other input, and verifies that the snapshot excludes the buffered record.
After the second barrier, the buffer is applied; reopen, generation takeover, and
restore return to the barrier snapshot.

Join admission binds each logical input to one immutable edge and partition and
stores the digest of every consumed sequence. Restore rolls back live state and
cursors while preserving immutable historical consumed digests and snapshots.
An exact historical suffix can therefore be reapplied, while a different record
at the same input sequence is rejected even if it produces the same aggregate
sum. Active and completed checkpoint numbers increase monotonically. This
particular operator sums integers, so cross-input buffer drain order is
deliberately commutative; non-commutative operators require a persisted
deterministic mailbox order.

The recovery planner now returns the failed branch and dependent tail, replay
cuts for every incoming boundary, and reasons for each node. Every cut carries
the selected checkpoint and per-partition positions; planning rejects a
checkpoint absent from the completed-checkpoint manifest. A boundary that cannot
serve that checkpoint expands the region upstream and produces a new replay cut.

The [subprocess recovery matrix](../prototypes/test_subprocess_recovery.py)
terminates independent processes at three named offsets around channel append,
barrier append, alignment/snapshot, buffered post-barrier data, lease takeover,
restore, and the combined checkpoint decision. It reopens each store, completes
a partial alignment, and verifies that a restored snapshot excludes buffered
post-barrier data while live state includes it after alignment. The prototype
does not yet prove concurrent joins, repartitioning, watermarks, cycles, host
loss, or retention under load. The ADR remains `progress`.

