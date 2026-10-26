# ADR-0002: Periodic barrier checkpoints and recovery domains

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0001](ADR-0001-runtime-boundary.md), [ADR-0010](ADR-0010-platform-durable-yield-channel.md)
- Leads to: [ADR-0013](ADR-0013-managed-state-recovery-classes.md), [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0006](ADR-0006-durable-channels-and-state.md), [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Replaces the checkpoint/sink-decision coupling rejected in: [ADR-0004](ADR-0004-multi-sink-commit.md)

## Problem

Per-record consumer progress tells a replacement worker which input to redeliver,
but it does not by itself give a consistent cut of state across a branched graph
or a multi-input join. Independent process snapshots can mix records from
different logical moments.

## Theory

Coordinator-injected ordered barriers create periodic consistent graph cuts.
They complement per-record `in_flight/completed_through` progress and do not
control arbitrary external effects.

## Agreement

The coordinator creates an immutable checkpoint intent and injects one
monotonically increasing `BARRIER(checkpoint_id, cut)` into every source
partition. DATA and BARRIER records share partition order. A worker never chooses
its own checkpoint ID or cut and cannot insert a barrier after observing a
failure.

Operators finish all pre-barrier input turns, persist their managed snapshot and
consumer positions, then forward the same checkpoint ID. Multi-input operators
align the same barrier on every active input as described by
[ADR-0003](ADR-0003-branch-and-join-recovery.md). Post-barrier records are held in
bounded durable buffers until alignment completes.

A checkpoint becomes `COMPLETED` only after every participant in its immutable
manifest has acknowledged a restorable snapshot and cut. Incomplete checkpoints
are ignored during recovery. The lifecycle is:

```text
STARTED → ALIGNING → ALIGNED → COMPLETED
     └────────────────────────→ ABORTED
```

Checkpoint completion contains no claim about PostgreSQL writes, HTTP requests,
or other client effects. That boundary is defined by
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

Each worker write carries a recovery-generation fence. Coordinator takeover uses
a monotonically increasing owner epoch. Stale owners cannot append DATA/BARRIER,
publish snapshots, or complete consumer turns.

## Relationship to per-record progress

For stateless work, a replacement normally redelivers from
`completed_through + 1` without rolling the whole graph back. For stateful work,
the runtime may start at the same next sequence only when the managed state
needed there was stored with every completed turn. Otherwise it restores an
older completed barrier checkpoint and replays the retained tail.

This distinction prevents the unsafe state:

```text
state restored at 100 + completed_through 200 + start at 201
```

unless state transitions 101 through 200 are also durable.
The executable classification and exact recovery equations are specified in
[ADR-0013](ADR-0013-managed-state-recovery-classes.md).

## Recovery rule

Recover the failed operator and each stateful dependant that cannot continue
from its own durable consumer progress and managed state. Expand upstream only
when a required channel cut is no longer replayable. Healthy producers need not
rerun when their immutable output remains in the channel.

## Stories

- As a developer, I want global checkpoint frequency independent of individual
  `yield` frequency.
- As a user, I want ordinary worker replacement to redeliver the first
  incomplete input without waiting for a new global checkpoint.
- As a system, I want joins to snapshot one consistent checkpoint cut.

## Acceptance criteria

- As a system, I can restore all checkpoint-managed state and input cuts from
  the latest completed barrier.
- As a system, I can prove an incomplete barrier never advances the global
  recovery pointer.
- As a system, I got rejected writes from a fenced obsolete generation.
- As a user, I can distinguish per-record redelivery from graph checkpoint
  restoration in telemetry.

## Prototype evidence

[barrier_protocol.py](../prototypes/barrier_protocol.py) models ordered DATA and
BARRIER records, aligned joins, durable post-barrier buffers, immutable cuts,
leases, fencing, and selective recovery. Its bounded
[TLA+ model](../models/BarrierAlignment.tla) checks same-checkpoint cuts and
fencing, with unsafe variants that produce expected counterexamples.

The former integrated checkpoint/source/sink decision model is retained as
historical evidence under [ADR-0004](ADR-0004-multi-sink-commit.md); its
`COMMIT_DECIDED` lifecycle is not part of this ADR.

## Failure matrix

| Failure point | Required behavior |
| --- | --- |
| Before local snapshot | Ignore the incomplete checkpoint; use per-record progress or the previous completed checkpoint. |
| After snapshot, before participant acknowledgement | The partial checkpoint remains unselectable. |
| After every acknowledgement, before `COMPLETED` | Reopen coordinator state and finish or ignore the same immutable intent. |
| After `COMPLETED` | Restore the exact snapshot and channel cuts. |
| Old worker writes after takeover | Reject by owner epoch. |

## Evidence required for `accepted`

- Production coordinator and snapshot backend.
- Process-loss matrix composed with ADR-0010 consumer progress.
- Stateful recovery proving both per-turn state and older-checkpoint replay paths.
