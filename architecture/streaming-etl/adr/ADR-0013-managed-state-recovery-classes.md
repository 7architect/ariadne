# ADR-0013: Managed state classes and recovery authority

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Leads to: [ADR-0014](ADR-0014-ordered-data-barrier-composition.md), [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0006](ADR-0006-durable-channels-and-state.md), [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-004S, DS-004a, DS-004b, DS-021b, DS-021c
- Review: DS-004S ETL/HighLoad Principal hostile PASS on 2026-10-21

## Problem

Per-input completion answers which document Tanya receives after Anna stops, but
stateful execution also needs the exact state from which Tanya continues. A
cursor at document 200 with state from document 100 silently skips 100 state
transitions. Independently snapshotting Python memory cannot prove that state,
the terminal `yield` or `return`, its platform output, and the input cursor refer
to the same completed turn.

The stronger alternative, restoring every operator from every periodic graph
checkpoint, is safe only with a retained replay tail and wastes the finer
recovery point already available to a single-input partition-local operator.
Multi-input, shared, timer, and watermark state cannot use that local shortcut
because event application order and a consistent input-vector cut matter.

## Theory

Recovery authority follows the state dependency shape. State private to one
serial partition turn can share the terminal-completion transaction. State that
spans inputs, partitions, timers, or watermarks needs a completed coordinator
checkpoint followed by deterministic replay of the retained ordered tail.

The platform never reconstructs arbitrary local variables or external client
effects. It only restores state declared under one supported managed-state
contract. Client code remains responsible for deterministic computation and
idempotency of PostgreSQL writes, HTTP calls, and other effects outside the
platform transaction, as defined in
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

## Agreement

### State classes

| Class | Recovery authority | Contract |
| --- | --- | --- |
| `EPHEMERAL` | None | Local Python values are recomputed and are not recovery state. |
| `PARTITION_LOCAL_PER_TURN` | Terminal turn completion | Mutable state belongs to one input partition, has one serialized owner, and can be serialized after each turn. |
| `CHECKPOINT_MANAGED_SHARED` | Completed coordinator checkpoint | State spans inputs or partitions, or depends on timers/watermarks. Replay is deterministic and multi-input application order is durable. |
| `CLIENT_EXTERNAL_EFFECT` | Client system | The client supplies stable keys, transactions, or another idempotency strategy. |
| `RECOVERY_METADATA` | Platform metadata | Immutable graph, code, state-schema, partition, generation, and identity fields select a compatible recovery plan. |

An operator declares one managed-state authority. If any field needs shared or
multi-input checkpoint recovery, the operator uses
`CHECKPOINT_MANAGED_SHARED` for its managed state or is split into operators
with separate boundaries. Recovery may not combine a local cursor head with an
older checkpoint snapshot.

### Partition-local recovery

For a local operator, one platform transaction binds:

```text
terminal completion N
+ optional platform output for yield N
+ managed state S_N
+ completed_through = N
```

Merely entering client code or observing an unfinished `yield` does not advance
the cursor. If Anna stops while processing document 200, recovery chooses the
highest contiguous terminal completion `H` with one exact output/state/cursor
witness, restores `S_H`, and redelivers `H + 1`:

```text
no durable terminal yield/return 200 → restore S_199 → Tanya receives 200
durable terminal yield/return 200    → restore S_200 → Tanya receives 201
```

An exact retry is a no-op. A retry that changes output identity, state, schema,
or code identity is rejected as drift.

### Checkpoint-managed recovery

For shared state, recovery selects the newest checkpoint `C*` that is:

1. completed by a create-only completion witness whose authority-log ordinal
   also precedes the recovery fence;
2. authorized by a create-only coordinator intent whose durable authority-log
   ordinal precedes the immutable recovery fence;
3. compatible with the graph, code, and state schema;
4. witnessed at every declared input by a BARRIER in the same ordered channel
   immediately after that input's declared cut; and
5. backed by every retained event needed from the cut to the recovery target.

The runtime restores snapshot `S_C*` and replays the exact persisted mailbox
order between the checkpoint cut vector and the durable target vector. An
incomplete, mixed-input, retroactive, incompatible, or expired-tail checkpoint
is not a recovery authority. Normal terminal completion rules create platform
outputs and advance cursors during replay; DS-004a and DS-004b must demonstrate
that composition with the durable channel.

The recovery fence is written once when recovery begins. Checkpoint intent,
checkpoint completion, and fence records share one monotonically ordered
authority log. A checkpoint created or completed after that fence is observably
retroactive even if its cuts and snapshot match the final state. A
caller-supplied `authorized=True` flag is insufficient and is not part of the
protocol.

The valid lifecycle order is strict:

```text
checkpoint intent ordinal < checkpoint completion ordinal < recovery fence ordinal
```

A completion event written before its intent cannot be attached later to make a
checkpoint eligible.

The storage model permits one recovery-fence event and rejects update or delete
of the authority log and fence. Checkpoint intent, cuts, barrier witnesses, and
their digest are atomically created and then sealed. Storage triggers reject a
coherent post-seal rewrite, while the independent oracle recomputes the digest
and validates the seal and both authority-log witnesses.
The completion event requires an already published matching seal. Once the
recovery fence exists, storage rejects publication of a new seal or completion
row, even when earlier event ordinals were preallocated. Authority ordinals are
contiguous and storage accepts only the exact next value, preventing backdating.

### Scope restrictions

- Multi-input state without a persisted deterministic mailbox order is rejected.
- Mutable state with nondeterministic replay is rejected.
- Arbitrary process or memory snapshots are not a substitute for a declared
  state schema, code identity, input cut, and ordered-tail witness.
- Stable platform record identity makes platform output replay checkable; it
  does not make client external effects exactly once.
- This SQLite spike deliberately did not select the physical production state
  and checkpoint backend. PostgreSQL was subsequently selected and validated
  by [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md).

## Stories

- As a developer, I want to declare whether an operator has partition-local
  per-turn state or checkpoint-managed shared state.
- As a user, I want Tanya to receive document 200 when Anna did not finish it,
  or 201 when Anna's terminal `yield` or `return` and state 200 are durable.
- As a system, I want one recovery authority for state, output, and progress so
  I cannot assemble a recovery point from unrelated writes.
- As a system, I want multi-input state restored from one completed vector cut
  and replayed in its original durable application order.

## Acceptance criteria

- As a developer, I can classify supported state and get a deterministic
  rejection for nondeterministic or unordered multi-input contracts.
- As a user, I can observe local recovery as exactly `S_199 + document 200` or
  `S_200 + document 201` around the terminal boundary.
- As a system, I got rejection of split cursor/state/output, incompatible state,
  mixed recovery authority, incomplete tail, wrong replay order, mixed barrier
  witnesses, and retroactive checkpoints.
- As a system, I can derive one plan: local atomic head, or completed checkpoint
  plus the exact retained ordered tail.
- As a system, I can compose DATA and BARRIER ordering with this state decision
  and survive process loss at every barrier lifecycle boundary before R0-B
  earns 6%.

## Prototype evidence

[state_recovery_spike.py](../prototypes/state_recovery_spike.py) persists the
two managed contracts and their recovery witnesses in a small SQLite model.
[state_recovery_oracle.py](../prototypes/state_recovery_oracle.py) is a separate
read-only process that imports no runtime implementation and derives the
recovery plan from one explicit database snapshot.
[test_state_recovery_spike.py](../prototypes/test_state_recovery_spike.py)
checks both sides of document 200, a completed two-input checkpoint plus an
order-sensitive tail, and unsafe mutations for every DS-004S invariant.
The controls include a checkpoint created after an immutable recovery fence,
hidden rows from the competing local authority, invalid input declarations, and
a valid empty replay tail. Storage controls also try to move the fence and
coherently rewrite a sealed intent, snapshot, cuts, witnesses, and completion;
each write is rejected.
The late-publication control records intent and completion labels, writes the
recovery fence, and then attempts to publish seal and completion rows; both
writes are rejected.

This proves the selected state equations in a local executable spike. DS-004a
later composed them with the epoch-2 DATA/BARRIER channel, and DS-004b injected
abrupt process loss around barriers. Both passed focused Principal review and
closed R0-B at 6%. Production backend and wider lifecycle evidence remain, so
this ADR stays `progress`.

The DS-004S hostile review passed after executable counterexamples forced
storage guards for immutable authority ordering, sealed manifests, lifecycle
ordering, and late-publication rejection. The focused state suite passes 10
tests. This closes W3 only; it does not satisfy DS-004a/b.

## Evidence required for `accepted`

- Production implementation of both declared managed-state contracts.
- Compatible state migration or deterministic rejection across code upgrades.
- Process, node, and storage failure evidence on the selected backend.
- Multi-input barrier alignment, ordered-tail retention, and load evidence.
- Completed acceptance criteria linked through ADR-0009.
