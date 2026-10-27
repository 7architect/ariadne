# ADR-0005: Replayable and idempotent operator contract

- Status: `rejected`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, SDK maintainers
- Depends on: [ADR-0001](ADR-0001-runtime-boundary.md)
- Leads to: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0004](ADR-0004-multi-sink-commit.md), [ADR-0007](ADR-0007-backpressure-and-high-load.md)
- Superseded by: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)

## Rejection decision — protocol epoch 2

The platform no longer requires deterministic operators or platform-controlled
sink effects as a precondition for streaming. It redelivers an input when no
durable `yield` or `return` exists; client code owns deterministic behavior and
idempotency of arbitrary effects. The earlier theory and its source/sink
prototype evidence remain below as historical evidence. They are excluded from
the active runtime contract.

## Problem

Python generators cannot resume an arbitrary instruction pointer after process
loss. Recreating a generator repeats code before the next `yield`, including
uncontrolled external side effects.

## Theory

Operators are safely replayable when persistent state belongs to the runtime,
records have stable identities, time/randomness are explicit inputs, and external
effects use the sink protocol. Requiring “idempotent tasks” is useful only when
the idempotency key and scope are specified.

## Proposed agreement

Streaming operators must satisfy the following contract:

- output is deterministic for input record, managed state, and declared context;
- durable state is accessed through runtime state APIs and included in snapshots;
- each input and output has a stable logical ID across replay;
- external side effects occur through declared sink adapters;
- direct effects must accept the runtime idempotency key
  `(job, graph-version, operator, partition, logical-record-id, effect-name)`;
- recovery generation is excluded from an effect key so the same logical effect
  retains its identity after reassignment;
- schema and operator identity changes require an explicit migration.

Local Python variables may cache derived data but are not recoverable. A source
must either restore a position or hand accepted input to a durable Ariadne
channel before acknowledging it.

## Versioned participant contracts

`CooperativeSinkV1` declares `prepare`, `commit`, `abort`, and `recover/status`.
Every call carries the canonical transaction ID plus coordinator and participant
fences. Prepare returns an immutable token identity; commit and abort accept only
that identity and are idempotent. Recover returns the durable participant state
without trusting coordinator-local acknowledgement. A conformance suite injects
lost acknowledgements, stale fences, changed payload/token identity, and repeated
terminal calls.

`ReplayableSourceV1` will declare acceptance, durable handoff, replay from an
opaque position, successor comparison, and acknowledgement. Its core invariant
is that acknowledgement is authorized only when a completed decision references
durable accepted-input/channel-cut coverage through that position. This contract
is planned for Sprint 3B after the cooperative sink contract freezes.

Sprint 3B1 now tests this contract in
[source_v1.py](../prototypes/source_v1.py). The adapter supplies a stable opaque
position and envelope; the runtime derives the record ID, assigns the contiguous
partition ordinal, and persists one authoritative row that represents both
acceptance and durable channel handoff. Opaque positions are equality keys and
are never ordered. A partition takeover passes through `FENCING`, calls the
external source fence, and becomes `ACTIVE` only after its acknowledgement.
Checkpoint vectors include `(0, null)` cuts for idle partitions and become
`COMMIT` only with one immutable cut per partition.

## Stories

- As a developer, I want the SDK to make replay-safe behavior the natural path.
- As a user, I want deployments to fail early when an operator declares an
  incompatible side effect.
- As a system, I want the same stable effect key on every retry and recovery.

## Acceptance criteria

- As a developer, I can store keyed state and timers without serializing a Python
  generator frame.
- As a developer, I can retrieve a stable idempotency key for every effect.
- As a system, I can detect operator identity or state-schema incompatibility
  before recovery.
- As a system, I got identical logical outputs after deterministic replay.

## Identity agreement

Canonical IDs are derived from immutable JSON manifests with a versioned hash
scheme:

- job from tenant and logical job name;
- graph version from job and canonical graph specification;
- operator and edge from graph version and named ports;
- partition from edge and partition number;
- source record from source partition and restorable source position;
- checkpoint from graph version and monotonic sequence;
- writer generation from graph version, partition, and fencing epoch.

The record registry separately binds a derived ID to an explicit lineage digest,
owner, and payload digest. Repeating an identical claim is safe; reusing the ID with different
lineage or payload is a conflict, including reuse in a later checkpoint for the
same job and graph version.

## Prototype evidence

[integrated_protocol.py](../prototypes/integrated_protocol.py) freezes graph
semantics into an identity-space digest, canonicalizes participant manifests,
binds the negotiated capability digest and complete participant-role manifest
into each checkpoint intent, and rejects capability, generation, role, or
participant drift. The earlier mutable/split model is retained as rejected
evidence in
[ADR-0092](ADR-0092-split-safety-state-machines.md).

The integrated coordinator now claims each `channel_append` in a durable record
registry keyed by job, graph version, and record ID. Record claim and artifact
write share one fenced transaction and channel admission requires explicit source
lineage; a later checkpoint cannot attach a different owner, lineage, or payload
digest to that ID.

The coordinator admits a checkpoint only after its requested/effective sink tier,
capability digest, and exact participant-role manifest match the registered
result returned by capability negotiation. Callers cannot silently substitute a
weaker contract or different participant identities for an existing graph
version.

The contract is now under executable validation, so this ADR moves to
`progress`. State APIs, timers, schema migration, and sync/async generator
integration remain unimplemented.

Hostile review rejected the first source-ledger iteration because it trusted an
adapter-supplied ordinal, allowed two records for one opaque position, permitted
channel/acceptance identity splits and deletion, lacked an external takeover
fence, and could not checkpoint an idle partition. Those counterexamples remain
part of the Sprint 3B1 evidence. The revised prototype rejects each case; ST-033c
and ST-033d remain open, so the 53% gate is not claimed.

A second hostile review kept ST-033a/b open. It found that an external fence ACK
must prove the exact durable epoch, raw storage can still forge a noncanonical
record ID or payload digest, an older committed cut can invoke an external ACK
after a newer cut, and concurrent identical accepts need conflict reconciliation
to the same ordinal. These are blocking counterexamples for the next iteration;
none changes the earned 49% Sprint 3A gate.

The corrected source contract passed the ST-033a/b gate. Its trust boundary is a
non-Byzantine control plane: database credentials and the decision-verification
channel are privileged. Protection from an arbitrary writer with those
credentials would additionally require signed receipts or storage privilege
separation.

The first ST-033c review then rejected a locally manufactured source `COMMIT`,
mutable checkpoint/vector storage, and a stale callback that could leave a false
`BLOCKED` result after a newer owner had already acknowledged a higher cut. The
next prototype revision replaces the local decision with a
`CombinedDecisionRef`. The source ledger is constructed with the trusted
coordinator decision-store location and reads that authority itself; callers
cannot supply a verifier per bind. It verifies job, graph, canonical checkpoint
and transaction identities, sequence, complete intent digest, capability digest,
current coordinator owner/fence, exact source-position vector, and the durable
`COMMIT` decision before binding a cut. Source scope freezes at
the first checkpoint; checkpoints start only in `BUILDING`; decision fields and
cuts cannot change after `COMMIT`. Covered old attempts reconcile to
`SUCCEEDED`. The external ACK effect key is
`(partition, opaque-position, runtime-ordinal)`; epoch authorizes a caller but is
not part of an already durable effect identity. This lets a newly fenced owner
confirm the same effect after an unknown result without duplicating it.

The second ST-033c review found three more bypasses: the caller could replace a
per-call verifier with an always-true function, transaction identity was not
recomputed, and SQLite `INSERT OR REPLACE` bypassed delete protection while
recursive triggers were disabled. The current revision removes the verifier
parameter, recomputes canonical transaction identity, and has create-only
`BEFORE INSERT` guards for checkpoints and cuts. A third hostile review is
pending, so ST-033c and the cumulative 53% gate remain open.

The third review confirmed those three guards, then found that treating epoch as
part of ACK identity prevented convergence after an external success, lost local
result, and partition takeover. The revised adapter contract accepts the same
effect under a higher active epoch and rejects stale epochs or position/ordinal
drift. A regression reopens the ledger after takeover and reaches `SUCCEEDED`
with one external effect. The fourth hostile review is pending.

The fourth review passed ST-033c after replay converged across the unknown-result
takeover. The final reference also binds each source partition to its generation
epoch from the coordinator intent; a current recovery owner may have a higher,
never lower, epoch. The trusted non-Byzantine decision-store boundary remains.
ST-033d subsequently passed after the independent oracle became total for
corrupted rows, validated terminal ACK evidence and source epochs, and retained
the earliest valid authorization for a cut repeated by a later checkpoint.
Sprint 3B1's cumulative 53% prototype gate is claimed; this ADR remains
`progress` because the SDK/runtime and production storage criteria are open.

ST-033d evidence is also under review. The independent
[source oracle](../prototypes/source_oracle.py) reads storage in read-only mode
and independently derives canonical IDs/digests, accepted ordinals, committed
cuts, external and local acknowledgements, replay starts, blockers, and safety
violations. The [source subprocess matrix](../prototypes/test_source_subprocess_recovery.py)
uses real `os._exit(17)` failures before acceptance, after its durable write,
after caller acknowledgement, before external ACK, after external ACK but before
the local result, and after the local result. An exact retry closes the unknown
external-result case; a deliberately corrupted database proves that the oracle
does not merely repeat runtime success.

## Evidence required for `accepted`

- SDK contract tests for sync and async generators.
- Static or runtime checks for undeclared sink effects where technically possible.
- Failure tests covering replay around every `yield` and state mutation boundary.

