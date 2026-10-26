# ADR-0004: Atomic outcome across multiple sinks

- Status: `rejected`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0005](ADR-0005-operator-contract.md), [ADR-0006](ADR-0006-durable-channels-and-state.md)
- Superseded by: [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)

## Rejection decision — protocol epoch 2

The platform will not coordinate arbitrary client effects or make multi-sink
atomicity part of the core streaming-flow contract. The client owns effect
idempotency; the platform provides at-least-once delivery and advances progress
when it durably observes `yield` or `return`. The theory and prototype evidence
below are retained unchanged as historical evidence and may inform an optional
connector-specific protocol. They are not active runtime requirements and their
former readiness percentage does not carry into protocol epoch 2.

## Problem

A branch can write one logical result to multiple external sinks. A crash after
sink 1 commits and before sink 2 commits creates a split outcome. Retrying a
non-idempotent sink can create duplicates. A barrier alone cannot make unrelated
external systems atomic.

## Theory

A durable global decision plus sink-specific recovery produces one final outcome
when every participant supports prepare/commit/recover. Idempotency without
prepare provides eventual convergence but cannot promise rollback or atomic
visibility. Strict simultaneous cross-system visibility additionally requires a
shared publish manifest or readers that honor a common version.

## Proposed agreement

Sink groups declare one of three capability tiers:

| Tier | Required sink capability | Runtime guarantee |
| --- | --- | --- |
| `T` cooperative | `prepare`, idempotent `commit`, `abort`, `recover`, fencing | One irreversible final decision; convergence requires coordinator and prepared sinks to become available again. Transient partial visibility can exist during commit. |
| `I` idempotent | Stable idempotency key and retryable write | Eventual convergence to all sinks; no atomic rollback or simultaneous visibility. |
| `B` basic | No prepare or idempotency contract | Best-effort writes with retries according to adapter policy; duplicates, loss after exhausted retries, or split outcomes are possible. |

Sprint 3B2 freezes a common observation vocabulary: `NONE`, `PARTIAL`,
`COMPLETE`, `UNCERTAIN`, and `GAPPED`. `T` may expose `PARTIAL` only after the
durable decision. `I` may expose `PARTIAL` or `UNCERTAIN` while stable-key
delivery converges. `B` may expose every state, including duplicates and a gap.

Every `B` execution persists a `BPolicy` in the immutable intent and capability
digest. The retry budget is the maximum total number of external attempts per
sink. The policy also fixes whether an unknown result is retried and
whether exhaustion blocks progress or explicitly advances with a gap. Source
acknowledgement across an exhausted gap is forbidden unless the deployment
selected the lossy `advance_with_gap` policy. The runtime must never describe
tier `B` as atomic, exactly-once, or eventually convergent.

Tier `I` persists an immutable delivery intent before its first external call.
The intent binds sink, stable effect key, and payload digest. Recovery repeats
the same key; drift is a conflict. `COMPLETED` and source acknowledgement remain
blocked until every required tier `I` delivery succeeds.

The executable ST-034a contract in
[tier_semantics.py](../prototypes/tier_semantics.py) encodes this agreement as
`TierGuaranteeV1`, `TierExecutionIntent`, `BPolicy`, and one common observation
schema. Sixteen focused tests validate actual effect identities and multiplicity,
exact attempt/manifests, canonical visibility, unknown outcomes, `B` budget and
lossy-gap policy, weakest-tier negotiation, and hostile runtime structures.
ETL/HighLoad Principal hostile review passed ST-034a on 2026-10-21. Adapter
behavior and durable authority evidence remain in ST-034b…e, so the 55% gate is
not yet claimed.

For tier `T`, the checkpoint coordinator:

1. asks every sink to prepare checkpoint `N` under a stable transaction ID;
2. records a durable `COMMIT` decision after all prepare acknowledgements;
3. sends idempotent commit to every participant;
4. on restart, reads the decision and repeats missing commits;
5. never changes a durable `COMMIT` decision to abort.

Each cooperative participant follows this versioned state machine:

```text
PENDING → PREPARED(token_digest) → COMMITTED
    └──────────────→ ABORTED
```

`prepare(transaction, payload, coordinator fence, participant fence)` returns a
stable token whose digest is persisted by both participant and coordinator.
`recover(transaction)` returns the durable participant state and token identity.
`commit` and `abort` are idempotent for the same token. `abort` is legal only
before the global `COMMIT`; it is retried for every prepared participant after a
prepare failure. Participant storage independently rejects an obsolete
participant generation as well as an obsolete coordinator fence.

Abort itself has a durable decision boundary. The authority writes `ABORTING /
ABORT`, advances the coordinator fence, and only then calls participants. A
failed participant does not stop cleanup of the remaining manifest. The
checkpoint reaches `ABORTED` only when every exact manifest participant is
durably `ABORTED`; a retry recovers unknown external state by transaction ID.
`COMMIT` and `ABORT` decisions are both irreversible.

Creation of the global decision is a storage invariant rather than only an API
precondition. The authority accepts `NULL → COMMIT` only with old phase
`PREPARED`, new phase `COMMIT_DECIDED`, unchanged intent and ownership, and every
sink row in `PREPARED`. Raw SQL that skips these conditions must fail.

Before the decision, a failed prepare causes abort. After the decision, recovery
must converge by commit. Coordinator state requires high availability and
fencing as defined in [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md).
The checkpoint cannot become `COMPLETED` or advance the acknowledged source
position before this durable `COMMIT_DECIDED` record exists.

The combined intent also binds graph version, fencing generation, operator
manifest, sink manifest, and source position. These fields prevent recovery from
joining a valid decision to a checkpoint produced by different code, ownership,
or participants. It also declares the expected visible record and payload digest
for every sink. The independent oracle compares this manifest with externally
observed effects, so a locally completed transaction with missing output fails
validation. Any visible effect before `COMMIT_DECIDED` also fails. Between the
decision and `COMPLETED`, observed effects must be a subset of the manifest;
after completion they must equal it. As soon as a particular sink reports an
external commit, all expected effects for that sink must already be visible.
External sink and source identities must be subsets of their immutable manifests
at every phase.

External calls use a canonical transaction ID derived from job, graph version,
and canonical checkpoint ID. This prevents equal checkpoint sequence numbers in
different jobs from collapsing into one external transaction. Calls also carry
coordinator and participant fencing epochs.

This is a safety guarantee, not unconditional liveness. If a prepared sink stays
unavailable, the transaction remains observably `COMMIT_DECIDED` or `COMMITTING`;
the runtime reports blocked participants, age, and retry state. It must not claim
`COMMITTED` or silently abort.

Recovery attempts every pending participant even if one callback is unavailable.
For each participant it durably records attempt count, last error, and blocked
state so operators can distinguish convergence from a stalled commit.
An attempt is registered as `IN_PROGRESS` before the external call, then changes
to `SUCCEEDED` or `BLOCKED` through an owner/fence/attempt-number CAS. On restart,
authoritative local `COMMITTED`, `ACKED`, or `ABORTED` state reconciles an
`IN_PROGRESS` row whose result write was interrupted. Operator, sink, and source
partition identities are pairwise disjoint, preventing one role from replacing
another role's diagnostics.

For strict cross-sink visibility, sinks first publish versioned invisible data.
The coordinator then atomically publishes a shared manifest. Consumers must read
only versions referenced by that manifest. This stronger protocol is optional
because it changes reader behavior.

## Stories

- As a developer, I want a sink adapter to declare and test its exact commit
  capability.
- As a user, I want the runtime to reject an atomicity request that the selected
  sinks cannot satisfy.
- As a system, I want to finish every durable commit decision after a crash.

## Acceptance criteria

- As a system, I can recover after failure before prepare, between prepares,
  before decision, after decision, and between any two sink commits.
- As a system, I got one final value per transaction in every tier `T` sink after
  repeated recovery.
- As a user, I can see the effective tier and whether transient partial visibility
  is possible.
- As a user, I can see every transaction blocked in `COMMITTING`, its unavailable
  participants, age, and retry status.
- As a developer, I can run a conformance suite for a sink adapter.
- As a system, I can fence an obsolete coordinator and sink writer.

## Prototype evidence

`test_durable_commit_decision_converges_after_coordinator_crash` in
[test_protocols.py](../prototypes/test_protocols.py) injects a crash after the
first of two sink commits. Recovery reads the durable decision and idempotently
commits both sinks. Repeated recovery does not duplicate the logical value.

This proves eventual convergence for cooperative prototype participants. It does
not prove simultaneous visibility: the test intentionally observes sink 1 while
sink 2 is still empty after the injected crash. The prototype also rejects reuse
of a transaction ID with a different value or participant manifest.

`Coordinator` in
[integrated_protocol.py](../prototypes/integrated_protocol.py) closes the earlier
model gap between checkpoint completion and sink decision. Its tests reject
source ACK and sink commit without a durable decision, reject manifest and
capability drift, bind negotiated roles to checkpoint participant identities,
fence coordinator and participant generations, detect missing expected output,
and idempotently retry each external sink and source participant after reopen.

The prototypes do not yet prove real timeout behavior, coordinator HA, network
partitions, or a real external connector. The ADR remains `progress`.

Sprint 3A adds a durable `CooperativeSinkV1` reference participant and tests
partial prepare, lost abort acknowledgement, unavailable commit and abort
participants, same-owner and takeover races, transaction-scoped fencing, raw
storage decision forgery, and crashes on both sides of the recovery-attempt
result write. The 92-test suite passes, and ETL/HighLoad Principal hostile review
approved the cumulative 49% prototype gate on 2026-10-20. Timeout exceptions are
covered as protocol inputs; real network, host, and storage failures remain
outside this evidence.

## Sprint 3 counterexamples

Hostile review reproduced four gaps in the initial model: prepared participants
were left without abort after a predecision failure; an external participant
accepted a lower participant epoch; raw SQL forged `COMMIT_DECIDED` directly
from `STARTED`; and recovery stopped at the first unavailable sink without
durable diagnostics. Sprint 3A retains these as negative controls while
implementing the agreement above. Source acceptance coverage, behavioral tiers
`I`/`B`, and the multi-sink TLA+ model remain subsequent work linked from
[ADR-0005](ADR-0005-operator-contract.md) and
[ADR-0009](ADR-0009-failure-injection-evidence.md).

## Rejected claim

“Idempotent tasks make multiple arbitrary sinks exactly-once and atomic” is
rejected. Idempotency prevents duplicate effects for a stable key; it does not
undo a committed write or hide partial visibility across systems.

