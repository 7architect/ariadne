# ADR-0021: PostgreSQL coordinator checkpoints and frozen recovery plans

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0013](ADR-0013-managed-state-recovery-classes.md), [ADR-0014](ADR-0014-ordered-data-barrier-composition.md), [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md), [ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Leads to: [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0012](ADR-0012-protocol-epoch-2-delivery-gates.md)
- Delivery tasks: DS-030a, DS-030b, DS-030c

## Problem

The production-backed DATA path can complete one yielded turn atomically, but
shared or multi-input state needs a coherent cut across several durable
partitions. A periodic memory snapshot cannot establish which DATA records
belong before the cut. A coordinator status flag also cannot prove that every
source barrier and participant snapshot existed before recovery selected it.

Coordinator loss creates another ambiguity. If its lease epoch were part of the
checkpoint identity, a replacement coordinator would have to abandon a safe
partially injected intent. If an expired owner could continue writing, two
coordinators could complete incompatible views of one checkpoint.

## Theory

A checkpoint is an immutable intent independent of its current writer. DATA and
BARRIER share a serial partition log, while the coordinator lease epoch fences
each mutation. A replacement owner resumes the same intent, injects only its
missing barriers, and produces the same content-addressed snapshots.

Recovery is another create-only decision. It records an authority fence and
freezes the newest compatible completed checkpoint visible before that fence.
A completion arriving later cannot change the selected target.

## Proposed solution

Use PostgreSQL as the single authority for four related protocols:

1. An immutable deployment revision binds graph version and digest, source and
   participant manifests, code digests, state schemas, and checkpoint budgets.
2. A DB-clock coordinator lease admits one writer epoch. The epoch fences
   writes but is excluded from checkpoint semantic identity.
3. A create-only checkpoint intent precedes source BARRIER entries. Injection
   takes the same partition advisory lock and row lock as DATA append, then
   derives its cut from the locked partition head. Callers cannot supply a cut.
4. Participant acknowledgement binds the exact input barrier vector,
   content-addressed state snapshot, mailbox high-water, replay-tail floor,
   code/state contract, and an output BARRIER inserted after existing output
   DATA.

The lifecycle projection is:

```text
STARTED → ALIGNING → ALIGNED → COMPLETED
                    └────────→ ABORTED
```

Only a create-only completion witness authorizes recovery. Mutable lifecycle
status is a projection and is insufficient by itself. Completion and abort are
mutually exclusive.

The recovery protocol writes a fence, selects the newest compatible completion
before it, and persists a frozen plan containing exact DATA cuts and snapshot
references. Runtime restoration accepts only this plan and verifies the
participant, code digest, state schema/version, object digest, and object key.
The plan advances from `RESTORING` to `READY`; its identity and target cannot be
updated.

Source barrier observations create sealed roots in the existing DATA retention
graph. Compaction and barrier injection use the same partition lock. Therefore
compaction may reclaim records at or before the snapshot cut, while the first
post-cut DATA record remains a live replay tail. Coordinator tables remain
separate from DATA checkpoint roots; the shared table is used only for
retention authority.

Every revision has hard limits for in-flight checkpoints, retained checkpoints,
checkpoint metadata bytes and age, and authority events. Admission stops when
a limit is reached and exposes the binding dimension through pressure
inspection. Retirement and long-running checkpoint GC are deferred to later
operational work; the safe behavior in this gate is bounded refusal.

Every growing coordinator transaction recomputes its projected logical metadata
bytes before commit; an overshoot rolls back the whole transaction. The ordered
partition log has a separate hard accounted entry capacity inherited from the
partition metadata budget. It currently stops at that limit. Prefix summaries
and online ordered-log reclamation remain later operational work.

The platform injects barriers. Client generator code observes ordinary durable
inputs and produces ObjectRefs through `yield` or `return`; it does not call a
manual `ctx.barrier()` API. Client effects remain under the idempotency contract
in [ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

## Agreement

- Intent identity contains revision, ordinal, request identity, graph digest,
  and immutable manifests. It does not contain lease epoch.
- Every DATA and BARRIER entry has one contiguous `log_sequence`; DATA retains
  its separate `data_sequence`.
- Source and output BARRIER cuts are calculated inside the serial partition
  transaction.
- A new coordinator resumes a valid old intent after lease expiry. It does not
  abort merely because the writer epoch changed.
- Recovery selects only a create-only completion whose graph and participant
  contracts match the immutable deployment revision.
- Runtime restoration reloads the exact durable recovery row and verifies its
  digest, pre-fence completion, participant snapshot, and state version; a
  caller-supplied dataclass has no authority.
- Coordinator replay-tail roots cannot be released through the generic DATA
  API while their checkpoint remains completed. `READY` rechecks sealed roots
  plus every retained record and ObjectRef byte in the frozen tail.
- A recovery request is idempotent by request identity and keeps its original
  target even when a newer checkpoint completes.
- Independent recovery of DAG regions, real join alignment, and selective
  branch replay belong to R3-B and [ADR-0003](ADR-0003-branch-and-join-recovery.md).

## Stories

- As a developer, I want to inspect an immutable intent, exact source cuts,
  participant snapshots, completion witness, and frozen recovery plan.
- As a developer, I want a replacement coordinator to continue an existing
  checkpoint without changing its identity.
- As a user, I want yielded DATA to keep flowing between checkpoints and to
  replay only the durable suffix after the selected cut.
- As a system, I want DATA and BARRIER order decided by one partition lock and
  one log sequence.
- As a system, I want to reject stale coordinators, graph drift, incomplete
  checkpoints, incompatible state, mutable recovery targets, and exhausted
  budgets.

## Acceptance criteria

- As a developer, I can inspect one immutable revision and trace every
  checkpoint and recovery mutation through contiguous authority ordinals.
- As a developer, I can reproduce the R3-A gate from one harness and verify its
  source hashes and synchronous PostgreSQL profile.
- As a user, I got `DATA → BARRIER → DATA` without waiting for the next periodic
  snapshot and retained the first post-cut replay record during compaction.
- As a system, I got a replacement coordinator completing the same intent after
  expiry while the old epoch was rejected.
- As a system, I got the newest compatible create-only completion before the
  recovery fence, and a later completion did not alter the frozen plan.
- As a system, I got exact retry after abrupt process loss before and after
  every intent, barrier, participant, completion, recovery-plan, and
  recovery-ready commit boundary.
- As a system, I reject status-only completion, source/participant manifest
  drift, graph drift, snapshot corruption, code/state incompatibility,
  DATA/BARRIER rewrites, output-barrier overtaking, stale writers, and budget
  overflow.

## Prototype and evidence

[postgres_barrier_coordinator.py](../prototypes/postgres_barrier_coordinator.py)
implements immutable revisions, leases, authority events, the ordered
DATA/BARRIER log, checkpoint lifecycle, content-addressed participant snapshots,
complete-only selection, retention pins, and frozen recovery plans.
[postgres_checkpoint_runtime.py](../prototypes/postgres_checkpoint_runtime.py)
is the separate `CHECKPOINT_MANAGED_SHARED` runtime adapter; per-turn state keeps
using the existing generator runtime.

[postgres_barrier_oracle.py](../prototypes/postgres_barrier_oracle.py) imports
neither coordinator nor runtime code. It derives revision, log, intent,
lifecycle, snapshot, completion, recovery, retention, and budget invariants from
one read-only transaction. [postgres_barrier_crash_worker.py](../prototypes/postgres_barrier_crash_worker.py)
executes fourteen deterministic `os._exit(86)` boundaries. The focused suite in
[test_postgres_barrier_coordinator.py](../prototypes/test_postgres_barrier_coordinator.py)
contains ten cases covering serial DATA/BARRIER races, compaction/barrier
races, output order, takeover, exact retry, complete-only recovery, frozen
selection, forged recovery authority, contract incompatibility, all hard budget
dimensions, replay-pin release, bounded ordered-log metadata, exact authority
cross-links, total oracle behavior, storage guards, and an unsafe
status-without-completion control.

The reproducible [R3-A evidence report](../evidence/postgres-barrier-r3a-2026-10-25.json)
records 10/10 passing tests, all fourteen process-loss boundaries, file hashes,
PostgreSQL 17.11, `synchronous_commit=remote_apply`, and a synchronous streaming
standby. The complete PostgreSQL regression suite passes 39/39, and the
active/expired lease takeover scenario passes 20/20 isolated repetitions.
ETL/HighLoad Principal reproduced the seven initial hostile failures, verified
their fixes and the final source hashes, then recorded independent PASS on
2026-10-25. DS-030a…c and R3-A are complete; earned readiness is 64%.

### Hostile review history

The first Principal review returned `BLOCKED`. It demonstrated that a caller
could forge an in-memory recovery plan, a generic release could remove a
coordinator tail pin, state version was absent from revision identity, logical
metadata could exceed its byte limit, authority ordinals lacked exact event
cross-links, the oracle crashed on a missing barrier, and the ordered log had no
accounted bound. None of those failures was waived.

The revised protocol reloads durable recovery authority, storage-protects pins,
binds state version, rolls back projected-byte overshoot, validates event links
in storage and the independent oracle, reports malformed evidence as named JSON
violations, and stops a partition at its immutable ordered-log capacity. A
second review found a timing race only in the takeover test; separating active
lease rejection from DB-clock expiry produced 20/20 stable repetitions. The
third review recorded `PASS` with all original traces closed.

## Consequences and remaining work

The coordinator can recover its own intent and checkpoint decisions without
turning lease epoch into data identity. The data plane has one auditable cut,
and a mutable status row cannot authorize recovery.

This ADR remains `progress`. R3-B must implement live per-input alignment,
durable join buffering and apply ordinals, branch-local recovery regions,
recovery expansion when a tail is unavailable, and branched backpressure.
Operational checkpoint retirement, migration, observability, orchestration
server integration, and long-running load/chaos evidence remain later gates.

## Evidence required for `accepted`

- Production coordinator and runtime integration behind the supported server
  and worker APIs.
- R3-B branch/join recovery evidence and bounded checkpoint retirement.
- Upgrade, operations, load, chaos, and soak evidence from R4–R5.
