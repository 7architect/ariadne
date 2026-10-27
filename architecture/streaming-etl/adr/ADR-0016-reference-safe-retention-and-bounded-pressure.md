# ADR-0016: Reference-safe retention and bounded pressure

- Status: `progress`
- Date: 2026-10-23
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0015](ADR-0015-independent-branches-and-durable-join-buffer.md)
- Refines: [ADR-0006](ADR-0006-durable-channels-and-state.md), [ADR-0007](ADR-0007-backpressure-and-high-load.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-005c, DS-005d

## Problem

An append-only durable channel eventually fills its storage. Deleting by the
smallest live consumer cursor is unsafe because an in-flight delivery, a
selectable recovery checkpoint, a buffered join tail, or an operator pin may
still require an older record or ObjectRef. Allowing append beyond a declared
capacity merely moves the failure to disk exhaustion.

## Theory

The platform can derive compaction from an explicit reference graph. Every
recovery authority contributes a lower-bound root. Records before the smallest
root are reclaimable only after all direct references disappear. Admission can
reserve record and byte capacity in the same transaction as append. Reclaiming
records returns credits and wakes blocked producers without skipping sequence
or weakening durability.

## Agreement

For each partition, the retention planner considers these durable roots:

- every consumer's `completed_through + 1`;
- every in-flight delivery sequence;
- the replay suffix after every selectable checkpoint cut;
- branch/join buffer and explicit recovery pins;
- immutable output or completion witnesses that have not been archived.

The effective compaction limit is the smaller of the requested limit and one
less than the oldest root. Planning records an immutable manifest containing
the exact record and ObjectRef identities. Deletion and manifest completion are
one transaction in the prototype. A record with a live reference cannot be
deleted even if a caller forges a larger watermark.

Capacity is configured per partition in both records and payload bytes. Append
reserves both dimensions atomically. The R0 overload policy is `BLOCK`: a full
channel returns an observable blocked result without consuming sequence or
publishing a partial envelope. A successful compaction returns credits; retry
then appends at the next contiguous sequence. Timeouts and distributed wake-up
mechanics remain production-runtime work.

R0 capacity accounts for live envelopes and distinct payload bytes reachable
from those envelopes. Append-attempt history, completion witnesses, lifecycle
events, manifests, and tombstones remain durable protocol metadata. Their
retention budget and garbage-collection policy are deliberately deferred to
DS-011; therefore the R0 result proves bounded live data-plane pressure, not a
bound on total database growth.

Every new consumer declares an immutable `start_sequence`; omission means the
current retained floor. Checkpoints and durable references must also begin at
or above that floor. The platform rejects a late recovery authority that asks
for an already compacted prefix. Root creation and release are recorded in an
append-only lifecycle ledger, independently checked against the mutable current
view.

An append attempt has a durable identity over record, ObjectRef, payload digest
and size, schema, and lineage. After compaction, its tombstone retains that full
identity. An exact retry returns the original sequence from either the live
record or tombstone; any drift is rejected.

## Stories

- As a developer, I want every retention decision to name the references that
  prevent deletion.
- As a user, I want a stalled Anna to preserve her recoverable input while
  Boris can use only the remaining declared capacity.
- As a system, I want record and byte limits enforced atomically with append.
- As an operator, I want to inspect used capacity, available credits, oldest
  root, and the reason a producer is blocked.

## Acceptance criteria

- As a system, I cannot delete a record referenced by a consumer, in-flight
  delivery, selectable checkpoint, durable buffer, completion, or recovery pin.
- As a developer, I can inspect an immutable compaction manifest and reproduce
  why each retained record survived.
- As a user, I got ObjectRefs reclaimed only after their last record reference
  disappeared.
- As a system, I got no sequence gap or partial append when record or byte
  capacity was exhausted.
- As a user, I can advance the blocking root, compact, and retry the same append
  successfully with newly returned credits.
- As a system, I reject forged watermarks, deletion without a manifest, manifest
  mutation, and append whose durable object identity changed.
- As a developer, I can create a late consumer at the retained floor and cannot
  create a consumer, checkpoint, or recovery pin behind it.
- As a system, I got the original sequence for an exact append retry after
  compaction and rejected drift in record, ObjectRef, payload, schema, or
  lineage identity.

## W7–W8 evidence plan

The focused standard-library prototype will use a small capacity so saturation
is deterministic. Positive traces cover a stalled consumer, in-flight record,
checkpoint replay suffix, explicit pin, shared ObjectRef, compaction, credit
return, and exact append retry. An independent read-only oracle will recompute
the reference graph and capacity from raw rows. Unsafe direct-SQL controls will
delete a rooted record, overstate reclaimed bytes, mutate a manifest, and create
a sequence gap.

The executable evidence now contains ten focused cases: reference-safe suffix
retention, shared-object reclamation, bounded block/compact/resume, concurrent
capacity reservation, nine abrupt compaction/append boundaries, independent
unsafe controls, byte-only saturation, lost acknowledgements, exact retry
through tombstones, late-root rejection, create-once guards, and lifecycle-ledger
reconciliation. The ETL/HighLoad Principal reran the suite and prior hostile
traces on 2026-10-23, found no material R0-C defect, and passed DS-005c/d. The
8% gate is earned. This ADR remains `progress`; the production compactor,
distributed wake-up, metadata budget, and load SLO are governed by later
DS-011 tasks.
