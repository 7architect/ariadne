# ADR-0015: Independent branches and durable join buffer

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0014](ADR-0014-ordered-data-barrier-composition.md)
- Refines: [ADR-0003](ADR-0003-branch-and-join-recovery.md)
- Leads to: [ADR-0007](ADR-0007-backpressure-and-high-load.md), [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-005a, DS-005b, DS-005c, DS-005d

## Problem

Fan-out cannot use one global downstream cursor. If Boris completes document
300 while Anna completed only 200, Boris must continue at 301 and Tanya must
replace Anna at 201. Advancing or rewinding both branches together either loses
Anna's records or duplicates Boris's completed work.

A join has a second boundary. A fast input can cross a checkpoint barrier while
a slow input still yields pre-barrier DATA. Post-barrier DATA from the fast side
must remain durable without entering the pre-barrier snapshot. A volatile queue
would lose that DATA on restart; applying it early would mix checkpoint cuts.

## Theory

One immutable upstream DATA envelope and ObjectRef can feed a frozen branch
manifest, while each branch owns a distinct delivery, in-flight, completion,
and cursor history. Join inputs remain bound to exact branch partitions. An
early join input records post-barrier DATA in a durable ordered buffer until all
inputs align; snapshot, output BARRIER, and buffer drain form one observable
order.

## Agreement

### Fan-out

Deployment freezes the exact branch manifest, for example `{anna, boris}`.
Upstream DATA is stored once. For each `(branch_id, partition)` the platform
stores independent:

- owner/generation and in-flight delivery;
- terminal completion witness;
- contiguous monotonic `completed_through`;
- stable output identity and lineage derived from deployment, branch, and
  upstream record identity.

Completion on one branch cannot read, advance, or rewind another branch cursor.
Replacing Anna with Tanya changes only Anna's ownership generation and resumes
at `anna.completed_through + 1`. Until DS-005d, the prototype assumes available
storage capacity. This is a test precondition rather than an unbounded-buffer
guarantee.

### Join

Each join input is immutably bound to one branch partition. DATA and BARRIER
retain the ordered identity and authority rules from
[ADR-0014](ADR-0014-ordered-data-barrier-composition.md). When one input observes
a barrier early, later records on that input enter a durable buffer containing:

```text
checkpoint, input, record identity, input sequence,
arrival_ordinal, payload digest/reference
```

Other inputs continue applying and yielding their pre-barrier DATA. Alignment
requires the same checkpoint, intent digest, and coordinator epoch on every
input. It atomically records the exact vector of DATA cuts and barrier log
positions, managed state, and mailbox high-water. All pre-cut outputs precede
one output BARRIER. Buffered records drain afterward in persisted
`arrival_ordinal`; neither local alignment nor drain waits for global
`COMPLETED`.

DATA admission requires the immutable output witness created with the bound
branch completion. BARRIER admission requires the immutable barrier-output
witness created when that branch observes the upstream fan-out barrier. The
join checkpoint intent includes the upstream intent digest, writer epoch, and
cut. A join therefore cannot synthesize a locally coherent vector that does not
belong to a checkpoint of the graph.

Recovery selects the latest compatible completed snapshot and computes:

```text
state_now = snapshot_state
          ⊕ ordered(applied turns and durable buffered turns after high-water)
```

Branch recovery remains branch-local. A healthy branch cursor is not part of
Anna's takeover transaction and cannot roll back with it.

## Stories

- As a developer, I want one upstream record and separate branch delivery and
  completion histories with stable lineage.
- As a user, I want Tanya to resume Anna at document 201 while Boris remains at
  301 and continues processing.
- As a system, I want branch completion to change only that branch's cursor.
- As a developer, I want to inspect the join barrier vector, mailbox high-water,
  and every durable buffered identity in its drain order.
- As a user, I want the healthy input to keep yielding pre-barrier DATA while a
  fast input accumulates post-barrier DATA durably.
- As a system, I want a snapshot containing every and only pre-barrier
  transition, followed by one output BARRIER and deterministic buffer drain.

## Acceptance criteria

- As a developer, I can inspect one upstream envelope referenced by Anna and
  Boris and two independent completion witnesses.
- As a user, I got Tanya receiving 201 after Anna stopped at 200, while Boris
  remained at 301 and completed a new record without waiting for Anna.
- As a system, I reject a shared cursor, branch cursor gap, cross-branch
  completion, manifest drift, output identity drift, and healthy cursor
  rollback during takeover.
- As a developer, I can inspect an exact join snapshot cut `{fast:2, slow:2}`,
  durable fast-side DATA 3 and 4, and their persisted drain order.
- As a user, I got slow-side DATA 1 and 2 before alignment, then one output
  BARRIER, then fast-side DATA 3 and 4.
- As a system, I reject rebound join inputs, mixed checkpoint/digest/epoch,
  barrier cuts beyond completion, pre-snapshot buffer application, live cursors
  substituted for barrier cuts, missing/duplicate/reordered buffer entries,
  early output BARRIER, early drain, retroactive barriers, and a join checkpoint
  without fan-out and branch barrier authority.
- As a system, I reproduce failure-free state and output identities after fresh
  reopen before the final barrier and after alignment.

## W6 evidence plan

PostgreSQL routed-join and regional-recovery work is now recorded in
[ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md). Its evidence
and limitations are separate from the W6 local model below.

The standard-library prototype will include an independent read-only oracle,
fresh-connection recovery tests, and hostile SQL controls. The primary fan-out
fixture covers upstream documents 1…301: Anna completes 1…200, Boris completes
1…300, Tanya gets 201, and Boris completes 301. The primary join fixture uses a
fast input with DATA 1/2, BARRIER, DATA 3/4 and a slow input with DATA 1/2 then
the same BARRIER.

W6 earned 7% after DS-005a and DS-005b received Principal PASS on 2026-10-22.
Retention, garbage collection, capacity, and backpressure remain DS-005c/d in W7–W8.
Subprocess crash evidence, production storage, and selective whole-DAG recovery
are later gates. This ADR remains `progress` until its broader production
acceptance evidence exists.

## Prototype evidence

[branch_join_v2.py](../prototypes/branch_join_v2.py) implements one immutable
fan-out input log, branch-local delivery/completion histories, branch-local
DATA and BARRIER output witnesses, exact join bindings, aligned vector
snapshots, and a durable join buffer ordered by the input log's global
`arrival_ordinal`.
[branch_join_oracle.py](../prototypes/branch_join_oracle.py) is a separate
read-only process and imports no runtime implementation.
[test_branch_join_v2.py](../prototypes/test_branch_join_v2.py) currently passes
seven focused cases.

The fan-out fixture stores 301 upstream DATA rows, completes Anna through 200
and Boris through 300, reopens the store, assigns Anna generation 2 to Tanya,
and proves Tanya receives 201 while Boris receives and completes 301 without a
shared cursor. A second fixture lets Boris observe a BARRIER and complete the
post-barrier record while Anna still has not observed it.

The join fixture first creates fan-out checkpoint 1 after DATA 1/2 and records
Anna's and Boris's branch barrier witnesses. Join DATA references their actual
branch output envelopes, and each join BARRIER references the witness of its
bound branch. Fast DATA 3 is persisted before join barrier observation and DATA
4 after observation. Both enter the same durable buffer with their original
global arrival order. Slow DATA 1/2 continue producing outputs. Alignment
captures `{fast:2,slow:2}` at mailbox high-water 4, emits one output BARRIER,
then drains fast DATA 3/4 at apply ordinals 5/6. Fresh reopen before the final
barrier and after alignment matches a failure-free copy byte-for-byte at the
logical row level.

Hostile controls cover extra branch membership, a shared/advanced branch
cursor, cross-record completion, missing or forged DATA and BARRIER source
witnesses, join-input rebinding, mixed snapshot cuts, changed BARRIER
digest/cut, changed buffer arrival identity, early buffer application, a
missing buffer row, and an output BARRIER that overtakes pre-cut DATA. A public
trace also proves that a join checkpoint cannot be created without its fan-out
checkpoint authority. Storage rejects protected mutations; after the relevant
guard is removed, the independent oracle reports the named violation.

The ETL Architect / HighLoad Principal gave DS-005a and DS-005b PASS on
2026-10-22 after independently checking the complete DATA and BARRIER authority
chains, branch-local recovery, exact reopen state, and rejected synthetic and
cross-branch inputs. W6 is complete and earned readiness is 7%. This ADR remains
`progress` because W7/W8 retention and pressure, subprocess crash evidence, and
production implementation remain open.
