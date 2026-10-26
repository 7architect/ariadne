# ADR-0014: Ordered DATA/BARRIER composition

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0013](ADR-0013-managed-state-recovery-classes.md)
- Leads to: [ADR-0015](ADR-0015-independent-branches-and-durable-join-buffer.md), [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-004a, DS-004b, DS-005a, DS-005b

## Problem

A periodic state snapshot alone loses the interval after its cut. A consumer
cursor alone cannot reconstruct shared multi-input state. DATA progress and a
checkpoint cut must therefore compose without becoming the same pointer.

Putting barriers in a separate control path creates an ordering race: the
platform cannot prove whether a DATA record belongs before or after a snapshot.
Waiting for global checkpoint completion before releasing every yielded record
would preserve a cut but destroy the required streaming behavior.

## Theory

DATA and BARRIER records share one immutable sequence namespace per input
partition. A DATA terminal `yield/return` advances only DATA completion. A
BARRIER fixes a state cut but never advances that completion cursor. Recovery
restores a completed snapshot and replays the exact durable DATA-turn suffix in
persisted mailbox order.

## Agreement

Each partition log contains:

```text
DATA 1, DATA 2, BARRIER checkpoint-7, DATA 3, ...
```

Every entry has one contiguous immutable `log_sequence`. DATA additionally has
a contiguous `data_sequence`. BARRIER contains a coordinator-authorized
checkpoint identity, immutable intent digest, coordinator epoch, and the DATA
cut preceding its position.

The platform maintains distinct positions:

```text
completed_data_through  # advanced only by terminal DATA turns
scanned_log_through     # includes handled DATA and BARRIER entries
snapshot vector cut     # fixed by ordered BARRIER entries
```

For one input, observing the barrier materializes the snapshot and immediately
releases subsequent DATA. Processing does not wait for the coordinator to move
the checkpoint from `ALIGNED` to global `COMPLETED`.

For multiple inputs, an early input pauses after its barrier. Its subsequent
DATA remains durable in the channel. Other inputs continue processing and
yielding their pre-barrier DATA until the same checkpoint arrives. Alignment
then atomically stores:

- the exact input vector of DATA cuts and barrier log positions;
- shared managed state;
- the durable mailbox apply high-water;
- the forwarded output BARRIER after every pre-barrier output.

The early buffered tail is then released without waiting for global checkpoint
completion. Zero blocking is not claimed: aligned snapshots temporarily pause
only an input that has crossed the barrier while another input has not.

Every shared-state DATA transition receives a unique durable `apply_ordinal`.
Arrival time is not recovery order. After failure, the system restores the
latest completed compatible snapshot and replays turns after its mailbox
high-water in `apply_ordinal` order. Stable output IDs identify already durable
platform outputs. External client effects remain governed by
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

The focused composition inherits DS-002 ObjectRef admission, in-flight turn,
lease, and terminal `yield/return` semantics from
[ADR-0010](ADR-0010-platform-durable-yield-channel.md). The prototype replaces
neither that evidence nor its payload-reference requirement; it adds ordered
BARRIER records and shared-state recovery around the already durable DATA turn.

Checkpoint lifecycle is:

```text
sealed intent → ordered barriers → local alignment/snapshot
              → ALIGNED → coordinator COMPLETED
```

A recovery fence rejects new intents, barriers, or completion. Stale
coordinator and producer epochs, missing or changed intent digests, decreasing
checkpoint IDs, sequence gaps, and DATA/BARRIER kind conversion are rejected.
An explicit recovery generation moves atomically from `RESTORING` to `READY`.
Before `READY`, normal DATA processing is rejected. Recovery validates the
selected intent, seal, barrier observations, vector cut, mailbox high-water,
tail state transitions, and existing output identities; it never inserts those
outputs again. At the `READY` transition, frozen target cursors must equal live
durable progress and `replayed_through_ordinal` must equal the complete mailbox
suffix. Subsequent DATA turns carry the recovery generation, so an independent
reader can distinguish restored tail from work performed after recovery. DATA
processing resumes after `READY`. The focused singleton-fence model permits
exactly one immutable recovery row with generation 1. A later recovery requires
a new authority/fence design and is outside DS-004a; `INSERT OR REPLACE` cannot
rewrite the existing witness.

## Example

```text
left:   DATA 1, DATA 2, BARRIER 7, DATA 3
right:  DATA 1, DATA 2, BARRIER 7, DATA 3

snapshot 7:
  cut = {left: 2, right: 2}
  mailbox_highwater = 4

current DATA cursors after continued streaming:
  {left: 3, right: 3}

recovery:
  restore snapshot 7
  replay durable mailbox turns left:3, right:3
  next DATA = {left: 4, right: 4}
```

The snapshot cut remains `{2,2}` while live DATA progress reaches `{3,3}`.

## Stories

- As a developer, I want to append DATA and coordinator-authorized BARRIER
  records through one ordered channel API and inspect separate DATA progress and
  checkpoint cuts.
- As a user, I want yielded records before and after local barrier handling
  without waiting for global checkpoint completion.
- As a system, I want an exact vector snapshot that includes every pre-barrier
  transition and excludes every post-barrier transition.
- As a system, I want to restore a snapshot and reproduce its exact durable
  suffix in persisted mailbox order.

## Acceptance criteria

- As a developer, I can inspect one contiguous immutable DATA/BARRIER log and
  independently inspect DATA completion, scan position, and snapshot cut.
- As a user, I got DATA 3 while the locally aligned checkpoint was not yet
  globally `COMPLETED`.
- As a user, I got document 200 after a barrier when completion remained 199,
  and document 201 after document 200 completed.
- As a system, I got a two-input snapshot excluding the early input's durable
  post-barrier DATA while the other input continued pre-barrier yields.
- As a system, I can restore the latest completed snapshot and replay the exact
  order-sensitive durable tail without duplicating platform output identity.
- As a system, I reject missing, mixed, stale, decreasing, divergent, and
  retroactive barriers, cursor/cut conflation, kind rewrites, sequence gaps,
  mailbox corruption, and output-barrier overtaking.
- As a system, I survive abrupt process exit and fresh reopen at every barrier
  lifecycle boundary before R0-B earns 6%.

## Prototype evidence

[durable_stream_barrier_v2.py](../prototypes/durable_stream_barrier_v2.py)
implements the composed in-process SQLite model.
[durable_stream_barrier_oracle.py](../prototypes/durable_stream_barrier_oracle.py)
is an independent read-only process that imports no runtime code.
[test_durable_stream_barrier_v2.py](../prototypes/test_durable_stream_barrier_v2.py)
covers single-input continuation, document 200, aligned two-input buffering,
exact ordered-tail recovery, incomplete-checkpoint exclusion, authority and
identity rejection, storage immutability, and corrupted-state controls.

The focused suite has nine cases. A fresh runtime connection fences recovery,
selects the completed compatible snapshot, restores it, replays turns 5 and 6,
persists recovery generation `READY`, preserves the exact pre-recovery output
set, and then processes left DATA 4. Independent negative controls corrupt a
BARRIER digest, coordinator epoch, checkpoint identity, and observation digest;
the oracle names and rejects each authority break. A direct-SQL negative control
tries to publish target `{left:2,right:2}` and replay high-water 4 over durable
progress `{left:3,right:3}` and turns through 6. Storage rejects both truncated
witnesses; after deliberately removing those guards, the oracle still rejects
the mismatch. The same case attempts `INSERT OR REPLACE` generation 2 after a
valid recovery and post-recovery turn; the create-once guard rejects it, and an
oracle control with removed guards rejects the unauthorized generation.

The DS-004a focused gate deliberately excluded abrupt process death. DS-004b
subsequently validated `os._exit` before and after barrier append, first
observation, final alignment, snapshot seal, forwarded barrier, coordinator
completion, recovery fence, and restore/replay.

The ETL/HighLoad Principal gave DS-004a a focused PASS on 2026-10-21 after
hostile controls for BARRIER authority, truncated recovery witnesses, and
`INSERT OR REPLACE` generation replacement passed. W4 is complete. This review
does not satisfy DS-004b or raise the R0-B earned percentage.

DS-004b has subprocess evidence in
[durable_stream_barrier_crash_worker.py](../prototypes/durable_stream_barrier_crash_worker.py),
[durable_stream_barrier_recovery_worker.py](../prototypes/durable_stream_barrier_recovery_worker.py),
and
[test_durable_stream_barrier_subprocess.py](../prototypes/test_durable_stream_barrier_subprocess.py).
The matrix performs 34 deterministic `os._exit(86)` crashes across barrier
append, first observation, final alignment, completion, recovery fence, and
snapshot/tail recovery. Each case opens a fresh recovery process, performs an
exact retry, checks SQLite integrity and foreign keys, and then invokes the
independent oracle. An fsynced caller marker distinguishes durable return with
lost ACK from an acknowledged call. The ETL/HighLoad Principal gave DS-004b and
the R0-B gate a focused PASS on 2026-10-21. DS-004b and W5 are `done`; earned
readiness is 6%. This ADR remains `progress` for its broader branch, production,
retention, and operations criteria.

R3-A now carries this composition into the PostgreSQL authority in
[ADR-0021](ADR-0021-postgresql-coordinator-checkpoints.md). The passed gate
uses the same partition lock for DATA append, BARRIER injection, and compaction;
proves an output order of `DATA → BARRIER → DATA`; freezes complete-only
recovery plans; and survives fourteen coordinator process-loss boundaries. This
is production-backend protocol evidence, while live DAG alignment and selective
branch recovery remain in R3-B.

## Evidence required for `accepted`

- Production ordered-log, state, snapshot, fencing, and retention backend.
- Branch/join and backpressure evidence from DS-005a…d.
- Upgrade, load, and production observability evidence.
