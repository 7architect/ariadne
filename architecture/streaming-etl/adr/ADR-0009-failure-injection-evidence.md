# ADR-0009: Failure-injection evidence

- Status: `progress`
- Date: 2026-10-19
- Owner: ETL Architect / HighLoad Principal
- Validates: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md), [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md), [ADR-0019](ADR-0019-production-generator-state-and-replacement.md), [ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md), [ADR-0021](ADR-0021-postgresql-coordinator-checkpoints.md), [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0003](ADR-0003-branch-and-join-recovery.md)
- Active test code: [durable_stream_v2.py](../prototypes/durable_stream_v2.py), [durable_object_store.py](../prototypes/durable_object_store.py), [durable_stream_v2_crash_worker.py](../prototypes/durable_stream_v2_crash_worker.py), [durable_stream_v2_recovery_worker.py](../prototypes/durable_stream_v2_recovery_worker.py), [durable_stream_v2_oracle.py](../prototypes/durable_stream_v2_oracle.py), [test_durable_stream_v2.py](../prototypes/test_durable_stream_v2.py), [test_durable_stream_v2_subprocess.py](../prototypes/test_durable_stream_v2_subprocess.py), [test_durable_stream_v2_r0_gate.py](../prototypes/test_durable_stream_v2_r0_gate.py), [state_recovery_spike.py](../prototypes/state_recovery_spike.py), [state_recovery_oracle.py](../prototypes/state_recovery_oracle.py), [test_state_recovery_spike.py](../prototypes/test_state_recovery_spike.py), [barrier_protocol.py](../prototypes/barrier_protocol.py), [test_barrier_protocol.py](../prototypes/test_barrier_protocol.py)
- Historical test code: [protocols.py](../prototypes/protocols.py), [integrated_protocol.py](../prototypes/integrated_protocol.py), [test_integrated_protocol.py](../prototypes/test_integrated_protocol.py)

## Protocol epoch 2 reset

ADR-0010 and ADR-0011 reset active correctness evidence to the durable-yield
model. The source/sink decision matrices below remain historical rejected-scope
evidence and do not contribute readiness to the new target. Active evidence
starts with [durable_stream_v2.py](../prototypes/durable_stream_v2.py) and
[test_durable_stream_v2.py](../prototypes/test_durable_stream_v2.py).

The new mandatory crash matrix covers ObjectRef upload, envelope append,
delivery claim, lease takeover, runtime observation of `yield`/`return`, atomic
output/state/progress, stale completion, barrier crossing, and retention. It
must prove both sides of the sequence-200 boundary.

## DS-003a real-process crash evidence

DS-003a passed ETL/HighLoad Principal review on 2026-10-21. The active matrix
uses 22 deterministic `os._exit(86)` cases and no timing sleeps. It covers:

- filesystem ObjectRef upload before publish, after file and directory `fsync`,
  around witness commit, after commit before acknowledgement, and after the API
  returns;
- envelope append and delivery claim before mutation, after mutation before
  commit, and after commit before acknowledgement;
- client runtime observation of both `yield` and `return` before platform
  completion;
- YIELD output insertion, immutable completion marker, managed state, cursor
  update, post-commit ambiguity, and a true post-return point;
- the corresponding RETURN completion/progress cuts without an output.

Every case starts with a fresh directory and database, kills a child process,
opens a new `DurableStream`, runs SQLite integrity and foreign-key checks, then
proves a convergent exact retry or logically timed fenced takeover. The
observation cases deliberately execute the client effect twice after replay;
this is evidence that arbitrary client effects remain outside platform
atomicity rather than an exactly-once claim.

A positive no-fault control reaches the same durable result. A clean-exception
control proves ordinary Python unwinding performs a live-process rollback and
therefore is not counted as abrupt-crash evidence. The filesystem model proves
process-loss behavior after file `fsync`, atomic rename, and directory `fsync`;
it does not certify host power loss or production storage durability.

DS-003a completed W1's planned work. At that point earned readiness remained 0%
pending the independent oracle, orphan ObjectRef, and lost runtime
acknowledgement evidence subsequently completed by DS-003b/c.

## DS-003b/c R0-A gate evidence

DS-003b and DS-003c passed ETL/HighLoad Principal hostile review on 2026-10-21,
closing R0-A and earning cumulative readiness of 3%. The independent oracle is
a separate standard-library process. It imports no runtime code, opens SQLite
with `mode=ro` and `query_only`, and reads one explicit transaction snapshot
against a declarative graph manifest.

The sequence-200 matrix seeds inputs through 201 and terminal completions through
199. Eleven abrupt process-loss cases cover observed YIELD/RETURN, marker and
cursor pre-commit cuts, post-commit/pre-return ambiguity, and true post-return
loss. Before recovery, the oracle derives one resume position: 200 when the
terminal transaction rolled back and 201 when completion 200 is durable. A
fresh recovery process advances logical time, fences Anna, assigns Tanya, and
proves the same decision. Exact completion retry creates no second platform
output.

The oracle independently checks byte digests and sizes, durable witnesses,
contiguous partitions, the exact unique declared consumer set, cursor and
in-flight bounds, complete terminal prefixes, input identity, canonical state,
the full YIELD envelope manifest, RETURN without output, exclusive ordered
output coverage, and the derived resume position. Seventeen unsafe controls
cover missing or changed blobs, non-durable references, sequence gaps, split
cursor/completion/state/output, malformed JSON, manifest drift, invalid
in-flight state, omitted/unknown/duplicate consumers, and a physical swap of
output records with matching rewritten completion manifests and digests.

Object upload crashes distinguish staging files from published blobs without a
witness. Both remain invisible to the stream and are reported separately; an
exact upload retry repairs them and divergent bytes are rejected. Detection and
retry are in scope. Garbage collection belongs to later retention work.

This gate proves local-filesystem and SQLite process-loss behavior for one
partition, one consumer, one exclusive output, and one terminal event per input.
It does not prove host or power loss, production object-store consistency,
async SDK behavior, barriers, branches, retention, or load. ADR-0009,
ADR-0010, and ADR-0011 therefore remain `progress` rather than `accepted`.

## DS-004S managed-state decision evidence

DS-004S passed ETL/HighLoad Principal hostile review on 2026-10-21 and selects
two managed recovery authorities in
[ADR-0013](ADR-0013-managed-state-recovery-classes.md). Partition-local
single-input state is stored atomically with its terminal output/completion and
cursor. Shared, multi-input, timer, or watermark state restores the newest
completed compatible coordinator checkpoint and replays the exact retained tail
in persisted mailbox order.

The independent state oracle runs in a separate process, imports no spike code,
and reads one read-only transaction. Its positive cases recover local state at
199 with next input 200 and at 200 with next input 201, and recover an
order-sensitive two-input state from checkpoint cuts `{left: 1, right: 1}` plus
the exact three-event tail. An incomplete newer checkpoint is ignored.

Unsafe controls independently alter cursor, state, output coverage, schema,
replay order, tail retention, recovery authority, barrier-log position,
checkpoint witness identity, checkpoint authorization, and compatibility. The
storage guards reject illegal lifecycle mutations, and the oracle rejects
deliberately corrupted states by named violation. This closes the blocking
state/replay decision only. At that W3 boundary, DATA/BARRIER composition and
abrupt barrier process-loss evidence still remained in DS-004a/b. Those tasks
subsequently passed as recorded below.

Checkpoint authorization is an ordered witness rather than a trusted boolean:
both the create-only checkpoint intent and completion must precede an immutable
recovery fence in the same authority log. Negative controls create and complete
otherwise plausible checkpoints after that fence; the oracle identifies both
as retroactive. A unique fence and storage triggers reject moving the fence or
rewriting sealed checkpoint content coherently after authorization.
Another negative control persists completion before intent; both the storage
guard and independent oracle reject the reversed lifecycle order.
The late-publication control proves that pre-fence event labels are insufficient:
the sealed manifest and completion row must already exist before recovery begins.

## DS-004a ordered DATA/BARRIER composition

DS-004a composes the epoch-2 DATA completion contract with coordinator BARRIER
records in one immutable per-partition sequence, as specified by
[ADR-0014](ADR-0014-ordered-data-barrier-composition.md). DATA completion,
barrier scan position, and snapshot cut remain separate authorities.

The single-input case processes post-barrier DATA while the checkpoint is only
`ALIGNED`. The document-200 case proves barrier handling leaves completion at
199 until DATA 200 reaches terminal completion. The two-input case durably holds
early left DATA 3 while right DATA 2 continues yielding, snapshots exact cuts
`{left:2,right:2}`, forwards the output barrier after four pre-barrier outputs,
then releases left 3 and right 3.

After closing the original connection, a fresh runtime fences recovery,
restores that snapshot, and replays the two-turn suffix in persisted apply
order. It records a `READY` recovery generation, preserves existing output
identities, and accepts the next DATA turn. The independent read-only process
derives the same plan. Storage and oracle controls reject cursor/cut
conflation, mailbox and output order corruption, kind rewrite, sequence gap,
missing/wrong/stale/retroactive barrier authority, corrupted BARRIER and
observation identity, divergent record identity, and incomplete-checkpoint
selection. A direct-SQL control also proves that a `READY` recovery witness
cannot truncate current cursors `{3,3}` to `{2,2}` or stop replay at ordinal 4
when durable turns continue through ordinal 6. A second direct-SQL control
proves `INSERT OR REPLACE` cannot discard generation 1 and reclassify a
post-recovery turn under a forged generation 2.

This is the W4 in-process composition evidence. Abrupt exit and fresh-process
reopen at each barrier lifecycle boundary were deferred to DS-004b and are
recorded next.

## DS-004b abrupt barrier and recovery matrix

DS-004b executes 34 named `os._exit(86)` boundaries. Barrier
append has separate exits after entry insertion and after partition-cursor
advance. Precommit
exits during barrier append, observation, alignment, completion, recovery-fence
creation, and recovery leave no partial transaction. Postcommit exits retain
the complete atomic result. An fsynced caller ACK makes the two postcommit
outcomes externally distinguishable without sleeps.

Every case reopens the database in a new recovery worker process. Exact retry
converges to one BARRIER per input, one observation per input, one vector
snapshot, one forwarded BARRIER output, one completion, one recovery fence, and
one generation-1 `READY` recovery witness. Recovery replays turns 5 and 6 over
snapshot high-water 4 without changing the seven existing outputs, then writes
new left DATA 4 as generation 1. SQLite integrity, foreign keys, and the
independent read-only oracle pass after convergence. The ETL/HighLoad Principal
gave DS-004b and R0-B a focused PASS on 2026-10-21. Earned readiness is now 6%.

## DS-005a/b independent branches and durable join buffer

The W6 candidate implements the agreement in
[ADR-0015](ADR-0015-independent-branches-and-durable-join-buffer.md). Its fan-out
fixture stores one 301-record upstream log while Anna and Boris maintain exact
independent delivery, completion, and cursor histories. After fresh reopen,
Tanya receives Anna's record 201 and Boris receives record 301. A branch-local
BARRIER case proves Boris can cross the barrier and complete post-barrier DATA
while Anna still lags.

The skewed join fixture captures vector cut `{fast:2,slow:2}` and mailbox
high-water 4. Fast DATA 3/4 remain in a durable buffer with global arrival
identity, slow DATA 1/2 continue before alignment, one output BARRIER follows
all pre-cut outputs, and buffer drain occupies apply ordinals 5/6. Fresh reopen
before and after alignment converges with a failure-free copy.

Every join DATA entry now references an immutable output envelope created in the
same transaction as its bound branch completion. Every join BARRIER references
an immutable witness created when that branch observes the upstream fan-out
barrier; the join intent binds the fan-out intent, epoch, and cut. The oracle
follows those witnesses through join entries, observations, buffer turns, and
the snapshot.

The independent read-only oracle rejects shared cursor advancement, branch
manifest/completion drift, missing or forged DATA/BARRIER source witnesses,
join-input rebinding, mixed cuts, barrier authority drift, buffer
loss/reordering/early application, and output-barrier overtaking. The public
negative trace rejects a join checkpoint with no fan-out authority. Seven
focused tests and the full 171-test suite pass. The ETL/HighLoad Principal gave
DS-005a/b PASS on 2026-10-22, closing W6 and raising earned readiness to 7%.

## DS-030a/b/c PostgreSQL coordinator gate

[ADR-0021](ADR-0021-postgresql-coordinator-checkpoints.md) moves checkpoint
authority onto the synchronous PostgreSQL data plane. The focused suite has
ten cases and a separate oracle process. Fourteen deterministic
`os._exit(86)` points cover both sides of intent, source BARRIER, participant
snapshot, completion, recovery-plan, and recovery-ready commits; exact retry
converges after each exit.

Hostile cases cover concurrent DATA/BARRIER order, barrier/compaction order,
output `DATA → BARRIER → DATA`, stale writer fencing, graph and state-contract
drift, incomplete-checkpoint exclusion, immutable recovery selection, hard
checkpoint pressure, storage mutation guards, and a deliberately forged
completed lifecycle with its create-only completion witness removed. The
independent oracle rejects the forged state.

The [R3-A evidence report](../evidence/postgres-barrier-r3a-2026-10-25.json)
records file hashes, 10/10 passing tests, all fourteen abrupt exits, PostgreSQL
17.11, `remote_apply`, and a synchronous streaming standby. A 39/39 regression
run and 20/20 isolated takeover repetitions also pass. ETL/HighLoad Principal
reproduced and closed seven hostile findings, verified the final hashes and
fourteen crash boundaries, and recorded PASS on 2026-10-25. R3-A earns 64%.

## Problem

Happy-path examples cannot establish recovery safety. Protocol claims must survive
crashes and conflicting retries at every durable boundary.

## Theory

Small executable models expose incorrect invariants before production code fixes
the storage or API shape. Each test should identify exactly which claim it proves
and which production conditions it omits.

## Agreement

Python standard-library prototypes test safety state machines first. Once the
state vocabulary is stable, TLA+ models explore checkpoint, fencing, and
multi-sink interleavings within documented bounds. Go prototypes then measure
segmented-log throughput, bounded queues, slow sinks, disk watermarks, and replay
performance after the safety model is stable.

Prototype evidence can keep an ADR at `progress`; it cannot by itself make a
production ADR `accepted`.

## R1/R2 PostgreSQL candidate evidence

The current candidate executes 22 backend and seven generator-runtime tests on
PostgreSQL. The hostile controls cover lost retention roots, cross-linked YIELD
outputs, forged committed-batch retry envelopes, object-key drift after payload
GC, record/byte/age/metadata/consumer pressure, metadata-operation cut drift,
retired-anchor corruption, missing hot append authority, content-address
corruption, 100 metadata-retirement cycles, retry-versus-retire races,
decorator admission, repeated sync-thread cancellation, and async code that
suppresses cancellation. The runtime subprocess matrix adds 22 abrupt
sync/async YIELD/RETURN traces with a fresh independent oracle.

Two unchanged-code load trials pass every pre-agreed threshold and a fresh HA
run preserves every acknowledged append and exact durable cursor through
primary loss and standby promotion. Raw evidence:
[trial 29](../evidence/postgres-load-release-trial29-2026-10-24.json),
[trial 30](../evidence/postgres-load-release-trial30-2026-10-24.json), and
[HA release 3](../evidence/postgres-ha-release3-2026-10-24.json). Candidate
hashes and measured values are recorded in [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md),
[ADR-0019](ADR-0019-production-generator-state-and-replacement.md), and
[ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md). The
ETL/HighLoad Principal independently reran 22/22 backend and 7/7 runtime tests,
verified both frozen-hash load artifacts and current-hash HA provenance, and
recorded PASS with no blockers on 2026-10-24. R1-A, R1-B, R2-A, and R2-B are
closed and cumulative protocol readiness is 55%. Production integration and
later acceptance evidence remain unclaimed.

## Current evidence

Run from `architecture/streaming-etl/prototypes`:

```shell
python -m unittest discover -v
```

The current suite covers:

- selective replay of a failed branch and its join tail;
- rejection of an incomplete checkpoint;
- immutable checkpoint participant manifests and snapshots;
- conflicting logical-record reuse;
- non-contiguous sequence rejection in the single-partition prototype;
- recovery after a crash between two cooperative sink commits;
- immutable multi-sink transaction value and participant manifest;
- stable derivation and immutable claims for all protocol identity classes;
- legal and illegal checkpoint lifecycle transitions across store reopen;
- capability negotiation and deployment rejection for unsupported guarantees;
- 27 deterministic boundary/offset failure points around nine durable
  boundaries, each running the actual protocol through reopen, retry, recovery,
  and oracle evaluation;
- 27 additional cases covering every legal forward and abort predecision phase
  edge at all three failure offsets;
- independent recovery, decision, payload, and duplicate-policy invariants;
- one combined checkpoint/decision/source-ACK lineage;
- crash recovery before and after `COMMIT_DECIDED` plus lost ACK retry;
- preservation of the immutable decision when an external sink commits before
  its local acknowledgement;
- rejection of visible sink effects before `COMMIT_DECIDED` and of stable record
  IDs reused with divergent payloads across checkpoints;
- rejection of a committed sink with missing expected effects, or any external
  sink/source identity absent from the immutable participant manifests;
- total oracle evaluation for missing required local sink/source rows, reported
  as named invariant failures rather than verifier exceptions;
- an unsafe split-decision control that the oracle rejects;
- durable cooperative-sink prepare tokens and transaction-scoped coordinator
  plus participant-generation fences;
- irreversible storage-authoritative `COMMIT` and `ABORT` decisions over an
  exact immutable sink manifest;
- cleanup and commit recovery that continue past an unavailable participant;
- durable `IN_PROGRESS`, `SUCCEEDED`, and `BLOCKED` recovery attempts across
  callback-result crashes, reopen, owner takeover, and terminal reconciliation;
- rejection of cross-role participant identity collisions before persistence.

## Required failure matrix

| Boundary | Before | After durable write, before ACK | After ACK |
| --- | --- | --- | --- |
| Phase transition | Retry expected transition | CAS resolves durable ambiguity | Repeated target state is a no-op |
| Channel append | Retry same logical record | Detect identical retry or conflict | Advance only contiguous partition cursor |
| Snapshot | Previous checkpoint remains | Recover immutable snapshot by identity | Participant ACK is repeatable |
| Checkpoint decision | Incomplete checkpoint ignored | Decision lookup resolves ambiguity | Completed checkpoint immutable |
| Sink prepare | Abort undecided transaction | Recover prepare token | Continue preparing or abort before commit decision |
| Commit decision | Abort allowed | Decision lookup resolves ambiguity | Abort forbidden; retry commit |
| Sink commit | Retry commit | Recover by transaction ID | ACK is repeatable |
| Checkpoint completion | Decision remains recoverable | Reopen and complete from participant state | Repeated completion is a no-op |
| Source acknowledgement | Reject without matching decision | Durable combined record preserves the implication | Retry acknowledgement without manifest drift |
| Recovery attempt | Durable `IN_PROGRESS` row exists before callback | Reconcile unknown outcome from participant state | Owner/fence/attempt CAS records final outcome |

## Sprint 1 evidence agreement

The integrated deterministic fault API names every point as
`<boundary>:before`, `<boundary>:after_durable_before_ack`, or
`<boundary>:after_ack`; it uses no sleeps. Every point runs against the SQLite
coordinator, reopens it, retries idempotent external effects, and feeds raw state
plus external observations to an independent oracle. The oracle checks global
recovery selection by job and graph, required restorable artifacts, sink effects,
source ACKs, visible payload stability, duplicate policy, participant
completeness, the complete expected-effect manifest, and agreement between local
and external state.

The combined-progress tests establish the target invariant:

```text
source_ack(N)
⇒ durable COMMIT_DECIDED(N, graph_version, generation,
                         operator_manifest, sink_manifest)
```

The test suite includes deliberately unsafe observations with source ACK ahead of
decision and a missing required sink; the oracle fails their named invariants.
The first split-state implementation and its counterexamples remain in
[ADR-0092](ADR-0092-split-safety-state-machines.md).

## Stories

- As a developer, I want a reproducible counterexample before changing a safety
  invariant.
- As a user, I want every reliability claim linked to a failure test.
- As a system, I want crashes injected at every persistence and acknowledgement
  boundary.

## Acceptance criteria

- As a developer, I can reproduce every accepted or rejected safety claim from a
  documented test command.
- As a user, I can see the untested limits of every prototype.
- As a system, I got equivalent platform-managed output for all enumerated crash
  points under the declared client-determinism assumption.
- As a system, I can reject conflicts rather than silently deduplicate divergent
  retries.

## Known gaps

The Sprint 2 subprocess matrix executes 21 abrupt `os._exit` cases: data append,
barrier append, join alignment/snapshot, post-barrier buffer, expired-lease
takeover, join restore, and combined checkpoint decision at `before`,
`after_durable_before_ack`, and `after_ack`. Fresh channel, join, and coordinator
instances verify each store; the coordinator case also runs the independent
oracle. The matrix does not simulate host/storage loss, network partitions,
concurrent alignment, real external participants, partition redistribution,
watermarks, or load. These gaps prevent `accepted` status.

The bounded [BarrierAlignment model](../models/BarrierAlignment.tla) complements
the executable tests. TLC 1.8.0 generates 9,471 states and finds 2,132 distinct
safe states through depth 17 for two inputs, checkpoints, epochs, and bounded
positions. The mixed-checkpoint mutation violates `SameCheckpointCut` at depth
4; the missing-fence mutation violates `FencingSafety` at depth 2. The
[verification runner](../models/verify_tla.py) asserts all three outcomes. This
is safety evidence within the stated bounds, not liveness or production
evidence.

## Sprint 3 failure matrix

Sprint 3A extends the matrix with partial prepare followed by abort, lost abort
acknowledgement, takeover during prepare/abort, an unavailable sink after
`COMMIT`, repeated recovery, stale participant fencing, and a forged raw SQL
decision. Every positive case must be checked after store reopen; the forged
decision and stale fence remain expected-failure controls. Sprint 3B adds source
acceptance/replay coverage, tier `I`/`B` behavior, partitions, and the multi-sink
TLA+ missing-decision/missing-fence mutations.

The Sprint 3A implementation passes 92 standard-library tests. Its named
recovery-attempt failure points cover crash after durable attempt start and crash
after a successful or failed callback but before the result write for sink
commit, source ACK, and abort. Hostile review also exercised unavailable-first
participant traversal, same-owner races, coordinator takeover, late diagnostic
results, and source/sink identity collision. ETL/HighLoad Principal review
approved the 49% prototype gate on 2026-10-20. This does not change the ADR to
`accepted`; ST-033…035, real adapters, production storage, and load evidence are
still open.

## Sprint 3B1 source evidence

The source lane adds two independent executable layers:

- [source_v1.py](../prototypes/source_v1.py) implements atomic acceptance and
  channel handoff, partition fencing, immutable cuts, verified combined-decision
  binding, idempotent ACK retry, and replay planning;
- [source_oracle.py](../prototypes/source_oracle.py) opens the ledger read-only
  and independently reconstructs accepted, committed, externally acknowledged,
  locally acknowledged, replay-start, and blocker observations.

The subprocess matrix in
[test_source_subprocess_recovery.py](../prototypes/test_source_subprocess_recovery.py)
kills a fresh Python process with `os._exit(17)` around acceptance and source ACK.
It covers an accepted-record INSERT before transaction commit, durable acceptance
before caller acknowledgement, durable ACK intent before the external call,
external ACK before the local result, and the local result before caller
acknowledgement. The unknown-result case persists the external ACK, loses the
local result, performs a fenced takeover and fresh reopen, then confirms the same
effect under the higher epoch and advances local progress.

The matrix now creates its decision with the real integrated `Coordinator`; the
hand-built coordinator-shaped fixture was removed. The oracle independently
matches every local decision to that global authority and checks every external
ACK against a known partition, accepted `(ordinal, position)`, and committed cut.
Negative controls include unknown partitions, a ghost position at ordinal 999,
a missing coordinator authority, and a deliberately corrupted record ID after a
test removes the storage guard. They also cover forged terminal ACK outcomes,
stale external epochs, malformed payload/decision JSON, decision-digest drift,
and a repeated cut authorized by checkpoints before and after takeover. Combined
with deterministic counterexamples for
forged decisions, mixed vectors, post-COMMIT mutation, late partition addition,
and stale callbacks, the 53% gate suite had 110 passing tests. ST-033c/d passed
ETL/HighLoad Principal hostile review on 2026-10-21, claiming the cumulative 53%
prototype gate. Production durability is not inferred from SQLite evidence.

## Protocol epoch 2 R3-B evidence

[ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md) and
[ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md) pass the
[final frozen-source PostgreSQL report](../evidence/postgres-r3b-final2-2026-10-25.json):
93/93 tests in 336.425 seconds, 39 matching source hashes, no source drift,
PostgreSQL 17.11 `remote_apply` and a synchronous streaming replica.

The added abrupt-process campaigns cover twelve join admission/terminal/
alignment/replay boundaries, four regional replay/READY boundaries, four
checkpoint/log retirement boundaries and three multi-transaction stream-GC
boundaries. The GC PLANNED case is an exception control and is not counted as a
real process loss. Fresh handles converge after the named losses. Independent
oracles verify durable stream, join, checkpoint and archive authority.

One hundred integrated source→join→checkpoint→regional-recovery→retirement
cycles include the regional graph, cuts, plans, worker generations, owner
attempts and rolling archive heads. All table row counts are identical at
cycles 25/50/75/100: 64 rows in total, with 30,322 / 30,340 / 30,346 / 30,452 JSON
bytes under the declared 512-row/256-KiB profile. This does not measure heap,
index, WAL or vacuum behavior. The separate bounded
[TLA+ report](../evidence/barrier-tla-r3b-2026-10-25.json) proves its alignment/
fencing abstraction only, with two unsafe controls; it excludes retirement and
regional liveness.

The [Principal PASS](../evidence/postgres-r3b-principal-2026-10-25.json) rechecks
the complete report and source hashes and independently reruns four key
controls: metadata-budget rollback, forged prior archive totals, corrupt
internal replay output refusal and successful expanded-region execution.
Unsupported missing-byte regeneration is rejected in
[ADR-0093](ADR-0093-implicit-regeneration-of-missing-durable-output.md).

Earlier candidate and interrupted reports remain under `evidence/`. A passing
candidate with open review findings does not award readiness. The final
Principal PASS closes DS-031a…d and earns 75%; the technical ADRs remain
`progress`, with production integration and the actual 72-hour soak still open.

## Protocol epoch 2 R4-A evidence

[ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md) links the
[110-test frozen report](../evidence/postgres-r4a-candidate2-2026-10-25.json)
and [Principal PASS](../evidence/postgres-r4a-principal-2026-10-25.json).
All 110 PostgreSQL tests pass in 444.602 seconds, with 92 matching Python source
hashes and no drift. Actual HTTP operations cover disabled/expired/foreign
capabilities, leadership takeover, exact graph registration, admission and
publication races, drain/cancel, and server loss. A 100-cycle native graph and
HTTP composition retains 62 logical rows at each measured checkpoint and at
most 25,303 JSON bytes in the declared profile. Counts exclude physical heap,
indexes, WAL and vacuum. The Principal independently reran five hostile cases.
The gate earns **83%**. Prefect integration, upgrades, infrastructure chaos and
the actual 72-hour soak remain open. Historical reports retain their original
source hashes when a later, separately tested revision changes a source.


## R4-B gate — 90% earned

[ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md), the
[139-test report](../evidence/postgres-r4b-candidate3-2026-10-25.json) and
[Principal PASS](../evidence/postgres-r4b-principal-2026-10-25.json) close
DS-041a…d and W34–W36. All 108 Python/HTML source hashes match, with no drift;
593.842 seconds of full-suite evidence and seven independently rerun hostile
cases pass on PostgreSQL 17.11 remote_apply with a synchronous replica. Earned
readiness is **90%**. Earlier interrupted/failed candidates remain recorded in
ADR-0025. Production acceptance stays open; R5-A compatibility/migration/backup
work proceeds under [ADR-0026](ADR-0026-release-compatibility-and-disaster-recovery.md),
followed by credential isolation, capacity evidence and the actual 72-hour soak.
