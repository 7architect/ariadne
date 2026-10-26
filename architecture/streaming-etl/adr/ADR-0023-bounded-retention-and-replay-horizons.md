# ADR-0023: Bounded retention and explicit replay horizons

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, storage/runtime maintainers
- Depends on: [ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md), [ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md), [ADR-0016](ADR-0016-reference-safe-retention-and-bounded-pressure.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-031c/d in [todo](../todo.md) and [sprint board](../sprint_board.md)

## Problem

Hard limits that stop a stream forever are safe but do not provide continuous
ETL operation. Completed checkpoints, per-input roots, ordered logs, mailbox
turns, invocation histories, recovery generations and ObjectRef identities all
retain different parts of the same evidence graph. Deleting one family alone
can remove a referenced replay suffix or leave metadata growing without bound.

An independent oracle also needs a new starting authority after prefix removal.
It cannot demand history from position 1 after the platform has explicitly
retired that history. Logical sequence identity must keep increasing even when
live metadata capacity is returned.

## Theory

Retirement is a transaction authorized by a frozen exact manifest and a
snapshot/rolling-summary anchor. Every retained consumer, checkpoint, active
invocation and recovery plan contributes a root. Only a prefix before every
required root can retire. A committed summary replaces the retired proof prefix;
it does not replace the live tail needed for replay.

## Agreement

The implementation is split into four dependent slices. Each slice retains
explicit unsupported behavior until its dependent storage families are covered.

1. **Join-local retirement.** Keep the latest completed compatible snapshot.
   Freeze exact receipts, turns and invocation history in one manifest; retire
   only a contiguous arrival prefix that is also a contiguous apply prefix.
   Advance each input root, persist the state anchor, and delete the authorized
   rows in one transaction. Active invocations and recovery plans block it.
2. **Coordinator and ordered-log archive.** Keep the newest completed checkpoint
   and all active recovery roots. Archive only an eligible terminal checkpoint
   prefix, retain bounded rolling heads/current operation, and reclaim the
   corresponding log prefix. `next_authority_ordinal` and `next_log_sequence`
   remain monotonic; live counters return capacity. Historical regional FKs
   cause an explicit refusal until slice 3 supports them.
3. **Regional archive.** Retire completed regional plans, generation and owner
   attempt history, and edge-cut roots under the same authority. Any restoring
   generation or active plan pins the required evidence. Regional compatibility
   rows in the global plan table must be classified by their regional authority,
   rather than treated as globally restoring forever.
4. **Stream metadata and objects.** Use structured output producer coordinates
   with the existing metadata horizon and ObjectRef GC. Stable logical output
   IDs remain independent of recovery generation. Reclaiming data and reclaiming
   proof metadata require separate exact cuts in the same reference graph.

No implementation uses `DISABLE TRIGGER` to bypass the protocol. Controlled
deletion defaults to rejection and requires exact frozen row witnesses in the
active retirement transaction. Deferred convergence guards require all roots,
anchors, deleted prefixes and operation status to agree at commit.

Opaque historical request IDs cannot have indefinite exact deduplication after
their detail is removed. Exact retry is guaranteed within the retained horizon;
older coordinates receive an explicit expired result where the caller supplies
an authoritative ordinal. A rolling digest alone is not a membership proof for
arbitrary old opaque IDs. This limitation must remain visible in internal APIs.

These limits concern live logical rows/bytes and admission credits. PostgreSQL
heap/WAL disk behavior still requires vacuum, capacity measurements and the
later soak gate; decrementing a counter is not evidence of reclaimed disk space.
The read-only [storage census](../prototypes/postgres_storage_footprint.py)
enumerates every ordinary table in the isolated schema and measures rows and
JSON representation bytes. This catches archival families omitted from a
hand-maintained pressure counter; it is not a physical-storage benchmark.

## Stories

- As a developer, I want to inspect the retained snapshot, mailbox/log horizons,
  exact retirement manifest and rolling summary that authorize deletion.
- As a user, I want a continuous stream to return capacity after safe retirement
  and recover from the newest retained snapshot plus the exact durable tail.
- As a system, I want active invocations and global/regional recovery roots to
  prevent removal of any required record, state or identity witness.
- As an operator, I want a named horizon-expired error and observable logical
  capacity instead of silent deduplication or an invented recovery position.

## Acceptance criteria

- As a system, I can prove exact row membership before controlled deletion and
  atomically publish the corresponding roots, state/log anchors and credits.
- As a system, I reject partial retirement on commit, forged manifests/counters,
  stale owners, active-plan deletion and divergent exact retries.
- As a user, I got recovery from the latest snapshot after at least 100 real
  data/checkpoint/retirement cycles while every claimed live metadata family
  remains within its declared bound.
- As a developer, I can reproduce independent oracle checks starting at the
  retained anchor and inspect explicit expired results below it.
- As an operator, I got returned record/byte/metadata/log credits without DATA
  loss, cursor regression or unaccounted archival growth.

## Current prototype and evidence

[postgres_join_retention.py](../prototypes/postgres_join_retention.py) implements
slice 1 with immutable manifest/row witnesses, rolling join anchors, controlled
deletion and deferred convergence. [test_postgres_join_retention.py](../prototypes/test_postgres_join_retention.py)
covers atomic failure rollback, exact retry, invocation/global/regional pins,
rejection of incomplete raw manifests, 100 real checkpoint cycles, latest
snapshot restoration, and skewed arrival/apply prefixes. On the skewed case,
snapshot high-water 4 permits retirement only through apply/arrival 2; the roots
become fast 3 and slow 1 so required receipts remain live.

The generator runtime emits structured producer coordinates, keeping its
stable logical output identity independent of the worker generation.

[postgres_checkpoint_retention.py](../prototypes/postgres_checkpoint_retention.py)
implements slice 2. A bounded current operation and rolling checkpoint,
authority, join-retirement and ordered-log heads replace the retired prefix.
Exact transient row witnesses disappear before commit. The
[independent checkpoint archive oracle](../prototypes/postgres_checkpoint_retention_oracle.py)
and [tests](../prototypes/test_postgres_checkpoint_retention.py) cover 100 cycles,
active recovery pins, another revision's partitions, retained leaf OUTPUT
barriers, raw-manifest rejection and four abrupt-process rollback boundaries.

[postgres_regional_retention.py](../prototypes/postgres_regional_retention.py)
implements slice 3. Only historical all-ACTIVE regional plans retire; generation,
owner-attempt, compatibility-plan and edge-cut history follow the same frozen
scope. Its [independent oracle](../prototypes/postgres_regional_retention_oracle.py)
and [tests](../prototypes/test_postgres_regional_retention.py) cover ten regional
generations, protected restoring workers, latest-root substitution rejection,
and a new recovery created after an older generation was archived. The latter
must remain valid: an archive oracle checks its retired scope rather than
mistaking every later RESTORING worker for leaked history. The focused suite
passed 3/3 tests on 2026-10-25.

Review found that a caller-supplied operation setting alone could be mistaken
for deletion authority. Witness admission now checks exact frozen plan/cut/root
membership; deferred guards require convergence and retained roots. Another
finding exposed global recovery ID reuse after opaque request history retired.
New recovery identities include the monotonic authority ordinal, while hot
request retries still return the existing operation.

These slices remain candidates until the frozen-source combined gate passes.
The join-local 100-cycle test alone cannot earn 75% or production acceptance.

[postgres_streaming_retention.py](../prototypes/postgres_streaming_retention.py)
composes slice 4 with the existing ObjectRef and structured-producer metadata
protocol. DATA compaction and metadata retirement remain separate commits. A
bounded frozen operation records PLANNED → DATA_COMPACTED → COMPLETED; retries
use its original checkpoint and preserve its recorded metrics after metadata
has disappeared. The terminal update checks the operation, status and current
coordinator owner. Storage guards reject illegal transitions and missing
outcome fields using null-safe comparisons.

All four retention writers assert the revision's metadata-byte budget before
commit. Join-local retirement also takes the revision lock before the worker
lock, matching the runtime's alignment order. A 7,000-byte test profile can
complete its checkpoint but cannot admit the larger retirement manifest; the
failed retirement rolls back rows, roots and summaries together and leaves all
three base oracles positive. Returning DATA credits does not bypass the budget
for the proof that authorizes retirement.

An unfinished stream-GC operation serializes checkpoint creation and checkpoint
prefix retirement. `CheckpointControlPressure` and the
`streaming_retention_pending` pressure dimension expose the reason. DATA append
and terminal task progress continue; retrying the old cut releases this control
pressure before the next checkpoint. Completed hot retries return their original
outcome even when a newer checkpoint exists.

[test_postgres_streaming_retention.py](../prototypes/test_postgres_streaming_retention.py)
uses bounded managed state (count and rolling digest), structured producers and
100 source→join→checkpoint→retirement cycles. Its declared profile permits at
most 512 logical rows and 256 KiB of JSON representation across every table;
row counts must be identical at cycles 25, 50, 75 and 100. The test then restores
the latest snapshot and checks an explicit expired producer retry. Three real
`os._exit(86)` cases cover DATA-compacted, metadata-compacted and completed
boundaries; PLANNED uses an exception control. Fresh handles converge to the
same cuts and metrics. A separate DAG case keeps A/C live during B/J/D recovery,
then drains the new document through the reactivated tail with stable state and
output identity.

Principal review rejected the earlier 100-cycle proof as incomplete because it
did not install the regional metadata families. The combined test now performs
regional snapshot replay and atomic activation on each cycle, archives the
previous regional generation before checkpoint/stream retirement, and includes
those tables in the whole-schema census. A separate negative control presents
genuine live witnesses with a forged prior archive total of 1,000,000. Archive
head admission now compares the frozen prior digest and all four prior counters
to the actual previous head, rather than trusting a self-consistent caller plan.

The [89-test candidate](../evidence/postgres-r3b-candidate2-2026-10-25.json)
passed on its recorded hashes but did not close these review findings. A later
[interrupted run](../evidence/postgres-r3b-final-2026-10-25.json) is retained with
its explicit stop reason. Neither report awards readiness.

## Gate result and remaining production work

The [final frozen-source report](../evidence/postgres-r3b-final2-2026-10-25.json)
passed 93/93 tests with no source drift. The combined 100-cycle profile includes
all four retirement layers and regional recovery on every cycle. Its census
measured 64 rows at each checkpoint and 30,322 / 30,340 / 30,346 / 30,452 JSON
bytes at cycles 25 / 50 / 75 / 100. DATA records, payload objects and tombstones
were zero after each eligible compaction; retained snapshots and rolling heads
continued to authorize recovery.

The [Principal gate review](../evidence/postgres-r3b-principal-2026-10-25.json)
verified all 39 source hashes and independently reran four hostile controls,
recording PASS on 2026-10-25. DS-031a…d and the R3-B gate close at **75%**.
The [bounded TLA+ report](../evidence/barrier-tla-r3b-2026-10-25.json) remains
separate alignment/fencing evidence; it does not model this retirement protocol.

This ADR remains `progress`: production/server integration, physical disk/WAL
capacity, infrastructure faults and the real 72-hour soak are later acceptance
work under [ADR-0012](ADR-0012-protocol-epoch-2-delivery-gates.md). External source
rewind and missing-byte reconstruction are not part of this completed scope.
