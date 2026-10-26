# ADR-0012: Protocol epoch 2 delivery gates

- Status: `planning`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, delivery owners
- Depends on: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery views: [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

Prototype correctness can create a misleading readiness percentage if storage,
runtime, orchestration, Prefect integration, load, and operations are not gated
separately. Protocol epoch 2 also has an early blocking decision: whether an
operator's state is durable per turn or restored from a barrier and replayed.

## Theory

Short evidence-gated sprints with cumulative problem coverage make progress
reviewable without treating unit-test volume as production readiness. High-risk
state, storage, and failure semantics must precede integration work.

## Agreement

Plan fourteen evidence gates, implemented through forty-two one-week sprints,
with cumulative gate targets:

```text
0 → 3 → 6 → 8 → 10 → 18 → 30 → 42 → 55
  → 64 → 75 → 83 → 90 → 95 → 100%
```

Each weekly sprint has a planned contribution to the target problem. That share
becomes earned readiness only when its enclosing evidence gate passes; a failed
gate leaves earned readiness at the previous gate. Work may run in parallel, but
cannot bypass the critical path. With fewer than three parallel owners the
schedule extends without compressing dependent tasks.

The critical path is:

```text
process crashes
→ state/replay decision and barrier composition
→ branches, retention, backpressure
→ generator SDK and R0 hostile gate
→ production storage and load
→ Python runtime and worker replacement
→ coordinator barriers and DAG recovery
→ orchestration server and Prefect integration
→ upgrade, chaos, soak, release gate
```

Protocol epoch 2 retains the one-terminal-`yield|return`-per-input restriction.
A non-blocking DS-009 spike records the next-epoch multi-output theory; its
implementation does not enter the current 100% scope.

## Stories

- As a developer, I want each sprint to end with executable evidence and an
  explicit dependency on earlier contracts.
- As a user, I want readiness to reflect production recovery behavior rather
  than prototype test count.
- As a system, I want state, storage, orchestration, and upgrade gates completed
  before a 100% claim.

## Acceptance criteria

- As a developer, I can trace every active task to an ADR and sprint gate.
- As a user, I can see the cumulative target and unearned blockers for every
  sprint.
- As a system, I got no earned percentage increment from a partially completed
  evidence gate, even though weekly planned contribution remains visible.
- As an operator, I got measured capacity, chaos, upgrade, and at least 72-hour
  soak evidence before release acceptance.

## Planning assumptions and blockers

- DS-004S must choose and test partition-local per-turn state versus
  checkpoint-managed restore and replay.
- Production storage topology and atomicity/HA assumptions must be explicit.
- Load SLO, retention horizon, catch-up target, and overload policy must be
  agreed before the 30% gate.
- Graph and deployment identities must survive Prefect upgrades.
- Cancellation and drain semantics must be fixed before integration.
- The intended Prefect fork/server extension points must remain available.

## Evidence required for `accepted`

- Every gate in [sprint_board.md](../sprint_board.md) has linked production or
  acceptance evidence.
- ETL/HighLoad Principal signs the R0, storage/load, runtime, DAG, integration,
  and final release gates.
- The final evidence graph has no dangling task or ADR links.

## Planning review evidence

ETL/HighLoad Principal review passed on 2026-10-21. The review confirmed 42
contiguous one-week sprints, a 100% total planned share, exact cumulative
targets, no hidden intra-week dependency chains, and a closed critical path.
In particular, DS-020a/b cannot bypass the DS-011d load/SLO gate. This PASS
validated the plan only; earned readiness remained 0% at that review. R0-A
subsequently passed and raised earned readiness to 3%. R0-B then passed its
state/barrier decision, composed model, 34-case process-loss matrix, independent
oracle, and Principal review, raising current earned readiness to 6%.
W6 passed independent branch recovery plus end-to-end DATA and BARRIER
authority through the aligned join. R0-C subsequently passed reference-safe
retention, bounded live-data pressure, exact tombstone retry, late-root bounds,
and hostile Principal review, raising current earned readiness to 8%.
R0-D then passed real sync/async generator execution, durable invocation
fencing, cancellation/timeout, sixteen process-loss boundaries, independent
oracle controls, and Principal hostile review. R1-A and R1-B then selected the
PostgreSQL `remote_apply` authority, proved HA fencing, bounded data and metadata
retention, and passed two consecutive frozen-hash 60-second load trials. R2-A
and R2-B ran sync/async generators on that authority, proved terminal atomicity,
process-loss recovery, state migration, and repeated replacement. The
ETL/HighLoad Principal independently reran 22 backend and seven runtime tests,
audited load trials 29/30 plus HA release 3, and recorded PASS with no blockers
on 2026-10-24. That gate earned 55%. The technical ADRs remain `progress`;
orchestration server, Prefect integration, soak, and release acceptance remain
in R3-B–R5.

The R3-A gate is recorded in
[ADR-0021](ADR-0021-postgresql-coordinator-checkpoints.md): ten focused tests,
fourteen real-process coordinator crashes, a separate invariant oracle, and the
full 39-test PostgreSQL regression suite pass on the synchronous `remote_apply`
profile. The active-lease takeover control passed 20/20 repetitions after its
expiry and rejection phases were separated. ETL/HighLoad Principal rechecked
all seven hostile findings, the final hashes, and the R3-B scope boundary, then
recorded PASS on 2026-10-25. W25–W27 are complete and earned readiness is 64%.

The R3-B gate subsequently passed on 2026-10-25, as recorded in
[ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md),
[ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md), the
[93-test frozen report](../evidence/postgres-r3b-final2-2026-10-25.json) and the
[Principal PASS](../evidence/postgres-r3b-principal-2026-10-25.json). Thirty-nine
source hashes match and source drift is empty. The integrated 100-cycle census
includes regional generations and all retirement families; it retains exactly
64 logical rows and at most 30,452 JSON bytes in its declared profile. Recovery
executes policy-driven expansion within the verified retained horizon and
refuses invalid physical authority or unsupported source rewind atomically.
All DS-031a…d criteria pass, completing W28–W30 and earning **75%**. Server,
Prefect integration, authorization, upgrades, infrastructure chaos and the
actual 72-hour soak remain R4–R5. The technical ADRs remain `progress` until
their broader production acceptance evidence exists.

R4-A subsequently passed all DS-040a…d criteria, as recorded in
[ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md), the
[110-test frozen report](../evidence/postgres-r4a-candidate2-2026-10-25.json) and
the [Principal PASS](../evidence/postgres-r4a-principal-2026-10-25.json).
The report freezes 92 Python sources without drift and passes in 444.602 seconds
on PostgreSQL 17.11 with `remote_apply` and a synchronous streaming replica.
Tenant isolation, exact native activation, lifecycle fencing, atomic admission
and publication, bounded control history, and HTTP restart pass. W31–W33 are
complete and earned readiness is **83%**. R4-B's real Prefect adapter and
operational view are in progress; passing isolated cases does not yet earn 90%.


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

## User amendment: short acceptance now

[ADR-0027](ADR-0027-short-acceptance-and-separate-long-soak.md) records the explicit user decision on 2026-10-25: short acceptance completes the current delivery; the actual 72-hour soak moves to separate nonblocking DS-052a. Earlier references to 72 hours as a blocking final condition are superseded by this decision. Full short-profile regression, fault, resource and credential-boundary checks remain required. No result is counted as a long soak without its actual elapsed duration.

## R5-A gate — 95% earned

The [159-test candidate](../evidence/postgres-r5a-candidate-2026-10-26.json) and
[architecture review](../evidence/postgres-r5a-architecture-review-2026-10-26.json)
close DS-050a…d and W37–W39 for the selected release/session, atomic migration,
signed schema-backup restore and tested runbook scope. Source drift is empty;
PostgreSQL 17.11 remote_apply/sync and documentation checks pass. The result is
95% of the agreed experimental delivery, not whole-cluster disaster recovery,
zero RPO or general production readiness. R5-B still requires its successor
complete regression and short capacity evidence under
[ADR-0029](ADR-0029-short-capacity-calibration-and-release-scope.md).

## R5-B gate — short experimental delivery complete

The [final complete gate](../evidence/postgres-r5b-final2-2026-10-26.json) passes
164/164 tests in 931.889 seconds with all 126 tested source hashes unchanged,
synchronous remote_apply/sync, documentation checks and no unclosed-socket
warnings. The [independent controls](../evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
pass 7/7 hostile checks. DS-051a…d and W40–W42 are complete for the agreed
short experimental scope, earning 100% of that delivery. ADR-0026, ADR-0027
and ADR-0029 are accepted only within their declared scope. Broader production
criteria and the separate DS-052a campaign remain open. No 72-hour PASS or
general production-readiness claim is made. Earlier failed/interrupted
candidates remain evidence; no source change followed this final gate.
