# Streaming ETL sprint board

Default cadence is one week with parallel owners. A task sized `L` consumes one
owner for most of the sprint. If staffing cannot satisfy the exit criteria, work
rolls forward and the cumulative target is not claimed.

The percentage measures cumulative readiness toward protocol epoch 2: yielded
ObjectRef records flow immediately through a platform-owned durable channel,
replacement workers redeliver the first input without a durable downstream
`yield`/`return`, and periodic barriers preserve graph state. Client code owns
determinism and external-effect idempotency.

Current earned readiness: **100% of the agreed short experimental delivery**. The earlier 53% measured a different protocol
that coupled source acknowledgement, global checkpoint decisions, and external
sink outcomes. Its evidence is retained below but is not counted toward the new
target.

## Protocol epoch 2 board

| Week | State | Planned target | Share | Tasks | Weekly exit |
| --- | --- | ---: | ---: | --- | --- |
| W1 | `done` | 1% | +1% | DS-003a | 22 named real-process crash cases pass without sleeps; Principal PASS recorded. |
| W2 / R0-A gate | `done` | 3% | +2% | DS-003b, DS-003c | Independent oracle and fresh recovery prove document-200 redelivery and all-or-none progress; Principal PASS recorded. |
| W3 | `done` | 4% | +1% | DS-004S | State classes and recovery rule are selected by executable spike and Principal PASS. |
| W4 | `done` | 5% | +1% | DS-004a | Ordered DATA/BARRIER composition, exact runtime replay, unsafe SQL controls, and Principal PASS complete. |
| W5 / R0-B gate | `done` | 6% | +1% | DS-004b | The 34-case real-process matrix, fresh-process convergence, and Principal PASS close R0-B. |
| W6 | `done` | 7% | +1% | DS-005a, DS-005b | Independent branch cursors, bound DATA/BARRIER witnesses, durable aligned-join buffer, and Principal PASS complete. |
| W7 | `done` | 7.5% | +0.5% | DS-005c | Retention roots, full tombstones, lifecycle authority, late-root bounds, and independent oracle pass. |
| W8 / R0-C gate | `done` | 8% | +0.5% | DS-005d | Atomic record/byte credits and block/compact/resume passed Principal review. |
| W9 | `done` | 9% | +1% | DS-006a | Sync generator executes the durable completion contract. |
| W10 | `done` | 9.5% | +0.5% | DS-006b | Async cancellation/timeout and cross-runtime exclusion pass. |
| W11 | `done` | 9.75% | +0.25% | DS-006c | Durable invocation fencing and illegal protocol controls pass. |
| W12 / R0-D gate | `done` | 10% | +0.25% | DS-007 | Principal hostile review passed all R0 evidence. |
| W13 | `done` | 11% | +1% | DS-010a | Production backend and HA assumptions are selected. |
| W14 | `done` | 15% | +4% | DS-010b | Ordered durable append/progress works on the actual backend. |
| W15 / R1-A gate | `done` | 18% | +3% | DS-010c, DS-010d | Fencing and ObjectRef visibility survive races and remount; Principal PASS recorded. |
| W16 | `done` | 21% | +3% | DS-011a, DS-011b | Compaction references and capacity budgets are enforced. |
| W17 | `done` | 24% | +3% | DS-011c | Recovery horizon and catch-up capacity model is reviewable. |
| W18 / R1-B gate | `done` | 30% | +6% | DS-011d | Agreed load SLO passes benchmark, failover, stalled consumer, and Principal review. |
| W19 | `done` | 35% | +5% | DS-020a, DS-020b | Sync and async runtimes execute on production storage. |
| W20 | `done` | 39% | +4% | DS-020c | Runtime atomically connects terminal output/state/progress. |
| W21 / R2-A gate | `done` | 42% | +3% | DS-020d | Exception, cancellation, timeout, and process-loss semantics pass Principal review. |
| W22 | `done` | 47% | +5% | DS-021a, DS-021b | Lease takeover and per-turn managed state compose. |
| W23 | `done` | 50% | +3% | DS-021c | State version migration/rejection is deterministic. |
| W24 / R2-B gate | `done` | 55% | +5% | DS-021d | Repeated Anna/Tanya replacement matrix and Principal review pass. |
| W25 | `done` | 57% | +2% | DS-030a | PostgreSQL coordinator injects BARRIER through the same partition order as DATA and compaction. |
| W26 | `done` | 61% | +4% | DS-030b | Immutable manifests, participant snapshots, create-only completion, and frozen recovery selection pass the independent oracle. |
| W27 / R3-A gate | `done` | 64% | +3% | DS-030c | Fourteen coordinator crashes, 20/20 deterministic takeover repetitions, exact graph/state authority, and Principal PASS close R3-A. |
| W28 | `done` | 68% | +4% | DS-031a | Routed alignment, durable mailbox/terminal turns, abort release, twelve real process-loss boundaries and Principal PASS complete. |
| W29 | `done` | 71% | +3% | DS-031b | B/J/D regional fencing/replay/activation leaves C live; owner takeover, four recovery-process crashes and Principal PASS complete. |
| W30 / R3-B gate | `done` | 75% | +4% | DS-031c, DS-031d | Retained-horizon expansion/refusal, four retirement layers, full 100-cycle census and live catch-up pass 93/93 frozen-source tests and independent Principal review. |
| W31 | `done` | 78% | +3% | DS-040a | Exact authenticated HTTP projections and independent authority oracle passed [ADR-0024](adr/ADR-0024-control-plane-authority-and-lifecycle.md)'s gate. |
| W32 | `done` | 80% | +2% | DS-040b, DS-040d | Serialized leadership fencing, revocable capabilities and exclusive tenant partition ownership pass Principal review. |
| W33 / R4-A gate | `done` | 83% | +3% | DS-040c | Full native graph/bootstrap validation, atomic admission/publication, cancel/drain and restart pass 110/110 tests and independent Principal review. |
| W34 | `done` | 86% | +3% | DS-041a | Actual feature-flagged Prefect SDK/API streaming and artifact identity pass. |
| W35 | `done` | 88% | +2% | DS-041b, DS-041c | Fenced resume, maintenance recovery and correlated telemetry pass. |
| W36 / R4-B gate | `done` | 90% | +2% | DS-041d | 139/139 frozen tests, real HTTP/Chromium runbook drill and Principal PASS close R4-B. |
| W37 | `done` | 91.5% | +1.5% | DS-050a | Durable release/session fencing and mixed-version acceptance in [ADR-0026](adr/ADR-0026-release-compatibility-and-disaster-recovery.md). |
| W38 | `done` | 93.5% | +2% | DS-050b, DS-050c | Migration and infrastructure chaos campaign pass. |
| W39 / R5-A gate | `done` | 95% | +1.5% | DS-050d | Backup/restore, drain, and recovery runbook pass. |
| W40 | `done` | 97% | +2% | DS-051a | Short declared acceptance profile and fault/resource checks pass; DS-052a tracks the separate 72-hour follow-up. |
| W41 | `done` | 98% | +1% | DS-051b, DS-051c | Capacity predictions and credential boundary are verified. |
| W42 / R5-B gate | `done` | 100% | +2% | DS-051d | Final Principal review closes the evidence graph. |

Every row is a one-week sprint assuming at least three parallel owners. The
planned target shows contribution to the target problem; earned readiness moves
only at rows marked `gate`. With fewer owners the dates move and dependent tasks
are never compressed. R5-A has earned 95% for the declared schema-recovery
scope. R5-B has now passed the successor complete short gate and closes the
selected experimental delivery at 100%.
These percentages measure the agreed experimental delivery, not general
production readiness or elapsed calendar weeks.

R5-B evidence: [164-test final report](evidence/postgres-r5b-final2-2026-10-26.json)
and [7 independent controls](evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
close W40–W42. All 126 tested Python/HTML hashes match, source drift is empty,
the remote_apply/sync profile passes, and no unclosed-socket warning occurs.
The actual capacity scenario recovers under live ingress and passes six
independent oracles. [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md)
records the exact limits and retained failed trials.

The [final architecture review](evidence/postgres-r5b-architecture-review-2026-10-26.json)
records `PASS_SHORT_EXPERIMENTAL_100`; the
[final graph validation](evidence/adr-graph-final-2026-10-26.json) closes the
documentation/status check. ADR-0026, ADR-0027 and ADR-0029 are accepted within
their declared scopes; broader production criteria remain open.

R5-A evidence: [159-test report](evidence/postgres-r5a-candidate-2026-10-26.json)
and [architecture review](evidence/postgres-r5a-architecture-review-2026-10-26.json)
close W37–W39 at 95%. The report passes in 736.183 seconds without source
drift on PostgreSQL 17.11 remote_apply/sync; the
[recovery runbook](recovery_runbook.md) records the exact selected procedure
and limits. The successor short capacity and integration changes are covered
by the final R5-B gate above.

R4-B evidence: [139-test report](evidence/postgres-r4b-candidate3-2026-10-25.json)
and [Principal PASS](evidence/postgres-r4b-principal-2026-10-25.json). All 108
Python/HTML hashes match with no drift, synchronous `remote_apply` is intact,
and seven hostile controls passed independent rerun. Earlier interrupted and
failed candidates remain linked in [ADR-0025](adr/ADR-0025-prefect-adapter-and-operational-projections.md).
DS-009 runs in a separate no-percentage research lane. Detailed task acceptance
is in [todo.md](todo.md), governed by
[ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md).

DS-052a is a separate `progress` lane with no percentage: configure the continuous
72-hour campaign on an existing self-hosted runner under
[ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md). Its
[CI status and run instructions](soak_ci.md) distinguish local checks, published
workflows, GitHub smoke evidence and actual long-duration acceptance.

CI follow-up on 2026-10-27: repository transfer to `Anacreonsystems` is verified.
The [static-analysis repair](evidence/ci-static-analysis-repair-2026-10-27.json)
passed hosted pre-commit and type checks. The replacement streaming smoke still
awaits an ARC host; DS-052a remains `progress` with no additional percentage.
The source-hygiene successor gate separately records every failed and passing
trial without rewriting historical acceptance evidence.

The [serial source-hygiene successor](evidence/postgres-ci-style-regression-serial-2026-10-27.json)
passes 164/164 on identical source bytes after the retained first-trial lease
expiry. The selected short-delivery percentage remains 100%; hosted runner and
72-hour acceptance remain separate open criteria.

[Principal acceptance](evidence/ci-source-hygiene-principal-review-2026-10-27.json)
confirms the repair with no blockers and retains the current scoped percentage.

Later ARC acceptance on 2026-10-27: the
[hosted smoke](evidence/ci-arc-smoke-observed-40578404646.json) and
[Principal review](evidence/ci-arc-smoke-principal-review-2026-10-27.json)
pass after the shared-directory and short-resource-policy repairs. Existing
runner access and the one-job smoke are verified. DS-052a remains `progress`
for the unstarted static-runner 72-hour campaign; no percentage is added.

Post-merge dispatch on 2026-10-27: the
[long-profile run](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
is queued on the merged `main` source. Its
[dispatch observation](evidence/ci-long-soak-dispatch-40601620025.json)
does not yet verify runner assignment or workload startup. DS-052a remains
`progress` with no percentage added.

2026-10-28 retrigger: the [unassigned static campaign was cancelled](evidence/ci-long-soak-cancelled-40601620025.json)
at the user's retrigger request; the
[fresh ARC smoke](evidence/ci-arc-smoke-observed-40650444560.json) passes on
merged `main`. DS-052a remains `progress` without additional percentage;
actual 72-hour execution and any ARC long-profile validation remain open.

R1/R2 gate evidence is recorded in
[ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md),
[ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md), and
[ADR-0020](adr/ADR-0020-bounded-metadata-and-idempotency-horizon.md). The final
candidate passed 22/22 backend and 7/7 runtime tests. Two consecutive frozen-hash
load runs passed the agreed 510 records/s workload, latency, catch-up,
compaction, capacity, and independent-oracle checks; release-3 HA preserved all
acknowledged records and recovered a contiguous 1…200 suffix. The ETL/HighLoad
Principal independently reran the suites and recorded PASS with no blockers on
2026-10-24. As a developer, I can run sync and async streaming tasks on the
selected authority; as a user, I got first-incomplete redelivery through
replacement; as a system, I got bounded durable progress and stale-owner
fencing under the measured profile.

R3-A evidence is recorded in
[ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md) and the
[frozen report](evidence/postgres-barrier-r3a-2026-10-25.json). The gate passes
10/10 focused tests, 14 abrupt coordinator boundaries, 20/20 isolated takeover
repetitions, and the 39/39 PostgreSQL regression suite. ETL/HighLoad Principal
closed seven hostile findings and recorded PASS on 2026-10-25.

R3-B evidence is recorded in
[ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md),
[ADR-0023](adr/ADR-0023-bounded-retention-and-replay-horizons.md), the
[final frozen report](evidence/postgres-r3b-final2-2026-10-25.json), and the
[Principal PASS](evidence/postgres-r3b-principal-2026-10-25.json). All 93 tests
pass in 336.425 seconds with 39 matching source hashes and no drift. Four
independent targeted review cases also pass. One hundred integrated
data/checkpoint/regional-recovery/retirement cycles retain exactly 64 logical
rows and at most 30,452 JSON bytes under the declared 512-row/256-KiB profile.
Source rewind and reconstruction of absent durable bytes are unsupported;
invalid physical authority receives a named atomic refusal. As a user, I got
healthy-sibling continuity and first-incomplete continuation through regional
replacement; as a system, I got bounded retained history with exact recovery
authority. This earns 75% and does not constitute production acceptance.

## Historical protocol epoch 1 portfolio

| Sprint | State | Target | Increment | Tasks | Exit decision |
| --- | --- | ---: | ---: | --- | --- |
| 0 — Architecture foundation | `done` | 15% | +15% | ST-001…007 | ADR graph, prototypes, counterexamples, and Principal PASS exist. |
| 1A — Atomic progress foundation | `done` | 20% | +5% | ST-010, ST-011, ST-014 foundation | Source ACK cannot advance beyond one immutable combined checkpoint/sink decision; Principal PASS recorded. |
| 1B — Safety contract freeze | `done` | 27% | +7% | ST-010…014 completion | Identities, state machines, participant contracts, failure vocabulary, and oracle passed Principal review. |
| 2 — Barriers and fencing | `done` | 43% | +16% | ST-020…025 | Actual barriers survive process loss; recovery regions are explainable; stale owners are fenced. |
| 3A — Cooperative sink safety | `done` | 49% | +6% | ST-030, ST-031, ST-032 slice | Versioned cooperative sinks abort before and converge after an authoritative decision; Principal PASS recorded. |
| 3B1 — Source acceptance and replay | `done` | 53% | +4% | ST-033a…d | Source acknowledgement is tied to exact durable input coverage and replay cuts; Principal PASS recorded. |
| 3B2 — Tiers and formal decision model | `progress` | 58% | +5% | ST-034…035 | Tier behavior is explicit and the end-to-end decision has bounded formal evidence. |
| 4 — Storage and high load | `blocked` | 72% | +14% | ST-040…045 | Backend and operating envelope are selected from crash/load evidence. |
| 5 — Experimental integration | `blocked` | 88% | +16% | ST-050…055 | Production-shaped experimental runtime and server control state pass contract tests. |
| 6 — External acceptance | `blocked` | 100% | +12% | ST-060…065 | Real adapters, crash/soak/upgrade evidence, runbook, and ADR status review pass. |

## Sprint 1A — Atomic progress foundation — `done`

Goal: establish the minimum end-to-end invariant before freezing all participant
contracts.

- **Identity lane:** canonical graph, checkpoint, generation, and record lineage.
- **Decision lane:** one immutable record binds checkpoint state, sink decision,
  and source position.
- **Evidence lane:** independent oracle and unsafe split-write control.

Exit criteria:

- As a developer, I can derive identical checkpoint and transaction identities
  after restart and enumerate their durable transitions.
- As a user, I got no source position reported as acknowledged while its required
  sink outcome is undecided.
- As a system, I can prove in the modeled crash points that acknowledged progress
  never exceeds the highest immutable combined decision.

Evidence: 50 standard-library prototype tests pass. The integrated suite runs the
actual protocol through 27 boundary/offset crash points plus a dedicated 27-case
legal phase-edge matrix, store reopen, external-effect retry,
recovery, and an independent oracle. It also checks exhaustive transition rules,
CAS conflicts and epoch takeover, capability tiers, opaque source-position
vectors, restorable-artifact manifests, canonical external transaction IDs,
owner-aware artifact identity, a global stable-record payload registry, foreign
keys and schema triggers that preserve decision/ACK ordering and immutable
intent, complete expected sink effects, fencing, and unsafe controls. The 20%
target passed ETL/HighLoad Principal review on 2026-10-20 and is now claimed.
This remains local prototype evidence rather than a production guarantee.

## Sprint 1B — Safety contract freeze — `done`

Goal: remove implicit safety assumptions before expanding the prototypes.

- **Protocol lane:** ST-010 stable identities → ST-011 checkpoint state machine.
- **Participant lane:** ST-010 → ST-012 versioned capability contracts.
- **Evidence lane:** ST-011 + ST-012 → ST-013 deterministic fault API;
  ST-010 + ST-011 → ST-014 safety oracle.

Exit criteria:

- As a developer, I can derive stable identities and implement a versioned
  participant contract.
- As a user, I got an explicit validation error for an unsupported guarantee.
- As a system, I can reject illegal state transitions and compare recovery with
  an independent oracle.

Implementation and tests exist for ST-010…014. ETL/HighLoad Principal review
passed on 2026-10-20, so the cumulative 27% target is claimed. Production claims
remain gated by later sprints.

## Sprint 2 — Barriers, recovery regions, and fencing — `done`

Goal: replace simulated snapshots with ordered barriers, real process loss, and
ownership protection.

- **Channel lane:** ST-020 barrier records → ST-021 aligned joins.
- **Recovery lane:** ST-020 → ST-022 recovery planner.
- **Ownership lane:** ST-023 generation leases and fencing.
- **Evidence lane:** ST-024 subprocess harness and ST-025 TLA+ model.

Exit criteria:

- As a system, I can checkpoint a join without mixing barrier sides.
- As a user, I can see the selected recovery region and its replay cuts.
- As a system, I got stale-writer exclusion in executable and bounded formal
  models.

Current evidence: `barrier_protocol.py` models ordered `DATA` and `BARRIER`
records, aligned joins with durable post-barrier buffers, immutable historical
replay identity, recovery cuts, trusted-clock leases, and generation fencing.
The full suite passes 66 tests. Its subprocess matrix executes 21 abrupt exits
across data/barrier append, partial alignment and snapshot, post-barrier buffer,
takeover, restore, and combined decision; fresh channel, join, and coordinator
instances verify recovery. The bounded TLA+ model generates 9,471 states and
finds 2,132 distinct safe states through depth 17; mixed-checkpoint and missing-
fence mutations produce the expected named counterexamples. ETL/HighLoad
Principal review passed on 2026-10-20, so the cumulative 43% target is claimed.
This remains prototype and bounded formal evidence rather than a production
guarantee.

## Sprint 3A — Cooperative sink safety — `done`

Goal: join accepted input, checkpoint completion, and external outcomes into one
recoverable protocol.

Hostile review of the Sprint 1 model found missing abort/recover contracts,
participant-side fencing, storage decision guards, and blocked-participant
diagnostics. Sprint 3A closes those gaps through the serialized path sink state
machine → decision constraint → recovery failure evidence. ETL/HighLoad
Principal review passed on 2026-10-20, so the cumulative 49% target is claimed.

- **Sink lane:** ST-030 protocol → ST-031 combined decision → ST-032 failures.
- **Source lane:** ST-020 + ST-031 → ST-033 acknowledgement/replay contract.
- **Conformance lane:** ST-034 tier suites.
- **Formal lane:** ST-035 multi-sink decision model.

Sprint 3B continues ST-032 and implements ST-033…035 after the 3A contracts
freeze. It remains blocked by the Sprint 3A gate.

Exit criteria:

- As a system, I can abort before `COMMIT_DECIDED` and only commit afterward.
- As a developer, I can run the durable cooperative-sink contract suite.
- As a user, I can see blocked participants, durable retry count, last error,
  owner, fence, and recovery outcome.

Evidence: the 92-test standard-library suite covers durable cooperative-sink
tokens and transaction fences, immutable manifests, authoritative COMMIT and
ABORT decisions, cleanup past an unavailable participant, lost acknowledgements,
owner takeover, durable attempt starts, terminal-state reconciliation, and
rejection of cross-role identity collisions. Recovery diagnostics expose owner,
fence, attempt count, outcome, blocked state, and last error. This is prototype
evidence; production acceptance and ST-033…035 remain open.

## Sprint 3B — Source, tiers, and formal model — `progress`

Goal: bind acknowledged input to durable channel coverage, define weaker sink
tier behavior, and model the irreversible multi-sink decision.

- **Source lane:** ST-033 acceptance, durable handoff, replay, and ACK contract.
- **Conformance lane:** ST-034 behavioral suites for tiers `T`, `I`, and `B`.
- **Formal lane:** ST-035 safe multi-sink model plus missing-decision and
  missing-fence controls.

The cumulative 58% target remains unclaimed until all three lanes pass hostile
review.

The first gate is **Sprint 3B1 / 53%**: ST-033a contract and cuts → ST-033b
atomic acceptance/handoff → ST-033c ACK and replay recovery → ST-033d crash and
oracle evidence. ST-034 and ST-035 remain in Sprint 3B2 for the last 5%.

Sprint 3B1 passed on 2026-10-21. Sprint 3B2 now splits into **3B2A / 55%** for
ST-034 tier conformance and **3B2B / 58%** for the tier `T` formal decision
model. ST-034 runs 034a contract → 034b/034c/034d tier evidence → 034e mixed-tier
oracle. ST-035 starts only after the tier `T` model boundary and invariant names
are frozen.

ST-034a passed Principal review on 2026-10-21. The immutable tier intent,
`BPolicy`, effect-authority field, and observation vocabulary are frozen for the
prototype. ST-034b…e remain open, so earned readiness stays at 53%.

Exit criteria:

- As a user, I got no acknowledged-input loss for enumerated source failures.
- As a user, I can see the enforced sink tier and partial-visibility semantics.
- As a system, I got bounded model evidence for one irreversible decision per
  immutable sink manifest and named counterexamples for missing guards.

## Sprint 4 — Durable storage, backpressure, and load — `blocked by Sprint 3`

Goal: select storage and capacity limits from measurements.

- **Transport lane:** ST-040 Go segmented log → ST-043 bounded credits.
- **State lane:** ST-041 snapshot formats.
- **Retention lane:** ST-042 reference-safe garbage collection.
- **Evidence lane:** ST-044 benchmarks → ST-045 storage/capacity decision.

Exit criteria:

- As a system, I can recover durable segments and never delete referenced data.
- As a system, I can bound memory and storage under slow-sink and catch-up load.
- As a user, I got measured throughput, latency, checkpoint, replay, and overload
  limits.

## Sprint 5 — Experimental production integration — `blocked by Sprint 4`

Goal: implement the validated protocol behind an experimental feature flag.

- **Runtime lane:** ST-050 sync/async `yield` runtime.
- **Coordinator lane:** ST-051 durable coordinator → ST-052 worker fencing.
- **Server lane:** ST-053 schemas and internal APIs.
- **Operations lane:** ST-054 observability.
- **Evidence lane:** ST-055 production contracts mapped to prototype and TLA+.

Exit criteria:

- As a developer, I can run the canonical branched `yield` DAG through Prefect.
- As a system, I can restart coordinator and workers without selecting incomplete
  state or accepting stale writes.
- As a user, I can observe lag, alignment, replay, recovery, and sink decisions.

## Sprint 6 — External acceptance and release gate — `blocked by Sprint 5`

Goal: establish production evidence and decide ADR statuses.

- **Adapters:** ST-060 replayable source and reference tier `T`/`I` sinks.
- **Correctness:** ST-061 branched-DAG crash matrix.
- **Load:** ST-062 soak, host loss, sink outage, and catch-up.
- **Lifecycle:** ST-063 drain, cancellation, upgrade, and state migration.
- **Operations:** ST-064 runbook and guarantee documentation.
- **Governance:** ST-065 acceptance index and ADR review.

Exit criteria:

- As a user, I got failure-free-equivalent logical output for every supported
  guarantee tier and documented capacity objective.
- As a system, I can safely reject or migrate incompatible recovery state.
- As a developer, I can trace every `accepted` criterion to production evidence.

## WIP and sequencing rules

- Maximum three active high-risk tasks across the portfolio.
- A sprint may begin preparatory work early, but its target percentage is earned
  only after all exit criteria pass.
- Production storage selection waits for ST-044 measurements.
- Source acknowledgement waits for ST-031 combined decision semantics.
- Production coordinator implementation waits for ST-025 and ST-035 formal
  evidence.
- ADR status changes occur in ST-065 and never follow prototype success alone.

Detailed dependencies, deliverables, and task-level acceptance criteria are in
[todo.md](todo.md).
