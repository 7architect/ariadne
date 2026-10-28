# Streaming ETL task registry

This registry slices the [ADR graph](adr/README.md) into delivery tasks. Task
completion does not make an ADR `accepted`; evidence for every criterion in
the declared ADR scope must be linked back to it.

Delivery states are separate from ADR statuses. Sizes assume `S` ≤ 2 engineer
days, `M` = 3–4 days, and `L` ≈ one engineer week. `H` and `M` denote high and
medium delivery risk.

## Protocol epoch 2 — short experimental delivery — target 100%, earned 100%

The previous source/checkpoint/sink contract is superseded. Historical tasks
remain below as evidence but do not count toward the current target.

| Workstream | State | Tasks | Exit |
| --- | --- | --- | --- |
| Contract and focused model | `done` | DS-001, DS-002 | Durable-yield boundary and focused Principal PASS exist. |
| R0-A subprocess durability | `done` | DS-003a…c | Fresh-process crash oracle earned 3%. |
| R0-B state and barriers | `done` | DS-004S, DS-004a…b | State/replay decision, composed barrier model, 34 process crashes, and Principal PASS earned 6%. |
| R0-C branches and pressure | `done` | DS-005a…d | Branch/join plus retention/backpressure passed Principal review and earned 8%. |
| R0-D SDK and hostile gate | `done` | DS-006a…c, DS-007 | Real generators and Principal PASS earned 10%. |
| R1-A production storage safety | `done` | DS-010a…d | PostgreSQL authority, HA, fencing, and ObjectRef visibility passed Principal review and earned 18%. |
| R1-B retention and load | `done` | DS-011a…d | Bounded compaction plus two frozen-hash SLO runs passed Principal review and earned 30%. |
| R2-A production-backed runtime | `done` | DS-020a…d | Sync/async terminal execution and failure semantics passed Principal review and earned 42%. |
| R2-B replacement and state | `done` | DS-021a…d | Takeover, state migration, and repeated Anna/Tanya replacement passed Principal review and earned 55%. |
| R3-A coordinator barriers | `done` | DS-030a…c | Ordered barriers, frozen recovery, hostile traces, and independent Principal PASS earned 64%. |
| R3-B DAG recovery and bounded history | `done` | DS-031a…d | Routed joins, regional recovery/takeover, retained-horizon refusal, bounded retirement and Principal PASS earned 75%. |
| R4-A control-plane authority | `done` | DS-040a…d | Actual HTTP lifecycle/tenant/leader authority, full native activation checks, bounded admissions and Principal PASS earned 83%. |
| R4-B Prefect and operations | `done` | DS-041a…d | Actual SDK/API, fenced replacement, continuous maintenance, operational drill, 139-test gate and Principal PASS earned 90%. |
| R5-A release and schema recovery | `done` | DS-050a…d | Frozen 159-test gate and architecture review earn 95% for selected migration, signed schema recovery and runbook scope. |
| R5-B short release acceptance | `done` | DS-051a…d | 164-test gate, actual capacity/catch-up, selected worker boundary and 7 independent hostile controls close the declared short experimental delivery at 100%. |

Sprint R0 exit: DS-001, DS-002, DS-003a…c, DS-004S/a/b, DS-005a…d,
DS-006a…c, and DS-007 pass; only then does earned readiness become
10%. R0-A process-loss recovery and the DS-004S state/replay spike pass their
focused Principal reviews. Barrier composition also passed its focused review;
branching, reference-safe retention, and bounded live-data pressure passed
their focused reviews. Sync/async generators, durable invocation fencing,
sixteen process-loss boundaries, timeout/cancellation, and the final DS-007
hostile gate then passed. R1 and R2 subsequently passed the current PostgreSQL
HA/load and generator/replacement hostile gate on 2026-10-24. The 55% R2-B
gate was followed by the PostgreSQL coordinator gate on 2026-10-25. R3-A now
earned 64%; R3-B subsequently passed 93/93 frozen-source tests and independent
Principal review, earning 75%. R4-A then passed 110/110 frozen-source tests and
Principal review, earning 83%. R4-B then earned 90%, R5-A earned 95%, and
the final R5-B gate completed the agreed short experimental delivery at 100%.
Evidence: [R4-A report](evidence/postgres-r4a-candidate2-2026-10-25.json) and
[Principal PASS](evidence/postgres-r4a-principal-2026-10-25.json).

## Epoch 2 weekly sprint plan

Percentages are cumulative and earned only by a complete sprint exit. Risk and
size use `H/M` and `S/M/L`. [ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md)
defines the gating policy.

### Active task to ADR map

| Tasks | Technical ADRs |
| --- | --- |
| DS-003a…c, DS-050a…d, DS-051a…d | [ADR-0009](adr/ADR-0009-failure-injection-evidence.md), [ADR-0012](adr/ADR-0012-protocol-epoch-2-delivery-gates.md) |
| DS-004S, DS-004a…b, DS-030a…c | [ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md), [ADR-0013](adr/ADR-0013-managed-state-recovery-classes.md), [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md), [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md) |
| DS-005a…d, DS-031a…d | [ADR-0003](adr/ADR-0003-branch-and-join-recovery.md), [ADR-0007](adr/ADR-0007-backpressure-and-high-load.md), [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md), [ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md) |
| DS-006a…c, DS-020a…d, DS-021a…d | [ADR-0011](adr/ADR-0011-yield-completion-and-client-effects.md), [ADR-0006](adr/ADR-0006-durable-channels-and-state.md) |
| DS-010a…d | [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md), [ADR-0006](adr/ADR-0006-durable-channels-and-state.md) |
| DS-011a…d | [ADR-0006](adr/ADR-0006-durable-channels-and-state.md), [ADR-0007](adr/ADR-0007-backpressure-and-high-load.md) |
| DS-040a…d, DS-041a…d | [ADR-0001](adr/ADR-0001-runtime-boundary.md), [ADR-0010](adr/ADR-0010-platform-durable-yield-channel.md) |
| DS-007 | [ADR-0008](adr/ADR-0008-architecture-role-charter.md), [ADR-0009](adr/ADR-0009-failure-injection-evidence.md) |
| DS-050a…d | [ADR-0026](adr/ADR-0026-release-compatibility-and-disaster-recovery.md) |
| DS-051a…d | [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md), [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md) |
| DS-009 | [ADR-0011](adr/ADR-0011-yield-completion-and-client-effects.md), future multi-output ADR |

### W1–W2 / R0-A — subprocess durability — target 3%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-003a** | `done` | DS-002 | H / M | Real `os._exit` at upload, append, delivery, yield observation, completion, and progress boundaries. As a developer, I can kill a worker at every named durable boundary without timing sleeps. Evidence: 22 abrupt exits, fresh reopen, retry convergence, and Principal PASS in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |
| **DS-003b** | `done` | DS-003a | H / M | Fresh-process reopen and independent oracle. As a system, I got output/state/completion/cursor together or none and rejected unsafe controls. Evidence: sequence-200 YIELD/RETURN matrix, 17 unsafe controls, and Principal PASS in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |
| **DS-003c** | `done` | DS-003a | M / S | Orphan ObjectRef and lost runtime acknowledgement cases. As a user, I got Tanya redelivering exactly the first incomplete input. Evidence: staging/published orphan detection and lost-ACK recovery to 200 or 201 in [ADR-0009](adr/ADR-0009-failure-injection-evidence.md). |

Exit: fresh storage connections and processes prove both sides of document 200;
mocked exceptions alone do not earn 3%.

### W3–W5 / R0-B — state decision and barrier composition — target 6%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-004S** | `done` | DS-003a…c | H / M | Blocking state/replay spike. As a developer, I can declare partition-local per-turn state or checkpoint-managed shared state. Evidence: independent oracle, document-200 boundary, sealed ordered two-input replay, lifecycle/storage controls, and Principal PASS in [ADR-0013](adr/ADR-0013-managed-state-recovery-classes.md). |
| **DS-004a** | `done` | DS-004S | H / M | Ordered DATA/BARRIER composition. As a system, I can keep per-record progress separate from a coordinator checkpoint cut. Evidence: composed log, independent oracle, document-200 boundary, aligned join, fresh-runtime restore, exact tail replay, stable outputs, post-recovery continuation, direct-SQL unsafe controls, and Principal PASS in [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md). |
| **DS-004b** | `done` | DS-004a | H / S | Crash before/after barrier. As a system, I cannot combine cursor 200 with state 100 or create a barrier retroactively after failure. Evidence: 34 named `os._exit(86)` cases, fsynced ACK marker, fresh recovery worker, integrity checks, exact retry, independent oracle, and Principal PASS in [ADR-0014](adr/ADR-0014-ordered-data-barrier-composition.md). |

Exit: executable controls show partition-local state resuming at 201 and shared
or multi-input state restoring a completed barrier then replaying its tail.

### W6–W8 / R0-C — branch, join, retention and pressure — target 8%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-005a** | `done` | DS-004S/a/b | H / M | Two independent consumer cursors. As a user, I can stop Anna while another branch continues. Evidence: one 301-row upstream log, Anna 200→Tanya 201, Boris 300→301, branch-local barrier progress, exact completion/output witnesses, independent oracle, and Principal PASS in [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md). |
| **DS-005b** | `done` | DS-004S/a/b | H / M | Aligned join and durable post-barrier buffer. As a system, I cannot mix cuts at a join. Evidence: bound branch DATA/BARRIER witnesses, exact `{fast:2,slow:2}` snapshot, durable DATA 3/4 buffer, ordered drain, fresh reopen, hostile controls, and Principal PASS in [ADR-0015](adr/ADR-0015-independent-branches-and-durable-join-buffer.md). |
| **DS-005c** | `done` | DS-005a…b | H / M | Retention roots, immutable manifests/lifecycle, tombstone retry, late-root rejection, independent oracle, and Principal PASS. As a system, I cannot delete referenced data and can reclaim unreferenced data. |
| **DS-005d** | `done` | DS-005c | M / S | Atomic record/byte credits, concurrent reservation, block/compact/resume and crash evidence passed Principal review. As a user, Petya continues only to the declared durable limit, then is throttled without silent loss. |

Exit: lagging-branch restart, retention negative control, and full-buffer
block/resume tests pass.

### W9–W12 / R0-D — generator SDK and hostile gate — target 10%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-006a** | `done` | DS-003a…c, DS-005a…d | H / M | Real sync generator, immutable terminal observation, exact YIELD/RETURN completion, and fresh recovery passed. As a developer, I can execute Petya→Anna without a manual commit API. |
| **DS-006b** | `done` | DS-006a | H / M | Async cancellation, timeout, cross-runtime exclusion and recovery passed. As a developer, I get the same progress contract under async cancellation. |
| **DS-006c** | `done` | DS-006a…b | M / S | Durable invocation holder/epoch/attempt fencing, illegal close/return controls and independent oracle passed. As a system, I reject next-input consumption and a second terminal event before completion. |
| **DS-007** | `done` | DS-003a…c, DS-004S/a/b, DS-005a…d, DS-006a…c | H / S | ETL/HighLoad Principal reran 11 tests and hostile races, then passed R0-D on 2026-10-23. As a system, I got PASS for stateless and stateful recovery before claiming 10%. |

Exit: real sync/async generators plus subprocess recovery pass.

### W13–W15 / R1-A — production storage safety — target 18%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-010a** | `done` | DS-007 | H / S | Backend/deployment ADR and explicit consistency/HA assumptions. As a developer, I can identify the authoritative store and failure boundary. Evidence: [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-010b** | `done` | DS-010a | H / L | Ordered partition append plus durable cursor/completion/state transaction. As a system, I preserve exact envelope and progress witnesses across remount/reopen. Evidence: 22/22 backend tests and [HA release 3](evidence/postgres-ha-release3-2026-10-24.json). |
| **DS-010c** | `done` | DS-010b | H / M | Production lease and fencing CAS. As a system, I reject stale writers after process or node reconnect. Evidence: stale-owner controls and promoted-primary recovery in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-010d** | `done` | DS-010b | M / M | ObjectRef visibility and orphan ledger. As a user, I never receive an envelope whose object is not durable. Evidence: staged-orphan and content-address controls in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |

Exit: integration tests run on the actual backend and persistent volume with
two-coordinator races; SQLite-only evidence cannot earn 18%.

### W16–W18 / R1-B — retention, backpressure and load — target 30%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-011a** | `done` | DS-010a…d, DS-004a…b | H / M | Reference graph and compaction. As a system, I reclaim every unreferenced segment and no referenced record/ObjectRef. Evidence: [ADR-0020](adr/ADR-0020-bounded-metadata-and-idempotency-horizon.md). |
| **DS-011b** | `done` | DS-010a…d | H / M | Per-edge byte/record/time/metadata/consumer budgets and credit propagation. As a user, I can see and predict producer throttling. Evidence: atomic capacity controls in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-011c** | `done` | DS-011a…b | H / M | Recovery-horizon and catch-up capacity model. As an operator, I can size storage from ingress, outage horizon, and catch-up target. Evidence: fixed 60-second horizon and 1.25× recovery floor in [ADR-0018](adr/ADR-0018-postgresql-authoritative-data-plane.md). |
| **DS-011d** | `done` | DS-011a…c | M / L | Benchmark/failover harness. As a user, I got agreed p50/p95/p99, sustained ingress, stalled consumer, compaction, and restart measurements. Evidence: [trials 29](evidence/postgres-load-release-trial29-2026-10-24.json), [30](evidence/postgres-load-release-trial30-2026-10-24.json), and [HA release 3](evidence/postgres-ha-release3-2026-10-24.json). |

Exit: agreed SLO thresholds and overload policy pass; measurements without an
agreed threshold do not earn 30%.

### W19–W21 / R2-A — Python runtime execution — target 42%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-020a** | `done` | DS-010a…d, DS-011d | H / L | Sync runtime over the production backend. As a developer, I can run documented decorators and generators. Evidence: [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020b** | `done` | DS-010a…d, DS-011d | H / L | Async runtime. As a developer, I get equivalent delivery semantics for async iteration. Evidence: [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020c** | `done` | DS-020a…b | H / M | Runtime adapter for output/state/completion transaction. As a system, I do not request N+1 before N terminates. Evidence: atomic terminal transaction and 22-boundary crash matrix in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-020d** | `done` | DS-020c | M / M | Exception, cancellation, and timeout semantics. As a user, I can distinguish retryable failure, cancellation, and completed return. Evidence: cancellation/timeout hostile controls in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |

Exit: actual generators and backend survive process loss around observation and
durable completion.

### W22–W24 / R2-B — worker replacement and managed state — target 55%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-021a** | `done` | DS-020a…d | H / M | Heartbeats, lease renewal, and takeover. As a system, I maintain one effective owner. Evidence: epoch/holder fencing in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021b** | `done` | DS-020a…d, DS-004S | H / L | Per-turn managed state API. As a user, Tanya gets the state matching document 200 or 201. Evidence: PostgreSQL state projection and replacement tests in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021c** | `done` | DS-021b | H / M | State schema/version compatibility. As a developer, I get deterministic migration or rejection. Evidence: double-run canonical migration and hostile controls in [ADR-0019](adr/ADR-0019-production-generator-state-and-replacement.md). |
| **DS-021d** | `done` | DS-021a…c | H / M | Repeated replacement-worker matrix. As a system, I reject stale completion and corrupted/incompatible state. Evidence: twenty-turn Anna/Tanya matrix and Principal PASS on 2026-10-24. |

Exit: repeated Anna/Tanya loss, takeover races, and state corruption controls
pass before claiming 55%.

### W25–W27 / R3-A — coordinator barriers — target 64%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-030a** | `done` | DS-021a…d | H / M | Periodic barrier injection shares the partition lock and ordered log with DATA and compaction. As a system, I never synthesize a caller-selected or post-failure cut. Evidence: concurrent ordering and retention-race tests in [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md). |
| **DS-030b** | `done` | DS-030a | H / L | Immutable intent, source/participant manifests, content-addressed snapshots, create-only completion, hard budgets, and frozen recovery plan. As a system, I select only a complete compatible checkpoint. Evidence: independent oracle and unsafe status-only control in [ADR-0021](adr/ADR-0021-postgresql-coordinator-checkpoints.md). |
| **DS-030c** | `done` | DS-030b | H / M | Coordinator takeover resumes the same intent, fences the old epoch, and binds graph/code/state identities. As a developer, I can inspect exact cuts and snapshot versions. Evidence: 14 real-process crash boundaries, 10/10 focused tests, 20/20 takeover repetitions, 39/39 regression tests, and Principal PASS on 2026-10-25. |

Exit: coordinator process loss at every transition and an unsafe incomplete-
checkpoint control pass.

### W28–W30 / R3-B — branches, joins and selective recovery — target 75%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-031a** | `done` | DS-030a…c | H / L | Per-input join alignment and buffering. As a system, I got one coherent cut through routed barriers, durable mailbox/terminal transactions, abort release and async generators. Twelve process-loss boundaries, independent oracles and final Principal PASS close [ADR-0022](adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md)'s R3-B scope. |
| **DS-031b** | `done` | DS-031a | H / M | Branch-local cursors and recovery regions. As a user, I got B/J/D fencing/replay/atomic activation while C stayed live. Append-only owner takeover, four recovery-process crashes, fresh-process convergence and final Principal PASS complete the task. |
| **DS-031c** | `done` | DS-031b | H / M | Upstream expansion inside the retained durable horizon. As a system, I can explain and execute policy-driven expansion, or refuse unsupported source/invalid physical replay authority before publishing a fence. Planner, B/C/J/D execution and internal-output integrity controls pass. External source rewind and missing-byte reconstruction remain explicitly unsupported in [ADR-0093](adr/ADR-0093-implicit-regeneration-of-missing-durable-output.md). |
| **DS-031d** | `done` | DS-031a…b | M / M | Backpressure through a branched DAG. As a user, I got hard record/byte limits, returned credits, DAG pressure and live regional catch-up. One hundred cycles of all four retirement layers retain exactly 64 logical rows within the declared byte budget; final Principal PASS closes [ADR-0023](adr/ADR-0023-bounded-retention-and-replay-horizons.md)'s R3-B scope. |

Exit: canonical branch/join DAG, skewed barriers, branch loss, unavailable cuts,
and mixed-cut/stale-fence model checks pass.

### W31–W33 / R4-A — orchestration server — target 83%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-040a** | `done` | DS-031a…d | H / L | Server persistence/internal APIs as projections of data-plane authority. As a developer, I can query actual run, partition, owner, input cursor, barrier and recovery authority through HTTP; an independent oracle rejects fabricated projections. Full 110-test gate and Principal PASS in [ADR-0024](adr/ADR-0024-control-plane-authority-and-lifecycle.md). |
| **DS-040b** | `done` | DS-040a | H / M | Coordinator leadership and fencing. As a system, I can reject stale leaders through a locked live epoch for lifecycle, reconcile and admission retirement; takeover cannot overtake command commit. Full gate and Principal PASS complete. |
| **DS-040c** | `done` | DS-040a…b | M / M | Deploy/run/cancel/drain. As a user, I got closed admission, atomic DATA publication, an exact full-graph drain and cancellation including regional recovery. Partial native graphs/missing guards fail before activation. HTTP process-loss resume, bounded 100-cycle history and Principal PASS complete. |
| **DS-040d** | `done` | DS-040a | M / S | Authorization and tenant identity. As a system, I reject forged/revoked/insufficient capabilities and cross-tenant controls; source and output partitions have exclusive deployment ownership. Concurrent collision and final Principal PASS complete. |

Exit: HA server failover, duplicate command, stale leader, and permission-negative
tests pass without control/data dual writes.

### W34–W36 / R4-B — Prefect integration and operability — target 90%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-041a** | `done` | DS-040a…d | H / L | Feature-flagged Prefect flow/task integration. As a user, I can deploy the production-shaped streaming flow. Actual SDK/API adapter and independent gate pass in [ADR-0025](adr/ADR-0025-prefect-adapter-and-operational-projections.md). The selected profile atomically rejects regional recovery while a Prefect attempt is active; composing both owner models remains explicit future work while standalone R3 regional recovery stays supported. |
| **DS-041b** | `done` | DS-041a | H / M | Deployment reconnect/resume. As a user, control-plane restart does not lose data-plane progress. |
| **DS-041c** | `done` | DS-041a | M / M | Metrics, traces, and log correlation. As an operator, I can identify lag, in-flight owner, last completion, barrier age, and replay cause. |
| **DS-041d** | `done` | DS-041b…c | M / M | API/UI projections, alerts, and runbook drill. As an operator, I can diagnose backpressure and stalled recovery. |

Exit: real Prefect server/worker E2E, server restart, and alert drill pass.

### W37–W39 / R5-A — upgrade and chaos acceptance — target 95%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-050a** | `done` | DS-041a…d | H / M | Rolling and mixed-version compatibility. As a system, I keep monotonic cursors or reject the rollout. |
| **DS-050b** | `done` | DS-050a | H / M | Schema migration and rollback policy. As a developer, I can migrate or receive a deterministic incompatibility error. |
| **DS-050c** | `done` | DS-050a | H / L | Process, node, network, and object-store fault campaign. As a user, I got the published recovery behavior under infrastructure faults. |
| **DS-050d** | `done` | DS-050b…c | M / M | Backup/restore, drain, and tested runbook. As an operator, I can recover without cursor regression or stale writes. |

Exit: old/new workers, coordinator failover, storage remount, and missing/corrupt
artifact controls pass.

Evidence: [R5-A full candidate](evidence/postgres-r5a-candidate-2026-10-26.json),
[architecture review](evidence/postgres-r5a-architecture-review-2026-10-26.json),
and [recovery runbook](recovery_runbook.md). This selected schema-recovery gate
earns 95%; it does not claim whole-cluster DR, zero RPO or general worker isolation.

### W40–W42 / R5-B — soak, capacity and release — target 100%

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-051a** | `done` | DS-050a…d | H / L | Short declared acceptance profile with injected failures and stalled branches under [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md). As a system, I got no unexplained loss, cursor regression, mixed cut, or stale write in the measured run. |
| **DS-051b** | `done` | DS-051a, DS-011a…d | H / M | Capacity/catch-up validation under [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md). As an operator, I can compare independent calibration with later recovery and inspect conditional headroom estimates, distinguishing logical admission credits from physical disk/WAL behavior. |
| **DS-051c** | `done` | DS-051a | M / M | Security and credential-boundary review. As a system, workers cannot forge storage authority. |
| **DS-051d** | `done` | DS-051a…c | H / S | Final Principal acceptance and ADR evidence graph. As a user, I got the published guarantee and measured limits with no missing evidence link. |

Separate nonblocking follow-up: **DS-052a** — `progress` — actual 72-hour soak, with its own workload, faults, resource and invariant evidence. The user directed use of the three existing self-hosted runners; the CI chain and deterministic reviewer are being configured under [ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md) and the [operator guide](soak_ci.md). It carries no percentage in the current delivery; the user selected short acceptance now in [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md). A short smoke is not a long-duration PASS.

2026-10-27 follow-up: the repository transfer is verified and the
[static-analysis repair](evidence/ci-static-analysis-repair-2026-10-27.json)
passes hosted repository hooks and type checking. The replacement smoke is
queued without an ARC host. Source-hygiene re-acceptance preserves the failed
full trial and requires a passing serial frozen-source successor; DS-052a and
its long-duration criteria remain open.

The [serial full successor](evidence/postgres-ci-style-regression-serial-2026-10-27.json)
now passes 164/164 with no drift and the same source hashes as the preserved
failed trial. Together with 197 fast regressions and hosted static analysis,
it validates the source-hygiene change without changing the short-delivery
percentage or closing long-duration work.

The [Principal review](evidence/ci-source-hygiene-principal-review-2026-10-27.json)
accepts this source-hygiene successor with no blockers.

Later ARC acceptance on 2026-10-27: the
[actual one-job smoke](evidence/ci-arc-smoke-observed-40578404646.json) passes
with recovery, four oracles, public export and owned cleanup, and its
[Principal acceptance](evidence/ci-arc-smoke-principal-review-2026-10-27.json)
closes the smoke repair. The 72-hour run and DS-052a remain open/unstarted;
this follow-up contributes no additional short-delivery percentage.

Post-merge dispatch on 2026-10-27: the
[72-hour campaign](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
now exists on `main`; the [dispatch record](evidence/ci-long-soak-dispatch-40601620025.json)
shows its prepare job queued without an assigned persistent runner. DS-052a
remains `progress`; startup and actual long-duration acceptance are still open.

2026-10-28 retrigger: the user confirmed ARC availability and requested a fresh
trigger. The [unstarted static-profile run was cancelled](evidence/ci-long-soak-cancelled-40601620025.json),
and the [fresh merged-source ARC smoke](evidence/ci-arc-smoke-observed-40650444560.json)
passes recovery, four oracles, review/export and cleanup. DS-052a remains
`progress`: no 72-hour workload ran, and an ARC long profile requires the
separate state/lifetime/evidence validation recorded in
[ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md).

Exit: workload, configuration, raw results, and invariant report are archived;
only this complete gate earns 100% and permits eligible ADRs to become
`accepted`.

### Epoch 2 critical path and parallel lanes

```text
DS-003a→b/c
→ DS-004S → DS-004a→b
→ DS-005a/b→c→d
→ DS-006a→b/c → DS-007                         10%
→ DS-010a→b→c/d → DS-011a/b→c/d                30%
→ DS-020a/b→c→d → DS-021a/b→c/d                55%
→ DS-030a→b→c → DS-031a→b→c/d                  75%
→ DS-040a→b/c/d → DS-041a→b/c→d                90%
→ DS-050a→b/c→d → DS-051a→b/c/d               100%
```

Benchmark harness,
observability schema, and deployment fixtures may start early in parallel, but
they cannot satisfy or bypass a dependency gate.

### Parallel research lane — no readiness percentage

| ID | State | Depends | Risk / size | Deliverable and acceptance criteria |
| --- | --- | --- | --- | --- |
| **DS-009** | `blocked` | DS-006a…c | H / S | Compare numbered `emit()+complete()` with generator multi-yield and create a planning or rejected ADR. As a developer, I can reason about crash after output N without expanding epoch-2 implementation scope. |

DS-009 may run after the SDK contract exists. It does not belong to any epoch-2
gate and cannot delay 100% unless it disproves the single-terminal-event
contract itself.

Each sprint exit requires linked artifacts, reproducible commands, raw failure
or load evidence, resolved blocking review findings, and updated ADR/task/board
status. A green test suite without the sprint-specific environment and evidence
does not earn the percentage.

## Historical protocol epoch 1 — frozen superseded planning

Everything below records the old plan and evidence. Checkboxes, critical paths,
and WIP instructions below are historical and do not direct protocol epoch 2.

- [x] **ST-001** Establish ADR lifecycle and dependency graph
  ([ADR-0000](adr/ADR-0000-decision-lifecycle.md)).
- [x] **ST-002** Define runtime boundary and guarantee vocabulary
  ([ADR-0001](adr/ADR-0001-runtime-boundary.md)).
- [x] **ST-003** Prototype incomplete-checkpoint rejection
  ([ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md)).
- [x] **ST-004** Prototype selective branch recovery
  ([ADR-0003](adr/ADR-0003-branch-and-join-recovery.md)).
- [x] **ST-005** Prototype recovery after a crash between sink commits
  ([ADR-0004](adr/ADR-0004-multi-sink-commit.md)).
- [x] **ST-006** Complete ETL/HighLoad Principal review and counterexamples
  ([ADR-0008](adr/ADR-0008-architecture-role-charter.md),
  [ADR-0009](adr/ADR-0009-failure-injection-evidence.md)).
- [x] **ST-007** Preserve rejected transaction and idempotency theories
  ([ADR-0090](adr/ADR-0090-prefect-transactions-as-coordinator.md),
  [ADR-0091](adr/ADR-0091-idempotency-alone-is-atomic.md)).

## Sprint 1A/1B — safety contracts — cumulative targets 20% and 27%

Sprint 1 was split after Principal review found that its original H-risk chain
did not fit one short sprint. Sprint 1A isolates the end-to-end safety invariant;
Sprint 1B freezes the complete contracts before barrier work begins.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-010** | ST-001…006 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Canonical job, graph-version, operator, edge, partition, record, checkpoint, and generation IDs. | As a developer, I can derive the same stable identity after restart. As a system, I can reject reuse with different lineage or payload. |
| **ST-011** | ST-010 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0004](adr/ADR-0004-multi-sink-commit.md) | Checkpoint state machine with start, alignment, prepare, commit-decision, completion, and abort paths. | As a system, I can validate every transition and reject rollback after `COMMIT_DECIDED`. |
| **ST-012** | ST-010 | H / M | [0001](adr/ADR-0001-runtime-boundary.md), [0005](adr/ADR-0005-operator-contract.md) | Versioned Source, Operator, Channel, StateBackend, and Sink contracts with capability negotiation. | As a developer, I can implement a participant against a versioned contract. As a user, I got deployment rejection when requested guarantees exceed capabilities. |
| **ST-013** | ST-011, ST-012 | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Named failure matrix and deterministic fault-injection API without timing sleeps. | As a developer, I can inject a crash before and after every durable boundary. As a system, I got a named invariant result for every point. |
| **ST-014** | ST-010, ST-011 | M / S | [0009](adr/ADR-0009-failure-injection-evidence.md) | Independent safety oracle for recovery checkpoint, visible records, decisions, and allowed duplicates. | As a system, I can compare recovered state with an independent oracle. |

Sprint exit: identities, transitions, and guarantees are explicit protocol
artifacts rather than implicit prototype behavior.

### Execution evidence

- [x] **Sprint 1A / 20% gate:** stable identity derivation, immutable lineage claims,
  combined checkpoint/decision/source-ACK record, recovery on both sides of the
  decision, independent oracle, and unsafe control are implemented; ETL/HighLoad
  Principal review passed on 2026-10-20.
- [x] **ST-010 implementation:** all eight canonical identity classes reproduce
  after restart; the integrated durable registry rejects lineage or payload drift
  for the same record across checkpoint transactions by storing an explicit
  lineage digest.
- [x] **ST-011 implementation:** the checkpoint lifecycle persists across reopen,
  rejects illegal transitions, and forbids abort after `COMMIT_DECIDED`.
- [x] **ST-012 implementation:** versioned participant contracts negotiate tier
  `T` and reject unsupported source or sink capabilities.
- [x] **ST-013 implementation:** nine durable boundaries expose deterministic
  before/write-before-ACK/after-ACK failure points without sleeps; every guarded
  predecision phase edge is exercised at all three offsets.
- [x] **ST-014 implementation:** an independent oracle names recovery, decision,
  payload, duplicate, expected-effect completeness, and transactional-visibility
  invariant results, including visible effects before a commit decision, effects
  missing from an externally committed sink, and external participant IDs absent
  from the immutable manifest. Missing required local rows produce failed named
  invariants rather than an oracle exception.
- [x] **Sprint 1B / 27% gate:** ETL/HighLoad Principal review found no remaining
  material defect within the Sprint 1 prototype scope on 2026-10-20; ADR/task/
  board links pass validation.

Evidence lives in
[integrated_protocol.py](prototypes/integrated_protocol.py),
[test_integrated_protocol.py](prototypes/test_integrated_protocol.py),
[ADR-0002](adr/ADR-0002-checkpoints-and-recovery-domains.md),
[ADR-0005](adr/ADR-0005-operator-contract.md), and
[ADR-0009](adr/ADR-0009-failure-injection-evidence.md). The first split-state
iteration is retained as rejected evidence in
[ADR-0092](adr/ADR-0092-split-safety-state-machines.md).

## Sprint 2 — barriers, recovery regions, and fencing — cumulative target 43%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-020** | ST-010…014 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Partitioned Python channel with real data and barrier records. | As a system, I can preserve per-partition order across restart and reject gaps or wrong epochs. |
| **ST-021** | ST-020 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0003](adr/ADR-0003-branch-and-join-recovery.md) | Aligned barrier state machine for multi-input joins. | As a system, I can snapshot a join without mixing pre-barrier and post-barrier records. |
| **ST-022** | ST-010, ST-020 | H / M | [0003](adr/ADR-0003-branch-and-join-recovery.md) | Recovery-region planner for descendants, replay cuts, join inputs, and upstream expansion. | As a user, I can see why each node entered recovery. As a system, I can replay every boundary input from one completed checkpoint. |
| **ST-023** | ST-010…012 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Generation leases and fencing on channel, snapshot, and decision writes. | As a system, I can reject every write from an obsolete worker or coordinator epoch. |
| **ST-024** | ST-013, ST-020 | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Subprocess kill/reopen harness with independently reopened stores. | As a developer, I can terminate a process at named boundaries and reopen every durable component in a new process. |
| **ST-025** | ST-011, ST-021, ST-023 | H / L | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | TLA+ checkpoint, alignment, and fencing model, including unsafe variants. | As a system, I got bounded model evidence for checkpoint consistency and stale-writer exclusion. |

Sprint exit: selective recovery uses actual barriers and process loss, and obsolete
workers cannot mutate the new generation.

### Execution evidence

- [x] **ST-020:** ordered partition records include barriers; reopen, stable
  record claims, contiguous sequence, and lease/generation fencing pass Principal
  review.
- [x] **ST-021:** an early join input is durably blocked after its barrier; the
  snapshot is recorded only after every input reaches the same checkpoint, then
  buffered post-barrier data is applied. Restore retains immutable historical
  consumed identities, rejects a divergent same-result suffix, and permits exact
  replay. Principal review passed.
- [x] **ST-022:** the planner binds one completed checkpoint to per-partition
  replay cuts, returns failed descendants and reasons, and expands upstream when
  the checkpoint is unavailable at a boundary. Principal review passed; binding
  the manifest digest to a production graph version remains an integration task.
- [x] **ST-023:** channel and join leases use a trusted durable monotonic store
  clock, non-shortening renewal, expiry-gated generation takeover, and atomic
  owner/generation/expiry guards on mutation. Principal review passed.
- [x] **ST-024:** 21 real `os._exit` cases cover seven boundaries at three
  offsets, followed by fresh channel, join, or coordinator reopen and independent
  oracle evaluation where applicable. Principal review passed.
- [x] **ST-025:** the two-input/two-checkpoint/two-epoch TLA+ model generates
  9,471 states and finds 2,132 distinct safe states to depth 17. Independent
  mixed-checkpoint and missing-fence mutations produce the expected
  `SameCheckpointCut` and `FencingSafety` counterexamples. Principal review
  passed within bounded safety scope; liveness remains outside the model.
- [x] **Sprint 2 / 43% gate:** all task-level prototype/formal criteria passed
  ETL/HighLoad Principal review on 2026-10-20. This is not production acceptance.

Evidence lives in [barrier_protocol.py](prototypes/barrier_protocol.py) and
[test_barrier_protocol.py](prototypes/test_barrier_protocol.py); process-loss
evidence is in
[test_subprocess_recovery.py](prototypes/test_subprocess_recovery.py), and formal
artifacts live in [models/README.md](models/README.md).

## Sprint 3A/3B — multi-sink and source acknowledgement — cumulative targets 49% and 58%

Sprint 3A is a one-week safety slice on the serialized critical path. It closes
the cooperative sink contract and authoritative decision creation before source
and weaker-tier execution expand the state space.

- [x] **ST-030a:** define `CooperativeSinkV1` prepare/commit/abort/recover
  semantics, immutable prepare-token identity, and coordinator plus participant
  fencing; pass a durable reference-sink contract suite.
- [x] **ST-030b:** abort every prepared participant after predecision failure,
  retry a lost abort acknowledgement, and forbid abort after durable `COMMIT`.
- [x] **ST-031a:** enforce COMMIT creation in storage from `PREPARED` only and
  only when every immutable sink participant is prepared; preserve a forged raw
  SQL decision as a rejected control.
- [x] **ST-032a:** continue recovery past an unavailable committed participant
  and expose durable attempt count, last error, and blocked participant state.

Sprint 3A exit: ST-030 and ST-031 pass Principal review and ST-032 has explicit
blocked-participant evidence. Only then is cumulative 49% claimed. ST-033,
behavioral tiers I/B, and ST-035 remain in Sprint 3B toward 58%.

### Sprint 3A execution evidence

- [x] `CooperativeSinkV1` has durable prepare tokens, transaction-scoped
  coordinator fences, independent participant-generation fences, recoverable
  terminal state, and lost-ACK contract tests.
- [x] The authority creates `COMMIT` only from the exact immutable prepared
  manifest. `ABORT` first advances the coordinator fence, is irreversible, and
  continues cleanup past unavailable participants.
- [x] Recovery attempts are registered durably before external calls and finish
  as `IN_PROGRESS`, `SUCCEEDED`, or `BLOCKED`. Owner/fence and attempt-number
  CAS guards reject late results; restart reconciles terminal participant state.
- [x] Operator, sink, and source-partition identities are disjoint, so one
  participant cannot overwrite another role's diagnostics.
- [x] Crash, lost-ACK, unavailable-participant, takeover, forged-decision, and
  terminal-reconciliation regressions pass in the 92-test prototype suite.
- [x] ETL/HighLoad Principal hostile review passed on 2026-10-20. Sprint 3A's
  cumulative 49% gate is claimed as prototype evidence; ADRs remain `progress`.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-030** | ST-011, ST-012, ST-023 | H / M | [0004](adr/ADR-0004-multi-sink-commit.md) | Versioned cooperative sink protocol with prepare, commit, abort, recover, fencing, and immutable intent digest. | As a developer, I can implement a cooperative sink and run its contract suite. |
| **ST-031** | ST-030 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0004](adr/ADR-0004-multi-sink-commit.md) | One durable decision record linking checkpoint and multi-sink transaction. | As a system, I cannot publish `COMPLETED` or acknowledge source progress before durable `COMMIT_DECIDED`. |
| **ST-032** | ST-024, ST-031 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tests for prepare failure, lost ACK, coordinator crash, partitions, repeated recovery, and unavailable sinks. | As a system, I can abort before the decision and only commit after it. As a user, I can see transactions blocked in `COMMITTING`. |
| **ST-033** | ST-020, ST-031 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Source acceptance, acknowledgement, and replay contract. | As a user, I got no acknowledged-input loss after any enumerated crash. As a system, I can replay unacknowledged input with stable IDs. |
| **ST-034** | ST-012, ST-030 | M / M | [0004](adr/ADR-0004-multi-sink-commit.md) | Conformance suites for sink tiers `T`, `I`, and `B`. | As a user, I can see the enforced tier and transient-visibility semantics. |
| **ST-035** | ST-025, ST-031 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | TLA+ multi-sink decision model with missing-decision and missing-fence variants. | As a system, I got one irreversible decision per immutable manifest within documented liveness assumptions. |

Sprint exit: the bounded model demonstrates end-to-end safety from accepted input
to multiple sinks; temporary partial visibility remains explicit.

### Sprint 3B1 — source acceptance and replay — cumulative target 53%

The ETL/HighLoad Principal split Sprint 3B after the 49% gate. ST-033 freezes the
source side before tier behavior and the multi-sink formal model encode it.

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-033a** ✓ | ST-020, ST-031 | H / S | [0005](adr/ADR-0005-operator-contract.md) | `ReplayableSourceV1` contract for stable record identity, opaque position, runtime ordinal, replay, acknowledgement, and participant fencing. | As a developer, I can implement read/replay/ack without comparing opaque positions. As a system, I got an immutable per-partition cut. |
| **ST-033b** ✓ | ST-033a | H / M | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Atomic accepted-input ledger and durable channel handoff. | As a system, I cannot report acceptance unless identity, lineage, payload, ordinal, position, and channel record are durable. As a developer, I can retry an identical handoff and drift is rejected. |
| **ST-033c** ✓ | ST-033b | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md), [0005](adr/ADR-0005-operator-contract.md) | Exact checkpoint vector, decision-bound ACK, and replay planner. | As a user, I got no acknowledged position without exact `COMMIT` and covered cut. As a system, I can replay every unacknowledged record without selecting a partial vector. |
| **ST-033d** ✓ | ST-024, ST-033c | H / M | [0009](adr/ADR-0009-failure-injection-evidence.md) | Deterministic and subprocess crash matrix with an independent source oracle. | As a developer, I can reproduce every source boundary after fresh reopen. As a user, I can inspect accepted, committed, externally acknowledged, and replay-start cuts plus blocking gaps. |

Sprint 3B1 exit: ST-033a…d pass ETL/HighLoad Principal hostile review. Only
then is cumulative 53% claimed. ST-034 contributes the next 2% and ST-035 the
final 3% toward Sprint 3B's 58% target.

Sprint 3B1 passed ETL/HighLoad Principal hostile review on 2026-10-21. The
cumulative 53% prototype gate is claimed. The source suite has 18 focused cases,
including real-process crashes, a real coordinator decision authority, takeover
after an unknown external result, repeated-cut epochs, and independent-oracle
negative controls.

### Sprint 3B2A — sink-tier conformance — cumulative target 55%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-034a** ✓ | ST-012, ST-030 | H / S | [0004](adr/ADR-0004-multi-sink-commit.md) | `TierGuaranteeV1`, immutable execution intent and common visibility/outcome vocabulary. | As a developer, I can map each tier to required capabilities and terminal states. As a user, I can see guarantees, partial visibility, duplicates, uncertainty, and gaps. |
| **ST-034b** | ST-034a | M / S | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `T` conformance suite over `CooperativeSinkV1`. | As a system, I got no visible `T` effect before `COMMIT` and one logical effect per sink after recovery. As a user, I can observe permitted `PARTIAL` visibility between commits. |
| **ST-034c** | ST-034a | H / M | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `I` reference adapter, durable delivery intent, and crash/recovery suite. | As a developer, I can retry an unknown write with the same effect key. As a system, I converge without logical duplicates and retain input while a delivery is unresolved. |
| **ST-034d** | ST-034a | H / M | [0004](adr/ADR-0004-multi-sink-commit.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Tier `B` reference adapter and persisted retry/exhaustion policy. | As a user, I got explicit duplicate, split, `UNKNOWN`, `EXHAUSTED`, and `GAPPED` evidence. As a system, I advance across a gap only with frozen lossy opt-in. |
| **ST-034e** | ST-034b, ST-034c, ST-034d | H / S | [0001](adr/ADR-0001-runtime-boundary.md), [0004](adr/ADR-0004-multi-sink-commit.md) | Mixed-tier restart matrix and independent tier oracle. | As a system, I select the weakest tier, reject a stronger request, and reject tier/capability/policy drift. As a user, I can inspect the reason for deployment or recovery rejection. |

Sprint 3B2A exit: ST-034a…e pass ETL/HighLoad Principal hostile review. Only
then is cumulative 55% claimed. Production adapters remain ST-060.

### Sprint 3B2B — formal tier-T decision model — cumulative target 58%

ST-035 models only the tier `T` decision core behind the barrier/checkpoint
assume-guarantee boundary. The safe model covers two checkpoints, two sinks,
immutable manifests, prepare tokens, coordinator and participant fencing,
irreversible `COMMIT`/`ABORT`, permitted partial visibility, completion, and
source ACK. Missing-decision and missing-fence mutations must produce named TLC
counterexamples. Conditional convergence assumes weak fairness and eventual
participant availability; simultaneous visibility remains outside the claim.

## Sprint 4 — durable storage, backpressure, and load — cumulative target 72%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-040** | ST-020, ST-033 | H / L | [0006](adr/ADR-0006-durable-channels-and-state.md) | Go segmented durable-channel prototype without an external broker. | As a system, I can reopen committed segments after process and storage-remount simulations without record loss. |
| **ST-041** | ST-011, ST-012 | H / M | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Full/incremental state-snapshot prototypes and compatibility metadata. | As a developer, I can restore a compatible schema and I get deterministic rejection for an incompatible schema. |
| **ST-042** | ST-022, ST-033, ST-040 | H / M | [0003](adr/ADR-0003-branch-and-join-recovery.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Retention watermark from checkpoints, consumer cursors, and pending sink decisions. | As a system, I can prove no referenced segment is deleted and reclaim every unreferenced segment. |
| **ST-043** | ST-040 | H / M | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Byte/record credits and bounded memory/durable backlog. | As a system, I can propagate sink pressure to the source without unbounded growth. |
| **ST-044** | ST-041…043 | H / L | [0007](adr/ADR-0007-backpressure-and-high-load.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Slow-sink, disk-full, skew, and recovery-catch-up benchmarks. | As a user, I got measured throughput, p95/p99 latency, checkpoint time, replay rate, and overload behavior. |
| **ST-045** | ST-040…044 | H / M | [0006](adr/ADR-0006-durable-channels-and-state.md), [0007](adr/ADR-0007-backpressure-and-high-load.md) | Storage decision, rejected alternatives, and capacity model. | As a developer, I can size memory and disk from ingress, recovery horizon, and catch-up target. |

Sprint exit: production integration has a selected backend and measured operating
envelope rather than an abstract high-load claim.

## Sprint 5 — experimental production integration — cumulative target 88%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-050** | Sprint 1–4 exits | H / L | [0001](adr/ADR-0001-runtime-boundary.md), [0005](adr/ADR-0005-operator-contract.md) | Experimental sync/async `yield` runtime behind a feature flag. | As a developer, I can run source → transform → branch → join → sink through the documented API. |
| **ST-051** | ST-025, ST-035, ST-045 | H / L | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Production checkpoint/recovery coordinator and durable decision store. | As a system, I can recover coordinator state without selecting an incomplete checkpoint. |
| **ST-052** | ST-023, ST-051 | H / M | [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Worker ownership, generation leases, and stale-writer enforcement. | As a system, I got one effective writer generation per partition after replacement. |
| **ST-053** | ST-011, ST-031, ST-045 | H / L | [0001](adr/ADR-0001-runtime-boundary.md), [0002](adr/ADR-0002-checkpoints-and-recovery-domains.md) | Server persistence schemas and internal APIs. | As a developer, I can inspect checkpoint, participant, recovery, and commit state through stable internal APIs. |
| **ST-054** | ST-050…053 | M / M | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Events, metrics, and recovery-graph observability. | As a user, I can see lag, replay, alignment, recovery region, sink tier, and blocked commits. |
| **ST-055** | ST-050…054 | H / L | [0009](adr/ADR-0009-failure-injection-evidence.md) | Production contract tests mapped to TLA+ actions and prototype invariants. | As a system, I got production evidence for every mapped safety transition. |

Sprint exit: an experimental production-shaped runtime exists, while ADRs remain
short of `accepted` until external and load acceptance passes.

## Sprint 6 — external acceptance and release gate — cumulative target 100%

| ID | Depends on | Risk / size | ADR | Deliverable | Acceptance criteria |
| --- | --- | --- | --- | --- | --- |
| **ST-060** | ST-034, ST-050 | H / L | [0004](adr/ADR-0004-multi-sink-commit.md) | Reference replayable source and tier `T`/`I` sinks without an external broker. | As a developer, I can validate real adapters with the prototype conformance suite. |
| **ST-061** | ST-055, ST-060 | H / L | [0003](adr/ADR-0003-branch-and-join-recovery.md), [0009](adr/ADR-0009-failure-injection-evidence.md) | Branched-DAG end-to-end crash matrix through multiple sinks. | As a user, I got outputs equal to the failure-free oracle for every supported tier. |
| **ST-062** | ST-044, ST-061 | H / L | [0007](adr/ADR-0007-backpressure-and-high-load.md) | Long soak, host loss, slow/unavailable sink, and catch-up tests. | As a user, I got documented capacity and recovery objectives sustained under load. |
| **ST-063** | ST-041, ST-051, ST-061 | H / L | [0005](adr/ADR-0005-operator-contract.md), [0006](adr/ADR-0006-durable-channels-and-state.md) | Cancellation, drain, upgrade, graph-version, and state-migration tests. | As a system, I can reject or migrate incompatible recovery state without silent corruption. |
| **ST-064** | ST-054, ST-061…063 | M / M | [0000](adr/ADR-0000-decision-lifecycle.md) | Operational runbook and user guarantee documentation. | As a user, I can diagnose stalled checkpoints and understand possible duplicates or partial visibility. |
| **ST-065** | All previous | H / M | [ADR graph](adr/README.md) | Acceptance-evidence index and ADR status review. | As a developer, I can trace every accepted criterion to production evidence. As a user, I got no guarantee based only on prototype results. |

Sprint exit: only ADRs with production code and complete evidence move to
`accepted`; the rest remain `progress`/`planning` or become `rejected`.

## Critical path and parallelism

```text
ST-010
→ ST-011 / ST-012
→ ST-020
→ ST-021 / ST-022 / ST-023
→ ST-024 / ST-025
→ ST-030 → ST-031
→ ST-032 / ST-033 / ST-035
→ ST-040 / ST-041
→ ST-042 / ST-043 → ST-044 → ST-045
→ ST-050 / ST-051 / ST-053 → ST-052 / ST-054 / ST-055
→ ST-060 → ST-061 → ST-062 / ST-063 → ST-065
```

Do not start barriers before identities and the checkpoint state machine; do not
start multi-sink coordination before sink contracts and fencing; do not advance
source acknowledgement before the checkpoint/decision link; do not implement
retention before recovery cuts and pending-commit references; and do not select
production storage before crash/load evidence.

After their contracts freeze, barrier/join, fencing, and subprocess work can run
in parallel. Executable and TLA+ models can also run in parallel, as can channel
and state-backend experiments. SDK and server work can overlap only after the
serialized protocol is stable.

## Final short delivery evidence

The [164-test final gate](evidence/postgres-r5b-final2-2026-10-26.json) and
[7 independent controls](evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
close DS-051a…d. [ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md)
publishes the workload, conditional forecasts, fixes, rejected trials and
limits. This is 100% of the agreed short experimental implementation, not a
claim of production completeness. DS-052a, hosted runner availability and
broader technical-ADR production criteria remain separate open work.

Final sign-off: [architecture review](evidence/postgres-r5b-architecture-review-2026-10-26.json)
and [documentation graph](evidence/adr-graph-final-2026-10-26.json).
