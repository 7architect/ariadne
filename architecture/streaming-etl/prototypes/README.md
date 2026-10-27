# Prototype protocol epochs

Current source-hygiene regression: the
[2026-10-27 serial gate](../evidence/postgres-ci-style-regression-serial-2026-10-27.json)
passes 164/164 tests against the published source after repository-hook cleanup.
The [repair report](../evidence/ci-static-analysis-repair-2026-10-27.json) links
the preserved failed first trial, isolated timing retry, 197 fast regressions,
source-byte comparisons and hosted CI results. Historical reports below retain
their original executed hashes. Scope and remaining runner acceptance are in
[ADR-0028](../adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md).

## Active protocol epoch 2

- `durable_stream_v2.py` and `test_durable_stream_v2.py`: durable yielded DATA,
  ObjectRef admission, consumer progress, redelivery, state, and fencing.
- `durable_object_store.py`, `durable_stream_v2_crash_worker.py`, and
  `test_durable_stream_v2_subprocess.py`: filesystem-backed ObjectRef publish
  and the DS-003a real-process crash matrix.
- `durable_stream_v2_oracle.py`, `durable_stream_v2_recovery_worker.py`, and
  `test_durable_stream_v2_r0_gate.py`: independent read-only R0-A oracle,
  sequence-200 recovery, lost-ack, orphan, and unsafe-control evidence.
- `barrier_protocol.py` and `test_barrier_protocol.py`: reusable ordered-barrier,
  join-alignment, fencing, and recovery-cut primitives. Its join cursor predates
  `completed_through` and must not be treated as the new completion contract.
- `state_recovery_spike.py`, `state_recovery_oracle.py`, and
  `test_state_recovery_spike.py`: DS-004S classification of partition-local
  per-turn and checkpoint-managed shared state, local recovery on both sides of
  document 200, completed-vector-cut replay in durable mailbox order, and
  deliberately unsafe split/mixed/retroactive controls.
- `durable_stream_barrier_v2.py`, `durable_stream_barrier_oracle.py`, and
  `test_durable_stream_barrier_v2.py`: DS-004a composition of DATA and BARRIER
  in one partition sequence, separate DATA cursors and snapshot cuts,
  single-input continuation before global completion, aligned multi-input
  buffering, fresh-connection snapshot-plus-mailbox-tail recovery, explicit
  recovery generation/phase, output-identity preservation, and continuation
  after recovery. Turn generations and direct-SQL controls prevent a truncated
  recovery witness from omitting part of the durable suffix.
- `durable_stream_barrier_crash_worker.py`,
  `durable_stream_barrier_recovery_worker.py`, and
  `test_durable_stream_barrier_subprocess.py`: DS-004b deterministic
  `os._exit(86)` matrix at 34 barrier, alignment, completion, fence, and recovery
  boundaries, followed by fresh-process exact retry and independent-oracle
  convergence.
- `branch_join_v2.py`, `branch_join_oracle.py`, and
  `test_branch_join_v2.py`: DS-005a/b fan-out with independent Anna/Boris
  cursors, Tanya takeover at 201, immutable branch DATA/BARRIER witnesses,
  exact fan-out-to-join authority, exact join vector cuts, and a durable
  post-barrier buffer drained in persisted arrival order.
- `postgres_barrier_coordinator.py`, `postgres_checkpoint_runtime.py`, and
  `test_postgres_barrier_coordinator.py`: DS-030a…c production-backed
  coordinator intents, serial DATA/BARRIER injection, exact participant
  snapshots, complete-only selection, frozen recovery plans, takeover, hard
  checkpoint budgets, and retention/compaction races. The runtime adapter is
  used only for checkpoint-managed shared state.
- `postgres_barrier_oracle.py`, `postgres_barrier_crash_worker.py`, and
  `postgres_barrier_gate_harness.py`: independent R3-A invariant derivation,
  fourteen real-process crash boundaries, and a reproducible synchronous
  PostgreSQL evidence report. See
  [ADR-0021](../adr/ADR-0021-postgresql-coordinator-checkpoints.md).

The completed R3-B gate uses `postgres_join_runtime.py`,
`postgres_join_oracle.py`, `postgres_join_crash_worker.py`, and
`test_postgres_join_runtime.py` for live routed barriers and durable mailbox
turns. `postgres_recovery_regions.py` and `test_postgres_recovery_regions.py`
exercise selective recovery planning and boundary expansion. Scope and the
remaining gate criteria are recorded in
[ADR-0022](../adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md).

`postgres_regional_recovery.py` executes fenced recovery generations and
region-wide activation. Retention is split across `postgres_join_retention.py`,
`postgres_checkpoint_retention.py`, `postgres_regional_retention.py`, and
`postgres_streaming_retention.py`; their independent archive oracles and
`postgres_storage_footprint.py` check retained proof and logical storage bounds.
The theory, horizon limits and combined acceptance are in
[ADR-0023](../adr/ADR-0023-bounded-retention-and-replay-horizons.md).

Run the current PostgreSQL candidate on the configured synchronous test pair:

```shell
python postgres_r3b_gate_harness.py --output ../evidence/postgres-r3b-local.json
```

The harness discovers the tests, freezes source hashes, rejects source drift,
records process-loss coverage and measurements, and checks the PostgreSQL
durability profile. A green candidate report does not award readiness: the
Principal review and every sprint acceptance criterion must also pass.

The [final R3-B report](../evidence/postgres-r3b-final2-2026-10-25.json) passed
93/93 tests with no source drift. The
[Principal review](../evidence/postgres-r3b-principal-2026-10-25.json) confirms
the gate at 75%, including 100 integrated regional-recovery/retirement cycles
with 64 retained logical rows. These are isolated prototypes; Prefect core and
server integration remain later work.

## Current control and orchestration work

R4-A earned **83%**, with [110/110 passing tests](../evidence/postgres-r4a-candidate2-2026-10-25.json)
and [independent Principal acceptance](../evidence/postgres-r4a-principal-2026-10-25.json).
`postgres_control_plane.py`, its HTTP server/client and independent oracle bind
tenant capabilities, native graph identity, bounded source admissions,
leadership and drain/cancel to the existing stream authority. See
[ADR-0024](../adr/ADR-0024-control-plane-authority-and-lifecycle.md).

R4-B earned **90%** with its
[139-test report](../evidence/postgres-r4b-candidate3-2026-10-25.json) and
[independent review](../evidence/postgres-r4b-principal-2026-10-25.json).
`prefect_stream_adapter.py` and
`postgres_prefect_attempts.py` run actual Prefect 3 flow/task actors and fenced
replacement attempts over the same logical stream. The selected server runs
its event services: disabling them prevents persisted task-run observations.
`postgres_prefect_maintenance.py` drives periodic barriers and bounded retirement
so a continuous source can exceed the queue's physical capacity. Declared
operator artifacts bind Python generators to immutable code identities.

`postgres_operational_server.py` adds an authenticated observation API,
Prometheus projections and a browser console with linked operational actions.
It reads actual Prefect states through `postgres_stream_observability.py` and
correlates bounded OpenTelemetry spans/events without persisting secret values.
`test_postgres_stream_operations.py` tests tenant isolation and an actual
capacity-return drill; `test_postgres_operations_browser.py` runs Chromium
against real HTTP/SQL and checks drain, mobile layout and credential cleanup.
Independent `postgres_prefect_attempt_oracle.py` and
`postgres_prefect_maintenance_oracle.py` verify authority and retirement.
Scope and acceptance are in
[ADR-0025](../adr/ADR-0025-prefect-adapter-and-operational-projections.md).

R5-A earned **95%** for the selected migration/session and signed schema-recovery
scope, with a [159-test candidate](../evidence/postgres-r5a-candidate-2026-10-26.json)
and [architecture review](../evidence/postgres-r5a-architecture-review-2026-10-26.json).
`postgres_release_authority.py`, `postgres_restore_registry.py` and
`postgres_schema_backup.py` implement the selected version, quiesce, exported
snapshot, source-retirement and exclusive target-activation protocol. Use the
[recovery runbook](../recovery_runbook.md) for exact procedure and limits.

The successor short gate adds actual stalled-branch capacity calibration and
independent predictions in `test_postgres_capacity_acceptance.py`. It also tests
retry of join retirement while an invocation is live and uses public future
completion callbacks instead of repeated server-backed state polling. Scope,
rejected trials and remaining final acceptance are in
[ADR-0029](../adr/ADR-0029-short-capacity-calibration-and-release-scope.md).
The [72-hour CI campaign](../soak_ci.md) remains separate and unpassed; its
workload must not be described as 72 hours of native Prefect execution.

R5-B now closes the selected short experimental delivery: the
[complete final gate](../evidence/postgres-r5b-final2-2026-10-26.json) passes
164/164 tests, and the [independent controls](../evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
pass 7/7. The gate has no source drift or unclosed-socket warnings. Technical
ADRs with broader production criteria remain open. The
[environment inventory](../evidence/postgres-short-acceptance-environment-2026-10-26.json)
records the actual runtime and pinned image identities.

The current release harness discovers all PostgreSQL tests and hashes both
Python and HTML sources. Run it after the static review closes; a candidate
report does not itself award readiness:

```shell
python postgres_release_gate_harness.py --phase R5-B --output ../evidence/postgres-r5b-local.json
```

## Historical protocol epoch 1 (retained evidence)

`integrated_protocol.py`, `multisink_v1.py`, `tier_semantics.py`, `source_v1.py`,
`source_oracle.py`, `safety_contracts.py`, `protocols.py`, and their tests are
retained as rejected or superseded evidence. They model platform-coordinated
source acknowledgement and external sink outcomes. Protocol epoch 2 does not
import them and their passing tests do not count toward current readiness.

The active focused check is:

```shell
python -m unittest -v \
  test_durable_stream_v2.py \
  test_durable_stream_v2_subprocess.py \
  test_durable_stream_v2_r0_gate.py \
  test_state_recovery_spike.py \
  test_durable_stream_barrier_v2.py \
  test_barrier_protocol.py \
  test_branch_join_v2.py
```
