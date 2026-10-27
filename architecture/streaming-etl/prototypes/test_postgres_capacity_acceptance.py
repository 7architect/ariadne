"""Short capacity profile for backlog, catch-up, and bounded headroom evidence.

The profile uses the actual Prefect adapter and PostgreSQL protocol authority.
It measures one declared workload and records its limits; it is not a production
capacity benchmark or long-duration stability claim.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
import unittest
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import test_postgres_prefect_adapter as fixtures
from postgres_barrier_oracle import evaluate as evaluate_barrier
from postgres_control_plane_oracle import evaluate as evaluate_control
from postgres_generator_runtime import stream_task
from postgres_join_oracle import evaluate as evaluate_join
from postgres_join_retention import JoinRetirementBusy
from postgres_prefect_attempt_oracle import evaluate as evaluate_attempt
from postgres_prefect_maintenance import PostgresPrefectMaintenance
from postgres_prefect_maintenance_oracle import evaluate as evaluate_maintenance
from postgres_storage_footprint import evaluate as evaluate_footprint
from postgres_stream_oracle import evaluate as evaluate_stream
from prefect_stream_adapter import (
    _FutureCompletionWatcher,
    prefect_stream_flow,
    register_operator_artifact,
)

from prefect.states import StateType

CALIBRATION_STARTED = threading.Event()
CALIBRATION_RELEASED = threading.Event()
STALL_STARTED = threading.Event()
STALL_RELEASED = threading.Event()
STALL_STARTED_AT: float | None = None
CALIBRATION_AT_SEQUENCE = 2
CALIBRATION_STALL_SECONDS = 2.5
STALL_AT_SEQUENCE = 10
TARGET_BACKLOG_RECORDS = 1
TOTAL_DOCUMENTS = 20
PAYLOAD_BYTES = 64 * 1024
DECLARED_INGRESS_RECORDS_PER_SECOND = 1.0
SAMPLE_INTERVAL_SECONDS = 0.05
SELECTED_DISK_HEADROOM_BYTES = 1024 * 1024 * 1024
LIVE_LOGICAL_BYTES_BUDGET = 768 * 1024
LOGICAL_JSON_BYTES_BUDGET = 4 * 1024 * 1024
STALL_ARTIFACT_ID = "acceptance/capacity-stall-return-v1"


@stream_task
async def capacity_stall_return(ctx, payload):
    global STALL_STARTED_AT
    del payload
    if ctx.input_sequence == CALIBRATION_AT_SEQUENCE:
        CALIBRATION_STARTED.set()
        await asyncio.sleep(CALIBRATION_STALL_SECONDS)
        CALIBRATION_RELEASED.set()
    if ctx.input_sequence == STALL_AT_SEQUENCE:
        STALL_STARTED_AT = time.monotonic()
        STALL_STARTED.set()
        released = await asyncio.to_thread(STALL_RELEASED.wait, 20)
        if not released:
            raise TimeoutError("capacity acceptance stall was not released")
    # A declared service cost keeps the selected model explicit.  During normal
    # live ingress the branch is idle between records; after release this yields
    # a measurable service rate above the 1 record/s ingress rate.
    await asyncio.sleep(0.2)
    ctx.managed_state["completed"] = ctx.managed_state.get("completed", 0) + 1
    ctx.managed_state["last_sequence"] = ctx.input_sequence
    if False:  # keep an async generator with a terminal RETURN
        yield


STALL_DIGEST = register_operator_artifact(STALL_ARTIFACT_ID, capacity_stall_return)


def deterministic_payload(number: int) -> bytes:
    # Avoid TOAST compressing a repeated-byte fixture into a misleadingly small
    # physical observation.  This is deterministic and contains no secret data.
    return hashlib.shake_256(f"capacity-document-{number}".encode()).digest(
        PAYLOAD_BYTES
    )


def predict_capacity(
    *,
    calibration_peak: dict[str, Any],
    calibration_recovered: dict[str, Any],
    baseline: dict[str, Any],
    peak: dict[str, Any],
    recovered: dict[str, Any],
    max_records: int,
    max_bytes: int,
) -> dict[str, Any]:
    if (
        baseline["accepted_published_at"] is None
        or peak["accepted_published_at"] is None
    ):
        raise AssertionError("durable source publication witness is missing")
    stall_seconds = peak["accepted_published_at"] - baseline["accepted_published_at"]
    observed_catchup_seconds = recovered["time"] - peak["time"]
    calibration_seconds = calibration_recovered["time"] - calibration_peak["time"]
    if min(stall_seconds, observed_catchup_seconds, calibration_seconds) <= 0:
        raise AssertionError("capacity windows must have positive duration")

    observed_credit_record_growth_rate = (
        peak["used_admission_records"] - baseline["used_admission_records"]
    ) / stall_seconds
    observed_credit_byte_growth_rate = (
        peak["used_admission_bytes"] - baseline["used_admission_bytes"]
    ) / stall_seconds
    if min(observed_credit_record_growth_rate, observed_credit_byte_growth_rate) < 0:
        raise AssertionError("credit use regressed inside the pressure window")
    observed_stall_ingress_rate = (
        peak["accepted"] - baseline["accepted"]
    ) / stall_seconds
    if observed_stall_ingress_rate <= 0:
        raise AssertionError("source did not advance inside the pressure window")

    predicted_record_saturation = (
        max(0, max_records - baseline["used_admission_records"])
        / DECLARED_INGRESS_RECORDS_PER_SECOND
    )
    predicted_byte_saturation = max(0, max_bytes - baseline["used_admission_bytes"]) / (
        DECLARED_INGRESS_RECORDS_PER_SECOND * PAYLOAD_BYTES
    )
    predicted_first_saturation = min(
        predicted_record_saturation, predicted_byte_saturation
    )
    observed_saturation_seconds = (
        0 if predicted_first_saturation == 0 else stall_seconds
    )

    calibrated_gross_rate = (
        calibration_recovered["branch_completed"] - calibration_peak["branch_completed"]
    ) / calibration_seconds
    calibrated_live_ingress_rate = (
        calibration_recovered["accepted"] - calibration_peak["accepted"]
    ) / calibration_seconds
    calibrated_net_rate = calibrated_gross_rate - calibrated_live_ingress_rate
    if calibrated_net_rate <= 0:
        raise AssertionError("calibration does not prove positive net catch-up")

    peak_backlog = peak["accepted"] - peak["branch_completed"]
    recovered_backlog = recovered["accepted"] - recovered["branch_completed"]
    recovery_work = peak_backlog - TARGET_BACKLOG_RECORDS
    if recovery_work <= 0 or recovered_backlog > TARGET_BACKLOG_RECORDS:
        raise AssertionError("target backlog does not prove bounded catch-up")
    observed_gross_rate = (
        recovered["branch_completed"] - peak["branch_completed"]
    ) / observed_catchup_seconds
    observed_live_ingress_rate = (
        recovered["accepted"] - peak["accepted"]
    ) / observed_catchup_seconds
    observed_net_rate = (peak_backlog - recovered_backlog) / observed_catchup_seconds
    if observed_net_rate <= 0:
        raise AssertionError("target branch did not catch up under live ingress")

    relation_growth = max(
        0, peak["physical_relation_bytes"] - baseline["physical_relation_bytes"]
    )
    wal_growth = max(0, peak["wal_bytes"] - baseline["wal_bytes"])
    # This is a selected-run write-amplification observation.  WAL bytes are not
    # claimed to stay resident because PostgreSQL can recycle WAL segments.
    observed_relation_growth_rate = relation_growth / stall_seconds
    observed_wal_insert_rate = wal_growth / stall_seconds
    return {
        "declared_ingress_records_per_second": DECLARED_INGRESS_RECORDS_PER_SECOND,
        "declared_payload_bytes": PAYLOAD_BYTES,
        "pressure_window_seconds": stall_seconds,
        "pressure_accepted_records": peak["accepted"] - baseline["accepted"],
        "pressure_time_authority": "durable admission published_at",
        "observed_stall_ingress_records_per_second": observed_stall_ingress_rate,
        "observed_credit_record_growth_per_second": observed_credit_record_growth_rate,
        "observed_credit_byte_growth_per_second": observed_credit_byte_growth_rate,
        "predicted_seconds_to_record_credit_exhaustion": predicted_record_saturation,
        "predicted_seconds_to_byte_credit_exhaustion": predicted_byte_saturation,
        "predicted_seconds_to_first_logical_saturation": predicted_first_saturation,
        "observed_seconds_to_selected_saturation": observed_saturation_seconds,
        "peak_backlog_records": peak_backlog,
        "target_backlog_records": TARGET_BACKLOG_RECORDS,
        "recovered_backlog_records": recovered_backlog,
        "observed_backlog_reduction_records": peak_backlog - recovered_backlog,
        "calibration_gross_catchup_records_per_second": calibrated_gross_rate,
        "calibration_live_ingress_records_per_second": calibrated_live_ingress_rate,
        "calibration_net_catchup_records_per_second": calibrated_net_rate,
        "predicted_backlog_recovery_seconds": recovery_work / calibrated_net_rate,
        "observed_target_gross_catchup_records_per_second": observed_gross_rate,
        "observed_target_live_ingress_records_per_second": observed_live_ingress_rate,
        "observed_target_net_catchup_records_per_second": observed_net_rate,
        "observed_backlog_recovery_seconds": observed_catchup_seconds,
        "physical_relation_growth_bytes": relation_growth,
        "wal_insert_growth_bytes": wal_growth,
        "observed_relation_growth_bytes_per_second": observed_relation_growth_rate,
        "observed_wal_insert_bytes_per_second": observed_wal_insert_rate,
        "selected_disk_headroom_bytes": SELECTED_DISK_HEADROOM_BYTES,
        "selected_relation_headroom_exhaustion_seconds": (
            SELECTED_DISK_HEADROOM_BYTES / observed_relation_growth_rate
            if observed_relation_growth_rate > 0
            else None
        ),
        "hypothetical_fully_retained_wal_headroom_seconds": (
            SELECTED_DISK_HEADROOM_BYTES / observed_wal_insert_rate
            if observed_wal_insert_rate > 0
            else None
        ),
        "storage_model_limit": (
            "relation headroom uses only observed schema-relation growth; WAL insert "
            "volume is reported separately and its headroom estimate assumes every "
            "inserted WAL byte remains retained. Neither is actual filesystem free "
            "space, vacuum/checkpoint amplification, or long-duration evidence"
        ),
    }


class PostgresCapacityAcceptanceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        global STALL_STARTED_AT
        self.artifact_patch = patch.multiple(
            fixtures,
            RETURN_ONLY_ARTIFACT=STALL_ARTIFACT_ID,
            RETURN_ONLY_DIGEST=STALL_DIGEST,
        )
        self.artifact_patch.start()
        self.addCleanup(self.artifact_patch.stop)
        CALIBRATION_STARTED.clear()
        CALIBRATION_RELEASED.clear()
        STALL_STARTED.clear()
        STALL_RELEASED.clear()
        STALL_STARTED_AT = None
        self.fixture = fixtures.PostgresPrefectAttemptTests("runTest")
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self):
        STALL_RELEASED.set()
        if hasattr(self, "fixture"):
            await self.fixture.asyncTearDown()

    async def sample(self) -> dict[str, Any]:
        f = self.fixture
        footprint = await evaluate_footprint(fixtures.DSN, f.schema)
        async with f.stream.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT p.max_records,p.max_bytes,"
                "r.max_admission_records,r.max_admission_bytes,"
                "p.used_records,p.used_bytes,"
                "r.used_admission_records,r.used_admission_bytes,"
                "COALESCE(pp.accepted_through,0) AS accepted,"
                "EXTRACT(EPOCH FROM a.published_at)::double precision "
                "AS accepted_published_at,"
                "COALESCE(i.completed,0) AS branch_completed,"
                "pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint AS wal_bytes,"
                "(SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0)::bigint "
                " FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                " WHERE n.nspname=current_schema() AND c.relkind IN('r','p')) "
                " AS physical_relation_bytes "
                "FROM partitions p CROSS JOIN control_runs_v1 r "
                "LEFT JOIN producer_progress pp ON pp.channel=p.channel "
                " AND pp.partition_id=p.partition_id AND pp.producer_id='stable-source' "
                "LEFT JOIN control_source_admissions_v1 a ON a.run_id=r.run_id "
                " AND a.channel=p.channel AND a.partition_id=p.partition_id "
                " AND a.producer_id='stable-source' "
                " AND a.producer_sequence=pp.accepted_through "
                " AND a.status='PUBLISHED' "
                "LEFT JOIN join_inputs_v3 i ON i.revision_id=$1 "
                " AND i.participant_id='b' AND i.channel=p.channel "
                " AND i.partition_id=p.partition_id "
                "WHERE p.channel='input' AND p.partition_id='p0' AND r.run_id=$2",
                f.revision.revision_id,
                f.run_id,
            )
            totals = await conn.fetchrow(
                "SELECT COALESCE(sum(used_records),0)::bigint AS records,"
                "COALESCE(sum(used_bytes),0)::bigint AS bytes FROM partitions"
            )
        return {
            "time": time.monotonic(),
            **dict(row),
            "logical_rows": footprint.total_rows,
            "logical_json_bytes": footprint.total_json_bytes,
            "total_partition_used_records": totals["records"],
            "total_partition_used_bytes": totals["bytes"],
        }

    async def wait_for(self, predicate, timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            last = await self.sample()
            if predicate(last):
                return last
            await asyncio.sleep(0.05)
        self.fail(
            "capacity acceptance condition timed out; last="
            + json.dumps(last, sort_keys=True)
        )

    async def test_live_stall_saturates_then_branch_catches_up_with_source_open(self):
        f = self.fixture
        resources = await f.start_prefect_server()
        flow = None
        sampler = None
        sampler_stopped = asyncio.Event()
        samples: list[dict[str, Any]] = []

        async def collect_samples() -> None:
            while not sampler_stopped.is_set():
                samples.append(await self.sample())
                try:
                    await asyncio.wait_for(
                        sampler_stopped.wait(), SAMPLE_INTERVAL_SECONDS
                    )
                except TimeoutError:
                    pass

        try:
            config = f.adapter_config(
                delay=1.0,
                maintenance=True,
                checkpoint_interval=4,
                max_checkpoint_delay=5.0,
            )
            documents = f.prefect_documents(
                *(deterministic_payload(n) for n in range(1, TOTAL_DOCUMENTS + 1))
            )
            sampler = asyncio.create_task(collect_samples())
            flow = asyncio.create_task(
                prefect_stream_flow(asdict(config), documents, return_state=True)
            )
            self.assertTrue(
                await asyncio.to_thread(CALIBRATION_STARTED.wait, 10),
                "independent catch-up calibration did not start",
            )
            calibration_peak = await self.wait_for(
                lambda item: (
                    item["accepted"] - item["branch_completed"] >= 2
                    and item["accepted"] < STALL_AT_SEQUENCE
                ),
                timeout=2.3,
            )
            self.assertTrue(
                await asyncio.to_thread(CALIBRATION_RELEASED.wait, 10),
                "independent catch-up calibration did not release",
            )
            calibration_recovered = await self.wait_for(
                lambda item: (
                    item["branch_completed"] >= item["accepted"]
                    and item["accepted"] >= 4
                    and item["accepted"] < STALL_AT_SEQUENCE
                ),
                timeout=10,
            )
            self.assertTrue(
                await asyncio.to_thread(STALL_STARTED.wait, 20),
                "declared branch stall was not reached",
            )
            self.assertIsNotNone(STALL_STARTED_AT)
            peak = await self.wait_for(
                lambda item: (
                    item["used_admission_records"]
                    == min(item["max_records"], item["max_admission_records"])
                    and item["accepted"] - item["branch_completed"] >= 3
                ),
                timeout=10,
            )
            stall_window = [
                sample
                for sample in [*samples, peak]
                if float(STALL_STARTED_AT) <= sample["time"] <= peak["time"]
            ]
            self.assertTrue(stall_window, "pressure sampler missed the stall window")
            minimum_credit_use = min(
                (sample["used_admission_records"], sample["used_admission_bytes"])
                for sample in stall_window
            )
            # A checkpoint may return credits just after the stalled turn
            # starts.  The pressure window begins at the last observation of
            # the resulting minimum, then ends at the hard-credit peak.
            baseline = max(
                (
                    sample
                    for sample in stall_window
                    if (
                        sample["used_admission_records"],
                        sample["used_admission_bytes"],
                    )
                    == minimum_credit_use
                ),
                key=lambda item: item["time"],
            )
            print(
                "R5_DRAFT_MEASUREMENT_JSON="
                + json.dumps(
                    {
                        "name": "prefect_two_branch_capacity_catchup_short",
                        "calibration_peak": calibration_peak,
                        "calibration_recovered": calibration_recovered,
                        "baseline": baseline,
                        "peak": peak,
                        "target_backlog_records": TARGET_BACKLOG_RECORDS,
                        "status": "pressure_observed_before_release",
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            self.assertFalse(flow.done(), "source closed before pressure was observed")
            STALL_RELEASED.set()
            recovered = await self.wait_for(
                lambda item: (
                    item["accepted"] - item["branch_completed"]
                    <= TARGET_BACKLOG_RECORDS
                    and item["accepted"] > peak["accepted"]
                    and item["accepted"] < TOTAL_DOCUMENTS
                ),
                timeout=10,
            )
            self.assertFalse(flow.done(), "source was not live during catch-up")
            state = await asyncio.wait_for(flow, 90)
            self.assertEqual(state.type, StateType.COMPLETED)
            result = await state.result()
            final = await self.sample()
            self.assertEqual(final["accepted"], TOTAL_DOCUMENTS)
            async with f.stream.pool.acquire() as conn:
                completed = await conn.fetch(
                    "SELECT participant_id,state FROM join_workers "
                    "WHERE revision_id=$1 ORDER BY participant_id",
                    f.revision.revision_id,
                )
            self.assertEqual(
                [json.loads(row["state"])["completed"] for row in completed],
                [TOTAL_DOCUMENTS, TOTAL_DOCUMENTS],
            )

            model = predict_capacity(
                calibration_peak=calibration_peak,
                calibration_recovered=calibration_recovered,
                baseline=baseline,
                peak=peak,
                recovered=recovered,
                max_records=min(peak["max_records"], peak["max_admission_records"]),
                max_bytes=min(peak["max_bytes"], peak["max_admission_bytes"]),
            )
            self.assertGreater(model["calibration_net_catchup_records_per_second"], 0)
            self.assertGreater(
                model["observed_target_net_catchup_records_per_second"], 0
            )
            self.assertGreater(model["predicted_backlog_recovery_seconds"], 0)
            self.assertGreater(model["observed_stall_ingress_records_per_second"], 0)
            self.assertGreaterEqual(
                model["pressure_window_seconds"],
                model["pressure_accepted_records"]
                / DECLARED_INGRESS_RECORDS_PER_SECOND,
            )
            self.assertGreaterEqual(model["observed_backlog_reduction_records"], 2)
            self.assertGreaterEqual(
                model["observed_seconds_to_selected_saturation"],
                max(
                    0,
                    model["predicted_seconds_to_first_logical_saturation"] * 0.25 - 0.5,
                ),
            )
            self.assertLessEqual(
                model["observed_seconds_to_selected_saturation"],
                model["predicted_seconds_to_first_logical_saturation"] * 2 + 1,
            )
            self.assertLessEqual(
                model["observed_backlog_recovery_seconds"],
                model["predicted_backlog_recovery_seconds"] * 2 + 0.5,
            )
            self.assertGreaterEqual(peak["used_admission_records"], 4)
            self.assertLessEqual(
                peak["total_partition_used_bytes"], LIVE_LOGICAL_BYTES_BUDGET
            )
            self.assertLessEqual(peak["logical_rows"], 512)
            self.assertLessEqual(peak["logical_json_bytes"], LOGICAL_JSON_BYTES_BUDGET)
            self.assertLessEqual(
                final["total_partition_used_bytes"], LIVE_LOGICAL_BYTES_BUDGET
            )
            self.assertLessEqual(final["logical_rows"], 512)
            self.assertLessEqual(final["logical_json_bytes"], LOGICAL_JSON_BYTES_BUDGET)

            for oracle in (
                evaluate_stream,
                evaluate_join,
                evaluate_barrier,
                evaluate_control,
                evaluate_attempt,
            ):
                report = await oracle(fixtures.DSN, f.schema)
                passed = (
                    report.passed if hasattr(report, "passed") else report["passed"]
                )
                self.assertTrue(passed, report)
            maintenance_report = await evaluate_maintenance(
                fixtures.DSN, f.schema, f.revision.revision_id
            )
            self.assertTrue(maintenance_report["passed"], maintenance_report)

            print(
                "R5_MEASUREMENT_JSON="
                + json.dumps(
                    {
                        "name": "prefect_two_branch_capacity_catchup_short",
                        "documents": TOTAL_DOCUMENTS,
                        "payload_bytes": PAYLOAD_BYTES,
                        "stall_participant": "b",
                        "stall_at_sequence": STALL_AT_SEQUENCE,
                        "source_remained_open_during_catchup": True,
                        "checkpoint_interval_records": 4,
                        "max_checkpoint_delay_seconds": 5.0,
                        "prefect_task_ids": {
                            "source": result["source_task_run_id"],
                            "operators": result["operator_task_run_ids"],
                            "maintenance": result["maintenance_task_run_id"],
                        },
                        "baseline": baseline,
                        "calibration_peak": calibration_peak,
                        "calibration_recovered": calibration_recovered,
                        "peak": peak,
                        "recovered": recovered,
                        "final": final,
                        "prediction": model,
                        "declared_live_logical_bytes_budget": LIVE_LOGICAL_BYTES_BUDGET,
                        "declared_logical_json_bytes_budget": LOGICAL_JSON_BYTES_BUDGET,
                        "long_soak_passed": False,
                        "scope": (
                            "single source, two independent terminal branches, real Prefect "
                            "tasks, PostgreSQL barriers/retention, 1 record/s declared ingress, "
                            "one bounded in-process branch stall; no production disk, WAL "
                            "retention, vacuum, multi-tenant, or 72-hour capacity claim"
                        ),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        finally:
            STALL_RELEASED.set()
            sampler_stopped.set()
            if sampler is not None:
                await sampler
            if flow is not None and not flow.done():
                flow.cancel()
                await asyncio.gather(flow, return_exceptions=True)
            await f.stop_prefect_server(resources)


class CapacityPredictionTests(unittest.TestCase):
    @staticmethod
    def point(seconds, accepted, completed, records, size, physical, wal):
        return {
            "time": float(seconds),
            "accepted": accepted,
            "accepted_published_at": float(seconds),
            "branch_completed": completed,
            "used_admission_records": records,
            "used_admission_bytes": size,
            "physical_relation_bytes": physical,
            "wal_bytes": wal,
        }

    def test_prediction_uses_prior_calibration_not_target_outcome(self):
        model = predict_capacity(
            calibration_peak=self.point(0, 4, 1, 3, 196_608, 1_000, 2_000),
            calibration_recovered=self.point(1, 5, 5, 1, 65_536, 1_100, 2_100),
            baseline=self.point(2, 6, 5, 1, 65_536, 1_100, 2_100),
            peak=self.point(5, 9, 5, 4, 262_144, 1_400, 2_700),
            recovered=self.point(6, 10, 9, 1, 65_536, 1_500, 2_900),
            max_records=4,
            max_bytes=262_144,
        )
        self.assertEqual(model["calibration_net_catchup_records_per_second"], 3)
        self.assertEqual(model["predicted_backlog_recovery_seconds"], 1)
        self.assertEqual(model["observed_backlog_recovery_seconds"], 1)

    def test_nonpositive_calibrated_net_rate_is_not_a_prediction(self):
        with self.assertRaisesRegex(AssertionError, "positive net catch-up"):
            predict_capacity(
                calibration_peak=self.point(0, 4, 2, 2, 131_072, 1_000, 2_000),
                calibration_recovered=self.point(2, 6, 4, 1, 65_536, 1_100, 2_100),
                baseline=self.point(3, 7, 4, 1, 65_536, 1_100, 2_100),
                peak=self.point(5, 9, 4, 3, 196_608, 1_300, 2_500),
                recovered=self.point(6, 10, 10, 1, 65_536, 1_400, 2_700),
                max_records=4,
                max_bytes=262_144,
            )


class FutureCompletionWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_state_is_never_polled_and_failure_still_propagates(self):
        class FailedState:
            @staticmethod
            def is_failed():
                return True

            @staticmethod
            def is_crashed():
                return False

        class FakeFuture:
            def __init__(self):
                self.callbacks = []
                self.completed = False
                self.waited = False
                self.state_accesses = 0

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def wait(self):
                if not self.completed:
                    raise AssertionError("wait called before completion")
                self.waited = True

            @property
            def state(self):
                self.state_accesses += 1
                if not self.waited:
                    raise AssertionError("pending state was polled")
                return FailedState()

            def result(self):
                raise RuntimeError("actor failed")

            def complete(self):
                self.completed = True
                for callback in self.callbacks:
                    callback(self)

        future = FakeFuture()
        watcher = _FutureCompletionWatcher(future)
        self.assertIsNone(watcher.final_state_if_done())
        self.assertEqual(future.state_accesses, 0)

        future.complete()
        await watcher.wait_done()
        state = watcher.final_state_if_done()
        self.assertTrue(state.is_failed())
        self.assertEqual(future.state_accesses, 1)
        with self.assertRaisesRegex(RuntimeError, "actor failed"):
            future.result()


class MaintenanceBusyRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_active_invocation_keeps_exact_checkpoint_for_retry(self):
        calls: list[tuple[str, str]] = []
        blocked = True

        async def retire_latest(_revision_id, participant_id, operation_id):
            nonlocal blocked
            calls.append((participant_id, operation_id))
            if participant_id == "b" and blocked:
                blocked = False
                raise JoinRetirementBusy("join retirement has an active invocation")

        maintenance = object.__new__(PostgresPrefectMaintenance)
        maintenance.revision = SimpleNamespace(
            revision_id="revision",
            graph_version="v1",
            graph_digest="graph-digest",
            participant_manifest=[
                {"participant_id": "a"},
                {"participant_id": "b"},
            ],
        )
        maintenance._live_completed_count = AsyncMock(return_value=2)
        maintenance.coordinator = SimpleNamespace(
            claim_coordinator=AsyncMock(return_value=7)
        )
        maintenance.join_retention = SimpleNamespace(retire_latest=retire_latest)
        maintenance.checkpoint_retention = SimpleNamespace(retire_prefix=AsyncMock())
        maintenance.streaming_retention = SimpleNamespace(compact_latest=AsyncMock())
        maintenance.control = SimpleNamespace(retire_admissions=AsyncMock())

        epoch, retired = await maintenance._retire(
            "coordinator",
            7,
            "checkpoint",
            5.0,
            "admin-token",
            "run",
            "leader",
            3,
        )
        self.assertEqual((epoch, retired), (7, False))
        maintenance.checkpoint_retention.retire_prefix.assert_not_awaited()
        maintenance.streaming_retention.compact_latest.assert_not_awaited()
        maintenance.control.retire_admissions.assert_not_awaited()

        epoch, retired = await maintenance._retire(
            "coordinator",
            epoch,
            "checkpoint",
            5.0,
            "admin-token",
            "run",
            "leader",
            3,
        )
        self.assertEqual((epoch, retired), (7, True))
        self.assertEqual(
            calls,
            [
                ("a", "prefect-join-retire:checkpoint:a"),
                ("b", "prefect-join-retire:checkpoint:b"),
                ("a", "prefect-join-retire:checkpoint:a"),
                ("b", "prefect-join-retire:checkpoint:b"),
            ],
        )
        maintenance.checkpoint_retention.retire_prefix.assert_awaited_once()
        maintenance.streaming_retention.compact_latest.assert_awaited_once()
        maintenance.control.retire_admissions.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
