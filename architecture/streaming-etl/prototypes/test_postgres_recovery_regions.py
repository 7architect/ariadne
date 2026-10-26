from __future__ import annotations

import os
import unittest
import uuid
from typing import ClassVar

import asyncpg
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_recovery_regions import (
    PostgresRecoveryRegionPlanner,
    RegionEdge,
    RegionNode,
    RegionPlanningError,
    UnrecoverableBoundary,
)
from postgres_stream_v2 import PostgresStream

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)


class PostgresRecoveryRegionTests(unittest.IsolatedAsyncioTestCase):
    LOCATIONS: ClassVar[dict[str, tuple[str, str]]] = {
        # A fan-out shares one physical source partition.  The planner keeps
        # separate logical edges and one exact physical cut.
        "e_ab": ("source", "left"),
        "e_ac": ("source", "left"),
        "e_bj": ("edge-bj", "p0"),
        "e_cj": ("source", "right"),
        "e_jd": ("edge-jd", "p0"),
        "e_ax": ("edge-ax", "p0"),
        "e_xy": ("edge-xy", "p0"),
    }

    async def asyncSetUp(self) -> None:
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds031c_" + uuid.uuid4().hex[:12]
        self.stream = await PostgresStream.open(DSN, self.schema)
        for channel, partition in sorted(set(self.LOCATIONS.values())):
            await self.stream.create_partition(
                channel,
                partition,
                max_records=100,
                max_bytes=1_000_000,
                max_metadata_records=1_000,
            )
        self.coordinator = await PostgresBarrierCoordinator.install(self.stream)
        participants = [
            self._participant("B", [("source", "left")]),
            self._participant("C", [("source", "left")]),
            self._participant("J", [("source", "left"), ("source", "right")]),
            self._participant("D", [("source", "right")]),
            self._participant("X", [("source", "left")]),
            self._participant("Y", [("source", "right")]),
        ]
        self.revision = await self.coordinator.register_revision(
            revision_id="revision-region-v1",
            deployment_id="deployment-region",
            graph_version="graph-v1",
            sources=[("source", "left"), ("source", "right")],
            participants=participants,
            code_manifest_digest="region-code-v1",
            state_contract_digest="region-state-v1",
            max_inflight_checkpoints=4,
            max_retained_checkpoints=20,
            max_checkpoint_metadata_bytes=20_000_000,
            max_authority_events=1_000,
        )
        self.owner = "region-coordinator"
        self.epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            self.owner,
            lease_seconds=300,
        )
        self.planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        for location in (("source", "left"), ("source", "right")):
            await self._append_location(*location, 1)
        self.checkpoint = await self._complete_checkpoint("checkpoint-one", 1)
        for location in (("source", "left"), ("source", "right")):
            await self._append_location(*location, 2)

    async def asyncTearDown(self) -> None:
        if not hasattr(self, "stream"):
            return
        await self.stream.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    def _participant(
        self, participant_id: str, locations: list[tuple[str, str]]
    ) -> dict:
        return {
            "participant_id": participant_id,
            "inputs": [
                {"channel": channel, "partition": partition}
                for channel, partition in locations
            ],
            # Routed output barriers belong to DS-031a and are deliberately
            # absent from this isolated planner fixture.
            "outputs": [],
            "code_digest": f"code-{participant_id}-v1",
            "state_schema": f"state-{participant_id}/v1",
            "state_version": 1,
        }

    def _common(self) -> tuple[str, str, str, str, int]:
        return (
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            self.owner,
            self.epoch,
        )

    async def _append_location(
        self, channel: str, partition: str, sequence: int
    ) -> None:
        identity = f"{channel}-{partition}"
        payload = f"{identity}-payload-{sequence}".encode()
        await self.stream.append(
            f"{identity}-attempt-{sequence}",
            channel,
            partition,
            f"{identity}-record-{sequence}",
            f"{identity}-object-{sequence}",
            payload,
            "bytes/v1",
            f"{identity}-lineage-{sequence}",
        )

    async def _complete_checkpoint(self, request_id: str, highwater: int):
        checkpoint = await self.coordinator.start_checkpoint(
            *self._common(), request_id
        )
        for channel, partition in (("source", "left"), ("source", "right")):
            await self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, channel, partition
            )
        for participant in self.revision.participant_manifest:
            await self.coordinator.acknowledge_participant(
                *self._common(),
                checkpoint.checkpoint_id,
                participant["participant_id"],
                {"participant": participant["participant_id"], "value": highwater},
                mailbox_highwater=highwater,
                state_version=1,
            )
        await self.coordinator.complete_checkpoint(
            *self._common(), checkpoint.checkpoint_id
        )
        return checkpoint

    async def _declare(
        self, *, source_replayable: bool = False, nonreplayable: set[str] | None = None
    ) -> str:
        nonreplayable = set() if nonreplayable is None else nonreplayable
        nodes = [
            RegionNode("A", "SOURCE", source_replayable),
            *(RegionNode(node) for node in ("B", "C", "J", "D", "X", "Y")),
        ]
        endpoints = {
            "e_ab": ("A", "B"),
            "e_ac": ("A", "C"),
            "e_bj": ("B", "J"),
            "e_cj": ("C", "J"),
            "e_jd": ("J", "D"),
            "e_ax": ("A", "X"),
            "e_xy": ("X", "Y"),
        }
        edges = [
            RegionEdge(
                edge_id,
                endpoints[edge_id][0],
                endpoints[edge_id][1],
                self.LOCATIONS[edge_id][0],
                self.LOCATIONS[edge_id][1],
                edge_id not in nonreplayable,
            )
            for edge_id in sorted(endpoints)
        ]
        return await self.planner.declare_graph(self.revision.revision_id, nodes, edges)

    async def _bind(self, *edge_ids: str, checkpoint_id: str | None = None) -> None:
        checkpoint_id = checkpoint_id or self.checkpoint.checkpoint_id
        for edge_id in edge_ids:
            await self.planner.bind_edge_cut(
                self.revision.revision_id, checkpoint_id, edge_id
            )

    async def _plan(self, request_id: str, failed_node: str = "B"):
        return await self.planner.create_plan(
            *self._common(), request_id, failed_node, self.checkpoint.checkpoint_id
        )

    async def test_available_boundaries_keep_healthy_siblings_outside(self) -> None:
        await self._declare()
        await self._bind("e_ab", "e_cj")
        plan = await self._plan("available-boundaries")
        self.assertEqual(plan.region, ("B", "D", "J"))
        self.assertEqual(
            tuple(boundary.edge_id for boundary in plan.boundaries),
            ("e_ab", "e_cj"),
        )
        for boundary in plan.boundaries:
            self.assertEqual(
                (
                    boundary.cut_log_sequence,
                    boundary.cut_data_sequence,
                    boundary.target_log_sequence,
                    boundary.target_data_sequence,
                ),
                (2, 1, 3, 2),
            )
        self.assertNotIn("C", plan.region)
        self.assertNotIn("X", plan.region)
        self.assertNotIn("Y", plan.region)
        retry = await self._plan("available-boundaries")
        self.assertEqual(retry.plan_digest, plan.plan_digest)
        with self.assertRaisesRegex(RegionPlanningError, "identity drift"):
            await self.planner.create_plan(
                *self._common(),
                "available-boundaries",
                "C",
                self.checkpoint.checkpoint_id,
            )

    async def test_declared_policy_boundary_expands_producer_not_unrelated_siblings(
        self,
    ) -> None:
        await self._declare(nonreplayable={"e_cj"})
        await self._bind("e_ab", "e_ac", "e_cj")
        plan = await self._plan("expand-c")
        self.assertEqual(plan.region, ("B", "C", "D", "J"))
        self.assertEqual(
            tuple(boundary.edge_id for boundary in plan.boundaries),
            ("e_ab", "e_ac"),
        )
        self.assertIn("unavailable edge e_cj", plan.reasons["C"])
        self.assertNotIn("A", plan.region)
        self.assertNotIn("X", plan.region)
        self.assertNotIn("Y", plan.region)

    async def test_missing_boundary_authority_is_named_outside_retained_horizon(
        self,
    ) -> None:
        await self._declare()
        await self._bind("e_ab", "e_ac")
        with self.assertRaisesRegex(
            UnrecoverableBoundary,
            "outside the retained durable horizon.*edge cut is unavailable",
        ):
            await self._plan("missing-edge-authority")
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1
                       WHERE request_id='missing-edge-authority')"""
                )
            )
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM coordinator_authority_events
                       WHERE payload->>'request_id'='missing-edge-authority')"""
                )
            )

    async def test_recursive_expansion_rewinds_source_or_fails_explicitly(self) -> None:
        await self._declare(source_replayable=True, nonreplayable={"e_ac", "e_cj"})
        await self._bind("e_ab", "e_ac", "e_cj")
        plan = await self._plan("rewind-source")
        self.assertEqual(plan.region, ("A", "B", "C", "D", "J"))
        self.assertEqual(plan.source_rewinds, ("A",))
        self.assertEqual(plan.boundaries, ())
        self.assertNotIn("X", plan.region)
        self.assertNotIn("Y", plan.region)

    async def test_nonrewindable_source_is_named_unrecoverable(self) -> None:
        await self._declare(source_replayable=False, nonreplayable={"e_ac", "e_cj"})
        await self._bind("e_ab", "e_ac", "e_cj")
        with self.assertRaisesRegex(
            UnrecoverableBoundary, "source A has no rewind contract"
        ):
            await self._plan("cannot-rewind")
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1
                       WHERE request_id='cannot-rewind')"""
                )
            )
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM coordinator_authority_events
                       WHERE payload->>'request_id'='cannot-rewind')"""
                )
            )

    async def test_corrupt_declared_boundary_refuses_before_plan_seal(self) -> None:
        await self._declare(nonreplayable={"e_cj"})
        await self._bind("e_ab", "e_ac", "e_cj")
        channel, partition = self.LOCATIONS["e_cj"]
        async with self.stream.pool.acquire() as conn:
            object_key = await conn.fetchval(
                """SELECT object_key FROM records
                   WHERE channel=$1 AND partition_id=$2 AND sequence=2""",
                channel,
                partition,
            )
            await conn.execute(
                "UPDATE objects SET payload=$2 WHERE object_key=$1",
                object_key,
                b"corrupt-payload",
            )
        with self.assertRaisesRegex(
            UnrecoverableBoundary, "outside the retained durable horizon.*corrupt"
        ):
            await self._plan("corrupt-tail")
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1
                       WHERE request_id='corrupt-tail')"""
                )
            )
            self.assertFalse(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM coordinator_authority_events
                       WHERE payload->>'request_id'='corrupt-tail')"""
                )
            )

    async def test_mutated_replay_root_cannot_authorize_boundary(self) -> None:
        await self._declare()
        await self._bind("e_ab", "e_ac", "e_cj")
        async with self.stream.pool.acquire() as conn:
            root_id = await conn.fetchval(
                """SELECT retention_ref_id FROM region_edge_cuts_v1
                   WHERE checkpoint_id=$1 AND edge_id='e_cj'""",
                self.checkpoint.checkpoint_id,
            )
            # Simulate storage corruption outside the public protocol.  The
            # planner must revalidate the root rather than trust insert-time
            # edge-cut admission.
            await conn.execute(
                "UPDATE checkpoints SET cut_sequence=0 WHERE checkpoint_id=$1",
                root_id,
            )
        with self.assertRaisesRegex(
            UnrecoverableBoundary,
            "outside the retained durable horizon.*replay root is unavailable",
        ):
            await self._plan("mutated-root")

    async def test_storage_rejects_graph_cut_and_plan_authority_forgery(self) -> None:
        graph_digest = await self._declare()
        await self._bind("e_ab", "e_cj")
        plan = await self._plan("storage-authority")
        async with self.stream.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE region_graph_nodes_v1 SET node_kind='SOURCE'
                       WHERE revision_id=$1 AND node_id='B'""",
                    self.revision.revision_id,
                )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE region_edge_cuts_v1 SET cut_data_sequence=0
                       WHERE checkpoint_id=$1 AND edge_id='e_ab'""",
                    self.checkpoint.checkpoint_id,
                )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE region_recovery_plans_v1 SET failed_node='C'
                       WHERE plan_id=$1""",
                    plan.plan_id,
                )
            completion = await conn.fetchrow(
                """SELECT * FROM checkpoint_completions_v2 WHERE checkpoint_id=$1""",
                self.checkpoint.checkpoint_id,
            )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """INSERT INTO region_recovery_plans_v1(
                       plan_id,request_id,revision_id,checkpoint_id,failed_node,
                       graph_manifest_digest,completion_digest,
                       completion_authority_ordinal,fence_authority_ordinal,
                       writer_epoch,region_json,boundaries_json,
                       source_rewinds_json,reasons_json,plan_digest)
                       VALUES('forged-plan','forged-request',$1,$2,'B',$3,$4,$5,$5,
                              $6,'[]'::jsonb,'[]'::jsonb,'[]'::jsonb,
                              '{}'::jsonb,'forged-digest')""",
                    self.revision.revision_id,
                    self.checkpoint.checkpoint_id,
                    graph_digest,
                    completion["completion_digest"],
                    completion["completion_authority_ordinal"],
                    self.epoch,
                )

    async def test_mixed_checkpoint_edge_cut_is_rejected_by_storage(self) -> None:
        await self._declare()
        second = await self._complete_checkpoint("checkpoint-two", 2)
        channel, partition = self.LOCATIONS["e_ac"]
        async with self.stream.pool.acquire() as conn:
            barrier = await conn.fetchrow(
                """SELECT * FROM partition_log_entries
                   WHERE channel=$1 AND partition_id=$2 AND checkpoint_id=$3""",
                channel,
                partition,
                second.checkpoint_id,
            )
            root_id = "forged-mixed-root"
            await conn.execute(
                "INSERT INTO checkpoints VALUES($1,$2,$3,$4,'SEALED')",
                root_id,
                channel,
                partition,
                barrier["cut_data_sequence"],
            )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """INSERT INTO region_edge_cuts_v1 VALUES(
                       $1,$2,'e_ac',$3,$4,$5,$6,$7,$8,'forged-cut')""",
                    self.checkpoint.checkpoint_id,
                    self.revision.revision_id,
                    channel,
                    partition,
                    barrier["log_sequence"],
                    barrier["cut_data_sequence"],
                    barrier["intent_digest"],
                    root_id,
                )


if __name__ == "__main__":
    unittest.main()
