"""Bounded checkpoint/retention loop for the Prefect streaming adapter."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from postgres_barrier_coordinator import PostgresBarrierCoordinator, Revision, decoded
from postgres_checkpoint_retention import PostgresCheckpointRetention
from postgres_control_plane import PostgresControlPlane
from postgres_join_retention import PostgresJoinRetention
from postgres_stream_v2 import PostgresProtocolError, PostgresStream
from postgres_streaming_retention import PostgresStreamingRetention


@dataclass(frozen=True)
class MaintenanceResult:
    checkpoints_completed: int
    last_source_cut: int


class PostgresPrefectMaintenance:
    """Drive global barriers and the existing bounded-retention protocols.

    This is a platform daemon role.  It creates no second progress authority:
    checkpoints, roots, compaction outcomes, and admission credits stay in the
    R3/R4 PostgreSQL protocol tables.
    """

    def __init__(
        self,
        stream: PostgresStream,
        coordinator: PostgresBarrierCoordinator,
        control: PostgresControlPlane,
        revision: Revision,
    ):
        self.stream = stream
        self.coordinator = coordinator
        self.control = control
        self.revision = revision
        self.join_retention = PostgresJoinRetention(coordinator)
        self.checkpoint_retention = PostgresCheckpointRetention(coordinator)
        self.streaming_retention = PostgresStreamingRetention(stream, coordinator)

    def _coordinator_args(self, owner_id: str, owner_epoch: int) -> tuple[object, ...]:
        return (
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            owner_id,
            owner_epoch,
        )

    async def _source_frontier(self, source: dict[str, str]) -> int:
        async with self.stream.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "SELECT next_sequence-1 FROM partitions WHERE channel=$1 "
                    "AND partition_id=$2",
                    source["channel"],
                    source["partition"],
                )
                or 0
            )

    async def _last_completed_cut(self, source: dict[str, str]) -> int:
        async with self.stream.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "SELECT COALESCE(max(o.cut_data_sequence),0) "
                    "FROM source_barrier_observations o "
                    "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                    "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                    "WHERE i.revision_id=$1 AND o.channel=$2 AND o.partition_id=$3 "
                    "AND l.status='COMPLETED'",
                    self.revision.revision_id,
                    source["channel"],
                    source["partition"],
                )
                or 0
            )

    async def _live_completed_count(self) -> int:
        async with self.stream.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "SELECT count(*) FROM checkpoint_intents_v2 i "
                    "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                    "WHERE i.revision_id=$1 AND l.status='COMPLETED'",
                    self.revision.revision_id,
                )
            )

    async def _has_retirement_anchor(self) -> bool:
        expected = sorted(
            item["participant_id"] for item in self.revision.participant_manifest
        )
        async with self.stream.pool.acquire() as conn:
            actual = await conn.fetchval(
                "SELECT COALESCE(array_agg(participant_id ORDER BY participant_id),"
                "ARRAY[]::text[]) FROM join_retention_anchors_v1 "
                "WHERE revision_id=$1",
                self.revision.revision_id,
            )
        return list(actual) == expected

    async def _oldest_uncheckpointed_age(
        self, source: dict[str, str], last_cut: int
    ) -> float | None:
        async with self.stream.pool.acquire() as conn:
            age = await conn.fetchval(
                "SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)) "
                "FROM records WHERE channel=$1 AND partition_id=$2 AND sequence>$3",
                source["channel"],
                source["partition"],
                last_cut,
            )
        return None if age is None else float(age)

    async def _latest_completed_checkpoint(self) -> str | None:
        async with self.stream.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT i.checkpoint_id FROM checkpoint_intents_v2 i "
                "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                "WHERE i.revision_id=$1 AND l.status='COMPLETED' "
                "ORDER BY i.checkpoint_ordinal DESC LIMIT 1",
                self.revision.revision_id,
            )

    async def _pending_checkpoint(self) -> str | None:
        async with self.stream.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT i.checkpoint_id FROM checkpoint_intents_v2 i "
                "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                "WHERE i.revision_id=$1 "
                "AND l.status NOT IN('COMPLETED','ABORTED','RETIRED') "
                "ORDER BY i.checkpoint_ordinal",
                self.revision.revision_id,
            )
        if len(rows) > 1:
            raise PostgresProtocolError("multiple active checkpoints")
        return rows[0]["checkpoint_id"] if rows else None

    async def _complete_checkpoint(
        self,
        owner_id: str,
        owner_epoch: int,
        source: dict[str, str],
        request_id: str,
        poll_seconds: float,
        coordinator_lease_seconds: float,
        admin_token: str,
        control_run_id: str,
        fault: Callable[[str], None] | None,
    ) -> tuple[str | None, int]:
        pending = await self._pending_checkpoint()
        if pending is None:
            checkpoint = await self.coordinator.start_checkpoint(
                *self._coordinator_args(owner_id, owner_epoch), request_id
            )
            checkpoint_id = checkpoint.checkpoint_id
        else:
            checkpoint_id = pending
        await self.coordinator.inject_source_barrier(
            *self._coordinator_args(owner_id, owner_epoch),
            checkpoint_id,
            source["channel"],
            source["partition"],
        )
        if fault is not None:
            fault("after_barrier")
        expected = len(self.revision.participant_manifest)
        loop = asyncio.get_running_loop()
        renew_at = loop.time() + coordinator_lease_seconds / 3
        while True:
            async with self.stream.pool.acquire() as conn:
                acknowledged = await conn.fetchval(
                    "SELECT count(*) FROM participant_checkpoint_acks "
                    "WHERE checkpoint_id=$1",
                    checkpoint_id,
                )
            if acknowledged == expected:
                break
            lifecycle = (
                await self.control.project_run(admin_token, run_id=control_run_id)
            )["control"]["lifecycle"]
            if lifecycle == "CANCELLED":
                return None, owner_epoch
            if loop.time() >= renew_at:
                effective_epoch = await self.coordinator.claim_coordinator(
                    self.revision.revision_id,
                    self.revision.graph_version,
                    self.revision.graph_digest,
                    owner_id,
                    lease_seconds=coordinator_lease_seconds,
                )
                if effective_epoch < owner_epoch:
                    raise PostgresProtocolError("coordinator epoch regressed")
                owner_epoch = effective_epoch
                renew_at = loop.time() + coordinator_lease_seconds / 3
            await asyncio.sleep(poll_seconds)
        await self.coordinator.complete_checkpoint(
            *self._coordinator_args(owner_id, owner_epoch), checkpoint_id
        )
        return checkpoint_id, owner_epoch

    async def _retire(
        self,
        owner_id: str,
        owner_epoch: int,
        checkpoint_id: str,
        coordinator_lease_seconds: float,
        admin_token: str,
        control_run_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> int:
        # The first snapshot cannot retire its own mailbox/log evidence.  A
        # second completed snapshot at the same or a newer frontier is the
        # initial durable anchor.
        if await self._live_completed_count() < 2:
            return owner_epoch
        for participant in self.revision.participant_manifest:
            owner_epoch = await self.coordinator.claim_coordinator(
                self.revision.revision_id,
                self.revision.graph_version,
                self.revision.graph_digest,
                owner_id,
                lease_seconds=coordinator_lease_seconds,
            )
            participant_id = participant["participant_id"]
            await self.join_retention.retire_latest(
                self.revision.revision_id,
                participant_id,
                f"prefect-join-retire:{checkpoint_id}:{participant_id}",
            )
        owner_epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            owner_id,
            lease_seconds=coordinator_lease_seconds,
        )
        args = self._coordinator_args(owner_id, owner_epoch)
        await self.checkpoint_retention.retire_prefix(
            *args, f"prefect-checkpoint-retire:{checkpoint_id}"
        )
        owner_epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            owner_id,
            lease_seconds=coordinator_lease_seconds,
        )
        args = self._coordinator_args(owner_id, owner_epoch)
        await self.streaming_retention.compact_latest(
            *args, f"prefect-stream-retire:{checkpoint_id}"
        )
        await self.control.retire_admissions(
            admin_token,
            run_id=control_run_id,
            leader_id=leader_id,
            leader_epoch=leader_epoch,
        )
        return owner_epoch

    async def run(
        self,
        *,
        admin_token: str,
        control_run_id: str,
        source_producer_id: str,
        coordinator_owner: str,
        coordinator_epoch: int,
        coordinator_lease_seconds: float,
        leader_id: str,
        leader_epoch: int,
        checkpoint_interval_records: int,
        max_checkpoint_delay_seconds: float,
        poll_seconds: float,
        fault: Callable[[str], None] | None = None,
    ) -> MaintenanceResult:
        if (
            checkpoint_interval_records <= 0
            or max_checkpoint_delay_seconds <= 0
            or poll_seconds <= 0
        ):
            raise ValueError("positive checkpoint interval and poll required")
        source = dict(self.revision.source_manifest[0])
        source["producer_id"] = source_producer_id
        completed = 0
        last_cut = await self._last_completed_cut(source)
        last_retired_checkpoint_id: str | None = None
        while True:
            effective_epoch = await self.coordinator.claim_coordinator(
                self.revision.revision_id,
                self.revision.graph_version,
                self.revision.graph_digest,
                coordinator_owner,
                lease_seconds=coordinator_lease_seconds,
            )
            if effective_epoch < coordinator_epoch:
                raise PostgresProtocolError("coordinator epoch regressed")
            coordinator_epoch = effective_epoch
            projection = await self.control.project_run(
                admin_token, run_id=control_run_id
            )
            lifecycle = projection["control"]["lifecycle"]
            if lifecycle == "CANCELLED":
                return MaintenanceResult(completed, last_cut)
            if lifecycle != "ACTIVE":
                return MaintenanceResult(completed, last_cut)
            latest_completed = await self._latest_completed_checkpoint()
            if (
                latest_completed is not None
                and latest_completed != last_retired_checkpoint_id
                and await self._live_completed_count() >= 2
            ):
                coordinator_epoch = await self._retire(
                    coordinator_owner,
                    coordinator_epoch,
                    latest_completed,
                    coordinator_lease_seconds,
                    admin_token,
                    control_run_id,
                    leader_id,
                    leader_epoch,
                )
                last_retired_checkpoint_id = latest_completed
            frontier = await self._source_frontier(source)
            needs_data_checkpoint = frontier - last_cut >= checkpoint_interval_records
            tail_age = await self._oldest_uncheckpointed_age(source, last_cut)
            needs_delayed_checkpoint = (
                frontier > last_cut
                and tail_age is not None
                and tail_age >= max_checkpoint_delay_seconds
            )
            needs_bootstrap_anchor = (
                frontier > 0
                and await self._live_completed_count() == 1
                and last_cut == frontier
                and not await self._has_retirement_anchor()
            )
            if (
                needs_data_checkpoint
                or needs_delayed_checkpoint
                or needs_bootstrap_anchor
            ):
                phase = (
                    "anchor"
                    if needs_bootstrap_anchor
                    else "delay"
                    if needs_delayed_checkpoint and not needs_data_checkpoint
                    else "data"
                )
                checkpoint_id, coordinator_epoch = await self._complete_checkpoint(
                    coordinator_owner,
                    coordinator_epoch,
                    source,
                    f"prefect-maint:{control_run_id}:{frontier}:{phase}",
                    poll_seconds,
                    coordinator_lease_seconds,
                    admin_token,
                    control_run_id,
                    fault,
                )
                if checkpoint_id is None:
                    return MaintenanceResult(completed, last_cut)
                completed += 1
                last_cut = await self._last_completed_cut(source)
                if fault is not None:
                    fault("after_checkpoint")
                coordinator_epoch = await self._retire(
                    coordinator_owner,
                    coordinator_epoch,
                    checkpoint_id,
                    coordinator_lease_seconds,
                    admin_token,
                    control_run_id,
                    leader_id,
                    leader_epoch,
                )
                last_retired_checkpoint_id = checkpoint_id
                continue
            await asyncio.sleep(poll_seconds)


def revision_from_row(row) -> Revision:
    return Revision(
        row["revision_id"],
        row["deployment_id"],
        row["graph_version"],
        row["graph_digest"],
        decoded(row["source_manifest"]),
        decoded(row["participant_manifest"]),
    )
