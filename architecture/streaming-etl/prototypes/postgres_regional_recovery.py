"""Focused DS-031b regional recovery composition for PostgreSQL prototypes.

The module derives one immutable recovery graph from `revision_edges`, seals
a selective plan with `PostgresRecoveryRegionPlanner`, fences every worker in
the selected region in the same transaction, and replays each worker's exact
applied mailbox from its checkpoint snapshot.  Region-wide activation is
atomic: no restored worker returns to `LIVE` before all region workers are
ready.

This remains a focused prototype.  It does not implement missing-boundary
retirement, source-rewind execution, bounded recovery catch-up, or Prefect
orchestration.  DS-031c and DS-031d remain open.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_barrier_coordinator import (
    PostgresBarrierCoordinator,
    canonical,
    decoded,
    digest,
    hit,
    location_key,
)
from postgres_join_runtime import PostgresJoinRuntime
from postgres_recovery_regions import (
    PostgresRecoveryRegionPlanner,
    RegionEdge,
    RegionNode,
    RegionRecoveryPlan,
    UnrecoverableBoundary,
)
from postgres_stream_v2 import PostgresProtocolError, StaleOwner, content_object_key

FaultHook = Callable[[str], None] | None


@dataclass(frozen=True)
class RegionalWorkerGeneration:
    participant_id: str
    old_owner_id: str | None
    old_owner_epoch: int
    recovery_owner_id: str
    recovery_owner_epoch: int
    snapshot_object_key: str
    mailbox_highwater: int
    target_apply: int
    status: str
    attempt_ordinal: int


@dataclass(frozen=True)
class RegionalOwnershipAttempt:
    plan_id: str
    attempt_ordinal: int
    request_id: str
    owner_id: str
    workers: tuple[tuple[str, int], ...]
    attempt_digest: str


@dataclass(frozen=True)
class RegionalRecovery:
    plan: RegionRecoveryPlan
    workers: tuple[RegionalWorkerGeneration, ...]


class PostgresRegionalRecovery:
    """Compose region planning, worker fencing, replay, and activation."""

    def __init__(
        self,
        coordinator: PostgresBarrierCoordinator,
        planner: PostgresRecoveryRegionPlanner,
        revision: Any,
    ):
        self.coordinator = coordinator
        self.planner = planner
        self.revision = revision
        self.pool = coordinator.pool

    @classmethod
    async def install(
        cls,
        coordinator: PostgresBarrierCoordinator,
        planner: PostgresRecoveryRegionPlanner,
        revision: Any,
    ) -> PostgresRegionalRecovery:
        recovery = cls(coordinator, planner, revision)
        await recovery._initialize()
        return recovery

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS regional_topology_bindings_v1 (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            edge_id text NOT NULL,
            channel text NOT NULL,
            partition_id text NOT NULL,
            producer_id text NOT NULL,
            target_id text NOT NULL,
            binding_digest text NOT NULL,
            PRIMARY KEY(revision_id,edge_id),
            FOREIGN KEY(revision_id,edge_id)
              REFERENCES region_graph_edges_v1(revision_id,edge_id),
            FOREIGN KEY(revision_id,channel,partition_id,target_id)
              REFERENCES revision_edges(revision_id,channel,partition_id,target_id)
        );
        CREATE TABLE IF NOT EXISTS regional_topology_seals_v1 (
            revision_id text PRIMARY KEY REFERENCES deployment_revisions(revision_id),
            graph_manifest_digest text NOT NULL,
            topology_digest text NOT NULL UNIQUE,
            topology_json jsonb NOT NULL,
            sealed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            FOREIGN KEY(revision_id) REFERENCES region_graph_seals_v1(revision_id)
        );
        CREATE TABLE IF NOT EXISTS regional_recovery_workers_v1 (
            plan_id text NOT NULL REFERENCES region_recovery_plans_v1(plan_id),
            revision_id text NOT NULL,
            participant_id text NOT NULL,
            checkpoint_id text NOT NULL,
            old_owner_id text,
            old_owner_epoch bigint NOT NULL CHECK(old_owner_epoch >= 0),
            recovery_owner_id text NOT NULL,
            recovery_owner_epoch bigint NOT NULL CHECK(recovery_owner_epoch > 0),
            worker_config_digest text NOT NULL,
            snapshot_object_key text NOT NULL,
            snapshot_payload_digest text NOT NULL,
            mailbox_highwater bigint NOT NULL CHECK(mailbox_highwater >= 0),
            target_apply bigint NOT NULL CHECK(target_apply >= mailbox_highwater),
            generation_digest text NOT NULL UNIQUE,
            status text NOT NULL CHECK(status IN('RESTORING','READY','ACTIVE')),
            PRIMARY KEY(plan_id,participant_id),
            FOREIGN KEY(revision_id,participant_id)
              REFERENCES join_workers(revision_id,participant_id),
            FOREIGN KEY(checkpoint_id,participant_id)
              REFERENCES participant_checkpoint_acks(checkpoint_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS regional_recovery_attempts_v1 (
            plan_id text NOT NULL REFERENCES region_recovery_plans_v1(plan_id),
            attempt_ordinal bigint NOT NULL CHECK(attempt_ordinal > 0),
            request_id text NOT NULL UNIQUE CHECK(request_id <> ''),
            owner_id text NOT NULL CHECK(owner_id <> ''),
            worker_manifest jsonb NOT NULL,
            attempt_digest text NOT NULL UNIQUE,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(plan_id,attempt_ordinal)
        );
        CREATE TABLE IF NOT EXISTS regional_recovery_attempt_workers_v1 (
            plan_id text NOT NULL,
            attempt_ordinal bigint NOT NULL,
            participant_id text NOT NULL,
            prior_owner_id text,
            prior_owner_epoch bigint NOT NULL CHECK(prior_owner_epoch >= 0),
            effective_owner_id text NOT NULL CHECK(effective_owner_id <> ''),
            effective_owner_epoch bigint NOT NULL CHECK(effective_owner_epoch > 0),
            generation_digest text NOT NULL,
            PRIMARY KEY(plan_id,attempt_ordinal,participant_id),
            FOREIGN KEY(plan_id,attempt_ordinal)
              REFERENCES regional_recovery_attempts_v1(plan_id,attempt_ordinal),
            FOREIGN KEY(plan_id,participant_id)
              REFERENCES regional_recovery_workers_v1(plan_id,participant_id)
        );

        CREATE OR REPLACE FUNCTION regional_reject_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'regional recovery authority is immutable';
        END $$;
        CREATE OR REPLACE FUNCTION regional_validate_topology_binding()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE graph_edge region_graph_edges_v1%ROWTYPE;
        DECLARE real_edge revision_edges%ROWTYPE;
        BEGIN
            SELECT * INTO graph_edge FROM region_graph_edges_v1
              WHERE revision_id=NEW.revision_id AND edge_id=NEW.edge_id;
            SELECT * INTO real_edge FROM revision_edges
              WHERE revision_id=NEW.revision_id AND channel=NEW.channel
                AND partition_id=NEW.partition_id AND target_id=NEW.target_id;
            IF graph_edge.edge_id IS NULL OR real_edge.target_id IS NULL
               OR (graph_edge.source_node,graph_edge.target_node,
                   graph_edge.channel,graph_edge.partition_id)
                    IS DISTINCT FROM
                  (NEW.producer_id,NEW.target_id,NEW.channel,NEW.partition_id)
               OR real_edge.producer_id IS DISTINCT FROM NEW.producer_id
            THEN RAISE EXCEPTION 'regional topology binding mismatch'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_validate_worker_generation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE worker join_workers%ROWTYPE;
        DECLARE plan region_recovery_plans_v1%ROWTYPE;
        DECLARE ack participant_checkpoint_acks%ROWTYPE;
        BEGIN
            SELECT * INTO worker FROM join_workers WHERE revision_id=NEW.revision_id
              AND participant_id=NEW.participant_id;
            SELECT * INTO plan FROM region_recovery_plans_v1
              WHERE plan_id=NEW.plan_id;
            SELECT * INTO ack FROM participant_checkpoint_acks
              WHERE checkpoint_id=NEW.checkpoint_id
                AND participant_id=NEW.participant_id;
            IF worker.participant_id IS NULL OR plan.plan_id IS NULL OR ack.participant_id IS NULL
               OR NOT (plan.region_json ? NEW.participant_id)
               OR plan.revision_id IS DISTINCT FROM NEW.revision_id
               OR plan.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
               OR (worker.owner_id,worker.owner_epoch,worker.config_digest)
                    IS DISTINCT FROM
                  (NEW.old_owner_id,NEW.old_owner_epoch,NEW.worker_config_digest)
               OR NEW.recovery_owner_epoch<>NEW.old_owner_epoch+1
               OR (ack.snapshot_object_key,ack.snapshot_payload_digest,
                   ack.mailbox_highwater)
                    IS DISTINCT FROM
                  (NEW.snapshot_object_key,NEW.snapshot_payload_digest,
                   NEW.mailbox_highwater)
            THEN RAISE EXCEPTION 'regional worker generation authority mismatch'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_guard_worker_fence()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.phase='RESTORING' AND (
                 OLD.phase IS DISTINCT FROM NEW.phase
                 OR OLD.owner_id IS DISTINCT FROM NEW.owner_id
                 OR OLD.owner_epoch IS DISTINCT FROM NEW.owner_epoch)
               AND NOT EXISTS(
                 SELECT 1 FROM regional_recovery_workers_v1 g
                 JOIN regional_recovery_attempt_workers_v1 aw
                   ON aw.plan_id=g.plan_id AND aw.participant_id=g.participant_id
                 WHERE g.revision_id=NEW.revision_id
                   AND g.participant_id=NEW.participant_id
                   AND g.status<>'ACTIVE'
                   AND aw.prior_owner_id IS NOT DISTINCT FROM OLD.owner_id
                   AND aw.prior_owner_epoch=OLD.owner_epoch
                   AND aw.effective_owner_id=NEW.owner_id
                   AND aw.effective_owner_epoch=NEW.owner_epoch
                   AND aw.attempt_ordinal=(SELECT max(x.attempt_ordinal)
                     FROM regional_recovery_attempts_v1 x
                     WHERE x.plan_id=g.plan_id))
            THEN RAISE EXCEPTION 'join RESTORING transition lacks region fence'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_validate_attempt_header()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.attempt_ordinal<>COALESCE((SELECT max(attempt_ordinal)+1
                 FROM regional_recovery_attempts_v1 WHERE plan_id=NEW.plan_id),1)
               OR jsonb_typeof(NEW.worker_manifest)<>'array'
            THEN RAISE EXCEPTION 'regional ownership attempt sequence invalid'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_validate_attempt_worker()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE worker join_workers%ROWTYPE;
        DECLARE generation regional_recovery_workers_v1%ROWTYPE;
        DECLARE attempt regional_recovery_attempts_v1%ROWTYPE;
        BEGIN
            SELECT * INTO worker FROM join_workers WHERE revision_id=(
              SELECT revision_id FROM regional_recovery_workers_v1
               WHERE plan_id=NEW.plan_id AND participant_id=NEW.participant_id)
              AND participant_id=NEW.participant_id;
            SELECT * INTO generation FROM regional_recovery_workers_v1
              WHERE plan_id=NEW.plan_id AND participant_id=NEW.participant_id;
            SELECT * INTO attempt FROM regional_recovery_attempts_v1
              WHERE plan_id=NEW.plan_id AND attempt_ordinal=NEW.attempt_ordinal;
            IF worker.participant_id IS NULL OR generation.participant_id IS NULL
               OR attempt.plan_id IS NULL OR generation.status='ACTIVE'
               OR attempt.owner_id IS DISTINCT FROM NEW.effective_owner_id
               OR (worker.owner_id,worker.owner_epoch,generation.generation_digest)
                    IS DISTINCT FROM
                  (NEW.prior_owner_id,NEW.prior_owner_epoch,NEW.generation_digest)
               OR NEW.effective_owner_epoch<>NEW.prior_owner_epoch+1
            THEN RAISE EXCEPTION 'regional ownership attempt worker mismatch'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_validate_attempt_manifest()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE actual jsonb;
        DECLARE expected_count bigint;
        BEGIN
            SELECT jsonb_agg(jsonb_build_object(
              'participant_id',w.participant_id,
              'prior_owner_id',w.prior_owner_id,
              'prior_owner_epoch',w.prior_owner_epoch,
              'effective_owner_epoch',w.effective_owner_epoch,
              'generation_digest',w.generation_digest)
              ORDER BY w.participant_id),count(*) INTO actual,expected_count
              FROM regional_recovery_attempt_workers_v1 w
              WHERE w.plan_id=NEW.plan_id AND w.attempt_ordinal=NEW.attempt_ordinal;
            IF actual IS DISTINCT FROM NEW.worker_manifest
               OR expected_count<>(SELECT count(*) FROM regional_recovery_workers_v1
                                    WHERE plan_id=NEW.plan_id)
            THEN RAISE EXCEPTION 'regional ownership attempt manifest mismatch'; END IF;
            RETURN NULL;
        END $$;
        CREATE OR REPLACE FUNCTION regional_guard_generation_status()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE worker join_workers%ROWTYPE;
        BEGIN
            IF (NEW.plan_id,NEW.revision_id,NEW.participant_id,NEW.checkpoint_id,
                NEW.old_owner_id,NEW.old_owner_epoch,NEW.recovery_owner_id,
                NEW.recovery_owner_epoch,NEW.worker_config_digest,
                NEW.snapshot_object_key,NEW.snapshot_payload_digest,
                NEW.mailbox_highwater,NEW.target_apply,NEW.generation_digest)
               IS DISTINCT FROM
               (OLD.plan_id,OLD.revision_id,OLD.participant_id,OLD.checkpoint_id,
                OLD.old_owner_id,OLD.old_owner_epoch,OLD.recovery_owner_id,
                OLD.recovery_owner_epoch,OLD.worker_config_digest,
                OLD.snapshot_object_key,OLD.snapshot_payload_digest,
                OLD.mailbox_highwater,OLD.target_apply,OLD.generation_digest)
            THEN RAISE EXCEPTION 'regional worker generation identity is immutable'; END IF;
            IF OLD.status='RESTORING' AND NEW.status='READY' THEN
              IF NOT EXISTS(SELECT 1 FROM join_recoveries_v3 r
                WHERE r.revision_id=NEW.revision_id
                  AND r.participant_id=NEW.participant_id
                  AND r.recovery_id=NEW.plan_id AND r.status='READY'
                  AND r.target_apply=NEW.target_apply
                  AND r.replayed_through=NEW.target_apply)
              THEN RAISE EXCEPTION 'regional worker READY lacks exact replay'; END IF;
            ELSIF OLD.status='READY' AND NEW.status='ACTIVE' THEN
              SELECT * INTO worker FROM join_workers WHERE revision_id=NEW.revision_id
                AND participant_id=NEW.participant_id;
              IF NOT EXISTS(SELECT 1
                FROM regional_recovery_attempt_workers_v1 aw
                WHERE aw.plan_id=NEW.plan_id AND aw.participant_id=NEW.participant_id
                  AND aw.attempt_ordinal=(SELECT max(attempt_ordinal)
                    FROM regional_recovery_attempts_v1 WHERE plan_id=NEW.plan_id)
                  AND (aw.effective_owner_id,aw.effective_owner_epoch,'LIVE')
                    IS NOT DISTINCT FROM
                    (worker.owner_id,worker.owner_epoch,worker.phase))
              THEN RAISE EXCEPTION 'regional worker ACTIVE lacks LIVE generation'; END IF;
            ELSE
              RAISE EXCEPTION 'illegal regional worker generation transition';
            END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_enforce_atomic_activation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE generation regional_recovery_workers_v1%ROWTYPE;
        BEGIN
            IF OLD.phase='RESTORING' AND NEW.phase='LIVE' THEN
              SELECT g.* INTO generation FROM regional_recovery_workers_v1 g
               JOIN regional_recovery_attempt_workers_v1 aw
                 ON aw.plan_id=g.plan_id AND aw.participant_id=g.participant_id
               WHERE g.revision_id=NEW.revision_id
                 AND g.participant_id=NEW.participant_id
                 AND aw.effective_owner_id=NEW.owner_id
                 AND aw.effective_owner_epoch=NEW.owner_epoch
                 AND aw.attempt_ordinal=(SELECT max(attempt_ordinal)
                   FROM regional_recovery_attempts_v1 WHERE plan_id=g.plan_id)
               ORDER BY g.plan_id DESC LIMIT 1;
              IF generation.plan_id IS NOT NULL AND EXISTS(
                SELECT 1 FROM regional_recovery_workers_v1 member
                 JOIN regional_recovery_attempt_workers_v1 aw
                   ON aw.plan_id=member.plan_id AND aw.participant_id=member.participant_id
                  AND aw.attempt_ordinal=(SELECT max(attempt_ordinal)
                    FROM regional_recovery_attempts_v1
                    WHERE plan_id=generation.plan_id)
                 LEFT JOIN join_workers worker
                   ON worker.revision_id=member.revision_id
                  AND worker.participant_id=member.participant_id
                 WHERE member.plan_id=generation.plan_id AND (
                   member.status<>'ACTIVE'
                   OR worker.phase IS DISTINCT FROM 'LIVE'
                   OR worker.owner_id IS DISTINCT FROM aw.effective_owner_id
                   OR worker.owner_epoch IS DISTINCT FROM aw.effective_owner_epoch))
              THEN RAISE EXCEPTION 'regional activation is partial'; END IF;
            END IF;
            RETURN NULL;
        END $$;

        DROP TRIGGER IF EXISTS regional_topology_binding_guard
          ON regional_topology_bindings_v1;
        CREATE TRIGGER regional_topology_binding_guard BEFORE INSERT
          ON regional_topology_bindings_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_validate_topology_binding();
        DROP TRIGGER IF EXISTS regional_topology_binding_immutable
          ON regional_topology_bindings_v1;
        CREATE TRIGGER regional_topology_binding_immutable BEFORE UPDATE OR DELETE
          ON regional_topology_bindings_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_reject_mutation();
        DROP TRIGGER IF EXISTS regional_topology_seal_immutable
          ON regional_topology_seals_v1;
        CREATE TRIGGER regional_topology_seal_immutable BEFORE UPDATE OR DELETE
          ON regional_topology_seals_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_reject_mutation();
        DROP TRIGGER IF EXISTS regional_generation_insert_guard
          ON regional_recovery_workers_v1;
        CREATE TRIGGER regional_generation_insert_guard BEFORE INSERT
          ON regional_recovery_workers_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_validate_worker_generation();
        DROP TRIGGER IF EXISTS regional_generation_update_guard
          ON regional_recovery_workers_v1;
        CREATE TRIGGER regional_generation_update_guard BEFORE UPDATE
          ON regional_recovery_workers_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_guard_generation_status();
        DROP TRIGGER IF EXISTS regional_generation_no_delete
          ON regional_recovery_workers_v1;
        CREATE TRIGGER regional_generation_no_delete BEFORE DELETE
          ON regional_recovery_workers_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_reject_mutation();
        DROP TRIGGER IF EXISTS regional_attempt_header_guard
          ON regional_recovery_attempts_v1;
        CREATE TRIGGER regional_attempt_header_guard BEFORE INSERT
          ON regional_recovery_attempts_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_validate_attempt_header();
        DROP TRIGGER IF EXISTS regional_attempt_header_immutable
          ON regional_recovery_attempts_v1;
        CREATE TRIGGER regional_attempt_header_immutable BEFORE UPDATE OR DELETE
          ON regional_recovery_attempts_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_reject_mutation();
        DROP TRIGGER IF EXISTS regional_attempt_worker_guard
          ON regional_recovery_attempt_workers_v1;
        CREATE TRIGGER regional_attempt_worker_guard BEFORE INSERT
          ON regional_recovery_attempt_workers_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_validate_attempt_worker();
        DROP TRIGGER IF EXISTS regional_attempt_worker_immutable
          ON regional_recovery_attempt_workers_v1;
        CREATE TRIGGER regional_attempt_worker_immutable BEFORE UPDATE OR DELETE
          ON regional_recovery_attempt_workers_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_reject_mutation();
        DROP TRIGGER IF EXISTS regional_attempt_manifest_guard
          ON regional_recovery_attempts_v1;
        CREATE CONSTRAINT TRIGGER regional_attempt_manifest_guard
          AFTER INSERT ON regional_recovery_attempts_v1
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION regional_validate_attempt_manifest();
        DROP TRIGGER IF EXISTS regional_worker_fence_guard ON join_workers;
        CREATE TRIGGER regional_worker_fence_guard BEFORE UPDATE ON join_workers
          FOR EACH ROW EXECUTE FUNCTION regional_guard_worker_fence();
        DROP TRIGGER IF EXISTS regional_atomic_activation ON join_workers;
        CREATE CONSTRAINT TRIGGER regional_atomic_activation
          AFTER UPDATE OF phase,owner_id,owner_epoch ON join_workers
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION regional_enforce_atomic_activation();
        """
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)

    @staticmethod
    def edge_id(revision_id: str, channel: str, partition: str, target_id: str) -> str:
        return "edge:" + digest(
            {
                "revision_id": revision_id,
                "channel": channel,
                "partition": partition,
                "target_id": target_id,
            }
        )

    async def bind_revision_topology(
        self,
        *,
        source_replayable: bool = False,
        nonreplayable_edges: set[str] | None = None,
    ) -> str:
        """Derive and seal the planner graph from real `revision_edges`."""
        nonreplayable_edges = (
            set() if nonreplayable_edges is None else set(nonreplayable_edges)
        )
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(
                conn, self.revision.revision_id
            )
            real_edges = list(
                await conn.fetch(
                    """SELECT * FROM revision_edges WHERE revision_id=$1
                       ORDER BY channel,partition_id,target_id""",
                    revision["revision_id"],
                )
            )
            participants = {
                row["participant_id"]
                for row in await conn.fetch(
                    """SELECT participant_id FROM revision_participants
                       WHERE revision_id=$1""",
                    revision["revision_id"],
                )
            }
        if not real_edges:
            raise PostgresProtocolError("revision topology has no edges")
        producers = {row["producer_id"] for row in real_edges}
        unknown = (producers | {row["target_id"] for row in real_edges}) - (
            participants | {"SOURCE"}
        )
        if unknown:
            raise PostgresProtocolError(
                "revision edge references an unknown participant"
            )
        nodes = [RegionNode("SOURCE", "SOURCE", source_replayable)] + [
            RegionNode(participant) for participant in sorted(participants)
        ]
        graph_edges = []
        topology = []
        for row in real_edges:
            edge_id = self.edge_id(
                row["revision_id"],
                row["channel"],
                row["partition_id"],
                row["target_id"],
            )
            graph_edges.append(
                RegionEdge(
                    edge_id,
                    row["producer_id"],
                    row["target_id"],
                    row["channel"],
                    row["partition_id"],
                    edge_id not in nonreplayable_edges,
                )
            )
            topology.append(
                {
                    "edge_id": edge_id,
                    "channel": row["channel"],
                    "partition": row["partition_id"],
                    "producer_id": row["producer_id"],
                    "target_id": row["target_id"],
                }
            )
        graph_manifest_digest = await self.planner.declare_graph(
            self.revision.revision_id, nodes, graph_edges
        )
        topology.sort(key=lambda item: item["edge_id"])
        topology_identity = {
            "revision_id": self.revision.revision_id,
            "graph_manifest_digest": graph_manifest_digest,
            "edges": topology,
        }
        topology_digest = digest(topology_identity)
        async with self.pool.acquire() as conn, conn.transaction():
            await self.coordinator._locked_revision(conn, self.revision.revision_id)
            existing = await conn.fetchrow(
                """SELECT * FROM regional_topology_seals_v1
                   WHERE revision_id=$1""",
                self.revision.revision_id,
            )
            if existing is not None:
                if (
                    existing["graph_manifest_digest"],
                    existing["topology_digest"],
                    decoded(existing["topology_json"]),
                ) != (
                    graph_manifest_digest,
                    topology_digest,
                    topology_identity,
                ):
                    raise PostgresProtocolError("regional topology binding drift")
                return topology_digest
            for item in topology:
                await conn.execute(
                    """INSERT INTO regional_topology_bindings_v1 VALUES(
                       $1,$2,$3,$4,$5,$6,$7)""",
                    self.revision.revision_id,
                    item["edge_id"],
                    item["channel"],
                    item["partition"],
                    item["producer_id"],
                    item["target_id"],
                    digest(item),
                )
            await conn.execute(
                """INSERT INTO regional_topology_seals_v1(
                   revision_id,graph_manifest_digest,topology_digest,topology_json)
                   VALUES($1,$2,$3,$4::jsonb)""",
                self.revision.revision_id,
                graph_manifest_digest,
                topology_digest,
                canonical(topology_identity),
            )
        return topology_digest

    async def bind_checkpoint_cuts(self, checkpoint_id: str) -> None:
        async with self.pool.acquire() as conn:
            edge_ids = await conn.fetch(
                """SELECT edge_id FROM regional_topology_bindings_v1
                   WHERE revision_id=$1 ORDER BY edge_id""",
                self.revision.revision_id,
            )
        if not edge_ids:
            raise PostgresProtocolError("regional topology is not bound")
        for row in edge_ids:
            await self.planner.bind_edge_cut(
                self.revision.revision_id, checkpoint_id, row["edge_id"]
            )

    async def start_recovery(
        self,
        coordinator_owner_id: str,
        coordinator_owner_epoch: int,
        request_id: str,
        failed_node: str,
        checkpoint_id: str,
        recovery_owner_id: str,
        *,
        lease_seconds: float = 300,
        fault: FaultHook = None,
    ) -> RegionalRecovery:
        if lease_seconds <= 0:
            raise ValueError("regional recovery lease must be positive")
        async with self.pool.acquire() as conn, conn.transaction():
            plan = await self.planner.create_plan(
                self.revision.revision_id,
                self.revision.graph_version,
                self.revision.graph_digest,
                coordinator_owner_id,
                coordinator_owner_epoch,
                request_id,
                failed_node,
                checkpoint_id,
                connection=conn,
            )
            hit(fault, "regional_plan_sealed")
            seal = await conn.fetchrow(
                """SELECT * FROM regional_topology_seals_v1
                   WHERE revision_id=$1""",
                self.revision.revision_id,
            )
            if (
                seal is None
                or seal["graph_manifest_digest"] != plan.graph_manifest_digest
            ):
                raise PostgresProtocolError("regional plan lacks real topology binding")
            if plan.source_rewinds:
                raise PostgresProtocolError(
                    "regional execution does not implement source rewind"
                )
            participants = tuple(node for node in plan.region if node != "SOURCE")
            existing = list(
                await conn.fetch(
                    """SELECT * FROM regional_recovery_workers_v1
                       WHERE plan_id=$1 ORDER BY participant_id""",
                    plan.plan_id,
                )
            )
            if existing:
                if tuple(row["participant_id"] for row in existing) != tuple(
                    sorted(participants)
                ) or any(
                    row["recovery_owner_id"] != recovery_owner_id for row in existing
                ):
                    raise PostgresProtocolError("regional recovery generation drift")
                return RegionalRecovery(
                    plan,
                    tuple(
                        self._generation(row)
                        for row in await self._generation_rows(conn, plan.plan_id)
                    ),
                )
            workers = list(
                await conn.fetch(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=ANY($2::text[])
                       ORDER BY participant_id FOR UPDATE""",
                    self.revision.revision_id,
                    list(participants),
                )
            )
            if [row["participant_id"] for row in workers] != sorted(participants):
                raise PostgresProtocolError("region contains an uninstalled worker")
            if any(row["phase"] != "LIVE" for row in workers):
                raise PostgresProtocolError("region worker is already restoring")
            acknowledgements = {
                row["participant_id"]: row
                for row in await conn.fetch(
                    """SELECT a.*,o.payload FROM participant_checkpoint_acks a
                       JOIN checkpoint_snapshot_objects o
                         ON o.object_key=a.snapshot_object_key
                       WHERE a.checkpoint_id=$1
                         AND a.participant_id=ANY($2::text[])""",
                    checkpoint_id,
                    list(participants),
                )
            }
            if set(acknowledgements) != set(participants):
                raise PostgresProtocolError("region checkpoint snapshot is incomplete")
            await self._preflight_replay_outputs(conn, plan, workers, acknowledgements)
            targets: dict[str, int] = {}
            for boundary in plan.boundaries:
                key = location_key(boundary.channel, boundary.partition)
                prior = targets.setdefault(key, boundary.target_data_sequence)
                if prior != boundary.target_data_sequence:
                    raise PostgresProtocolError("shared boundary target drift")
            snapshots = {
                participant: acknowledgements[participant]["snapshot_object_key"]
                for participant in sorted(participants)
            }
            compatibility_identity = {
                "recovery_id": plan.plan_id,
                "request_id": request_id,
                "revision_id": self.revision.revision_id,
                "checkpoint_id": checkpoint_id,
                "completion_digest": plan.completion_digest,
                "fence_authority_ordinal": plan.fence_authority_ordinal,
                "target_data_sequences": targets,
                "participant_snapshots": snapshots,
            }
            compatibility_digest = digest(compatibility_identity)
            await conn.execute(
                """INSERT INTO recovery_plans_v2(
                   recovery_id,request_id,revision_id,checkpoint_id,
                   fence_authority_ordinal,target_data_sequences,
                   participant_snapshots,plan_digest,status,writer_epoch)
                   VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,'RESTORING',$9)""",
                plan.plan_id,
                request_id,
                self.revision.revision_id,
                checkpoint_id,
                plan.fence_authority_ordinal,
                canonical(targets),
                canonical(snapshots),
                compatibility_digest,
                coordinator_owner_epoch,
            )
            generations = []
            attempt_manifest = []
            for worker in workers:
                participant = worker["participant_id"]
                ack = acknowledgements[participant]
                snapshot_payload = bytes(ack["payload"])
                if (
                    hashlib.sha256(snapshot_payload).hexdigest()
                    != ack["snapshot_payload_digest"]
                ):
                    raise PostgresProtocolError("regional snapshot bytes are corrupt")
                snapshot_state = json.loads(snapshot_payload)
                target_apply = worker["next_apply"] - 1
                highwater = ack["mailbox_highwater"]
                if highwater > target_apply:
                    raise PostgresProtocolError(
                        "regional snapshot exceeds durable worker history"
                    )
                turn_count = await conn.fetchval(
                    """SELECT count(*) FROM join_turns_v3
                       WHERE revision_id=$1 AND participant_id=$2
                         AND apply_ordinal>$3 AND apply_ordinal<=$4""",
                    self.revision.revision_id,
                    participant,
                    highwater,
                    target_apply,
                )
                if turn_count != target_apply - highwater:
                    raise PostgresProtocolError("regional applied mailbox has a gap")
                new_epoch = worker["owner_epoch"] + 1
                generation_identity = {
                    "plan_id": plan.plan_id,
                    "revision_id": self.revision.revision_id,
                    "participant_id": participant,
                    "checkpoint_id": checkpoint_id,
                    "old_owner_id": worker["owner_id"],
                    "old_owner_epoch": worker["owner_epoch"],
                    "recovery_owner_id": recovery_owner_id,
                    "recovery_owner_epoch": new_epoch,
                    "worker_config_digest": worker["config_digest"],
                    "snapshot_object_key": ack["snapshot_object_key"],
                    "snapshot_payload_digest": ack["snapshot_payload_digest"],
                    "mailbox_highwater": highwater,
                    "target_apply": target_apply,
                }
                generation_digest = digest(generation_identity)
                await conn.execute(
                    """INSERT INTO regional_recovery_workers_v1 VALUES(
                       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                       'RESTORING')""",
                    plan.plan_id,
                    self.revision.revision_id,
                    participant,
                    checkpoint_id,
                    worker["owner_id"],
                    worker["owner_epoch"],
                    recovery_owner_id,
                    new_epoch,
                    worker["config_digest"],
                    ack["snapshot_object_key"],
                    ack["snapshot_payload_digest"],
                    highwater,
                    target_apply,
                    generation_digest,
                )
                await conn.execute(
                    """INSERT INTO join_recoveries_v3 VALUES(
                       $1,$2,$3,$4,$5,$6::jsonb,'RESTORING')""",
                    self.revision.revision_id,
                    participant,
                    plan.plan_id,
                    target_apply,
                    highwater,
                    canonical(snapshot_state),
                )
                attempt_manifest.append(
                    {
                        "participant_id": participant,
                        "prior_owner_id": worker["owner_id"],
                        "prior_owner_epoch": worker["owner_epoch"],
                        "effective_owner_epoch": new_epoch,
                        "generation_digest": generation_digest,
                    }
                )
                generations.append(
                    RegionalWorkerGeneration(
                        participant,
                        worker["owner_id"],
                        worker["owner_epoch"],
                        recovery_owner_id,
                        new_epoch,
                        ack["snapshot_object_key"],
                        highwater,
                        target_apply,
                        "RESTORING",
                        1,
                    )
                )
            attempt_manifest.sort(key=lambda item: item["participant_id"])
            attempt_identity = {
                "plan_id": plan.plan_id,
                "attempt_ordinal": 1,
                "request_id": "initial:" + plan.plan_id,
                "owner_id": recovery_owner_id,
                "workers": attempt_manifest,
            }
            await conn.execute(
                """INSERT INTO regional_recovery_attempts_v1(
                   plan_id,attempt_ordinal,request_id,owner_id,
                   worker_manifest,attempt_digest)
                   VALUES($1,1,$2,$3,$4::jsonb,$5)""",
                plan.plan_id,
                attempt_identity["request_id"],
                recovery_owner_id,
                canonical(attempt_manifest),
                digest(attempt_identity),
            )
            for item in attempt_manifest:
                await conn.execute(
                    """INSERT INTO regional_recovery_attempt_workers_v1 VALUES(
                       $1,1,$2,$3,$4,$5,$6,$7)""",
                    plan.plan_id,
                    item["participant_id"],
                    item["prior_owner_id"],
                    item["prior_owner_epoch"],
                    recovery_owner_id,
                    item["effective_owner_epoch"],
                    item["generation_digest"],
                )
            hit(fault, "regional_attempt_appended")
            for worker in workers:
                participant = worker["participant_id"]
                attempt = next(
                    item
                    for item in attempt_manifest
                    if item["participant_id"] == participant
                )
                await conn.execute(
                    """DELETE FROM join_invocations_v3
                       WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    participant,
                )
                await conn.execute(
                    """UPDATE join_workers SET owner_id=$3,owner_epoch=$4,
                       lease_expires_at=clock_timestamp()
                         +$5::double precision*interval '1 second',
                       phase='RESTORING'
                       WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    participant,
                    recovery_owner_id,
                    attempt["effective_owner_epoch"],
                    lease_seconds,
                )
            await self.coordinator._assert_metadata_budget(
                conn, self.revision.revision_id
            )
            hit(fault, "regional_workers_fenced")
        hit(fault, "regional_start_committed")
        return RegionalRecovery(plan, tuple(generations))

    async def _preflight_replay_outputs(
        self,
        conn: asyncpg.Connection,
        plan: RegionRecoveryPlan,
        workers: list[asyncpg.Record],
        acknowledgements: dict[str, asyncpg.Record],
    ) -> None:
        """Reject a retired/corrupt regional replay tail before fencing workers."""
        specs = {
            item["participant_id"]: item for item in self.revision.participant_manifest
        }
        for worker in workers:
            participant = worker["participant_id"]
            highwater = acknowledgements[participant]["mailbox_highwater"]
            turns = await conn.fetch(
                """SELECT * FROM join_turns_v3
                   WHERE revision_id=$1 AND participant_id=$2
                     AND apply_ordinal>$3 AND apply_ordinal<$4
                   ORDER BY apply_ordinal""",
                self.revision.revision_id,
                participant,
                highwater,
                worker["next_apply"],
            )
            for turn in turns:
                if turn["terminal"] == "RETURN":
                    if turn["output_record_id"] is not None:
                        raise UnrecoverableBoundary(
                            f"participant {participant} replay output is corrupt "
                            f"outside retained durable horizon for {plan.plan_id}"
                        )
                    continue
                outputs = specs[participant]["outputs"]
                if len(outputs) != 1 or turn["output_record_id"] is None:
                    raise UnrecoverableBoundary(
                        f"participant {participant} replay output authority is missing "
                        f"outside retained durable horizon for {plan.plan_id}"
                    )
                row = await conn.fetchrow(
                    """SELECT r.*,o.payload,o.published_at,
                              o.payload_digest AS object_digest,
                              o.payload_size AS object_size,
                              o.schema_id AS object_schema,
                              i.payload_digest AS identity_digest,
                              i.payload_size AS identity_size,
                              i.schema_id AS identity_schema
                       FROM records r JOIN objects o USING(object_key)
                       JOIN object_identities i USING(object_key)
                       WHERE r.record_id=$1""",
                    turn["output_record_id"],
                )
                output = outputs[0]
                if row is None:
                    raise UnrecoverableBoundary(
                        f"participant {participant} replay output is missing outside "
                        f"retained durable horizon for {plan.plan_id}"
                    )
                payload = bytes(row["payload"])
                if (
                    row["published_at"] is None
                    or (row["channel"], row["partition_id"])
                    != (output["channel"], output["partition"])
                    or row["record_id"] != "join-output:" + row["lineage_digest"]
                    or row["payload_digest"] != turn["output_digest"]
                    or row["object_key"]
                    != content_object_key(payload, row["schema_id"])
                    or (
                        hashlib.sha256(payload).hexdigest(),
                        len(payload),
                        row["schema_id"],
                    )
                    != (
                        row["payload_digest"],
                        row["payload_size"],
                        row["object_schema"],
                    )
                    or (
                        row["object_digest"],
                        row["object_size"],
                        row["object_schema"],
                    )
                    != (
                        row["identity_digest"],
                        row["identity_size"],
                        row["identity_schema"],
                    )
                ):
                    raise UnrecoverableBoundary(
                        f"participant {participant} replay output is corrupt outside "
                        f"retained durable horizon for {plan.plan_id}"
                    )

    async def takeover_recovery(
        self,
        plan_id: str,
        owner_id: str,
        *,
        request_id: str | None = None,
        lease_seconds: float = 300,
        fault: FaultHook = None,
    ) -> RegionalOwnershipAttempt:
        """Atomically fence an expired region under a new append-only attempt."""
        if not owner_id or lease_seconds <= 0:
            raise ValueError("takeover requires owner and positive lease")
        request_id = (
            f"takeover:{plan_id}:{owner_id}" if request_id is None else request_id
        )
        if not request_id:
            raise ValueError("takeover request_id must be non-empty")
        async with self.pool.acquire() as conn, conn.transaction():
            await self.coordinator._locked_revision(conn, self.revision.revision_id)
            existing = await conn.fetchrow(
                """SELECT * FROM regional_recovery_attempts_v1
                   WHERE request_id=$1""",
                request_id,
            )
            if existing is not None:
                if (existing["plan_id"], existing["owner_id"]) != (plan_id, owner_id):
                    raise PostgresProtocolError(
                        "regional takeover request identity drift"
                    )
                existing_workers = await conn.fetch(
                    """SELECT participant_id,effective_owner_epoch
                       FROM regional_recovery_attempt_workers_v1
                       WHERE plan_id=$1 AND attempt_ordinal=$2
                       ORDER BY participant_id""",
                    plan_id,
                    existing["attempt_ordinal"],
                )
                return RegionalOwnershipAttempt(
                    plan_id,
                    existing["attempt_ordinal"],
                    request_id,
                    owner_id,
                    tuple(
                        (row["participant_id"], row["effective_owner_epoch"])
                        for row in existing_workers
                    ),
                    existing["attempt_digest"],
                )
            generations = list(
                await conn.fetch(
                    """SELECT * FROM regional_recovery_workers_v1
                       WHERE plan_id=$1 ORDER BY participant_id FOR UPDATE""",
                    plan_id,
                )
            )
            if not generations or any(row["status"] == "ACTIVE" for row in generations):
                raise PostgresProtocolError(
                    "regional recovery is not takeover eligible"
                )
            workers = list(
                await conn.fetch(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=ANY($2::text[])
                       ORDER BY participant_id FOR UPDATE""",
                    self.revision.revision_id,
                    [row["participant_id"] for row in generations],
                )
            )
            if len(workers) != len(generations) or any(
                row["phase"] != "RESTORING" for row in workers
            ):
                raise PostgresProtocolError(
                    "regional takeover requires RESTORING region"
                )
            if any(
                [
                    await conn.fetchval(
                        "SELECT $1::timestamptz>clock_timestamp()",
                        row["lease_expires_at"],
                    )
                    for row in workers
                ]
            ):
                raise StaleOwner("regional recovery attempt still has a live lease")
            prior_ordinal = await conn.fetchval(
                """SELECT max(attempt_ordinal) FROM regional_recovery_attempts_v1
                   WHERE plan_id=$1""",
                plan_id,
            )
            if prior_ordinal is None:
                raise PostgresProtocolError("regional ownership history is missing")
            current = list(
                await conn.fetch(
                    """SELECT * FROM regional_recovery_attempt_workers_v1
                       WHERE plan_id=$1 AND attempt_ordinal=$2
                       ORDER BY participant_id""",
                    plan_id,
                    prior_ordinal,
                )
            )
            if [row["participant_id"] for row in current] != [
                row["participant_id"] for row in workers
            ] or any(
                (worker["owner_id"], worker["owner_epoch"])
                != (attempt["effective_owner_id"], attempt["effective_owner_epoch"])
                for worker, attempt in zip(workers, current, strict=True)
            ):
                raise StaleOwner("regional current attempt authority drift")
            attempt_ordinal = prior_ordinal + 1
            by_participant = {row["participant_id"]: row for row in generations}
            manifest = [
                {
                    "participant_id": worker["participant_id"],
                    "prior_owner_id": worker["owner_id"],
                    "prior_owner_epoch": worker["owner_epoch"],
                    "effective_owner_epoch": worker["owner_epoch"] + 1,
                    "generation_digest": by_participant[worker["participant_id"]][
                        "generation_digest"
                    ],
                }
                for worker in workers
            ]
            attempt_identity = {
                "plan_id": plan_id,
                "attempt_ordinal": attempt_ordinal,
                "request_id": request_id,
                "owner_id": owner_id,
                "workers": manifest,
            }
            attempt_digest = digest(attempt_identity)
            await conn.execute(
                """INSERT INTO regional_recovery_attempts_v1(
                   plan_id,attempt_ordinal,request_id,owner_id,
                   worker_manifest,attempt_digest)
                   VALUES($1,$2,$3,$4,$5::jsonb,$6)""",
                plan_id,
                attempt_ordinal,
                request_id,
                owner_id,
                canonical(manifest),
                attempt_digest,
            )
            for item in manifest:
                await conn.execute(
                    """INSERT INTO regional_recovery_attempt_workers_v1 VALUES(
                       $1,$2,$3,$4,$5,$6,$7,$8)""",
                    plan_id,
                    attempt_ordinal,
                    item["participant_id"],
                    item["prior_owner_id"],
                    item["prior_owner_epoch"],
                    owner_id,
                    item["effective_owner_epoch"],
                    item["generation_digest"],
                )
            hit(fault, "regional_takeover_attempt_appended")
            for item in manifest:
                await conn.execute(
                    """DELETE FROM join_invocations_v3
                       WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    item["participant_id"],
                )
                await conn.execute(
                    """UPDATE join_workers SET owner_id=$3,owner_epoch=$4,
                       lease_expires_at=clock_timestamp()
                         +$5::double precision*interval '1 second'
                       WHERE revision_id=$1 AND participant_id=$2
                         AND phase='RESTORING'""",
                    self.revision.revision_id,
                    item["participant_id"],
                    owner_id,
                    item["effective_owner_epoch"],
                    lease_seconds,
                )
            await self.coordinator._assert_metadata_budget(
                conn, self.revision.revision_id
            )
            hit(fault, "regional_takeover_workers_fenced")
        hit(fault, "regional_takeover_committed")
        return RegionalOwnershipAttempt(
            plan_id,
            attempt_ordinal,
            request_id,
            owner_id,
            tuple(
                (item["participant_id"], item["effective_owner_epoch"])
                for item in manifest
            ),
            attempt_digest,
        )

    async def renew_recovery(
        self,
        plan_id: str,
        attempt_ordinal: int,
        owner_id: str,
        *,
        lease_seconds: float = 300,
    ) -> None:
        """Renew every worker in the exact current regional ownership attempt."""
        if lease_seconds <= 0:
            raise ValueError("renewal lease must be positive")
        async with self.pool.acquire() as conn, conn.transaction():
            latest = await conn.fetchrow(
                """SELECT * FROM regional_recovery_attempts_v1
                   WHERE plan_id=$1 ORDER BY attempt_ordinal DESC LIMIT 1 FOR UPDATE""",
                plan_id,
            )
            if latest is None or (latest["attempt_ordinal"], latest["owner_id"]) != (
                attempt_ordinal,
                owner_id,
            ):
                raise StaleOwner("regional renewal attempt is stale")
            attempt_workers = list(
                await conn.fetch(
                    """SELECT * FROM regional_recovery_attempt_workers_v1
                       WHERE plan_id=$1 AND attempt_ordinal=$2
                       ORDER BY participant_id""",
                    plan_id,
                    attempt_ordinal,
                )
            )
            workers = list(
                await conn.fetch(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=ANY($2::text[])
                       ORDER BY participant_id FOR UPDATE""",
                    self.revision.revision_id,
                    [row["participant_id"] for row in attempt_workers],
                )
            )
            authority_mismatch = len(workers) != len(attempt_workers)
            for worker, attempt in zip(workers, attempt_workers, strict=False):
                authority_mismatch = authority_mismatch or (
                    (
                        worker["participant_id"],
                        worker["owner_id"],
                        worker["owner_epoch"],
                        worker["phase"],
                    )
                    != (
                        attempt["participant_id"],
                        owner_id,
                        attempt["effective_owner_epoch"],
                        "RESTORING",
                    )
                )
                authority_mismatch = authority_mismatch or not await conn.fetchval(
                    "SELECT $1::timestamptz>clock_timestamp()",
                    worker["lease_expires_at"],
                )
            if authority_mismatch:
                raise StaleOwner("regional renewal worker authority is stale")
            await conn.execute(
                """UPDATE join_workers SET lease_expires_at=GREATEST(
                     lease_expires_at,clock_timestamp()
                       +$4::double precision*interval '1 second')
                   WHERE revision_id=$1 AND participant_id=ANY($2::text[])
                     AND owner_id=$3""",
                self.revision.revision_id,
                [row["participant_id"] for row in attempt_workers],
                owner_id,
                lease_seconds,
            )

    async def replay_worker(
        self,
        plan_id: str,
        runtime: PostgresJoinRuntime,
        task: Any,
        owner_id: str,
        owner_epoch: int,
        *,
        fault: FaultHook = None,
    ) -> dict[str, Any]:
        participant = runtime.participant
        while True:
            async with self.pool.acquire() as conn, conn.transaction():
                generation = await conn.fetchrow(
                    """SELECT * FROM regional_recovery_workers_v1
                       WHERE plan_id=$1 AND participant_id=$2 FOR UPDATE""",
                    plan_id,
                    participant,
                )
                if generation is None:
                    raise PostgresProtocolError("worker is outside regional recovery")
                attempt = await conn.fetchrow(
                    """SELECT aw.* FROM regional_recovery_attempt_workers_v1 aw
                       WHERE aw.plan_id=$1 AND aw.participant_id=$2
                       ORDER BY aw.attempt_ordinal DESC LIMIT 1""",
                    plan_id,
                    participant,
                )
                if (
                    attempt["effective_owner_id"],
                    attempt["effective_owner_epoch"],
                ) != (owner_id, owner_epoch):
                    raise StaleOwner("regional recovery worker fenced")
                worker = await conn.fetchrow(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=$2 FOR UPDATE""",
                    self.revision.revision_id,
                    participant,
                )
                if (worker["owner_id"], worker["owner_epoch"], worker["phase"]) != (
                    owner_id,
                    owner_epoch,
                    "RESTORING",
                ):
                    raise StaleOwner("regional recovery worker authority changed")
                if not await conn.fetchval(
                    "SELECT $1::timestamptz>clock_timestamp()",
                    worker["lease_expires_at"],
                ):
                    raise StaleOwner("regional recovery worker lease expired")
                progress = await conn.fetchrow(
                    """SELECT * FROM join_recoveries_v3
                       WHERE revision_id=$1 AND participant_id=$2
                         AND recovery_id=$3 FOR UPDATE""",
                    self.revision.revision_id,
                    participant,
                    plan_id,
                )
                if progress["status"] == "READY":
                    return dict(decoded(progress["state"]))
                if progress["replayed_through"] == progress["target_apply"]:
                    if decoded(progress["state"]) != decoded(worker["state"]):
                        raise PostgresProtocolError(
                            "regional replay differs from durable terminal state"
                        )
                    await conn.execute(
                        """UPDATE join_recoveries_v3 SET status='READY'
                           WHERE revision_id=$1 AND participant_id=$2
                             AND recovery_id=$3""",
                        self.revision.revision_id,
                        participant,
                        plan_id,
                    )
                    await conn.execute(
                        """UPDATE regional_recovery_workers_v1 SET status='READY'
                           WHERE plan_id=$1 AND participant_id=$2""",
                        plan_id,
                        participant,
                    )
                    hit(fault, "regional_worker_ready_before_commit")
                    state = dict(decoded(progress["state"]))
                    done = True
                else:
                    next_apply = progress["replayed_through"] + 1
                    turn = await conn.fetchrow(
                        """SELECT * FROM join_turns_v3 WHERE revision_id=$1
                           AND participant_id=$2 AND apply_ordinal=$3""",
                        self.revision.revision_id,
                        participant,
                        next_apply,
                    )
                    receipt = (
                        None
                        if turn is None
                        else await conn.fetchrow(
                            """SELECT * FROM join_receipts_v3 WHERE revision_id=$1
                           AND participant_id=$2 AND arrival_ordinal=$3""",
                            self.revision.revision_id,
                            participant,
                            turn["arrival_ordinal"],
                        )
                    )
                    if turn is None or receipt is None:
                        raise PostgresProtocolError("regional replay mailbox has a gap")
                    source = await runtime._source(
                        conn,
                        receipt["channel"],
                        receipt["partition_id"],
                        receipt["data_sequence"],
                    )
                    before = decoded(progress["state"])
                    payload = bytes(source["payload"])
                    replayed_before = progress["replayed_through"]
                    done = False
            if done:
                hit(fault, "regional_worker_ready_after_commit")
                return state
            terminal, after, output = await runtime._execute(
                task, before, dict(receipt), payload
            )
            output_digest = (
                None if output is None else hashlib.sha256(output.payload).hexdigest()
            )
            async with self.pool.acquire() as conn, conn.transaction():
                generation = await conn.fetchrow(
                    """SELECT * FROM regional_recovery_workers_v1
                       WHERE plan_id=$1 AND participant_id=$2 FOR UPDATE""",
                    plan_id,
                    participant,
                )
                progress = await conn.fetchrow(
                    """SELECT * FROM join_recoveries_v3
                       WHERE revision_id=$1 AND participant_id=$2
                         AND recovery_id=$3 FOR UPDATE""",
                    self.revision.revision_id,
                    participant,
                    plan_id,
                )
                worker = await conn.fetchrow(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=$2 FOR UPDATE""",
                    self.revision.revision_id,
                    participant,
                )
                attempt = await conn.fetchrow(
                    """SELECT aw.* FROM regional_recovery_attempt_workers_v1 aw
                       WHERE aw.plan_id=$1 AND aw.participant_id=$2
                       ORDER BY aw.attempt_ordinal DESC LIMIT 1""",
                    plan_id,
                    participant,
                )
                if (
                    generation["status"],
                    attempt["effective_owner_id"],
                    attempt["effective_owner_epoch"],
                    progress["replayed_through"],
                    worker["owner_id"],
                    worker["owner_epoch"],
                    worker["phase"],
                ) != (
                    "RESTORING",
                    owner_id,
                    owner_epoch,
                    replayed_before,
                    owner_id,
                    owner_epoch,
                    "RESTORING",
                ):
                    raise StaleOwner("regional replay claim changed")
                if not await conn.fetchval(
                    "SELECT $1::timestamptz>clock_timestamp()",
                    worker["lease_expires_at"],
                ):
                    raise StaleOwner("regional recovery worker lease expired")
                current_turn = await conn.fetchrow(
                    """SELECT * FROM join_turns_v3 WHERE revision_id=$1
                       AND participant_id=$2 AND apply_ordinal=$3""",
                    self.revision.revision_id,
                    participant,
                    replayed_before + 1,
                )
                if (
                    before,
                    after,
                    terminal,
                    output_digest,
                ) != (
                    decoded(current_turn["before_state"]),
                    decoded(current_turn["after_state"]),
                    current_turn["terminal"],
                    current_turn["output_digest"],
                ):
                    raise PostgresProtocolError(
                        "regional client replay diverged from terminal identity"
                    )
                await self._verify_stable_output(
                    conn, runtime, dict(receipt), current_turn, output
                )
                await conn.execute(
                    """UPDATE join_recoveries_v3
                       SET replayed_through=$4,state=$5::jsonb
                       WHERE revision_id=$1 AND participant_id=$2
                         AND recovery_id=$3""",
                    self.revision.revision_id,
                    participant,
                    plan_id,
                    current_turn["apply_ordinal"],
                    canonical(after),
                )
                hit(fault, "regional_replay_turn_before_commit")
            hit(fault, "regional_replay_turn_after_commit")

    async def _verify_stable_output(
        self,
        conn: asyncpg.Connection,
        runtime: PostgresJoinRuntime,
        receipt: dict[str, Any],
        turn: asyncpg.Record,
        output: Any,
    ) -> None:
        if output is None:
            if (
                turn["output_record_id"] is not None
                or turn["output_digest"] is not None
            ):
                raise PostgresProtocolError("regional RETURN has a forged output")
            return
        identity = digest(
            [
                self.revision.revision_id,
                runtime.participant,
                receipt["channel"],
                receipt["partition_id"],
                receipt["record_id"],
            ]
        )
        output_id = "join-output:" + identity
        if turn["output_record_id"] != output_id:
            raise PostgresProtocolError("regional output logical identity drift")
        declared = runtime.spec["outputs"][0]
        row = await conn.fetchrow(
            """SELECT r.*,o.payload,o.published_at FROM records r
               JOIN objects o USING(object_key)
               WHERE r.record_id=$1""",
            output_id,
        )
        if (
            row is None
            or row["published_at"] is None
            or (
                row["channel"],
                row["partition_id"],
                row["object_key"],
                row["payload_digest"],
                row["schema_id"],
                bytes(row["payload"]),
            )
            != (
                declared["channel"],
                declared["partition"],
                content_object_key(output.payload, output.schema_id),
                hashlib.sha256(output.payload).hexdigest(),
                output.schema_id,
                output.payload,
            )
        ):
            raise PostgresProtocolError("regional output payload identity drift")

    async def activate_region(self, plan_id: str) -> None:
        """Atomically publish every READY worker generation as `LIVE`."""
        async with self.pool.acquire() as conn, conn.transaction():
            generations = list(
                await conn.fetch(
                    """SELECT * FROM regional_recovery_workers_v1
                       WHERE plan_id=$1 ORDER BY participant_id FOR UPDATE""",
                    plan_id,
                )
            )
            if not generations or any(row["status"] != "READY" for row in generations):
                raise PostgresProtocolError(
                    "regional activation requires every worker READY"
                )
            for generation in generations:
                recovery = await conn.fetchrow(
                    """SELECT * FROM join_recoveries_v3 WHERE revision_id=$1
                       AND participant_id=$2 AND recovery_id=$3 FOR UPDATE""",
                    generation["revision_id"],
                    generation["participant_id"],
                    plan_id,
                )
                worker = await conn.fetchrow(
                    """SELECT * FROM join_workers WHERE revision_id=$1
                       AND participant_id=$2 FOR UPDATE""",
                    generation["revision_id"],
                    generation["participant_id"],
                )
                attempt = await conn.fetchrow(
                    """SELECT * FROM regional_recovery_attempt_workers_v1
                       WHERE plan_id=$1 AND participant_id=$2
                       ORDER BY attempt_ordinal DESC LIMIT 1""",
                    plan_id,
                    generation["participant_id"],
                )
                if (
                    recovery["status"],
                    recovery["replayed_through"],
                    recovery["target_apply"],
                    decoded(recovery["state"]),
                    worker["owner_id"],
                    worker["owner_epoch"],
                    worker["phase"],
                    decoded(worker["state"]),
                ) != (
                    "READY",
                    generation["target_apply"],
                    generation["target_apply"],
                    decoded(worker["state"]),
                    attempt["effective_owner_id"],
                    attempt["effective_owner_epoch"],
                    "RESTORING",
                    decoded(recovery["state"]),
                ):
                    raise PostgresProtocolError(
                        "regional activation state or generation drift"
                    )
                if not await conn.fetchval(
                    "SELECT $1::timestamptz>clock_timestamp()",
                    worker["lease_expires_at"],
                ):
                    raise StaleOwner("regional activation lease expired")
            for generation in generations:
                await conn.execute(
                    """UPDATE join_workers SET phase='LIVE'
                       WHERE revision_id=$1 AND participant_id=$2""",
                    generation["revision_id"],
                    generation["participant_id"],
                )
            for generation in generations:
                await conn.execute(
                    """UPDATE regional_recovery_workers_v1 SET status='ACTIVE'
                       WHERE plan_id=$1 AND participant_id=$2""",
                    plan_id,
                    generation["participant_id"],
                )

    async def generations(self, plan_id: str) -> tuple[RegionalWorkerGeneration, ...]:
        async with self.pool.acquire() as conn:
            rows = await self._generation_rows(conn, plan_id)
        return tuple(self._generation(row) for row in rows)

    @staticmethod
    async def _generation_rows(
        conn: asyncpg.Connection, plan_id: str
    ) -> list[asyncpg.Record]:
        return list(
            await conn.fetch(
                """SELECT g.*,aw.attempt_ordinal AS current_attempt_ordinal,
                          aw.effective_owner_id AS current_owner_id,
                          aw.effective_owner_epoch AS current_owner_epoch
                   FROM regional_recovery_workers_v1 g
                   JOIN regional_recovery_attempt_workers_v1 aw
                     ON aw.plan_id=g.plan_id AND aw.participant_id=g.participant_id
                   WHERE g.plan_id=$1 AND aw.attempt_ordinal=(
                     SELECT max(attempt_ordinal)
                     FROM regional_recovery_attempts_v1 WHERE plan_id=g.plan_id)
                   ORDER BY g.participant_id""",
                plan_id,
            )
        )

    @staticmethod
    def _generation(row: asyncpg.Record) -> RegionalWorkerGeneration:
        return RegionalWorkerGeneration(
            participant_id=row["participant_id"],
            old_owner_id=row["old_owner_id"],
            old_owner_epoch=row["old_owner_epoch"],
            recovery_owner_id=row["current_owner_id"],
            recovery_owner_epoch=row["current_owner_epoch"],
            snapshot_object_key=row["snapshot_object_key"],
            mailbox_highwater=row["mailbox_highwater"],
            target_apply=row["target_apply"],
            status=row["status"],
            attempt_ordinal=row["current_attempt_ordinal"],
        )
