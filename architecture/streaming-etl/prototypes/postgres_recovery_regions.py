"""Focused PostgreSQL model for selective recovery-region planning.

This spike proves only planner theory for DS-031c.  It binds an immutable DAG,
one globally completed checkpoint, one coordinator recovery fence, and exact
replay availability at every edge entering the selected region.  It does not
restart workers, restore operator generations, route barriers through a DAG,
or provide Prefect integration.  DS-031b and DS-031c therefore remain open.
"""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_barrier_coordinator import (
    PostgresBarrierCoordinator,
    canonical,
    decoded,
    digest,
)
from postgres_stream_v2 import PostgresProtocolError


class RegionPlanningError(PostgresProtocolError):
    """The declared graph or requested recovery plan is invalid."""


class UnrecoverableBoundary(RegionPlanningError):
    """Recovery reached a source that has no durable rewind contract."""


@dataclass(frozen=True)
class RegionNode:
    node_id: str
    kind: str = "OPERATOR"
    source_replayable: bool = False


@dataclass(frozen=True)
class RegionEdge:
    edge_id: str
    source: str
    target: str
    channel: str
    partition: str
    replayable: bool = True


@dataclass(frozen=True)
class ReplayBoundary:
    edge_id: str
    channel: str
    partition: str
    cut_log_sequence: int
    cut_data_sequence: int
    target_log_sequence: int
    target_data_sequence: int
    availability_digest: str


@dataclass(frozen=True)
class RegionRecoveryPlan:
    plan_id: str
    request_id: str
    revision_id: str
    checkpoint_id: str
    failed_node: str
    graph_manifest_digest: str
    completion_digest: str
    fence_authority_ordinal: int
    region: tuple[str, ...]
    boundaries: tuple[ReplayBoundary, ...]
    source_rewinds: tuple[str, ...]
    reasons: dict[str, str]
    plan_digest: str


def _nonempty(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


class PostgresRecoveryRegionPlanner:
    """Create immutable selective-recovery plans in coordinator storage."""

    def __init__(self, coordinator: PostgresBarrierCoordinator):
        self.coordinator = coordinator
        self.pool = coordinator.pool
        self.schema = coordinator.schema

    @asynccontextmanager
    async def _connection(self, connection: asyncpg.Connection | None):
        if connection is not None:
            yield connection
        else:
            async with self.pool.acquire() as conn:
                yield conn

    @classmethod
    async def install(
        cls, coordinator: PostgresBarrierCoordinator
    ) -> PostgresRecoveryRegionPlanner:
        planner = cls(coordinator)
        await planner._initialize()
        return planner

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS region_graph_nodes_v1 (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            node_id text NOT NULL,
            node_kind text NOT NULL CHECK(node_kind IN('SOURCE','OPERATOR')),
            source_replayable boolean NOT NULL,
            node_digest text NOT NULL,
            PRIMARY KEY(revision_id,node_id)
        );
        CREATE TABLE IF NOT EXISTS region_graph_edges_v1 (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            edge_id text NOT NULL,
            source_node text NOT NULL,
            target_node text NOT NULL,
            channel text NOT NULL,
            partition_id text NOT NULL,
            replayable boolean NOT NULL,
            edge_digest text NOT NULL,
            PRIMARY KEY(revision_id,edge_id),
            FOREIGN KEY(revision_id,source_node)
              REFERENCES region_graph_nodes_v1(revision_id,node_id),
            FOREIGN KEY(revision_id,target_node)
              REFERENCES region_graph_nodes_v1(revision_id,node_id),
            FOREIGN KEY(channel,partition_id)
              REFERENCES partitions(channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS region_graph_seals_v1 (
            revision_id text PRIMARY KEY REFERENCES deployment_revisions(revision_id),
            manifest_digest text NOT NULL UNIQUE,
            manifest_json jsonb NOT NULL,
            sealed_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS region_edge_cuts_v1 (
            checkpoint_id text NOT NULL REFERENCES checkpoint_completions_v2(checkpoint_id),
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            edge_id text NOT NULL,
            channel text NOT NULL,
            partition_id text NOT NULL,
            barrier_log_sequence bigint NOT NULL CHECK(barrier_log_sequence > 0),
            cut_data_sequence bigint NOT NULL CHECK(cut_data_sequence >= 0),
            intent_digest text NOT NULL,
            retention_ref_id text NOT NULL UNIQUE,
            cut_digest text NOT NULL,
            PRIMARY KEY(checkpoint_id,edge_id),
            FOREIGN KEY(revision_id,edge_id)
              REFERENCES region_graph_edges_v1(revision_id,edge_id),
            FOREIGN KEY(channel,partition_id,barrier_log_sequence)
              REFERENCES partition_log_entries(channel,partition_id,log_sequence),
            FOREIGN KEY(retention_ref_id) REFERENCES checkpoints(checkpoint_id)
        );
        CREATE TABLE IF NOT EXISTS region_recovery_plans_v1 (
            plan_id text PRIMARY KEY,
            request_id text NOT NULL UNIQUE,
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            checkpoint_id text NOT NULL REFERENCES checkpoint_completions_v2(checkpoint_id),
            failed_node text NOT NULL,
            graph_manifest_digest text NOT NULL,
            completion_digest text NOT NULL,
            completion_authority_ordinal bigint NOT NULL,
            fence_authority_ordinal bigint NOT NULL,
            writer_epoch bigint NOT NULL,
            region_json jsonb NOT NULL,
            boundaries_json jsonb NOT NULL,
            source_rewinds_json jsonb NOT NULL,
            reasons_json jsonb NOT NULL,
            plan_digest text NOT NULL UNIQUE,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );

        CREATE OR REPLACE FUNCTION region_reject_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'region authority is immutable';
        END $$;
        CREATE OR REPLACE FUNCTION region_reject_graph_insert_after_seal()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS(SELECT 1 FROM region_graph_seals_v1
                      WHERE revision_id=NEW.revision_id)
            THEN RAISE EXCEPTION 'region graph is sealed'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION region_validate_edge_cut()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE log_row partition_log_entries%ROWTYPE;
        DECLARE edge_row region_graph_edges_v1%ROWTYPE;
        DECLARE root_row checkpoints%ROWTYPE;
        BEGIN
            IF NOT EXISTS(SELECT 1 FROM checkpoint_completions_v2 c
                          JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                          JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                          WHERE c.checkpoint_id=NEW.checkpoint_id
                            AND i.revision_id=NEW.revision_id
                            AND l.status='COMPLETED')
            THEN RAISE EXCEPTION 'edge cut lacks completed checkpoint'; END IF;
            SELECT * INTO edge_row FROM region_graph_edges_v1
              WHERE revision_id=NEW.revision_id AND edge_id=NEW.edge_id;
            SELECT * INTO log_row FROM partition_log_entries
              WHERE channel=NEW.channel AND partition_id=NEW.partition_id
                AND log_sequence=NEW.barrier_log_sequence;
            SELECT * INTO root_row FROM checkpoints
              WHERE checkpoint_id=NEW.retention_ref_id;
            IF edge_row.edge_id IS NULL
               OR (edge_row.channel,edge_row.partition_id)
                    IS DISTINCT FROM (NEW.channel,NEW.partition_id)
               OR log_row.entry_kind IS DISTINCT FROM 'BARRIER'
               OR log_row.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
               OR log_row.cut_data_sequence IS DISTINCT FROM NEW.cut_data_sequence
               OR log_row.intent_digest IS DISTINCT FROM NEW.intent_digest
               OR root_row.status IS DISTINCT FROM 'SEALED'
               OR (root_row.channel,root_row.partition_id,root_row.cut_sequence)
                    IS DISTINCT FROM
                      (NEW.channel,NEW.partition_id,NEW.cut_data_sequence)
            THEN RAISE EXCEPTION 'edge cut authority mismatch'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION region_validate_plan()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE completion checkpoint_completions_v2%ROWTYPE;
        DECLARE event coordinator_authority_events%ROWTYPE;
        BEGIN
            IF NOT EXISTS(SELECT 1 FROM region_graph_seals_v1
                          WHERE revision_id=NEW.revision_id
                            AND manifest_digest=NEW.graph_manifest_digest)
            THEN RAISE EXCEPTION 'region plan graph authority mismatch'; END IF;
            SELECT * INTO completion FROM checkpoint_completions_v2
              WHERE checkpoint_id=NEW.checkpoint_id;
            SELECT * INTO event FROM coordinator_authority_events
              WHERE revision_id=NEW.revision_id
                AND authority_ordinal=NEW.fence_authority_ordinal;
            IF completion.completion_digest IS DISTINCT FROM NEW.completion_digest
               OR completion.completion_authority_ordinal
                    IS DISTINCT FROM NEW.completion_authority_ordinal
               OR completion.completion_authority_ordinal
                    >= NEW.fence_authority_ordinal
               OR event.event_type IS DISTINCT FROM 'RECOVERY_FENCE'
               OR event.recovery_id IS DISTINCT FROM NEW.plan_id
               OR event.writer_epoch IS DISTINCT FROM NEW.writer_epoch
               OR event.payload->>'request_id' IS DISTINCT FROM NEW.request_id
               OR event.payload->>'kind' IS DISTINCT FROM 'REGION'
               OR event.payload->>'failed_node' IS DISTINCT FROM NEW.failed_node
               OR event.payload->>'graph_manifest_digest'
                    IS DISTINCT FROM NEW.graph_manifest_digest
            THEN RAISE EXCEPTION 'region plan completion/fence authority mismatch'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION region_guard_replay_root_release()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.status='SEALED' AND NEW.status='RELEASED' AND EXISTS(
                SELECT 1 FROM region_edge_cuts_v1 c
                 WHERE c.retention_ref_id=OLD.checkpoint_id)
            THEN RAISE EXCEPTION 'region replay root remains referenced'; END IF;
            RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS region_node_insert_guard ON region_graph_nodes_v1;
        CREATE TRIGGER region_node_insert_guard BEFORE INSERT ON region_graph_nodes_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_graph_insert_after_seal();
        DROP TRIGGER IF EXISTS region_edge_insert_guard ON region_graph_edges_v1;
        CREATE TRIGGER region_edge_insert_guard BEFORE INSERT ON region_graph_edges_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_graph_insert_after_seal();
        DROP TRIGGER IF EXISTS region_node_immutable ON region_graph_nodes_v1;
        CREATE TRIGGER region_node_immutable BEFORE UPDATE OR DELETE ON region_graph_nodes_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_mutation();
        DROP TRIGGER IF EXISTS region_edge_immutable ON region_graph_edges_v1;
        CREATE TRIGGER region_edge_immutable BEFORE UPDATE OR DELETE ON region_graph_edges_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_mutation();
        DROP TRIGGER IF EXISTS region_graph_seal_immutable ON region_graph_seals_v1;
        CREATE TRIGGER region_graph_seal_immutable BEFORE UPDATE OR DELETE ON region_graph_seals_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_mutation();
        DROP TRIGGER IF EXISTS region_edge_cut_insert_guard ON region_edge_cuts_v1;
        CREATE TRIGGER region_edge_cut_insert_guard BEFORE INSERT ON region_edge_cuts_v1
          FOR EACH ROW EXECUTE FUNCTION region_validate_edge_cut();
        DROP TRIGGER IF EXISTS region_edge_cut_immutable ON region_edge_cuts_v1;
        CREATE TRIGGER region_edge_cut_immutable BEFORE UPDATE OR DELETE ON region_edge_cuts_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_mutation();
        DROP TRIGGER IF EXISTS region_plan_insert_guard ON region_recovery_plans_v1;
        CREATE TRIGGER region_plan_insert_guard BEFORE INSERT ON region_recovery_plans_v1
          FOR EACH ROW EXECUTE FUNCTION region_validate_plan();
        DROP TRIGGER IF EXISTS region_plan_immutable ON region_recovery_plans_v1;
        CREATE TRIGGER region_plan_immutable BEFORE UPDATE OR DELETE ON region_recovery_plans_v1
          FOR EACH ROW EXECUTE FUNCTION region_reject_mutation();
        DROP TRIGGER IF EXISTS region_replay_root_release_guard ON checkpoints;
        CREATE TRIGGER region_replay_root_release_guard BEFORE UPDATE OF status
          ON checkpoints FOR EACH ROW EXECUTE FUNCTION region_guard_replay_root_release();
        """
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)

    async def declare_graph(
        self,
        revision_id: str,
        nodes: list[RegionNode],
        edges: list[RegionEdge],
    ) -> str:
        node_manifest, edge_manifest = self._validate_graph(nodes, edges)
        manifest = {
            "revision_id": _nonempty(revision_id, "revision_id"),
            "nodes": node_manifest,
            "edges": edge_manifest,
        }
        manifest_digest = digest(manifest)
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            existing = await conn.fetchrow(
                "SELECT * FROM region_graph_seals_v1 WHERE revision_id=$1",
                revision_id,
            )
            if existing is not None:
                if (
                    existing["manifest_digest"],
                    decoded(existing["manifest_json"]),
                ) != (manifest_digest, manifest):
                    raise RegionPlanningError("region graph declaration drift")
                return manifest_digest
            participants = {
                row["participant_id"]
                for row in await conn.fetch(
                    "SELECT participant_id FROM revision_participants WHERE revision_id=$1",
                    revision_id,
                )
            }
            operators = {
                item["node_id"] for item in node_manifest if item["kind"] == "OPERATOR"
            }
            if not operators.issubset(participants):
                raise RegionPlanningError(
                    "operator node is absent from revision participants"
                )
            for item in node_manifest:
                await conn.execute(
                    """INSERT INTO region_graph_nodes_v1 VALUES($1,$2,$3,$4,$5)""",
                    revision["revision_id"],
                    item["node_id"],
                    item["kind"],
                    item["source_replayable"],
                    digest(item),
                )
            for item in edge_manifest:
                await conn.execute(
                    """INSERT INTO region_graph_edges_v1 VALUES(
                       $1,$2,$3,$4,$5,$6,$7,$8)""",
                    revision_id,
                    item["edge_id"],
                    item["source"],
                    item["target"],
                    item["channel"],
                    item["partition"],
                    item["replayable"],
                    digest(item),
                )
            await conn.execute(
                """INSERT INTO region_graph_seals_v1(
                   revision_id,manifest_digest,manifest_json)
                   VALUES($1,$2,$3::jsonb)""",
                revision_id,
                manifest_digest,
                canonical(manifest),
            )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        return manifest_digest

    @staticmethod
    def _validate_graph(
        nodes: list[RegionNode], edges: list[RegionEdge]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not nodes or not edges:
            raise ValueError("region graph requires nodes and edges")
        node_ids = [_nonempty(node.node_id, "node_id") for node in nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("region node IDs must be unique")
        node_manifest = []
        for node in nodes:
            if node.kind not in {"SOURCE", "OPERATOR"}:
                raise ValueError("region node kind is invalid")
            if node.kind != "SOURCE" and node.source_replayable:
                raise ValueError("only source nodes declare rewind capability")
            node_manifest.append(
                {
                    "node_id": node.node_id,
                    "kind": node.kind,
                    "source_replayable": node.source_replayable,
                }
            )
        node_manifest.sort(key=lambda item: item["node_id"])
        edge_ids = [_nonempty(edge.edge_id, "edge_id") for edge in edges]
        if len(edge_ids) != len(set(edge_ids)):
            raise ValueError("region edge IDs must be unique")
        edge_manifest = []
        producers: dict[tuple[str, str], str] = {}
        adjacency = {node_id: [] for node_id in node_ids}
        incoming = {node_id: 0 for node_id in node_ids}
        kinds = {node.node_id: node.kind for node in nodes}
        for edge in edges:
            if edge.source not in adjacency or edge.target not in adjacency:
                raise ValueError("region edge endpoint is unknown")
            if edge.source == edge.target or kinds[edge.target] == "SOURCE":
                raise ValueError("region edge direction is invalid")
            _nonempty(edge.channel, "channel")
            _nonempty(edge.partition, "partition")
            location = (edge.channel, edge.partition)
            producer = producers.setdefault(location, edge.source)
            if producer != edge.source:
                raise ValueError("physical edge location has multiple producers")
            adjacency[edge.source].append(edge.target)
            incoming[edge.target] += 1
            edge_manifest.append(
                {
                    "edge_id": edge.edge_id,
                    "source": edge.source,
                    "target": edge.target,
                    "channel": edge.channel,
                    "partition": edge.partition,
                    "replayable": edge.replayable,
                }
            )
        queue = sorted(node_id for node_id, count in incoming.items() if count == 0)
        visited: list[str] = []
        while queue:
            current = queue.pop(0)
            visited.append(current)
            for target in sorted(adjacency[current]):
                incoming[target] -= 1
                if incoming[target] == 0:
                    queue.append(target)
                    queue.sort()
        if len(visited) != len(nodes):
            raise ValueError("region graph must be acyclic")
        edge_manifest.sort(key=lambda item: item["edge_id"])
        return node_manifest, edge_manifest

    async def bind_edge_cut(
        self, revision_id: str, checkpoint_id: str, edge_id: str
    ) -> str:
        async with self.pool.acquire() as conn, conn.transaction():
            await self.coordinator._locked_revision(conn, revision_id)
            edge = await conn.fetchrow(
                """SELECT * FROM region_graph_edges_v1
                   WHERE revision_id=$1 AND edge_id=$2""",
                revision_id,
                edge_id,
            )
            if edge is None:
                raise RegionPlanningError("unknown region edge")
            completion = await conn.fetchrow(
                """SELECT c.*,i.intent_digest FROM checkpoint_completions_v2 c
                   JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE c.checkpoint_id=$1 AND i.revision_id=$2
                     AND l.status='COMPLETED'""",
                checkpoint_id,
                revision_id,
            )
            if completion is None:
                raise RegionPlanningError("edge cut checkpoint is not completed")
            barriers = await conn.fetch(
                """SELECT * FROM partition_log_entries
                   WHERE channel=$1 AND partition_id=$2
                     AND checkpoint_id=$3 AND entry_kind='BARRIER'""",
                edge["channel"],
                edge["partition_id"],
                checkpoint_id,
            )
            if len(barriers) != 1:
                raise RegionPlanningError("edge cut barrier is missing or ambiguous")
            barrier = barriers[0]
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{edge['channel']}|{edge['partition_id']}",
            )
            part = await conn.fetchrow(
                """SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2
                   FOR UPDATE""",
                edge["channel"],
                edge["partition_id"],
            )
            floor = await conn.fetchval(
                """SELECT COALESCE(
                     (SELECT min(sequence) FROM records
                       WHERE channel=$1 AND partition_id=$2),$3)""",
                edge["channel"],
                edge["partition_id"],
                part["next_sequence"],
            )
            if barrier["cut_data_sequence"] + 1 < floor:
                raise RegionPlanningError("edge replay suffix was already reclaimed")
            retention_ref = "region-cut:" + digest(
                [revision_id, checkpoint_id, edge_id]
            )
            await conn.execute(
                """INSERT INTO checkpoints VALUES($1,$2,$3,$4,'SEALED')
                   ON CONFLICT(checkpoint_id) DO NOTHING""",
                retention_ref,
                edge["channel"],
                edge["partition_id"],
                barrier["cut_data_sequence"],
            )
            identity = {
                "checkpoint_id": checkpoint_id,
                "revision_id": revision_id,
                "edge_id": edge_id,
                "channel": edge["channel"],
                "partition": edge["partition_id"],
                "barrier_log_sequence": barrier["log_sequence"],
                "cut_data_sequence": barrier["cut_data_sequence"],
                "intent_digest": barrier["intent_digest"],
                "retention_ref_id": retention_ref,
            }
            cut_digest = digest(identity)
            await conn.execute(
                """INSERT INTO region_edge_cuts_v1 VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                   ON CONFLICT(checkpoint_id,edge_id) DO NOTHING""",
                checkpoint_id,
                revision_id,
                edge_id,
                edge["channel"],
                edge["partition_id"],
                barrier["log_sequence"],
                barrier["cut_data_sequence"],
                barrier["intent_digest"],
                retention_ref,
                cut_digest,
            )
            existing = await conn.fetchrow(
                """SELECT * FROM region_edge_cuts_v1
                   WHERE checkpoint_id=$1 AND edge_id=$2""",
                checkpoint_id,
                edge_id,
            )
            if existing["cut_digest"] != cut_digest:
                raise RegionPlanningError("edge cut identity drift")
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        return cut_digest

    async def create_plan(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        request_id: str,
        failed_node: str,
        checkpoint_id: str,
        *,
        connection: asyncpg.Connection | None = None,
    ) -> RegionRecoveryPlan:
        _nonempty(request_id, "request_id")
        _nonempty(failed_node, "failed_node")
        async with self._connection(connection) as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            self.coordinator._check_graph(revision, graph_version, graph_digest)
            await self.coordinator._assert_owner(
                conn, revision_id, owner_id, owner_epoch
            )
            existing = await conn.fetchrow(
                "SELECT * FROM region_recovery_plans_v1 WHERE request_id=$1",
                request_id,
            )
            if existing is not None:
                if (
                    existing["revision_id"],
                    existing["checkpoint_id"],
                    existing["failed_node"],
                ) != (revision_id, checkpoint_id, failed_node):
                    raise RegionPlanningError("region recovery request identity drift")
                return self._plan(existing)
            seal = await conn.fetchrow(
                "SELECT * FROM region_graph_seals_v1 WHERE revision_id=$1",
                revision_id,
            )
            if seal is None:
                raise RegionPlanningError("region graph is not declared")
            completion = await conn.fetchrow(
                """SELECT c.*,i.revision_id,l.status
                   FROM checkpoint_completions_v2 c
                   JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE c.checkpoint_id=$1""",
                checkpoint_id,
            )
            if completion is None or (
                completion["revision_id"],
                completion["status"],
            ) != (revision_id, "COMPLETED"):
                raise RegionPlanningError(
                    "region recovery requires a completed checkpoint"
                )
            nodes = {
                row["node_id"]: row
                for row in await conn.fetch(
                    "SELECT * FROM region_graph_nodes_v1 WHERE revision_id=$1",
                    revision_id,
                )
            }
            edges = list(
                await conn.fetch(
                    """SELECT * FROM region_graph_edges_v1
                       WHERE revision_id=$1 ORDER BY edge_id""",
                    revision_id,
                )
            )
            if failed_node not in nodes:
                raise RegionPlanningError("failed node is absent from region graph")
            # The authority ordinal is monotonic across rolling metadata GC.
            # Including it prevents an opaque request reused below the horizon
            # from aliasing a retired plan's semantic identity.
            plan_id = "region-recovery:" + digest(
                [revision_id, request_id, revision["next_authority_ordinal"]]
            )
            fence_ordinal = await self.coordinator._authority_event(
                conn,
                revision,
                "RECOVERY_FENCE",
                owner_epoch,
                {
                    "request_id": request_id,
                    "kind": "REGION",
                    "failed_node": failed_node,
                    "graph_manifest_digest": seal["manifest_digest"],
                },
                recovery_id=plan_id,
            )
            if completion["completion_authority_ordinal"] >= fence_ordinal:
                raise RegionPlanningError("completion does not precede recovery fence")
            region = self._descendants(failed_node, edges)
            reasons = {failed_node: "failed node"}
            for node_id in sorted(region - {failed_node}):
                reasons[node_id] = f"downstream of {failed_node}"
            availability_reasons: dict[str, str] = {}
            while True:
                expanded = False
                for edge in self._incoming_boundaries(region, edges):
                    boundary, unavailable = await self._availability(
                        conn, checkpoint_id, edge
                    )
                    if boundary is not None:
                        continue
                    source = nodes[edge["source_node"]]
                    reason = unavailable or "unknown availability failure"
                    availability_reasons[edge["edge_id"]] = reason
                    if not reason.startswith("edge is declared non-replayable;"):
                        raise UnrecoverableBoundary(
                            f"edge {edge['edge_id']} is unavailable outside the "
                            f"retained durable horizon ({reason})"
                        )
                    if (
                        source["node_kind"] == "SOURCE"
                        and not source["source_replayable"]
                    ):
                        raise UnrecoverableBoundary(
                            f"edge {edge['edge_id']} is unavailable ({reason}); "
                            f"source {source['node_id']} has no rewind contract"
                        )
                    if source["node_id"] not in region:
                        region.add(source["node_id"])
                        reasons[source["node_id"]] = (
                            f"expanded through unavailable edge {edge['edge_id']}: "
                            f"{reason}"
                        )
                        expanded = True
                if not expanded:
                    break
            boundaries = []
            for edge in self._incoming_boundaries(region, edges):
                boundary, unavailable = await self._availability(
                    conn, checkpoint_id, edge
                )
                if boundary is None:
                    raise RegionPlanningError(
                        f"boundary {edge['edge_id']} changed availability: {unavailable}"
                    )
                boundaries.append(boundary)
            boundaries.sort(key=lambda item: item["edge_id"])
            source_rewinds = sorted(
                node_id for node_id in region if nodes[node_id]["node_kind"] == "SOURCE"
            )
            plan_identity = {
                "plan_id": plan_id,
                "request_id": request_id,
                "revision_id": revision_id,
                "checkpoint_id": checkpoint_id,
                "failed_node": failed_node,
                "graph_manifest_digest": seal["manifest_digest"],
                "completion_digest": completion["completion_digest"],
                "completion_authority_ordinal": completion[
                    "completion_authority_ordinal"
                ],
                "fence_authority_ordinal": fence_ordinal,
                "region": sorted(region),
                "boundaries": boundaries,
                "source_rewinds": source_rewinds,
                "reasons": reasons,
            }
            plan_digest = digest(plan_identity)
            await conn.execute(
                """INSERT INTO region_recovery_plans_v1(
                   plan_id,request_id,revision_id,checkpoint_id,failed_node,
                   graph_manifest_digest,completion_digest,
                   completion_authority_ordinal,fence_authority_ordinal,
                   writer_epoch,region_json,boundaries_json,source_rewinds_json,
                   reasons_json,plan_digest)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,
                          $13::jsonb,$14::jsonb,$15)""",
                plan_id,
                request_id,
                revision_id,
                checkpoint_id,
                failed_node,
                seal["manifest_digest"],
                completion["completion_digest"],
                completion["completion_authority_ordinal"],
                fence_ordinal,
                owner_epoch,
                canonical(sorted(region)),
                canonical(boundaries),
                canonical(source_rewinds),
                canonical(reasons),
                plan_digest,
            )
            row = await conn.fetchrow(
                "SELECT * FROM region_recovery_plans_v1 WHERE plan_id=$1", plan_id
            )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        return self._plan(row)

    @staticmethod
    def _descendants(seed: str, edges: list[asyncpg.Record]) -> set[str]:
        region = {seed}
        changed = True
        while changed:
            changed = False
            for edge in edges:
                if edge["source_node"] in region and edge["target_node"] not in region:
                    region.add(edge["target_node"])
                    changed = True
        return region

    @staticmethod
    def _incoming_boundaries(
        region: set[str], edges: list[asyncpg.Record]
    ) -> list[asyncpg.Record]:
        return [
            edge
            for edge in edges
            if edge["target_node"] in region and edge["source_node"] not in region
        ]

    async def _availability(
        self,
        conn: asyncpg.Connection,
        checkpoint_id: str,
        edge: asyncpg.Record,
    ) -> tuple[dict[str, Any] | None, str | None]:
        cut = await conn.fetchrow(
            """SELECT * FROM region_edge_cuts_v1
               WHERE checkpoint_id=$1 AND edge_id=$2""",
            checkpoint_id,
            edge["edge_id"],
        )
        if cut is None:
            return None, "checkpoint edge cut is unavailable"
        cut_identity = {
            "checkpoint_id": checkpoint_id,
            "revision_id": edge["revision_id"],
            "edge_id": edge["edge_id"],
            "channel": edge["channel"],
            "partition": edge["partition_id"],
            "barrier_log_sequence": cut["barrier_log_sequence"],
            "cut_data_sequence": cut["cut_data_sequence"],
            "intent_digest": cut["intent_digest"],
            "retention_ref_id": cut["retention_ref_id"],
        }
        if cut["cut_digest"] != digest(cut_identity):
            return None, "checkpoint edge-cut digest is invalid"
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"{self.schema}|{edge['channel']}|{edge['partition_id']}",
        )
        part = await conn.fetchrow(
            """SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2
               FOR UPDATE""",
            edge["channel"],
            edge["partition_id"],
        )
        # Root release and compaction use the same partition lock.  Re-read the
        # root after taking it so a release immediately before lock acquisition
        # cannot authorize a stale replay boundary.
        root = await conn.fetchrow(
            "SELECT * FROM checkpoints WHERE checkpoint_id=$1",
            cut["retention_ref_id"],
        )
        if root is None or (
            root["status"],
            root["channel"],
            root["partition_id"],
            root["cut_sequence"],
        ) != (
            "SEALED",
            edge["channel"],
            edge["partition_id"],
            cut["cut_data_sequence"],
        ):
            return None, "checkpoint replay root is unavailable"
        head = await conn.fetchrow(
            """SELECT * FROM partition_log_heads
               WHERE channel=$1 AND partition_id=$2 FOR UPDATE""",
            edge["channel"],
            edge["partition_id"],
        )
        if part is None or head is None:
            return None, "ordered partition authority is unavailable"
        target_log = head["next_log_sequence"] - 1
        target_data = part["next_sequence"] - 1
        if (
            cut["barrier_log_sequence"] > target_log
            or cut["cut_data_sequence"] > target_data
        ):
            return None, "checkpoint cut exceeds current partition head"
        logs = list(
            await conn.fetch(
                """SELECT * FROM partition_log_entries
                   WHERE channel=$1 AND partition_id=$2
                     AND log_sequence>$3 AND log_sequence<=$4
                   ORDER BY log_sequence""",
                edge["channel"],
                edge["partition_id"],
                cut["barrier_log_sequence"],
                target_log,
            )
        )
        expected_logs = list(range(cut["barrier_log_sequence"] + 1, target_log + 1))
        if [row["log_sequence"] for row in logs] != expected_logs:
            return None, "ordered log suffix has a gap"
        data_rows = [row for row in logs if row["entry_kind"] == "DATA"]
        expected_data = list(range(cut["cut_data_sequence"] + 1, target_data + 1))
        if [row["data_sequence"] for row in data_rows] != expected_data:
            return None, "DATA suffix has a gap"
        for log in data_rows:
            record = await conn.fetchrow(
                """SELECT r.*,o.payload,o.payload_digest AS object_digest,
                          o.payload_size AS object_size,o.schema_id AS object_schema,
                          o.published_at,
                          oi.payload_digest AS identity_digest,
                          oi.payload_size AS identity_size,
                          oi.schema_id AS identity_schema
                   FROM records r JOIN objects o USING(object_key)
                   JOIN object_identities oi USING(object_key)
                   WHERE r.channel=$1 AND r.partition_id=$2 AND r.sequence=$3""",
                edge["channel"],
                edge["partition_id"],
                log["data_sequence"],
            )
            if record is None or record["record_id"] != log["record_id"]:
                return None, f"record {log['data_sequence']} is unavailable"
            payload = bytes(record["payload"])
            if (
                record["published_at"] is None
                or (
                    hashlib.sha256(payload).hexdigest(),
                    len(payload),
                    record["schema_id"],
                )
                != (
                    record["payload_digest"],
                    record["payload_size"],
                    record["object_schema"],
                )
                or (
                    record["object_digest"],
                    record["object_size"],
                    record["object_schema"],
                    record["identity_digest"],
                    record["identity_size"],
                    record["identity_schema"],
                )
                != (
                    record["payload_digest"],
                    record["payload_size"],
                    record["schema_id"],
                    record["payload_digest"],
                    record["payload_size"],
                    record["schema_id"],
                )
            ):
                return (
                    None,
                    f"record {log['data_sequence']} payload identity is corrupt",
                )
        if not edge["replayable"]:
            return (
                None,
                "edge is declared non-replayable; retained exact bytes verified",
            )
        identity = {
            "checkpoint_id": checkpoint_id,
            "edge_id": edge["edge_id"],
            "channel": edge["channel"],
            "partition": edge["partition_id"],
            "cut_log_sequence": cut["barrier_log_sequence"],
            "cut_data_sequence": cut["cut_data_sequence"],
            "target_log_sequence": target_log,
            "target_data_sequence": target_data,
            "cut_digest": cut["cut_digest"],
            "retention_ref_id": cut["retention_ref_id"],
        }
        identity["availability_digest"] = digest(identity)
        return identity, None

    @staticmethod
    def _plan(row: asyncpg.Record) -> RegionRecoveryPlan:
        return RegionRecoveryPlan(
            plan_id=row["plan_id"],
            request_id=row["request_id"],
            revision_id=row["revision_id"],
            checkpoint_id=row["checkpoint_id"],
            failed_node=row["failed_node"],
            graph_manifest_digest=row["graph_manifest_digest"],
            completion_digest=row["completion_digest"],
            fence_authority_ordinal=row["fence_authority_ordinal"],
            region=tuple(decoded(row["region_json"])),
            boundaries=tuple(
                ReplayBoundary(
                    edge_id=item["edge_id"],
                    channel=item["channel"],
                    partition=item["partition"],
                    cut_log_sequence=item["cut_log_sequence"],
                    cut_data_sequence=item["cut_data_sequence"],
                    target_log_sequence=item["target_log_sequence"],
                    target_data_sequence=item["target_data_sequence"],
                    availability_digest=item["availability_digest"],
                )
                for item in decoded(row["boundaries_json"])
            ),
            source_rewinds=tuple(decoded(row["source_rewinds_json"])),
            reasons=dict(decoded(row["reasons_json"])),
            plan_digest=row["plan_digest"],
        )
