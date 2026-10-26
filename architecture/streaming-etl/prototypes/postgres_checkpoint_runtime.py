"""Runtime adapter for CHECKPOINT_MANAGED_SHARED state.

Partition-local state continues to use ``PostgresGeneratorRuntime``.  This
adapter is deliberately separate: it snapshots shared/multi-input state only
against a coordinator-authored barrier vector and restores only a frozen
recovery plan.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from postgres_barrier_coordinator import (
    PostgresBarrierCoordinator,
    RecoveryPlan,
    canonical,
    decoded,
    digest,
)
from postgres_stream_v2 import PostgresProtocolError, content_object_key


@dataclass(frozen=True)
class SharedStateContract:
    participant_id: str
    state_schema: str
    state_version: int
    code_digest: str


@dataclass(frozen=True)
class RestoredSharedState:
    checkpoint_id: str
    participant_id: str
    state: Any
    target_data_sequences: dict[str, int]
    recovery_id: str


class PostgresCheckpointRuntime:
    """Platform-only checkpoint path; client tasks do not call a barrier API."""

    def __init__(
        self,
        coordinator: PostgresBarrierCoordinator,
        *,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        contract: SharedStateContract,
    ):
        if contract.state_version <= 0:
            raise ValueError("shared state version must be positive")
        self.coordinator = coordinator
        self.revision_id = revision_id
        self.graph_version = graph_version
        self.graph_digest = graph_digest
        self.owner_id = owner_id
        self.owner_epoch = owner_epoch
        self.contract = contract

    def _common(self) -> tuple[str, str, str, str, int]:
        return (
            self.revision_id, self.graph_version, self.graph_digest,
            self.owner_id, self.owner_epoch,
        )

    async def snapshot_aligned_checkpoint(
        self,
        checkpoint_id: str,
        state: Any,
        *,
        mailbox_highwater: int,
        fault=None,
    ) -> str:
        participant = next(
            (
                item for item in self.coordinator_revision_participants
                if item["participant_id"] == self.contract.participant_id
            ),
            None,
        )
        if participant is None or (
            participant["state_schema"], participant["state_version"],
            participant["code_digest"],
        ) != (
            self.contract.state_schema, self.contract.state_version,
            self.contract.code_digest,
        ):
            raise PostgresProtocolError("shared state contract is incompatible")
        try:
            frozen = json.loads(canonical(state))
        except (TypeError, ValueError) as exc:
            raise PostgresProtocolError("shared state is not canonical JSON") from exc
        return await self.coordinator.acknowledge_participant(
            *self._common(), checkpoint_id, self.contract.participant_id, frozen,
            mailbox_highwater=mailbox_highwater,
            state_version=self.contract.state_version,
            fault=fault,
        )

    @property
    def coordinator_revision_participants(self) -> list[dict[str, Any]]:
        # The immutable manifest is injected by ``bind`` so task code cannot
        # substitute a participant or state contract at snapshot time.
        manifest = getattr(self, "_participant_manifest", None)
        if manifest is None:
            raise PostgresProtocolError("checkpoint runtime is not bound")
        return manifest

    async def bind(self) -> None:
        async with self.coordinator.pool.acquire() as conn:
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1",
                self.revision_id,
            )
        if revision is None or (
            revision["graph_version"], revision["graph_digest"]
        ) != (self.graph_version, self.graph_digest):
            raise PostgresProtocolError("checkpoint runtime graph is incompatible")
        manifest = revision["participant_manifest"]
        if isinstance(manifest, str):
            manifest = json.loads(manifest)
        participant = next(
            (
                item for item in manifest
                if item["participant_id"] == self.contract.participant_id
            ),
            None,
        )
        if participant is None or (
            participant["state_schema"], participant["state_version"],
            participant["code_digest"],
        ) != (
            self.contract.state_schema, self.contract.state_version,
            self.contract.code_digest,
        ):
            raise PostgresProtocolError("shared state contract is incompatible")
        object.__setattr__(self, "_participant_manifest", list(manifest))

    async def restore_frozen_plan(
        self, plan: RecoveryPlan,
    ) -> RestoredSharedState:
        if plan.revision_id != self.revision_id:
            raise PostgresProtocolError("recovery plan revision is incompatible")
        async with self.coordinator.pool.acquire() as conn:
            authority = await conn.fetchrow(
                """SELECT r.*,c.completion_digest,c.completion_authority_ordinal,
                          l.status AS checkpoint_status
                   FROM recovery_plans_v2 r
                   JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE r.recovery_id=$1""",
                plan.recovery_id,
            )
            if authority is None:
                raise PostgresProtocolError("recovery plan has no durable authority")
            targets = dict(decoded(authority["target_data_sequences"]))
            snapshots = dict(decoded(authority["participant_snapshots"]))
            plan_identity = {
                "recovery_id": authority["recovery_id"],
                "request_id": authority["request_id"],
                "revision_id": authority["revision_id"],
                "checkpoint_id": authority["checkpoint_id"],
                "completion_digest": authority["completion_digest"],
                "fence_authority_ordinal": authority["fence_authority_ordinal"],
                "target_data_sequences": targets,
                "participant_snapshots": snapshots,
            }
            if (
                authority["revision_id"] != self.revision_id
                or authority["checkpoint_status"] != "COMPLETED"
                or authority["completion_authority_ordinal"]
                    >= authority["fence_authority_ordinal"]
                or authority["plan_digest"] != digest(plan_identity)
                or (
                    plan.checkpoint_id, plan.revision_id,
                    plan.target_data_sequences, plan.participant_snapshots,
                    plan.plan_digest, plan.status,
                ) != (
                    authority["checkpoint_id"], authority["revision_id"],
                    targets, snapshots, authority["plan_digest"],
                    authority["status"],
                )
            ):
                raise PostgresProtocolError("recovery plan authority is incompatible")
            object_key = snapshots.get(self.contract.participant_id)
            if object_key is None:
                raise PostgresProtocolError("recovery plan lacks participant snapshot")
            row = await conn.fetchrow(
                """SELECT * FROM checkpoint_snapshot_objects
                   WHERE object_key=$1 AND published_at IS NOT NULL""",
                object_key,
            )
            acknowledgement = await conn.fetchrow(
                """SELECT state_schema,state_version,code_digest,snapshot_object_key
                   FROM participant_checkpoint_acks
                   WHERE checkpoint_id=$1 AND participant_id=$2""",
                plan.checkpoint_id, self.contract.participant_id,
            )
        if row is None or acknowledgement is None:
            raise PostgresProtocolError("recovery snapshot is unavailable")
        if (
            acknowledgement["state_schema"], acknowledgement["state_version"],
            acknowledgement["code_digest"],
            acknowledgement["snapshot_object_key"],
        ) != (
            self.contract.state_schema, self.contract.state_version,
            self.contract.code_digest, object_key,
        ):
            raise PostgresProtocolError("recovery state contract is incompatible")
        payload = bytes(row["payload"])
        if (
            hashlib.sha256(payload).hexdigest(),
            content_object_key(payload, row["schema_id"]),
            row["schema_id"],
        ) != (
            row["payload_digest"], object_key, self.contract.state_schema,
        ):
            raise PostgresProtocolError("recovery snapshot identity is corrupted")
        return RestoredSharedState(
            checkpoint_id=plan.checkpoint_id,
            participant_id=self.contract.participant_id,
            state=json.loads(payload),
            target_data_sequences=targets,
            recovery_id=plan.recovery_id,
        )
