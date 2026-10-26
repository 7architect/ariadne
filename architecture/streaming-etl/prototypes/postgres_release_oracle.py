"""Independent read-only oracle for the release/session authority prototype."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Any

import asyncpg

OWN_TABLES = {
    "release_heads_v1",
    "release_runtime_compatibility_v1",
    "release_sessions_v1",
    "release_session_archive_v1",
    "release_migrations_v1",
    "release_grant_witnesses_v1",
    "release_grant_archive_v1",
    "release_source_finalizations_v1",
}
AUTHORITY_TRIGGERS = {
    "release_heads_v1": "release_head_guard",
    "release_session_archive_v1": "release_archive_guard",
    "release_runtime_compatibility_v1": "release_compatibility_guard",
    "release_sessions_v1": "release_session_guard",
    "release_migrations_v1": "release_migration_guard",
    "release_grant_witnesses_v1": "release_grant_guard",
    "release_grant_archive_v1": "release_grant_archive_guard",
    "release_source_finalizations_v1": "release_finalization_guard",
}


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class ReleaseOracleReport:
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, Any]:
        return {"passed": self.passed, "violations": list(self.violations)}


async def _current_cuts(conn: asyncpg.Connection) -> dict[str, Any] | None:
    runs = await conn.fetch(
        "SELECT run_id,deployment_id,revision_id FROM control_runs_v1 ORDER BY run_id"
    )
    if len(runs) != 1:
        return None
    run = dict(runs[0])
    revision_id = run["revision_id"]
    queries: dict[str, tuple[str, tuple[Any, ...]]] = {
        "partitions": (
            (
                "SELECT channel,partition_id,next_sequence,used_records,used_bytes "
                "FROM partitions ORDER BY channel,partition_id"
            ),
            (),
        ),
        "producers": (
            (
                "SELECT channel,partition_id,producer_id,accepted_through,retired_through "
                "FROM producer_progress ORDER BY channel,partition_id,producer_id"
            ),
            (),
        ),
        "consumers": (
            (
                "SELECT consumer_id,channel,partition_id,completed_through,"
                "in_flight_sequence FROM consumers ORDER BY consumer_id"
            ),
            (),
        ),
        "workers": (
            (
                "SELECT participant_id,state,next_arrival,next_apply,owner_id,"
                "owner_epoch,phase FROM join_workers WHERE revision_id=$1 "
                "ORDER BY participant_id"
            ),
            (revision_id,),
        ),
        "inputs": (
            (
                "SELECT participant_id,channel,partition_id,scanned,completed,"
                "blocked_checkpoint FROM join_inputs_v3 WHERE revision_id=$1 "
                "ORDER BY participant_id,channel,partition_id"
            ),
            (revision_id,),
        ),
        "checkpoints": (
            (
                "SELECT i.checkpoint_id,i.checkpoint_ordinal,l.status "
                "FROM checkpoint_intents_v2 i JOIN checkpoint_lifecycle_v2 l "
                "USING(checkpoint_id) WHERE i.revision_id=$1 "
                "ORDER BY i.checkpoint_ordinal"
            ),
            (revision_id,),
        ),
    }
    material: dict[str, Any] = {"run": run}
    for name, (query, args) in queries.items():
        material[name] = [dict(row) for row in await conn.fetch(query, *args)]
    attempt = await conn.fetchrow(
        "SELECT h.current_attempt_id,h.current_ordinal,h.current_epoch,"
        "h.retired_through,a.prefect_flow_run_id "
        "FROM prefect_attempt_heads_v1 h JOIN prefect_execution_attempts_v1 a "
        "ON a.attempt_id=h.current_attempt_id WHERE h.run_id=$1",
        run["run_id"],
    )
    material["attempt"] = dict(attempt) if attempt is not None else None
    return material


async def evaluate(dsn: str, schema: str) -> ReleaseOracleReport:
    violations: set[str] = set()
    conn: asyncpg.Connection | None = None
    try:
        conn = await asyncpg.connect(
            dsn, server_settings={"search_path": f'"{schema}",public'}
        )
        await conn.execute("SET default_transaction_read_only=on")
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            heads = await conn.fetch("SELECT * FROM release_heads_v1")
            if len(heads) != 1:
                return ReleaseOracleReport(("release_head_cardinality",))
            head = heads[0]
            try:
                raw_key = base64.b64decode(
                    head["restore_public_key_b64"], validate=True
                )
                if hashlib.sha256(raw_key).hexdigest() != head["restore_key_id"]:
                    violations.add("restore_key_identity")
            except (TypeError, ValueError):
                violations.add("restore_key_shape")

            tables = {
                row["table_name"]
                for row in await conn.fetch(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema=$1 AND table_type='BASE TABLE'",
                    schema,
                )
            }
            try:
                declared_guarded = set(json.loads(head["guarded_tables"]))
            except (TypeError, ValueError):
                declared_guarded = set()
                violations.add("guarded_table_manifest_shape")
            if declared_guarded != tables - OWN_TABLES:
                violations.add("guarded_table_manifest_exact")
            triggers = {
                (row["table_name"], row["trigger_name"])
                for row in await conn.fetch(
                    "SELECT event_object_table AS table_name,trigger_name "
                    "FROM information_schema.triggers WHERE trigger_schema=$1",
                    schema,
                )
            }
            for table in declared_guarded:
                if (table, "release_write_guard") not in triggers:
                    violations.add("protected_table_missing_release_guard")
            for table, trigger in AUTHORITY_TRIGGERS.items():
                if (table, trigger) not in triggers:
                    violations.add("authority_table_missing_guard")

            compatibility = await conn.fetch(
                "SELECT * FROM release_runtime_compatibility_v1 WHERE enabled"
            )
            if len(compatibility) > head["max_compatibility_rows"]:
                violations.add("runtime_compatibility_capacity")
            current = [
                row
                for row in compatibility
                if row["schema_version"] == head["schema_version"]
            ]
            if not current:
                violations.add("current_runtime_manifest_empty")
            if any(
                row["writer_semantics_digest"] != head["writer_semantics_digest"]
                or row["feature_digest"] != head["feature_digest"]
                for row in current
            ):
                violations.add("current_runtime_manifest_drift")

            migrations = await conn.fetch("SELECT * FROM release_migrations_v1")
            if len(migrations) > head["max_migrations"]:
                violations.add("migration_history_capacity")
            for migration in migrations:
                if migration["identity_digest"] != _digest(
                    [
                        migration["migration_id"],
                        migration["from_version"],
                        migration["to_version"],
                        migration["artifact_digest"],
                    ]
                ):
                    violations.add("migration_identity_digest")
                if migration["to_version"] in {
                    head["schema_version"],
                    head["pending_schema_version"],
                } and migration["guarded_tables_digest"] != _digest(
                    sorted(declared_guarded)
                ):
                    violations.add("migration_guarded_tables_digest")
            expanded = [row for row in migrations if row["status"] == "EXPANDED"]
            if head["pending_schema_version"] is None:
                if expanded:
                    violations.add("expanded_migration_without_pending_release")
            elif (
                len(expanded) != 1
                or expanded[0]["from_version"] != head["schema_version"]
                or expanded[0]["to_version"] != head["pending_schema_version"]
            ):
                violations.add("pending_migration_authority")

            sessions = await conn.fetch("SELECT * FROM release_sessions_v1")
            active = [row for row in sessions if row["status"] == "ACTIVE"]
            if len(active) > head["max_hot_sessions"]:
                violations.add("release_session_capacity")
            if head["state"] != "ACTIVE" and active:
                violations.add("terminal_release_has_active_session")
            current_runtime_ids = {row["runtime_version"] for row in current}
            if any(
                row["fence_epoch"] != head["fence_epoch"]
                or row["runtime_version"] not in current_runtime_ids
                for row in active
            ):
                violations.add("active_release_session_authority")
            if len(
                {
                    (
                        row["fence_epoch"],
                        row["runtime_version"],
                        row["actor_id"],
                        row["role"],
                    )
                    for row in active
                }
            ) != len(active):
                violations.add("duplicate_active_release_session")
            archive = await conn.fetchrow(
                "SELECT * FROM release_session_archive_v1 WHERE release_id=$1",
                head["release_id"],
            )
            if (
                archive is None
                or archive["retired_count"] < 0
                or archive["retired_through"] < 0
                or len(archive["rolling_digest"]) != 64
                or any(row["status"] != "ACTIVE" for row in sessions)
            ):
                violations.add("release_session_archive")
            if archive is not None and head["next_session_ordinal"] <= max(
                [archive["retired_through"], *(row["ordinal"] for row in sessions)]
            ):
                violations.add("release_session_ordinal_highwater")

            cuts: dict[str, Any] | None = None
            if (
                head["state"] in ("QUIESCED", "CUTOVER_RETIRED")
                or head["quiesced_cuts"] is not None
            ):
                try:
                    cuts = json.loads(head["quiesced_cuts"])
                except (TypeError, ValueError):
                    violations.add("quiesced_cuts_shape")
                if not isinstance(cuts, dict):
                    violations.add("quiesced_cuts_shape")
                elif _digest(cuts) != head["quiesced_cuts_digest"]:
                    violations.add("quiesced_cuts_digest")
                else:
                    if head["state"] != "ACTIVE" and await _current_cuts(conn) != cuts:
                        violations.add("quiesced_cuts_not_current_authority")
                    manifest = {
                        "release_id": head["release_id"],
                        "run_id": head["quiesced_run_id"],
                        "release_generation": head["release_generation"],
                        "fence_epoch": head["fence_epoch"],
                        "schema_version": head["schema_version"],
                        "restore_public_key_b64": head["restore_public_key_b64"],
                        "restore_key_id": head["restore_key_id"],
                        "cuts": cuts,
                        "cuts_digest": head["quiesced_cuts_digest"],
                    }
                    # Restored targets advance generation/fence after the source
                    # manifest was created; their signed grant preserves the source
                    # identity, so only a still-quiesced/source-retired head can be
                    # recomputed directly.
                    if (
                        head["state"] != "ACTIVE"
                        and _digest(manifest) != head["quiesce_manifest_digest"]
                    ):
                        violations.add("quiesce_manifest_digest")

            grant_archive = await conn.fetchrow(
                "SELECT * FROM release_grant_archive_v1 WHERE release_id=$1",
                head["release_id"],
            )
            finalizations = await conn.fetch(
                "SELECT * FROM release_source_finalizations_v1"
            )
            completed = [row for row in finalizations if row["status"] == "COMPLETED"]
            if (
                head["source_finalize_digest"] is not None
                and not any(
                    row["outcome_digest"] == head["source_finalize_digest"]
                    and row["quiesce_manifest_digest"]
                    == head["quiesce_manifest_digest"]
                    and row["cuts_digest"] == head["quiesced_cuts_digest"]
                    for row in completed
                )
                and not await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM release_grant_witnesses_v1)"
                )
                and (
                    grant_archive is None
                    or grant_archive["latest_source_finalize_digest"]
                    != head["source_finalize_digest"]
                )
            ):
                # A restored copy is taken before source finalization; its signed
                # local grant is the durable target witness instead.
                violations.add("source_finalization_projection")
            if (
                head["state"] == "CUTOVER_RETIRED"
                and head["source_finalize_digest"] is None
            ):
                violations.add("retired_source_without_finalization")

            grants = await conn.fetch("SELECT * FROM release_grant_witnesses_v1")
            if (
                grant_archive is None
                or grant_archive["retired_through_generation"] < 0
                or grant_archive["retired_count"] < 0
                or (
                    grant_archive["retired_count"] == 0
                    and grant_archive["latest_source_finalize_digest"] is not None
                )
                or (
                    grant_archive["retired_count"] > 0
                    and grant_archive["latest_source_finalize_digest"] is None
                )
                or len(grant_archive["rolling_digest"]) != 64
            ):
                violations.add("restore_grant_archive")
            if len(grants) > 1:
                violations.add("restore_grant_cardinality")
            if grants:
                grant = grants[0]
                session = next(
                    (
                        row
                        for row in sessions
                        if row["session_id"] == grant["session_id"]
                    ),
                    None,
                )
                system_id = str(
                    await conn.fetchval(
                        "SELECT system_identifier FROM pg_control_system()"
                    )
                )
                database = await conn.fetchval("SELECT current_database()")
                if (
                    head["state"] != "ACTIVE"
                    or head["source_system_id"] != system_id
                    or head["source_database"] != database
                    or head["source_schema"] != schema
                    or grant["target_system_id"] != system_id
                    or grant["target_database"] != database
                    or grant["target_schema"] != schema
                    or grant["cuts_digest"] != head["quiesced_cuts_digest"]
                    or grant["target_release_generation"] != head["release_generation"]
                    or grant["source_release_generation"]
                    != grant["target_release_generation"] - 1
                    or grant["source_finalize_digest"] != head["source_finalize_digest"]
                    or grant["activated_fence"] != head["fence_epoch"]
                    or (
                        grant_archive is not None
                        and grant["target_release_generation"]
                        <= grant_archive["retired_through_generation"]
                    )
                    or session is None
                    or session["status"] != "ACTIVE"
                ):
                    violations.add("restore_grant_projection")
    except (
        asyncpg.PostgresError,
        AttributeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:  # oracle remains total on malformed disposable state
        violations.add(f"oracle_read_error:{type(exc).__name__}")
    finally:
        if conn is not None:
            await conn.close()
    return ReleaseOracleReport(tuple(sorted(violations)))


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dsn")
    parser.add_argument("schema")
    args = parser.parse_args()
    report = await evaluate(args.dsn, args.schema)
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
