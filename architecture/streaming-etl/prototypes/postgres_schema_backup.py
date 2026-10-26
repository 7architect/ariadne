"""Real exported-snapshot schema dump and isolated PostgreSQL restore helpers."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import uuid
from pathlib import Path

import asyncpg
from postgres_restore_contracts import RestoreContractError, canonical, digest
from postgres_restore_integrity import assert_retained_payload_integrity
from postgres_restore_registry import cuts_document, database_scope, decoded
from postgres_schema_identity import schema_identity

DOCKER = (
    "env",
    "-u",
    "DOCKER_HOST",
    "-u",
    "DOCKER_CONTEXT",
    "-u",
    "DOCKER_TLS",
    "-u",
    "DOCKER_TLS_VERIFY",
    "-u",
    "DOCKER_CERT_PATH",
    "docker",
    "--host=unix:///var/run/docker.sock",
)


class IsolatedRestorePostgres:
    """Disposable target owned by one test; never stops the shared source pair."""

    def __init__(self, schema: str, *, image: str = "postgres:17-alpine"):
        self.name = "ariadne-restore-" + uuid.uuid4().hex[:12]
        self.schema, self.image = schema, image
        self.pool = None
        self.dsn = None

    async def __aenter__(self):
        try:
            await docker_command(
                "run",
                "-d",
                "--name",
                self.name,
                "--memory=512m",
                "--cpus=1",
                "-e",
                "POSTGRES_USER=ariadne",
                "-e",
                "POSTGRES_PASSWORD=ariadne",
                "-e",
                "POSTGRES_DB=streaming",
                "-p",
                "127.0.0.1::5432",
                self.image,
            )
            address = await docker_command("port", self.name, "5432/tcp")
            port = int(address.rsplit(":", 1)[1])
            self.dsn = f"postgresql://ariadne:ariadne@127.0.0.1:{port}/streaming"
            last_error = None
            for _ in range(300):
                try:
                    self.pool = await asyncpg.create_pool(
                        self.dsn,
                        min_size=1,
                        max_size=8,
                        timeout=1,
                        server_settings={"search_path": f'"{self.schema}",public'},
                    )
                    return self
                except (OSError, asyncpg.PostgresError) as exc:
                    last_error = type(exc).__name__
                    await asyncio.sleep(0.05)
            raise RestoreContractError(
                "isolated PostgreSQL did not become ready: " + str(last_error)
            )
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *_args):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
        await docker_command("rm", "-f", self.name)


def file_digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def container_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,120}", value):
        raise ValueError("unsafe Docker container name")
    return value


async def docker_command(*args, input_file=None, output_file=None, timeout=120):
    process = await asyncio.create_subprocess_exec(
        *DOCKER,
        *args,
        stdin=input_file,
        stdout=output_file or asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _stderr = await asyncio.wait_for(process.communicate(), timeout)
        if process.returncode:
            # Caller receives only the command category, not SQL/data/credentials.
            raise RestoreContractError("Docker PostgreSQL command failed: " + args[0])
        return (stdout or b"").decode().strip()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def assert_container_system(container, scope):
    actual = await docker_command(
        "exec",
        container_name(container),
        "psql",
        "-At",
        "-U",
        "ariadne",
        "-d",
        scope["database"],
        "-c",
        "SELECT system_identifier FROM pg_control_system()",
    )
    if actual != scope["system_id"]:
        raise RestoreContractError(
            "backup container is not the selected PostgreSQL system"
        )


def _partial_path(artifact: Path, backup_id: str) -> Path:
    if not re.fullmatch(r"schema-backup:[0-9a-f]{64}", backup_id):
        raise RestoreContractError("invalid backup artifact identity")
    return artifact.with_name(artifact.name + ".partial." + backup_id.split(":", 1)[1])


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_receipt(path: Path, envelope: dict) -> None:
    temporary = path.with_name(path.name + ".writing." + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        stream.write(canonical(envelope) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    _sync_directory(path.parent)


def _publish_artifact(partial: Path, artifact: Path) -> None:
    # Atomic no-overwrite publication; the durable receipt is written first.
    os.link(partial, artifact)
    _sync_directory(artifact.parent)
    partial.unlink()
    _sync_directory(artifact.parent)


async def _reconcile_schema_backup_locked(
    registry, source_authority, quiesced, artifact: Path, *, max_artifact_bytes: int
):
    artifact = Path(artifact)
    request_id = digest([str(artifact.resolve()), quiesced.manifest_digest])
    # The per-artifact lock excludes a live writer of these unpublished files.
    # Their authoritative outcome remains in the bounded external registry.
    for stale in artifact.parent.glob(artifact.name + ".json.writing.*"):
        stale.unlink()
    rows = await registry.backup_records_for_request(request_id)
    receipt = artifact.with_suffix(artifact.suffix + ".json")
    envelope = None
    if receipt.exists():
        try:
            envelope = json.loads(await asyncio.to_thread(receipt.read_text))
        except (ValueError, OSError) as exc:
            raise RestoreContractError("backup receipt is corrupt") from exc
    elif artifact.exists():
        completed = [row for row in rows if row["status"] == "COMPLETED"]
        if len(completed) != 1:
            raise RestoreContractError("backup artifact has no committed receipt")
        manifest = decoded(completed[0]["manifest_json"])
        envelope = {
            "manifest": manifest,
            "manifest_sha256": completed[0]["manifest_digest"],
        }
    if envelope is not None:
        if not isinstance(envelope, dict) or set(envelope) != {
            "manifest",
            "manifest_sha256",
        }:
            raise RestoreContractError("backup receipt envelope drift")
        manifest = envelope["manifest"]
        if digest(manifest) != envelope["manifest_sha256"]:
            raise RestoreContractError("backup receipt digest mismatch")
        record = await registry.backup_record(manifest["backup_id"])
        pending = decoded(record["pending_json"])
        if (
            record["status"] == "REJECTED"
            or any(manifest.get(key) != value for key, value in pending.items())
            or manifest["artifact_request_id"] != request_id
            or manifest["quiesce_manifest_digest"] != quiesced.manifest_digest
            or manifest["max_artifact_bytes"] != max_artifact_bytes
        ):
            raise RestoreContractError("backup receipt differs from immutable request")
        if manifest["source_scope"] != await database_scope(
            source_authority.pool, source_authority.schema
        ):
            raise RestoreContractError("backup recovery source scope drift")
        partial = _partial_path(artifact, manifest["backup_id"])
        durable = artifact if artifact.exists() else partial
        if (
            not durable.exists()
            or durable.stat().st_size != manifest["artifact_bytes"]
            or durable.stat().st_size > max_artifact_bytes
            or await asyncio.to_thread(file_digest, durable)
            != manifest["artifact_sha256"]
        ):
            if record["status"] == "PENDING":
                await registry.reject_backup(
                    manifest["backup_id"], "BackupArtifactCorruptOrMissing"
                )
            raise RestoreContractError("backup artifact is missing or corrupt")
        if not artifact.exists():
            await asyncio.to_thread(_publish_artifact, partial, artifact)
        elif partial.exists():
            if (
                await asyncio.to_thread(file_digest, partial)
                != manifest["artifact_sha256"]
            ):
                raise RestoreContractError("backup duplicate partial digest mismatch")
            partial.unlink()
        await registry.finish_backup(manifest["backup_id"], manifest)
        if not receipt.exists():
            await asyncio.to_thread(_write_receipt, receipt, envelope)
        return envelope
    for row in rows:
        pending = decoded(row["pending_json"])
        if pending["max_artifact_bytes"] != max_artifact_bytes:
            raise RestoreContractError("backup capacity request drift")
        if row["status"] == "COMPLETED":
            raise RestoreContractError("completed backup artifact is missing")
        if row["status"] == "PENDING":
            # Its exported snapshot belonged to a dead transaction. A new dump
            # may use a new snapshot only over the same quiesced authority.
            await registry.reject_backup(row["backup_id"], "InterruptedSnapshotExport")
        if row["status"] in {"PENDING", "REJECTED"}:
            partial = _partial_path(artifact, row["backup_id"])
            partial.unlink(missing_ok=True)
            partial.with_name(partial.name + ".unverified").unlink(missing_ok=True)
    return None


async def create_schema_backup(
    registry,
    source_authority,
    quiesced,
    artifact: Path,
    *,
    source_container: str,
    max_artifact_bytes: int = 64 * 1024 * 1024,
    fault=None,
):
    artifact = Path(artifact)
    request_id = digest([str(artifact.resolve()), quiesced.manifest_digest])
    async with registry.backup_request_lock(
        digest(["artifact-path", str(artifact.resolve())])
    ):
        prior = await _reconcile_schema_backup_locked(
            registry,
            source_authority,
            quiesced,
            artifact,
            max_artifact_bytes=max_artifact_bytes,
        )
        if prior is not None:
            return prior
        return await _export_schema_backup(
            registry,
            source_authority,
            quiesced,
            artifact,
            source_container=source_container,
            max_artifact_bytes=max_artifact_bytes,
            fault=fault,
            request_id=request_id,
        )


async def reconcile_schema_backup(
    registry,
    source_authority,
    quiesced,
    artifact: Path,
    *,
    max_artifact_bytes: int = 64 * 1024 * 1024,
):
    artifact = Path(artifact)
    async with registry.backup_request_lock(
        digest(["artifact-path", str(artifact.resolve())])
    ):
        return await _reconcile_schema_backup_locked(
            registry,
            source_authority,
            quiesced,
            artifact,
            max_artifact_bytes=max_artifact_bytes,
        )


async def _export_schema_backup(
    registry,
    source_authority,
    quiesced,
    artifact: Path,
    *,
    source_container: str,
    max_artifact_bytes: int = 64 * 1024 * 1024,
    fault=None,
    request_id: str,
):
    """Keep RR snapshot exported until pg_dump ends, then finalize its artifact.

    The registry is outside the dumped schema. A pending/failed artifact cannot
    issue a restore grant. This profile keeps the same schema name on restore.
    """
    artifact = Path(artifact)
    if artifact.exists():
        raise FileExistsError(artifact)
    if max_artifact_bytes <= 0:
        raise ValueError("positive backup artifact limit required")
    head = await source_authority.project_head()
    if (
        head["state"] != "QUIESCED"
        or head["fence_epoch"] != quiesced.fence_epoch
        or head["release_generation"] != quiesced.release_generation
        or head["quiesce_manifest_digest"] != quiesced.manifest_digest
    ):
        raise RestoreContractError("export requires the exact quiesced release")
    scope = await database_scope(source_authority.pool, source_authority.schema)
    if registry.schema == source_authority.schema:
        raise RestoreContractError(
            "restore issuer registry must be outside backup schema"
        )
    if await registry.verification_key() != quiesced.restore_public_key_b64:
        raise RestoreContractError("backup issuer differs from the pinned release key")
    await assert_container_system(source_container, scope)
    artifact.parent.mkdir(parents=True, exist_ok=True)
    backup_id = None
    receipt_durable = False
    try:
        async with (
            source_authority.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            snapshot_id = await conn.fetchval("SELECT pg_export_snapshot()")
            observed_lsn = str(await conn.fetchval("SELECT pg_current_wal_lsn()"))
            cuts = cuts_document(
                await source_authority.snapshot_cuts(quiesced.run_id, conn=conn)
            )
            if digest(cuts) != quiesced.cuts.digest:
                raise RestoreContractError("export cuts differ from quiesced manifest")
            await assert_retained_payload_integrity(conn)
            identity = await schema_identity(conn, source_authority.schema)
            run = cuts["run"]
            pending = {
                "source_scope": scope,
                "snapshot_id": snapshot_id,
                "observed_wal_lsn": observed_lsn,
                "run_id": quiesced.run_id,
                "deployment_id": run["deployment_id"],
                "revision_id": run["revision_id"],
                "release_generation": quiesced.release_generation,
                "release_fence": quiesced.fence_epoch,
                "quiesce_manifest_digest": quiesced.manifest_digest,
                "issuer_key_id": quiesced.restore_key_id,
                "cuts": cuts,
                "cuts_digest": digest(cuts),
                "schema_digest": identity["sha256"],
                "profile": "quiesced-schema-backup/v1",
                "artifact_request_id": request_id,
                "max_artifact_bytes": max_artifact_bytes,
            }
            backup_id = await registry.begin_backup(pending)
            partial = _partial_path(artifact, backup_id)
            if fault is not None:
                fault("after_pending_manifest")
            output_file = await asyncio.to_thread(partial.open, "xb")
            with output_file as output:
                await docker_command(
                    "exec",
                    container_name(source_container),
                    "pg_dump",
                    "-U",
                    "ariadne",
                    "-d",
                    scope["database"],
                    "--format=custom",
                    "--no-owner",
                    "--no-privileges",
                    "--schema=" + scope["schema"],
                    "--snapshot=" + snapshot_id,
                    output_file=output,
                )
                await asyncio.to_thread(output.flush)
                await asyncio.to_thread(os.fsync, output.fileno())
            if partial.stat().st_size > max_artifact_bytes:
                raise RestoreContractError("declared backup artifact limit exceeded")
            manifest = {
                **pending,
                "backup_id": backup_id,
                "artifact_sha256": await asyncio.to_thread(file_digest, partial),
                "artifact_bytes": partial.stat().st_size,
            }
        envelope = {"manifest": manifest, "manifest_sha256": digest(manifest)}
        if fault is not None:
            fault("after_dump_before_receipt")
        await asyncio.to_thread(
            _write_receipt, artifact.with_suffix(artifact.suffix + ".json"), envelope
        )
        receipt_durable = True
        if fault is not None:
            fault("after_receipt_before_publish")
        await asyncio.to_thread(_publish_artifact, partial, artifact)
        if fault is not None:
            fault("after_artifact_before_registry")
        await registry.finish_backup(backup_id, manifest)
        if fault is not None:
            fault("after_registry_before_ack")
        return envelope
    except BaseException as exc:
        if backup_id is not None and not receipt_durable:
            try:
                await registry.reject_backup(backup_id, type(exc).__name__)
            except RestoreContractError:
                # A completed outcome may have lost its acknowledgement. Its
                # immutable row remains authoritative; never rewrite it failed.
                pass
        raise


async def restore_schema_backup(
    registry,
    backup_id: str,
    artifact: Path,
    target_pool,
    *,
    target_container: str,
):
    manifest, manifest_sha = await registry.completed_backup(backup_id)
    artifact = Path(artifact)
    if await asyncio.to_thread(file_digest, artifact) != manifest["artifact_sha256"]:
        raise RestoreContractError("backup artifact hash mismatch")
    schema = manifest["source_scope"]["schema"]
    scope = await database_scope(target_pool, schema)
    if scope["system_id"] == manifest["source_scope"]["system_id"]:
        raise RestoreContractError("restore requires a separate PostgreSQL system")
    await assert_container_system(target_container, scope)
    async with target_pool.acquire() as conn:
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", schema
        ):
            raise RestoreContractError("restore target schema must be absent")
    opened = await asyncio.to_thread(artifact.open, "rb")
    with opened as input_file:
        await docker_command(
            "exec",
            "-i",
            container_name(target_container),
            "pg_restore",
            "-U",
            "ariadne",
            "-d",
            scope["database"],
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "--single-transaction",
            input_file=input_file,
        )
    async with (
        target_pool.acquire() as conn,
        conn.transaction(isolation="repeatable_read", readonly=True),
    ):
        await conn.execute(
            "SELECT set_config('search_path',$1,true)", '"' + schema + '",public'
        )
        identity = await schema_identity(conn, schema)
        await assert_retained_payload_integrity(conn)
    if identity["sha256"] != manifest["schema_digest"]:
        raise RestoreContractError("restored schema definition hash mismatch")
    return {
        "scope": scope,
        "backup_id": backup_id,
        "manifest_sha256": manifest_sha,
        "schema_digest": identity["sha256"],
    }
