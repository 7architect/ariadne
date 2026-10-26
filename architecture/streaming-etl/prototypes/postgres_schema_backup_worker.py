"""Fresh-process driver for named schema-backup crash boundaries."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

import asyncpg
from postgres_release_authority import (
    CanonicalReleaseCuts,
    PostgresReleaseAuthority,
    QuiescedManifest,
)
from postgres_restore_registry import PostgresRestoreRegistry
from postgres_schema_backup import create_schema_backup


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="ARIADNE_STREAM_DSN")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--registry-schema", required=True)
    parser.add_argument("--quiesce-manifest", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--source-container", default="ariadne-stream-postgres")
    parser.add_argument(
        "--fault-stage",
        choices=(
            "after_pending_manifest",
            "after_dump_before_receipt",
            "after_receipt_before_publish",
            "after_artifact_before_registry",
            "after_registry_before_ack",
        ),
    )
    args = parser.parse_args()
    material = json.loads(args.quiesce_manifest.read_text())
    quiesced = QuiescedManifest(
        material["release_id"],
        material["run_id"],
        material["release_generation"],
        material["fence_epoch"],
        material["schema_version"],
        material["restore_public_key_b64"],
        material["restore_key_id"],
        CanonicalReleaseCuts(material["cuts"], material["cuts_digest"]),
    )
    pool = await asyncpg.create_pool(
        os.environ[args.dsn_env],
        min_size=1,
        max_size=8,
        server_settings={"search_path": f'"{args.schema}",public'},
    )
    try:
        source = PostgresReleaseAuthority.connect_existing(pool, args.schema)
        registry = PostgresRestoreRegistry(pool, args.registry_schema)

        def fault(stage):
            if stage == args.fault_stage:
                os._exit(86)

        envelope = await create_schema_backup(
            registry,
            source,
            quiesced,
            args.artifact,
            source_container=args.source_container,
            fault=fault,
        )
        print(
            json.dumps(
                {
                    "backup_id": envelope["manifest"]["backup_id"],
                    "manifest_sha256": envelope["manifest_sha256"],
                    "artifact_sha256": envelope["manifest"]["artifact_sha256"],
                }
            ),
            flush=True,
        )
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
