"""Real-process crash worker for release migration boundaries."""

from __future__ import annotations

import argparse
import asyncio
import os

import asyncpg
from postgres_control_plane import PostgresControlPlane
from postgres_prefect_attempts import PostgresPrefectAttempts
from postgres_release_authority import PostgresReleaseAuthority, RuntimeContract
from postgres_stream_v2 import PostgresStream


def _crash(_stage: str) -> None:
    os._exit(86)


async def _run(args: argparse.Namespace) -> None:
    pool = await asyncpg.create_pool(
        args.dsn,
        min_size=1,
        max_size=2,
        server_settings={"search_path": f'"{args.schema}",public'},
    )
    try:
        release = PostgresReleaseAuthority.connect_existing(pool, args.schema)
        if args.operation == "expand":
            stream = PostgresStream(pool, args.schema)
            attempts = PostgresPrefectAttempts.connect_existing(
                PostgresControlPlane.connect_existing(stream)
            )
            await release.expand_prefect_attempts(
                args.session_id,
                attempts,
                migration_id=args.migration_id,
                from_version="r4a",
                to_version="r4b",
                artifact_digest=args.artifact_digest,
                fault=_crash,
            )
        else:
            await release.cutover(
                args.session_id,
                migration_id=args.migration_id,
                target_contracts=(RuntimeContract("prefect-r4b", "2" * 64, "b" * 64),),
                fault=_crash if args.operation == "cutover-before-commit" else None,
            )
            os._exit(86)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--migration-id", required=True)
    parser.add_argument("--artifact-digest", required=True)
    parser.add_argument(
        "--operation",
        required=True,
        choices=("expand", "cutover-before-commit", "cutover-after-commit"),
    )
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
