"""Read-only logical row/JSON byte census for an isolated prototype schema.

This measures retained evidence, including tables omitted from hand-maintained
admission counters. It does not measure heap, indexes, WAL or vacuum progress.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass

import asyncpg


@dataclass(frozen=True)
class TableFootprint:
    table: str
    rows: int
    json_bytes: int


@dataclass(frozen=True)
class StorageFootprint:
    schema: str
    tables: tuple[TableFootprint, ...]

    @property
    def total_rows(self) -> int:
        return sum(table.rows for table in self.tables)

    @property
    def total_json_bytes(self) -> int:
        return sum(table.json_bytes for table in self.tables)


def _identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


async def evaluate(dsn: str, schema: str) -> StorageFootprint:
    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            tables = await connection.fetch(
                """SELECT c.relname FROM pg_class c JOIN pg_namespace n
                   ON n.oid=c.relnamespace
                   WHERE n.nspname=$1 AND c.relkind IN('r','p')
                   ORDER BY c.relname""",
                schema,
            )
            if not tables:
                raise ValueError("schema has no ordinary tables")
            result = []
            for table in tables:
                name = table["relname"]
                row = await connection.fetchrow(
                    "SELECT count(*) AS rows,"
                    "COALESCE(sum(octet_length(row_to_json(t)::text)),0) AS bytes "
                    f"FROM {_identifier(schema)}.{_identifier(name)} t"
                )
                result.append(TableFootprint(name, row["rows"], int(row["bytes"])))
            return StorageFootprint(schema, tuple(result))
    finally:
        await connection.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    report = await evaluate(args.dsn, args.schema)
    print(
        json.dumps(
            {
                **asdict(report),
                "total_rows": report.total_rows,
                "total_json_bytes": report.total_json_bytes,
                "scope": "logical rows/JSON bytes; excludes physical storage",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
