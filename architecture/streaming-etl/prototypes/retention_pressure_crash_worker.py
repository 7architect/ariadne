from __future__ import annotations

import argparse
import os
from pathlib import Path

from retention_pressure_v2 import RetentionPressureStore


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    parser.add_argument("operation", choices=("append", "blocked", "compact"))
    parser.add_argument("point")
    args = parser.parse_args()
    store = RetentionPressureStore(args.database, max_records=4, max_bytes=40)

    def fault(point: str) -> None:
        if point == args.point:
            os._exit(86)

    if args.operation == "append":
        store.append("attempt-3", "record-3", "object-3", b"3xxxxxxxxx", "lineage-3", fault=fault)
    elif args.operation == "blocked":
        store.append("attempt-5", "record-5", "object-5", b"5xxxxxxxxx", "lineage-5", fault=fault)
    else:
        store.compact(2, fault=fault)
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
