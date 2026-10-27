"""Tiny persistent supervisor for deliberate ETL child-process loss.

The container and its resource accounting stay alive for the whole campaign.
Only the workload child is killed at declared fault boundaries, then restarted
against the same PostgreSQL authority and durable state.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def main() -> int:
    state = Path(os.environ.get("SOAK_STATE_DIR", "/state"))
    state.mkdir(parents=True, exist_ok=True)
    pid_path = state / "service-child.pid"
    stopping = False
    child: subprocess.Popen | None = None

    def stop(signum, _frame) -> None:
        nonlocal stopping
        stopping = True
        if child is not None and child.poll() is None:
            child.send_signal(signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        child = subprocess.Popen([sys.executable, "/app/ci_soak/service.py"])
        temporary = pid_path.with_suffix(".tmp")
        temporary.write_text(str(child.pid) + "\n")
        temporary.replace(pid_path)
        returncode = child.wait()
        pid_path.unlink(missing_ok=True)
        if stopping:
            return (
                0 if returncode in {0, -signal.SIGTERM, -signal.SIGINT} else returncode
            )
        # Bounded backoff prevents a corrupt durable state from producing an
        # unbounded fork loop. The reviewer rejects unexpected process boots.
        time.sleep(1.0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
