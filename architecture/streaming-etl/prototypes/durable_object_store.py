"""Filesystem-backed ObjectRef upload model for process-crash evidence."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from durable_stream_v2 import (
    DurableStream,
    FaultHook,
    ObjectRef,
    StreamProtocolError,
    _hit,
)


class DurableObjectStore:
    """Publishes bytes before registering their durable ObjectRef witness."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, ref: ObjectRef) -> Path:
        if not ref.key or Path(ref.key).name != ref.key:
            raise StreamProtocolError("object key must be one safe path segment")
        return self.root / ref.key

    def upload(
        self,
        stream: DurableStream,
        ref: ObjectRef,
        payload: bytes,
        *,
        fault: FaultHook | None = None,
    ) -> None:
        digest = hashlib.sha256(payload).hexdigest()
        if ref.digest != digest or ref.size != len(payload):
            raise StreamProtocolError("payload bytes do not match ObjectRef")

        destination = self.path_for(ref)
        staging = self.root / f".{ref.key}.uploading"
        if destination.exists():
            if destination.read_bytes() != payload:
                raise StreamProtocolError("published object bytes drift")
        else:
            with staging.open("wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            _hit(fault, "upload:before_publish")
            os.replace(staging, destination)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

        _hit(fault, "upload:after_blob_fsync_before_witness")

        def witness_fault(point: str) -> None:
            mapped = {
                "object_witness:after_insert_before_commit": (
                    "upload:after_witness_insert_before_commit"
                ),
                "object_witness:after_commit_before_ack": (
                    "upload:after_commit_before_ack"
                ),
            }.get(point)
            if mapped is not None:
                _hit(fault, mapped)

        stream.register_object(ref, fault=witness_fault)
