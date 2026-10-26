"""Bounded, secret-filtered OpenTelemetry spans and stream lifecycle events."""

from __future__ import annotations

import copy
import json
import logging
import threading
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.trace import Status, StatusCode

SAFE_ATTRIBUTES = {
    "stream.run_id",
    "stream.revision_id",
    "stream.attempt_id",
    "stream.attempt_epoch",
    "stream.participant_id",
    "stream.owner_id",
    "stream.owner_epoch",
    "stream.input.channel",
    "stream.input.partition",
    "stream.input.sequence",
    "prefect.flow_run_id",
    "prefect.task_run_id",
    "event.type",
    "error.type",
}
LOGGER = logging.getLogger("ariadne.streaming")


def safe_attributes(values: dict[str, Any]) -> dict[str, Any]:
    return {
        key: str(value)[:512] if isinstance(value, (str, UUID)) else value
        for key, value in values.items()
        if key in SAFE_ATTRIBUTES and isinstance(value, (str, UUID, int, float, bool))
    }


class BoundedSpanExporter(SpanExporter):
    def __init__(self, capacity: int):
        self._items: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def export(self, spans) -> SpanExportResult:
        with self._lock:
            for span in spans:
                self._items.append(
                    {
                        "name": span.name,
                        "trace_id": f"{span.context.trace_id:032x}",
                        "span_id": f"{span.context.span_id:016x}",
                        "parent_span_id": None
                        if span.parent is None
                        else f"{span.parent.span_id:016x}",
                        "status": span.status.status_code.name,
                        "attributes": safe_attributes(dict(span.attributes or {})),
                    }
                )
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None

    def for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(item)
                for item in self._items
                if item["attributes"].get("stream.run_id") == run_id
            ]


class StreamTelemetry:
    def __init__(self, *, capacity: int = 128):
        if capacity <= 0:
            raise ValueError("positive telemetry capacity required")
        self.exporter = BoundedSpanExporter(capacity)
        self.provider = TracerProvider()
        self.provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        self.tracer = self.provider.get_tracer("ariadne.streaming", "protocol-2")
        self._events: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    @contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any],
        *,
        carrier: dict[str, str] | None = None,
    ):
        context = None if carrier is None else extract(carrier)
        with self.tracer.start_as_current_span(
            name,
            context=context,
            attributes=safe_attributes(attributes),
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            try:
                yield span
            except BaseException as exc:
                span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
                span.add_event("stream.error", {"error.type": type(exc).__name__})
                raise

    def event(self, kind: str, attributes: dict[str, Any]) -> dict[str, Any]:
        context = trace.get_current_span().get_span_context()
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": kind,
            "trace_id": f"{context.trace_id:032x}" if context.is_valid else None,
            "span_id": f"{context.span_id:016x}" if context.is_valid else None,
            **safe_attributes(attributes),
        }
        with self._lock:
            self._events.append(record)
        LOGGER.info(json.dumps(record, sort_keys=True))
        return record

    def events_for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(item)
                for item in self._events
                if item.get("stream.run_id") == run_id
            ]

    def close(self) -> None:
        self.provider.shutdown()


def trace_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    inject(headers)
    return headers
