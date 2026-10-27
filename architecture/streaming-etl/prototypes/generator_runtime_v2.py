"""DS-006 sync/async generator adapter over the durable-stream v2 contract."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Callable, Generator

from durable_stream_v2 import (
    CompletionKind,
    DataEnvelope,
    DurableStream,
    ObjectRef,
    PendingOutput,
)


class GeneratorProtocolError(RuntimeError):
    pass


@dataclass(frozen=True)
class OutputSpec:
    payload: bytes
    schema_id: str = "bytes/v1"
    managed_state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StreamContext:
    consumer_id: str
    input_sequence: int
    attempt: int


SyncTask = Callable[
    [StreamContext, DataEnvelope], Generator[OutputSpec, None, dict[str, Any] | None]
]
AsyncTask = Callable[[StreamContext, DataEnvelope], AsyncGenerator[OutputSpec, None]]


class GeneratorRuntime:
    def __init__(
        self,
        store: DurableStream,
        *,
        output_channel: str,
        output_partition: str = "p0",
        invocation_ttl: int = 10,
    ):
        self.store = store
        self.output_channel = output_channel
        self.output_partition = output_partition
        self.invocation_ttl = invocation_ttl
        self._active: set[str] = set()

    def _enter(self, consumer_id: str) -> None:
        if consumer_id in self._active:
            raise GeneratorProtocolError("consumer invocation already active")
        self._active.add(consumer_id)

    def _leave(self, consumer_id: str) -> None:
        self._active.discard(consumer_id)

    def _pending_output(
        self, consumer_id: str, envelope: DataEnvelope, spec: OutputSpec
    ) -> PendingOutput:
        identity = f"{consumer_id}:{envelope.record_id}"
        object_key = f"stream-output:{identity}"
        ref = ObjectRef.for_payload(object_key, spec.payload, spec.schema_id)
        self.store.register_object(ref)
        lineage = hashlib.sha256(
            f"{consumer_id}|{envelope.lineage_digest}|{envelope.sequence}".encode()
        ).hexdigest()
        return PendingOutput(
            self.output_channel,
            self.output_partition,
            f"stream-output:{identity}",
            ref,
            lineage,
        )

    @staticmethod
    def _freeze_spec(spec: OutputSpec) -> OutputSpec:
        if not isinstance(spec.managed_state, dict):
            raise GeneratorProtocolError("YIELD state must be a dictionary")
        try:
            state = json.loads(
                json.dumps(spec.managed_state, sort_keys=True, separators=(",", ":"))
            )
        except (TypeError, ValueError) as exc:
            raise GeneratorProtocolError(
                "YIELD state must be JSON serializable"
            ) from exc
        return OutputSpec(bytes(spec.payload), str(spec.schema_id), state)

    @staticmethod
    def _hit(fault: Callable[[str], None] | None, point: str) -> None:
        if fault is not None:
            fault(point)

    def run_sync(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        task: SyncTask,
        *,
        completion_fault: Callable[[str], None] | None = None,
        adapter_fault: Callable[[str], None] | None = None,
    ) -> str | None:
        self._enter(consumer_id)
        holder_id: str | None = None
        try:
            delivery = self.store.deliver_next(consumer_id, owner_id, owner_epoch)
            if delivery is None:
                return None
            token, envelope = delivery
            holder_id = uuid.uuid4().hex
            try:
                self.store.acquire_invocation(
                    token, holder_id, invocation_ttl=self.invocation_ttl
                )
            except RuntimeError as exc:
                raise GeneratorProtocolError(
                    "consumer invocation already active"
                ) from exc
            self._hit(adapter_fault, "adapter:after_delivery_before_client")
            ctx = StreamContext(consumer_id, token.sequence, token.attempt)
            generator = task(ctx, envelope)
            if not inspect.isgenerator(generator):
                raise GeneratorProtocolError("stream task must return a sync generator")
            try:
                spec = next(generator)
            except StopIteration as stopped:
                state = {} if stopped.value is None else stopped.value
                if not isinstance(state, dict):
                    raise GeneratorProtocolError("RETURN state must be a dictionary")
                state = json.loads(
                    json.dumps(state, sort_keys=True, separators=(",", ":"))
                )
                self._hit(adapter_fault, "adapter:after_terminal_observed_before_close")
                self.store.complete(
                    token,
                    CompletionKind.RETURN,
                    managed_state=state,
                    fault=completion_fault,
                    invocation_holder_id=holder_id,
                )
                return "RETURN"
            if not isinstance(spec, OutputSpec):
                generator.close()
                raise GeneratorProtocolError("generator must yield OutputSpec")
            spec = self._freeze_spec(spec)
            self._hit(adapter_fault, "adapter:after_terminal_observed_before_close")
            try:
                generator.close()
            except RuntimeError as exc:
                raise GeneratorProtocolError(
                    "generator emitted after terminal yield"
                ) from exc
            self._hit(adapter_fault, "adapter:after_close_before_object")
            output = self._pending_output(consumer_id, envelope, spec)
            self._hit(adapter_fault, "adapter:after_object_before_completion")
            self.store.complete(
                token,
                CompletionKind.YIELD,
                output=output,
                managed_state=spec.managed_state,
                fault=completion_fault,
                invocation_holder_id=holder_id,
            )
            return "YIELD"
        finally:
            if holder_id is not None:
                self.store.release_invocation(consumer_id, holder_id)
            self._leave(consumer_id)

    async def run_async(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        task: AsyncTask,
        *,
        completion_fault: Callable[[str], None] | None = None,
        adapter_fault: Callable[[str], None] | None = None,
        timeout_seconds: float | None = None,
    ) -> str | None:
        self._enter(consumer_id)
        holder_id: str | None = None
        try:
            delivery = self.store.deliver_next(consumer_id, owner_id, owner_epoch)
            if delivery is None:
                return None
            token, envelope = delivery
            holder_id = uuid.uuid4().hex
            try:
                self.store.acquire_invocation(
                    token, holder_id, invocation_ttl=self.invocation_ttl
                )
            except RuntimeError as exc:
                raise GeneratorProtocolError(
                    "consumer invocation already active"
                ) from exc
            self._hit(adapter_fault, "adapter:after_delivery_before_client")
            ctx = StreamContext(consumer_id, token.sequence, token.attempt)
            generator = task(ctx, envelope)
            if not inspect.isasyncgen(generator):
                raise GeneratorProtocolError(
                    "stream task must return an async generator"
                )
            try:
                operation = generator.__anext__()
                spec = (
                    await operation
                    if timeout_seconds is None
                    else await asyncio.wait_for(operation, timeout_seconds)
                )
            except StopAsyncIteration:
                self._hit(adapter_fault, "adapter:after_terminal_observed_before_close")
                self.store.complete(
                    token,
                    CompletionKind.RETURN,
                    managed_state={},
                    fault=completion_fault,
                    invocation_holder_id=holder_id,
                )
                return "RETURN"
            if not isinstance(spec, OutputSpec):
                await generator.aclose()
                raise GeneratorProtocolError("async generator must yield OutputSpec")
            spec = self._freeze_spec(spec)
            self._hit(adapter_fault, "adapter:after_terminal_observed_before_close")
            try:
                await generator.aclose()
            except RuntimeError as exc:
                raise GeneratorProtocolError(
                    "async generator emitted after terminal yield"
                ) from exc
            self._hit(adapter_fault, "adapter:after_close_before_object")
            output = self._pending_output(consumer_id, envelope, spec)
            self._hit(adapter_fault, "adapter:after_object_before_completion")
            self.store.complete(
                token,
                CompletionKind.YIELD,
                output=output,
                managed_state=spec.managed_state,
                fault=completion_fault,
                invocation_holder_id=holder_id,
            )
            return "YIELD"
        except asyncio.CancelledError:
            raise
        finally:
            if holder_id is not None:
                self.store.release_invocation(consumer_id, holder_id)
            self._leave(consumer_id)
