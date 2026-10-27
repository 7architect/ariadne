"""Sync/async generator adapter on the PostgreSQL authoritative data plane."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Callable, Generator

from postgres_stream_v2 import (
    Envelope,
    Output,
    PostgresStream,
    content_object_key,
)


class RuntimeProtocolError(RuntimeError):
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
    managed_state: dict[str, Any]


SyncTask = Callable[
    [StreamContext, Envelope], Generator[OutputSpec, None, dict[str, Any] | None]
]
AsyncTask = Callable[[StreamContext, Envelope], AsyncGenerator[OutputSpec, None]]
Migration = Callable[[dict[str, Any]], dict[str, Any]]


def stream_task(function):
    """Declare a protocol-epoch-2 sync or async generator task.

    The callable stays a normal Python generator function; durable execution
    still goes through `PostgresGeneratorRuntime`.
    """
    if inspect.isgeneratorfunction(function):
        mode = "sync"
    elif inspect.isasyncgenfunction(function):
        mode = "async"
    else:
        raise RuntimeProtocolError("@stream_task requires a generator function")
    setattr(function, "__ariadne_stream_protocol__", "yield-v2")
    setattr(function, "__ariadne_stream_mode__", mode)
    return function


class PostgresGeneratorRuntime:
    def __init__(
        self,
        store: PostgresStream,
        *,
        output_channel: str,
        output_partition: str = "p0",
        state_schema: str = "task-state/v1",
        state_version: int = 1,
        migrations: dict[int, Migration] | None = None,
        invocation_lease_seconds: float = 30,
        heartbeat_interval_seconds: float | None = None,
    ):
        self.store = store
        self.output_channel = output_channel
        self.output_partition = output_partition
        self.state_schema = state_schema
        self.state_version = state_version
        self.migrations = migrations or {}
        self.invocation_lease_seconds = invocation_lease_seconds
        self.heartbeat_interval_seconds = (
            invocation_lease_seconds / 3
            if heartbeat_interval_seconds is None
            else heartbeat_interval_seconds
        )
        if not 0 < self.heartbeat_interval_seconds < invocation_lease_seconds:
            raise ValueError("heartbeat interval must be inside the lease duration")

    @staticmethod
    def _canonical_copy(value: dict[str, Any], label: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise RuntimeProtocolError(f"{label} state must be a dictionary")
        try:
            return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":")))
        except (TypeError, ValueError) as exc:
            raise RuntimeProtocolError(
                f"{label} state must be JSON serializable"
            ) from exc

    def _restore_state(self, position: dict[str, Any]) -> dict[str, Any]:
        raw = position["managed_state"]
        state = json.loads(raw) if isinstance(raw, str) else raw
        if (
            position["state_schema"] == "none/v1"
            and position["completed_through"] < position["start_sequence"]
        ):
            return {}
        if position["state_schema"] != self.state_schema:
            raise RuntimeProtocolError("managed state schema is incompatible")
        version = position["state_version"]
        if version > self.state_version:
            raise RuntimeProtocolError("managed state version is newer than runtime")
        state = self._canonical_copy(state, "managed")
        while version < self.state_version:
            migration = self.migrations.get(version)
            if migration is None:
                raise RuntimeProtocolError(
                    f"missing state migration {version}->{version + 1}"
                )
            first = self._canonical_copy(
                migration(self._canonical_copy(state, "migration")), "migration"
            )
            second = self._canonical_copy(
                migration(self._canonical_copy(state, "migration")), "migration"
            )
            if first != second:
                raise RuntimeProtocolError("state migration is nondeterministic")
            state = first
            version += 1
        return state

    def _output(self, consumer_id: str, envelope: Envelope, spec: OutputSpec) -> Output:
        identity = f"{consumer_id}:{envelope.record_id}"
        lineage = hashlib.sha256(
            f"{consumer_id}|{envelope.lineage_digest}|{envelope.sequence}".encode()
        ).hexdigest()
        return Output(
            self.output_channel,
            self.output_partition,
            f"stream-output:{identity}",
            content_object_key(spec.payload, spec.schema_id),
            spec.payload,
            spec.schema_id,
            lineage,
        )

    @staticmethod
    def _hit(fault: Callable[[str], None] | None, point: str) -> None:
        if fault is not None:
            fault(point)

    @staticmethod
    def _validate_task(task: Callable[..., Any], expected_mode: str) -> None:
        if getattr(task, "__ariadne_stream_protocol__", None) != "yield-v2":
            raise RuntimeProtocolError("stream task is not declared with @stream_task")
        actual_mode = getattr(task, "__ariadne_stream_mode__", None)
        if actual_mode != expected_mode:
            raise RuntimeProtocolError(
                f"stream task mode {actual_mode!r} cannot run as {expected_mode}"
            )

    async def _heartbeat(self, token, holder: str, stop: asyncio.Event) -> None:
        await self.store.renew_runtime_leases(
            token, holder, lease_seconds=self.invocation_lease_seconds
        )
        while True:
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.heartbeat_interval_seconds
                )
            except asyncio.TimeoutError:
                await self.store.renew_runtime_leases(
                    token, holder, lease_seconds=self.invocation_lease_seconds
                )
            else:
                return

    @staticmethod
    def _sync_next(generator):
        try:
            return False, next(generator)
        except StopIteration as stopped:
            return True, stopped.value

    @staticmethod
    def _raise_heartbeat_failure(heartbeat: asyncio.Task[None]) -> None:
        if heartbeat.done():
            heartbeat.result()

    @staticmethod
    async def _cancel_async_operation(
        operation: asyncio.Task[OutputSpec], generator: AsyncGenerator
    ) -> None:
        operation.cancel()
        try:
            await operation
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        finally:
            await generator.aclose()

    async def run_sync(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        task: SyncTask,
        *,
        timeout_seconds: float | None = None,
        fault: Callable[[str], None] | None = None,
    ) -> str | None:
        self._validate_task(task, "sync")
        if timeout_seconds is not None:
            raise RuntimeProtocolError(
                "sync timeout requires worker-process supervision"
            )
        delivery = await self.store.deliver_next(consumer_id, owner_id, owner_epoch)
        if delivery is None:
            return None
        token, envelope = delivery
        holder = uuid.uuid4().hex
        await self.store.acquire_invocation(
            token, holder, lease_seconds=self.invocation_lease_seconds
        )
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(token, holder, heartbeat_stop))
        try:
            self._hit(fault, "runtime:after_delivery")
            state = self._restore_state(await self.store.verified_position(consumer_id))
            generator = task(
                StreamContext(consumer_id, token.sequence, token.attempt, state),
                envelope,
            )
            if not inspect.isgenerator(generator):
                raise RuntimeProtocolError("stream task must return a sync generator")
            operation = asyncio.create_task(
                asyncio.to_thread(self._sync_next, generator)
            )
            try:
                returned, observed = await asyncio.shield(operation)
            except asyncio.CancelledError:
                # Python cannot stop the underlying thread.  Keep the exact
                # invocation and its heartbeat until client code really exits;
                # discard the observed terminal result and then propagate the
                # authoritative cancellation.  A separate drain task owns the
                # wait so repeated Task.cancel() calls cannot release the
                # holder while the thread is still inside generator code.
                async def drain_cancelled_sync() -> None:
                    try:
                        await operation
                    except BaseException:
                        pass
                    finally:
                        try:
                            await asyncio.to_thread(generator.close)
                        except (RuntimeError, ValueError):
                            pass

                drain = asyncio.create_task(drain_cancelled_sync())
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        current = asyncio.current_task()
                        if current is not None:
                            current.uncancel()
                await drain
                raise asyncio.CancelledError
            self._raise_heartbeat_failure(heartbeat)
            if returned:
                result_state = {} if observed is None else observed
                result_state = self._canonical_copy(result_state, "RETURN")
                self._hit(fault, "runtime:after_terminal")
                heartbeat_stop.set()
                await heartbeat
                await self.store.complete(
                    token,
                    "RETURN",
                    managed_state=result_state,
                    state_schema=self.state_schema,
                    state_version=self.state_version,
                    invocation_holder_id=holder,
                    fault=fault,
                )
                return "RETURN"
            spec = observed
            if not isinstance(spec, OutputSpec):
                await asyncio.to_thread(generator.close)
                raise RuntimeProtocolError("generator must yield OutputSpec")
            frozen = OutputSpec(
                bytes(spec.payload),
                str(spec.schema_id),
                self._canonical_copy(spec.managed_state, "YIELD"),
            )
            self._hit(fault, "runtime:after_terminal")
            try:
                await asyncio.to_thread(generator.close)
            except RuntimeError as exc:
                raise RuntimeProtocolError(
                    "generator emitted after terminal yield"
                ) from exc
            self._raise_heartbeat_failure(heartbeat)
            heartbeat_stop.set()
            await heartbeat
            await self.store.complete(
                token,
                "YIELD",
                output=self._output(consumer_id, envelope, frozen),
                managed_state=frozen.managed_state,
                state_schema=self.state_schema,
                state_version=self.state_version,
                invocation_holder_id=holder,
                fault=fault,
            )
            return "YIELD"
        finally:
            heartbeat_stop.set()
            try:
                await heartbeat
            finally:
                await self.store.release_invocation(consumer_id, holder)

    async def run_async(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        task: AsyncTask,
        *,
        timeout_seconds: float | None = None,
        fault: Callable[[str], None] | None = None,
    ) -> str | None:
        self._validate_task(task, "async")
        delivery = await self.store.deliver_next(consumer_id, owner_id, owner_epoch)
        if delivery is None:
            return None
        token, envelope = delivery
        holder = uuid.uuid4().hex
        await self.store.acquire_invocation(
            token, holder, lease_seconds=self.invocation_lease_seconds
        )
        heartbeat_stop = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(token, holder, heartbeat_stop))
        try:
            self._hit(fault, "runtime:after_delivery")
            state = self._restore_state(await self.store.verified_position(consumer_id))
            generator = task(
                StreamContext(consumer_id, token.sequence, token.attempt, state),
                envelope,
            )
            if not inspect.isasyncgen(generator):
                raise RuntimeProtocolError("stream task must return an async generator")
            operation = asyncio.create_task(generator.__anext__())
            try:
                if timeout_seconds is None:
                    spec = await asyncio.shield(operation)
                else:
                    done, _ = await asyncio.wait({operation}, timeout=timeout_seconds)
                    if not done:
                        await self._cancel_async_operation(operation, generator)
                        raise asyncio.TimeoutError
                    spec = operation.result()
            except asyncio.CancelledError:
                await self._cancel_async_operation(operation, generator)
                raise
            except StopAsyncIteration:
                self._hit(fault, "runtime:after_terminal")
                heartbeat_stop.set()
                await heartbeat
                await self.store.complete(
                    token,
                    "RETURN",
                    managed_state=state,
                    state_schema=self.state_schema,
                    state_version=self.state_version,
                    invocation_holder_id=holder,
                    fault=fault,
                )
                return "RETURN"
            self._raise_heartbeat_failure(heartbeat)
            if not isinstance(spec, OutputSpec):
                await generator.aclose()
                raise RuntimeProtocolError("async generator must yield OutputSpec")
            frozen = OutputSpec(
                bytes(spec.payload),
                str(spec.schema_id),
                self._canonical_copy(spec.managed_state, "YIELD"),
            )
            self._hit(fault, "runtime:after_terminal")
            try:
                await generator.aclose()
            except RuntimeError as exc:
                raise RuntimeProtocolError(
                    "async generator emitted after terminal yield"
                ) from exc
            heartbeat_stop.set()
            await heartbeat
            await self.store.complete(
                token,
                "YIELD",
                output=self._output(consumer_id, envelope, frozen),
                managed_state=frozen.managed_state,
                state_schema=self.state_schema,
                state_version=self.state_version,
                invocation_holder_id=holder,
                fault=fault,
            )
            return "YIELD"
        except asyncio.CancelledError:
            raise
        finally:
            heartbeat_stop.set()
            try:
                await heartbeat
            finally:
                await self.store.release_invocation(consumer_id, holder)
