# ADR-0001: Streaming runtime boundary

- Status: `progress`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0000](ADR-0000-decision-lifecycle.md)
- Leads to: [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md)

## Problem

Prefect generator flows and tasks can emit values through `yield`, but yielded
values are not durable stream records. On retry the generator is recreated and
values are emitted again. Existing Prefect transactions commit a final task
result; they do not provide durable stream channels, per-consumer progress,
periodic graph checkpoints, or worker fencing.

## Theory

A separate streaming runtime can preserve the Python `yield` programming model
while Prefect remains responsible for deployments, runs, workers, cancellation,
and observability.

## Proposed agreement

The flow run owns the lifetime of a streaming job. Streaming operators execute
inside operator runs but do not become `Completed` after each item. The streaming
runtime owns:

- partitioned channels and stable record identities;
- backpressure;
- managed operator state;
- `in_flight` and `completed_through` consumer progress;
- durable observation of terminal `yield` and `return` events;
- checkpoint coordination and restoration;
- worker fencing;

Prefect run state remains the control-plane truth. Streaming checkpoints are a
separate data-plane progress model and must not be represented as repeated
terminal Prefect states.

No external broker is assumed. The runtime may use pluggable durable object,
file, or database storage selected by [ADR-0006](ADR-0006-durable-channels-and-state.md).

## Target guarantee levels

The runtime will expose guarantees explicitly. These are target contracts, not
claims about the current Prefect runtime:

1. Platform progress: one active fenced owner and one monotonic
   `completed_through` cursor per consumer partition.
2. Delivery: at-least-once; an input without a durable downstream `yield` or
   `return` is redelivered.
3. Managed state: state required after sequence N is stored with completion of
   N, or an older checkpoint is restored and its tail replayed.
4. External effects: determinism and idempotency belong to client code as
   specified by [ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

## Stories

- As a developer, I want to write Python generator operators without managing
  worker failure or channel offsets manually.
- As a user, I want each flow run to expose current checkpoint, lag, replay count,
  `in_flight`, `completed_through`, lag, replay count, and checkpoint state.
- As a system, I want control-plane retries to delegate stream recovery to the
  checkpoint coordinator.

## Acceptance criteria

- As a developer, I can build a source, transform, branch, join, and sink using a
  documented `yield`-based API.
- As a user, I can see that input delivery and client invocation are
  at-least-once, while an arbitrary external effect may happen zero, one, or
  multiple times.
- As a system, I can reject a graph that requests unsupported ordering, state,
  or retention behavior.
- As a system, I got exactly one active recovery generation for an operator after
  a worker replacement.

## Rejected alternative

Reusing existing Prefect task transactions as the stream data plane is rejected.
They do not provide an append-only partition log, independent consumer cursors,
per-record redelivery, or fencing.

## Prototype evidence

[durable_stream_v2.py](../prototypes/durable_stream_v2.py) exercises the new
runtime boundary around a platform-owned log, ObjectRef envelopes, consumer
leases, redelivery, and durable `yield`/`return` progress. Historical source/sink
capability prototypes are retained but excluded from protocol epoch 2.

## Evidence required for `accepted`

- Production runtime API and compatibility tests for sync and async generators.
- End-to-end deployment tests showing checkpoint recovery under worker loss.
- User-facing guarantee and metrics documentation.

