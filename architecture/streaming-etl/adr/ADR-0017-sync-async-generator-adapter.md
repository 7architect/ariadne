# ADR-0017: Sync and async generator adapter

- Status: `progress`
- Date: 2026-10-23
- Owners: ETL Architect / HighLoad Principal, Python runtime maintainers
- Depends on: [ADR-0011](ADR-0011-yield-completion-and-client-effects.md), [ADR-0016](ADR-0016-reference-safe-retention-and-bounded-pressure.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-006a, DS-006b, DS-006c, DS-007

## Problem

The storage protocol is not yet a usable Python execution model. A callback can
accidentally request input N+1 while N is unresolved, treat cancellation as
completion, or publish output without the matching state and cursor. Sync and
async generators must expose the same durable boundary.

## Theory

The platform can drive one generator invocation for one durable input delivery.
The first yielded output, or clean generator exhaustion, is terminal for that
invocation. The runtime closes the generator and atomically records output,
managed state, completion, and cursor through the data-plane API. An exception
or cancellation before that transaction leaves the input in flight for exact
redelivery.

## Agreement

The adapter accepts actual Python generators and async generators. A yielded
`OutputSpec` means terminal `YIELD`; exhaustion means terminal `RETURN`. The
runtime derives output record and ObjectRef identities from task and input
identity, publishes the object witness, and asks the durable channel to append
output plus completion/state/progress in one transaction.

One invocation holds one delivery. The adapter cannot request N+1 until the
terminal transaction for N commits. A durable per-consumer invocation lease,
in addition to a local re-entry guard, rejects concurrent sync or async
execution across adapter instances. A replacement may take that invocation
lease only after expiry under a newer consumer owner epoch. Closing after the
first yield makes later client generator code unreachable; a generator that
yields while handling close is rejected before completion.

At the first terminal yield, the adapter copies bytes, schema, and canonical
JSON state before calling generator close. Client cleanup may mutate its own
objects but cannot change the observed terminal value. Only `None` maps to an
empty RETURN state; every non-dictionary return value is rejected.

Cancellation, timeout, client exception, and process loss before the durable
transaction all leave N in flight. Loss of the acknowledgement after commit may
raise to the caller, but recovery observes N completed and delivers N+1. Client
external effects can repeat before completion and remain the client's
idempotency responsibility.

## Stories

- As a developer, I want the same task body shape for sync and async streaming.
- As a user, I want Anna's first completed output visible to downstream without
  waiting for the remaining input records.
- As a system, I want input N+1 unavailable until N has one durable terminal
  outcome.
- As a developer, I want cancellation and retry to have an explicit result.

## Acceptance criteria

- As a developer, I can run real sync and async generator functions over durable
  deliveries.
- As a system, I got output, managed state, completion, and cursor together or
  none.
- As a user, I got the same input redelivered after exception, cancellation, or
  timeout before completion.
- As a user, I got the next input after acknowledgement loss following a durable
  completion.
- As a system, I reject concurrent consumption, non-generator tasks, invalid
  yielded values, output identity drift, and a generator that emits while being
  closed.
- As a developer, I can reproduce sync and async process-loss boundaries in a
  fresh runtime and verify them with an independent oracle.
- As a system, I reject concurrent client execution from two adapter instances,
  and I got the state observed at terminal yield even when generator cleanup
  mutates the original object.
- As a user, I got the same input after an async timeout before terminal
  completion.

## Evidence plan

The focused evidence now has ten direct tests plus sixteen real-process crashes:
sync/async × delivery-before-client, terminal-observed, generator-closed,
ObjectRef-witness, output insert, completion marker, cursor update, and lost
acknowledgement boundaries. Fresh recovery expires the abandoned invocation
lease, takes a newer consumer epoch, and converges. Hostile controls cover two
adapter instances, state mutation during close, consumer-state projection
corruption, false non-dictionary returns, cancellation, and timeout. DS-007
closes R0 only after Principal review. Production storage integration remains
R1/R2 work, so this ADR stays `progress` after the R0 prototype gate.

The ETL/HighLoad Principal reran all eleven focused tests and independently
replayed same-epoch expiry, lost-ack N+1, and RETURN holder retirement on
2026-10-23. DS-006a/b/c and DS-007 passed; the R0-D gate earned 10%.
