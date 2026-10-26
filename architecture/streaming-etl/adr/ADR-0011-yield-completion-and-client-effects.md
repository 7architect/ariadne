# ADR-0011: Yield completion and client-owned effects

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, SDK maintainers
- Depends on: [ADR-0010](ADR-0010-platform-durable-yield-channel.md)
- Supersedes: [ADR-0004](ADR-0004-multi-sink-commit.md), [ADR-0005](ADR-0005-operator-contract.md)
- Related rejected boundary: [ADR-0091](ADR-0091-idempotency-alone-is-atomic.md)

## Problem

The platform can observe inputs and Python `yield`/`return` events, but it cannot
atomically include an arbitrary PostgreSQL write, HTTP request, file mutation,
or other client effect in its own progress transaction. A crash after an effect
and before an observed output makes the external outcome unknowable.

## Theory

The platform can give a precise at-least-once invocation and durable-progress
contract without pretending to own arbitrary effects. Redelivery is safe only
to the degree that client code is deterministic and its external effects are
idempotent.

## Agreement

The user-facing completion boundary is a runtime-observed `yield` or `return`.
There is no client `commit` call. “Observed” means the platform durably recorded
the completion and its platform-managed outputs; merely reaching a Python yield
instruction is insufficient.

For input sequence 200:

```text
delivered, no durable yield/return  → redeliver 200
durable yield/return                → deliver 201 next
```

The platform guarantees:

- durable input envelopes are not silently lost;
- an incomplete delivery is redelivered;
- platform-managed outputs and consumer progress advance together;
- a stale worker cannot advance progress after ownership transfer;
- the same immutable input envelope is supplied on redelivery.

The client owns:

- deterministic output when output equivalence is required;
- idempotency of PostgreSQL, HTTP, filesystem, and other external effects;
- correct use of stable business or platform-provided effect keys;
- compatibility of code and schemas across replay.

An arbitrary external effect may happen more than once. The platform does not
claim exactly-once processing, multi-sink atomicity, rollback, or automatic
effect deduplication. A connector may offer stronger behavior, but that is a
connector-specific client contract and not the core flow guarantee.

Local Python variables are not recovery state. Stateful operators that must
resume at 201 store their required state through a managed state API included in
sequence 200's durable completion. Otherwise they restore an older graph
checkpoint and replay its tail, with the same client-owned idempotency rule.

## Client shape

```python
@stream_task
def sign_documents(ctx, documents):
    for document_ref in documents:
        signed_ref = sign(document_ref)  # client owns repeat safety
        yield signed_ref                 # platform progress boundary
```

Protocol epoch 2 rejects consuming another input before the current one reaches
one terminal `yield` or `return`. Multi-output processing is deferred to an
explicit `emit(...); complete()` protocol.

## Stories

- As a developer, I want `yield` and `return` to define platform progress.
- As a user, I want the guarantee to state clearly that my external effects can
  repeat after a crash.
- As a system, I want to redeliver the current input when no completion is
  durable.

## Acceptance criteria

- As a developer, I can see `completed_through`, `in_flight`, attempt, owner, and
  replay reason for each consumer partition.
- As a user, I got the same input sequence after a crash before durable output.
- As a system, I cannot advance a consumer from delivery alone.
- As a system, I can reject a second input request while the current input is
  incomplete in protocol epoch 2.
- As a user, I got no platform claim that an arbitrary client effect happened
  exactly once.

## Prototype evidence

[test_durable_stream_v2.py](../prototypes/test_durable_stream_v2.py) verifies
redelivery before observed output, advance after observed output, stale-owner
rejection, and atomic platform-managed output/state/progress. It intentionally
does not model or certify arbitrary external effects.

## Evidence required for `accepted`

- Sync and async SDK integration around real Python `yield` and `return`.
- Worker `os._exit` tests before and after runtime observation and durable
  completion.
- Documentation and telemetry that never label client effects exactly-once.

