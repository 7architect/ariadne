# ADR-0019: Production generator state and replacement

- Status: `progress`
- Date: 2026-10-24
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0013](ADR-0013-managed-state-recovery-classes.md), [ADR-0017](ADR-0017-sync-async-generator-adapter.md), [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-020a…d, DS-021a…d

## Problem

Passing the SQLite generator contract does not prove that real sync and async
tasks compose with PostgreSQL leases, process loss, managed-state upgrades, or
repeated Anna/Tanya replacement. A stale worker can still execute client code
or attempt completion after takeover. A new runtime version can also interpret
old state differently and corrupt every later turn.

## Theory

The PostgreSQL data plane can give each delivery one durable invocation holder
bound to `(consumer, input, attempt, owner, epoch)`. Completion can validate and
retire that holder in the same transaction as output, state, completion, and
cursor. A replacement can acquire an expired unfinished invocation only with a
strictly newer owner epoch and delivery attempt. State can cross versions only
through a declared migration that produces the same canonical result twice.

## Agreement

Only functions declared with `@stream_task` enter this runtime; the decorator
accepts sync or async generator functions and rejects an ordinary callable.
Deployment bootstrap uses `PostgresStream.open`; worker processes use
`connect_existing` and never acquire schema DDL locks while records are live.
Sync and async generators use the same terminal protocol on PostgreSQL. At the
first yield, payload, schema, and state are copied before generator cleanup.
`RETURN` has no output. Cancellation, timeout, client exception, or process loss
before commit leaves the current input incomplete. Loss after commit removes
the invocation holder and exposes the next input immediately.

Async timeout is enforced by cancelling the pending `__anext__`. A sync task
cannot be safely stopped inside its Python thread, so the in-process adapter
rejects a sync timeout request by name; the worker supervisor must enforce it by
terminating the task process. Process-loss recovery uses the same incomplete
input rule and is exercised by the crash matrix.

The in-process async timeout is a safety boundary, not a hard liveness boundary
against hostile code that catches cancellation forever. Such code retains its
invocation and cannot overlap a replacement until the worker supervisor kills
the process. The production supervisor deadline is therefore mandatory for
both sync code and cancellation-suppressing async code. Repeated cancellation
of a sync caller is drained by a separate task; heartbeat and holder remain
live until the underlying thread exits.

Consumer ownership uses `clock_timestamp()` and a monotonically increasing
epoch. Takeover of an in-flight input also increments its delivery attempt.
Every completion predicate includes owner, epoch, input sequence, attempt, and
invocation holder. Same-epoch expiry cannot authorize a second client body.
The heartbeat renews the exact consumer lease and invocation holder in one
transaction. Sync generator advancement runs outside the event-loop thread so
that heartbeat renewal continues while client code is executing.

Managed state carries a schema and integer version. A runtime rejects a foreign
schema, a future version, or a missing migration. Each migration step runs twice
from independent canonical copies; unequal results reject the turn without
moving progress. A successful terminal transaction stores the target schema,
version, and state beside the completion and consumer projection.

Client effects outside the data plane remain the client's idempotency
responsibility, as defined by [ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

## Stories

- As a developer, I want real sync and async generators to run on the selected
  production data plane.
- As a user, I want Tanya to receive Anna's first incomplete document and the
  matching managed state.
- As a system, I want stale Anna completions rejected after Tanya takes over.
- As a developer, I want state upgrades either deterministic or rejected before
  client processing changes durable progress.

## Acceptance criteria

- As a developer, I can run sync and async YIELD/RETURN turns on PostgreSQL.
- As a system, I got output, state, completion, cursor, and invocation retirement
  in one transaction.
- As a user, I got exact redelivery after exception, cancellation, timeout, and
  real process loss before commit; after lost acknowledgement I got N+1.
- As a system, I reject an expired invocation under the same owner epoch and
  every stale completion after takeover.
- As a developer, I can migrate compatible state deterministically and got a
  named rejection for incompatible, future, missing, or nondeterministic state.
- As a user, I got twenty ordered documents through repeated Anna/Tanya
  replacements with twenty completions, twenty outputs, final state 20, and no
  live invocation.
- As a developer, I can inspect an independent invariant report after every
  replacement and process-loss trace.

## Current evidence

Seven focused PostgreSQL suites pass. They cover enforced `@stream_task`
admission and cross-mode rejection before delivery; sync/async exception
behavior; cancellation, authoritative async timeout even when hostile client
code suppresses `CancelledError`, and named sync-timeout rejection; sync-thread
cancellation that retains its invocation holder until the client body exits;
atomic heartbeat and same-owner recovery; deterministic migration and rejection
of foreign schema, future version, missing migration, nondeterministic
migration, and corrupt state projection; and twenty-turn Anna/Tanya
replacement.

The subprocess matrix contains 22 `os._exit(86)` traces: sync and async tasks,
YIELD at six named boundaries, and RETURN at five named boundaries. The points
cover delivery, terminal observation, output, completion marker, progress,
commit, and acknowledgement. Fresh processes recover every trace and inspect
the durable database rather than reusing runtime memory. Every trace invokes
the independent read-only PostgreSQL oracle as a separate process. The repeated
replacement case ends with twenty input completions, twenty output records,
state `{count: 20}`, cursor 20, and no live invocation.

The current data-plane SHA-256 is
`02abed18e9a419d826b970468408a8ccb61c862abf2f89c46afb90eb4f06965e`;
the runtime SHA-256 is
`6b4af0b3c07a7667643f63135827e2697b049da3e084079ec2cd5e3473235198`;
the oracle SHA-256 is
`2c1db770e65277a90ced9c2f10af83ab16a8e11aae0da4fb346dd445bde5d2e2`.

Backend regression is 22/22 and generator-runtime regression is 7/7 on the
same data-plane revision. The sync cancellation test includes repeated caller
cancellation while the worker thread remains active; the drain task preserves
the holder and heartbeat until the client body exits. The ETL/HighLoad
Principal independently reran both suites and recorded R2-A and R2-B PASS with
no blockers on 2026-10-24, earning cumulative protocol readiness 55%. This ADR
remains `progress` because Prefect worker and orchestration-server integration
belongs to later ADR/gates.
