# ADR-0091: Idempotency alone provides multi-sink atomicity

- Status: `rejected`
- Date: 2026-10-19
- Superseded by: [ADR-0004](ADR-0004-multi-sink-commit.md)

## Problem

Requiring every task and sink write to be idempotent seems to promise safe retry
after a crash between multiple external writes.

## Theory

A stable idempotency key alone makes writes to independent sinks exactly-once and
atomically visible.

## Proposed agreement

Require stable idempotency keys for every task and sink, then treat retries as an
atomic multi-sink commit protocol.

## Agreement

Rejected. Idempotency prevents a repeated logical write from creating an
additional effect in one sink. It cannot undo a completed write, make another
sink available, establish one global commit decision, or prevent readers from
observing sink 1 before sink 2.

The multi-sink prototype demonstrates this visibility window: after an injected
crash, sink 1 contains the value while sink 2 is empty. Durable decision recovery
eventually converges both sinks, but simultaneous visibility requires cooperative
version staging and a shared publish manifest honored by readers.

## Stories

- As a developer, I want idempotency to have a precise scope and stable key.
- As a user, I want eventual convergence distinguished from atomic visibility.
- As a system, I want to reject an atomicity request based only on idempotency.

## Acceptance criteria

- As a user, I can observe a partial result between sink commits.
- As a system, I cannot abort the already visible idempotent write.
- As a developer, I need a separate commit protocol for a stronger guarantee.

