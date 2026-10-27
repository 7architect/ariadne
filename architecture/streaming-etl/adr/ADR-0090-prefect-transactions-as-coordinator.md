# ADR-0090: Use Prefect transactions as the streaming coordinator

- Status: `rejected`
- Date: 2026-10-19
- Superseded by: [ADR-0001](ADR-0001-runtime-boundary.md), [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md)

## Problem

Reusing existing transactions appears to reduce implementation work for streaming
checkpoint and multi-sink coordination.

## Theory

Prefect task transactions might already contain the durable prepare, decision,
participant recovery, and fencing semantics required by a streaming runtime.

## Proposed agreement

Use Prefect task transactions as the sole durable authority for streaming
checkpoint and multi-sink coordination.

## Agreement

Rejected. Generator tasks emit values before terminal `handle_success`. On retry,
the generator is recreated and values are emitted again. Existing transaction
commit executes child commits and hooks sequentially and persists a final result;
it does not durably record an immutable participant set, prepare tokens, a global
commit decision, recovery acknowledgements, or fencing epochs.

## Stories

- As a developer, I want to reuse an existing primitive only when its failure
  semantics match streaming checkpoints.
- As a user, I want stream recovery independent of terminal task result commit.
- As a system, I want an explicit coordinator instead of implicit hook ordering.

## Acceptance criteria

- As a developer, I cannot recover an immutable participant set from the existing
  transaction.
- As a system, I cannot fence stale streaming producers with the existing
  transaction contract.
- As a user, I did not get a multi-sink atomic outcome from sequential hooks.

## Consequence

Prefect transactions may remain an implementation detail for isolated final
results, but they are not the streaming checkpoint coordinator.

