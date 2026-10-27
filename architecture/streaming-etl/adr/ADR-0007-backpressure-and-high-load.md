# ADR-0007: Backpressure and high-load operation

- Status: `pending`
- Date: 2026-10-19
- Owners: HighLoad Principal, runtime maintainers
- Depends on: [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0010](ADR-0010-platform-durable-yield-channel.md), [ADR-0006](ADR-0006-durable-channels-and-state.md)

## Problem

A slow sink, stalled join input, or recovering branch can grow buffers without
bound. Checkpoint alignment can amplify this growth and turn a local slowdown
into job-wide failure.

## Theory

Byte- and record-bounded channels, credit-based flow control, per-partition lag
metrics, and explicit overload policies can keep memory and durable backlog
within declared limits.

## Candidate agreement

This ADR remains `pending` until capacity prototypes select exact algorithms.
The runtime should propagate credits from sinks to sources, bound both memory and
durable backlog, and expose whether it pauses, spills, rejects, or degrades a
source when capacity is exhausted.

Aligned checkpoint timeout and buffered bytes must be observable. Unaligned
checkpoints are a future option only after channel-state snapshot and recovery
are proven.

## Stories

- As a developer, I want operator code to await capacity without implementing
  custom queues.
- As a user, I want predictable resource limits and an explicit overload policy.
- As a system, I want a slow tail to throttle its upstream recovery domain.

## Acceptance criteria

- As a system, I can run at the declared sustained rate without unbounded memory
  or storage growth.
- As a user, I can see per-edge throughput, lag, credits, spill bytes, and oldest
  pending record age.
- As a system, I got deterministic behavior when a sink remains unavailable past
  the configured retention and capacity limits.
- As a developer, I can reproduce overload and recovery in a local test harness.

## Required experiments

- Slow and unavailable sinks, skewed partitions, and one stalled join input.
- Recovery while upstream continues at peak rate.
- Checkpoint alignment latency versus buffer and state size.
- Capacity planning for steady state, burst, and recovery catch-up.

