# ADR-0010: Platform-owned durable yield channel

- Status: `progress`
- Date: 2026-10-21
- Owners: ETL Architect / HighLoad Principal, runtime and storage maintainers
- Depends on: [ADR-0001](ADR-0001-runtime-boundary.md)
- Supersedes core progress semantics in: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0005](ADR-0005-operator-contract.md), [ADR-0006](ADR-0006-durable-channels-and-state.md)
- Leads to: [ADR-0011](ADR-0011-yield-completion-and-client-effects.md), [ADR-0003](ADR-0003-branch-and-join-recovery.md), [ADR-0007](ADR-0007-backpressure-and-high-load.md)

## Problem

A Python task that consumes 500 documents must not wait for all 500 before its
downstream task starts. Each `yield` must become independently available. When a
consumer worker fails while processing document 200, its replacement must read
document 200 again if no durable `yield` or `return` was observed, or document
201 if document 200 completed.

Periodic graph checkpoints alone are too coarse for this rule. A destructive
queue also cannot serve independent branches or replay an earlier record.

## Theory

A platform-owned append-only log, ordered inside each partition, can expose each
producer `yield` immediately and retain it for independent consumers. Per-
consumer progress separates a currently delivered record from the last record
whose downstream `yield` or `return` was durably observed.

## Agreement

Every streaming edge is a durable append-only channel. The runtime guarantees
strict order inside a partition. A deployment that needs one total order uses
one partition; no order is promised between partitions.

The channel stores immutable envelopes rather than large payloads:

```text
DATA {
  channel_id, partition_id, sequence, record_id,
  lineage_digest, payload_ref, payload_digest,
  payload_size, schema_id, producer_epoch
}
```

The payload behind `payload_ref` must be durable before its envelope becomes
visible. Uploads that never acquire a visible envelope are orphan candidates for
garbage collection.

Each consumer partition stores these distinct fields:

```text
completed_through = 199
in_flight         = { sequence: 200, attempt: 1,
                      owner: "anna", owner_epoch: 7, lease: ... }
resume_from       = 200
```

`completed_through` advances only after the runtime durably records the
consumer's terminal `yield` or `return`. A worker crash or lease loss before that
point leaves the sequence incomplete. A new fenced owner receives the same
sequence under a higher attempt and owner epoch.

For one input sequence, the platform progress transition stores in one data-
plane transaction:

- the terminal `YIELD` or `RETURN` marker;
- the immutable manifest of platform-managed output envelopes;
- the new `completed_through` value;
- any managed state delta or version required to start at the next sequence.

Output envelopes become visible with that transition. A stale owner cannot
complete after takeover. Client-side external effects are outside this
transaction and follow [ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

Protocol epoch 2 initially permits one terminal `yield` or `return` per input
record and one in-flight record per consumer partition. A future multi-output
contract must durably number `emit` operations and advance the input cursor only
on an explicit terminal completion; observing the first of several yields is
insufficient.

Coordinator-injected `BARRIER` records remain separate. They create periodic
consistent graph cuts for state, branches, and joins. They are never inserted
retroactively after a worker crash and do not replace per-consumer progress.

The authoritative hot path belongs to the platform data plane. A first backend
may use dedicated tables in the same database, but control-plane state consumes
a projection rather than participating in a dual write.

The prototype assumes a non-Byzantine storage authority: workers use the runtime
API and do not possess credentials that can forge completion rows or disable
schema guards. Within that boundary, storage constraints reject cursor jumps,
witness deletion, mutable object identity, and output/completion drift. Giving
arbitrary SQL credentials to workers additionally requires privilege separation
or signed receipts.

## Example

```text
Petya channel:  ... 198, 199, 200, 201, 202 ...
Anna:           completed_through=199, in_flight=200
Anna fails
Tanya takeover: completed_through=199, in_flight=200, owner_epoch+1
Tanya reads:    200, then 201, then 202
```

If Anna's output for 200 and its completion marker were already durable, the
stored position would instead be `completed_through=200`, and Tanya would start
with 201.

## Stories

- As a developer, I want a yielded document to reach downstream immediately.
- As a user, I want a replacement worker to resume at the first input without a
  durable downstream `yield` or `return`.
- As a system, I want immutable ordered records and fenced consumer ownership.
- As a system, I want state required for sequence 201 to be durable when
  sequence 200 advances.

## Acceptance criteria

- As a developer, I can stream `ObjectRef` values without collecting a batch.
- As a user, I got document 200 again when its delivery ended without a durable
  `yield` or `return`.
- As a user, I got document 201 when document 200's completion was durable.
- As a system, I can reject stale-owner completion and record-identity drift.
- As a system, I got output envelopes, managed state, and progress together or
  none of them after every injected crash.
- As a system, I can retain data until all mandatory consumers and recoverable
  checkpoint cuts have advanced beyond it.

## Prototype evidence

[durable_stream_v2.py](../prototypes/durable_stream_v2.py) is the first
executable protocol-epoch-2 model. Its focused tests cover 500 durable input
references, Anna-to-Tanya takeover on sequence 200, output/progress rollback,
stale-owner fencing, durable-object admission, managed state on completion, a
two-connection takeover race, forged completion, shared-output collapse, and
pre-existing-output adoption and raw-storage mutation counterexamples.
Exact completion retry also validates the full stored envelope and canonical
manifest rather than a partial record identity.

ETL/HighLoad Principal hostile review passed this focused DS-002 model on
2026-10-21 within the documented non-Byzantine runtime/storage-authority scope.
At that point the R0 gate remained open and readiness was 0%. DS-003…DS-007,
the PostgreSQL storage/load gates, production-backed runtime/replacement, and
the PostgreSQL coordinator gate subsequently passed Principal review; current
earned readiness is 64% as
recorded in [ADR-0012](ADR-0012-protocol-epoch-2-delivery-gates.md).

The existing [barrier prototype](../prototypes/barrier_protocol.py) remains
reusable evidence for ordered records, fencing, alignment, and periodic cuts. It
does not itself implement the new `in_flight/completed_through` contract.

## Evidence required for `accepted`

- Production channel backend and ObjectRef integration.
- Process-loss tests on every upload, append, delivery, completion, takeover,
  barrier, and retention boundary.
- Multi-partition load, lag, retention, and recovery measurements.
- SDK tests proving one input cannot be advanced by a nonterminal output.

