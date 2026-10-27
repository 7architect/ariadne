# ADR-0020: Bounded metadata and idempotency horizon

- Status: `progress`
- Date: 2026-10-24
- Owners: ETL Architect / HighLoad Principal, storage maintainers
- Depends on: [ADR-0016](ADR-0016-reference-safe-retention-and-bounded-pressure.md), [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md)
- Refines: [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-011a…d

## Problem

Deleting payload records while retaining every append attempt, completion,
tombstone, manifest, and object identity bounds live data but leaves total
database growth unbounded. Deleting those witnesses without another authority
would let an old retry become a second append and would erase the binding
between input completion and yielded output.

## Theory

A bounded set of producers with monotonic sequence numbers can replace old
per-record retry witnesses with a retired watermark. A bounded set of consumers
can do the same for completion retries while retaining its latest state
projection. Content-addressed ObjectRef keys preserve object identity after the
per-object registry row is retired. One atomic transaction can advance a
partition's hash-chain accumulator, advance producer and consumer watermarks,
and delete the corresponding hot detail.

## Agreement

The PostgreSQL schema namespace represents one job and graph/deployment version.
A production producer is registered for exactly one channel partition and has a
strict contiguous `producer_sequence`. The registered producer set is bounded
per partition and producer IDs are never reused. At most the next sequence may
be new; the current pending head may be retried exactly; gaps and divergent
reuse are rejected. A blocked append does not advance `accepted_through`.

GC-eligible ObjectRefs use
`sha256(schema_id || NUL || canonical_payload)` as their key. Arbitrary legacy
keys remain supported only inside a visible hard metadata budget and are not
eligible for metadata retirement.

`compact_metadata` takes a set of partition cuts. It accepts only contiguous
prefixes whose DATA has already been compacted and which contain no active
retention root or in-flight invocation. A YIELD input can retire only in the
same transaction as its output partition and output sequence. The transaction
updates a monotonic rolling digest, producer `retired_through`, consumer
`completion_retired_through`, hot-metadata credit, and all detail deletes.
Operation ID is `metadata:sha256(canonical_full_cut_map)`. The same transaction
stores one immutable operation witness containing that full cut map, its digest,
and the exact outcome. An immediate lost-ack retry returns that outcome; changed
cuts or a changed partition set cannot reuse the ID. A later operation removes
old witnesses after no current summary references them, so witness count is
bounded by the partition count. Content-derived identity prevents a retired ID
from being reused for future cuts without an unbounded global registry.

Within the hot horizon, exact append and completion retries return the original
location and drift is rejected. At or below a retired watermark both exact and
drifted retries receive `IdempotencyHorizonExpired`; the request is never
reinterpreted as new work. The rolling digest proves aggregate contiguous
coverage and tamper evidence. Exact historical audit after retirement requires
a future external sealed raw-metadata archive and is outside this ADR.

## Stories

- As a developer, I want retry semantics to change explicitly at a bounded
  horizon rather than silently forgetting an idempotency key.
- As a user, I want long-running streams to reuse metadata capacity without
  losing current recovery state.
- As a system, I want producer, consumer, input, and output retirement to be one
  atomic authority transition.
- As an operator, I want hot metadata rows, age, usage, limits, and the binding
  pressure dimension exposed by the public pressure API.

## Acceptance criteria

- As a developer, I can retry a hot append exactly, while hot drift, sequence
  gaps, and concurrent divergent reuse are rejected.
- As a user, I got `IdempotencyHorizonExpired` for exact or drifted append and
  completion retries after retirement.
- As a system, I got either the old complete hot state or the new complete
  summary state under retry-versus-retire and transaction loss; I never got a
  second append.
- As a system, I can account for retired summary ranges plus hot ranges as the
  exact interval `[1,next_sequence)` without overlap or gaps.
- As a system, I reject early retirement of a YIELD whose output partition is
  absent from the same cut.
- As an operator, I got at least 100 append/complete/data-compaction/metadata-
  compaction cycles under fixed metadata limits with bounded hot rows and a
  fresh independent-oracle PASS.
- As a developer, I can distinguish aggregate post-horizon evidence from an
  exact external audit archive.

## Current evidence

The PostgreSQL prototype implements bounded producer registration, strict
producer sequences, content-addressed keys, per-producer and per-consumer
retired watermarks, atomic multi-partition retirement, a rolling digest,
logical metadata credits, public pressure observations, and named expiry. The
backend suite includes 100 complete cycles under a one-record metadata budget,
retry-versus-retire serialization, gap/divergence controls, and independent
summary/hot-range verification. Exact batch retry validates every supplied
envelope against live or tombstoned identity before returning a prior result;
content identity remains authoritative after payload deletion.

The immutable operation witness binds the canonical full cut map, its digest,
the exact per-partition outcome, each rolling digest, and canonical retired
producer/consumer anchors. The independent oracle reverses every hot record to
exactly one accepted append attempt, recomputes every live content address,
requires exact contiguous `(retired_through, accepted_through]` producer ranges,
and rejects a retired watermark without the matching operation anchors. A
consumer batch that straddles `completion_retired_through` is expired as a
whole; a fully hot suffix retains exact retry. Consumer creation is serialized
under the partition lock and enforced by the public `max_consumers` dimension.

Two unchanged-code 60-second load trials each retired 30,600 append attempts,
30,596 historical completion rows, and every data tombstone.
Logical metadata usage returned to zero, each partition retained four bounded
hot rows, immediate operation retry returned the same summary, and a retry
behind the producer watermark received `IdempotencyHorizonExpired`. The one
content-derived operation witness is shared by all four current summaries and
superseded witnesses are collected after no summary references them. The
independent oracle accounted for four retired prefixes, their exact anchors,
and four latest state completions with no live records, tombstones, or object
identities. Raw evidence:
[trial 29](../evidence/postgres-load-release-trial29-2026-10-24.json) and
[trial 30](../evidence/postgres-load-release-trial30-2026-10-24.json).
They bind data-plane SHA-256
`02abed18e9a419d826b970468408a8ccb61c862abf2f89c46afb90eb4f06965e`
and oracle SHA-256
`2c1db770e65277a90ced9c2f10af83ab16a8e11aae0da4fb346dd445bde5d2e2`.

This ADR remains `progress` because production integration and external sealed
raw-metadata archival are later work. The ETL/HighLoad Principal independently
reran the 22-test backend suite, audited both frozen-hash load artifacts, and
recorded R1-B PASS with no blockers on 2026-10-24.
