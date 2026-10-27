# ADR-0018: PostgreSQL authoritative data plane

- Status: `progress`
- Date: 2026-10-23
- Owners: ETL Architect / HighLoad Principal, storage maintainers
- Depends on: [ADR-0016](ADR-0016-reference-safe-retention-and-bounded-pressure.md), [ADR-0017](ADR-0017-sync-async-generator-adapter.md)
- Refines: [ADR-0006](ADR-0006-durable-channels-and-state.md)
- Leads to: [ADR-0007](ADR-0007-backpressure-and-high-load.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery tasks: DS-010a…d, DS-011a…d

## Problem

SQLite establishes protocol shape but does not establish multi-process
isolation, server-time leases, durable restart behavior, synchronous replicas,
or an operating envelope. The production gate needs one explicit authority and
an acknowledgement point that survives coordinator and backend loss.

## Theory

PostgreSQL row locks, unique constraints, server time, synchronous WAL commit,
and synchronous streaming replication can provide the metadata and hot-record
authority. ObjectRef payloads can initially use a PostgreSQL `bytea` object
table so payload and witness share the same durability domain. This is less
cost-efficient than object storage, but it removes a split publication boundary
while the protocol and load envelope are measured.

## Agreement

Protocol epoch 2 selects PostgreSQL 17 as the first production backend. The
authoritative deployment profile is:

- one writable primary plus one synchronous physical standby;
- `synchronous_commit=remote_apply` and one required synchronous standby;
- persistent volumes for both nodes;
- `max_wal_size=4GB`, `min_wal_size=1GB`, `checkpoint_timeout=15min`,
  `checkpoint_completion_target=0.9` for the measured single-host profile;
- server `clock_timestamp()` for lease expiry;
- transaction-scoped row locks for sequence/capacity allocation;
- recovery admission control sized above the agreed catch-up floor so replay
  cannot consume the live-ingress latency budget;
- owner epoch predicates on every output, state, completion, cursor, and
  diagnostic mutation;
- immutable ObjectRef payload, digest, schema, and visibility witness stored in
  the same PostgreSQL cluster for this profile.

An append acknowledgement is returned only after the transaction commits under
the configured synchronous replication policy. On primary loss, the standby is
promoted and stale connections must reconnect. A deployment with no synchronous
standby may remain available as a weaker profile, but it cannot claim this
ADR's R1-A durability guarantee.

The object table is suitable only while measured payload and throughput SLOs
pass. Moving payloads to external object storage requires a later ADR and must
re-prove publish ordering, orphan repair, read-after-write, and remount behavior.

## Stories

- As a developer, I want one transaction boundary for append, completion,
  managed state, and cursor.
- As a user, I want an acknowledged record to survive primary loss and standby
  promotion.
- As a system, I want two coordinators to allocate one contiguous sequence and
  one terminal completion.
- As an operator, I want capacity and recovery SLOs tied to a concrete database
  configuration.

## Acceptance criteria

- As a system, I got exact append retry and contiguous ordering under concurrent
  coordinators.
- As a system, I reject every stale owner mutation after lease takeover using
  database time.
- As a user, I got acknowledged payload, output, state, completion, and cursor
  after backend restart and synchronous standby promotion.
- As a developer, I can inspect and repair an orphan object without making it
  visible as a stream record.
- As an operator, I got raw p50/p95/p99, sustained ingress, stalled-consumer,
  compaction, failover, and catch-up measurements against thresholds selected
  before the run.
- As a system, I keep record and byte budgets within their hard limits under
  concurrent producers and near-capacity operation.

## Pre-agreed R1-B SLO

The first profile is deliberately modest and reproducible on one development
host:

- payload distribution: 80% 1 KiB, 15% 16 KiB, 5% 256 KiB;
- sustained ingress: at least 500 records/s for 60 seconds;
- append latency: p95 ≤ 40 ms and p99 ≤ 100 ms with synchronous standby;
- failover detection plus promotion: ≤ 15 seconds;
- no acknowledged loss, duplicate sequence, cursor regression, or capacity
  overshoot;
- catch-up throughput after a 10-second consumer outage: at least 1.25× live
  ingress until backlog reaches zero;
- configured backlog must cover at least 60 seconds at the sustained ingress
  and measured payload distribution;
- data compaction must reclaim at least 1.25× measured live ingress, so one
  60-second full horizon returns its credits within 48 seconds;
- metadata compaction must return logical metadata usage to zero for the
  measured partitions, leave only bounded producer/consumer/state anchors, and
  reject a retired retry by name.

These thresholds are fixed before the refreshed benchmark execution. Failure
keeps readiness at the previous gate and changes require a new dated ADR record
rather than editing the result.

## Evidence plan and limits

DS-010 uses Dockerized PostgreSQL primary/standby with named persistent volumes,
two independent coordinator processes, abrupt process loss, container restart,
and promotion. DS-011 adds reference-safe compaction, enforced capacity,
versioned benchmark results, stalled consumers, and catch-up.

This ADR does not claim multi-region durability, zero-RPO under simultaneous
loss of both PostgreSQL volumes, external object-store semantics, or Prefect
server integration. It remains `progress` until production-shaped code and all
acceptance criteria have evidence.

## Executed evidence

The production-shaped prototype now uses PostgreSQL 17 through independent
worker pools. Twenty-two focused backend tests cover concurrent contiguous
append, exact retry, atomic record/byte/metadata/consumer limits, server-time
lease takeover and stale-writer rejection, staged-orphan invisibility,
reference-safe compaction, tombstone and retired-horizon retry, retained-floor
enforcement, credit return, root-creation races, forged delivery attempts,
bounded delivery with whole-range replay, exact metadata-operation identity,
retired-watermark anchors, content-address verification, independent-oracle
corruption controls, and the time budget. The time check uses an index on
`(channel, partition_id, created_at)`; the negative test makes the oldest live
record exceed the budget, observes append blocking, compacts the completed
prefix, and then observes the exact retry succeed.

The bounded batch is a durability optimization, not a completion shortcut. The
platform first persists one contiguous in-flight range and returns those
envelopes to client code. Completion then writes one exact `RETURN` witness per
input and advances the cursor for the range in one transaction. Loss before the
transaction redelivers the whole range; an exact retry after commit returns the
stored result; owner, epoch, attempt, envelope, state, or range drift is
rejected. The maximum range is 32 and the measured catch-up profile uses eight.

The release-3 HA harness creates separate primary and standby
containers and named volumes, waits for `streaming/sync`, enables
`remote_apply`, and uses the actual data-plane schema. It restarts the primary,
then kills it while append and `RETURN` completion are active. The promoted
standby retained all 24 appends acknowledged before loss and the exact durable
cursor at 20. Retrying all 200 immutable attempts produced one contiguous
sequence 1…200 and the replacement drained the suffix to cursor 200. Restart
took 0.410 seconds and promotion took 1.726 seconds against the fixed 15-second
limits. The former primary volume was quarantined and could not rejoin as a
writer. Raw evidence:
[release-3 HA](../evidence/postgres-ha-release3-2026-10-24.json).
The evidence binds harness SHA-256
`9d207a725c13e4471e7cfbcc2705f3d466162ed86465909d7fd1eef29cbeee60`
and data-plane SHA-256
`02abed18e9a419d826b970468408a8ccb61c862abf2f89c46afb90eb4f06965e`.

The release load harness applies the declared payload distribution independently
to four ordered partitions and measures every record from its deterministic
scheduled offer to group-commit acknowledgement. One process runs four async
producer coroutines over four pre-warmed connections. Global batch ordinals
place one three-record batch every `3/510` seconds instead of creating a
four-partition burst. A separate consumer process starts with `spawn`, opens its
own pool, signals READY before measurement, and waits while ingress runs alone
for ten seconds. Existing consumer roots and leases are also created before the
measurement; at `t=10` the parent sends only GO. This models a stalled existing
consumer without charging process and control-plane startup to steady-state
append latency.

Catch-up admission control is fixed at 900 records/s: above the required
637.5/s while preventing recovery work from starving live ingress. The harness
retains scheduled-offer latency semantics and records event-loop wake, group
queue, payload encoding, database call, top-20 batch outliers, process topology,
and PostgreSQL checkpointer/WAL/database counters. It uses normal bounded
delivery and completion, runs actual data and metadata compaction over the live
measured partitions, and invokes both the implementation invariant report and
an independent read-only oracle process. Completion rows for a range use one
database batch while preserving one durable witness per input.

Two consecutive unchanged-code trials each ingested 30,600 records in 60
seconds at 510 records/s, above the pre-agreed 500 records/s threshold. Trial
29 measured p95 22.80 ms, p99 27.66 ms, 900.73 completion/s, and 788.89
compacted records/s. Trial 30 measured p95 22.57 ms, p99 26.91 ms, 901.21
completion/s, and 760.87 compacted records/s. In both trials the 60-second
record, byte, and time horizons passed; compaction reclaimed all 30,600 records
and 501,350,400 payload bytes above the 637.5/s requirement. Record, byte, and
logical metadata usage returned to zero. Each partition retained four bounded
hot rows: latest completion, consumer, producer, and the shared current
metadata-operation witness. Retired exact retry was rejected by name; the
independent oracle verified four complete summary prefixes, exact retired
anchors, no live records, no tombstones, and no object identities. Raw evidence:
[trial 29](../evidence/postgres-load-release-trial29-2026-10-24.json) and
[trial 30](../evidence/postgres-load-release-trial30-2026-10-24.json).
Both bind load-harness SHA-256
`7c6ef7c83149ade3e9a0da68ae3dd8022ae0dcfa9cda7e0dff95b7da1fa09dce`,
the data-plane hash above, and oracle SHA-256
`2c1db770e65277a90ced9c2f10af83ab16a8e11aae0da4fb346dd445bde5d2e2`.

Failed theories remain observable. Earlier trials exposed a full scan in the
time-budget query, synchronized large-payload bursts, a 1 ms empty-consumer
poll storm, and unstable latency at the optional 600 records/s offered rate.
They selected the age index, phase offsets, 20 ms idle poll, and the explicit
510 records/s acceptance workload without changing the pre-agreed 500 records/s
minimum or latency thresholds. Trial 6 also crossed a primary checkpoint and
trial 9 crossed a standby restartpoint; both kept all correctness invariants but
failed latency. Refreshed trials 14–20 additionally rejected shared object keys
between partitions, two-record and byte-bounded batch experiments, larger WAL
buffers, unbounded catch-up, and a single-process/single-event-loop harness.
Per-stage diagnostics showed that unbounded recovery and client scheduler
contention, rather than payload size or PostgreSQL append time, formed the
tail. Natural trials 25 and 27 then disproved that an isolated passing pair or a
700/s catch-up setting was sufficient. The four-producer-process probe worsened
scheduler tails on the five-CPU host. Trial 28 passed with a single producer
event loop but still created control-plane state and forked the consumer inside
the measurement. The final READY/GO plus uniform-offer method removes that
measurement artifact without changing rate, payload, offer timestamps, or SLO.
All failed and superseded artifacts remain under `evidence/postgres-load-*` and
are not counted as passes. Checkpoint/restartpoint latency under continuous soak
remains an explicit later-gate concern, not an R1-B claim.

The ETL/HighLoad Principal independently reran the 22-test backend suite,
verified trial 29/30 provenance and release-3 HA against the current hashes, and
recorded R1-A and R1-B PASS with no blockers on 2026-10-24. Trial 29 crossed a
timed checkpoint and still met the percentile SLO. The resulting 30% storage
and load gate is earned; this ADR remains `progress` until the later production
integration and operational acceptance gates.

The older [data-plane HA run](../evidence/postgres-ha-dataplane-load-2026-10-24.json)
and [2026-10-23 harness](../evidence/postgres-ha-2026-10-23.json) remain as
historical evidence. The latter exercised an unrelated row and is insufficient
for the current gate.
