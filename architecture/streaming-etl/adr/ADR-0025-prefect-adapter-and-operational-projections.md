# ADR-0025: Feature-flagged Prefect adapter and operational projections

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, SDK/orchestration maintainers
- Depends on: [ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md), [ADR-0001](ADR-0001-runtime-boundary.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-041a…d, W34–W36 in [todo](../todo.md) and [sprint board](../sprint_board.md)

## Problem

A working streaming control service is not yet an integrated Prefect workflow.
Run restarts must reconnect to the same durable stream rather than accidentally
create another channel history. Per-document task runs would also confuse a
long-lived operator with a completed task and add avoidable orchestration load.
Operators need enough correlated telemetry to distinguish lag, admission
pressure, a lost owner and an unfinished recovery.

## Theory

Use Prefect for the outer workflow and long-lived operator runs, and use the
stream runtime for per-input progress. An explicit experimental adapter binds
Prefect identities to one durable control run. Native Prefect state changes
continue through the Prefect API. Telemetry and the operator view derive from
the same read projections as the control service.

## Proposed solution and agreement

Implement ordinary Python integration modules outside Prefect core. Keep the
adapter behind an explicit feature flag and reject an unsupported graph or
missing identity before source admission. The environment has the actual
Prefect 3.6.24 SDK; acceptance uses its real local API server rather than a
mocked orchestration client.

A stream operator is a long-lived task whose decorated generator executes one
terminal `yield` or `return` for each input under the existing protocol. It does
not create a new Prefect task run per document. Platform output/state/cursor
atomicity and client-owned external-effect idempotency remain unchanged.

Persist correlation between Prefect flow/task IDs, the stable logical stream
run, revision, operator, partition, owner epoch and input coordinate. Reconnect
with the explicit existing stream identity and graph contract; a new controller
process must not silently allocate a replacement channel history.

The control plane's desired lifecycle and actual Prefect flow/task states are
separate authorities. Reconcile them through the real Prefect API and document
the resulting states. Do not claim an operator is Completed or Cancelled merely
because a private Python flag changed.

Expose authenticated metrics and an operator view for input lag, in-flight
owner, completed position, checkpoint age, recovery state and admission/retention
pressure. Keep secret tokens out of URLs, persisted UI state and logs. Alerts
must use declared thresholds and link to a tested action in the runbook.

## Execution attempts and bounded authority

A transport/API restart reconnects the existing attempt. Replacing execution
actors creates a new fenced attempt over the same logical stream run. Keep one
current attempt and a fixed retained terminal history; fold older attempts into
a bounded count/digest/horizon head. An explicit older ordinal receives
`AdapterAttemptHorizonExpired`. Native Prefect history has its separate
retention policy; the adapter does not copy it indefinitely.

Controller leadership and native worker ownership are different leases. Losing
the controller lease alone does not revoke an existing R3 worker's ability to
publish. Replacement must atomically fence the old worker epochs/invocations,
change the active attempt, and bind the exact new operator owners and actual
Prefect task identities. Storage must reject a late terminal write even while
the old worker's former lease would otherwise remain valid.

Source actors likewise need per-attempt capability authority. Disable the old
source capability in the same replacement transaction and bind its successor to
the active attempt and exact producer/partition coordinates. A tenant-wide
source token is insufficient for a stale-attempt fence. Acquire capability
locks before revision/run/attempt/worker locks, matching source authorization.

Owner heartbeat renews the existing R4 leader and R3 worker leases; it must not
silently create a new execution identity. Replacement uses a fresh process
identity. An unfinished regional recovery must be explicitly reconciled or
rebound by a tested protocol before an incompatible attempt can activate.

The selected prototype profile therefore rejects the start of regional
recovery while a Prefect execution attempt is active, before any region worker
or recovery plan is published. Regional recovery remains supported by the R3
runtime outside an active Prefect attempt. Composing regional replay ownership
with an active Prefect task binding is future work; the adapter does not leave
workers half-`RESTORING` or claim this unsupported path as recovery evidence.

The adapter acceptance campaign must include an old source token after swap, a
late output under an unexpired old worker lease, an API restart that must retain
the attempt, repeated replacements with bounded metadata, and a retired
flow/attempt retry that must not alias a new execution.

## Stories

- As a developer, I want to deploy a Python streaming flow through an explicit
  experimental adapter and inspect its actual Prefect run identities.
- As a user, I want restart/reconnect to continue the same stream from its first
  incomplete input without duplicating its platform channel history.
- As an operator, I want to identify a stalled owner, backpressure and a pending
  recovery from correlated API, metrics and UI observations.
- As a system, I want stop/drain/cancel to keep native Prefect orchestration and
  durable stream authority coherent under response loss and server restart.

## Acceptance criteria

- As a user, I got a real Prefect flow and long-lived operator task runs with
  immediate per-document delivery before the source finishes.
- As a system, I reject disabled integration, graph drift and incorrect resume
  identity before accepting DATA.
- As a developer, I can restart the controller/API, reconnect through public
  clients and compare exact durable output/state/cursors with the failure-free
  run using independent oracles.
- As a user, I got the documented actual Prefect flow/task states after drain,
  cancellation and worker replacement; state proposals went through its server.
- As an operator, I can inspect authenticated API/UI metrics with consistent
  flow, stream, node and owner correlation, without exposing another tenant.
- As an operator, I can reproduce an alert, perform the runbook's safe recovery
  action, observe returned capacity and see processing resume.

## Scope and evidence required for `accepted`

R4-A must pass before implementation acceptance is attempted. Prototype
integration must use actual installed SDK/server behavior and real HTTP sockets;
it remains outside `src/prefect`. Production packaging, migration/upgrade,
transport/credential security, infrastructure chaos and the actual 72-hour soak
remain later gates. The planned R4-B target is 90%; this document awards no
readiness by itself.

## Operational implementation and review discussion

The operator console is [stream_dashboard.html](../prototypes/stream_dashboard.html),
served by [postgres_operational_server.py](../prototypes/postgres_operational_server.py).
Its authenticated observation, metrics and trace routes authorize the logical
run before reading tenant data. Public static pages contain no run data or
credential. The browser retains its bearer token only in the active session's
memory; reconnect invalidates pending responses, and disconnect/auth failure
clears the credential and rendered identifiers. Drain and Cancel use stable
request IDs and the current leader headers through the actual control API.

[postgres_stream_observability.py](../prototypes/postgres_stream_observability.py)
derives per-input lag, owner/epoch, in-flight work, admission pressure and
checkpoint/recovery age from durable authority. Native flow/task states are
read from the actual Prefect API; unavailable API state is shown explicitly.
Auxiliary metrics/native reads have a declared sampling window and are not one
distributed transaction with the core projection. Metrics cannot change
progress. Prometheus targets are per logical run: node cardinality is bounded
inside a deployment, while an unlimited number of retained run targets is not.
Source counts are cumulative DATA frontiers, not the number of live rows after
garbage collection.

[postgres_stream_telemetry.py](../prototypes/postgres_stream_telemetry.py) uses an
actual OpenTelemetry SDK provider, W3C context propagation, bounded exported
spans/events and an allowlist of correlation fields. It excludes payloads,
tokens and exception messages. Worker-owned providers survive an operational
HTTP server restart. This in-memory prototype exporter is not a durable tracing
backend or a cross-process trace storage service.

[test_postgres_stream_operations.py](../prototypes/test_postgres_stream_operations.py)
passes four focused tests: tenant/auth isolation and read-only metrics, the
capacity-return drill, trace parentage/secret filtering/bounds/API restart, and
linked static alert actions. The drill fills actual admission capacity, proves
that premature retirement returns no credit, completes an input, safely retires
its data/metadata, returns admission credit through HTTP and admits the next
input. The [runbook](../prototypes/stream_runbook.html) documents this order and
the distinction between Drain, Cancel and external client effects.

[test_postgres_operations_browser.py](../prototypes/test_postgres_operations_browser.py)
passes against real Chromium, HTTP and PostgreSQL. It checks live observation,
Drain, actual DRAINED projection, a mobile viewport, absence of browser storage,
session-bound credentials and cleanup after invalid authentication. A captured
desktop screenshot was visually inspected; this test does not claim a deployed
production UI or a persistent live demo.

Principal review identified and closed a gap in the independent
[attempt oracle](../prototypes/postgres_prefect_attempt_oracle.py): terminal
attempts must have matching terminal operator states, replacement digests must
be valid, and active source authority must not be expired. Unsafe controls
deliberately reactivate an old source or corrupt an operator status/digest in a
disposable schema and must be detected. These controls are never recovery APIs.

The full R4-B freeze must include Python and HTML hashes, all historical
PostgreSQL regressions and the actual Prefect integration. Targeted PASS reports
above do not award the 90% gate while maintenance/restart review remains open.

## Interrupted first R4-B candidate and byte-pressure finding

The [first complete-source candidate report](../evidence/postgres-r4b-candidate-2026-10-25.json)
and its [raw log](../evidence/postgres-r4b-candidate-2026-10-25.log) are retained.
The test child was deliberately terminated after 262.742 seconds when late
Principal review found a liveness counterexample. Its return code is -15, its
expected suite size is 138, and it has no completed-suite count. This is an
interrupted candidate, not a test failure being counted as a pass; it earns no
readiness. Source hashes had no drift and the synchronous durability profile
was intact. Later reports must use a new file, preserving this result.

The counterexample uses a byte quota of 10 and a record-based barrier interval
of two. After process loss, one retained input of nine bytes remains after the
last completed cut. A new two-byte input cannot be admitted, while the retained
tail is only one record, so a records-only maintenance threshold never starts
the barrier that could free it. Preflighting only the replacement's new payload
list misses the existing retained bytes.

The agreed correction is a finite maximum checkpoint delay whenever the DATA
frontier has advanced, in addition to the record threshold. This does not delay
or redefine per-input durable completion. The exact retained-tail replacement
must converge under a bounded timeout. The same review found that counting one
live completed checkpoint does not identify initial bootstrap: normal R2
retirement keeps one latest checkpoint. Bootstrap must instead use the durable
participant-anchor witness, with a source-open test proving that an idle stream
does not churn zero-delta checkpoints indefinitely.

## Second R4-B candidate and browser cleanup correction

The [second candidate](../evidence/postgres-r4b-candidate2-2026-10-25.json) ran all
139 tests in 579.704 seconds. It has 138 passing cases and one browser-test
teardown error: Chromium descendants were still writing the temporary profile
after its launcher exited, producing `ENOTEMPTY` during removal. All browser
functional assertions had passed, but failed cleanup still makes this a failed
gate. The report and [raw log](../evidence/postgres-r4b-candidate2-2026-10-25.log)
remain unchanged. Source drift is empty, the durability profile passes and all
538 documentation links checked by that report exist.

The correction isolates the test browser in its own process session, requests
graceful `Browser.close`, waits for exit, confines descendant cleanup to that
session and retries only transient profile-directory races within a bound.
Database/server fixture cleanup now runs in `finally` even if browser teardown
fails. No product behavior, cursor, recovery rule or assertion is bypassed.
A successor full candidate must pass before the gate can earn readiness.

## Third R4-B full candidate

The [third report](../evidence/postgres-r4b-candidate3-2026-10-25.json) passes
139/139 tests in 593.842 seconds, with all 108 Python/HTML hashes unchanged.
PostgreSQL 17.11 uses `remote_apply` with a synchronous streaming replica; all
36 Markdown documents and 540 checked local links pass. Its
[raw log](../evidence/postgres-r4b-candidate3-2026-10-25.log) has SHA-256
`6e301fa98a146e1ed6909224f8bb13f817d11a96c4c0ad010f291eac9b90fee4`.
Compared with the failed second candidate, the only changed source is the
browser test's process/profile teardown; runtime and protocol hashes match.
Principal review, rather than this candidate alone, awards the 90% gate.

The [Principal PASS](../evidence/postgres-r4b-principal-2026-10-25.json) independently
reruns seven hostile cases in 54.321 seconds, verifies all 108 hashes and closes
DS-041a…d. Its SHA-256 is
`49d082e5bf7da83d239498b6dc9987f6cee8a9b7b724b0257710157130a7cbc2`.
W34–W36 are complete and **90% prototype readiness** is earned. Regional recovery
inside an active Prefect attempt, arbitrary byte amplification, direct hostile
database credentials, durable tracing infrastructure and broader production
acceptance retain the explicit limits in that review. This ADR remains
`progress` while [ADR-0026](ADR-0026-release-compatibility-and-disaster-recovery.md)
and the later security/72-hour soak/release criteria remain open.
