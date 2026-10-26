# Short release and schema-recovery runbook

Authority: [ADR-0026](adr/ADR-0026-release-compatibility-and-disaster-recovery.md).
Acceptance: [ADR-0027](adr/ADR-0027-short-acceptance-and-separate-long-soak.md).
Related: [stream alerts and drain](prototypes/stream_runbook.html),
[long CI campaign](soak_ci.md), [tasks](todo.md).

## Preconditions and reproducible commands

This procedure reproduces isolated prototype experiments. It does not migrate
an application database or establish whole-cluster disaster recovery. Use the
dedicated PostgreSQL 17 synchronous test pair: port 55432, container
`ariadne-stream-postgres`, `remote_apply`, and a streaming synchronous replica.
Keep the registry available outside the backed-up schema. Use the prepared
Python environment with the prototype dependencies; Docker and Chromium must
be available. Fixtures create unique schemas and disposable restore targets.
The prepared Prefect fixture registers an explicit local result-storage block
through the public SDK; stream DATA/state authority remains in PostgreSQL and
native result persistence stays disabled. This avoids the inherited automatic
storage-discovery client lifetime issue documented in
[ADR-0029](adr/ADR-0029-short-capacity-calibration-and-release-scope.md).
Do not promote, delete or prune unrelated containers or volumes.

From the repository root, activate that environment, then run:

```shell
cd architecture/streaming-etl/prototypes
python -m unittest -v test_postgres_release_authority
python -m unittest -v test_postgres_schema_backup
python -m unittest -v test_postgres_network_faults
python -m unittest -v test_postgres_isolated_operator
python -m unittest -v test_postgres_short_acceptance
python postgres_release_gate_harness.py --phase R5-A \
  --output ../evidence/postgres-r5a-local.json --timeout 1500
```

Every selected profile must pass without skips. The harness archives exact
commands, source hashes, raw output, counts, documentation checks and the
synchronous-replication profile. A green report still requires independent review.

## Migration and reconnect

`test_real_attempt_migration_is_atomic_and_stale_connection_fenced` starts from
the actual R4-A authority schema, performs additive EXPAND, then semantic CUTOVER.
An old connection that passed admission before cutover must fail on its next
protected write. Exact retries return the original outcome; changed identities
fail before mutation. Do not repair compatibility by editing a release marker.

`test_real_process_loss_rolls_back_or_recovers_migration` exits a fresh process
at DDL-before-marker, cutover-before-commit and after-commit boundaries. A fresh
successor must observe the complete operation or no operation. A lost response
does not prove that a transaction rolled back.

## Quiesce and establish the backup cut

1. Call `PostgresReleaseAuthority.quiesce(session_id, run_id=...)`. If an
   invocation, recovery or incomplete maintenance mutation is active, expect
   `ReleaseQuiesceBusy` without a partial fence. Finish that work through its
   normal authority before retrying.
2. Successful quiescence returns a manifest and exact canonical cuts. The
   release is QUIESCED and old sessions are fenced; the logical run remains
   ACTIVE. Pending DATA is the recoverable suffix. Do not advance a cursor by hand.
3. Call `create_schema_backup(registry, release, quiesced, artifact_path,
   source_container=...)`. It holds an exported REPEATABLE READ snapshot through
   actual `pg_dump --snapshot`, verifies the physical source and retained bytes,
   and publishes the artifact plus receipt.
4. Preserve the artifact, sidecar receipt and completed registry manifest. They
   bind the LSN, physical scope, schema identity and stream cuts. The selected
   artifact cap is 64 MiB; an oversized export fails explicitly.

The executable reference is
[`test_actual_export_restore_one_grant_and_first_incomplete_resume`](prototypes/test_postgres_schema_backup.py).
It completes inputs 1 and 2, retains input 3, and proves resumption from that
first incomplete input. A live backup cannot become a writable target after
its data cuts advanced unless a supported rewind can recover the difference.

## Restore, retire the source and activate one target

1. Create a fresh isolated PostgreSQL system with the same schema name. Run
   `restore_schema_backup(...)`. It verifies the artifact hash, performs a real
   transactional `pg_restore`, and checks catalog identity and retained bytes.
   The restored ACTIVE logical run remains behind a QUIESCED release guard.
2. On the source call `finalize_source(...)`, then `mark_cutover_retired(...)`,
   with the original quiesce digest and cuts. Narrow finalization cancels the
   source run and revokes actors without changing its backed-up DATA cuts.
   Its release becomes CUTOVER_RETIRED. Direct table edits are unsupported.
3. Call `registry.mint_grant(backup_id, source_authority, target_authority)`.
   Before source retirement this must fail. The signed grant binds backup,
   physical source/target, schema, cuts and next generation. Competing targets
   get exactly one winner.
4. On the winning target call `activate_restored(...)` with the compatible
   runtime. Signature, pinned issuer, target, schema, cuts and retained bytes
   must pass before a local witness or writable session can appear.
5. Open the session-bound pool, use `replace_execution(...)` to replace source
   and worker attempt authority, reacquire coordinator ownership and resume the
   suffix. This activates the restored ACTIVE run; it does not reopen the
   original CANCELLED source. Old source capabilities must still fail.
6. Run stream, join, control, attempt and release oracles on the target and the
   release oracle on the retired source. Same-target retries must return the
   original grant/session.

Exact argument construction and cleanup order are in the linked schema-backup
test. Its `AsyncExitStack` closes bound pools before removing disposable targets.

## Failure handling

| Observation | Expected response |
| --- | --- |
| Quiesce is busy | No partial fence; finish in-flight work, then retry. |
| Backup process exits during publication | Retry the same request. A valid durable receipt reconciles it; an unproven snapshot is rejected before a new export. |
| Artifact, receipt, schema or retained bytes are missing/corrupt | Named refusal; never invent payloads or silently replace a committed backup. |
| Publication response is lost | Repeat the same immutable request and inspect its durable outcome. |
| Grant belongs to another physical target | Refuse activation; never reassign the consumed grant. |
| Registry/key is unavailable or unverifiable | Fail closed; a caller's success flag is not authority. |
| Activation response is lost | Retry the same grant and actor on the same target. |
| Winning target is lost after grant consumption | That backup cannot activate elsewhere in this profile. A surviving healthy generation can establish a new backup/grant. |

The crash test covers five real process-exit boundaries: pending manifest,
dump-before-receipt, receipt-before-artifact, artifact-before-registry, and
registry-before-response. Socket tests cut before forwarding and after commit.
Another test terminates its own PostgreSQL connection before terminal commit.
Two-generation restoration verifies rolling grant history and the next suffix.

Fixtures clean their own schemas and disposable targets. After interruption,
preserve failed artifacts, receipt, report and raw log before removing only the
identified test resources. Never run global Docker prune or remove the shared pair.

## Limits

- The recovery point is the quiesced backup cut. Zero RPO, recovery of later DATA
  and whole-cluster disaster recovery are not established. Physical HA is separate.
- External effects before a missing terminal yield/return can repeat. Client
  code owns [idempotency](adr/ADR-0011-yield-completion-and-client-effects.md).
- The registry is outside the tenant dump but remains required live authority;
  its own disaster recovery is outside this schema experiment.
- Native Prefect, release/restore and isolation tests are independent profiles,
  not one production execution containing every boundary.
- Isolated execution supports one pinned uppercase artifact without network or
  connector credentials. Docker, the builder, host kernel and platform process
  remain trusted. General hostile-kernel or multi-tenant isolation is not claimed.
- Short observations do not establish long-term WAL, vacuum, memory or disk
  stability. [DS-052a](soak_ci.md) remains separate.
