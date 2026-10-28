# Segmented 72-hour streaming soak

This harness runs one detached, immutable Docker workload across 18 sequential
four-hour monitor steps in one ARC job, or across monitor jobs on the existing
static runner when explicitly selected. Monitor boundaries do not restart the
ETL process, PostgreSQL primary, or synchronous standby. The ARC profile uses
a fresh dependent job to reconstruct public evidence from bound log frames
and publish its artifact without relying on a 72-hour-old repository token.
See [ADR-0030](../../adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md).

The selected profile is a real PostgreSQL-backed Python yield DAG
`source -> A -> {B,C} -> J -> D`, periodic global barriers, bounded retirement
and metadata GC, release-session fencing, deterministic owner loss, and
scheduled process loss. Actual Prefect integration is covered by the separate
short R4-B gate and is deliberately not claimed by this soak.

```bash
ROOT=/var/tmp/ariadne-soak
RUN="soak-${GITHUB_RUN_ID}"
python cli.py --root "$ROOT" bootstrap --run-id "$RUN" --require-clean \
  --target-seconds 259200 --segment-seconds 14400 --segment-count 18 \
  --process-loss-segments 5,11
python cli.py --root "$ROOT" start --run-id "$RUN"

# A sequential matrix/job chain invokes exactly one segment per job.
python cli.py --root "$ROOT" watch --run-id "$RUN" \
  --segment "$SEGMENT" --duration 14400 --poll-seconds 60

python cli.py --root "$ROOT" status --run-id "$RUN"
python cli.py --root "$ROOT" review --run-id "$RUN"
python cli.py --root "$ROOT" export-public --run-id "$RUN" \
  --output "$ROOT/public-$RUN"
python cli.py --root "$ROOT" stop --run-id "$RUN" --cleanup
```

For local validation, use `--smoke`, a short target, and a declared process-loss
segment. A smoke can prove the controls work, but the reviewer always emits
`long_soak_passed=false` until 72 continuous wall-clock hours and all 18 linked
segments have been observed.

The persistent state directory contains `credentials.json`; it must never be
uploaded. CI artifacts are restricted to `manifest.json`, `segments/*.json`,
`review.json`, and `review.md`. The manifest records dirty local provenance;
`--require-clean` makes CI bootstrap fail unless the pinned checkout is clean.
