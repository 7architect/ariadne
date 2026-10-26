"""Durable orchestration intent over the PostgreSQL streaming authority.

Control rows never copy cursor, barrier, owner, or recovery truth. Those values
are projected from protocol-epoch-2 tables in a repeatable-read transaction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_stream_v2 import PostgresProtocolError, PostgresStream


class AuthenticationFailed(PostgresProtocolError):
    pass


class AuthorizationFailed(PostgresProtocolError):
    pass


class ControlConflict(PostgresProtocolError):
    pass


class ControlNotFound(PostgresProtocolError):
    pass


class ControlCapacityBlocked(PostgresProtocolError):
    pass


class ControlIdempotencyHorizonExpired(PostgresProtocolError):
    pass


class StaleLeader(PostgresProtocolError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def token_digest(token: str) -> str:
    if not token:
        raise AuthenticationFailed("bearer token is required")
    return hashlib.sha256(token.encode()).hexdigest()


@dataclass(frozen=True)
class LeaderLease:
    deployment_id: str
    leader_id: str
    leader_epoch: int


class PostgresControlPlane:
    """Store desired lifecycle and project existing data-plane authority."""

    def __init__(self, stream: PostgresStream):
        self.stream = stream
        self.pool = stream.pool
        self.schema = stream.schema

    @classmethod
    async def install(cls, stream: PostgresStream) -> PostgresControlPlane:
        control = cls(stream)
        await control._initialize()
        return control

    @classmethod
    def connect_existing(cls, stream: PostgresStream) -> PostgresControlPlane:
        """Connect a restarted server without running DDL."""
        return cls(stream)

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS control_tenants_v1(
          tenant_id text PRIMARY KEY CHECK(tenant_id<>''));
        CREATE TABLE IF NOT EXISTS control_capabilities_v1(
          token_digest text PRIMARY KEY,
          tenant_id text NOT NULL REFERENCES control_tenants_v1,
          capabilities jsonb NOT NULL,disabled boolean NOT NULL DEFAULT false,
          expires_at timestamptz);
        CREATE TABLE IF NOT EXISTS control_deployments_v1(
          deployment_id text PRIMARY KEY,
          tenant_id text NOT NULL REFERENCES control_tenants_v1,
          revision_id text NOT NULL UNIQUE REFERENCES deployment_revisions,
          graph_version text NOT NULL,graph_digest text NOT NULL,
          created_at timestamptz NOT NULL DEFAULT clock_timestamp());
        CREATE TABLE IF NOT EXISTS control_leaders_v1(
          deployment_id text PRIMARY KEY REFERENCES control_deployments_v1,
          leader_id text NOT NULL CHECK(leader_id<>''),
          leader_epoch bigint NOT NULL CHECK(leader_epoch>0),
          lease_expires_at timestamptz NOT NULL);
        CREATE TABLE IF NOT EXISTS control_partition_ownership_v1(
          deployment_id text NOT NULL REFERENCES control_deployments_v1,
          tenant_id text NOT NULL REFERENCES control_tenants_v1,
          revision_id text NOT NULL REFERENCES deployment_revisions,
          channel text NOT NULL,partition_id text NOT NULL,
          partition_role text NOT NULL CHECK(partition_role IN('SOURCE','OUTPUT')),
          PRIMARY KEY(deployment_id,channel,partition_id),
          UNIQUE(channel,partition_id));
        CREATE TABLE IF NOT EXISTS control_runs_v1(
          run_id text PRIMARY KEY,request_id text NOT NULL,
          tenant_id text NOT NULL REFERENCES control_tenants_v1,
          deployment_id text NOT NULL REFERENCES control_deployments_v1,
          revision_id text NOT NULL UNIQUE REFERENCES deployment_revisions,
          execution_mode text NOT NULL
            CHECK(execution_mode IN('LEGACY_SOURCE','NATIVE_JOIN')),
          desired_state text NOT NULL CHECK(desired_state IN('ACTIVE','DRAINING','CANCELLED')),
          lifecycle text NOT NULL CHECK(lifecycle IN('ACTIVE','DRAINING','DRAINED','CANCELLED')),
          next_admission_ordinal bigint NOT NULL DEFAULT 1 CHECK(next_admission_ordinal>0),
          max_admission_records bigint NOT NULL CHECK(max_admission_records>0),
          max_admission_bytes bigint NOT NULL CHECK(max_admission_bytes>0),
          used_admission_records bigint NOT NULL DEFAULT 0
            CHECK(used_admission_records>=0 AND used_admission_records<=max_admission_records),
          used_admission_bytes bigint NOT NULL DEFAULT 0
            CHECK(used_admission_bytes>=0 AND used_admission_bytes<=max_admission_bytes),
          drain_admission_cut bigint CHECK(drain_admission_cut>=0),
          control_version bigint NOT NULL DEFAULT 1 CHECK(control_version>0),
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          UNIQUE(tenant_id,deployment_id,request_id));
        CREATE TABLE IF NOT EXISTS control_run_consumers_v1(
          run_id text NOT NULL REFERENCES control_runs_v1,
          consumer_id text NOT NULL REFERENCES consumers,
          channel text NOT NULL,partition_id text NOT NULL,
          PRIMARY KEY(run_id,consumer_id));
        CREATE TABLE IF NOT EXISTS control_source_ownership_v1(
          run_id text NOT NULL REFERENCES control_runs_v1,
          revision_id text NOT NULL REFERENCES deployment_revisions,
          channel text NOT NULL,partition_id text NOT NULL,
          PRIMARY KEY(run_id,channel,partition_id),UNIQUE(channel,partition_id));
        CREATE TABLE IF NOT EXISTS control_source_admissions_v1(
          run_id text NOT NULL REFERENCES control_runs_v1,
          admission_ordinal bigint NOT NULL CHECK(admission_ordinal>0),
          admission_id text NOT NULL UNIQUE,request_id text NOT NULL,
          channel text NOT NULL,partition_id text NOT NULL,record_id text NOT NULL,
          attempt_id text NOT NULL,object_key text NOT NULL,payload_digest text NOT NULL,
          producer_id text NOT NULL,producer_sequence bigint NOT NULL CHECK(producer_sequence>0),
          payload_size bigint NOT NULL CHECK(payload_size>=0),schema_id text NOT NULL,
          lineage_digest text NOT NULL,
          sequence bigint CHECK(sequence>0),
          status text NOT NULL CHECK(status IN('ADMITTED','PUBLISHED')),
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),published_at timestamptz,
          PRIMARY KEY(run_id,admission_ordinal),UNIQUE(run_id,request_id),
          UNIQUE(attempt_id),
          UNIQUE(run_id,channel,partition_id,record_id),
          UNIQUE(run_id,channel,partition_id,producer_id,producer_sequence));
        CREATE TABLE IF NOT EXISTS control_admission_producers_v1(
          run_id text NOT NULL REFERENCES control_runs_v1,
          channel text NOT NULL,partition_id text NOT NULL,producer_id text NOT NULL,
          accepted_through bigint NOT NULL DEFAULT 0 CHECK(accepted_through>=0),
          retired_through bigint NOT NULL DEFAULT 0 CHECK(retired_through>=0),
          retired_records bigint NOT NULL DEFAULT 0 CHECK(retired_records>=0),
          retired_bytes bigint NOT NULL DEFAULT 0 CHECK(retired_bytes>=0),
          rolling_digest text NOT NULL,
          PRIMARY KEY(run_id,channel,partition_id,producer_id));
        CREATE UNIQUE INDEX IF NOT EXISTS control_admission_producer_location_unique
          ON control_admission_producers_v1(run_id,channel,partition_id);
        CREATE TABLE IF NOT EXISTS control_commands_v1(
          run_id text NOT NULL REFERENCES control_runs_v1,request_id text NOT NULL,
          command text NOT NULL CHECK(command IN('DRAIN','CANCEL')),
          leader_id text NOT NULL,leader_epoch bigint NOT NULL CHECK(leader_epoch>0),
          outcome jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          PRIMARY KEY(run_id,request_id),UNIQUE(run_id,command));
        CREATE TABLE IF NOT EXISTS control_worker_fences_v1(
          run_id text PRIMARY KEY REFERENCES control_runs_v1,
          fence_epoch bigint NOT NULL CHECK(fence_epoch>0),
          reason text NOT NULL CHECK(reason IN('CANCELLED','DRAINED')),
          created_at timestamptz NOT NULL DEFAULT clock_timestamp());
        CREATE UNIQUE INDEX IF NOT EXISTS control_command_kind_unique
          ON control_commands_v1(run_id,command);

        CREATE OR REPLACE FUNCTION control_reject_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'control authority immutable'; END $$;
        CREATE OR REPLACE FUNCTION control_guard_run_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF (NEW.run_id,NEW.request_id,NEW.tenant_id,NEW.deployment_id,
              NEW.revision_id,NEW.execution_mode,NEW.created_at)
             IS DISTINCT FROM
             (OLD.run_id,OLD.request_id,OLD.tenant_id,OLD.deployment_id,
              OLD.revision_id,OLD.execution_mode,OLD.created_at)
          THEN RAISE EXCEPTION 'control run identity immutable'; END IF;
          IF (NEW.max_admission_records,NEW.max_admission_bytes) IS DISTINCT FROM
             (OLD.max_admission_records,OLD.max_admission_bytes)
          THEN RAISE EXCEPTION 'control admission capacity immutable'; END IF;
          IF NEW.control_version<>OLD.control_version+1
          THEN RAISE EXCEPTION 'control version must advance once'; END IF;
          IF OLD.desired_state='CANCELLED' AND
             (NEW.desired_state,NEW.lifecycle) IS DISTINCT FROM ('CANCELLED','CANCELLED')
          THEN RAISE EXCEPTION 'cancelled run terminal'; END IF;
          IF OLD.desired_state='DRAINING' AND NEW.desired_state='ACTIVE'
          THEN RAISE EXCEPTION 'drain intent irreversible'; END IF;
          IF OLD.lifecycle='DRAINED' AND NEW.lifecycle NOT IN('DRAINED','CANCELLED')
          THEN RAISE EXCEPTION 'drained run terminal'; END IF;
          IF (NEW.desired_state,NEW.lifecycle) NOT IN(
               ('ACTIVE','ACTIVE'),('DRAINING','DRAINING'),
               ('DRAINING','DRAINED'),('CANCELLED','CANCELLED'))
          THEN RAISE EXCEPTION 'illegal control lifecycle pair'; END IF;
          IF NEW.lifecycle IN('DRAINED','CANCELLED') AND NOT EXISTS(
               SELECT 1 FROM control_worker_fences_v1 f
               WHERE f.run_id=NEW.run_id AND f.reason=NEW.lifecycle
                 AND f.fence_epoch<=NEW.control_version)
          THEN RAISE EXCEPTION 'terminal lifecycle lacks exact worker fence'; END IF;
          IF NEW.lifecycle IN('DRAINED','CANCELLED')
             AND NEW.lifecycle IS DISTINCT FROM OLD.lifecycle AND NOT EXISTS(
               SELECT 1 FROM control_worker_fences_v1 f
               WHERE f.run_id=NEW.run_id AND f.fence_epoch=NEW.control_version
                 AND f.reason=NEW.lifecycle)
          THEN RAISE EXCEPTION 'terminal transition lacks current worker fence'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_worker_fence_insert()
        RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE run control_runs_v1%ROWTYPE;
          DECLARE pending boolean; BEGIN
          SELECT * INTO run FROM control_runs_v1 WHERE run_id=NEW.run_id;
          IF run.run_id IS NULL OR NEW.fence_epoch<>run.control_version+1
          THEN RAISE EXCEPTION 'worker fence epoch mismatch'; END IF;
          IF NEW.reason='DRAINED' AND
             (run.desired_state,run.lifecycle) IS DISTINCT FROM ('DRAINING','DRAINING')
          THEN RAISE EXCEPTION 'drain fence lacks draining intent'; END IF;
          IF NEW.reason='CANCELLED' AND run.lifecycle NOT IN('ACTIVE','DRAINING')
          THEN RAISE EXCEPTION 'cancel fence lacks cancellable run'; END IF;
          IF EXISTS(SELECT 1 FROM control_run_consumers_v1 b JOIN consumers c
               USING(consumer_id) WHERE b.run_id=NEW.run_id AND
               (c.owner_id IS NOT NULL OR c.in_flight_sequence IS NOT NULL))
             OR EXISTS(SELECT 1 FROM control_run_consumers_v1 b
               JOIN runtime_invocations v USING(consumer_id)
               WHERE b.run_id=NEW.run_id)
          THEN RAISE EXCEPTION 'worker fence leaves legacy execution authority'; END IF;
          IF to_regclass('join_invocations_v3') IS NOT NULL THEN
            EXECUTE 'SELECT EXISTS(SELECT 1 FROM join_invocations_v3 v '
              'WHERE v.revision_id=$1)' INTO pending USING run.revision_id;
            IF pending THEN RAISE EXCEPTION 'worker fence leaves join invocation'; END IF;
          END IF;
          IF NEW.reason='DRAINED' THEN
            IF EXISTS(SELECT 1 FROM control_source_admissions_v1 a
                 WHERE a.run_id=NEW.run_id
                   AND a.admission_ordinal<=run.drain_admission_cut
                   AND a.status<>'PUBLISHED')
               OR EXISTS(SELECT 1 FROM control_run_consumers_v1 b JOIN consumers c
                 USING(consumer_id) JOIN partitions p ON p.channel=c.channel
                 AND p.partition_id=c.partition_id WHERE b.run_id=NEW.run_id
                 AND c.completed_through<>p.next_sequence-1)
            THEN RAISE EXCEPTION 'drain fence has pending source or consumer work'; END IF;
            IF EXISTS(SELECT 1 FROM checkpoint_intents_v2 i
                 JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                 WHERE i.revision_id=run.revision_id
                   AND l.status NOT IN('COMPLETED','ABORTED','RETIRED'))
            THEN RAISE EXCEPTION 'drain fence has pending checkpoint'; END IF;
            IF to_regclass('join_workers') IS NOT NULL THEN
              EXECUTE 'SELECT EXISTS(SELECT 1 FROM join_inputs_v3 i '
                'JOIN partitions p USING(channel,partition_id) '
                'JOIN partition_log_heads h USING(channel,partition_id) '
                'WHERE i.revision_id=$1 AND (i.completed<>p.next_sequence-1 '
                'OR i.scanned<>h.next_log_sequence-1 '
                'OR i.blocked_checkpoint IS NOT NULL)) OR EXISTS('
                'SELECT 1 FROM join_workers w WHERE w.revision_id=$1 '
                'AND w.phase<>''LIVE'')' INTO pending USING run.revision_id;
              IF pending THEN RAISE EXCEPTION 'drain fence has pending join work'; END IF;
            END IF;
            IF to_regclass('region_recovery_plans_v1') IS NOT NULL THEN
              EXECUTE 'SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 p '
                'WHERE p.revision_id=$1 AND p.status=''RESTORING'' '
                'AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r '
                'WHERE r.plan_id=p.recovery_id))' INTO pending USING run.revision_id;
            ELSE
              SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 p
                WHERE p.revision_id=run.revision_id AND p.status='RESTORING')
                INTO pending;
            END IF;
            IF pending THEN RAISE EXCEPTION 'drain fence has active recovery'; END IF;
          END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_admission_update() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF (NEW.run_id,NEW.admission_ordinal,NEW.admission_id,NEW.request_id,
              NEW.channel,NEW.partition_id,NEW.record_id,NEW.attempt_id,NEW.object_key,
              NEW.payload_digest,NEW.producer_id,NEW.producer_sequence,NEW.payload_size,
              NEW.schema_id,NEW.lineage_digest,NEW.created_at)
             IS DISTINCT FROM
             (OLD.run_id,OLD.admission_ordinal,OLD.admission_id,OLD.request_id,
              OLD.channel,OLD.partition_id,OLD.record_id,OLD.attempt_id,OLD.object_key,
              OLD.payload_digest,OLD.producer_id,OLD.producer_sequence,OLD.payload_size,
              OLD.schema_id,OLD.lineage_digest,OLD.created_at)
          THEN RAISE EXCEPTION 'admission identity immutable'; END IF;
          IF OLD.status='PUBLISHED' OR (OLD.status,NEW.status) IS DISTINCT FROM
             ('ADMITTED','PUBLISHED') OR NEW.sequence IS NULL OR NEW.published_at IS NULL
          THEN RAISE EXCEPTION 'illegal admission transition'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_join_input_lags(
          selected_run text,selected_channel text,selected_partition text,
          selected_sequence bigint) RETURNS boolean LANGUAGE plpgsql AS $$
        DECLARE lag boolean; BEGIN
          IF to_regclass('join_inputs_v3') IS NULL THEN RETURN false; END IF;
          EXECUTE 'SELECT EXISTS(SELECT 1 FROM control_runs_v1 run '
            'JOIN join_inputs_v3 i ON i.revision_id=run.revision_id '
            'WHERE run.run_id=$1 AND i.channel=$2 AND i.partition_id=$3 '
            'AND i.completed<$4)' INTO lag USING selected_run,selected_channel,
            selected_partition,selected_sequence;
          RETURN lag;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_admission_delete() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF current_setting('ariadne.control_retire',true) IS DISTINCT FROM 'on'
             OR OLD.status<>'PUBLISHED'
             OR NOT EXISTS(SELECT 1 FROM metadata_summaries s
               WHERE s.channel=OLD.channel AND s.partition_id=OLD.partition_id
                 AND s.retired_through>=OLD.sequence)
             OR EXISTS(SELECT 1 FROM control_run_consumers_v1 b JOIN consumers c
               USING(consumer_id) WHERE b.run_id=OLD.run_id
                 AND b.channel=OLD.channel AND b.partition_id=OLD.partition_id
                 AND c.completed_through<OLD.sequence)
             OR control_join_input_lags(
               OLD.run_id,OLD.channel,OLD.partition_id,OLD.sequence)
          THEN RAISE EXCEPTION 'admission retirement outside data horizon'; END IF;
          RETURN OLD;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_admission_producer_update()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF (NEW.run_id,NEW.channel,NEW.partition_id,NEW.producer_id)
             IS DISTINCT FROM (OLD.run_id,OLD.channel,OLD.partition_id,OLD.producer_id)
          THEN RAISE EXCEPTION 'admission producer identity immutable'; END IF;
          IF NEW.accepted_through=OLD.accepted_through+1 AND
             (NEW.retired_through,NEW.retired_records,NEW.retired_bytes,NEW.rolling_digest)
             IS NOT DISTINCT FROM
             (OLD.retired_through,OLD.retired_records,OLD.retired_bytes,OLD.rolling_digest)
          THEN RETURN NEW; END IF;
          IF NEW.accepted_through=OLD.accepted_through AND
             NEW.retired_through>OLD.retired_through AND
             NEW.retired_through<=NEW.accepted_through AND
             NEW.retired_records>OLD.retired_records AND
             NEW.retired_bytes>=OLD.retired_bytes AND
             NEW.rolling_digest IS DISTINCT FROM OLD.rolling_digest
          THEN RETURN NEW; END IF;
          RAISE EXCEPTION 'illegal admission producer transition';
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_source_record() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE own control_source_ownership_v1%ROWTYPE;
          DECLARE run control_runs_v1%ROWTYPE; BEGIN
          SELECT * INTO own FROM control_source_ownership_v1
            WHERE channel=NEW.channel AND partition_id=NEW.partition_id;
          IF own.run_id IS NULL THEN RETURN NEW; END IF;
          SELECT * INTO run FROM control_runs_v1 WHERE run_id=own.run_id FOR UPDATE;
          IF NOT EXISTS(SELECT 1 FROM control_source_admissions_v1 a
              JOIN append_attempts x ON x.channel=a.channel
                AND x.partition_id=a.partition_id AND x.record_id=a.record_id
                AND x.producer_id=a.producer_id
                AND x.producer_sequence=a.producer_sequence
                AND x.attempt_id=a.attempt_id AND x.object_key=a.object_key
                AND x.payload_digest=a.payload_digest AND x.payload_size=a.payload_size
                AND x.schema_id=a.schema_id AND x.lineage_digest=a.lineage_digest
              WHERE a.run_id=run.run_id AND a.channel=NEW.channel
                AND a.partition_id=NEW.partition_id AND a.record_id=NEW.record_id
                AND a.status='ADMITTED' AND
                (run.desired_state='ACTIVE' OR
                 (run.desired_state='DRAINING'
                  AND a.admission_ordinal<=run.drain_admission_cut)))
          THEN RAISE EXCEPTION 'source record lacks open durable admission'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_publish_source_record() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE changed bigint; BEGIN
          UPDATE control_source_admissions_v1 a SET status='PUBLISHED',
            sequence=NEW.sequence,published_at=clock_timestamp()
          FROM append_attempts x WHERE a.channel=NEW.channel
            AND a.partition_id=NEW.partition_id AND a.record_id=NEW.record_id
            AND a.status='ADMITTED' AND x.attempt_id=a.attempt_id
            AND x.channel=NEW.channel AND x.partition_id=NEW.partition_id
            AND x.record_id=NEW.record_id AND x.producer_id=a.producer_id
            AND x.producer_sequence=a.producer_sequence
            AND x.object_key=a.object_key AND x.payload_digest=a.payload_digest
            AND x.payload_size=a.payload_size AND x.schema_id=a.schema_id
            AND x.lineage_digest=a.lineage_digest;
          GET DIAGNOSTICS changed=ROW_COUNT;
          IF EXISTS(SELECT 1 FROM control_source_ownership_v1
              WHERE channel=NEW.channel AND partition_id=NEW.partition_id)
             AND changed<>1
          THEN RAISE EXCEPTION 'source append did not publish exact admission'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_validate_published_admission()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.status='PUBLISHED' AND NOT EXISTS(
            SELECT 1 FROM append_attempts x JOIN records r
              ON r.channel=x.channel AND r.partition_id=x.partition_id
             AND r.record_id=x.record_id AND r.sequence=NEW.sequence
            WHERE x.attempt_id=NEW.attempt_id AND x.status='APPENDED'
              AND x.channel=NEW.channel AND x.partition_id=NEW.partition_id
              AND x.record_id=NEW.record_id AND x.object_key=NEW.object_key
              AND x.payload_digest=NEW.payload_digest AND x.payload_size=NEW.payload_size
              AND x.schema_id=NEW.schema_id AND x.lineage_digest=NEW.lineage_digest
              AND x.producer_id=NEW.producer_id
              AND x.producer_sequence=NEW.producer_sequence)
          THEN RAISE EXCEPTION 'published admission lacks committed append witness'; END IF;
          RETURN NULL;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_consumer_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_run_consumers_v1 b
               WHERE b.consumer_id=NEW.consumer_id)
             AND (NEW.channel,NEW.partition_id,NEW.start_sequence) IS DISTINCT FROM
                 (OLD.channel,OLD.partition_id,OLD.start_sequence)
          THEN RAISE EXCEPTION 'controlled consumer identity immutable'; END IF;
          IF NEW IS DISTINCT FROM OLD AND EXISTS(
               SELECT 1 FROM control_run_consumers_v1 b
               JOIN control_worker_fences_v1 f USING(run_id)
               WHERE b.consumer_id=NEW.consumer_id)
             AND ((to_jsonb(NEW)-'completion_retired_through')
                    IS DISTINCT FROM
                  (to_jsonb(OLD)-'completion_retired_through')
                  OR NEW.completion_retired_through<OLD.completion_retired_through
                  OR NEW.completion_retired_through>NEW.completed_through)
          THEN RAISE EXCEPTION 'control worker fence rejects consumer mutation'; END IF;
          IF NEW.owner_id IS NOT NULL AND (NEW.owner_id,NEW.owner_epoch)
             IS DISTINCT FROM (OLD.owner_id,OLD.owner_epoch) AND EXISTS(
               SELECT 1 FROM control_run_consumers_v1 b JOIN control_runs_v1 r USING(run_id)
               WHERE b.consumer_id=NEW.consumer_id AND
                 (r.desired_state='CANCELLED' OR r.lifecycle='DRAINED'))
          THEN RAISE EXCEPTION 'control run rejects worker claim'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_consumer_registration()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          PERFORM pg_advisory_xact_lock(hashtextextended(
            current_schema()||'|control-consumer|'||NEW.channel||'|'||NEW.partition_id,0));
          IF EXISTS(SELECT 1 FROM control_partition_ownership_v1 o
              WHERE o.channel=NEW.channel AND o.partition_id=NEW.partition_id)
          THEN RAISE EXCEPTION 'controlled partition consumer manifest is frozen'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_validate_consumer_registration()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_partition_ownership_v1 o
              WHERE o.channel=NEW.channel AND o.partition_id=NEW.partition_id)
          THEN RAISE EXCEPTION 'controlled partition consumer manifest is frozen'; END IF;
          RETURN NULL;
        END $$;
        CREATE OR REPLACE FUNCTION control_validate_terminal_consumer_maintenance()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          IF NEW.completion_retired_through IS DISTINCT FROM OLD.completion_retired_through
             AND EXISTS(SELECT 1 FROM control_run_consumers_v1 b
               JOIN control_worker_fences_v1 f USING(run_id)
               WHERE b.consumer_id=NEW.consumer_id)
             AND NOT EXISTS(SELECT 1 FROM metadata_summaries s
               WHERE s.channel=NEW.channel AND s.partition_id=NEW.partition_id
                 AND s.retired_through>=NEW.completion_retired_through)
          THEN RAISE EXCEPTION 'terminal consumer maintenance lacks metadata horizon'; END IF;
          RETURN NULL;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_join_claim() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF NEW IS DISTINCT FROM OLD AND EXISTS(
               SELECT 1 FROM control_runs_v1 r
               JOIN control_worker_fences_v1 f USING(run_id)
               WHERE r.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION 'control worker fence rejects join mutation'; END IF;
          IF NEW.owner_id IS NOT NULL AND (NEW.owner_id,NEW.owner_epoch)
             IS DISTINCT FROM (OLD.owner_id,OLD.owner_epoch) AND EXISTS(
               SELECT 1 FROM control_runs_v1 r WHERE r.revision_id=NEW.revision_id
                 AND (r.desired_state='CANCELLED' OR r.lifecycle='DRAINED'))
          THEN RAISE EXCEPTION 'control run rejects join claim'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_consumer_terminal() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_run_consumers_v1 b
            JOIN control_worker_fences_v1 f USING(run_id)
            WHERE b.consumer_id=NEW.consumer_id)
          THEN RAISE EXCEPTION 'control worker fence rejects terminal write'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_join_runtime() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
            JOIN control_worker_fences_v1 f USING(run_id)
            WHERE r.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION 'control worker fence rejects join write'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_region_plan() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
              JOIN control_worker_fences_v1 f USING(run_id)
              WHERE r.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION 'control worker fence rejects regional recovery'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_region_runtime() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE selected_plan text;
          DECLARE selected_revision text; BEGIN
          selected_plan:=CASE WHEN TG_OP='DELETE' THEN OLD.plan_id ELSE NEW.plan_id END;
          SELECT revision_id INTO selected_revision FROM region_recovery_plans_v1
            WHERE plan_id=selected_plan;
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
              JOIN control_worker_fences_v1 f USING(run_id)
              WHERE r.revision_id=selected_revision)
          THEN RAISE EXCEPTION 'control worker fence rejects regional recovery'; END IF;
          IF TG_OP='DELETE' THEN RETURN OLD; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_join_recovery() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
              JOIN control_worker_fences_v1 f USING(run_id)
              WHERE r.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION 'control worker fence rejects join recovery'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_join_manifest_registration()
        RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE selected_revision text; BEGIN
          selected_revision:=CASE WHEN TG_OP='DELETE'
            THEN OLD.revision_id ELSE NEW.revision_id END;
          IF EXISTS(SELECT 1 FROM control_runs_v1
              WHERE revision_id=selected_revision)
          THEN RAISE EXCEPTION 'active control run freezes native graph manifest'; END IF;
          IF TG_OP='DELETE' THEN RETURN OLD; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_global_recovery() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
              JOIN control_worker_fences_v1 f USING(run_id)
              WHERE r.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION 'control worker fence rejects global recovery'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_checkpoint_start() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM control_runs_v1 r
              WHERE r.revision_id=NEW.revision_id
                AND r.desired_state<>'ACTIVE')
          THEN RAISE EXCEPTION 'control lifecycle rejects checkpoint start'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION control_guard_source_barrier() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(SELECT 1 FROM checkpoint_intents_v2 i
              JOIN control_runs_v1 r USING(revision_id)
              JOIN control_worker_fences_v1 f USING(run_id)
              WHERE i.checkpoint_id=NEW.checkpoint_id)
          THEN RAISE EXCEPTION 'control worker fence rejects source barrier'; END IF;
          RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS control_deployment_immutable ON control_deployments_v1;
        CREATE TRIGGER control_deployment_immutable BEFORE UPDATE OR DELETE ON control_deployments_v1
          FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_partition_ownership_immutable
          ON control_partition_ownership_v1;
        CREATE TRIGGER control_partition_ownership_immutable BEFORE UPDATE OR DELETE
          ON control_partition_ownership_v1 FOR EACH ROW
          EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_run_guard ON control_runs_v1;
        CREATE TRIGGER control_run_guard BEFORE UPDATE ON control_runs_v1
          FOR EACH ROW EXECUTE FUNCTION control_guard_run_update();
        DROP TRIGGER IF EXISTS control_run_delete_guard ON control_runs_v1;
        CREATE TRIGGER control_run_delete_guard BEFORE DELETE ON control_runs_v1
          FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_run_consumer_immutable ON control_run_consumers_v1;
        CREATE TRIGGER control_run_consumer_immutable BEFORE UPDATE OR DELETE ON control_run_consumers_v1
          FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_source_ownership_immutable ON control_source_ownership_v1;
        CREATE TRIGGER control_source_ownership_immutable BEFORE UPDATE OR DELETE ON control_source_ownership_v1
          FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_admission_guard ON control_source_admissions_v1;
        CREATE TRIGGER control_admission_guard BEFORE UPDATE ON control_source_admissions_v1
          FOR EACH ROW EXECUTE FUNCTION control_guard_admission_update();
        DROP TRIGGER IF EXISTS control_admission_delete_guard ON control_source_admissions_v1;
        CREATE TRIGGER control_admission_delete_guard BEFORE DELETE ON control_source_admissions_v1
          FOR EACH ROW EXECUTE FUNCTION control_guard_admission_delete();
        DROP TRIGGER IF EXISTS control_admission_producer_delete_guard
          ON control_admission_producers_v1;
        CREATE TRIGGER control_admission_producer_delete_guard BEFORE DELETE
          ON control_admission_producers_v1 FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_admission_producer_update_guard
          ON control_admission_producers_v1;
        CREATE TRIGGER control_admission_producer_update_guard BEFORE UPDATE
          ON control_admission_producers_v1 FOR EACH ROW
          EXECUTE FUNCTION control_guard_admission_producer_update();
        DROP TRIGGER IF EXISTS control_command_immutable ON control_commands_v1;
        CREATE TRIGGER control_command_immutable BEFORE UPDATE OR DELETE ON control_commands_v1
          FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_worker_fence_immutable ON control_worker_fences_v1;
        DROP TRIGGER IF EXISTS control_worker_fence_insert_guard ON control_worker_fences_v1;
        CREATE TRIGGER control_worker_fence_insert_guard BEFORE INSERT
          ON control_worker_fences_v1 FOR EACH ROW
          EXECUTE FUNCTION control_guard_worker_fence_insert();
        CREATE TRIGGER control_worker_fence_immutable BEFORE UPDATE OR DELETE
          ON control_worker_fences_v1 FOR EACH ROW EXECUTE FUNCTION control_reject_mutation();
        DROP TRIGGER IF EXISTS control_source_record_guard ON records;
        CREATE TRIGGER control_source_record_guard BEFORE INSERT ON records
          FOR EACH ROW EXECUTE FUNCTION control_guard_source_record();
        DROP TRIGGER IF EXISTS control_source_record_publish ON records;
        CREATE TRIGGER control_source_record_publish AFTER INSERT ON records
          FOR EACH ROW EXECUTE FUNCTION control_publish_source_record();
        DROP TRIGGER IF EXISTS control_admission_publish_converged
          ON control_source_admissions_v1;
        CREATE CONSTRAINT TRIGGER control_admission_publish_converged
          AFTER UPDATE ON control_source_admissions_v1 DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION control_validate_published_admission();
        DROP TRIGGER IF EXISTS control_consumer_claim_guard ON consumers;
        CREATE TRIGGER control_consumer_claim_guard BEFORE UPDATE ON consumers
          FOR EACH ROW EXECUTE FUNCTION control_guard_consumer_claim();
        DROP TRIGGER IF EXISTS control_consumer_registration_guard ON consumers;
        CREATE TRIGGER control_consumer_registration_guard BEFORE INSERT ON consumers
          FOR EACH ROW EXECUTE FUNCTION control_guard_consumer_registration();
        DROP TRIGGER IF EXISTS control_consumer_registration_converged ON consumers;
        CREATE CONSTRAINT TRIGGER control_consumer_registration_converged
          AFTER INSERT ON consumers DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION control_validate_consumer_registration();
        DROP TRIGGER IF EXISTS control_terminal_consumer_maintenance ON consumers;
        CREATE CONSTRAINT TRIGGER control_terminal_consumer_maintenance
          AFTER UPDATE ON consumers DEFERRABLE INITIALLY DEFERRED
          FOR EACH ROW EXECUTE FUNCTION control_validate_terminal_consumer_maintenance();
        DROP TRIGGER IF EXISTS control_consumer_terminal_guard ON turn_completions;
        CREATE TRIGGER control_consumer_terminal_guard BEFORE INSERT ON turn_completions
          FOR EACH ROW EXECUTE FUNCTION control_guard_consumer_terminal();
        DROP TRIGGER IF EXISTS control_global_recovery_guard ON recovery_plans_v2;
        CREATE TRIGGER control_global_recovery_guard BEFORE INSERT OR UPDATE
          ON recovery_plans_v2 FOR EACH ROW
          EXECUTE FUNCTION control_guard_global_recovery();
        DROP TRIGGER IF EXISTS control_checkpoint_start_guard ON checkpoint_intents_v2;
        CREATE TRIGGER control_checkpoint_start_guard BEFORE INSERT ON checkpoint_intents_v2
          FOR EACH ROW EXECUTE FUNCTION control_guard_checkpoint_start();
        DROP TRIGGER IF EXISTS control_source_barrier_guard ON source_barrier_observations;
        CREATE TRIGGER control_source_barrier_guard BEFORE INSERT
          ON source_barrier_observations FOR EACH ROW
          EXECUTE FUNCTION control_guard_source_barrier();
        """
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)
            if await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL"):
                await conn.execute(
                    "DROP TRIGGER IF EXISTS control_join_claim_guard ON join_workers;"
                    "CREATE TRIGGER control_join_claim_guard BEFORE UPDATE ON join_workers "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_claim();"
                    "DROP TRIGGER IF EXISTS control_join_manifest_worker_guard ON join_workers;"
                    "CREATE TRIGGER control_join_manifest_worker_guard BEFORE INSERT OR DELETE "
                    "ON join_workers FOR EACH ROW "
                    "EXECUTE FUNCTION control_guard_join_manifest_registration();"
                    "DROP TRIGGER IF EXISTS control_join_manifest_input_guard ON join_inputs_v3;"
                    "CREATE TRIGGER control_join_manifest_input_guard BEFORE INSERT OR DELETE "
                    "ON join_inputs_v3 FOR EACH ROW "
                    "EXECUTE FUNCTION control_guard_join_manifest_registration();"
                    "DROP TRIGGER IF EXISTS control_join_receipt_guard ON join_receipts_v3;"
                    "CREATE TRIGGER control_join_receipt_guard BEFORE INSERT ON join_receipts_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_runtime();"
                    "DROP TRIGGER IF EXISTS control_join_turn_guard ON join_turns_v3;"
                    "CREATE TRIGGER control_join_turn_guard BEFORE INSERT ON join_turns_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_runtime();"
                    "DROP TRIGGER IF EXISTS control_join_barrier_guard ON join_barriers_v3;"
                    "CREATE TRIGGER control_join_barrier_guard BEFORE INSERT ON join_barriers_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_runtime();"
                    "DROP TRIGGER IF EXISTS control_join_alignment_guard ON join_alignments_v3;"
                    "CREATE TRIGGER control_join_alignment_guard BEFORE INSERT ON join_alignments_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_runtime();"
                    "DROP TRIGGER IF EXISTS control_join_invocation_guard ON join_invocations_v3;"
                    "CREATE TRIGGER control_join_invocation_guard BEFORE INSERT ON join_invocations_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_runtime();"
                    "DROP TRIGGER IF EXISTS control_join_recovery_guard ON join_recoveries_v3;"
                    "CREATE TRIGGER control_join_recovery_guard BEFORE INSERT OR UPDATE ON join_recoveries_v3 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_join_recovery();"
                )
            if await conn.fetchval(
                "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
            ):
                await conn.execute(
                    "DROP TRIGGER IF EXISTS control_region_plan_guard ON region_recovery_plans_v1;"
                    "CREATE TRIGGER control_region_plan_guard BEFORE INSERT ON region_recovery_plans_v1 "
                    "FOR EACH ROW EXECUTE FUNCTION control_guard_region_plan();"
                )
            if await conn.fetchval(
                "SELECT to_regclass('regional_recovery_workers_v1') IS NOT NULL"
            ):
                await conn.execute(
                    "DROP TRIGGER IF EXISTS control_regional_worker_guard ON regional_recovery_workers_v1;"
                    "CREATE TRIGGER control_regional_worker_guard BEFORE INSERT OR UPDATE "
                    "ON regional_recovery_workers_v1 FOR EACH ROW "
                    "EXECUTE FUNCTION control_guard_region_runtime();"
                    "DROP TRIGGER IF EXISTS control_regional_attempt_guard ON regional_recovery_attempts_v1;"
                    "CREATE TRIGGER control_regional_attempt_guard BEFORE INSERT "
                    "ON regional_recovery_attempts_v1 FOR EACH ROW "
                    "EXECUTE FUNCTION control_guard_region_runtime();"
                    "DROP TRIGGER IF EXISTS control_regional_attempt_worker_guard "
                    "ON regional_recovery_attempt_workers_v1;"
                    "CREATE TRIGGER control_regional_attempt_worker_guard BEFORE INSERT "
                    "ON regional_recovery_attempt_workers_v1 FOR EACH ROW "
                    "EXECUTE FUNCTION control_guard_region_runtime();"
                )

    async def provision_tenant(
        self, tenant_id: str, token: str, capabilities: set[str]
    ) -> None:
        known = {"deploy", "run", "read", "control", "source"}
        if not tenant_id or not capabilities or not capabilities <= known:
            raise ValueError("tenant and known capabilities required")
        caps = sorted(capabilities)
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO control_tenants_v1 VALUES($1) ON CONFLICT DO NOTHING",
                tenant_id,
            )
            prior = await conn.fetchrow(
                "SELECT tenant_id,capabilities,disabled FROM control_capabilities_v1 "
                "WHERE token_digest=$1 FOR UPDATE",
                token_digest(token),
            )
            if prior is None:
                await conn.execute(
                    "INSERT INTO control_capabilities_v1("
                    "token_digest,tenant_id,capabilities,disabled) "
                    "VALUES($1,$2,$3::jsonb,false)",
                    token_digest(token),
                    tenant_id,
                    canonical(caps),
                )
            elif (
                prior["tenant_id"],
                decoded(prior["capabilities"]),
                prior["disabled"],
            ) != (
                tenant_id,
                caps,
                False,
            ):
                raise ControlConflict("capability token identity drift")

    async def _authorize(self, token: str, capability: str) -> str:
        async with self.pool.acquire() as conn, conn.transaction(readonly=True):
            row = await conn.fetchrow(
                "SELECT tenant_id,capabilities,disabled,"
                "(expires_at IS NULL OR expires_at>clock_timestamp()) AS live "
                "FROM control_capabilities_v1 "
                "WHERE token_digest=$1",
                token_digest(token),
            )
        if row is None or row["disabled"] or not row["live"]:
            raise AuthenticationFailed("invalid bearer token")
        if capability not in decoded(row["capabilities"]):
            raise AuthorizationFailed(f"missing {capability} capability")
        return row["tenant_id"]

    @staticmethod
    async def _assert_authorized(
        conn: asyncpg.Connection,
        token: str,
        tenant_id: str,
        capability: str,
        *,
        lock: bool = True,
    ) -> None:
        lock_clause = " FOR KEY SHARE" if lock else ""
        row = await conn.fetchrow(
            "SELECT tenant_id,capabilities,disabled,"
            "(expires_at IS NULL OR expires_at>clock_timestamp()) AS live "
            "FROM control_capabilities_v1 WHERE token_digest=$1" + lock_clause,
            token_digest(token),
        )
        if row is None or row["disabled"] or not row["live"]:
            raise AuthenticationFailed("invalid bearer token")
        if row["tenant_id"] != tenant_id or capability not in decoded(
            row["capabilities"]
        ):
            raise AuthorizationFailed(f"missing {capability} capability")

    async def revoke_token(self, token: str) -> None:
        """Trusted bootstrap-side revocation used by the prototype gate."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE control_capabilities_v1 SET disabled=true WHERE token_digest=$1",
                token_digest(token),
            )

    async def expire_token(self, token: str) -> None:
        """Trusted bootstrap-side expiry used by the prototype gate."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE control_capabilities_v1 SET "
                "expires_at=clock_timestamp()-interval '1 second' "
                "WHERE token_digest=$1",
                token_digest(token),
            )

    @staticmethod
    async def _tenant_row(
        conn: asyncpg.Connection, table: str, key: str, value: str, tenant_id: str
    ) -> asyncpg.Record:
        if (table, key) not in {
            ("control_deployments_v1", "deployment_id"),
            ("control_runs_v1", "run_id"),
        }:
            raise AssertionError("unsafe tenant lookup")
        row = await conn.fetchrow(f"SELECT * FROM {table} WHERE {key}=$1", value)
        if row is None or row["tenant_id"] != tenant_id:
            raise ControlNotFound("resource is not visible to tenant")
        return row

    @staticmethod
    async def _lock_revision(conn: asyncpg.Connection, revision_id: str) -> None:
        if (
            await conn.fetchrow(
                "SELECT revision_id FROM deployment_revisions WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            is None
        ):
            raise ControlNotFound("revision not found")

    @staticmethod
    async def _validate_native_runtime(
        conn: asyncpg.Connection,
        revision_id: str,
        participants: list[dict[str, Any]],
    ) -> bool:
        if not await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL"):
            return False
        workers = list(
            await conn.fetch(
                "SELECT participant_id,config,config_digest FROM join_workers "
                "WHERE revision_id=$1 ORDER BY participant_id FOR SHARE",
                revision_id,
            )
        )
        if not workers:
            return False
        expected_by_id = {item["participant_id"]: item for item in participants}
        if [row["participant_id"] for row in workers] != sorted(expected_by_id):
            raise ControlConflict("native participant manifest is incomplete")
        for worker in workers:
            config = decoded(worker["config"])
            if (
                digest(config) != worker["config_digest"]
                or config.get("spec") != expected_by_id[worker["participant_id"]]
            ):
                raise ControlConflict("native participant configuration drift")
        expected_inputs = sorted(
            (
                item["participant_id"],
                source["channel"],
                source["partition"],
            )
            for item in participants
            for source in item["inputs"]
        )
        actual_inputs = [
            (row["participant_id"], row["channel"], row["partition_id"])
            for row in await conn.fetch(
                "SELECT participant_id,channel,partition_id FROM join_inputs_v3 "
                "WHERE revision_id=$1 ORDER BY participant_id,channel,partition_id "
                "FOR SHARE",
                revision_id,
            )
        ]
        if actual_inputs != expected_inputs:
            raise ControlConflict("native input manifest is incomplete")
        required_tables = (
            "region_graph_seals_v1",
            "region_recovery_plans_v1",
            "regional_topology_seals_v1",
            "regional_recovery_workers_v1",
            "regional_recovery_attempts_v1",
            "regional_recovery_attempt_workers_v1",
        )
        if not all(
            [
                await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table)
                for table in required_tables
            ]
        ):
            raise ControlConflict("native runtime bootstrap is incomplete")
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM region_graph_seals_v1 g "
            "JOIN regional_topology_seals_v1 t USING(revision_id) "
            "WHERE g.revision_id=$1)",
            revision_id,
        ):
            raise ControlConflict("native regional topology is not sealed")
        required_triggers = {
            ("join_workers", "control_join_claim_guard"),
            ("join_workers", "control_join_manifest_worker_guard"),
            ("join_inputs_v3", "control_join_manifest_input_guard"),
            ("join_receipts_v3", "control_join_receipt_guard"),
            ("join_turns_v3", "control_join_turn_guard"),
            ("join_barriers_v3", "control_join_barrier_guard"),
            ("join_alignments_v3", "control_join_alignment_guard"),
            ("join_invocations_v3", "control_join_invocation_guard"),
            ("join_recoveries_v3", "control_join_recovery_guard"),
            ("region_recovery_plans_v1", "control_region_plan_guard"),
            ("regional_recovery_workers_v1", "control_regional_worker_guard"),
            ("regional_recovery_attempts_v1", "control_regional_attempt_guard"),
            (
                "regional_recovery_attempt_workers_v1",
                "control_regional_attempt_worker_guard",
            ),
        }
        installed_triggers = {
            (row["relname"], row["tgname"])
            for row in await conn.fetch(
                "SELECT c.relname,t.tgname FROM pg_trigger t "
                "JOIN pg_class c ON c.oid=t.tgrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=current_schema() AND NOT t.tgisinternal "
                "AND t.tgenabled<>'D'"
            )
        }
        if not required_triggers <= installed_triggers:
            raise ControlConflict("native control storage guards are incomplete")
        return True

    async def register_deployment(
        self,
        token: str,
        *,
        deployment_id: str,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "deploy")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "deploy")
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1 FOR SHARE",
                revision_id,
            )
            if revision is None or (
                revision["deployment_id"],
                revision["graph_version"],
                revision["graph_digest"],
            ) != (deployment_id, graph_version, graph_digest):
                raise ControlConflict("deployment does not match revision authority")
            roles = {
                (item["channel"], item["partition"]): "SOURCE"
                for item in decoded(revision["source_manifest"])
            }
            for participant in decoded(revision["participant_manifest"]):
                for item in participant["outputs"]:
                    roles[(item["channel"], item["partition"])] = "OUTPUT"
            for channel, partition in sorted(roles):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"{self.schema}|control-consumer|{channel}|{partition}",
                )
            inserted = await conn.execute(
                "INSERT INTO control_deployments_v1 VALUES($1,$2,$3,$4,$5,clock_timestamp()) "
                "ON CONFLICT DO NOTHING",
                deployment_id,
                tenant,
                revision_id,
                graph_version,
                graph_digest,
            )
            if inserted == "INSERT 0 1":
                try:
                    await conn.executemany(
                        "INSERT INTO control_partition_ownership_v1 "
                        "VALUES($1,$2,$3,$4,$5,$6)",
                        [
                            (
                                deployment_id,
                                tenant,
                                revision_id,
                                channel,
                                partition,
                                role,
                            )
                            for (channel, partition), role in sorted(roles.items())
                        ],
                    )
                except asyncpg.UniqueViolationError as exc:
                    raise ControlConflict(
                        "revision partition is owned by another deployment"
                    ) from exc
            row = await conn.fetchrow(
                "SELECT * FROM control_deployments_v1 WHERE deployment_id=$1",
                deployment_id,
            )
            if row is None or (
                row["tenant_id"],
                row["revision_id"],
                row["graph_version"],
                row["graph_digest"],
            ) != (tenant, revision_id, graph_version, graph_digest):
                raise ControlConflict("deployment identity drift")
            ownership = {
                (item["channel"], item["partition_id"]): item["partition_role"]
                for item in await conn.fetch(
                    "SELECT channel,partition_id,partition_role "
                    "FROM control_partition_ownership_v1 WHERE deployment_id=$1",
                    deployment_id,
                )
            }
            if ownership != roles:
                raise ControlConflict("deployment partition ownership drift")
        return {
            "tenant_id": tenant,
            "deployment_id": deployment_id,
            "revision_id": revision_id,
            "graph_version": graph_version,
            "graph_digest": graph_digest,
        }

    async def claim_leader(
        self,
        token: str,
        *,
        deployment_id: str,
        leader_id: str,
        lease_seconds: float,
    ) -> LeaderLease:
        tenant = await self._authorize(token, "control")
        if not leader_id or lease_seconds <= 0:
            raise ValueError("leader and positive lease required")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "control")
            await self._tenant_row(
                conn, "control_deployments_v1", "deployment_id", deployment_id, tenant
            )
            row = await conn.fetchrow(
                "SELECT *,lease_expires_at>clock_timestamp() AS live "
                "FROM control_leaders_v1 WHERE deployment_id=$1 FOR UPDATE",
                deployment_id,
            )
            if row is not None and row["live"] and row["leader_id"] != leader_id:
                raise StaleLeader("control leader lease held")
            if row is None:
                epoch = 1
                await conn.execute(
                    "INSERT INTO control_leaders_v1 VALUES("
                    "$1,$2,$3,clock_timestamp()+make_interval(secs=>$4))",
                    deployment_id,
                    leader_id,
                    epoch,
                    lease_seconds,
                )
            elif row["live"]:
                epoch = row["leader_epoch"]
                await conn.execute(
                    "UPDATE control_leaders_v1 SET lease_expires_at="
                    "clock_timestamp()+make_interval(secs=>$2) WHERE deployment_id=$1",
                    deployment_id,
                    lease_seconds,
                )
            else:
                epoch = row["leader_epoch"] + 1
                await conn.execute(
                    "UPDATE control_leaders_v1 SET leader_id=$2,leader_epoch=$3,"
                    "lease_expires_at=clock_timestamp()+make_interval(secs=>$4) "
                    "WHERE deployment_id=$1",
                    deployment_id,
                    leader_id,
                    epoch,
                    lease_seconds,
                )
        return LeaderLease(deployment_id, leader_id, epoch)

    @staticmethod
    async def _assert_leader(
        conn: asyncpg.Connection,
        deployment_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> None:
        lease = await conn.fetchrow(
            "SELECT leader_epoch FROM control_leaders_v1 WHERE deployment_id=$1 "
            "AND leader_id=$2 AND leader_epoch=$3 "
            "AND lease_expires_at>clock_timestamp() FOR UPDATE",
            deployment_id,
            leader_id,
            leader_epoch,
        )
        if lease is None:
            raise StaleLeader("control leader fenced")

    @staticmethod
    def _control(row: asyncpg.Record) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "tenant_id": row["tenant_id"],
            "deployment_id": row["deployment_id"],
            "revision_id": row["revision_id"],
            "execution_mode": row["execution_mode"],
            "desired_state": row["desired_state"],
            "lifecycle": row["lifecycle"],
            "drain_admission_cut": row["drain_admission_cut"],
            "control_version": row["control_version"],
        }

    async def create_run(
        self,
        token: str,
        *,
        deployment_id: str,
        request_id: str,
        consumer_ids: list[str],
        leader_id: str,
        leader_epoch: int,
        max_admission_records: int = 1024,
        max_admission_bytes: int = 64 * 1024 * 1024,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "run")
        if not request_id or len(set(consumer_ids)) != len(consumer_ids):
            raise ValueError("request and unique consumers required")
        if min(max_admission_records, max_admission_bytes) <= 0:
            raise ValueError("positive admission capacity required")
        run_id = "run:" + digest(
            {"tenant": tenant, "deployment": deployment_id, "request": request_id}
        )
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "run")
            deployment = await self._tenant_row(
                conn, "control_deployments_v1", "deployment_id", deployment_id, tenant
            )
            await self._lock_revision(conn, deployment["revision_id"])
            await self._assert_leader(conn, deployment_id, leader_id, leader_epoch)
            existing = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            if existing is not None:
                bound = [
                    x["consumer_id"]
                    for x in await conn.fetch(
                        "SELECT consumer_id FROM control_run_consumers_v1 "
                        "WHERE run_id=$1 ORDER BY consumer_id",
                        run_id,
                    )
                ]
                if bound != sorted(consumer_ids) or (
                    existing["max_admission_records"],
                    existing["max_admission_bytes"],
                ) != (max_admission_records, max_admission_bytes):
                    raise ControlConflict("run consumer manifest drift")
                return self._control(existing)
            revision = await conn.fetchrow(
                "SELECT source_manifest,participant_manifest "
                "FROM deployment_revisions WHERE revision_id=$1",
                deployment["revision_id"],
            )
            sources = {
                (item["channel"], item["partition"])
                for item in decoded(revision["source_manifest"])
            }
            participants = decoded(revision["participant_manifest"])
            source_channels = [item[0] for item in sorted(sources)]
            source_partitions = [item[1] for item in sorted(sources)]
            consumers = list(
                await conn.fetch(
                    "SELECT consumer_id,channel,partition_id FROM consumers "
                    "WHERE (channel,partition_id) IN (SELECT channel,partition_id "
                    "FROM unnest($1::text[],$2::text[]) AS source(channel,partition_id)) "
                    "ORDER BY consumer_id FOR SHARE",
                    source_channels,
                    source_partitions,
                )
            )
            if [x["consumer_id"] for x in consumers] != sorted(consumer_ids):
                raise ControlConflict(
                    "run must bind exact installed source consumer manifest"
                )
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM consumers c "
                "JOIN control_partition_ownership_v1 o "
                "ON o.channel=c.channel AND o.partition_id=c.partition_id "
                "WHERE o.deployment_id=$1 AND o.partition_role='OUTPUT')",
                deployment_id,
            ):
                raise ControlConflict(
                    "legacy consumers on revision outputs are unsupported"
                )
            if not consumers and not (
                await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL")
                and await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM join_workers WHERE revision_id=$1)",
                    deployment["revision_id"],
                )
            ):
                raise ControlConflict("run has no installed data-plane worker")
            if any((x["channel"], x["partition_id"]) not in sources for x in consumers):
                raise ControlConflict("run consumer must bind declared source")
            native = await self._validate_native_runtime(
                conn, deployment["revision_id"], participants
            )
            if native and consumers:
                raise ControlConflict(
                    "native run does not support legacy consumer bindings"
                )
            if not native and not consumers:
                raise ControlConflict("legacy run has no installed source consumer")
            if not native and any(
                (source["channel"], source["partition"]) not in sources
                for participant in participants
                for source in participant["inputs"]
            ):
                raise ControlConflict(
                    "legacy run does not support routed participant inputs"
                )
            execution_mode = "NATIVE_JOIN" if native else "LEGACY_SOURCE"
            try:
                await conn.execute(
                    "INSERT INTO control_runs_v1("
                    "run_id,request_id,tenant_id,deployment_id,revision_id,execution_mode,"
                    "desired_state,lifecycle,max_admission_records,max_admission_bytes) "
                    "VALUES($1,$2,$3,$4,$5,$6,'ACTIVE','ACTIVE',$7,$8)",
                    run_id,
                    request_id,
                    tenant,
                    deployment_id,
                    deployment["revision_id"],
                    execution_mode,
                    max_admission_records,
                    max_admission_bytes,
                )
            except asyncpg.UniqueViolationError as exc:
                raise ControlConflict("revision already owned by another run") from exc
            await conn.executemany(
                "INSERT INTO control_run_consumers_v1 VALUES($1,$2,$3,$4)",
                [
                    (run_id, x["consumer_id"], x["channel"], x["partition_id"])
                    for x in consumers
                ],
            )
            await conn.executemany(
                "INSERT INTO control_source_ownership_v1 VALUES($1,$2,$3,$4)",
                [
                    (run_id, deployment["revision_id"], channel, partition)
                    for channel, partition in sorted(sources)
                ],
            )
            row = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1", run_id
            )
            return self._control(row)

    async def admit_source(
        self,
        token: str,
        *,
        run_id: str,
        request_id: str,
        channel: str,
        partition: str,
        record_id: str,
        attempt_id: str,
        object_key: str,
        payload_digest: str,
        payload_size: int,
        schema_id: str,
        lineage_digest: str,
        producer_id: str,
        producer_sequence: int,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "source")
        scalar_values = (
            request_id,
            record_id,
            attempt_id,
            object_key,
            payload_digest,
            schema_id,
            lineage_digest,
            producer_id,
        )
        if (
            any(not value for value in scalar_values)
            or producer_sequence <= 0
            or payload_size < 0
        ):
            raise ValueError("complete admission and producer identity required")
        identity = (
            channel,
            partition,
            record_id,
            attempt_id,
            object_key,
            payload_digest,
            payload_size,
            schema_id,
            lineage_digest,
            producer_id,
            producer_sequence,
        )
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "source")
            await self._tenant_row(conn, "control_runs_v1", "run_id", run_id, tenant)
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            await self._assert_prefect_source_authority(
                conn, token, run_id, channel, partition, producer_id
            )
            prior = await conn.fetchrow(
                "SELECT * FROM control_source_admissions_v1 "
                "WHERE run_id=$1 AND request_id=$2",
                run_id,
                request_id,
            )
            if prior is not None:
                actual = tuple(
                    prior[key]
                    for key in (
                        "channel",
                        "partition_id",
                        "record_id",
                        "attempt_id",
                        "object_key",
                        "payload_digest",
                        "payload_size",
                        "schema_id",
                        "lineage_digest",
                        "producer_id",
                        "producer_sequence",
                    )
                )
                if actual != identity:
                    raise ControlConflict("source admission identity drift")
                return dict(prior)
            if run["desired_state"] != "ACTIVE":
                raise ControlConflict("source admission closed")
            if not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM control_source_ownership_v1 WHERE run_id=$1 "
                "AND channel=$2 AND partition_id=$3)",
                run_id,
                channel,
                partition,
            ):
                raise ControlConflict("partition not owned by run")
            bound_producer = await conn.fetchval(
                "SELECT producer_id FROM control_admission_producers_v1 "
                "WHERE run_id=$1 AND channel=$2 AND partition_id=$3 FOR UPDATE",
                run_id,
                channel,
                partition,
            )
            if bound_producer is not None and bound_producer != producer_id:
                raise ControlConflict("source partition producer binding drift")
            producer = await conn.fetchrow(
                "SELECT * FROM control_admission_producers_v1 WHERE run_id=$1 "
                "AND channel=$2 AND partition_id=$3 AND producer_id=$4 FOR UPDATE",
                run_id,
                channel,
                partition,
                producer_id,
            )
            if producer is None:
                progress = await conn.fetchrow(
                    "SELECT accepted_through FROM producer_progress WHERE channel=$1 "
                    "AND partition_id=$2 AND producer_id=$3 FOR SHARE",
                    channel,
                    partition,
                    producer_id,
                )
                if progress is None:
                    raise ControlConflict("source producer is not registered")
                await conn.execute(
                    "INSERT INTO control_admission_producers_v1 VALUES("
                    "$1,$2,$3,$4,$5,$5,0,0,$6)",
                    run_id,
                    channel,
                    partition,
                    producer_id,
                    progress["accepted_through"],
                    "0" * 64,
                )
                accepted = retired = progress["accepted_through"]
            else:
                accepted = producer["accepted_through"]
                retired = producer["retired_through"]
            if producer_sequence <= retired:
                raise ControlIdempotencyHorizonExpired(
                    "source admission retry outside hot horizon"
                )
            same_sequence = await conn.fetchrow(
                "SELECT * FROM control_source_admissions_v1 WHERE run_id=$1 "
                "AND channel=$2 AND partition_id=$3 AND producer_id=$4 "
                "AND producer_sequence=$5",
                run_id,
                channel,
                partition,
                producer_id,
                producer_sequence,
            )
            if same_sequence is not None:
                actual = tuple(
                    same_sequence[key]
                    for key in (
                        "channel",
                        "partition_id",
                        "record_id",
                        "attempt_id",
                        "object_key",
                        "payload_digest",
                        "payload_size",
                        "schema_id",
                        "lineage_digest",
                        "producer_id",
                        "producer_sequence",
                    )
                )
                if actual != identity or same_sequence["request_id"] != request_id:
                    raise ControlConflict("source producer sequence reuse")
                return dict(same_sequence)
            if producer_sequence != accepted + 1:
                raise ControlConflict("source producer sequence gap")
            if (
                run["used_admission_records"] + 1 > run["max_admission_records"]
                or run["used_admission_bytes"] + payload_size
                > run["max_admission_bytes"]
            ):
                raise ControlCapacityBlocked("source admission capacity exhausted")
            ordinal = run["next_admission_ordinal"]
            admission_id = "admission:" + digest(
                {"run": run_id, "ordinal": ordinal, "request": request_id}
            )
            await conn.execute(
                "INSERT INTO control_source_admissions_v1("
                "run_id,admission_ordinal,admission_id,request_id,channel,partition_id,"
                "record_id,attempt_id,object_key,payload_digest,producer_id,"
                "producer_sequence,payload_size,schema_id,lineage_digest,status) "
                "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'ADMITTED')",
                run_id,
                ordinal,
                admission_id,
                request_id,
                channel,
                partition,
                record_id,
                attempt_id,
                object_key,
                payload_digest,
                producer_id,
                producer_sequence,
                payload_size,
                schema_id,
                lineage_digest,
            )
            await conn.execute(
                "UPDATE control_admission_producers_v1 SET accepted_through=$5 "
                "WHERE run_id=$1 AND channel=$2 AND partition_id=$3 AND producer_id=$4",
                run_id,
                channel,
                partition,
                producer_id,
                producer_sequence,
            )
            await conn.execute(
                "UPDATE control_runs_v1 SET next_admission_ordinal=$2,"
                "used_admission_records=used_admission_records+1,"
                "used_admission_bytes=used_admission_bytes+$3,"
                "control_version=control_version+1,updated_at=clock_timestamp() "
                "WHERE run_id=$1",
                run_id,
                ordinal + 1,
                payload_size,
            )
            return dict(
                await conn.fetchrow(
                    "SELECT * FROM control_source_admissions_v1 WHERE admission_id=$1",
                    admission_id,
                )
            )

    async def publish_admission(
        self,
        token: str,
        *,
        run_id: str,
        admission_id: str,
        payload: bytes,
    ) -> dict[str, Any]:
        """Append and publish in one transaction, or observe an exact native append."""
        tenant = await self._authorize(token, "source")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "source")
            await self._tenant_row(conn, "control_runs_v1", "run_id", run_id, tenant)
            admission = await conn.fetchrow(
                "SELECT * FROM control_source_admissions_v1 "
                "WHERE run_id=$1 AND admission_id=$2",
                run_id,
                admission_id,
            )
            if admission is None:
                raise ControlIdempotencyHorizonExpired(
                    "source publication is outside hot horizon"
                )
            await self._assert_prefect_source_authority(
                conn,
                token,
                run_id,
                admission["channel"],
                admission["partition_id"],
                admission["producer_id"],
            )
            if (hashlib.sha256(payload).hexdigest(), len(payload)) != (
                admission["payload_digest"],
                admission["payload_size"],
            ):
                raise ControlConflict("published payload does not match admission")
            if admission["status"] == "ADMITTED":
                state = await conn.fetchval(
                    "SELECT desired_state FROM control_runs_v1 WHERE run_id=$1",
                    run_id,
                )
                if state == "CANCELLED":
                    raise ControlConflict("cancelled run rejects source publication")
                try:
                    await self.stream._append_with_connection(
                        conn,
                        admission["attempt_id"],
                        admission["channel"],
                        admission["partition_id"],
                        admission["record_id"],
                        admission["object_key"],
                        payload,
                        admission["schema_id"],
                        admission["lineage_digest"],
                        admission["producer_id"],
                        admission["producer_sequence"],
                    )
                except asyncpg.RaiseError as exc:
                    if "source record lacks open durable admission" in str(exc):
                        raise ControlConflict(
                            "run lifecycle rejects source publication"
                        ) from exc
                    raise
                admission = await conn.fetchrow(
                    "SELECT * FROM control_source_admissions_v1 "
                    "WHERE run_id=$1 AND admission_id=$2",
                    run_id,
                    admission_id,
                )
            if admission["status"] != "PUBLISHED" or admission["sequence"] is None:
                raise ControlConflict("source admission is not durably published")
            return dict(admission)

    @staticmethod
    async def _assert_prefect_source_authority(
        conn: asyncpg.Connection,
        token: str,
        run_id: str,
        channel: str,
        partition: str,
        producer_id: str,
    ) -> None:
        """Fence adapter source actors without creating a second DATA ledger."""
        if not await conn.fetchval(
            "SELECT to_regclass('prefect_attempt_heads_v1') IS NOT NULL"
        ):
            return
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM prefect_attempt_heads_v1 WHERE run_id=$1)",
            run_id,
        ):
            return
        allowed = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM prefect_attempt_heads_v1 h "
            "JOIN prefect_execution_attempts_v1 a "
            "ON a.attempt_id=h.current_attempt_id AND a.status='ACTIVE' "
            "JOIN prefect_attempt_source_caps_v1 c "
            "ON c.attempt_id=a.attempt_id AND c.enabled "
            "WHERE h.run_id=$1 AND c.channel=$2 AND c.partition_id=$3 "
            "AND c.producer_id=$4 AND c.token_digest=$5)",
            run_id,
            channel,
            partition,
            producer_id,
            token_digest(token),
        )
        if not allowed:
            raise AuthenticationFailed("Prefect source execution attempt fenced")

    async def retire_admissions(
        self,
        token: str,
        *,
        run_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> dict[str, Any]:
        """Retire hot admissions only below the existing R3 metadata horizon."""
        tenant = await self._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "control")
            visible = await self._tenant_row(
                conn, "control_runs_v1", "run_id", run_id, tenant
            )
            await self._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            await self._assert_leader(
                conn, run["deployment_id"], leader_id, leader_epoch
            )
            producers = list(
                await conn.fetch(
                    "SELECT * FROM control_admission_producers_v1 WHERE run_id=$1 "
                    "ORDER BY channel,partition_id,producer_id FOR UPDATE",
                    run_id,
                )
            )
            retired_ids: list[str] = []
            retired_records = 0
            retired_bytes = 0
            updates: list[tuple[Any, ...]] = []
            for producer in producers:
                metadata_floor = await conn.fetchval(
                    "SELECT retired_through FROM metadata_summaries "
                    "WHERE channel=$1 AND partition_id=$2",
                    producer["channel"],
                    producer["partition_id"],
                )
                floors = [
                    await conn.fetchval(
                        "SELECT min(c.completed_through) "
                        "FROM control_run_consumers_v1 b JOIN consumers c "
                        "USING(consumer_id) WHERE b.run_id=$1 AND b.channel=$2 "
                        "AND b.partition_id=$3",
                        run_id,
                        producer["channel"],
                        producer["partition_id"],
                    )
                ]
                if await conn.fetchval(
                    "SELECT to_regclass('join_inputs_v3') IS NOT NULL"
                ):
                    floors.append(
                        await conn.fetchval(
                            "SELECT min(completed) FROM join_inputs_v3 "
                            "WHERE revision_id=$1 AND channel=$2 AND partition_id=$3",
                            run["revision_id"],
                            producer["channel"],
                            producer["partition_id"],
                        )
                    )
                actual_floors = [floor for floor in floors if floor is not None]
                consumer_floor = min(actual_floors) if actual_floors else None
                if metadata_floor is None or consumer_floor is None:
                    continue
                rows = list(
                    await conn.fetch(
                        "SELECT * FROM control_source_admissions_v1 WHERE run_id=$1 "
                        "AND channel=$2 AND partition_id=$3 AND producer_id=$4 "
                        "AND producer_sequence>$5 ORDER BY producer_sequence FOR UPDATE",
                        run_id,
                        producer["channel"],
                        producer["partition_id"],
                        producer["producer_id"],
                        producer["retired_through"],
                    )
                )
                eligible = []
                expected = producer["retired_through"] + 1
                for row in rows:
                    if row["producer_sequence"] != expected:
                        raise PostgresProtocolError("control admission producer gap")
                    if (
                        row["status"] != "PUBLISHED"
                        or row["sequence"] > metadata_floor
                        or row["sequence"] > consumer_floor
                    ):
                        break
                    eligible.append(row)
                    expected += 1
                if not eligible:
                    continue
                material = [
                    {
                        "admission_id": row["admission_id"],
                        "producer_sequence": row["producer_sequence"],
                        "record_id": row["record_id"],
                        "sequence": row["sequence"],
                        "payload_digest": row["payload_digest"],
                        "payload_size": row["payload_size"],
                    }
                    for row in eligible
                ]
                rolling = hashlib.sha256(
                    (producer["rolling_digest"] + "\0" + canonical(material)).encode()
                ).hexdigest()
                count = len(eligible)
                byte_count = sum(row["payload_size"] for row in eligible)
                retired_ids.extend(row["admission_id"] for row in eligible)
                retired_records += count
                retired_bytes += byte_count
                updates.append(
                    (
                        run_id,
                        producer["channel"],
                        producer["partition_id"],
                        producer["producer_id"],
                        eligible[-1]["producer_sequence"],
                        producer["retired_records"] + count,
                        producer["retired_bytes"] + byte_count,
                        rolling,
                    )
                )
            if retired_ids:
                await conn.execute(
                    "SELECT set_config('ariadne.control_retire','on',true)"
                )
                await conn.execute(
                    "DELETE FROM control_source_admissions_v1 "
                    "WHERE admission_id=ANY($1::text[])",
                    retired_ids,
                )
                await conn.executemany(
                    "UPDATE control_admission_producers_v1 SET retired_through=$5,"
                    "retired_records=$6,retired_bytes=$7,rolling_digest=$8 "
                    "WHERE run_id=$1 AND channel=$2 AND partition_id=$3 AND producer_id=$4",
                    updates,
                )
                await conn.execute(
                    "UPDATE control_runs_v1 SET "
                    "used_admission_records=used_admission_records-$2,"
                    "used_admission_bytes=used_admission_bytes-$3,"
                    "control_version=control_version+1,updated_at=clock_timestamp() "
                    "WHERE run_id=$1",
                    run_id,
                    retired_records,
                    retired_bytes,
                )
            current = await conn.fetchrow(
                "SELECT used_admission_records,used_admission_bytes,"
                "max_admission_records,max_admission_bytes FROM control_runs_v1 "
                "WHERE run_id=$1",
                run_id,
            )
            return {
                "run_id": run_id,
                "retired_records": retired_records,
                "retired_bytes": retired_bytes,
                "pressure": dict(current),
            }

    @staticmethod
    async def _existing_command(
        conn: asyncpg.Connection, run_id: str, request_id: str, command: str
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            "SELECT command,outcome FROM control_commands_v1 WHERE run_id=$1 AND request_id=$2",
            run_id,
            request_id,
        )
        if row is None:
            prior = await conn.fetchrow(
                "SELECT request_id FROM control_commands_v1 "
                "WHERE run_id=$1 AND command=$2",
                run_id,
                command,
            )
            if prior is not None:
                raise ControlConflict("control command already has another request")
            return None
        if row["command"] != command:
            raise ControlConflict("control request identity drift")
        return decoded(row["outcome"])

    async def drain_run(
        self,
        token: str,
        *,
        run_id: str,
        request_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "control")
            visible = await self._tenant_row(
                conn, "control_runs_v1", "run_id", run_id, tenant
            )
            await self._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            await self._assert_leader(
                conn, run["deployment_id"], leader_id, leader_epoch
            )
            existing = await self._existing_command(conn, run_id, request_id, "DRAIN")
            if existing is not None:
                return existing
            if run["desired_state"] == "CANCELLED":
                raise ControlConflict("cancelled run cannot drain")
            cut = run["next_admission_ordinal"] - 1
            if run["desired_state"] == "DRAINING":
                cut = run["drain_admission_cut"]
            else:
                await conn.execute(
                    "UPDATE control_runs_v1 SET desired_state='DRAINING',"
                    "lifecycle='DRAINING',drain_admission_cut=$2,"
                    "control_version=control_version+1,updated_at=clock_timestamp() "
                    "WHERE run_id=$1",
                    run_id,
                    cut,
                )
            outcome = {
                "run_id": run_id,
                "desired_state": "DRAINING",
                "drain_admission_cut": cut,
            }
            await conn.execute(
                "INSERT INTO control_commands_v1 VALUES("
                "$1,$2,'DRAIN',$3,$4,$5::jsonb,clock_timestamp())",
                run_id,
                request_id,
                leader_id,
                leader_epoch,
                canonical(outcome),
            )
            return outcome

    @staticmethod
    async def _fence_run_workers(
        conn: asyncpg.Connection, run_id: str, revision_id: str, reason: str
    ) -> None:
        consumer_ids = [
            row["consumer_id"]
            for row in await conn.fetch(
                "SELECT consumer_id FROM control_run_consumers_v1 WHERE run_id=$1",
                run_id,
            )
        ]
        await conn.fetch(
            "SELECT consumer_id FROM consumers WHERE consumer_id=ANY($1::text[]) "
            "FOR UPDATE",
            consumer_ids,
        )
        await conn.execute(
            "DELETE FROM runtime_invocations WHERE consumer_id=ANY($1::text[])",
            consumer_ids,
        )
        await conn.execute(
            "UPDATE consumers SET owner_id=NULL,owner_epoch=owner_epoch+1,"
            "lease_expires_at=NULL,in_flight_sequence=NULL,in_flight_end=NULL "
            "WHERE consumer_id=ANY($1::text[])",
            consumer_ids,
        )
        if await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL"):
            await conn.execute(
                "DELETE FROM join_invocations_v3 WHERE revision_id=$1", revision_id
            )
        await conn.execute(
            "INSERT INTO control_worker_fences_v1(run_id,fence_epoch,reason) "
            "SELECT run_id,control_version+1,$2 FROM control_runs_v1 WHERE run_id=$1",
            run_id,
            reason,
        )

    async def cancel_run(
        self,
        token: str,
        *,
        run_id: str,
        request_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "control")
            visible = await self._tenant_row(
                conn, "control_runs_v1", "run_id", run_id, tenant
            )
            await self._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            await self._assert_leader(
                conn, run["deployment_id"], leader_id, leader_epoch
            )
            existing = await self._existing_command(conn, run_id, request_id, "CANCEL")
            if existing is not None:
                return existing
            if run["lifecycle"] == "DRAINED":
                raise ControlConflict("drained run is terminal")
            if run["desired_state"] != "CANCELLED":
                await self._fence_run_workers(
                    conn, run_id, run["revision_id"], "CANCELLED"
                )
                await conn.execute(
                    "UPDATE control_runs_v1 SET desired_state='CANCELLED',"
                    "lifecycle='CANCELLED',control_version=control_version+1,"
                    "updated_at=clock_timestamp() WHERE run_id=$1",
                    run_id,
                )
            outcome = {"run_id": run_id, "desired_state": "CANCELLED"}
            await conn.execute(
                "INSERT INTO control_commands_v1 VALUES("
                "$1,$2,'CANCEL',$3,$4,$5::jsonb,clock_timestamp())",
                run_id,
                request_id,
                leader_id,
                leader_epoch,
                canonical(outcome),
            )
            return outcome

    async def reconcile_run(
        self,
        token: str,
        *,
        run_id: str,
        leader_id: str,
        leader_epoch: int,
    ) -> dict[str, Any]:
        tenant = await self._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_authorized(conn, token, tenant, "control")
            visible = await self._tenant_row(
                conn, "control_runs_v1", "run_id", run_id, tenant
            )
            await self._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            await self._assert_leader(
                conn, run["deployment_id"], leader_id, leader_epoch
            )
            participants = decoded(
                await conn.fetchval(
                    "SELECT participant_manifest FROM deployment_revisions "
                    "WHERE revision_id=$1",
                    run["revision_id"],
                )
            )
            native = await self._validate_native_runtime(
                conn, run["revision_id"], participants
            )
            if (run["execution_mode"] == "NATIVE_JOIN") != native:
                raise ControlConflict("run execution mode authority drift")
            if run["desired_state"] != "DRAINING" or run["lifecycle"] == "DRAINED":
                return self._control(run)
            incomplete = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM control_source_admissions_v1 "
                "WHERE run_id=$1 AND admission_ordinal<=$2 AND status<>'PUBLISHED')",
                run_id,
                run["drain_admission_cut"],
            )
            consumers = list(
                await conn.fetch(
                    "SELECT c.* FROM control_run_consumers_v1 b JOIN consumers c "
                    "USING(consumer_id) WHERE b.run_id=$1 FOR UPDATE OF c",
                    run_id,
                )
            )
            for consumer in consumers:
                head = await conn.fetchval(
                    "SELECT next_sequence-1 FROM partitions WHERE channel=$1 AND partition_id=$2",
                    consumer["channel"],
                    consumer["partition_id"],
                )
                if (
                    consumer["completed_through"] != head
                    or consumer["in_flight_sequence"] is not None
                ):
                    incomplete = True
            if await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL"):
                lagging = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM join_inputs_v3 i JOIN partitions p "
                    "ON p.channel=i.channel AND p.partition_id=i.partition_id "
                    "JOIN partition_log_heads h ON h.channel=i.channel "
                    "AND h.partition_id=i.partition_id WHERE i.revision_id=$1 "
                    "AND (i.completed<>p.next_sequence-1 "
                    "OR i.scanned<>h.next_log_sequence-1 "
                    "OR i.blocked_checkpoint IS NOT NULL))",
                    run["revision_id"],
                )
                busy = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM join_workers w WHERE w.revision_id=$1 "
                    "AND (w.phase<>'LIVE' OR EXISTS(SELECT 1 FROM join_invocations_v3 v "
                    "WHERE v.revision_id=w.revision_id AND v.participant_id=w.participant_id)))",
                    run["revision_id"],
                )
                incomplete = incomplete or lagging or busy
            pending_checkpoint = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_intents_v2 i "
                "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                "WHERE i.revision_id=$1 "
                "AND l.status NOT IN('COMPLETED','ABORTED','RETIRED'))",
                run["revision_id"],
            )
            if await conn.fetchval(
                "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
            ):
                global_restoring = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 p "
                    "WHERE p.revision_id=$1 AND p.status='RESTORING' "
                    "AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r "
                    "WHERE r.plan_id=p.recovery_id))",
                    run["revision_id"],
                )
            else:
                global_restoring = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 "
                    "WHERE revision_id=$1 AND status='RESTORING')",
                    run["revision_id"],
                )
            incomplete = incomplete or pending_checkpoint or global_restoring
            if not incomplete:
                await self._fence_run_workers(
                    conn, run_id, run["revision_id"], "DRAINED"
                )
                await conn.execute(
                    "UPDATE control_runs_v1 SET lifecycle='DRAINED',"
                    "control_version=control_version+1,updated_at=clock_timestamp() "
                    "WHERE run_id=$1",
                    run_id,
                )
                run = await conn.fetchrow(
                    "SELECT * FROM control_runs_v1 WHERE run_id=$1", run_id
                )
            return self._control(run)

    async def project_run(self, token: str, *, run_id: str) -> dict[str, Any]:
        tenant = await self._authorize(token, "read")
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            await self._assert_authorized(conn, token, tenant, "read", lock=False)
            run = await self._tenant_row(
                conn, "control_runs_v1", "run_id", run_id, tenant
            )
            consumers = [
                dict(row)
                for row in await conn.fetch(
                    "SELECT c.consumer_id,c.channel,c.partition_id,c.completed_through,"
                    "c.in_flight_sequence,c.in_flight_end,c.owner_id,c.owner_epoch,"
                    "c.lease_expires_at FROM control_run_consumers_v1 b "
                    "JOIN consumers c USING(consumer_id) WHERE b.run_id=$1 "
                    "ORDER BY c.consumer_id",
                    run_id,
                )
            ]
            revision = await conn.fetchrow(
                "SELECT source_manifest,participant_manifest FROM deployment_revisions "
                "WHERE revision_id=$1",
                run["revision_id"],
            )
            sources = decoded(revision["source_manifest"])
            participants = decoded(revision["participant_manifest"])
            locations = {(item["channel"], item["partition"]) for item in sources} | {
                (item["channel"], item["partition"])
                for participant in participants
                for item in participant["outputs"]
            }
            partitions = []
            for channel, partition in sorted(locations):
                row = await conn.fetchrow(
                    "SELECT channel,partition_id,next_sequence,used_records,used_bytes "
                    "FROM partitions WHERE channel=$1 AND partition_id=$2",
                    channel,
                    partition,
                )
                if row is None:
                    raise PostgresProtocolError("revision source disappeared")
                partitions.append(dict(row))
            operators: list[dict[str, Any]] = []
            inputs: list[dict[str, Any]] = []
            if await conn.fetchval("SELECT to_regclass('join_workers') IS NOT NULL"):
                for row in await conn.fetch(
                    "SELECT participant_id,owner_id,owner_epoch,lease_expires_at,"
                    "phase,next_arrival,next_apply,state FROM join_workers "
                    "WHERE revision_id=$1 ORDER BY participant_id",
                    run["revision_id"],
                ):
                    item = dict(row)
                    item["state_digest"] = digest(decoded(item.pop("state")))
                    operators.append(item)
                inputs = [
                    dict(row)
                    for row in await conn.fetch(
                        "SELECT participant_id,channel,partition_id,scanned,completed,"
                        "blocked_checkpoint FROM join_inputs_v3 WHERE revision_id=$1 "
                        "ORDER BY participant_id,channel,partition_id",
                        run["revision_id"],
                    )
                ]
            barrier = await conn.fetchrow(
                "SELECT i.checkpoint_id,i.checkpoint_ordinal,i.intent_digest,"
                "l.status,l.last_authority_ordinal FROM checkpoint_intents_v2 i "
                "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                "WHERE i.revision_id=$1 ORDER BY i.checkpoint_ordinal DESC LIMIT 1",
                run["revision_id"],
            )
            barrier_projection = None if barrier is None else dict(barrier)
            if barrier_projection is not None:
                barrier_projection["authority"] = "HOT"
            retired_barrier = await conn.fetchrow(
                "SELECT checkpoint_id,checkpoint_ordinal,completion_digest,"
                "retired_authority_ordinal FROM retired_checkpoint_summaries "
                "WHERE revision_id=$1 ORDER BY checkpoint_ordinal DESC LIMIT 1",
                run["revision_id"],
            )
            if retired_barrier is not None and (
                barrier_projection is None
                or retired_barrier["checkpoint_ordinal"]
                > barrier_projection["checkpoint_ordinal"]
            ):
                barrier_projection = dict(retired_barrier)
                barrier_projection.update(
                    {"status": "RETIRED", "authority": "ARCHIVED"}
                )
            recovery = None
            has_regional_recovery = await conn.fetchval(
                "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL "
                "AND to_regclass('regional_recovery_workers_v1') IS NOT NULL"
            )
            if has_regional_recovery:
                regional = await conn.fetchrow(
                    "SELECT plan_id,checkpoint_id,failed_node,reasons_json,created_at "
                    "FROM region_recovery_plans_v1 WHERE revision_id=$1 "
                    "AND EXISTS(SELECT 1 FROM regional_recovery_workers_v1 w "
                    "WHERE w.plan_id=region_recovery_plans_v1.plan_id "
                    "AND w.status<>'ACTIVE') "
                    "ORDER BY created_at DESC LIMIT 1",
                    run["revision_id"],
                )
                if regional is not None:
                    recovery = dict(regional)
                    recovery["kind"] = "REGION"
                    recovery["reasons"] = decoded(recovery.pop("reasons_json"))
            if recovery is None:
                if has_regional_recovery:
                    global_plan = await conn.fetchrow(
                        "SELECT recovery_id,checkpoint_id,status,created_at "
                        "FROM recovery_plans_v2 p WHERE revision_id=$1 "
                        "AND status='RESTORING' AND NOT EXISTS(SELECT 1 "
                        "FROM region_recovery_plans_v1 r "
                        "WHERE r.plan_id=p.recovery_id) "
                        "ORDER BY created_at DESC LIMIT 1",
                        run["revision_id"],
                    )
                else:
                    global_plan = await conn.fetchrow(
                        "SELECT recovery_id,checkpoint_id,status,created_at "
                        "FROM recovery_plans_v2 WHERE revision_id=$1 "
                        "AND status='RESTORING' ORDER BY created_at DESC LIMIT 1",
                        run["revision_id"],
                    )
                if global_plan is not None:
                    recovery = dict(global_plan)
                    recovery["kind"] = "GLOBAL"
            leader = await conn.fetchrow(
                "SELECT leader_id,leader_epoch,lease_expires_at,"
                "lease_expires_at>clock_timestamp() AS live FROM control_leaders_v1 "
                "WHERE deployment_id=$1",
                run["deployment_id"],
            )
            return {
                "control": self._control(run),
                "leader": None if leader is None else dict(leader),
                "partitions": partitions,
                "consumers": consumers,
                "operators": operators,
                "inputs": inputs,
                "admission_pressure": {
                    "used_records": run["used_admission_records"],
                    "max_records": run["max_admission_records"],
                    "used_bytes": run["used_admission_bytes"],
                    "max_bytes": run["max_admission_bytes"],
                },
                "barrier": barrier_projection,
                "recovery": recovery,
            }
