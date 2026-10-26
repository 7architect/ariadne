import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from source_v1 import *


class External:
    def __init__(self): self.epochs={}; self.acks=[]
    def fence(self,p,e):
        current=self.epochs.get(p,0)
        if e<current: raise SourceProtocolError("stale fence")
        self.epochs[p]=e
        return e
    def ack(self,p,pos,ordinal,e):
        if self.epochs.get(p)!=e: raise SourceProtocolError("stale ACK")
        if self.acks and self.acks[-1][0]==p and ordinal<self.acks[-1][2]: raise SourceProtocolError("ACK regression")
        if self.acks and self.acks[-1][:3]==(p,pos,ordinal): return
        self.acks.append((p,pos,ordinal,e))

def env(p,pos,payload=None):
    v={"p":pos} if payload is None else payload
    return SourceEnvelope(p,pos,source_record_id(p,pos),f"lineage:{p}:{pos}",value_digest(v),v)

def committed(cid, cuts):
    return CombinedDecisionRef(
        "job", "graph", cid,
        "ariadne:transaction:"+value_digest({"job":"job","graph":"graph","checkpoint":cid}),
        1, f"intent:{cid}", "capabilities",
        "coordinator", 11, tuple(sorted((p, pos) for p, _, pos in cuts)),
        tuple(sorted((p,{"p0":4,"p1":7}[p]) for p,_,_ in cuts)),
    )

def authorize(ledger, decision):
    authority=sqlite3.connect(ledger.decision_db_path)
    with authority:
        authority.execute("""CREATE TABLE IF NOT EXISTS checkpoints(
          job_id TEXT,graph_version_id TEXT,checkpoint_id TEXT,sequence INTEGER,
          manifest_digest TEXT,phase TEXT,decision TEXT,active_owner_id TEXT,
          fence_epoch INTEGER,intent_json TEXT,PRIMARY KEY(job_id,graph_version_id,checkpoint_id))""")
        authority.execute("INSERT OR REPLACE INTO checkpoints VALUES(?,?,?,?,?,?,?,?,?,?)",(
          decision.job_id,decision.graph_version_id,decision.checkpoint_id,decision.sequence,
          decision.intent_digest,"COMMIT_DECIDED","COMMIT",decision.coordinator_owner_id,
          decision.coordinator_epoch,json.dumps({"capability_digest":decision.capability_digest,
          "source_positions":decision.source_positions,"generation_manifest":decision.source_epochs})))

def bind(ledger, cid, cuts):
    decision=committed(cid,cuts)
    authorize(ledger,decision)
    ledger.bind_committed_checkpoint(decision,cuts)
    return decision

class SourceV1Tests(unittest.TestCase):
    def open(self,d):
        x=External(); l=AcceptedInputLedger(Path(d)/"s.db",Path(d)/"authority.db")
        l.claim_partition("p0","a",4,x.fence); l.claim_partition("p1","a",7,x.fence)
        return l,x
    def test_runtime_ordinals_position_identity_and_opaque_order(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.open(d); a=l.accept(env("p0","10"),"a",4)
            self.assertEqual(a.ordinal,1); self.assertEqual(l.accept(env("p0","10"),"a",4),a)
            with self.assertRaises(SourceProtocolError): l.accept(env("p0","10",{"x":2}),"a",4)
            self.assertEqual(l.accept(env("p0","2"),"a",4).ordinal,2)
            self.assertEqual([x.opaque_position for x in l.replay_plan("p0")],["10","2"])
    def test_atomic_accept_and_raw_storage_guards(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.open(d)
            with self.assertRaises(SourceInjectedFailure): l.accept(env("p0","x"),"a",4,fault_point="accept:before_commit")
            self.assertEqual(l.replay_plan("p0"),())
            r=l.accept(env("p0","x"),"a",4)
            for sql in ("DELETE FROM accepted_records", "INSERT INTO accepted_records VALUES('p0',3,'gap','g','l','d','{}','a',4)"):
                with self.assertRaises(sqlite3.IntegrityError), l.connection: l.connection.execute(sql)
            with self.assertRaises(sqlite3.OperationalError): l.connection.execute("INSERT INTO channel_records VALUES('p1',1,'x','x','x','x','{}')")
            self.assertEqual(l.replay_plan("p0"),(r,))
    def test_idle_zero_cut_ack_and_external_takeover_fence(self):
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d); r=l.accept(env("p0","z"),"a",4)
            decision=bind(l,"c",(("p0",r.ordinal,"z"),("p1",0,None)))
            def takeover(p,pos,ordinal,e): l.claim_partition(p,"b",5,x.fence); x.ack(p,pos,ordinal,e)
            with self.assertRaisesRegex(SourceProtocolError,"stale ACK"): l.acknowledge("c",decision.digest,"p0","a",4,takeover)
            l.acknowledge("c",decision.digest,"p0","b",5,x.ack); self.assertEqual(x.acks,[("p0","z",1,5)])
    def test_concurrent_identity_conflict_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.open(d); other=AcceptedInputLedger(Path(d)/"s.db"); one=env("p0","same")
            accepted=l.accept(one,"a",4,race_hook=lambda: other.accept(one,"a",4))
            self.assertEqual(accepted.ordinal,1)
            with self.assertRaises(SourceProtocolError): l.accept(replace(one,record_id=source_record_id("p0","other")),"a",4)
    def test_exact_fence_storage_identity_and_ack_monotonicity(self):
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d)
            with self.assertRaises(sqlite3.IntegrityError), l.connection:
                l.connection.execute("INSERT INTO accepted_records VALUES('p0',1,'forged','raw','l','wrong','{}','a',4)")
            x.epochs["p0"]=6
            with self.assertRaisesRegex(SourceProtocolError,"exact epoch|stale fence"):
                l.claim_partition("p0","b",5,x.fence)
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d); one=l.accept(env("p0","one"),"a",4)
            d1=bind(l,"c1",(("p0",one.ordinal,"one"),("p1",0,None)))
            two=l.accept(env("p0","two"),"a",4)
            d2=bind(l,"c2",(("p0",two.ordinal,"two"),("p1",0,None)))
            l.acknowledge("c2",d2.digest,"p0","a",4,x.ack)
            l.acknowledge("c1",d1.digest,"p0","a",4,x.ack)
            self.assertEqual(x.acks,[("p0","two",2,4)])
    def test_storage_rejects_partition_rollback_replace_delete_and_forged_ack(self):
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d); l.claim_partition("p0","b",5,x.fence)
            for sql in (
                "UPDATE partitions SET owner_id='a',epoch=4,state='ACTIVE' WHERE partition_id='p0'",
                "DELETE FROM partitions WHERE partition_id='p0'",
                "INSERT OR REPLACE INTO partitions VALUES('p0','a',4,'ACTIVE',0)",
                "UPDATE partitions SET ack_ordinal=999 WHERE partition_id='p0'",
            ):
                with self.assertRaises(sqlite3.IntegrityError), l.connection:
                    l.connection.execute(sql)
            with self.assertRaisesRegex(SourceProtocolError,"stale source owner"):
                l.accept(env("p0","stale-write"),"a",4)
    def test_storage_rejects_forged_initial_authority_and_requires_fence_witness(self):
        for state,ack in (("ACTIVE",0),("ACTIVE",999),("FENCING",999),("FENCING",-1)):
            with self.subTest(state=state,ack=ack), tempfile.TemporaryDirectory() as d:
                l=AcceptedInputLedger(Path(d)/"s.db")
                with self.assertRaises(sqlite3.IntegrityError), l.connection:
                    l.connection.execute("INSERT INTO partitions VALUES('p0','forged',7,?,?)",(state,ack))
                self.assertIsNone(l.connection.execute("SELECT 1 FROM partitions").fetchone())
        with tempfile.TemporaryDirectory() as d:
            l=AcceptedInputLedger(Path(d)/"s.db")
            with l.connection:
                l.connection.execute("INSERT INTO partitions VALUES('p0','owner',7,'FENCING',0)")
            with self.assertRaises(sqlite3.IntegrityError), l.connection:
                l.connection.execute("UPDATE partitions SET state='ACTIVE' WHERE partition_id='p0'")
            with self.assertRaisesRegex(SourceProtocolError,"stale source owner"):
                l.accept(env("p0","hidden"),"owner",7)
    def test_ack_attempt_survives_lost_local_write_and_replays_until_retry(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/"s.db"; l,x=self.open(d); one=l.accept(env("p0","one"),"a",4)
            cuts=(("p0",one.ordinal,"one"),("p1",0,None)); decision=bind(l,"c1",cuts)
            l.bind_committed_checkpoint(decision,cuts)
            with self.assertRaises(SourceInjectedFailure):
                l.acknowledge("c1",decision.digest,"p0","a",4,x.ack,fault_point="ack:after_external_before_local")
            self.assertEqual(l.ack_status("c1","p0")["outcome"],"IN_PROGRESS")
            self.assertEqual(l.replay_plan("p0"),(one,)); l.connection.close()
            reopened=AcceptedInputLedger(path); reopened.acknowledge("c1",decision.digest,"p0","a",4,x.ack)
            status=reopened.ack_status("c1","p0")
            self.assertEqual(status["attempt_count"],2); self.assertEqual(status["outcome"],"SUCCEEDED")
            self.assertEqual(reopened.replay_plan("p0"),())
    def test_unavailable_ack_is_durably_blocked_then_recovers(self):
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d); one=l.accept(env("p0","one"),"a",4)
            decision=bind(l,"c1",(("p0",one.ordinal,"one"),("p1",0,None)))
            def offline(*_): raise ConnectionError("source offline")
            with self.assertRaises(ConnectionError): l.acknowledge("c1",decision.digest,"p0","a",4,offline)
            status=l.ack_status("c1","p0"); self.assertEqual(status["outcome"],"BLOCKED"); self.assertIn("offline",status["last_error"])
            l.acknowledge("c1",decision.digest,"p0","a",4,x.ack)
            self.assertEqual(l.ack_status("c1","p0")["outcome"],"SUCCEEDED")

    def test_forged_or_mixed_decision_cannot_be_bound(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.open(d); one=l.accept(env("p0","one"),"a",4)
            cuts=(("p0",one.ordinal,"one"),("p1",0,None))
            decision=committed("forged",cuts)
            with self.assertRaisesRegex(SourceProtocolError,"unverified"):
                l.bind_committed_checkpoint(decision,cuts)
            mixed=replace(decision,source_positions=(("p0","different"),("p1",None)))
            authorize(l,decision)
            with self.assertRaisesRegex(SourceProtocolError,"unverified"):
                l.bind_committed_checkpoint(mixed,cuts)
            with self.assertRaises(TypeError):
                l.bind_committed_checkpoint(decision,cuts,lambda _: True)
            self.assertEqual(l.connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0],0)

    def test_checkpoint_vector_and_source_scope_are_frozen(self):
        with tempfile.TemporaryDirectory() as d:
            l,_=self.open(d); one=l.accept(env("p0","one"),"a",4)
            decision=bind(l,"c1",(("p0",one.ordinal,"one"),("p1",0,None)))
            statements=(
                "INSERT INTO checkpoints VALUES('raw','d','{}',2,'COMMIT')",
                "UPDATE checkpoints SET expected=999 WHERE checkpoint_id='c1'",
                "UPDATE checkpoints SET decision_digest='rewritten' WHERE checkpoint_id='c1'",
                "INSERT OR REPLACE INTO checkpoints VALUES('c1','forged','{}',2,'BUILDING')",
                "INSERT OR REPLACE INTO cuts VALUES('c1','p0',1,'one')",
                "DELETE FROM checkpoints WHERE checkpoint_id='c1'",
                "INSERT INTO partitions VALUES('late','owner',1,'FENCING',0)",
            )
            for statement in statements:
                with self.subTest(statement=statement), self.assertRaises(sqlite3.IntegrityError), l.connection:
                    l.connection.execute(statement)
            l.bind_committed_checkpoint(decision,(("p0",one.ordinal,"one"),("p1",0,None)))

    def test_stale_failed_callback_is_reconciled_after_newer_ack(self):
        with tempfile.TemporaryDirectory() as d:
            l,x=self.open(d); one=l.accept(env("p0","one"),"a",4)
            d1=bind(l,"c1",(("p0",one.ordinal,"one"),("p1",0,None)))
            two=l.accept(env("p0","two"),"a",4)
            d2=bind(l,"c2",(("p0",two.ordinal,"two"),("p1",0,None)))
            def stale_callback(*_):
                l.claim_partition("p0","b",5,x.fence)
                l.acknowledge("c2",d2.digest,"p0","b",5,x.ack)
                raise ConnectionError("late stale failure")
            with self.assertRaises(ConnectionError):
                l.acknowledge("c1",d1.digest,"p0","a",4,stale_callback)
            self.assertEqual(l.ack_status("c1","p0")["outcome"],"SUBSUMED")
            self.assertEqual(l.replay_plan("p0"),())

    def test_unknown_external_ack_converges_after_takeover(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/"s.db"; l,x=self.open(d)
            one=l.accept(env("p0","one"),"a",4)
            decision=bind(l,"c1",(("p0",one.ordinal,"one"),("p1",0,None)))
            with self.assertRaises(SourceInjectedFailure):
                l.acknowledge("c1",decision.digest,"p0","a",4,x.ack,
                              fault_point="ack:after_external_before_local")
            self.assertEqual(l.replay_plan("p0"),(one,))
            l.claim_partition("p0","b",5,x.fence); l.connection.close()
            reopened=AcceptedInputLedger(path,Path(d)/"authority.db")
            reopened.acknowledge("c1",decision.digest,"p0","b",5,x.ack)
            self.assertEqual(reopened.replay_plan("p0"),())
            self.assertEqual(reopened.ack_status("c1","p0")["outcome"],"SUCCEEDED")
            self.assertEqual(x.acks,[("p0","one",1,4)])

if __name__=="__main__": unittest.main()
