from __future__ import annotations

import unittest
from dataclasses import replace

from integrated_protocol import SinkTier
from tier_semantics import (
    BPolicy, EffectAuthority, Exhaustion, GUARANTEES, TierContractError, TierExecutionIntent,
    TierObservation, Visibility, validate_observation,
)


def intent(*tiers: SinkTier, requested: SinkTier, policy: BPolicy | None=None) -> TierExecutionIntent:
    participants=tuple((f"sink-{i}",tier) for i,tier in enumerate(tiers))
    return TierExecutionIntent(
        "tx",requested,min(tiers,key=lambda tier:{SinkTier.B:0,SinkTier.I:1,SinkTier.T:2}[tier]),
        participants,"capability",tuple((sink,f"key:{sink}","payload") for sink,_ in participants),policy,
    )


class TierSemanticsTests(unittest.TestCase):
    def test_guarantee_vocabulary_is_explicit_for_every_tier(self):
        self.assertEqual(set(GUARANTEES),set(SinkTier))
        self.assertTrue(GUARANTEES[SinkTier.T].prepare_required)
        self.assertTrue(GUARANTEES[SinkTier.I].stable_effect_key_required)
        self.assertTrue(GUARANTEES[SinkTier.B].duplicates_possible)

    def test_mixed_group_uses_weakest_tier_and_rejects_stronger_request(self):
        mixed=intent(SinkTier.T,SinkTier.I,requested=SinkTier.I)
        self.assertEqual(mixed.effective_tier,SinkTier.I)
        with self.assertRaisesRegex(TierContractError,"below requested"):
            intent(SinkTier.I,SinkTier.B,requested=SinkTier.I,
                   policy=BPolicy(2,True,Exhaustion.BLOCK))

    def test_b_policy_is_mandatory_and_part_of_immutable_digest(self):
        with self.assertRaisesRegex(TierContractError,"B policy"):
            intent(SinkTier.B,requested=SinkTier.B)
        base=intent(SinkTier.B,requested=SinkTier.B,
                    policy=BPolicy(2,True,Exhaustion.BLOCK))
        changed=replace(base,b_policy=BPolicy(3,True,Exhaustion.BLOCK))
        self.assertNotEqual(base.digest,changed.digest)

    def test_t_and_i_reject_duplicates_and_gaps(self):
        for tier in (SinkTier.T,SinkTier.I):
            with self.subTest(tier=tier):
                contract=intent(tier,requested=tier)
                observed=TierObservation(contract.digest,tier,Visibility.COMPLETE,
                    EffectAuthority.COMMIT if tier is SinkTier.T else EffectAuthority.DELIVERY_INTENT,
                    (("sink-0",2),),(contract.effects[0],contract.effects[0]),gaps=("sink-0",))
                violations=validate_observation(contract,observed)
                self.assertIn("logical duplicate violates tier guarantee",violations)
                self.assertIn("gap violates tier guarantee",violations)

    def test_b_gap_requires_explicit_lossy_policy(self):
        blocked=intent(SinkTier.B,requested=SinkTier.B,
                       policy=BPolicy(1,False,Exhaustion.BLOCK))
        observed=TierObservation(blocked.digest,SinkTier.B,Visibility.GAPPED,
            EffectAuthority.DELIVERY_INTENT,(("sink-0",1),),(),gaps=("sink-0",))
        self.assertIn("gap lacks frozen lossy opt-in",validate_observation(blocked,observed))
        lossy=replace(blocked,b_policy=BPolicy(1,False,Exhaustion.ADVANCE_WITH_GAP))
        observed=replace(observed,intent_digest=lossy.digest)
        self.assertEqual(validate_observation(lossy,observed),())

    def test_observation_rejects_tier_and_intent_drift(self):
        contract=intent(SinkTier.I,requested=SinkTier.I)
        observed=TierObservation("forged",SinkTier.B,Visibility.GAPPED,
            EffectAuthority.DELIVERY_INTENT,(("sink-0",1),),(contract.effects[0],))
        self.assertEqual(validate_observation(contract,observed)[:2],
                         ("tier execution intent drift","effective tier drift"))

    def test_visibility_is_derived_from_actual_effect_multiset(self):
        contract=intent(SinkTier.T,requested=SinkTier.T)
        cases=(
            TierObservation(contract.digest,SinkTier.T,Visibility.COMPLETE,
                EffectAuthority.COMMIT,(("sink-0",1),),()),
            TierObservation(contract.digest,SinkTier.T,Visibility.NONE,
                EffectAuthority.COMMIT,(("sink-0",1),),(contract.effects[0],)),
            TierObservation(contract.digest,SinkTier.T,Visibility.UNCERTAIN,
                EffectAuthority.COMMIT,(("sink-0",1),),()),
        )
        for observed in cases:
            with self.subTest(visibility=observed.visibility):
                self.assertTrue(validate_observation(contract,observed))
        valid_unknown=replace(cases[-1],unresolved=("sink-0",))
        self.assertEqual(validate_observation(contract,valid_unknown),())

    def test_observation_structure_is_exact_and_does_not_collapse_duplicates(self):
        contract=intent(SinkTier.I,requested=SinkTier.I)
        malformed=(
            TierObservation(contract.digest,SinkTier.I,Visibility.NONE,
                EffectAuthority.NONE,(),()),
            TierObservation(contract.digest,SinkTier.I,Visibility.NONE,
                EffectAuthority.DELIVERY_INTENT,(("sink-0",1),("sink-0",0)),()),
            TierObservation(contract.digest,SinkTier.I,Visibility.NONE,
                EffectAuthority.DELIVERY_INTENT,(("sink-0",-1),),()),
            TierObservation(contract.digest,SinkTier.I,Visibility.COMPLETE,
                EffectAuthority.DELIVERY_INTENT,(("sink-0",1),),( ("sink-0","wrong","payload"),)),
        )
        for observed in malformed:
            with self.subTest(observed=observed):
                self.assertTrue(validate_observation(contract,observed))

    def test_b_policy_rejects_ambiguous_runtime_types(self):
        for args in ((True,True,Exhaustion.BLOCK),(1,1,Exhaustion.BLOCK),(1,True,"block")):
            with self.subTest(args=args),self.assertRaises(TierContractError):
                BPolicy(*args)

    def test_gapped_requires_nonempty_known_disjoint_gap_set(self):
        contract=intent(SinkTier.B,requested=SinkTier.B,
                        policy=BPolicy(1,False,Exhaustion.ADVANCE_WITH_GAP))
        for gaps in ((),("ghost",),("sink-0","sink-0")):
            observed=TierObservation(contract.digest,SinkTier.B,Visibility.GAPPED,
                EffectAuthority.DELIVERY_INTENT,(("sink-0",1),),(),gaps=gaps)
            self.assertTrue(validate_observation(contract,observed))

    def test_effects_and_unresolved_require_attempts_and_exact_remainder(self):
        contract=intent(SinkTier.T,SinkTier.T,requested=SinkTier.T)
        zero=TierObservation(contract.digest,SinkTier.T,Visibility.COMPLETE,
            EffectAuthority.COMMIT,(("sink-0",0),("sink-1",0)),contract.effects)
        self.assertIn("visible or unresolved sink lacks an external attempt",
                      validate_observation(contract,zero))
        partial=TierObservation(contract.digest,SinkTier.T,Visibility.PARTIAL,
            EffectAuthority.COMMIT,(("sink-0",1),("sink-1",0)),
            (contract.effects[0],),unresolved=("sink-0",))
        self.assertIn("PARTIAL unresolved set differs from missing effects",
                      validate_observation(contract,partial))

    def test_gapped_requires_exhausted_total_attempt_budget(self):
        contract=intent(SinkTier.B,requested=SinkTier.B,
                        policy=BPolicy(3,False,Exhaustion.ADVANCE_WITH_GAP))
        observed=TierObservation(contract.digest,SinkTier.B,Visibility.GAPPED,
            EffectAuthority.DELIVERY_INTENT,(("sink-0",2),),(),gaps=("sink-0",))
        self.assertIn("GAPPED sink has not exhausted retry budget",
                      validate_observation(contract,observed))
        exhausted=replace(observed,per_sink_attempts=(("sink-0",3),))
        self.assertEqual(validate_observation(contract,exhausted),())

    def test_duplicate_effect_key_and_hostile_unhashable_values_are_rejected(self):
        with self.assertRaisesRegex(TierContractError,"duplicate effect"):
            TierExecutionIntent("tx",SinkTier.T,SinkTier.T,(("s",SinkTier.T),),
                "cap",(("s","key","one"),("s","key","two")))
        with self.assertRaises(TierContractError):
            TierExecutionIntent("tx",SinkTier.T,SinkTier.T,[["s",SinkTier.T]],
                "cap",(("s","key","one"),))
        contract=intent(SinkTier.T,requested=SinkTier.T)
        hostile=TierObservation(contract.digest,SinkTier.T,Visibility.COMPLETE,
            EffectAuthority.COMMIT,(("sink-0",1),),(("sink-0",[],"payload"),))
        self.assertIn("actual effect identity is invalid",
                      validate_observation(contract,hostile))

    def test_gapped_requires_complete_delivered_effects_and_budget_ceiling(self):
        contract=TierExecutionIntent("tx",SinkTier.B,SinkTier.B,
            (("s0",SinkTier.B),("s1",SinkTier.B)),"cap",
            (("s0","k0","p0"),("s0","k1","p1"),("s1","k2","p2")),
            BPolicy(3,True,Exhaustion.ADVANCE_WITH_GAP))
        incomplete=TierObservation(contract.digest,SinkTier.B,Visibility.GAPPED,
            EffectAuthority.DELIVERY_INTENT,(("s0",1),("s1",3)),
            (("s0","k0","p0"),),gaps=("s1",))
        self.assertIn("GAPPED delivered effects are incomplete",
                      validate_observation(contract,incomplete))
        over_budget=replace(incomplete,per_sink_attempts=(("s0",1),("s1",4)),
                            actual_effects=contract.effects[:2])
        self.assertIn("B attempts exceed frozen retry budget",
                      validate_observation(contract,over_budget))

    def test_invalid_policy_and_hostile_identifiers_fail_cleanly(self):
        with self.assertRaises(TierContractError):
            TierExecutionIntent("tx",SinkTier.B,SinkTier.B,(("s",SinkTier.B),),
                "cap",(("s","k","p"),),"not-policy")
        for participants in ((([],SinkTier.T),),((1,SinkTier.T),("s",SinkTier.T))):
            with self.subTest(participants=participants),self.assertRaises(TierContractError):
                TierExecutionIntent("tx",SinkTier.T,SinkTier.T,participants,
                    "cap",(("s","k","p"),))
        contract=intent(SinkTier.T,requested=SinkTier.T)
        hostile=TierObservation(contract.digest,SinkTier.T,Visibility.NONE,
            EffectAuthority.NONE,(([],0),),())
        self.assertIn("attempt observation must exactly cover sink manifest",
                      validate_observation(contract,hostile))

    def test_gap_cannot_be_hidden_and_observation_containers_are_strict(self):
        contract=intent(SinkTier.B,requested=SinkTier.B,
                        policy=BPolicy(2,False,Exhaustion.ADVANCE_WITH_GAP))
        hidden=TierObservation(contract.digest,SinkTier.B,Visibility.NONE,
            EffectAuthority.DELIVERY_INTENT,(("sink-0",2),),(),gaps=("sink-0",))
        self.assertIn("gap evidence requires GAPPED visibility",
                      validate_observation(contract,hidden))
        for field,value in (("per_sink_attempts",None),("actual_effects",None),
                            ("unresolved",None),("gaps",None),
                            ("unresolved","sink-0"),("gaps","sink-0")):
            with self.subTest(field=field):
                hostile=replace(hidden,**{field:value})
                self.assertTrue(validate_observation(contract,hostile))


if __name__=="__main__": unittest.main()
