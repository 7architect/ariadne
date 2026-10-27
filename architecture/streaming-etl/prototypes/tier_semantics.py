"""ST-034a executable sink-tier contract and observation vocabulary."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from integrated_protocol import TIER_RANK, SinkTier


class TierContractError(RuntimeError):
    pass


class Visibility(str, Enum):
    NONE = "NONE"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"
    UNCERTAIN = "UNCERTAIN"
    GAPPED = "GAPPED"


class Exhaustion(str, Enum):
    BLOCK = "block"
    ADVANCE_WITH_GAP = "advance_with_gap"


class EffectAuthority(str, Enum):
    NONE = "NONE"
    DELIVERY_INTENT = "DELIVERY_INTENT"
    COMMIT = "COMMIT"


@dataclass(frozen=True)
class BPolicy:
    retry_budget: int
    retry_unknown: bool
    exhaustion: Exhaustion

    def __post_init__(self) -> None:
        if type(self.retry_budget) is not int or self.retry_budget < 1:
            raise TierContractError("B retry budget must be positive")
        if type(self.retry_unknown) is not bool:
            raise TierContractError("B retry_unknown must be boolean")
        if not isinstance(self.exhaustion, Exhaustion):
            raise TierContractError("B exhaustion policy is invalid")


@dataclass(frozen=True)
class TierGuaranteeV1:
    tier: SinkTier
    prepare_required: bool
    stable_effect_key_required: bool
    rollback_before_decision: bool
    conditional_convergence: bool
    duplicates_possible: bool
    gaps_possible: bool
    allowed_visibility: frozenset[Visibility]


GUARANTEES = {
    SinkTier.T: TierGuaranteeV1(
        SinkTier.T,
        True,
        True,
        True,
        True,
        False,
        False,
        frozenset(
            {
                Visibility.NONE,
                Visibility.PARTIAL,
                Visibility.COMPLETE,
                Visibility.UNCERTAIN,
            }
        ),
    ),
    SinkTier.I: TierGuaranteeV1(
        SinkTier.I,
        False,
        True,
        False,
        True,
        False,
        False,
        frozenset(
            {
                Visibility.NONE,
                Visibility.PARTIAL,
                Visibility.COMPLETE,
                Visibility.UNCERTAIN,
            }
        ),
    ),
    SinkTier.B: TierGuaranteeV1(
        SinkTier.B,
        False,
        False,
        False,
        False,
        True,
        True,
        frozenset(Visibility),
    ),
}


def canonical_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True)
class TierExecutionIntent:
    transaction_id: str
    requested_tier: SinkTier
    effective_tier: SinkTier
    participant_tiers: tuple[tuple[str, SinkTier], ...]
    capability_digest: str
    effects: tuple[tuple[str, str, str], ...]
    b_policy: BPolicy | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.requested_tier, SinkTier) or not isinstance(
            self.effective_tier, SinkTier
        ):
            raise TierContractError("requested and effective tiers must be enums")
        if not all(
            isinstance(item, tuple) and len(item) == 2
            for item in self.participant_tiers
        ):
            raise TierContractError("participant tier entry is invalid")
        if not all(isinstance(item, tuple) and len(item) == 3 for item in self.effects):
            raise TierContractError("effect identity fields must be nonempty strings")
        if any(
            not isinstance(item[0], str)
            or not item[0]
            or not isinstance(item[1], SinkTier)
            for item in self.participant_tiers
        ):
            raise TierContractError("participant tier entry is invalid")
        if any(
            any(not isinstance(value, str) or not value for value in item)
            for item in self.effects
        ):
            raise TierContractError("effect identity fields must be nonempty strings")
        participants = tuple(sorted(self.participant_tiers, key=lambda item: item[0]))
        effects = tuple(sorted(self.effects))
        ids = [participant for participant, _ in participants]
        if (
            not isinstance(self.transaction_id, str)
            or not self.transaction_id
            or not isinstance(self.capability_digest, str)
            or not self.capability_digest
        ):
            raise TierContractError("transaction and capability digest are required")
        if not participants or len(ids) != len(set(ids)):
            raise TierContractError(
                "participant tier manifest must be nonempty and unique"
            )
        if not effects or {sink for sink, _, _ in effects} != set(ids):
            raise TierContractError("effect manifest must cover every sink")
        if len(effects) != len(set(effects)) or len(
            {(sink, key) for sink, key, _ in effects}
        ) != len(effects):
            raise TierContractError("duplicate effect identity")
        weakest = min((tier for _, tier in participants), key=TIER_RANK.__getitem__)
        if self.effective_tier is not weakest:
            raise TierContractError("effective tier must equal weakest participant")
        if TIER_RANK[self.effective_tier] < TIER_RANK[self.requested_tier]:
            raise TierContractError("effective tier is below requested guarantee")
        if self.effective_tier is SinkTier.B and not isinstance(self.b_policy, BPolicy):
            raise TierContractError("effective tier B requires a valid B policy")
        if self.effective_tier is not SinkTier.B and self.b_policy is not None:
            raise TierContractError("B policy is required only for effective tier B")
        object.__setattr__(self, "participant_tiers", participants)
        object.__setattr__(self, "effects", effects)

    @property
    def digest(self) -> str:
        return canonical_digest(
            {
                "transaction_id": self.transaction_id,
                "requested_tier": self.requested_tier.value,
                "effective_tier": self.effective_tier.value,
                "participant_tiers": tuple(
                    (p, t.value) for p, t in self.participant_tiers
                ),
                "capability_digest": self.capability_digest,
                "effects": self.effects,
                "b_policy": None
                if self.b_policy is None
                else {
                    "retry_budget": self.b_policy.retry_budget,
                    "retry_unknown": self.b_policy.retry_unknown,
                    "exhaustion": self.b_policy.exhaustion.value,
                },
            }
        )


@dataclass(frozen=True)
class TierObservation:
    intent_digest: str
    effective_tier: SinkTier
    visibility: Visibility
    authority: EffectAuthority
    per_sink_attempts: tuple[tuple[str, int], ...]
    actual_effects: tuple[tuple[str, str, str], ...]
    unresolved: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()

    @property
    def per_sink_effect_count(self) -> tuple[tuple[str, int], ...]:
        counts: dict[str, int] = {}
        effects = self.actual_effects if isinstance(self.actual_effects, tuple) else ()
        for effect in effects:
            if (
                isinstance(effect, (tuple, list))
                and len(effect) == 3
                and isinstance(effect[0], str)
            ):
                counts[effect[0]] = counts.get(effect[0], 0) + 1
        return tuple(sorted(counts.items()))


def validate_observation(
    intent: TierExecutionIntent, observed: TierObservation
) -> tuple[str, ...]:
    violations = []
    if observed.intent_digest != intent.digest:
        violations.append("tier execution intent drift")
    if observed.effective_tier is not intent.effective_tier:
        violations.append("effective tier drift")
    if (
        not isinstance(observed.effective_tier, SinkTier)
        or not isinstance(observed.visibility, Visibility)
        or not isinstance(observed.authority, EffectAuthority)
    ):
        return tuple(violations + ["observation tier or visibility type is invalid"])
    guarantee = GUARANTEES[intent.effective_tier]
    if observed.visibility not in guarantee.allowed_visibility:
        violations.append("visibility is forbidden for effective tier")
    expected = {sink for sink, _, _ in intent.effects}
    attempts_container = isinstance(observed.per_sink_attempts, tuple)
    actual_container = isinstance(observed.actual_effects, tuple)
    unresolved_container = isinstance(observed.unresolved, tuple)
    gaps_container = isinstance(observed.gaps, tuple)
    attempts = observed.per_sink_attempts if attempts_container else ()
    actual = observed.actual_effects if actual_container else ()
    unresolved = observed.unresolved if unresolved_container else ()
    gaps = observed.gaps if gaps_container else ()
    attempts_structural = attempts_container and all(
        isinstance(item, (tuple, list))
        and len(item) == 2
        and isinstance(item[0], str)
        and bool(item[0])
        and type(item[1]) is int
        and item[1] >= 0
        for item in attempts
    )
    attempt_ids = [item[0] for item in attempts] if attempts_structural else []
    if (
        not attempts_structural
        or len(attempt_ids) != len(set(attempt_ids))
        or set(attempt_ids) != expected
    ):
        violations.append("attempt observation must exactly cover sink manifest")
    unresolved_valid = unresolved_container and all(
        isinstance(value, str) and value for value in unresolved
    )
    gaps_valid = gaps_container and all(
        isinstance(value, str) and value for value in gaps
    )
    unresolved_set = set(unresolved) if unresolved_valid else set()
    gaps_set = set(gaps) if gaps_valid else set()
    if (
        not unresolved_valid
        or len(unresolved) != len(unresolved_set)
        or not unresolved_set <= expected
    ):
        violations.append("unresolved participants are invalid")
    if not gaps_valid or len(gaps) != len(gaps_set) or not gaps_set <= expected:
        violations.append("gap participants are invalid")
    if unresolved_set & gaps_set:
        violations.append("unresolved and gap participants overlap")
    actual_structural = actual_container and all(
        isinstance(effect, (tuple, list))
        and len(effect) == 3
        and all(isinstance(value, str) and value for value in effect)
        for effect in actual
    )
    if not actual_structural:
        violations.append("actual effect identity is invalid")
    valid_actual = tuple(
        tuple(effect)
        for effect in actual
        if isinstance(effect, (tuple, list))
        and len(effect) == 3
        and all(isinstance(value, str) and value for value in effect)
    )
    actual_set = set(valid_actual)
    expected_effects = set(intent.effects)
    actual_keys = [(sink, key) for sink, key, _ in valid_actual]
    if intent.effective_tier in {SinkTier.T, SinkTier.I}:
        if not actual_set <= expected_effects:
            violations.append("effect key or payload differs from intent")
        if len(actual_keys) != len(set(actual_keys)):
            violations.append("logical duplicate violates tier guarantee")
    elif not actual_set <= expected_effects:
        violations.append("effect key or payload differs from intent")
    has_activity = bool(actual or unresolved or gaps) or any(
        isinstance(item, (tuple, list))
        and len(item) == 2
        and type(item[1]) is int
        and item[1] > 0
        for item in attempts
    )
    if intent.effective_tier is SinkTier.T:
        if observed.authority is EffectAuthority.DELIVERY_INTENT:
            violations.append("tier T cannot use delivery intent as effect authority")
        if (
            actual
            or observed.visibility
            in {Visibility.PARTIAL, Visibility.COMPLETE, Visibility.UNCERTAIN}
        ) and observed.authority is not EffectAuthority.COMMIT:
            violations.append("tier T visibility requires COMMIT authority")
    else:
        if observed.authority is EffectAuthority.COMMIT:
            violations.append("tier I/B cannot claim COMMIT authority")
        if has_activity and observed.authority is not EffectAuthority.DELIVERY_INTENT:
            violations.append("tier I/B activity requires durable delivery intent")
    attempts_by_sink = {
        item[0]: item[1]
        for item in attempts
        if isinstance(item, (tuple, list))
        and len(item) == 2
        and isinstance(item[0], str)
        and type(item[1]) is int
    }
    active_sinks = {sink for sink, _, _ in valid_actual} | unresolved_set
    if any(attempts_by_sink.get(sink, 0) < 1 for sink in active_sinks):
        violations.append("visible or unresolved sink lacks an external attempt")
    if (
        intent.effective_tier is SinkTier.B
        and intent.b_policy is not None
        and any(
            count > intent.b_policy.retry_budget for count in attempts_by_sink.values()
        )
    ):
        violations.append("B attempts exceed frozen retry budget")
    if gaps and intent.effective_tier is not SinkTier.B:
        violations.append("gap violates tier guarantee")
    if gaps and (
        intent.b_policy is None
        or intent.b_policy.exhaustion is not Exhaustion.ADVANCE_WITH_GAP
    ):
        violations.append("gap lacks frozen lossy opt-in")
    if observed.visibility is Visibility.NONE and actual:
        violations.append("NONE visibility has effects")
    if observed.visibility is Visibility.PARTIAL and not (
        actual and actual_set < expected_effects and unresolved_set
    ):
        violations.append(
            "PARTIAL visibility requires effects and unresolved remainder"
        )
    if observed.visibility is Visibility.PARTIAL:
        missing_sinks = {sink for sink, _, _ in expected_effects - actual_set}
        if unresolved_set != missing_sinks:
            violations.append("PARTIAL unresolved set differs from missing effects")
    if observed.visibility is Visibility.COMPLETE and not (
        actual_set == expected_effects and not unresolved and not gaps
    ):
        violations.append("COMPLETE visibility differs from exact effect manifest")
    if observed.visibility is Visibility.UNCERTAIN and not unresolved:
        violations.append("UNCERTAIN visibility requires unresolved participant")
    if gaps and observed.visibility is not Visibility.GAPPED:
        violations.append("gap evidence requires GAPPED visibility")
    if observed.visibility is Visibility.GAPPED:
        effect_sinks = {sink for sink, _, _ in valid_actual}
        if not gaps or effect_sinks | gaps_set != expected or effect_sinks & gaps_set:
            violations.append(
                "GAPPED visibility must partition delivered sinks and gaps"
            )
        delivered_expected = {
            effect for effect in expected_effects if effect[0] not in gaps_set
        }
        if actual_set != delivered_expected:
            violations.append("GAPPED delivered effects are incomplete")
        if intent.b_policy is not None and any(
            attempts_by_sink.get(sink, 0) != intent.b_policy.retry_budget
            for sink in gaps_set
        ):
            violations.append("GAPPED sink has not exhausted retry budget")
    return tuple(violations)
