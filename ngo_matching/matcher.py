from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from ngo_matching.models import MatchingPolicy, Participant
from ngo_matching.storage import DataStore


@dataclass(frozen=True)
class MatchResult:
    participant_one: Participant
    participant_two: Participant
    score: float
    reasons: List[str]


@dataclass(frozen=True)
class MatchRoundResult:
    pairs: List[MatchResult]
    unmatched: List[Participant]


def _pair_key(a_id: str, b_id: str) -> Tuple[str, str]:
    return tuple(sorted((a_id, b_id)))


def _score_pair(a: Participant, b: Participant, policy: MatchingPolicy) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    score = 0.0

    age_gap = abs(a.age - b.age)
    if policy.strict_age_gap and age_gap > policy.max_age_gap:
        return float("-inf"), ["age gap above strict max"]
    age_points = max(policy.max_age_gap - age_gap, 0) / max(policy.max_age_gap, 1)
    score += age_points * policy.age_weight
    reasons.append(f"age gap {age_gap}")

    if a.attendance_experience != b.attendance_experience:
        score += policy.experience_mix_weight
        reasons.append("experience mixed")
    elif policy.require_experience_mix:
        return float("-inf"), ["experience must be mixed"]

    if a.ethnicity != b.ethnicity:
        if policy.prefer_different_ethnicity:
            score += policy.ethnicity_weight
        reasons.append("different ethnicity")
    elif policy.strict_diversity and policy.prefer_different_ethnicity:
        return float("-inf"), ["ethnicity must be different"]

    if a.culture != b.culture:
        if policy.prefer_different_culture:
            score += policy.culture_weight
        reasons.append("different culture")
    elif policy.strict_diversity and policy.prefer_different_culture:
        return float("-inf"), ["culture must be different"]

    if a.gender != b.gender:
        if policy.prefer_different_gender:
            score += policy.gender_weight
        reasons.append("different gender")
    elif policy.strict_diversity and policy.prefer_different_gender:
        return float("-inf"), ["gender must be different"]

    return score, reasons


def create_matches(
    participants: Sequence[Participant],
    policy: MatchingPolicy,
    previous_pairs: Set[Tuple[str, str]],
) -> Tuple[List[MatchResult], List[Participant]]:
    """
    Greedy maximum-score matching with rematch prevention.
    """
    scored_pairs: List[Tuple[float, Participant, Participant, List[str]]] = []
    by_id: Dict[str, Participant] = {p.participant_id: p for p in participants}

    ids = list(by_id.keys())
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            p1 = by_id[ids[i]]
            p2 = by_id[ids[j]]
            pair = _pair_key(p1.participant_id, p2.participant_id)
            if pair in previous_pairs:
                continue

            score, reasons = _score_pair(p1, p2, policy)
            if score == float("-inf"):
                continue
            scored_pairs.append((score, p1, p2, reasons))

    scored_pairs.sort(key=lambda row: row[0], reverse=True)

    matched_ids: Set[str] = set()
    results: List[MatchResult] = []

    for score, p1, p2, reasons in scored_pairs:
        if p1.participant_id in matched_ids or p2.participant_id in matched_ids:
            continue
        matched_ids.add(p1.participant_id)
        matched_ids.add(p2.participant_id)
        results.append(
            MatchResult(
                participant_one=p1,
                participant_two=p2,
                score=round(score, 3),
                reasons=reasons,
            )
        )

    unmatched = [p for p in participants if p.participant_id not in matched_ids]
    return results, unmatched


class MatchingEngine:
    def __init__(
        self,
        store: Optional[DataStore] = None,
        *,
        policy: Optional[MatchingPolicy] = None,
        prior_pairs: Optional[Set[Tuple[str, str]]] = None,
    ) -> None:
        self.store = store
        self.policy = policy
        self.prior_pairs = prior_pairs or set()

    def match(self, participants: Sequence[Participant]) -> MatchRoundResult:
        policy = self.policy or MatchingPolicy()
        pairs, unmatched = create_matches(participants, policy, self.prior_pairs)
        return MatchRoundResult(pairs=pairs, unmatched=unmatched)

    def run_round(self) -> Tuple[List[MatchResult], List[Participant]]:
        if self.store is None:
            raise ValueError("run_round requires a DataStore-backed engine")

        participants = self.store.list_participants()
        policy = self.store.get_policy()
        prior_pairs = self.store.get_prior_pair_set()
        pairs, unmatched = create_matches(participants, policy, prior_pairs)
        round_payload = [
            (
                pair.participant_one.participant_id,
                pair.participant_two.participant_id,
                pair.score,
                "; ".join(pair.reasons),
            )
            for pair in pairs
        ]
        self.store.record_round(round_payload)
        return pairs, unmatched
