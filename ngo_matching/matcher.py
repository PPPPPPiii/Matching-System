from __future__ import annotations

from itertools import combinations
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from ngo_matching.models import MatchingPolicy, Participant
from ngo_matching.storage import DataStore


@dataclass(frozen=True)
class MatchGroup:
    participants: Tuple[Participant, ...]
    score: float
    reasons: List[str]
    strictness_level: int
    used_rematch: bool


@dataclass(frozen=True)
class MatchRoundResult:
    groups: List[MatchGroup]
    unmatched: List[Participant]
    strictness_level: int
    used_rematch: bool


def _pair_key(a_id: str, b_id: str) -> tuple[str, str]:
    return tuple(sorted((a_id, b_id)))


_STRICT_PRIORITY = [
    "attendance_experience",  # most strict
    "culture",
    "ethnicity",
    "age",
    "gender",
    "is_emory_student",  # least strict
]


def _active_required_dimensions(strictness_level: int) -> set[str]:
    # Level 0: all required, level 1 drops least strict, ..., level 6 drops all.
    keep_count = max(len(_STRICT_PRIORITY) - strictness_level, 0)
    return set(_STRICT_PRIORITY[:keep_count])


def _evaluate_pair(
    a: Participant,
    b: Participant,
    *,
    policy: MatchingPolicy,
    strictness_level: int,
    allow_rematch: bool,
    pair_history_count: int,
) -> tuple[float, list[str]]:
    required = _active_required_dimensions(strictness_level)
    reasons: list[str] = []

    attendance_mixed = a.attendance_experience != b.attendance_experience
    culture_diff = a.culture != b.culture
    ethnicity_diff = a.ethnicity != b.ethnicity
    age_gap = abs(a.age - b.age)
    age_ok = age_gap <= policy.max_age_gap
    gender_diff = a.gender != b.gender
    emory_diff = a.is_emory_student != b.is_emory_student

    checks = {
        "attendance_experience": attendance_mixed,
        "culture": culture_diff,
        "ethnicity": ethnicity_diff,
        "age": age_ok,
        "gender": gender_diff,
        "is_emory_student": emory_diff,
    }
    for key in required:
        if not checks[key]:
            return float("-inf"), [f"failed required dimension: {key}"]

    if pair_history_count > 0 and not allow_rematch:
        return float("-inf"), ["rematch blocked"]

    # Score as weighted preference, not a hard constraint.
    score = 0.0
    if attendance_mixed:
        score += policy.experience_mix_weight
        reasons.append("mixed experience")
    else:
        score -= policy.experience_mix_weight * 0.4
        reasons.append("same experience")

    if culture_diff:
        score += policy.culture_weight
        reasons.append("different culture")
    if ethnicity_diff:
        score += policy.ethnicity_weight
        reasons.append("different ethnicity")
    if gender_diff:
        score += policy.gender_weight
        reasons.append("different gender")
    if emory_diff:
        score += 0.5
        reasons.append("different emory status")

    age_points = max(policy.max_age_gap - age_gap, 0) / max(policy.max_age_gap, 1)
    score += age_points * policy.age_weight
    reasons.append(f"age gap {age_gap}")

    # Prefer people who have not talked before. This is soft because rematches
    # are allowed only when no non-rematch full solution exists.
    if pair_history_count == 0:
        score += 3.0
        reasons.append("new partner")
    else:
        score -= 50.0 * pair_history_count
        reasons.append(f"rematch x{pair_history_count}")

    return score, reasons


def _greedy_groups_for_level(
    participants: Sequence[Participant],
    *,
    policy: MatchingPolicy,
    strictness_level: int,
    allow_rematch: bool,
    pair_history_counts: Dict[tuple[str, str], int],
) -> MatchRoundResult:
    by_id: Dict[str, Participant] = {p.participant_id: p for p in participants}
    ids = list(by_id.keys())
    pair_scores: Dict[tuple[str, str], tuple[float, list[str], int]] = {}
    scored_pairs: list[tuple[float, str, str, list[str], int]] = []

    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            p1 = by_id[ids[i]]
            p2 = by_id[ids[j]]
            key = _pair_key(p1.participant_id, p2.participant_id)
            history_count = pair_history_counts.get(key, 0)
            score, reasons = _evaluate_pair(
                p1,
                p2,
                policy=policy,
                strictness_level=strictness_level,
                allow_rematch=allow_rematch,
                pair_history_count=history_count,
            )
            if score == float("-inf"):
                continue
            row = (score, p1.participant_id, p2.participant_id, reasons, history_count)
            pair_scores[key] = (score, reasons, history_count)
            scored_pairs.append(row)

    scored_pairs.sort(key=lambda row: row[0], reverse=True)
    used_ids: set[str] = set()
    groups: list[MatchGroup] = []

    for score, a_id, b_id, reasons, history_count in scored_pairs:
        if a_id in used_ids or b_id in used_ids:
            continue
        used_ids.add(a_id)
        used_ids.add(b_id)
        groups.append(
            MatchGroup(
                participants=(by_id[a_id], by_id[b_id]),
                score=round(score, 3),
                reasons=reasons,
                strictness_level=strictness_level,
                used_rematch=history_count > 0,
            )
        )

    unmatched_ids = [pid for pid in ids if pid not in used_ids]

    # Odd participant count: create one triad by attaching leftover person to
    # the most compatible existing pair.
    if len(unmatched_ids) == 1 and groups:
        extra_id = unmatched_ids[0]
        best_idx = -1
        best_score = float("-inf")
        best_reason = ""
        best_used_rematch = False

        for idx, group in enumerate(groups):
            a_id = group.participants[0].participant_id
            b_id = group.participants[1].participant_id
            key1 = _pair_key(extra_id, a_id)
            key2 = _pair_key(extra_id, b_id)
            if key1 not in pair_scores or key2 not in pair_scores:
                continue
            extra_score = pair_scores[key1][0] + pair_scores[key2][0]
            triad_score = group.score + extra_score
            if triad_score > best_score:
                best_score = triad_score
                best_idx = idx
                best_used_rematch = (
                    group.used_rematch
                    or pair_scores[key1][2] > 0
                    or pair_scores[key2][2] > 0
                )
                best_reason = (
                    f"triad formed; {pair_scores[key1][1][0]}; "
                    f"{pair_scores[key2][1][0]}"
                )

        if best_idx >= 0:
            original = groups[best_idx]
            triad_people = (
                original.participants[0],
                original.participants[1],
                by_id[extra_id],
            )
            groups[best_idx] = MatchGroup(
                participants=triad_people,
                score=round(best_score, 3),
                reasons=original.reasons + [best_reason],
                strictness_level=strictness_level,
                used_rematch=best_used_rematch,
            )
            unmatched_ids = []

    unmatched = [by_id[pid] for pid in unmatched_ids]
    return MatchRoundResult(
        groups=groups,
        unmatched=unmatched,
        strictness_level=strictness_level,
        used_rematch=allow_rematch,
    )


def create_matches(
    participants: Sequence[Participant],
    policy: MatchingPolicy,
    pair_history_counts: Dict[tuple[str, str], int],
) -> MatchRoundResult:
    if len(participants) < 2:
        return MatchRoundResult(
            groups=[],
            unmatched=list(participants),
            strictness_level=0,
            used_rematch=False,
        )

    best_attempt: Optional[MatchRoundResult] = None
    # Rematch avoidance has higher priority than experience preference.
    for allow_rematch in (False, True):
        for strictness_level in range(0, len(_STRICT_PRIORITY) + 1):
            result = _greedy_groups_for_level(
                participants,
                policy=policy,
                strictness_level=strictness_level,
                allow_rematch=allow_rematch,
                pair_history_counts=pair_history_counts,
            )
            if (
                best_attempt is None
                or len(result.unmatched) < len(best_attempt.unmatched)
                or (
                    len(result.unmatched) == len(best_attempt.unmatched)
                    and sum(g.score for g in result.groups)
                    > sum(g.score for g in best_attempt.groups)
                )
            ):
                best_attempt = result

            if not result.unmatched:
                return result

    # Fallback should always exist for >=2 participants, but return best attempt.
    return best_attempt if best_attempt is not None else MatchRoundResult([], list(participants), 0, True)


class MatchingEngine:
    def __init__(
        self,
        store: Optional[DataStore] = None,
        *,
        policy: Optional[MatchingPolicy] = None,
        pair_history_counts: Optional[Dict[tuple[str, str], int]] = None,
    ) -> None:
        self.store = store
        self.policy = policy
        self.pair_history_counts = pair_history_counts or {}

    def match(self, participants: Sequence[Participant]) -> MatchRoundResult:
        policy = self.policy or MatchingPolicy()
        return create_matches(participants, policy, self.pair_history_counts)

    def run_round(self) -> MatchRoundResult:
        if self.store is None:
            raise ValueError("run_round requires a DataStore-backed engine")

        participants = self.store.list_participants()
        policy = self.store.get_policy()
        pair_history_counts = self.store.get_pair_match_counts()
        result = create_matches(participants, policy, pair_history_counts)

        round_payload = [
            (
                group.participants[i].participant_id,
                group.participants[j].participant_id,
                group.score,
                "; ".join(group.reasons),
            )
            for group in result.groups
            for i, j in combinations(range(len(group.participants)), 2)
        ]
        self.store.record_round(round_payload)
        return result
