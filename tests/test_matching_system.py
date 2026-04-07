from __future__ import annotations

import tempfile
from pathlib import Path

from ngo_matching.matcher import MatchingEngine
from ngo_matching.models import MatchingPolicy, Participant
from ngo_matching.storage import DataStore


def _participant(
    name: str,
    age: int,
    gender: str,
    experience: bool,
    ethnicity: str,
    culture: str,
    emory: bool = True,
) -> Participant:
    return Participant.from_signup(
        name=name,
        age=age,
        is_emory_student=emory,
        gender=gender,
        attendance_experience=experience,
        ethnicity=ethnicity,
        culture=culture,
    )


def test_policy_update_requires_controller_secret() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path, controller_secret="abc123")

        ok = store.set_policy(
            controller_secret="abc123",
            updates={"max_age_gap": 3, "strict_age_gap": True},
        )
        assert ok is True
        assert store.get_policy().max_age_gap == 3

        rejected = store.set_policy(
            controller_secret="wrong-secret",
            updates={"max_age_gap": 10},
        )
        assert rejected is False
        assert store.get_policy().max_age_gap == 3


def test_matching_prefers_diversity_and_experience_mix() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path, controller_secret="secret")
        engine = MatchingEngine(store)

        participants = [
            _participant("Alex", 22, "male", False, "Asian", "Chinese"),
            _participant("Jordan", 23, "female", True, "Latino", "Mexican"),
            _participant("Riley", 22, "female", False, "White", "American"),
            _participant("Sam", 21, "male", True, "Black", "Nigerian"),
        ]
        for p in participants:
            store.add_participant(p)

        matches, unmatched = engine.run_round()
        assert len(matches) == 2
        assert len(unmatched) == 0

        for m in matches:
            # Ensure no same-experience pair with default policy.
            assert (
                m.participant_one.attendance_experience
                != m.participant_two.attendance_experience
            )


def test_prevent_previous_rematch() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path, controller_secret="secret")
        engine = MatchingEngine(store)

        p1 = _participant("A", 25, "male", False, "Asian", "Chinese")
        p2 = _participant("B", 25, "female", True, "White", "American")
        p3 = _participant("C", 25, "male", False, "Latino", "Mexican")
        p4 = _participant("D", 25, "female", True, "Black", "Nigerian")

        for p in (p1, p2, p3, p4):
            store.add_participant(p)

        # Record an old pairing to block future rematch.
        store.record_round(
            [
                (
                    p1.participant_id,
                    p2.participant_id,
                    99.0,
                    "legacy",
                )
            ]
        )

        matches, _ = engine.run_round()
        pair_ids = {tuple(sorted((m.participant_one.participant_id, m.participant_two.participant_id))) for m in matches}

        assert tuple(sorted((p1.participant_id, p2.participant_id))) not in pair_ids


def test_strict_age_gap_blocks_far_ages() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path, controller_secret="secret")
        policy = MatchingPolicy(max_age_gap=2, strict_age_gap=True)
        store.set_policy("secret", policy.to_dict())
        engine = MatchingEngine(store)

        p1 = _participant("Young", 18, "male", False, "Asian", "Chinese")
        p2 = _participant("Older", 30, "female", True, "White", "American")
        store.add_participant(p1)
        store.add_participant(p2)

        matches, unmatched = engine.run_round()
        assert len(matches) == 0
        assert len(unmatched) == 2


def test_single_controller_only_first_registration_wins() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)

        first = store.set_controller("first-secret")
        second = store.set_controller("second-secret")

        assert first is True
        assert second is False
