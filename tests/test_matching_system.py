from __future__ import annotations

import tempfile
from pathlib import Path

from ngo_matching.google_forms import (
    build_public_csv_url,
    parse_google_form_rows,
)
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

        result = engine.run_round()
        assert len(result.groups) == 2
        assert len(result.unmatched) == 0

        for g in result.groups:
            assert len(g.participants) == 2
            # Ensure no same-experience pair with default policy.
            assert (
                g.participants[0].attendance_experience
                != g.participants[1].attendance_experience
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

        result = engine.run_round()
        pair_ids = {
            tuple(
                sorted(
                    (
                        g.participants[0].participant_id,
                        g.participants[1].participant_id,
                    )
                )
            )
            for g in result.groups
            if len(g.participants) == 2
        }

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

        result = engine.run_round()
        assert len(result.groups) == 1
        assert len(result.groups[0].participants) == 2
        assert len(result.unmatched) == 0
        # Should have relaxed to allow age mismatch when no strict candidate.
        assert result.strictness_level >= 3


def test_single_controller_only_first_registration_wins() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)

        first = store.set_controller("first-secret")
        second = store.set_controller("second-secret")

        assert first is True
        assert second is False


def test_google_forms_csv_url_builder() -> None:
    sheet_url = (
        "https://docs.google.com/spreadsheets/d/"
        "abcDEF1234567890/edit?gid=987654321#gid=987654321"
    )
    csv_url = build_public_csv_url(sheet_url)
    assert (
        csv_url
        == "https://docs.google.com/spreadsheets/d/abcDEF1234567890/export?format=csv&gid=987654321"
    )


def test_google_form_import_is_idempotent_by_record_key() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        source = "https://docs.google.com/spreadsheets/d/abc/export?format=csv&gid=0"
        rows = [
            {
                "timestamp": "2026/04/07 10:00:00",
                "email address": "alex@example.com",
                "name": "Alex",
                "age": "22",
                "is emory student": "true",
                "gender": "male",
                "attendance experience": "false",
                "ethnicity": "Asian",
                "culture": "Korean",
            },
            {
                "timestamp": "2026/04/07 10:01:00",
                "email address": "jordan@example.com",
                "name": "Jordan",
                "age": "23",
                "is emory student": "false",
                "gender": "female",
                "attendance experience": "true",
                "ethnicity": "Latino",
                "culture": "Mexican",
            },
        ]

        parsed_once = parse_google_form_rows(source, rows)
        imported_once = 0
        for record_key, participant in parsed_once:
            if store.add_participant_from_source(
                participant, source=source, record_key=record_key
            ):
                imported_once += 1
        assert imported_once == 2

        parsed_twice = parse_google_form_rows(source, rows)
        imported_twice = 0
        for record_key, participant in parsed_twice:
            if store.add_participant_from_source(
                participant, source=source, record_key=record_key
            ):
                imported_twice += 1

        assert imported_twice == 0
        assert len(store.list_participants()) == 2


def test_odd_participants_produce_triad() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        for p in (
            _participant("A", 20, "male", False, "Asian", "Korean"),
            _participant("B", 21, "female", True, "Latino", "Mexican"),
            _participant("C", 22, "male", False, "Black", "Nigerian"),
            _participant("D", 23, "female", True, "White", "American"),
            _participant("E", 24, "male", False, "Middle Eastern", "Arab"),
        ):
            store.add_participant(p)

        result = engine.run_round()
        sizes = sorted(len(group.participants) for group in result.groups)
        assert sizes == [2, 3]
        assert len(result.unmatched) == 0


def test_rematch_only_when_necessary_overrides_experience_mix() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        p1 = _participant("P1", 21, "male", False, "Asian", "Korean")
        p2 = _participant("P2", 22, "female", False, "Latino", "Mexican")
        p3 = _participant("P3", 23, "male", True, "Black", "Nigerian")
        p4 = _participant("P4", 24, "female", True, "White", "American")
        for p in (p1, p2, p3, p4):
            store.add_participant(p)

        # Block all mixed-experience options by pre-populating history.
        # This should force the engine to relax attendance strictness and choose
        # non-rematch same-experience pairs, instead of rematching old mixed pairs.
        blocked_pairs = [
            (p1.participant_id, p3.participant_id),
            (p1.participant_id, p4.participant_id),
            (p2.participant_id, p3.participant_id),
            (p2.participant_id, p4.participant_id),
        ]
        store.record_round([(a, b, 1.0, "old") for a, b in blocked_pairs])

        result = engine.run_round()
        assert len(result.groups) == 2
        assert result.used_rematch is False
        assert result.strictness_level == 6


def test_second_round_prefers_new_people_when_possible() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        for p in (
            _participant("A", 20, "male", False, "Asian", "Korean"),
            _participant("B", 21, "female", True, "Latino", "Mexican"),
            _participant("C", 22, "male", False, "Black", "Nigerian"),
            _participant("D", 23, "female", True, "White", "American"),
        ):
            store.add_participant(p)

        round1 = engine.run_round()
        round2 = engine.run_round()

        round1_pairs = {
            tuple(sorted((g.participants[0].name, g.participants[1].name)))
            for g in round1.groups
            if len(g.participants) == 2
        }
        round2_pairs = {
            tuple(sorted((g.participants[0].name, g.participants[1].name)))
            for g in round2.groups
            if len(g.participants) == 2
        }

        assert round1_pairs.isdisjoint(round2_pairs)


def test_score_breakdown_sums_to_group_score_for_pair() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        store.add_participant(
            _participant("A", 20, "male", False, "Asian", "Korean", emory=True)
        )
        store.add_participant(
            _participant("B", 21, "female", True, "Latino", "Mexican", emory=False)
        )

        result = engine.run_round()
        assert len(result.groups) == 1
        group = result.groups[0]
        assert len(group.participants) == 2
        assert round(sum(group.score_breakdown.values()), 3) == group.score


def test_dry_run_does_not_persist_round_history() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        store.add_participant(_participant("A", 20, "male", False, "Asian", "Korean"))
        store.add_participant(_participant("B", 21, "female", True, "Latino", "Mexican"))

        counts_before = store.get_pair_match_counts()
        assert counts_before == {}

        _ = engine.run_round(persist=False)
        counts_after_dry_run = store.get_pair_match_counts()
        assert counts_after_dry_run == {}

        _ = engine.run_round(persist=True)
        counts_after_real = store.get_pair_match_counts()
        assert len(counts_after_real) == 1


def test_add_participant_overwrites_by_name_case_insensitive() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)

        first = Participant.from_signup(
            name="Alice",
            age=22,
            is_emory_student=True,
            gender="female",
            attendance_experience=False,
            ethnicity="Korean",
            culture="East Asian",
        )
        second = Participant.from_signup(
            name="aLiCe",
            age=25,
            is_emory_student=False,
            gender="female",
            attendance_experience=True,
            ethnicity="Korean",
            culture="East Asian",
        )

        r1 = store.add_participant(first)
        r2 = store.add_participant(second)

        assert r1["action"] == "created"
        assert r2["action"] == "updated"
        participants = store.list_participants()
        assert len(participants) == 1
        assert participants[0].age == 25
        assert participants[0].is_emory_student is False
        assert participants[0].attendance_experience is True


def test_participant_profile_records_each_match() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)
        engine = MatchingEngine(store)

        for p in (
            _participant("A", 20, "male", False, "Asian", "Korean"),
            _participant("B", 21, "female", True, "Latino", "Mexican"),
            _participant("C", 22, "male", False, "Black", "Nigerian"),
            _participant("D", 23, "female", True, "White", "American"),
        ):
            store.add_participant(p)

        _ = engine.run_round()
        _ = engine.run_round()

        profile = store.get_participant_profile("a")
        assert profile is not None
        history = profile["match_history"]
        assert len(history) >= 2
        # The same partner should not repeat early when alternatives exist.
        matched_names = {row["matched_with_name"] for row in history}
        assert len(matched_names) >= 2


def test_reset_matching_table_clears_current_not_history() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        secret = "secret-key"
        store = DataStore(db_path=db_path, controller_secret=secret)
        engine = MatchingEngine(store)

        store.add_participant(_participant("A", 20, "male", False, "Asian", "Korean"))
        store.add_participant(_participant("B", 21, "female", True, "Latino", "Mexican"))
        _ = engine.run_round()

        with store._connect() as conn:  # noqa: SLF001 - test-only inspection
            before_count = conn.execute(
                "SELECT COUNT(*) AS c FROM current_matching_table"
            ).fetchone()["c"]
        assert int(before_count) > 0

        ok = store.reset_matching_table(secret)
        assert ok is True

        with store._connect() as conn:  # noqa: SLF001 - test-only inspection
            after_count = conn.execute(
                "SELECT COUNT(*) AS c FROM current_matching_table"
            ).fetchone()["c"]
        assert int(after_count) == 0

        # Pair history remains for future rematch prevention.
        counts = store.get_pair_match_counts()
        assert len(counts) == 1


def test_first_last_name_identity_ignores_case_and_spacing() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        store = DataStore(db_path=db_path)

        store.add_participant(
            Participant.from_signup(
                name="Alice   Smith",
                age=20,
                is_emory_student=True,
                gender="female",
                attendance_experience=False,
                ethnicity="Korean",
                culture="East Asian",
            )
        )
        update_result = store.add_participant(
            Participant.from_signup(
                name="  ALICE smith ",
                age=25,
                is_emory_student=False,
                gender="female",
                attendance_experience=True,
                ethnicity="Korean",
                culture="East Asian",
            )
        )

        participants = store.list_participants()
        assert len(participants) == 1
        assert update_result["action"] == "updated"
        assert participants[0].name == "ALICE smith"
        assert participants[0].age == 25
        assert participants[0].attendance_experience is True


def test_cleanup_participants_deduplicates_by_first_last_name() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = str(Path(temp_dir) / "matching.sqlite")
        secret = "cleanup-secret"
        store = DataStore(db_path=db_path, controller_secret=secret)

        p_old = Participant.from_signup(
            name="Alice Smith",
            age=20,
            is_emory_student=True,
            gender="female",
            attendance_experience=False,
            ethnicity="Korean",
            culture="East Asian",
        )
        p_new = Participant.from_signup(
            name="ALICE   smith",
            age=22,
            is_emory_student=False,
            gender="female",
            attendance_experience=True,
            ethnicity="Korean",
            culture="East Asian",
        )
        p_other = Participant.from_signup(
            name="Bob Jones",
            age=23,
            is_emory_student=True,
            gender="male",
            attendance_experience=False,
            ethnicity="Chinese",
            culture="East Asian",
        )

        with store._connect() as conn:  # noqa: SLF001 - test fixture setup
            conn.execute(
                """
                INSERT INTO participants (
                    participant_id, name, name_key, age, is_emory_student, gender,
                    attendance_experience, ethnicity, culture, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p_old.participant_id,
                    p_old.name,
                    "alicesmith",
                    p_old.age,
                    int(p_old.is_emory_student),
                    p_old.gender,
                    int(p_old.attendance_experience),
                    p_old.ethnicity,
                    p_old.culture,
                    p_old.created_at,
                ),
            )
            conn.execute(
                """
                INSERT INTO participants (
                    participant_id, name, name_key, age, is_emory_student, gender,
                    attendance_experience, ethnicity, culture, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p_new.participant_id,
                    p_new.name,
                    "alicesmith",
                    p_new.age,
                    int(p_new.is_emory_student),
                    p_new.gender,
                    int(p_new.attendance_experience),
                    p_new.ethnicity,
                    p_new.culture,
                    p_new.created_at,
                ),
            )
            conn.execute(
                """
                INSERT INTO participants (
                    participant_id, name, name_key, age, is_emory_student, gender,
                    attendance_experience, ethnicity, culture, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p_other.participant_id,
                    p_other.name,
                    "bobjones",
                    p_other.age,
                    int(p_other.is_emory_student),
                    p_other.gender,
                    int(p_other.attendance_experience),
                    p_other.ethnicity,
                    p_other.culture,
                    p_other.created_at,
                ),
            )
            conn.execute(
                """
                INSERT INTO ingestion_records (source, record_key, participant_id, imported_at)
                VALUES (?, ?, ?, ?)
                """,
                ("source", "row1", p_old.participant_id, p_old.created_at),
            )

        summary = store.cleanup_duplicate_participants(secret)
        assert summary["ok"] is True
        assert summary["deleted_duplicate_participants"] == 1

        participants = store.list_participants()
        names = sorted(p.name for p in participants)
        assert names == ["ALICE smith", "Bob Jones"]

        with store._connect() as conn:  # noqa: SLF001 - test-only inspection
            row = conn.execute(
                "SELECT participant_id FROM ingestion_records WHERE source = ? AND record_key = ?",
                ("source", "row1"),
            ).fetchone()
        assert row is not None
        assert row["participant_id"] == p_new.participant_id
