from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .models import MatchingPolicy, Participant


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pair_tuple(a: str, b: str) -> Tuple[str, str]:
    return tuple(sorted((a, b)))


def _pair_key(a: str, b: str) -> str:
    x, y = _pair_tuple(a, b)
    return f"{x}::{y}"


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _name_key(name: str) -> str:
    return " ".join(name.strip().lower().split())


class DataStore:
    def __init__(
        self,
        *,
        db_path: str,
        controller_secret: Optional[str] = None,
        controller_id: str = "controller",
        events_path: Optional[str] = None,
    ) -> None:
        self.db_path = Path(db_path)
        self.events_path = (
            Path(events_path) if events_path else self.db_path.with_suffix(".events.jsonl")
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

        if controller_secret and not self._has_controller():
            self.set_controller(controller_key=controller_secret, controller_id=controller_id)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS participants (
                    participant_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL,
                    is_emory_student INTEGER NOT NULL,
                    gender TEXT NOT NULL,
                    attendance_experience INTEGER NOT NULL,
                    ethnicity TEXT NOT NULL,
                    culture TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS controller_access (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    controller_id TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS matching_policy (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    policy_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    updated_by TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS match_rounds (
                    round_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS match_pairs (
                    round_id INTEGER NOT NULL,
                    participant_a TEXT NOT NULL,
                    participant_b TEXT NOT NULL,
                    score REAL NOT NULL,
                    rationale TEXT NOT NULL,
                    PRIMARY KEY (round_id, participant_a, participant_b)
                );

                CREATE TABLE IF NOT EXISTS pair_history (
                    pair_key TEXT PRIMARY KEY,
                    participant_a TEXT NOT NULL,
                    participant_b TEXT NOT NULL,
                    first_matched_round INTEGER NOT NULL,
                    last_matched_round INTEGER NOT NULL,
                    times_matched INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS analytics_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingestion_records (
                    source TEXT NOT NULL,
                    record_key TEXT NOT NULL,
                    participant_id TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    PRIMARY KEY (source, record_key)
                );

                CREATE TABLE IF NOT EXISTS participant_match_history (
                    person_id TEXT NOT NULL,
                    matched_with_id TEXT NOT NULL,
                    round_id INTEGER NOT NULL,
                    matched_at TEXT NOT NULL,
                    score REAL NOT NULL,
                    rationale TEXT NOT NULL,
                    PRIMARY KEY (person_id, matched_with_id, round_id)
                );

                CREATE TABLE IF NOT EXISTS current_matching_table (
                    round_id INTEGER NOT NULL,
                    group_index INTEGER NOT NULL,
                    participant_id TEXT NOT NULL,
                    member_order INTEGER NOT NULL,
                    group_size INTEGER NOT NULL,
                    score REAL NOT NULL,
                    reasons_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (group_index, participant_id)
                );
                """
            )

    def _append_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        created_at = _utc_now()
        event_json = json.dumps(payload, sort_keys=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO analytics_events (event_type, payload_json, created_at)
                VALUES (?, ?, ?)
                """,
                (event_type, event_json, created_at),
            )
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {"event_type": event_type, "payload": payload, "created_at": created_at},
                    sort_keys=True,
                )
                + "\n"
            )

    def _has_controller(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT controller_id FROM controller_access WHERE id = 1").fetchone()
        return row is not None

    def set_controller(self, controller_key: str, controller_id: str = "controller") -> bool:
        with self._connect() as conn:
            existing = conn.execute("SELECT id FROM controller_access WHERE id = 1").fetchone()
            if existing:
                return False
            conn.execute(
                """
                INSERT INTO controller_access (id, controller_id, secret_hash, created_at)
                VALUES (1, ?, ?, ?)
                """,
                (controller_id, _hash_secret(controller_key), _utc_now()),
            )
        self._append_event("controller_configured", {"controller_id": controller_id})
        return True

    def _verify_controller(self, controller_secret: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT secret_hash FROM controller_access WHERE id = 1"
            ).fetchone()
        if row is None:
            return False
        return row["secret_hash"] == _hash_secret(controller_secret)

    def _find_participant_by_name_ci(
        self, conn: sqlite3.Connection, name: str
    ) -> Optional[sqlite3.Row]:
        return conn.execute(
            """
            SELECT participant_id, name
            FROM participants
            WHERE lower(name) = ?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (_name_key(name),),
        ).fetchone()

    def _upsert_participant_by_name(
        self, conn: sqlite3.Connection, participant: Participant
    ) -> Dict[str, Any]:
        existing = self._find_participant_by_name_ci(conn, participant.name)
        if existing is None:
            conn.execute(
                """
                INSERT INTO participants (
                    participant_id, name, age, is_emory_student, gender,
                    attendance_experience, ethnicity, culture, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    participant.participant_id,
                    participant.name,
                    participant.age,
                    int(participant.is_emory_student),
                    participant.gender,
                    int(participant.attendance_experience),
                    participant.ethnicity,
                    participant.culture,
                    participant.created_at,
                ),
            )
            return {
                "participant_id": participant.participant_id,
                "name": participant.name,
                "action": "created",
            }

        participant_id = str(existing["participant_id"])
        conn.execute(
            """
            UPDATE participants
            SET name = ?, age = ?, is_emory_student = ?, gender = ?,
                attendance_experience = ?, ethnicity = ?, culture = ?, created_at = ?
            WHERE participant_id = ?
            """,
            (
                participant.name,
                participant.age,
                int(participant.is_emory_student),
                participant.gender,
                int(participant.attendance_experience),
                participant.ethnicity,
                participant.culture,
                participant.created_at,
                participant_id,
            ),
        )
        return {
            "participant_id": participant_id,
            "name": participant.name,
            "action": "updated",
        }

    def add_participant(self, participant: Participant) -> Dict[str, Any]:
        with self._connect() as conn:
            result = self._upsert_participant_by_name(conn, participant)
        event_type = (
            "participant_signup"
            if result["action"] == "created"
            else "participant_updated_by_name_ci"
        )
        self._append_event(
            event_type,
            {
                "participant_id": result["participant_id"],
                "name": result["name"],
                "data": participant.to_dict(),
            },
        )
        return result

    def add_participant_from_source(
        self,
        participant: Participant,
        *,
        source: str,
        record_key: str,
    ) -> bool:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT participant_id
                FROM ingestion_records
                WHERE source = ? AND record_key = ?
                """,
                (source, record_key),
            ).fetchone()
            if existing is not None:
                return False

            result = self._upsert_participant_by_name(conn, participant)
            conn.execute(
                """
                INSERT INTO ingestion_records (
                    source, record_key, participant_id, imported_at
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    source,
                    record_key,
                    result["participant_id"],
                    _utc_now(),
                ),
            )
        self._append_event(
            "participant_imported",
            {
                "source": source,
                "record_key": record_key,
                "action": result["action"],
                "participant": participant.to_dict(),
            },
        )
        return True

    def list_participants(self) -> List[Participant]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT participant_id, name, age, is_emory_student, gender,
                       attendance_experience, ethnicity, culture, created_at
                FROM participants
                ORDER BY created_at ASC
                """
            ).fetchall()
        return [
            Participant(
                participant_id=row["participant_id"],
                name=row["name"],
                age=int(row["age"]),
                is_emory_student=bool(row["is_emory_student"]),
                gender=row["gender"],
                attendance_experience=bool(row["attendance_experience"]),
                ethnicity=row["ethnicity"],
                culture=row["culture"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def get_policy(self) -> MatchingPolicy:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT policy_json FROM matching_policy WHERE id = 1"
            ).fetchone()
        if row is None:
            return MatchingPolicy()
        return MatchingPolicy.from_dict(json.loads(row["policy_json"]))

    def set_policy(self, controller_secret: str, updates: Dict[str, Any]) -> bool:
        if not self._verify_controller(controller_secret):
            return False

        policy = MatchingPolicy.from_dict(self.get_policy().to_dict() | updates)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO matching_policy (id, policy_json, updated_at, updated_by)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    policy_json=excluded.policy_json,
                    updated_at=excluded.updated_at,
                    updated_by=excluded.updated_by
                """,
                (json.dumps(policy.to_dict(), sort_keys=True), _utc_now(), "controller"),
            )
        self._append_event("policy_updated", {"policy": policy.to_dict()})
        return True

    def get_prior_pair_set(self) -> set[Tuple[str, str]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT participant_a, participant_b FROM pair_history").fetchall()
        return {_pair_tuple(row["participant_a"], row["participant_b"]) for row in rows}

    def get_pair_match_counts(self) -> Dict[Tuple[str, str], int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT participant_a, participant_b, times_matched
                FROM pair_history
                """
            ).fetchall()
        return {
            _pair_tuple(row["participant_a"], row["participant_b"]): int(row["times_matched"])
            for row in rows
        }

    def record_round(self, pairs: Sequence[Tuple[str, str, float, str]]) -> int:
        matched_at = _utc_now()
        with self._connect() as conn:
            round_id = conn.execute(
                "INSERT INTO match_rounds (run_at) VALUES (?)",
                (matched_at,),
            ).lastrowid

            for a, b, score, rationale in pairs:
                x, y = _pair_tuple(a, b)
                conn.execute(
                    """
                    INSERT INTO match_pairs (round_id, participant_a, participant_b, score, rationale)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (round_id, x, y, float(score), rationale),
                )
                conn.execute(
                    """
                    INSERT INTO pair_history (
                        pair_key, participant_a, participant_b, first_matched_round, last_matched_round, times_matched
                    )
                    VALUES (?, ?, ?, ?, ?, 1)
                    ON CONFLICT(pair_key) DO UPDATE SET
                        last_matched_round=excluded.last_matched_round,
                        times_matched=pair_history.times_matched + 1
                    """,
                    (_pair_key(x, y), x, y, round_id, round_id),
                )
                conn.execute(
                    """
                    INSERT INTO participant_match_history (
                        person_id, matched_with_id, round_id, matched_at, score, rationale
                    )
                    VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        x,
                        y,
                        round_id,
                        matched_at,
                        float(score),
                        rationale,
                        y,
                        x,
                        round_id,
                        matched_at,
                        float(score),
                        rationale,
                    ),
                )

        self._append_event(
            "matching_round",
            {"round_id": int(round_id), "pair_count": len(pairs)},
        )
        return int(round_id)

    def replace_current_matching_table(
        self,
        *,
        round_id: int,
        rows: Sequence[Tuple[int, str, int, int, float, str]],
    ) -> None:
        updated_at = _utc_now()
        with self._connect() as conn:
            conn.execute("DELETE FROM current_matching_table")
            for group_index, participant_id, member_order, group_size, score, reasons_json in rows:
                conn.execute(
                    """
                    INSERT INTO current_matching_table (
                        round_id, group_index, participant_id, member_order,
                        group_size, score, reasons_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(round_id),
                        int(group_index),
                        participant_id,
                        int(member_order),
                        int(group_size),
                        float(score),
                        reasons_json,
                        updated_at,
                    ),
                )
        self._append_event(
            "current_matching_table_replaced",
            {"round_id": int(round_id), "row_count": len(rows)},
        )

    def reset_matching_table(self, controller_secret: str) -> bool:
        if not self._verify_controller(controller_secret):
            return False
        with self._connect() as conn:
            conn.execute("DELETE FROM current_matching_table")
        self._append_event("current_matching_table_reset", {})
        return True

    def list_current_matching_table(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT cmt.round_id, cmt.group_index, cmt.participant_id,
                       cmt.member_order, cmt.group_size, cmt.score, cmt.reasons_json,
                       cmt.updated_at, p.name
                FROM current_matching_table cmt
                JOIN participants p ON p.participant_id = cmt.participant_id
                ORDER BY cmt.group_index ASC, cmt.member_order ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_participant_profile(self, name: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            person = conn.execute(
                """
                SELECT participant_id, name, age, is_emory_student, gender,
                       attendance_experience, ethnicity, culture, created_at
                FROM participants
                WHERE lower(name) = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (_name_key(name),),
            ).fetchone()
            if person is None:
                return None
            history_rows = conn.execute(
                """
                SELECT pmh.round_id, pmh.matched_at, pmh.score, pmh.rationale,
                       p.name AS matched_with_name, p.participant_id AS matched_with_id
                FROM participant_match_history pmh
                JOIN participants p ON p.participant_id = pmh.matched_with_id
                WHERE pmh.person_id = ?
                ORDER BY pmh.round_id DESC, pmh.matched_with_id ASC
                """,
                (person["participant_id"],),
            ).fetchall()

        return {
            "participant": {
                "participant_id": person["participant_id"],
                "name": person["name"],
                "age": int(person["age"]),
                "is_emory_student": bool(person["is_emory_student"]),
                "gender": person["gender"],
                "attendance_experience": bool(person["attendance_experience"]),
                "ethnicity": person["ethnicity"],
                "culture": person["culture"],
                "created_at": person["created_at"],
            },
            "match_history": [dict(row) for row in history_rows],
        }

    def list_pair_history(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT pair_key, participant_a, participant_b, first_matched_round, last_matched_round, times_matched
                FROM pair_history
                ORDER BY last_matched_round DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]


# Backward-compatible aliases for callers that used earlier naming.
MatchingRepository = DataStore
Repository = DataStore
