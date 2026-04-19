from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import re
from typing import Any, Dict, Optional
from uuid import uuid4


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())


def _normalize_identity_phrase(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", value.lower())
    normalized_tokens: list[str] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "united" and i + 1 < len(tokens) and tokens[i + 1] in {
            "states",
            "state",
        }:
            normalized_tokens.extend(["united", "states"])
            i += 2
            continue
        if token in {"usa", "us", "america", "american"}:
            normalized_tokens.extend(["united", "states"])
            i += 1
            continue
        normalized_tokens.append(token)
        i += 1

    return " ".join(normalized_tokens)


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value in (0, 1):
            return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    raise ValueError(f"Unable to parse bool from value: {value!r}")


@dataclass(frozen=True)
class Participant:
    participant_id: str
    name: str
    age: int
    is_emory_student: bool
    gender: str
    attendance_experience: bool
    ethnicity: str
    culture: str
    created_at: str

    @classmethod
    def from_signup(
        cls,
        *,
        name: str,
        age: int,
        is_emory_student: Any,
        gender: str,
        attendance_experience: Any,
        ethnicity: str,
        culture: str,
        participant_id: Optional[str] = None,
    ) -> "Participant":
        if not name or not name.strip():
            raise ValueError("name is required")
        if int(age) <= 0:
            raise ValueError("age must be positive")

        created_at = datetime.now(timezone.utc).isoformat()
        return cls(
            participant_id=participant_id or str(uuid4()),
            name=_normalize_text(name),
            age=int(age),
            is_emory_student=parse_bool(is_emory_student),
            gender=_normalize_text(gender),
            attendance_experience=parse_bool(attendance_experience),
            ethnicity=_normalize_identity_phrase(ethnicity),
            culture=_normalize_identity_phrase(culture),
            created_at=created_at,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchingPolicy:
    prefer_different_ethnicity: bool = True
    prefer_different_culture: bool = True
    prefer_different_gender: bool = True
    require_experience_mix: bool = True
    max_age_gap: int = 6
    strict_diversity: bool = False
    strict_age_gap: bool = True
    ethnicity_weight: float = 3.0
    culture_weight: float = 3.0
    gender_weight: float = 2.0
    experience_mix_weight: float = 5.0
    age_weight: float = 4.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, values: Dict[str, Any]) -> "MatchingPolicy":
        return cls(
            prefer_different_ethnicity=parse_bool(
                values.get("prefer_different_ethnicity", True)
            ),
            prefer_different_culture=parse_bool(
                values.get("prefer_different_culture", True)
            ),
            prefer_different_gender=parse_bool(
                values.get("prefer_different_gender", True)
            ),
            require_experience_mix=parse_bool(
                values.get("require_experience_mix", True)
            ),
            max_age_gap=int(values.get("max_age_gap", 6)),
            strict_diversity=parse_bool(values.get("strict_diversity", False)),
            strict_age_gap=parse_bool(values.get("strict_age_gap", True)),
            ethnicity_weight=float(values.get("ethnicity_weight", 3.0)),
            culture_weight=float(values.get("culture_weight", 3.0)),
            gender_weight=float(values.get("gender_weight", 2.0)),
            experience_mix_weight=float(values.get("experience_mix_weight", 5.0)),
            age_weight=float(values.get("age_weight", 4.0)),
        )
