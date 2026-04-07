from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from ngo_matching.google_forms import (
    build_public_csv_url,
    fetch_csv_rows,
    parse_google_form_rows,
)
from ngo_matching.matcher import MatchingEngine
from ngo_matching.models import MatchingPolicy, Participant, parse_bool
from ngo_matching.storage import DataStore


DEFAULT_DB = Path("data/ngo_matching.db")


def _repo_from_path(path: str) -> DataStore:
    return DataStore(db_path=path)


def register_participant(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    participant = Participant.from_signup(
        name=args.name,
        age=args.age,
        is_emory_student=args.is_emory_student,
        gender=args.gender,
        attendance_experience=args.attendance_experience,
        ethnicity=args.ethnicity,
        culture=args.culture,
    )
    repo.add_participant(participant)
    print(json.dumps({"participant_id": participant.participant_id, "name": participant.name}))


def list_participants(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    participants = [p.to_dict() for p in repo.list_participants()]
    print(json.dumps(participants, indent=2))


def set_controller(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    created = repo.set_controller(args.controller_key)
    print("controller configured" if created else "controller already configured")


def update_policy(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    updates: Dict[str, Any] = {}
    for entry in args.set:
        if "=" not in entry:
            raise ValueError(f"Invalid --set value: {entry}")
        key, raw = entry.split("=", 1)
        key = key.strip()
        raw = raw.strip()
        if key in {
            "prefer_different_ethnicity",
            "prefer_different_culture",
            "prefer_different_gender",
            "require_experience_mix",
            "strict_diversity",
            "strict_age_gap",
        }:
            updates[key] = parse_bool(raw)
        elif key in {"max_age_gap"}:
            updates[key] = int(raw)
        elif key in {
            "ethnicity_weight",
            "culture_weight",
            "gender_weight",
            "experience_mix_weight",
            "age_weight",
        }:
            updates[key] = float(raw)
        else:
            raise ValueError(f"Unsupported policy key: {key}")

    # Also support direct flags (e.g. --strict-diversity true).
    direct_updates = {
        "prefer_different_ethnicity": args.prefer_different_ethnicity,
        "prefer_different_culture": args.prefer_different_culture,
        "prefer_different_gender": args.prefer_different_gender,
        "require_experience_mix": args.require_experience_mix,
        "strict_diversity": args.strict_diversity,
        "strict_age_gap": args.strict_age_gap,
        "max_age_gap": args.max_age_gap,
        "ethnicity_weight": args.ethnicity_weight,
        "culture_weight": args.culture_weight,
        "gender_weight": args.gender_weight,
        "experience_mix_weight": args.experience_mix_weight,
        "age_weight": args.age_weight,
    }
    for key, value in direct_updates.items():
        if value is not None:
            updates[key] = value

    ok = repo.set_policy(args.controller_key, updates)
    if not ok:
        raise SystemExit("controller key invalid or controller not configured")
    print(json.dumps(repo.get_policy().to_dict(), indent=2))


def run_matching(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    engine = MatchingEngine(repo)
    pairs, unmatched = engine.run_round()
    payload = {
        "pair_count": len(pairs),
        "pairs": [
            {
                "left_id": pair.participant_one.participant_id,
                "left_name": pair.participant_one.name,
                "right_id": pair.participant_two.participant_id,
                "right_name": pair.participant_two.name,
                "score": pair.score,
            }
            for pair in pairs
        ],
        "unmatched": [p.to_dict() for p in unmatched],
    }
    print(json.dumps(payload, indent=2))


def import_google_form(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    csv_url = args.csv_url if args.csv_url else build_public_csv_url(args.sheet_url)
    rows = fetch_csv_rows(csv_url)
    parsed = parse_google_form_rows(csv_url, rows)

    imported = 0
    skipped = 0
    for record_key, participant in parsed:
        was_new = repo.add_participant_from_source(
            participant,
            source=csv_url,
            record_key=record_key,
        )
        if was_new:
            imported += 1
        else:
            skipped += 1

    print(
        json.dumps(
            {
                "source": csv_url,
                "rows_read": len(rows),
                "imported": imported,
                "skipped_existing": skipped,
            },
            indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NGO participant matching system")
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    subparsers = parser.add_subparsers(required=True)

    init_cmd = subparsers.add_parser("init")
    init_cmd.add_argument("--controller-key", required=True)
    init_cmd.set_defaults(func=set_controller)

    register_cmd = subparsers.add_parser("add-participant")
    register_cmd.add_argument("--name", required=True)
    register_cmd.add_argument("--age", required=True, type=int)
    register_cmd.add_argument("--is-emory-student", required=True)
    register_cmd.add_argument("--gender", required=True)
    register_cmd.add_argument("--attendance-experience", required=True)
    register_cmd.add_argument("--ethnicity", required=True)
    register_cmd.add_argument("--culture", required=True)
    register_cmd.set_defaults(func=register_participant)

    list_cmd = subparsers.add_parser("list-participants")
    list_cmd.set_defaults(func=list_participants)

    policy_cmd = subparsers.add_parser("set-policy")
    policy_cmd.add_argument("--controller-key", required=True)
    policy_cmd.add_argument(
        "--set",
        action="append",
        default=[],
        help="Policy update in key=value form. Repeat for multiple fields.",
    )
    policy_cmd.add_argument("--prefer-different-ethnicity", type=parse_bool)
    policy_cmd.add_argument("--prefer-different-culture", type=parse_bool)
    policy_cmd.add_argument("--prefer-different-gender", type=parse_bool)
    policy_cmd.add_argument("--require-experience-mix", type=parse_bool)
    policy_cmd.add_argument("--strict-diversity", type=parse_bool)
    policy_cmd.add_argument("--strict-age-gap", type=parse_bool)
    policy_cmd.add_argument("--max-age-gap", type=int)
    policy_cmd.add_argument("--ethnicity-weight", type=float)
    policy_cmd.add_argument("--culture-weight", type=float)
    policy_cmd.add_argument("--gender-weight", type=float)
    policy_cmd.add_argument("--experience-mix-weight", type=float)
    policy_cmd.add_argument("--age-weight", type=float)
    policy_cmd.set_defaults(func=update_policy)

    match_cmd = subparsers.add_parser("run-match")
    match_cmd.set_defaults(func=run_matching)

    import_cmd = subparsers.add_parser("import-google-form")
    import_source_group = import_cmd.add_mutually_exclusive_group(required=True)
    import_source_group.add_argument(
        "--sheet-url",
        help="Public Google Sheets URL containing form responses.",
    )
    import_source_group.add_argument(
        "--csv-url",
        help="Direct CSV export URL.",
    )
    import_cmd.set_defaults(func=import_google_form)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
