from __future__ import annotations

import argparse
import json
import re
import sys
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
_ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
_MEMBER_COLORS = [
    "\033[96m",  # cyan
    "\033[95m",  # magenta
    "\033[94m",  # blue
    "\033[92m",  # green
    "\033[93m",  # yellow
]
_ANSI_RESET = "\033[0m"


def _repo_from_path(path: str) -> DataStore:
    return DataStore(db_path=path)


def _supports_color() -> bool:
    return bool(getattr(sys.stdout, "isatty", lambda: False)())


def _visible_width(value: str) -> int:
    return len(_ANSI_PATTERN.sub("", value))


def _pad_cell(value: str, width: int) -> str:
    return value + (" " * max(width - _visible_width(value), 0))


def _colorize_member(name: str, index: int, enabled: bool) -> str:
    if not enabled:
        return name
    color = _MEMBER_COLORS[index % len(_MEMBER_COLORS)]
    return f"{color}{name}{_ANSI_RESET}"


def _format_breakdown(score_breakdown: Dict[str, float]) -> str:
    ordered = [
        "attendance_experience",
        "culture",
        "ethnicity",
        "age",
        "gender",
        "is_emory_student",
        "new_partner_bonus",
        "rematch_penalty",
    ]
    keys = [k for k in ordered if k in score_breakdown] + [
        k for k in score_breakdown.keys() if k not in ordered
    ]
    return ", ".join(f"{k}={score_breakdown[k]:+.2f}" for k in keys)


def _score_formula() -> Dict[str, str]:
    return {
        "attendance_experience": "+experience_mix_weight if mixed, else -0.4 * experience_mix_weight",
        "culture": "+culture_weight when different",
        "ethnicity": "+ethnicity_weight when different",
        "age": "+age_weight * max(max_age_gap - age_gap, 0) / max_age_gap",
        "gender": "+gender_weight when different",
        "is_emory_student": "+0.5 when different",
        "new_partner_bonus": "+3.0 if pair has never matched before",
        "rematch_penalty": "-50.0 * times_matched_before",
        "strictness_fallback": "Relax in this order when needed: attendance -> culture -> ethnicity -> age -> gender -> is_emory_student",
        "rematch_priority": "Avoid rematch first; only allow rematch when no sufficient non-rematch grouping exists",
    }


def _print_table(headers: list[str], rows: list[list[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], _visible_width(cell))

    horizontal = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    print(horizontal)
    print("| " + " | ".join(_pad_cell(h, widths[i]) for i, h in enumerate(headers)) + " |")
    print(horizontal)
    for row in rows:
        print(
            "| "
            + " | ".join(_pad_cell(cell, widths[i]) for i, cell in enumerate(row))
            + " |"
        )
    print(horizontal)


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
    result = repo.add_participant(participant)
    print(
        json.dumps(
            {
                "participant_id": result["participant_id"],
                "name": result["name"],
                "action": result["action"],
            }
        )
    )


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
    result = engine.run_round(persist=not args.dry_run)
    rows = []
    use_color = bool(args.color_members) and _supports_color()
    for index, group in enumerate(result.groups, start=1):
        names = [
            _colorize_member(p.name, idx, use_color)
            for idx, p in enumerate(group.participants)
        ]
        base_row = {
            "group": index,
            "size": len(group.participants),
            "members": names,
            "score": group.score,
        }
        if args.show_score_details:
            base_row["score_breakdown"] = {
                key: round(value, 3) for key, value in group.score_breakdown.items()
            }
            base_row["reasons"] = group.reasons
        rows.append(base_row)

    if args.json:
        payload = {
            "group_count": len(result.groups),
            "strictness_level": result.strictness_level,
            "used_rematch": result.used_rematch,
            "match_table": rows,
            "unmatched": [p.name for p in result.unmatched],
        }
        if args.show_score_details:
            payload["score_formula"] = _score_formula()
        print(json.dumps(payload, indent=2))
        return

    table_rows: list[list[str]] = []
    for row in rows:
        line = [
            str(row["group"]),
            str(row["size"]),
            ", ".join(row["members"]),
            f"{float(row['score']):.3f}",
        ]
        if args.show_score_details:
            line.append(_format_breakdown(row.get("score_breakdown", {})))
        table_rows.append(line)

    headers = ["Group", "Size", "Members", "Score"]
    if args.show_score_details:
        headers.append("Score Breakdown")

    print(
        f"Round result: groups={len(result.groups)}, unmatched={len(result.unmatched)}, "
        f"strictness_level={result.strictness_level}, used_rematch={result.used_rematch}"
    )
    if args.show_score_details:
        print("Score formula:")
        for key, detail in _score_formula().items():
            print(f"- {key}: {detail}")
    _print_table(headers, table_rows)
    if args.dry_run:
        print("Dry run only: this round was NOT saved to history.")
    if result.unmatched:
        print("Unmatched:", ", ".join(p.name for p in result.unmatched))


def reset_matching_table(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    ok = repo.reset_matching_table(args.controller_key)
    if not ok:
        raise SystemExit("controller key invalid or controller not configured")
    print(
        json.dumps(
            {
                "status": "ok",
                "message": "current matching table reset",
            }
        )
    )


def participant_profile(args: argparse.Namespace) -> None:
    repo = _repo_from_path(args.db_path)
    profile = repo.get_participant_profile(args.name)
    if profile is None:
        raise SystemExit(f"participant not found: {args.name}")
    print(json.dumps(profile, indent=2))


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
    match_cmd.add_argument("--json", action="store_true", help="Print JSON output.")
    match_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview matches without saving this round into history.",
    )
    match_cmd.add_argument(
        "--show-score-details",
        action="store_true",
        help="Include score formula and component breakdown details.",
    )
    match_cmd.add_argument(
        "--color-members",
        type=parse_bool,
        default=True,
        help="Enable ANSI colors for member names in table output (default: true).",
    )
    match_cmd.set_defaults(func=run_matching)

    reset_cmd = subparsers.add_parser("reset-matching-table")
    reset_cmd.add_argument("--controller-key", required=True)
    reset_cmd.set_defaults(func=reset_matching_table)

    profile_cmd = subparsers.add_parser("participant-profile")
    profile_cmd.add_argument("--name", required=True)
    profile_cmd.set_defaults(func=participant_profile)

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
