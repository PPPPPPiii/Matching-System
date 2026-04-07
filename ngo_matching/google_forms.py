from __future__ import annotations

import csv
import hashlib
from io import StringIO
from typing import Dict, Iterable, List, Tuple
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from .models import Participant


class GoogleFormImportError(ValueError):
    pass


def _normalize_header(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _record_key(source: str, row: Dict[str, str]) -> str:
    # Timestamp+email are stable in Google Form response exports.
    timestamp = row.get("timestamp", "").strip().lower()
    email = (
        row.get("email address", "")
        or row.get("email", "")
        or row.get("e-mail", "")
    ).strip().lower()
    raw = f"{source}|{timestamp}|{email}|{row.get('name', '').strip().lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _extract_sheet_id(url: str) -> str:
    parsed = urlparse(url)
    if "docs.google.com" not in parsed.netloc:
        raise GoogleFormImportError(
            "Expected a Google Sheets URL from docs.google.com."
        )
    parts = [part for part in parsed.path.split("/") if part]
    if "d" not in parts:
        raise GoogleFormImportError("Could not parse Google Sheet ID from URL.")
    idx = parts.index("d")
    if idx + 1 >= len(parts):
        raise GoogleFormImportError("Could not parse Google Sheet ID from URL.")
    return parts[idx + 1]


def _extract_gid(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    gid = query.get("gid", ["0"])[0]
    return gid


def build_public_csv_url(sheet_url: str) -> str:
    sheet_id = _extract_sheet_id(sheet_url)
    gid = _extract_gid(sheet_url)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def fetch_csv_rows(csv_url: str) -> List[Dict[str, str]]:
    with urlopen(csv_url) as resp:  # nosec B310 - URL provided by operator
        payload = resp.read().decode("utf-8")
    reader = csv.DictReader(StringIO(payload))
    rows: List[Dict[str, str]] = []
    for row in reader:
        normalized = {_normalize_header(k): (v or "").strip() for k, v in row.items() if k}
        rows.append(normalized)
    return rows


def _field(row: Dict[str, str], *candidates: str) -> str:
    for key in candidates:
        if key in row and row[key]:
            return row[key]
    raise GoogleFormImportError(
        f"Missing required field. Expected one of: {', '.join(candidates)}"
    )


def participant_from_form_row(row: Dict[str, str]) -> Participant:
    return Participant.from_signup(
        name=_field(row, "name", "full name"),
        age=int(_field(row, "age")),
        is_emory_student=_field(
            row,
            "is emory student",
            "are you an emory student",
            "emory student",
            "is_emory_student",
        ),
        gender=_field(row, "gender"),
        attendance_experience=_field(
            row,
            "attendance experience",
            "have you attended before",
            "attendance_experience",
            "experience",
        ),
        ethnicity=_field(row, "ethnicity"),
        culture=_field(row, "culture", "culture identify with"),
    )


def parse_google_form_rows(
    source: str, rows: Iterable[Dict[str, str]]
) -> List[Tuple[str, Participant]]:
    parsed: List[Tuple[str, Participant]] = []
    for row in rows:
        participant = participant_from_form_row(row)
        parsed.append((_record_key(source, row), participant))
    return parsed
