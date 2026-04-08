# NGO Matching System

This repository contains a configurable participant matching system for NGO
programs.

## Features

- Participant sign-up records:
  - age
  - name
  - Emory student flag (`true/false`)
  - gender
  - attendance experience (`true/false`)
  - ethnicity
  - culture
- Matching priorities:
  - prefer different ethnicity/culture/gender
  - prefer matching experienced participants with inexperienced participants
  - prefer close ages
- Controller-only matching policy configuration
- Persistent "big data" style storage through SQLite
- Historical no-rematch protection (previous pairings are not matched again)

## Quickstart

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
python3 -m ngo_matching --help
```

> This project requires **Python 3.9+**. On macOS, `python` may still point to
> Python 2.7, so use `python3` explicitly.

## CLI Usage

### 1. Initialize storage and set controller key

```bash
python3 -m ngo_matching init --controller-key "super-secret-key"
```

### 2. Add participants

```bash
python3 -m ngo_matching add-participant \
  --name "Alice" \
  --age 22 \
  --is-emory-student true \
  --gender "female" \
  --attendance-experience false \
  --ethnicity "Korean" \
  --culture "East Asian"
```

### 3. Update matching policy (controller only)

```bash
python3 -m ngo_matching set-policy \
  --controller-key "super-secret-key" \
  --strict-diversity true \
  --max-age-gap 5 \
  --age-weight 6
```

### 4. Run a matching round

```bash
python3 -m ngo_matching run-match
```

Optional flags for this command:
- `--show-score-details`: print score formula and score component breakdown.
- `--dry-run`: preview matching without saving the round to history.

```bash
python3 -m ngo_matching run-match --dry-run
```

### 5. Import participants from Google Form responses

1. In Google Forms, link responses to a Google Sheet.
2. In Google Sheets, share the response sheet for viewing (so CSV export is readable).
3. Run import from either sheet URL or direct CSV URL:

```bash
# Option A: pass the sheet URL
python3 -m ngo_matching import-google-form \
  --sheet-url "https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit?gid=0"

# Option B: pass direct CSV export URL
python3 -m ngo_matching import-google-form \
  --csv-url "https://docs.google.com/spreadsheets/d/<SHEET_ID>/export?format=csv&gid=0"
```

Expected Google Form column names (case-insensitive):
- Name (or Full Name)
- Age
- Is Emory Student
- Gender
- Attendance Experience
- Ethnicity
- Culture

Import is idempotent: previously imported response rows are skipped on re-run.

Run with `--json` for machine-readable output.

## Testing

```bash
python3 -m pytest -q
```
