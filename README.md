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

`add-participant` is case-insensitive on name. If the same name is entered again
with different case (for example `Alice` vs `alice`), the new submission
overwrites the prior participant profile.

Identity matching is based on first and last name, ignoring capitalization and
extra spaces (for example `Alice Smith`, `ALICE smith`, and ` alice   SMITH `
are treated as the same participant).

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
- `--print-users-table`: print all users' characteristic data in a table before matching.

```bash
python3 -m ngo_matching run-match --dry-run
```

Reset only the current matching table (history remains in participant profiles):

```bash
python3 -m ngo_matching reset-matching-table \
  --controller-key "super-secret-key"
```

View one participant profile and full match history (case-insensitive by name):

```bash
python3 -m ngo_matching participant-profile --name "alice"
```

Clean duplicate participants by first+last-name identity (controller only):

```bash
python3 -m ngo_matching cleanup-participants \
  --controller-key "super-secret-key"
```

### 5. Import participants from uploaded CSV/XLSX sheet

Use a local spreadsheet file instead of URL import:

```bash
python3 -m ngo_matching import-sheet --file-path "./participants.xlsx"
```

(`import-google-form --file-path ...` is also supported as a backward-compatible alias.)

Supported file types:
- `.csv`
- `.xlsx`

The importer scans the **first row** and detects columns by keywords for these required characteristics:
- name (full name OR first name + last name)
- countries of citizen
- nationalities/culture identified as
- gender
- Emory student or not
- first time or not

Notes:
- Header detection is case-insensitive and word-based.
- `nationalities/culture` values are normalized case-insensitively by phrase tokens.
- `America`, `US`, `USA`, and `United States` are treated as the same value.
- Import is idempotent: previously imported rows are skipped on re-run.

### 6. Run the HTML website

Start the local web interface:

```bash
python3 -m ngo_matching run-web --host 127.0.0.1 --port 8000
```

Then open `http://127.0.0.1:8000` in your browser.

- Participant login:
  - Enter name in the password field (name matching is case-insensitive using first+last identity).
  - The page returns that participant's table number in the current matching table.
- Controller login:
  - Enter the controller key.
  - The page shows the full matching table with all table numbers and members.

Run with `--json` for machine-readable output.

## Testing

```bash
python3 -m pytest -q
```
