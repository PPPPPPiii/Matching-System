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
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m ngo_matching --help
```

## CLI Usage

### 1. Initialize storage and set controller key

```bash
python -m ngo_matching init --controller-key "super-secret-key"
```

### 2. Add participants

```bash
python -m ngo_matching add-participant \
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
python -m ngo_matching set-policy \
  --controller-key "super-secret-key" \
  --strict-diversity true \
  --max-age-gap 5 \
  --age-weight 6
```

### 4. Run a matching round

```bash
python -m ngo_matching run-match
```

Run with `--json` for machine-readable output.

## Testing

```bash
pytest -q
```
