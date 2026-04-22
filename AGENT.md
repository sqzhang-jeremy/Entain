# AGENT.md

This file is a concise implementation summary for the completed Entain Australia junior MLE take-home assignment.

## Assignment Outcome

The repository now contains a local, reproducible, batch-oriented Python package that:

- validates raw betting data against the business rules from the brief
- separates valid and invalid bets
- produces a machine-readable validation report
- builds customer-level features from validated bets
- exposes both workflows through a CLI
- runs in Docker
- includes automated tests, a README, an architecture diagram, and a design note

## Input Data

- source file: `data/bets.csv`
- columns:
  `bet_id,customer_id,bet_datetime,bet_num,betting_amount,price,category,stake_type,bet_result,payout,return_for_entain`

## Implemented Business Rules

Field/domain rules:

- `betting_amount > 0`
- `price > 1`
- `category in {"sports", "racing"}`
- `stake_type in {"cash", "bonus"}`
- `bet_result in {"return", "no-return"}`

Derived-value rules:

- if `bet_result == "no-return"`, then `payout = 0`
- if `bet_result == "return"` and `stake_type == "cash"`, then `payout = betting_amount * price`
- if `bet_result == "return"` and `stake_type == "bonus"`, then `payout = betting_amount * (price - 1)`
- if `bet_result == "no-return"` and `stake_type == "cash"`, then `return_for_entain = betting_amount`
- if `bet_result == "no-return"` and `stake_type == "bonus"`, then `return_for_entain = 0`
- if `bet_result == "return"` and `stake_type == "cash"`, then `return_for_entain = betting_amount - payout`
- if `bet_result == "return"` and `stake_type == "bonus"`, then `return_for_entain = -payout`

Sequencing rules:

- `bet_id` must be unique
- `bet_num` is the authoritative ordering field per customer
- ordering inconsistencies are surfaced during validation

## Feature-Build Decisions

- feature generation uses validated data only
- features are built from the first 20 valid bets ordered by `bet_num`
- if invalid bets appear within raw `bet_num` positions 1-20, they are skipped and the window extends forward until 20 valid bets are collected
- if fewer than 20 valid bets exist overall, the customer is still emitted with a partial window and `bets_used < 20`
- `customer_features` is emitted as CSV for simplicity and inspectability, with that trade-off documented in the design note

## CLI Contract

- `bet-pipeline validate --input /data/bets.csv --output /outputs/validation/`
- `bet-pipeline build-features --input /data/bets.csv --output /outputs/features/`

## Outputs

Validation:

- `outputs/validation/valid_bets.csv`
- `outputs/validation/invalid_bets.csv`
- `outputs/validation/validation_report.json`

Features:

- `outputs/features/customer_features.csv`
- `outputs/features/feature_build_report.json`

## Verification

Implemented and checked:

- validation pipeline
- feature pipeline
- CLI entrypoint
- Docker build and mounted-volume runtime
- automated tests for validation, feature generation, and CLI behavior
- architecture diagram
- design note

Suggested local verification commands:

```bash
PYTHONPATH=src python3.11 -m unittest tests.test_validate tests.test_build_features tests.test_cli
docker build -t entain-bet-pipeline .
docker run --rm -v "$(pwd)/data:/data" -v "$(pwd)/outputs:/outputs" entain-bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
docker run --rm -v "$(pwd)/data:/data" -v "$(pwd)/outputs:/outputs" entain-bet-pipeline build-features --input /data/bets.csv --output /outputs/features/
```
