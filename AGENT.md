# AGENT.md

This file is the working brief for the Entain Australia junior Machine Learning Engineer take-home assignment.

It is intentionally focused on understanding and planning the task before implementation.

Please be concise while helping candidate finsih this assignment.

## Assignment Summary

Build a small, local, reproducible, batch-oriented Python package that:

- validates raw betting data from `bets.csv` against explicit business rules
- makes invalid records explicit rather than silently dropping them
- builds a customer-level feature dataset from each customer's first 20 bets
- exposes the workflow through CLI commands
- can run in Docker
- includes tests, a README, an architecture diagram, and a short design note

The brief explicitly says:

- use Python
- keep the solution local and batch-oriented
- do not build an API
- do not train a model
- do not use cloud services

## Source Assets In This Workspace

- `jr-mle-task.docx`: assignment brief
- `bets.csv`: input dataset

Quick facts gathered from the local files:

- `bets.csv` has 372,296 lines including the header
- current header:
  `bet_id,customer_id,bet_datetime,bet_num,betting_amount,price,category,stake_type,bet_result,payout,return_for_entain`

## Required Deliverables From The Brief

- source code
- `pyproject.toml`
- `Dockerfile`
- `README.md`
- tests
- validation outputs
- customer feature output
- architecture diagram
- design note

## Business Rules To Enforce

Field/domain rules:

- `betting_amount > 0`
- `price > 1`
- `category in {"sports", "racing"}`
- `stake_type in {"cash", "bonus"}`
- `bet_result in {"return", "no-return"}`

Derived-value rules:

- if `bet_result == "no-return"`, then `payout = 0`
- if `bet_result == "return"` and `stake_type == "cash"`, then
  `payout = betting_amount * price`
- if `bet_result == "return"` and `stake_type == "bonus"`, then
  `payout = betting_amount * (price - 1)`

- if `bet_result == "no-return"` and `stake_type == "cash"`, then
  `return_for_entain = betting_amount`
- if `bet_result == "no-return"` and `stake_type == "bonus"`, then
  `return_for_entain = 0`
- if `bet_result == "return"` and `stake_type == "cash"`, then
  `return_for_entain = betting_amount - payout`
- if `bet_result == "return"` and `stake_type == "bonus"`, then
  `return_for_entain = -payout`

Sequencing rules:

- `bet_id` should be unique per row
- `bet_num` is the authoritative bet order for each customer
- ordering consistency should be validated where relevant

## Feature Output Requirements

The customer-level feature dataset must be built from each customer's first 20 bets using validated data.

The brief requires at minimum:

- `customer_id`
- `first_bet_datetime`
- `twentieth_bet_datetime`
- `bets_used`
- `total_betting_amount`
- `mean_betting_amount`
- `mean_price`
- `pct_racing`
- `pct_cash`
- `pct_return`
- `total_payout`
- `total_return_for_entain`
- `feature_generated_at`

The brief also says:

- if invalid records appear within the first 20 bets, document the handling clearly
- behaviour must stay deterministic
- parquet is preferred, CSV is acceptable with justification

## Expected CLI Contract

The package should support commands equivalent to:

- `bet-pipeline validate --input /data/bets.csv --output /outputs/validation/`
- `bet-pipeline build-features --input /data/bets.csv --output /outputs/features/`

## Open Decisions To Resolve During Implementation

1. How to handle invalid rows in the first 20 bets for a customer.
2. Whether to emit parquet or CSV for `customer_features`.
3. What to include in the machine-readable validation report beyond rule counts.
4. Whether to treat ordering issues as row-level failures, customer-level failures, or both.
5. Which dependency footprint is reasonable for a junior take-home submission.

## Suggested Implementation Plan

1. Inspect and profile the raw dataset.
2. Define the validation contract and failure taxonomy.
3. Implement validation logic and machine-readable outputs.
4. Decide and document deterministic handling for invalid rows inside the first 20 bets.
5. Implement feature generation from validated data.
6. Add CLI entry points.
7. Add tests for validation and aggregation edge cases.
8. Finalize Docker packaging.
9. Finish the architecture diagram and design note.
10. Run the pipeline locally and save final outputs.

## Delivery Checklist

- [ ] Validation pipeline implemented
- [ ] Feature pipeline implemented
- [ ] CLI commands working
- [ ] Docker build/run documented and tested
- [ ] Tests added
- [ ] Validation outputs generated
- [ ] Customer feature output generated
- [ ] Architecture diagram finalized
- [ ] Design note finalized
- [ ] README finalized

## Current Status

This repository is currently scaffolded only. The delivery files exist, but the assignment logic has not been implemented yet.
