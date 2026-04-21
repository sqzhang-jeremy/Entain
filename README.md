# Entain Australia Junior MLE Take-Home

This repository is currently set up as a submission scaffold for the Entain Australia junior Machine Learning Engineer take-home assignment.

At this stage, the goal is to understand the brief, create the expected delivery structure, and document the implementation plan. The actual validation and feature-engineering logic is intentionally not finished yet.

## Task Summary

Build a local, reproducible, batch-oriented Python package that:

- validates `bets.csv` against the business rules from the brief
- separates valid and invalid records
- produces a machine-readable validation summary
- generates customer-level features from each customer's first 20 bets
- exposes both workflows through a CLI
- runs in Docker
- includes tests and design documentation

## Inputs Available In This Workspace

- [jr-mle-task.docx](/Users/jeremyzhang/Documents/Entain-mle-task/jr-mle-task.docx)
- [bets.csv](/Users/jeremyzhang/Documents/Entain-mle-task/bets.csv)

Observed from the local dataset:

- file size is approximately 42 MB
- CSV columns are:
  `bet_id, customer_id, bet_datetime, bet_num, betting_amount, price, category, stake_type, bet_result, payout, return_for_entain`

## Deliverables Required By The Brief

- source code
- `pyproject.toml`
- `Dockerfile`
- `README.md`
- tests
- validation outputs
- customer feature output
- architecture diagram
- design note

## Scaffolded Repository Layout

```text
.
|-- AGENT.md
|-- Dockerfile
|-- README.md
|-- bets.csv
|-- docs/
|   |-- architecture-diagram.mmd
|   `-- design-note.md
|-- jr-mle-task.docx
|-- outputs/
|   |-- features/
|   `-- validation/
|-- pyproject.toml
|-- src/
|   `-- bet_pipeline/
|       |-- __init__.py
|       |-- build_features.py
|       |-- cli.py
|       `-- validate.py
`-- tests/
    `-- README.md
```

## Expected CLI Contract

These are the commands the final submission is expected to support:

```bash
bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
bet-pipeline build-features --input /data/bets.csv --output /outputs/features/
```

The current codebase only contains CLI scaffolding and placeholder modules.

## Planned Outputs

Validation task:

- `valid_bets`
- `invalid_bets`
- `validation_report.json` or equivalent machine-readable summary

Feature task:

- `customer_features` in parquet if practical, otherwise CSV with justification

## Working Assumptions

- `bet_num` is the authoritative ordering field per customer
- invalid data should be surfaced explicitly, not hidden
- handling of invalid rows within the first 20 bets must be deterministic and documented
- this repo will stay local-only and batch-only to match the brief

## Files To Complete Later

- [src/bet_pipeline/validate.py](/Users/jeremyzhang/Documents/Entain-mle-task/src/bet_pipeline/validate.py): validation implementation
- [src/bet_pipeline/build_features.py](/Users/jeremyzhang/Documents/Entain-mle-task/src/bet_pipeline/build_features.py): feature generation implementation
- [src/bet_pipeline/cli.py](/Users/jeremyzhang/Documents/Entain-mle-task/src/bet_pipeline/cli.py): CLI wiring
- [tests/README.md](/Users/jeremyzhang/Documents/Entain-mle-task/tests/README.md): test plan to be replaced with actual tests
- [docs/architecture-diagram.mmd](/Users/jeremyzhang/Documents/Entain-mle-task/docs/architecture-diagram.mmd): architecture diagram template
- [docs/design-note.md](/Users/jeremyzhang/Documents/Entain-mle-task/docs/design-note.md): design note template

## Runbook Placeholder

Local install:

```bash
python -m pip install -e .
```

Target CLI usage after implementation:

```bash
bet-pipeline validate --input bets.csv --output outputs/validation
bet-pipeline build-features --input bets.csv --output outputs/features
```

Docker placeholder:

```bash
docker build -t entain-bet-pipeline .
docker run --rm -v "$(pwd):/workspace" entain-bet-pipeline --help
```

## Completion Checklist

- [ ] Implement validation rules and outputs
- [ ] Implement customer feature generation
- [ ] Decide and document invalid-row handling for the first 20 bets
- [ ] Add tests
- [ ] Generate final output artifacts
- [ ] Complete architecture diagram
- [ ] Complete design note
- [ ] Verify Docker workflow

## Notes

This scaffold is designed to make the assignment easier to complete incrementally while keeping the final submission requirements visible from the start.
