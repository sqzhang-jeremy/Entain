# Entain Australia Junior MLE Take-Home

This repository contains the completed submission for the Entain Australia junior Machine Learning Engineer take-home assignment.

The package supports two workflows:

- validating `bets.csv` against the business rules in the brief
- building customer-level features from each customer's first 20 valid bets

## Project Layout

```text
.
|-- Dockerfile
|-- README.md
|-- data/
|   `-- bets.csv
|-- docs/
|   |-- architecture-diagram.mmd
|   `-- design-note.md
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
    |-- test_build_features.py
    |-- test_cli.py
    `-- test_validate.py
```

## Requirements

- Python 3.11 or newer

## Install

Install the package locally in editable mode:

```bash
python3.11 -m pip install -e .
```

## CLI Usage

Validation:

```bash
bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
```

Feature generation:

```bash
bet-pipeline build-features --input /data/bets.csv --output /outputs/features/
```

The `build-features` command is self-contained: it re-validates the raw input before generating customer features, so it does not require a prior `validate` run.

## Docker

Build the image:

```bash
docker build -t entain-bet-pipeline .
```

Run validation in Docker:

```bash
docker run --rm \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/outputs:/outputs" \
  entain-bet-pipeline validate --input /data/bets.csv --output /outputs/validation/
```

Run feature generation in Docker:

```bash
docker run --rm \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/outputs:/outputs" \
  entain-bet-pipeline build-features --input /data/bets.csv --output /outputs/features/
```

The container image includes only the package code and runtime dependencies. Input data and generated outputs are passed in through mounted `data/` and `outputs/` directories so the workflow stays reproducible and the artifacts remain on the host machine.

## Output Files

Validation outputs:

- `outputs/validation/valid_bets.csv`
- `outputs/validation/invalid_bets.csv`
- `outputs/validation/validation_report.json`

Feature outputs:

- `outputs/features/customer_features.csv`
- `outputs/features/feature_build_report.json`

`customer_features` is emitted as CSV for this submission to keep the package lightweight, dependency-minimal, and easy to inspect manually. Parquet would be a stronger default in a larger production pipeline where schema preservation, compression, and columnar reads matter more.

## Testing

Run the test suite with the standard library test runner:

```bash
PYTHONPATH=src python3.11 -m unittest tests.test_validate tests.test_build_features tests.test_cli
```

The current suite covers:

- validation rule checks and report output
- customer feature aggregation and invalid-bet handling
- CLI parser and command dispatch

## Submission Status

Completed:

- Task 1: validation pipeline
- Task 2: customer feature pipeline
- Task 3: Python packaging, CLI entrypoint, and automated tests
- Task 4: Docker image build and mounted-volume runtime verification
- Task 5: architecture diagram and design note
