"""Command-line interface for the Entain bet pipeline package."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from bet_pipeline.build_features import run_feature_build
from bet_pipeline.validate import run_validation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bet-pipeline",
        description="Run validation and customer feature workflows for the Entain betting dataset.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate", help="Validate raw betting data against business rules."
    )
    validate_parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the raw bets CSV file to validate.",
    )
    validate_parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Directory where validation outputs will be written.",
    )

    feature_parser = subparsers.add_parser(
        "build-features",
        help="Build customer features from validated betting data.",
    )
    feature_parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to the raw bets CSV file used to build customer features.",
    )
    feature_parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Directory where customer feature outputs will be written.",
    )

    validate_parser.set_defaults(handler=run_validation)
    feature_parser.set_defaults(handler=run_feature_build)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.handler(args.input, args.output)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
