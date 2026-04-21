"""CLI scaffold for the Entain bet pipeline package."""

from __future__ import annotations

import argparse
from pathlib import Path

from bet_pipeline.build_features import run_feature_build
from bet_pipeline.validate import run_validation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bet-pipeline",
        description="Scaffold CLI for the Entain Australia junior MLE assignment.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser(
        "validate", help="Validate raw betting data against business rules."
    )
    validate_parser.add_argument("--input", required=True, type=Path)
    validate_parser.add_argument("--output", required=True, type=Path)

    feature_parser = subparsers.add_parser(
        "build-features",
        help="Build customer features from validated betting data.",
    )
    feature_parser.add_argument("--input", required=True, type=Path)
    feature_parser.add_argument("--output", required=True, type=Path)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "validate":
            run_validation(args.input, args.output)
        elif args.command == "build-features":
            run_feature_build(args.input, args.output)
        else:
            parser.error(f"Unsupported command: {args.command}")
    except NotImplementedError as exc:
        print(exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
