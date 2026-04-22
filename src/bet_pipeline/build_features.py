"""Customer feature generation for the Entain bet dataset."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from bet_pipeline.validate import ValidationRow, load_validated_rows

FEATURE_FIELDNAMES = [
    "customer_id",
    "first_bet_datetime",
    "twentieth_bet_datetime",
    "bets_used",
    "total_betting_amount",
    "mean_betting_amount",
    "mean_price",
    "pct_racing",
    "pct_cash",
    "pct_return",
    "total_payout",
    "total_return_for_entain",
    "feature_generated_at",
]

FEATURE_WINDOW_SIZE = 20
OUTPUT_DECIMAL_PLACES = Decimal("0.000001")


def run_feature_build(input_path: Path, output_dir: Path) -> None:
    """Build customer-level features from validated betting rows."""

    rows, _ = load_validated_rows(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    grouped_valid_rows, customers_with_invalid_in_first_20 = _group_rows_for_features(rows)
    feature_generated_at = datetime.now(timezone.utc).isoformat()

    feature_rows = []
    skipped_customers = 0
    for customer_id, customer_rows in grouped_valid_rows.items():
        ordered_rows = sorted(
            customer_rows,
            key=lambda row: (row.bet_num, row.bet_datetime, row.line_number),
        )
        selected_rows = ordered_rows[:FEATURE_WINDOW_SIZE]
        if len(selected_rows) < FEATURE_WINDOW_SIZE:
            skipped_customers += 1
            continue

        feature_rows.append(
            _build_feature_row(
                customer_id=customer_id,
                rows=selected_rows,
                feature_generated_at=feature_generated_at,
            )
        )

    features_path = output_dir / "customer_features.csv"
    report_path = output_dir / "feature_build_report.json"

    _write_feature_rows(features_path, feature_rows)
    _write_feature_report(
        report_path=report_path,
        input_path=input_path,
        output_path=features_path,
        rows=rows,
        emitted_customers=len(feature_rows),
        skipped_customers=skipped_customers,
        customers_with_invalid_in_first_20=customers_with_invalid_in_first_20,
        feature_generated_at=feature_generated_at,
    )


def _group_rows_for_features(
    rows: list[ValidationRow],
) -> tuple[dict[str, list[ValidationRow]], int]:
    grouped_valid_rows: dict[str, list[ValidationRow]] = defaultdict(list)
    customers_with_invalid_in_first_20: set[str] = set()

    for row in rows:
        if row.customer_id and row.bet_num is not None and row.bet_num <= FEATURE_WINDOW_SIZE:
            if not row.is_valid:
                customers_with_invalid_in_first_20.add(row.customer_id)

        if not _is_feature_eligible_row(row):
            continue

        grouped_valid_rows[row.customer_id].append(row)

    return grouped_valid_rows, len(customers_with_invalid_in_first_20)


def _is_feature_eligible_row(row: ValidationRow) -> bool:
    return (
        row.is_valid
        and row.customer_id is not None
        and row.bet_num is not None
        and row.bet_datetime is not None
        and row.betting_amount is not None
        and row.price is not None
        and row.category is not None
        and row.stake_type is not None
        and row.bet_result is not None
        and row.payout is not None
        and row.return_for_entain is not None
    )


def _build_feature_row(
    *, customer_id: str, rows: list[ValidationRow], feature_generated_at: str
) -> dict[str, str]:
    bets_used = len(rows)
    total_betting_amount = sum(row.betting_amount for row in rows)
    total_price = sum(row.price for row in rows)
    total_payout = sum(row.payout for row in rows)
    total_return_for_entain = sum(row.return_for_entain for row in rows)

    return {
        "customer_id": customer_id,
        "first_bet_datetime": rows[0].bet_datetime.isoformat(timespec="microseconds"),
        "twentieth_bet_datetime": rows[-1].bet_datetime.isoformat(timespec="microseconds"),
        "bets_used": str(bets_used),
        "total_betting_amount": _decimal_to_str(total_betting_amount),
        "mean_betting_amount": _decimal_to_str(total_betting_amount / bets_used),
        "mean_price": _decimal_to_str(total_price / bets_used),
        "pct_racing": _decimal_to_str(_ratio(sum(row.category == "racing" for row in rows), bets_used)),
        "pct_cash": _decimal_to_str(_ratio(sum(row.stake_type == "cash" for row in rows), bets_used)),
        "pct_return": _decimal_to_str(_ratio(sum(row.bet_result == "return" for row in rows), bets_used)),
        "total_payout": _decimal_to_str(total_payout),
        "total_return_for_entain": _decimal_to_str(total_return_for_entain),
        "feature_generated_at": feature_generated_at,
    }


def _ratio(numerator: int, denominator: int) -> Decimal:
    return Decimal(numerator) / Decimal(denominator)


def _decimal_to_str(value: Decimal) -> str:
    text = format(value.quantize(OUTPUT_DECIMAL_PLACES), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _write_feature_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FEATURE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _write_feature_report(
    *,
    report_path: Path,
    input_path: Path,
    output_path: Path,
    rows: list[ValidationRow],
    emitted_customers: int,
    skipped_customers: int,
    customers_with_invalid_in_first_20: int,
    feature_generated_at: str,
) -> None:
    customer_ids = {row.customer_id for row in rows if row.customer_id}
    report = {
        "input_file": str(input_path),
        "generated_at": feature_generated_at,
        "output_files": {
            "customer_features": str(output_path),
        },
        "output_format": "csv",
        "output_format_reason": "CSV emitted to avoid adding a parquet dependency for this local batch task.",
        "total_customers_in_input": len(customer_ids),
        "customers_with_invalid_bets_within_raw_first_20_by_bet_num": customers_with_invalid_in_first_20,
        "customers_emitted": emitted_customers,
        "customers_skipped_for_insufficient_valid_bets": skipped_customers,
        "invalid_bets_within_first_20_handling": (
            "If a customer has invalid bets within raw bet_num positions 1-20, those bets are skipped "
            "and the feature window is extended forward until 20 valid bets are collected."
        ),
        "feature_window_policy": (
            "Features are built from the first 20 valid bets after Task 1 validation, "
            "ordered by bet_num and then bet_datetime."
        ),
    }

    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
