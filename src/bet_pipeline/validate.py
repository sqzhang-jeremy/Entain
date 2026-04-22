"""Validation pipeline implementation for the Entain bet dataset."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

REQUIRED_COLUMNS = [
    "bet_id",
    "customer_id",
    "bet_datetime",
    "bet_num",
    "betting_amount",
    "price",
    "category",
    "stake_type",
    "bet_result",
    "payout",
    "return_for_entain",
]

ALLOWED_CATEGORIES = {"sports", "racing"}
ALLOWED_STAKE_TYPES = {"cash", "bonus"}
ALLOWED_BET_RESULTS = {"return", "no-return"}
ZERO = Decimal("0")
DECIMAL_TOLERANCE = Decimal("0.000001")
DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
)
ALL_RULE_NAMES = [
    "bet_id_not_integer",
    "customer_id_missing",
    "bet_datetime_invalid",
    "bet_num_not_integer",
    "bet_num_not_positive",
    "betting_amount_not_numeric",
    "betting_amount_not_gt_0",
    "price_not_numeric",
    "price_not_gt_1",
    "invalid_category",
    "invalid_stake_type",
    "invalid_bet_result",
    "payout_not_numeric",
    "return_for_entain_not_numeric",
    "payout_mismatch",
    "return_for_entain_mismatch",
    "duplicate_bet_id",
    "duplicate_bet_num_for_customer",
    "bet_datetime_out_of_order_for_customer",
]


@dataclass
class ValidationRow:
    """Represents one input row plus parsed values and validation outcomes."""

    line_number: int
    raw: dict[str, str]
    errors: list[str] = field(default_factory=list)
    bet_id: int | None = None
    customer_id: str | None = None
    bet_datetime: datetime | None = None
    bet_num: int | None = None
    betting_amount: Decimal | None = None
    price: Decimal | None = None
    category: str | None = None
    stake_type: str | None = None
    bet_result: str | None = None
    payout: Decimal | None = None
    return_for_entain: Decimal | None = None

    def add_error(self, rule_name: str) -> None:
        if rule_name not in self.errors:
            self.errors.append(rule_name)

    @property
    def is_valid(self) -> bool:
        return not self.errors


def load_validated_rows(input_path: Path) -> tuple[list[ValidationRow], list[str]]:
    """Load input rows and apply all Task 1 validations without writing outputs."""

    rows, fieldnames = _load_rows(input_path)
    _apply_cross_row_validation(rows)
    return rows, fieldnames


def run_validation(input_path: Path, output_dir: Path) -> None:
    """Validate the raw bet CSV and emit valid/invalid outputs plus a report."""

    rows, fieldnames = load_validated_rows(input_path)

    valid_rows = [row for row in rows if row.is_valid]
    invalid_rows = [row for row in rows if not row.is_valid]

    output_dir.mkdir(parents=True, exist_ok=True)
    valid_path = output_dir / "valid_bets.csv"
    invalid_path = output_dir / "invalid_bets.csv"
    report_path = output_dir / "validation_report.json"

    _write_valid_rows(valid_path, fieldnames, valid_rows)
    _write_invalid_rows(invalid_path, fieldnames, invalid_rows)
    _write_report(report_path, input_path, valid_path, invalid_path, rows)


def _load_rows(input_path: Path) -> tuple[list[ValidationRow], list[str]]:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        _validate_columns(fieldnames)

        rows: list[ValidationRow] = []
        for line_number, raw in enumerate(reader, start=2):
            row = ValidationRow(line_number=line_number, raw=raw)
            _parse_and_validate_row(row)
            rows.append(row)

    return rows, fieldnames


def _validate_columns(fieldnames: list[str]) -> None:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Input file is missing required columns: {missing}")


def _parse_and_validate_row(row: ValidationRow) -> None:
    raw = row.raw

    row.bet_id = _parse_int(row, "bet_id", "bet_id_not_integer")
    row.customer_id = raw.get("customer_id") or None
    if not row.customer_id:
        row.add_error("customer_id_missing")

    row.bet_datetime = _parse_datetime(row, "bet_datetime", "bet_datetime_invalid")
    row.bet_num = _parse_int(row, "bet_num", "bet_num_not_integer")
    if row.bet_num is not None and row.bet_num <= 0:
        row.add_error("bet_num_not_positive")

    row.betting_amount = _parse_decimal(
        row, "betting_amount", "betting_amount_not_numeric"
    )
    if row.betting_amount is not None and row.betting_amount <= ZERO:
        row.add_error("betting_amount_not_gt_0")

    row.price = _parse_decimal(row, "price", "price_not_numeric")
    if row.price is not None and row.price <= Decimal("1"):
        row.add_error("price_not_gt_1")

    row.category = raw.get("category") or None
    if row.category not in ALLOWED_CATEGORIES:
        row.add_error("invalid_category")

    row.stake_type = raw.get("stake_type") or None
    if row.stake_type not in ALLOWED_STAKE_TYPES:
        row.add_error("invalid_stake_type")

    row.bet_result = raw.get("bet_result") or None
    if row.bet_result not in ALLOWED_BET_RESULTS:
        row.add_error("invalid_bet_result")

    row.payout = _parse_decimal(row, "payout", "payout_not_numeric")
    row.return_for_entain = _parse_decimal(
        row, "return_for_entain", "return_for_entain_not_numeric"
    )

    _validate_derived_values(row)


def _parse_int(row: ValidationRow, column: str, error_name: str) -> int | None:
    value = row.raw.get(column, "")
    try:
        return int(value)
    except (TypeError, ValueError):
        row.add_error(error_name)
        return None


def _parse_decimal(row: ValidationRow, column: str, error_name: str) -> Decimal | None:
    value = row.raw.get(column, "")
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError):
        row.add_error(error_name)
        return None


def _parse_datetime(
    row: ValidationRow, column: str, error_name: str
) -> datetime | None:
    value = row.raw.get(column, "")
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except (TypeError, ValueError):
            continue

    row.add_error(error_name)
    return None


def _validate_derived_values(row: ValidationRow) -> None:
    required_values = (
        row.betting_amount,
        row.price,
        row.stake_type,
        row.bet_result,
        row.payout,
        row.return_for_entain,
    )
    if any(value is None for value in required_values):
        return

    if row.stake_type not in ALLOWED_STAKE_TYPES or row.bet_result not in ALLOWED_BET_RESULTS:
        return

    expected_payout = _expected_payout(
        betting_amount=row.betting_amount,
        price=row.price,
        stake_type=row.stake_type,
        bet_result=row.bet_result,
    )
    if not _decimal_matches(row.payout, expected_payout):
        row.add_error("payout_mismatch")

    expected_return = _expected_return_for_entain(
        betting_amount=row.betting_amount,
        payout=expected_payout,
        stake_type=row.stake_type,
        bet_result=row.bet_result,
    )
    if not _decimal_matches(row.return_for_entain, expected_return):
        row.add_error("return_for_entain_mismatch")


def _decimal_matches(actual: Decimal, expected: Decimal) -> bool:
    return abs(actual - expected) <= DECIMAL_TOLERANCE


def _expected_payout(
    *,
    betting_amount: Decimal,
    price: Decimal,
    stake_type: str,
    bet_result: str,
) -> Decimal:
    if bet_result == "no-return":
        return ZERO
    if stake_type == "cash":
        return betting_amount * price
    return betting_amount * (price - Decimal("1"))


def _expected_return_for_entain(
    *,
    betting_amount: Decimal,
    payout: Decimal,
    stake_type: str,
    bet_result: str,
) -> Decimal:
    if bet_result == "no-return" and stake_type == "cash":
        return betting_amount
    if bet_result == "no-return" and stake_type == "bonus":
        return ZERO
    if bet_result == "return" and stake_type == "cash":
        return betting_amount - payout
    return -payout


def _apply_cross_row_validation(rows: list[ValidationRow]) -> None:
    bet_id_counts = Counter(row.bet_id for row in rows if row.bet_id is not None)
    for row in rows:
        if row.bet_id is not None and bet_id_counts[row.bet_id] > 1:
            row.add_error("duplicate_bet_id")

    customers: dict[str, list[ValidationRow]] = defaultdict(list)
    for row in rows:
        if row.customer_id:
            customers[row.customer_id].append(row)

    for customer_rows in customers.values():
        _validate_customer_sequence(customer_rows)


def _validate_customer_sequence(customer_rows: list[ValidationRow]) -> None:
    bet_num_counts = Counter(row.bet_num for row in customer_rows if row.bet_num is not None)
    for row in customer_rows:
        if row.bet_num is not None and bet_num_counts[row.bet_num] > 1:
            row.add_error("duplicate_bet_num_for_customer")

    ordered_rows = [
        row
        for row in customer_rows
        if row.bet_num is not None and row.bet_datetime is not None
    ]
    ordered_rows.sort(key=lambda row: (row.bet_num, row.line_number))

    latest_datetime: datetime | None = None
    for row in ordered_rows:
        if latest_datetime is not None and row.bet_datetime < latest_datetime:
            row.add_error("bet_datetime_out_of_order_for_customer")
            continue
        latest_datetime = row.bet_datetime


def _write_valid_rows(
    output_path: Path, fieldnames: list[str], rows: list[ValidationRow]
) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.raw)


def _write_invalid_rows(
    output_path: Path, fieldnames: list[str], rows: list[ValidationRow]
) -> None:
    invalid_fieldnames = [*fieldnames, "source_line_number", "validation_errors"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=invalid_fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row.raw)
            payload["source_line_number"] = row.line_number
            payload["validation_errors"] = ";".join(sorted(row.errors))
            writer.writerow(payload)


def _write_report(
    output_path: Path,
    input_path: Path,
    valid_path: Path,
    invalid_path: Path,
    rows: list[ValidationRow],
) -> None:
    valid_rows = sum(1 for row in rows if row.is_valid)
    invalid_rows = len(rows) - valid_rows

    failure_counts = Counter({rule_name: 0 for rule_name in ALL_RULE_NAMES})
    for row in rows:
        for error in row.errors:
            failure_counts[error] += 1

    report = {
        "input_file": str(input_path),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(rows),
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "failure_counts_by_rule": dict(sorted(failure_counts.items())),
        "output_files": {
            "valid_bets": str(valid_path),
            "invalid_bets": str(invalid_path),
        },
    }

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
