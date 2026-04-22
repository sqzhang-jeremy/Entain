from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from bet_pipeline.validate import run_validation


FIELDNAMES = [
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


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class RunValidationTests(unittest.TestCase):
    def test_run_validation_accepts_datetime_without_fractional_seconds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            _write_csv(
                input_path,
                [
                    {
                        "bet_id": "1",
                        "customer_id": "customer-1",
                        "bet_datetime": "2024-09-19 02:38:45",
                        "bet_num": "1",
                        "betting_amount": "10.0",
                        "price": "2.5",
                        "category": "sports",
                        "stake_type": "cash",
                        "bet_result": "return",
                        "payout": "25.0",
                        "return_for_entain": "-15.0",
                    }
                ],
            )

            run_validation(input_path, output_dir)

            report = json.loads((output_dir / "validation_report.json").read_text())
            self.assertEqual(report["invalid_rows"], 0)
            self.assertEqual(report["failure_counts_by_rule"]["bet_datetime_invalid"], 0)
            self.assertEqual(len(_read_csv(output_dir / "valid_bets.csv")), 1)

    def test_validation_report_lists_zero_count_rules(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            _write_csv(
                input_path,
                [
                    {
                        "bet_id": "1",
                        "customer_id": "customer-1",
                        "bet_datetime": "2024-09-19 02:38:45.000",
                        "bet_num": "1",
                        "betting_amount": "10.0",
                        "price": "2.5",
                        "category": "sports",
                        "stake_type": "cash",
                        "bet_result": "no-return",
                        "payout": "0.0",
                        "return_for_entain": "10.0",
                    }
                ],
            )

            run_validation(input_path, output_dir)

            report = json.loads((output_dir / "validation_report.json").read_text())
            self.assertEqual(report["failure_counts_by_rule"]["duplicate_bet_id"], 0)
            self.assertEqual(report["failure_counts_by_rule"]["invalid_category"], 0)
            self.assertEqual(report["failure_counts_by_rule"]["payout_mismatch"], 0)

    def test_run_validation_uses_decimal_tolerance_for_derived_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            _write_csv(
                input_path,
                [
                    {
                        "bet_id": "1",
                        "customer_id": "customer-1",
                        "bet_datetime": "2024-09-19 02:38:45.000",
                        "bet_num": "1",
                        "betting_amount": "5.7",
                        "price": "3.31",
                        "category": "racing",
                        "stake_type": "cash",
                        "bet_result": "return",
                        "payout": "18.867",
                        "return_for_entain": "-13.167000000000002",
                    }
                ],
            )

            run_validation(input_path, output_dir)

            report = json.loads((output_dir / "validation_report.json").read_text())
            self.assertEqual(report["invalid_rows"], 0)
            self.assertEqual(report["failure_counts_by_rule"]["return_for_entain_mismatch"], 0)


if __name__ == "__main__":
    unittest.main()
