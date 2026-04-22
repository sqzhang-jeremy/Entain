from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bet_pipeline import cli


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


def _make_row(*, bet_id: int, customer_id: str, bet_num: int) -> dict[str, str]:
    day = ((bet_num - 1) % 28) + 1
    return {
        "bet_id": str(bet_id),
        "customer_id": customer_id,
        "bet_datetime": f"2024-01-{day:02d} 10:00:00",
        "bet_num": str(bet_num),
        "betting_amount": "10",
        "price": "2",
        "category": "sports",
        "stake_type": "cash",
        "bet_result": "no-return",
        "payout": "0",
        "return_for_entain": "10",
    }


class CliTests(unittest.TestCase):
    def test_main_requires_a_subcommand(self) -> None:
        with self.assertRaises(SystemExit) as exc_info:
            cli.main([])

        self.assertEqual(exc_info.exception.code, 2)

    def test_main_dispatches_validate_command(self) -> None:
        with patch("bet_pipeline.cli.run_validation") as mock_validate:
            exit_code = cli.main(["validate", "--input", "bets.csv", "--output", "outputs/validation"])

        self.assertEqual(exit_code, 0)
        mock_validate.assert_called_once_with(Path("bets.csv"), Path("outputs/validation"))

    def test_main_dispatches_build_features_command(self) -> None:
        with patch("bet_pipeline.cli.run_feature_build") as mock_build_features:
            exit_code = cli.main(
                ["build-features", "--input", "bets.csv", "--output", "outputs/features"]
            )

        self.assertEqual(exit_code, 0)
        mock_build_features.assert_called_once_with(Path("bets.csv"), Path("outputs/features"))

    def test_main_runs_validate_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "validation"
            _write_csv(
                input_path,
                [
                    _make_row(bet_id=1, customer_id="customer-1", bet_num=1),
                    _make_row(bet_id=2, customer_id="customer-1", bet_num=2),
                ],
            )

            exit_code = cli.main(
                ["validate", "--input", str(input_path), "--output", str(output_dir)]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "valid_bets.csv").exists())
            self.assertTrue((output_dir / "invalid_bets.csv").exists())
            self.assertTrue((output_dir / "validation_report.json").exists())

    def test_main_runs_build_features_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "features"
            rows = [_make_row(bet_id=index, customer_id="customer-1", bet_num=index) for index in range(1, 21)]
            _write_csv(input_path, rows)

            exit_code = cli.main(
                ["build-features", "--input", str(input_path), "--output", str(output_dir)]
            )

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "customer_features.csv").exists())
            self.assertTrue((output_dir / "feature_build_report.json").exists())


if __name__ == "__main__":
    unittest.main()
