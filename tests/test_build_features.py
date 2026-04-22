from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from bet_pipeline.build_features import run_feature_build


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


def _make_row(
    *,
    bet_id: int,
    customer_id: str,
    bet_num: int,
    betting_amount: str = "10",
    price: str = "2",
    category: str = "sports",
    stake_type: str = "cash",
    bet_result: str = "no-return",
    payout: str = "0",
    return_for_entain: str = "10",
) -> dict[str, str]:
    day = ((bet_num - 1) % 28) + 1
    return {
        "bet_id": str(bet_id),
        "customer_id": customer_id,
        "bet_datetime": f"2024-01-{day:02d} 10:00:00",
        "bet_num": str(bet_num),
        "betting_amount": betting_amount,
        "price": price,
        "category": category,
        "stake_type": stake_type,
        "bet_result": bet_result,
        "payout": payout,
        "return_for_entain": return_for_entain,
    }


class BuildFeaturesTests(unittest.TestCase):
    def test_build_features_aggregates_first_20_valid_bets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "features"
            rows = [
                _make_row(
                    bet_id=index,
                    customer_id="customer-1",
                    bet_num=index,
                    betting_amount=str(index),
                    price="2",
                    category="racing" if index % 2 == 0 else "sports",
                    stake_type="cash" if index <= 10 else "bonus",
                    bet_result="return" if index <= 5 else "no-return",
                    payout=str(index * 2) if index <= 5 else "0",
                    return_for_entain=str(-index) if index <= 5 else (str(index) if index <= 10 else "0"),
                )
                for index in range(1, 21)
            ]
            _write_csv(input_path, rows)

            run_feature_build(input_path, output_dir)

            feature_rows = _read_csv(output_dir / "customer_features.csv")
            self.assertEqual(len(feature_rows), 1)
            feature_row = feature_rows[0]
            self.assertEqual(feature_row["customer_id"], "customer-1")
            self.assertEqual(feature_row["bets_used"], "20")
            self.assertEqual(feature_row["total_betting_amount"], "210")
            self.assertEqual(feature_row["mean_betting_amount"], "10.5")
            self.assertEqual(feature_row["mean_price"], "2")
            self.assertEqual(feature_row["pct_racing"], "0.5")
            self.assertEqual(feature_row["pct_cash"], "0.5")
            self.assertEqual(feature_row["pct_return"], "0.25")
            self.assertEqual(feature_row["total_payout"], "30")
            self.assertEqual(feature_row["total_return_for_entain"], "25")

    def test_build_features_skips_invalid_bets_and_extends_window_to_next_valid_bets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "features"
            rows = [_make_row(bet_id=index, customer_id="customer-1", bet_num=index) for index in range(1, 22)]
            rows[4]["price"] = "1"
            _write_csv(input_path, rows)

            run_feature_build(input_path, output_dir)

            feature_rows = _read_csv(output_dir / "customer_features.csv")
            self.assertEqual(len(feature_rows), 1)
            feature_row = feature_rows[0]
            self.assertEqual(feature_row["first_bet_datetime"], "2024-01-01T10:00:00.000000")
            self.assertEqual(feature_row["twentieth_bet_datetime"], "2024-01-21T10:00:00.000000")

            report = json.loads((output_dir / "feature_build_report.json").read_text())
            self.assertEqual(report["customers_with_invalid_bets_within_raw_first_20_by_bet_num"], 1)
            self.assertEqual(report["customers_emitted"], 1)
            self.assertEqual(report["customers_skipped_for_insufficient_valid_bets"], 0)
            self.assertIn("extended forward", report["invalid_bets_within_first_20_handling"])

    def test_build_features_skips_customers_with_fewer_than_20_valid_bets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            input_path = tmp_path / "bets.csv"
            output_dir = tmp_path / "features"
            rows = [_make_row(bet_id=index, customer_id="customer-1", bet_num=index) for index in range(1, 20)]
            _write_csv(input_path, rows)

            run_feature_build(input_path, output_dir)

            feature_rows = _read_csv(output_dir / "customer_features.csv")
            self.assertEqual(feature_rows, [])

            report = json.loads((output_dir / "feature_build_report.json").read_text())
            self.assertEqual(report["customers_emitted"], 0)
            self.assertEqual(report["customers_skipped_for_insufficient_valid_bets"], 1)


if __name__ == "__main__":
    unittest.main()
