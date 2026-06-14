import math
import sys
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from convert_to_json import normalize_price
from scrapping_bpn import parse_prices
from sql import build_combined


class PipelineTests(unittest.TestCase):
    def test_parse_prices_rejects_missing_current_price(self):
        payload = {
            "status": "success",
            "data": [
                {"name": "Beras Medium", "today": None},
                {"name": "Beras Premium", "today": 15000},
            ],
        }

        with self.assertRaisesRegex(ValueError, "kosong"):
            parse_prices(payload)

    def test_combined_preserves_missing_source_price(self):
        silinda = pd.DataFrame(
            [
                {"date": "2026-06-13", "medium": 13000, "premium": 14000},
                {"date": "2026-06-14", "medium": 13100, "premium": 14100},
            ]
        )
        bapanas = pd.DataFrame(
            [
                {"date": "2026-06-13", "medium": 13500, "premium": 14500},
            ]
        )

        combined = build_combined(silinda, bapanas)
        latest = combined.iloc[-1]

        self.assertTrue(math.isnan(latest["medium_bapanas"]))
        self.assertTrue(math.isnan(latest["premium_bapanas"]))
        self.assertTrue(
            all(
                math.isfinite(value)
                for value in latest[
                    [
                        "medium_silinda",
                        "premium_silinda",
                    ]
                ]
            )
        )

    def test_non_finite_price_becomes_json_null(self):
        self.assertIsNone(normalize_price(float("nan")))
        self.assertIsNone(normalize_price(float("inf")))


if __name__ == "__main__":
    unittest.main()
