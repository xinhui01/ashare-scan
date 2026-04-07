import unittest

import pandas as pd

from stock_filter import StockFilter


def build_filter_for_analysis() -> StockFilter:
    stock_filter = StockFilter.__new__(StockFilter)
    stock_filter.trend_days = 2
    stock_filter.ma_period = 2
    stock_filter.limit_up_lookback_days = 3
    stock_filter.volume_lookback_days = 2
    stock_filter.volume_expand_enabled = False
    stock_filter.volume_expand_factor = 2.0
    stock_filter.require_limit_up_within_days = False
    stock_filter._log = None
    return stock_filter


class AnalysisLogicTests(unittest.TestCase):
    def test_analyze_history_passes_when_recent_closes_stay_above_ma(self):
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-01", "close": 10.0, "change_pct": 1.0, "volume": 100},
                {"date": "2026-04-02", "close": 11.0, "change_pct": 10.0, "volume": 200},
                {"date": "2026-04-03", "close": 12.0, "change_pct": 9.0, "volume": 300},
                {"date": "2026-04-07", "close": 13.0, "change_pct": 8.0, "volume": 400},
            ]
        )

        stock_filter = build_filter_for_analysis()
        analysis = stock_filter.analyze_history(history_df)

        self.assertTrue(analysis["passed"])
        self.assertIn("最近2日收盘全部高于MA2", analysis["summary"])
        self.assertEqual(analysis["latest_date"], "2026-04-07")
        self.assertAlmostEqual(analysis["latest_close"], 13.0, places=2)
        self.assertAlmostEqual(analysis["latest_ma"], 12.5, places=2)

    def test_analyze_history_marks_broken_limit_up_after_two_boards(self):
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-01", "close": 10.0, "change_pct": 10.0, "volume": 100},
                {"date": "2026-04-02", "close": 11.0, "change_pct": 10.0, "volume": 120},
                {"date": "2026-04-03", "close": 10.8, "change_pct": -1.5, "volume": 200},
            ]
        )

        stock_filter = build_filter_for_analysis()
        analysis = stock_filter.analyze_history(history_df)

        self.assertTrue(analysis["broken_limit_up"])
        self.assertEqual(analysis["broken_streak_count"], 2)
        self.assertTrue(analysis["after_two_limit_up"])
        self.assertIn("断板", analysis["summary"])


if __name__ == "__main__":
    unittest.main()
