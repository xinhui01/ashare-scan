import unittest

import pandas as pd

from scan_models import FilterSettings
from src.models.analysis_models import HistoryAnalysisConfig
from src.services.history_analysis_service import HistoryAnalysisService


class HistoryAnalysisConfigTests(unittest.TestCase):
    def test_from_filter_settings_applies_overrides_and_clamps_values(self):
        settings = FilterSettings(
            trend_days=5,
            ma_period=5,
            limit_up_lookback_days=5,
            volume_lookback_days=5,
            volume_expand_enabled=True,
            volume_expand_factor=2.0,
        )

        config = HistoryAnalysisConfig.from_filter_settings(
            settings,
            trend_days=0,
            ma_period=3,
            volume_expand_enabled=False,
            volume_expand_factor=0.5,
        )

        self.assertEqual(config.trend_days, 1)
        self.assertEqual(config.ma_period, 3)
        self.assertFalse(config.volume_expand_enabled)
        self.assertEqual(config.volume_expand_factor, 1.0)


class HistoryAnalysisServiceTests(unittest.TestCase):
    def test_analyze_history_handles_unsorted_rows_and_keeps_stock_code(self):
        config = HistoryAnalysisConfig.from_filter_settings(FilterSettings(trend_days=2, ma_period=2))
        service = HistoryAnalysisService(config)
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-03", "close": 12.0, "change_pct": 3.0, "volume": 300},
                {"date": "2026-04-01", "close": 10.0, "change_pct": 1.0, "volume": 100},
                {"date": "2026-04-02", "close": 11.0, "change_pct": 2.0, "volume": 200},
                {"date": "2026-04-07", "close": 13.0, "change_pct": 4.0, "volume": 400},
            ]
        )

        result = service.analyze_history(
            history_df,
            board="主板",
            stock_name="平安银行",
            stock_code="1",
        )

        self.assertTrue(result["passed"])
        self.assertEqual(result["latest_date"], "2026-04-07")
        self.assertEqual(result["stock_code"], "000001")

    def test_limit_up_threshold_distinguishes_st_and_growth_board(self):
        service = HistoryAnalysisService(
            HistoryAnalysisConfig.from_filter_settings(FilterSettings())
        )

        self.assertEqual(service.limit_up_threshold(board="主板", stock_name="ST测试"), 5.0)
        self.assertEqual(service.limit_up_threshold(board="创业板", stock_name="普通股"), 20.0)
        self.assertEqual(service.limit_up_threshold(board="主板", stock_name="普通股"), 10.0)


if __name__ == "__main__":
    unittest.main()
