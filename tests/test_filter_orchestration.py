import unittest

import pandas as pd

from stock_filter import StockFilter


class DummyFetcher:
    def __init__(self, history_df):
        self.history_df = history_df

    def get_history_data(self, *args, **kwargs):
        return self.history_df


def build_filter(history_df, require_limit_up_within_days=False) -> StockFilter:
    from scan_models import FilterSettings
    stock_filter = StockFilter.__new__(StockFilter)
    stock_filter.fetcher = DummyFetcher(history_df)
    stock_filter._log = None
    stock_filter.apply_settings(FilterSettings(
        trend_days=2,
        ma_period=2,
        limit_up_lookback_days=3,
        volume_lookback_days=2,
        volume_expand_enabled=False,
        volume_expand_factor=2.0,
        require_limit_up_within_days=require_limit_up_within_days,
    ))
    return stock_filter


class FilterOrchestrationTests(unittest.TestCase):
    def test_filter_stock_returns_history_error_when_history_is_empty(self):
        stock_filter = build_filter(pd.DataFrame())

        result = stock_filter.filter_stock("000001", "平安银行", board="主板", exchange="SZ")

        self.assertFalse(result["passed"])
        self.assertEqual(result["reasons"], ["无法获取历史数据"])
        self.assertEqual(result["data"]["board"], "主板")
        self.assertEqual(result["data"]["exchange"], "SZ")

    def test_filter_stock_blocks_when_limit_up_requirement_is_not_met(self):
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-01", "close": 10.0, "change_pct": 1.0, "volume": 100},
                {"date": "2026-04-02", "close": 11.0, "change_pct": 2.0, "volume": 200},
                {"date": "2026-04-03", "close": 12.0, "change_pct": 3.0, "volume": 300},
                {"date": "2026-04-07", "close": 13.0, "change_pct": 4.0, "volume": 400},
            ]
        )
        stock_filter = build_filter(history_df, require_limit_up_within_days=True)

        result = stock_filter.filter_stock("000001", "平安银行")

        self.assertFalse(result["passed"])
        self.assertIn("未命中过去3个交易日涨停条件", result["reasons"][0])
        self.assertFalse(result["data"]["analysis"]["limit_up_within_days"])

    def test_filter_stock_marks_passed_result_when_analysis_passes(self):
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-01", "close": 10.0, "change_pct": 1.0, "volume": 100},
                {"date": "2026-04-02", "close": 11.0, "change_pct": 2.0, "volume": 200},
                {"date": "2026-04-03", "close": 12.0, "change_pct": 3.0, "volume": 300},
                {"date": "2026-04-07", "close": 13.0, "change_pct": 4.0, "volume": 400},
            ]
        )
        stock_filter = build_filter(history_df, require_limit_up_within_days=False)

        result = stock_filter.filter_stock("000001", "平安银行")

        self.assertTrue(result["passed"])
        self.assertIn("最近2日收盘全部高于MA2", result["reasons"][0])
        self.assertEqual(len(result["data"]["history_tail"]), 3)


if __name__ == "__main__":
    unittest.main()
