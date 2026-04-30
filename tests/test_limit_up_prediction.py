import unittest

import pandas as pd

from scan_models import FilterSettings
from stock_filter import StockFilter


class _CompareFetcher:
    def _recent_trade_dates(self, trade_date, count):
        return ["20260421", "20260422", "20260423"]

    def compare_limit_up_pools(self, today_date, yesterday_date):
        mapping = {
            ("20260422", "20260421"): {
                "yesterday_first": [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}],
                "continued_codes": ["000001"],
                "lost_codes": ["000002", "000003"],
                "today_first": [{"code": "000010"}, {"code": "000011"}],
            },
            ("20260423", "20260422"): {
                "yesterday_first": [{"code": "000010"}, {"code": "000011"}],
                "continued_codes": ["000010"],
                "lost_codes": ["000011"],
                "today_first": [{"code": "000020"}],
            },
        }
        return mapping[(today_date, yesterday_date)]


class _HistoryFetcher:
    def get_history_data(self, code, days=65, force_refresh=False, request_plan=None):
        return pd.DataFrame({
            "date": [
                "2026-04-08", "2026-04-09", "2026-04-10", "2026-04-11", "2026-04-12",
                "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
                "2026-04-18",
            ],
            "open": [8.95, 9.05, 9.15, 9.30, 9.50, 9.80, 10.65, 10.50, 10.40, 10.35, 10.32],
            "high": [9.05, 9.15, 9.25, 9.45, 9.75, 10.67, 10.70, 10.55, 10.42, 10.38, 10.48],
            "low":  [8.90, 9.00, 9.10, 9.25, 9.45, 9.78, 10.45, 10.38, 10.30, 10.28, 10.30],
            "close": [9.00, 9.10, 9.20, 9.40, 9.70, 10.67, 10.50, 10.40, 10.35, 10.30, 10.45],
            "change_pct": [0.5, 1.1, 1.1, 2.2, 3.2, 10.0, -1.6, -0.95, -0.48, -0.48, 1.46],
            "volume": [1_000_000, 1_100_000, 1_150_000, 1_200_000, 1_300_000, 6_000_000, 2_300_000, 1_800_000, 1_550_000, 1_420_000, 1_350_000],
            "amount": [9_000_000, 10_000_000, 10_500_000, 11_300_000, 12_600_000, 64_000_000, 24_000_000, 18_700_000, 16_000_000, 14_600_000, 14_100_000],
        })


class TestLimitUpPredictionHelpers(unittest.TestCase):
    def _build_filter(self) -> StockFilter:
        f = StockFilter.__new__(StockFilter)
        f._log = None
        f.apply_settings(FilterSettings(
            strong_ft_enabled=True,
            strong_ft_max_pullback_pct=3.0,
            strong_ft_max_volume_ratio=0.7,
            strong_ft_min_hold_days=1,
            limit_up_lookback_days=5,
        ))
        return f

    def test_build_compare_market_context(self):
        f = self._build_filter()
        f.fetcher = _CompareFetcher()

        ctx = f._build_compare_market_context("20260423", 2)

        self.assertEqual(ctx["pair_count"], 2)
        self.assertEqual(ctx["latest_continuation_rate"], 50.0)
        self.assertEqual(ctx["avg_continuation_rate"], 41.6)

    def test_parse_spot_record_keeps_industry(self):
        row = pd.Series({
            "代码": "000001",
            "名称": "平安银行",
            "最新价": 12.3,
            "涨跌幅": 4.2,
            "成交额": 80_000_000,
            "成交量": 1_500_000,
            "换手率": 5.6,
            "所属行业": "银行",
        })

        rec = StockFilter._parse_spot_record(row, set())

        self.assertIsNotNone(rec)
        self.assertEqual(rec["industry"], "银行")

    def test_score_followthrough_candidate_hits_recent_burst_pullback(self):
        f = self._build_filter()
        f.fetcher = _HistoryFetcher()

        rec = {
            "code": "000001",
            "name": "测试股",
            "industry": "机器人",
            "close": 10.45,
            "change_pct": 1.46,
            "turnover": 8.0,
        }

        result = f._score_followthrough_candidate(
            rec, {"机器人": 3}, {}, lookback_days=5,
        )

        self.assertIsNotNone(result)
        self.assertGreaterEqual(result["score"], 50)
        self.assertEqual(result["predict_type"], "五日承接")
        self.assertEqual(result["burst_date"], "2026-04-13")
        self.assertEqual(result["days_since_burst"], 5)
        self.assertIsNotNone(result["dist_ma5_pct"])
        self.assertGreater(result["dist_ma5_pct"], 0)
        self.assertIn("爆量", result["reasons"])


if __name__ == "__main__":
    unittest.main()
