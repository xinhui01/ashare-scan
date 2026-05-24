import unittest

import pandas as pd

from scan_models import FilterSettings
from stock_filter import StockFilter
from src.services.scoring.predict import _AsOfHistoryFetcher


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
    """二波接力典型形态：4/13 涨停 → 5 日盘整 → 4/18 放量启动 +5.5% 收盘强势。"""
    def get_history_data(self, code, days=65, force_refresh=False, request_plan=None):
        return pd.DataFrame({
            "date": [
                "2026-04-08", "2026-04-09", "2026-04-10", "2026-04-11", "2026-04-12",
                "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17",
                "2026-04-18",
            ],
            "open": [8.95, 9.05, 9.15, 9.30, 9.50, 9.80, 10.65, 10.50, 10.40, 10.35, 10.50],
            "high": [9.05, 9.15, 9.25, 9.45, 9.75, 10.67, 10.70, 10.55, 10.42, 10.38, 10.98],
            "low":  [8.90, 9.00, 9.10, 9.25, 9.45, 9.78, 10.45, 10.38, 10.30, 10.28, 10.50],
            "close": [9.00, 9.10, 9.20, 9.40, 9.70, 10.67, 10.50, 10.40, 10.35, 10.30, 10.95],
            "change_pct": [0.5, 1.1, 1.1, 2.2, 3.2, 10.0, -1.6, -0.95, -0.48, -0.48, 5.5],
            "volume": [1_000_000, 1_100_000, 1_150_000, 1_200_000, 1_300_000, 6_000_000, 2_300_000, 1_800_000, 1_550_000, 1_420_000, 7_000_000],
            "amount": [9_000_000, 10_000_000, 10_500_000, 11_300_000, 12_600_000, 64_000_000, 24_000_000, 18_700_000, 16_000_000, 14_600_000, 76_000_000],
        })


class _WrapHistoryFetcher:
    """5/31 这类中期断板反包：前涨停不在 5 日内，但仍应识别。"""
    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return pd.DataFrame({
            "date": [
                "2026-05-19",
                "2026-05-20",
                "2026-05-21",
                "2026-05-22",
                "2026-05-25",
                "2026-05-26",
                "2026-05-27",
                "2026-05-28",
                "2026-05-29",
                "2026-05-31",
            ],
            "open":  [4.00, 4.38, 4.84, 5.20, 4.70, 4.45, 4.55, 4.42, 4.50, 4.55],
            "high":  [4.05, 4.40, 4.88, 5.22, 4.72, 4.58, 4.60, 4.55, 4.58, 4.62],
            "low":   [3.96, 4.00, 4.36, 4.70, 4.28, 4.30, 4.38, 4.35, 4.45, 4.48],
            "close": [4.00, 4.40, 4.84, 4.48, 4.44, 4.52, 4.42, 4.50, 4.55, 4.50],
            "change_pct": [0.0, 10.0, 10.0, -7.4, -0.9, 1.8, -2.2, 1.8, 1.1, -1.1],
            "volume": [400_000_000, 520_000_000, 910_000_000, 650_000_000, 430_000_000, 470_000_000, 420_000_000, 390_000_000, 360_000_000, 380_000_000],
            "amount": [1_700_000_000, 2_200_000_000, 3_900_000_000, 2_700_000_000, 1_800_000_000, 1_900_000_000, 1_700_000_000, 1_600_000_000, 1_500_000_000, 1_600_000_000],
        })


class _WeakDropWrapHistoryFetcher:
    """弱断板但放量承接：不出现 -3% 以上深阴线，也应保留进反包评分。"""
    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return pd.DataFrame({
            "date": [
                "2026-05-13",
                "2026-05-14",
                "2026-05-15",
                "2026-05-16",
                "2026-05-17",
                "2026-05-18",
                "2026-05-19",
                "2026-05-20",
                "2026-05-21",
            ],
            "open":  [8.20, 8.30, 8.40, 8.50, 8.95, 9.00, 9.90, 10.89, 10.62],
            "high":  [8.30, 8.40, 8.50, 8.60, 9.05, 9.10, 10.89, 11.98, 10.75],
            "low":   [8.15, 8.20, 8.30, 8.40, 8.90, 8.95, 9.88, 10.86, 10.50],
            "close": [8.25, 8.35, 8.45, 8.55, 9.00, 9.90, 10.89, 11.98, 11.68],
            "change_pct": [0.0, 1.2, 1.2, 1.2, 10.0, 10.0, 10.0, 10.0, -2.5],
            "volume": [80_000_000, 82_000_000, 84_000_000, 86_000_000, 90_000_000, 100_000_000, 320_000_000, 610_000_000, 950_000_000],
            "amount": [660_000_000, 680_000_000, 710_000_000, 730_000_000, 810_000_000, 900_000_000, 3_200_000_000, 6_100_000_000, 9_500_000_000],
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

    def test_score_followthrough_candidate_hits_relay_breakout(self):
        """二波接力：今日放量启动 +5.5% 且收盘强势，距前涨停 5 日。"""
        f = self._build_filter()
        f.fetcher = _HistoryFetcher()

        rec = {
            "code": "000001",
            "name": "测试股",
            "industry": "机器人",
            "close": 10.95,
            "change_pct": 5.5,
            "turnover": 12.0,
        }

        result = f._score_followthrough_candidate(
            rec, {"机器人": 3}, {}, lookback_days=5,
        )

        self.assertIsNotNone(result)
        self.assertGreaterEqual(result["score"], 50)
        self.assertEqual(result["predict_type"], "二波接力")
        self.assertEqual(result["prior_lu_date"], "2026-04-13")
        self.assertEqual(result["days_since_prior_lu"], 5)
        # 兼容旧字段名（GUI 仍读 burst_date / days_since_burst）
        self.assertEqual(result["burst_date"], "2026-04-13")
        self.assertEqual(result["days_since_burst"], 5)
        self.assertIsNotNone(result["dist_ma5_pct"])
        self.assertGreater(result["dist_ma5_pct"], 0)
        self.assertTrue(result["is_strong_close"])
        self.assertIn("启动", result["reasons"])

    def test_score_followthrough_rejects_deep_drop(self):
        """当日深跌（change_pct < -3）应被硬过滤拒掉。"""
        f = self._build_filter()
        f.fetcher = _HistoryFetcher()

        rec = {
            "code": "000001",
            "name": "测试股",
            "industry": "机器人",
            "close": 10.95,
            "change_pct": -5.0,  # 越过 [-3, +9.5) 下界
            "turnover": 8.0,
        }

        result = f._score_followthrough_candidate(
            rec, {"机器人": 3}, {}, lookback_days=5,
        )

        self.assertIsNone(result)

    def test_score_continuation_by_compare_rewards_market_top_board(self):
        from src.services.scoring.cont import score_continuation_by_compare

        rec = {
            "code": "000001",
            "name": "测试股",
            "industry": "机器人",
            "consecutive_boards": 3,
            "close": 10.95,
            "change_pct": 5.5,
            "turnover": 12.0,
            "break_count": 0,
            "first_board_time": "09:30",
        }
        base = score_continuation_by_compare(
            rec,
            {"机器人": 3},
            {},
            fetcher=_HistoryFetcher(),
        )
        top = score_continuation_by_compare(
            rec,
            {"机器人": 3},
            {"market_max_boards": 3},
            fetcher=_HistoryFetcher(),
        )

        self.assertGreater(top["score"], base["score"])
        self.assertIn("市场最高", top["reasons"])

    def test_score_broken_board_wrap_finds_mid_term_wrap(self):
        from src.services.scoring.wrap import score_broken_board_wrap

        rec = {
            "code": "002421",
            "name": "达实智能",
            "industry": "软件开发",
            "close": 4.50,
            "change_pct": -1.1,
            "turnover": 8.5,
        }
        result = score_broken_board_wrap(
            rec,
            {"软件开发": 3},
            {},
            fetcher=_WrapHistoryFetcher(),
            lookback_days=5,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["predict_type"], "断板反包")
        self.assertEqual(result["prior_lu_date"], "2026-05-21")
        self.assertEqual(result["days_since_lu"], 7)
        self.assertGreater(result["score"], 0)

    def test_score_broken_board_wrap_penalizes_market_top_board(self):
        from src.services.scoring.wrap import score_broken_board_wrap

        rec = {
            "code": "000417",
            "name": "合百集团",
            "industry": "商业百货",
            "close": 11.68,
            "change_pct": -2.5,
            "turnover": 8.5,
        }
        normal = score_broken_board_wrap(
            rec,
            {"商业百货": 3},
            {"market_max_boards": 6},
            fetcher=_WeakDropWrapHistoryFetcher(),
            lookback_days=5,
        )
        top = score_broken_board_wrap(
            rec,
            {"商业百货": 3},
            {"market_max_boards": 4},
            fetcher=_WeakDropWrapHistoryFetcher(),
            lookback_days=5,
        )

        self.assertIsNotNone(normal)
        self.assertIsNotNone(top)
        self.assertGreater(normal["score"], top["score"])
        self.assertIn("市场最高", top["reasons"])

    def test_score_broken_board_wrap_accepts_weak_drop_with_volume(self):
        from src.services.scoring.wrap import score_broken_board_wrap

        rec = {
            "code": "000417",
            "name": "合百集团",
            "industry": "商业百货",
            "close": 11.68,
            "change_pct": -2.5,
            "turnover": 8.5,
        }
        result = score_broken_board_wrap(
            rec,
            {"商业百货": 3},
            {},
            fetcher=_WeakDropWrapHistoryFetcher(),
            lookback_days=5,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["predict_type"], "断板反包")
        self.assertIsNone(result["worst_drop"])
        self.assertGreater(result["score"], 0)

    def test_score_broken_board_wrap_rewards_oversold_rebound_when_sentiment_cold(self):
        from src.services.scoring.wrap import score_broken_board_wrap

        rec = {
            "code": "000417",
            "name": "合百集团",
            "industry": "商业百货",
            "close": 11.68,
            "change_pct": -2.5,
            "turnover": 8.5,
        }
        neutral = score_broken_board_wrap(
            rec,
            {"商业百货": 3},
            {"market_max_boards": 6, "sentiment_score": 50},
            fetcher=_WeakDropWrapHistoryFetcher(),
            lookback_days=5,
        )
        cold = score_broken_board_wrap(
            rec,
            {"商业百货": 3},
            {"market_max_boards": 6, "sentiment_score": 28},
            fetcher=_WeakDropWrapHistoryFetcher(),
            lookback_days=5,
        )

        self.assertIsNotNone(neutral)
        self.assertIsNotNone(cold)
        self.assertGreater(cold["score"], neutral["score"])
        self.assertIn("超跌反包", cold["reasons"])

    def test_as_of_history_fetcher_trims_future_rows(self):
        class _BaseFetcher:
            def get_history_data(self, code, days=120, force_refresh=False, preferred_mirror=None, mirror_pool=None, request_plan=None):
                return pd.DataFrame({
                    "date": ["2026-05-20", "2026-05-21", "2026-05-22"],
                    "close": [4.27, 3.96, 4.36],
                })

        fetcher = _AsOfHistoryFetcher(_BaseFetcher(), "20260521")
        df = fetcher.get_history_data("002421", days=120)

        self.assertEqual(df["date"].tolist(), ["2026-05-20", "2026-05-21"])

    def test_as_of_history_fetcher_passes_cutoff_to_base_fetcher(self):
        calls = []

        class _BaseFetcher:
            def get_history_data(self, code, days=120, force_refresh=False, preferred_mirror=None, mirror_pool=None, request_plan=None, as_of_trade_date=""):
                calls.append(as_of_trade_date)
                return pd.DataFrame({
                    "date": ["2026-05-20", "2026-05-21", "2026-05-22"],
                    "close": [4.27, 3.96, 4.36],
                })

        fetcher = _AsOfHistoryFetcher(_BaseFetcher(), "20260521")
        fetcher.get_history_data("002421", days=120)

        self.assertEqual(calls, ["20260521"])


if __name__ == "__main__":
    unittest.main()
