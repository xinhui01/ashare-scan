"""承接强势形态（涨停 → 次日回落 → 承接强势）的单元测试。

覆盖 `HistoryAnalysisService.analyze_limit_up_followthrough` 的 7 条规则组合 +
`StockFilter` 层的过滤 gate 集成。
"""
import unittest

import pandas as pd

from scan_models import FilterSettings
from src.models.analysis_models import HistoryAnalysisConfig
from src.services.history_analysis_service import HistoryAnalysisService
from stock_filter import StockFilter


def _make_config(
    *,
    limit_up_lookback_days: int = 10,
    max_pullback_pct: float = 3.0,
    max_volume_ratio: float = 0.7,
    min_hold_days: int = 1,
) -> HistoryAnalysisConfig:
    settings = FilterSettings(
        trend_days=5,
        ma_period=5,
        limit_up_lookback_days=limit_up_lookback_days,
        volume_lookback_days=5,
        volume_expand_enabled=True,
        volume_expand_factor=2.0,
        require_limit_up_within_days=False,
        strong_ft_enabled=True,
        strong_ft_max_pullback_pct=max_pullback_pct,
        strong_ft_max_volume_ratio=max_volume_ratio,
        strong_ft_min_hold_days=min_hold_days,
    )
    return HistoryAnalysisConfig.from_filter_settings(settings)


def _series(dates, opens, highs, lows, closes, changes, volumes):
    return pd.DataFrame({
        "date": dates,
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "change_pct": changes,
        "volume": volumes,
    })


class TestFollowthroughPositiveCase(unittest.TestCase):
    """命中形态的经典场景：涨停 → 次日小回落缩量 → 后续站稳。"""

    def test_classic_strong_followthrough(self):
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"],
            opens=[9.2, 9.5, 10.5, 10.3, 10.35],
            highs=[9.3, 9.6, 10.8, 10.45, 10.5],
            # T 收盘 10.45；T+1 最低 10.2（回撤约 2.4%），后续不破 10.2
            lows=[9.1, 9.4, 10.3, 10.2, 10.3],
            closes=[9.2, 9.5, 10.45, 10.3, 10.42],
            changes=[0.0, 3.26, 10.0, -1.4, 1.17],  # T 涨停
            # T+1 量能 1_800_000 / 3_000_000 = 60% < 70% 上限
            volumes=[1_000_000, 1_100_000, 3_000_000, 1_800_000, 1_500_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertTrue(result["has_strong_followthrough"])
        self.assertEqual(result["limit_up_date"], "2026-04-15")
        self.assertEqual(result["pullback_date"], "2026-04-16")
        self.assertLess(result["pullback_pct"], 3.0)
        self.assertLess(result["pullback_volume_ratio"], 0.7)
        self.assertGreaterEqual(result["hold_days"], 1)


class TestFollowthroughNegativeCases(unittest.TestCase):
    def test_no_limit_up_in_lookback(self):
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15"],
            opens=[9.0, 9.1, 9.2],
            highs=[9.2, 9.3, 9.4],
            lows=[8.9, 9.0, 9.1],
            closes=[9.1, 9.2, 9.3],
            changes=[1.0, 1.1, 1.1],  # 全部涨幅 < 10%，无涨停
            volumes=[1_000_000] * 3,
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertIsNone(result["limit_up_date"])

    def test_limit_up_is_last_day_no_pullback_yet(self):
        """涨停就是最后一天 → 没有 T+1 可判断，一律不命中。"""
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15"],
            opens=[9.0, 9.1, 9.5],
            highs=[9.2, 9.3, 10.45],
            lows=[8.9, 9.0, 9.4],
            closes=[9.1, 9.5, 10.45],
            changes=[1.0, 4.4, 10.0],  # 最后一天才涨停
            volumes=[1_000_000, 1_200_000, 3_000_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])

    def test_next_day_makes_new_high_not_pullback(self):
        """次日继续上涨 → 不是"回落" → 不算承接形态。"""
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"],
            opens=[9.0, 9.1, 9.5, 10.5],
            highs=[9.2, 9.3, 10.45, 11.0],
            lows=[8.9, 9.0, 9.4, 10.4],
            closes=[9.1, 9.5, 10.45, 10.9],  # T+1 继续涨
            changes=[1.0, 4.4, 10.0, 4.3],
            volumes=[1_000_000, 1_200_000, 3_000_000, 1_500_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["is_pullback_day"])

    def test_pullback_too_deep(self):
        """次日最低跌破 T 收盘 -5%，超出 3% 上限。"""
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"],
            opens=[9.0, 9.1, 9.5, 10.5],
            highs=[9.2, 9.3, 10.45, 10.5],
            lows=[8.9, 9.0, 9.4, 9.85],  # T+1 最低 9.85，跌幅 ~5.7%
            closes=[9.1, 9.5, 10.45, 9.9],
            changes=[1.0, 4.4, 10.0, -5.3],
            volumes=[1_000_000, 1_200_000, 3_000_000, 1_000_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["pullback_within_limit"])

    def test_next_day_not_shrunk(self):
        """次日放量回落 → 缩量检查未通过。"""
        svc = HistoryAnalysisService(_make_config(max_volume_ratio=0.7))
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"],
            opens=[9.0, 9.1, 9.5, 10.3],
            highs=[9.2, 9.3, 10.45, 10.4],
            lows=[8.9, 9.0, 9.4, 10.2],
            closes=[9.1, 9.5, 10.45, 10.3],
            changes=[1.0, 4.4, 10.0, -1.4],
            volumes=[1_000_000, 1_200_000, 2_000_000, 2_400_000],  # T+1 量 1.2x
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["volume_shrunk"])

    def test_breaks_below_pullback_low(self):
        """后续几天跌破了回落日的最低价 → 承接失败。"""
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17", "2026-04-18"],
            opens=[9.0, 9.1, 9.5, 10.3, 10.2, 9.9],
            highs=[9.2, 9.3, 10.45, 10.4, 10.3, 10.0],
            lows=[8.9, 9.0, 9.4, 10.2, 10.0, 9.7],   # T+3 = 9.7 < T+1 low 10.2
            closes=[9.1, 9.5, 10.45, 10.3, 10.15, 9.75],
            changes=[1.0, 4.4, 10.0, -1.4, -1.5, -3.9],
            volumes=[1_000_000, 1_200_000, 2_000_000, 1_000_000, 800_000, 1_500_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["holds_above_pullback_low"])

    def test_min_hold_days_not_satisfied(self):
        """min_hold_days=3，但 T+1 后只站稳了 1 天 → 不达标。"""
        svc = HistoryAnalysisService(_make_config(min_hold_days=3))
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"],
            opens=[9.2, 9.5, 10.5, 10.0, 10.1],
            highs=[9.3, 9.6, 10.8, 10.3, 10.4],
            lows=[9.1, 9.4, 10.3, 9.85, 10.0],
            closes=[9.2, 9.5, 10.45, 10.0, 10.25],
            changes=[0.0, 3.26, 10.0, -4.3, 2.5],
            volumes=[1_000_000, 1_100_000, 3_000_000, 1_800_000, 1_500_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertEqual(result["hold_days"], 1)
        self.assertEqual(result["min_hold_days"], 3)


class TestLookbackWindowSemantics(unittest.TestCase):
    """lookback=N 语义：应扫到 N 个可能是 T 的 K 线（排除最后一天，因为没有 T+1）。"""

    def test_lookback_covers_exactly_n_candidate_days(self):
        # 构造 10 根 K 线：涨停发生在 index=4（倒数第 6 根）。
        # lookback=5 时,候选 T 的下标是 [4,5,6,7,8](共 5 根),所以 index=4 应该被扫到。
        dates = [f"2026-04-{d:02d}" for d in range(13, 23)]  # 10 根
        opens = highs = lows = closes = [10.0] * 10
        volumes = [1_000_000] * 10
        change = [0.0] * 10
        change[4] = 10.0  # 涨停在 index=4
        # 构造一个合法的承接结构在 index=4~6
        closes = [9.5, 9.6, 9.7, 9.8, 10.78, 10.5, 10.6, 10.65, 10.7, 10.72]
        lows = [9.4, 9.5, 9.6, 9.7, 10.7, 10.45, 10.55, 10.6, 10.65, 10.67]
        volumes = [1_000_000] * 4 + [3_000_000, 1_800_000] + [1_500_000] * 4

        svc = HistoryAnalysisService(_make_config(limit_up_lookback_days=5))
        df = _series(dates, opens, highs, lows, closes, change, volumes)
        result = svc.analyze_limit_up_followthrough(df)
        self.assertEqual(result["limit_up_date"], "2026-04-17")  # index=4 的日期


class TestLimitUpIsToday(unittest.TestCase):
    """最后一天刚涨停,T+1 没到:应返回 `limit_up_is_today=True` 以便给出精准反馈。"""

    def test_last_day_limit_up_marks_today_flag(self):
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-18", "2026-04-19", "2026-04-20", "2026-04-21"],
            opens=[9.0, 9.2, 9.5, 9.8],
            highs=[9.2, 9.3, 9.6, 10.45],
            lows=[8.9, 9.1, 9.4, 9.8],
            closes=[9.1, 9.2, 9.5, 10.45],
            changes=[0.0, 1.1, 3.26, 10.0],  # 最后一天刚涨停
            volumes=[1_000_000, 1_100_000, 1_200_000, 3_000_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertTrue(result["limit_up_is_today"])
        self.assertEqual(result["limit_up_date"], "2026-04-21")

    def test_no_limit_up_at_all_does_not_mark_today_flag(self):
        svc = HistoryAnalysisService(_make_config())
        df = _series(
            dates=["2026-04-19", "2026-04-20", "2026-04-21"],
            opens=[9.0, 9.1, 9.2],
            highs=[9.2, 9.3, 9.4],
            lows=[8.9, 9.0, 9.1],
            closes=[9.1, 9.2, 9.3],
            changes=[1.0, 1.1, 1.1],
            volumes=[1_000_000] * 3,
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["limit_up_is_today"])


class TestFailureReasonForLimitUpIsToday(unittest.TestCase):
    """_build_strong_ft_failure_reason 应区分"没涨停"和"刚涨停还没次日"。"""

    def test_today_limit_up_reason_is_specific(self):
        from scan_models import FilterSettings
        f = StockFilter.__new__(StockFilter)
        f._log = None
        f.apply_settings(FilterSettings(strong_ft_enabled=True, limit_up_lookback_days=5))
        ft = {
            "has_strong_followthrough": False,
            "limit_up_date": "2026-04-21",
            "limit_up_is_today": True,
        }
        reason = f._build_strong_ft_failure_reason(ft)
        self.assertIn("2026-04-21", reason)
        self.assertIn("次日走势还未出现", reason)

    def test_no_limit_up_reason_is_generic(self):
        from scan_models import FilterSettings
        f = StockFilter.__new__(StockFilter)
        f._log = None
        f.apply_settings(FilterSettings(strong_ft_enabled=True, limit_up_lookback_days=5))
        ft = {"has_strong_followthrough": False, "limit_up_date": None, "limit_up_is_today": False}
        reason = f._build_strong_ft_failure_reason(ft)
        self.assertIn("未找到可承接的涨停日", reason)


class TestMinHoldDaysZero(unittest.TestCase):
    def test_zero_min_hold_allows_pullback_as_latest(self):
        """min_hold_days=0：T+1 就是最后一天也允许命中（抢先信号）。"""
        svc = HistoryAnalysisService(_make_config(min_hold_days=0))
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"],
            opens=[9.2, 9.5, 10.5, 10.1],
            highs=[9.3, 9.6, 10.8, 10.3],
            lows=[9.1, 9.4, 10.3, 9.9],   # 回撤 ~5.3% >3%
            closes=[9.2, 9.5, 10.45, 10.0],
            changes=[0.0, 3.26, 10.0, -4.3],
            volumes=[1_000_000, 1_100_000, 3_000_000, 1_800_000],
        )
        result = svc.analyze_limit_up_followthrough(df)
        # 这个场景 pullback_pct 超限（回撤 >3%），仍不命中——只是测试 min_hold=0 不再阻挡
        self.assertFalse(result["has_strong_followthrough"])
        self.assertFalse(result["pullback_within_limit"])

    def test_zero_min_hold_passes_when_other_rules_ok(self):
        svc = HistoryAnalysisService(_make_config(min_hold_days=0))
        df = _series(
            dates=["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16"],
            opens=[9.2, 9.5, 10.5, 10.1],
            highs=[9.3, 9.6, 10.8, 10.3],
            lows=[9.1, 9.4, 10.3, 10.22],  # 回撤 ~2.2% <3%
            closes=[9.2, 9.5, 10.45, 10.3],
            changes=[0.0, 3.26, 10.0, -1.4],
            volumes=[1_000_000, 1_100_000, 3_000_000, 1_500_000],  # T+1 量 50%
        )
        result = svc.analyze_limit_up_followthrough(df)
        self.assertTrue(result["has_strong_followthrough"])
        self.assertEqual(result["hold_days"], 0)


class TestFilterGateIntegration(unittest.TestCase):
    """验证 StockFilter 在 strong_ft_enabled=True 时会拒掉未命中的股票。"""

    def _build_filter(self, strong_ft_enabled: bool) -> StockFilter:
        class _DummyFetcher:
            def get_history_data(self, *args, **kwargs):
                return None
        f = StockFilter.__new__(StockFilter)
        f.fetcher = _DummyFetcher()
        f._log = None
        settings = FilterSettings(
            strong_ft_enabled=strong_ft_enabled,
            strong_ft_max_pullback_pct=3.0,
            strong_ft_max_volume_ratio=0.7,
            strong_ft_min_hold_days=1,
        )
        f.apply_settings(settings)
        return f

    def test_apply_settings_roundtrip(self):
        """get_settings() 应能拿回 apply_settings() 传进去的 strong_ft_* 字段。"""
        f = self._build_filter(strong_ft_enabled=True)
        settings = f.get_settings()
        self.assertTrue(settings.strong_ft_enabled)
        self.assertEqual(settings.strong_ft_max_pullback_pct, 3.0)
        self.assertEqual(settings.strong_ft_max_volume_ratio, 0.7)
        self.assertEqual(settings.strong_ft_min_hold_days, 1)

    def test_gate_rejects_missing_pattern(self):
        """开启过滤时，没有 strong_followthrough 的分析结果会被 gate 拒掉。"""
        f = self._build_filter(strong_ft_enabled=True)
        result = {"reasons": []}
        analysis = {
            "strong_followthrough": {
                "has_strong_followthrough": False,
                "limit_up_date": None,
            },
            "summary": "",
        }
        rejected = f._apply_strong_followthrough_failure(result, analysis)
        self.assertTrue(rejected)
        self.assertTrue(result["reasons"])
        self.assertIn("涨停", result["reasons"][0])

    def test_gate_is_noop_when_disabled(self):
        f = self._build_filter(strong_ft_enabled=False)
        result = {"reasons": []}
        analysis = {"strong_followthrough": {"has_strong_followthrough": False}}
        rejected = f._apply_strong_followthrough_failure(result, analysis)
        self.assertFalse(rejected)
        self.assertEqual(result["reasons"], [])

    def test_gate_is_noop_when_pattern_hits(self):
        f = self._build_filter(strong_ft_enabled=True)
        result = {"reasons": []}
        analysis = {
            "strong_followthrough": {"has_strong_followthrough": True, "limit_up_date": "2026-04-15"}
        }
        rejected = f._apply_strong_followthrough_failure(result, analysis)
        self.assertFalse(rejected)


if __name__ == "__main__":
    unittest.main()
