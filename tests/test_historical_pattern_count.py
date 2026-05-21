"""测试 stock_filter 中 3 个 _count_historical_* helper。

定义复述（来自 spec）：
- _count_historical_continuation：涨停日 → 次日继续涨停 → 计 1 次连板成功
- _count_historical_followthrough：涨停日 → 后续 window=5 日内出现另一次涨停 → 计 1 次
- _count_historical_wrap：涨停日 → window=5 日内至少一根 ≤ drop% 阴线 → 之后再涨停 → 计 1 次
"""
from __future__ import annotations

import pandas as pd
import pytest

from stock_filter import (
    _count_historical_any_limit_up,
    _count_historical_continuation,
    _count_historical_followthrough,
    _count_historical_wrap,
)


def _make_df(rows):
    """rows: [(date_str, close_float), ...]"""
    return pd.DataFrame({
        "date": [r[0] for r in rows],
        "close": [r[1] for r in rows],
        "low": [r[1] for r in rows],
    })


def _threshold_main_board(_code: str) -> float:
    return 10.0


def _threshold_growth_board(_code: str) -> float:
    return 20.0


# ============== _count_historical_continuation ==============

class TestHistoricalContinuation:
    def test_empty_df_returns_zero(self):
        df = pd.DataFrame()
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_single_limit_up_no_followup(self):
        # 一次涨停但次日下跌，不算连板成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% 涨停
            ("2024-01-03", 10.8),  # -1.8% 不算连板
            ("2024-01-04", 10.5),
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_two_consecutive_limit_ups_counts_one(self):
        # 涨停 + 次日继续涨停 = 1 次连板成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% 涨停
            ("2024-01-03", 12.1),  # +10% 连板成功
            ("2024-01-04", 12.0),  # 跳过 today
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_three_consecutive_limit_ups_counts_two(self):
        # 3 连板：D1↑ D2↑ D3↑ → D1→D2 + D2→D3 共 2 次成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),   # +10%
            ("2024-01-03", 12.1),   # +10%
            ("2024-01-04", 13.31),  # +10%
            ("2024-01-05", 13.0),   # 跳过 today
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 2

    def test_gap_day_does_not_count(self):
        # 涨停 + 间隔一日下跌 + 再涨停 — 不算连板（不是 T+1）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.0),  # 跌
            ("2024-01-04", 11.0),  # +10%（但与前涨停隔了一天，非连板）
            ("2024-01-05", 11.0),
        ])
        cnt, last = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_growth_board_20pct_threshold(self):
        # 创业板 20% 阈值：+11% 不算涨停，+20% 才算
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.1),   # +11% 主板算涨停，创业板不算
            ("2024-01-03", 12.21),  # +10% 主板算涨停
            ("2024-01-04", 12.0),
        ])
        cnt, last = _count_historical_continuation(
            df, "300001", lookback_days=90, threshold_fn=_threshold_growth_board,
        )
        assert cnt == 0  # 创业板下两个 +10% 都不算涨停

        # 创业板真涨停（+20%）
        df2 = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 12.0),   # +20% 涨停
            ("2024-01-03", 14.4),   # +20% 连板
            ("2024-01-04", 14.0),
        ])
        cnt2, _ = _count_historical_continuation(
            df2, "300001", lookback_days=90, threshold_fn=_threshold_growth_board,
        )
        assert cnt2 == 1

    def test_lookback_filter(self):
        # 100 日前的涨停不计入（超出 lookback=90）
        dates = pd.date_range("2024-01-01", periods=105, freq="D")
        closes = [10.0] * 105
        # 在第 0/1 天造一次连板（距 today 即第 104 天有 103 天差距 > 90）
        closes[1] = 11.0  # +10%
        closes[2] = 12.1  # +10% 连板
        df = _make_df([(d.strftime("%Y-%m-%d"), c) for d, c in zip(dates, closes)])
        cnt, _ = _count_historical_continuation(
            df, "000001", lookback_days=90, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0


# ============== _count_historical_followthrough ==============

class TestHistoricalFollowthrough:
    def test_empty_df(self):
        cnt, last = _count_historical_followthrough(
            pd.DataFrame(), "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_single_limit_up_no_followup_window(self):
        # 涨停后 7 日内无再涨停 → 不计入
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.8),
            ("2024-01-04", 10.6),
            ("2024-01-05", 10.4),
            ("2024-01-06", 10.2),
            ("2024-01-07", 10.0),
            ("2024-01-08", 10.0),  # today
        ])
        cnt, _ = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_limit_up_then_within_5d_another_limit_up(self):
        # 涨停后 3 日内出现另一次涨停 → 1 次接力成功
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 10.5),
            ("2024-01-04", 10.0),
            ("2024-01-05", 11.0),  # +10% 在 window=5 内
            ("2024-01-06", 11.0),  # today
        ])
        cnt, last = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_limit_up_then_6d_later_does_not_count(self):
        # 涨停后第 6 日才涨停 — 超出 window=5
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),   # +10%
            ("2024-01-03", 10.5),
            ("2024-01-04", 10.4),
            ("2024-01-05", 10.3),
            ("2024-01-06", 10.2),
            ("2024-01-07", 10.1),
            ("2024-01-08", 11.11),  # +10% 但距前涨停 6 日
            ("2024-01-09", 11.11),  # today
        ])
        cnt, _ = _count_historical_followthrough(
            df, "000001", lookback_days=90, window=5,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0


# ============== _count_historical_wrap ==============

class TestHistoricalWrap:
    def test_empty_df(self):
        cnt, last = _count_historical_wrap(
            pd.DataFrame(), "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_consecutive_limit_ups_not_wrap(self):
        # 涨停直接再涨停（无阴线打回）→ 不算反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10%
            ("2024-01-03", 12.1),  # +10% 直接连板，不算反包
            ("2024-01-04", 12.0),  # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_limit_up_drop_then_wrap_counts(self):
        # 涨停 → -4% 阴线 → 再涨停 → 1 次反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10%
            ("2024-01-03", 10.56),   # -4%
            ("2024-01-04", 11.62),   # +10% 反包
            ("2024-01-05", 11.5),    # today
        ])
        cnt, last = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_limit_up_minor_drop_then_wrap_does_not_count(self):
        # 涨停 → -2% 阴线（未达 -3% 阈值）→ 再涨停 → 不算反包
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10%
            ("2024-01-03", 10.78),   # -2%（未达阈值）
            ("2024-01-04", 11.86),   # +10% 但不算反包
            ("2024-01-05", 11.8),    # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_multiple_wraps_counted(self):
        # 2 次反包：[涨停 → 阴线 → 涨停] × 2
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),    # +10% (1st 涨停)
            ("2024-01-03", 10.56),   # -4% 阴线
            ("2024-01-04", 11.62),   # +10% (1st 反包)
            ("2024-01-05", 11.15),   # -4%
            ("2024-01-06", 12.27),   # +10% (2nd 反包)
            ("2024-01-07", 12.2),    # today
        ])
        cnt, _ = _count_historical_wrap(
            df, "000001", lookback_days=90, window=5,
            drop_threshold=-3.0, threshold_fn=_threshold_main_board,
        )
        assert cnt == 2


# ============== _count_historical_any_limit_up ==============

class TestHistoricalAnyLimitUp:
    def test_empty_df_returns_zero(self):
        cnt, last = _count_historical_any_limit_up(
            pd.DataFrame(), "000001", lookback_days=60,
            threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_no_limit_up_in_history(self):
        # 历史里没有涨停
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 10.5),  # +5%
            ("2024-01-03", 10.7),
            ("2024-01-04", 10.8),  # today
        ])
        cnt, last = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_single_limit_up_counts_one(self):
        # 单次涨停（无 T+1 跟进也算）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% 涨停
            ("2024-01-03", 10.5),  # 跌
            ("2024-01-04", 10.5),  # today
        ])
        cnt, last = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert cnt == 1
        assert last is not None

    def test_multiple_scattered_limit_ups(self):
        # 多次散点涨停（不要求连续 / 同形态）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),   # +10%
            ("2024-01-03", 10.5),
            ("2024-01-04", 10.0),
            ("2024-01-05", 11.0),   # +10%
            ("2024-01-06", 10.5),
            ("2024-01-07", 10.0),
            ("2024-01-08", 11.0),   # +10%
            ("2024-01-09", 10.8),   # today
        ])
        cnt, last = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert cnt == 3

    def test_today_is_skipped(self):
        # today 即使是涨停也不计入（避免自计）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 10.2),
            ("2024-01-03", 11.22),  # today: +10% 但要被跳过
        ])
        cnt, last = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0
        assert last is None

    def test_growth_board_20pct_threshold(self):
        # 创业板 +11% 不算涨停（阈值 20%）
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.1),  # +11% 主板算涨停，创业板不算
            ("2024-01-03", 11.5),
        ])
        cnt, _ = _count_historical_any_limit_up(
            df, "300001", lookback_days=60, threshold_fn=_threshold_growth_board,
        )
        assert cnt == 0

    def test_lookback_filter(self):
        # 80 日前的涨停不计入（超出 lookback=60）
        dates = pd.date_range("2024-01-01", periods=90, freq="D")
        closes = [10.0] * 90
        closes[1] = 11.0  # +10%，距 today=89 共 88 日 > 60
        df = _make_df([(d.strftime("%Y-%m-%d"), c) for d, c in zip(dates, closes)])
        cnt, _ = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert cnt == 0

    def test_last_hit_days_recent(self):
        # 最后一次涨停距 today 的偏移正确
        df = _make_df([
            ("2024-01-01", 10.0),
            ("2024-01-02", 11.0),  # +10% (idx=1)
            ("2024-01-03", 10.5),  # idx=2
            ("2024-01-04", 10.5),  # today idx=3
        ])
        _, last = _count_historical_any_limit_up(
            df, "000001", lookback_days=60, threshold_fn=_threshold_main_board,
        )
        assert last == 2  # today_idx(3) - hit_idx(1) = 2
