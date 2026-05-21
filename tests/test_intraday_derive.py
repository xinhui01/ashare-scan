"""测试 intraday 派生 首次封板时间 + 炸板次数 helper。"""
from __future__ import annotations

import pandas as pd
import pytest

from stock_data import _derive_seal_time_from_intraday, _count_intraday_breaks


def _make_intraday(rows):
    """rows: [(time_str, close_price), ...]"""
    return pd.DataFrame({
        "time": pd.to_datetime([r[0] for r in rows]),
        "close": [r[1] for r in rows],
    })


class TestDeriveSealTime:
    def test_empty_returns_none(self):
        assert _derive_seal_time_from_intraday(pd.DataFrame(), 11.0) is None

    def test_no_seal_returns_none(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.0),
            ("2026-05-20 09:31:00", 10.5),
            ("2026-05-20 09:32:00", 10.8),
        ])
        assert _derive_seal_time_from_intraday(df, 11.0) is None

    def test_seal_at_open(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 11.0),  # 秒板
            ("2026-05-20 09:31:00", 11.0),
            ("2026-05-20 09:32:00", 11.0),
        ])
        result = _derive_seal_time_from_intraday(df, 11.0)
        assert result is not None
        assert result.startswith("09:30")

    def test_seal_mid_morning(self):
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:15:00", 10.8),
            ("2026-05-20 10:30:00", 11.0),
            ("2026-05-20 10:31:00", 11.0),
        ])
        result = _derive_seal_time_from_intraday(df, 11.0)
        assert result is not None
        assert result.startswith("10:30")

    def test_seal_with_tolerance(self):
        # 价格略低于涨停价但在容差内（0.1%）
        # 涨停价 11.00, 容差 0.1% → 接受 10.989 及以上
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 09:35:00", 10.99),  # 在 0.1% 容差内
        ])
        result = _derive_seal_time_from_intraday(df, 11.0, tolerance_pct=0.1)
        assert result is not None
        assert result.startswith("09:35")

    def test_seal_outside_tolerance(self):
        # 价格 10.95（差 4.5%），不算封板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 09:35:00", 10.95),
        ])
        assert _derive_seal_time_from_intraday(df, 11.0, tolerance_pct=0.1) is None


class TestCountIntradayBreaks:
    def test_empty_returns_zero(self):
        assert _count_intraday_breaks(pd.DataFrame(), 11.0) == 0

    def test_no_seal_returns_zero(self):
        # 全天没封板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.0),
            ("2026-05-20 11:00:00", 10.5),
            ("2026-05-20 14:00:00", 10.8),
        ])
        assert _count_intraday_breaks(df, 11.0) == 0

    def test_seal_no_break(self):
        # 封板后一直保持
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:00:00", 11.0),  # 封板
            ("2026-05-20 10:30:00", 11.0),
            ("2026-05-20 14:00:00", 11.0),
        ])
        assert _count_intraday_breaks(df, 11.0) == 0

    def test_one_break(self):
        # 封板 → 跌破 → 再封 = 1 次炸板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 10.5),
            ("2026-05-20 10:00:00", 11.0),  # 封板
            ("2026-05-20 10:30:00", 10.8),  # 跌破
            ("2026-05-20 11:00:00", 11.0),  # 再封
        ])
        assert _count_intraday_breaks(df, 11.0) == 1

    def test_multiple_breaks(self):
        # 多次封板炸板
        df = _make_intraday([
            ("2026-05-20 09:30:00", 11.0),  # 封板 #1
            ("2026-05-20 09:45:00", 10.8),  # 跌破
            ("2026-05-20 10:00:00", 11.0),  # 封板 #2 (1 次炸板)
            ("2026-05-20 10:30:00", 10.7),  # 跌破
            ("2026-05-20 11:00:00", 11.0),  # 封板 #3 (2 次炸板)
        ])
        assert _count_intraday_breaks(df, 11.0) == 2
