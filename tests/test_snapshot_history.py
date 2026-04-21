"""快照行 → history 行的字段转换测试，含 INTRADAY 硬门禁。"""
import unittest

import pandas as pd

from src.utils.snapshot_history import (
    snapshot_row_to_history_row,
    snapshot_rows_to_history_rows,
)
from src.utils.trade_calendar import TradePhase


class SnapshotRowToHistoryRowTest(unittest.TestCase):
    def test_intraday_is_blocked(self):
        row = {"代码": "000001", "最新价": 10.5, "今开": 10.0,
               "最高": 11.0, "最低": 9.8, "成交量": 1000, "成交额": 10500}
        result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.INTRADAY)
        self.assertIsNone(result, "盘中态必须硬拦截，不能返回 history 行")

    def test_closed_full_fields(self):
        row = {
            "代码": "sh600000", "名称": "浦发银行", "最新价": 10.5,
            "涨跌幅": 1.25, "涨跌额": 0.13, "成交量": 12345,
            "成交额": 1500000.0, "振幅": 2.1, "最高": 10.6,
            "最低": 10.2, "今开": 10.3, "换手率": 0.45,
        }
        result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED)
        self.assertIsNotNone(result)
        self.assertEqual(result["code"], "600000")  # 去前缀 + zfill
        self.assertEqual(result["date"], "2026-04-22")
        self.assertAlmostEqual(result["close"], 10.5)
        self.assertAlmostEqual(result["open"], 10.3)
        self.assertAlmostEqual(result["high"], 10.6)
        self.assertAlmostEqual(result["low"], 10.2)
        self.assertEqual(result["partial_fields"], "")
        self.assertEqual(result["needs_repair"], 0)

    def test_non_trading_allowed(self):
        row = {"代码": "000001", "最新价": 10.5, "今开": 10.0,
               "最高": 11.0, "最低": 9.8}
        result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.NON_TRADING)
        self.assertIsNotNone(result)
        self.assertEqual(result["code"], "000001")

    def test_missing_ohlc_reports_partial(self):
        """只有 close，没有 open/high/low → partial_fields 应列出缺失。"""
        row = {"代码": "000001", "最新价": 10.5, "成交量": 1000}
        result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED)
        self.assertIsNotNone(result)
        self.assertEqual(result["needs_repair"], 1)
        missing = set(result["partial_fields"].split(","))
        self.assertEqual(missing, {"open", "high", "low"})

    def test_missing_code_returns_none(self):
        row = {"最新价": 10.5, "今开": 10.0}
        self.assertIsNone(snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED))

    def test_missing_close_returns_none(self):
        row = {"代码": "000001", "今开": 10.0}
        self.assertIsNone(snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED))

    def test_nan_close_returns_none(self):
        import math
        row = {"代码": "000001", "最新价": math.nan}
        self.assertIsNone(snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED))

    def test_code_normalization(self):
        for raw, expected in [
            ("sh600000", "600000"),
            ("SZ000001", "000001"),
            ("bj830000", "830000"),
            ("1", "000001"),
            ("  600000  ", "600000"),
        ]:
            row = {"代码": raw, "最新价": 10.0}
            result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED)
            self.assertIsNotNone(result)
            self.assertEqual(result["code"], expected)

    def test_batch_conversion_skips_invalid(self):
        rows = [
            {"代码": "000001", "最新价": 10.0, "今开": 9.8, "最高": 10.1, "最低": 9.7},
            {"代码": "", "最新价": 10.0},               # 跳过
            {"代码": "000002", "最新价": None},           # 跳过
            {"代码": "000003", "最新价": 5.0, "今开": 4.9, "最高": 5.1, "最低": 4.8},
        ]
        out = snapshot_rows_to_history_rows(rows, "2026-04-22", TradePhase.CLOSED)
        self.assertEqual(len(out), 2)
        self.assertEqual({r["code"] for r in out}, {"000001", "000003"})

    def test_batch_conversion_intraday_empty(self):
        rows = [
            {"代码": "000001", "最新价": 10.0},
            {"代码": "000002", "最新价": 20.0},
        ]
        out = snapshot_rows_to_history_rows(rows, "2026-04-22", TradePhase.INTRADAY)
        self.assertEqual(out, [])

    def test_pandas_series_input(self):
        """真实快照来自 DataFrame.iterrows()，传入 Series 要兼容。"""
        df = pd.DataFrame([
            {"代码": "sh600000", "最新价": 10.5, "今开": 10.0,
             "最高": 10.6, "最低": 9.9, "成交额": 1500000},
        ])
        _, row = next(df.iterrows())
        result = snapshot_row_to_history_row(row, "2026-04-22", TradePhase.CLOSED)
        self.assertIsNotNone(result)
        self.assertEqual(result["code"], "600000")


if __name__ == "__main__":
    unittest.main()
