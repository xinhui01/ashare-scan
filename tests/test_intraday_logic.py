import unittest

import pandas as pd

from stock_data import _normalize_intraday_source_frame, _select_intraday_trade_date
from stock_filter import StockFilter


class IntradayLogicTests(unittest.TestCase):
    def test_select_intraday_trade_date_by_negative_offset(self):
        trade_dates = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-07"]
        selected, offset = _select_intraday_trade_date(trade_dates, day_offset=-1)
        self.assertEqual(selected, "2026-04-03")
        self.assertEqual(offset, -1)

    def test_select_intraday_trade_date_by_target_with_fallback(self):
        trade_dates = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-07"]
        selected, offset = _select_intraday_trade_date(trade_dates, target_trade_date="2026-04-06")
        self.assertEqual(selected, "2026-04-03")
        self.assertEqual(offset, -1)

    def test_resolve_intraday_prev_close_uses_previous_trade_date(self):
        history_df = pd.DataFrame(
            [
                {"date": "2026-04-01", "close": 11.15},
                {"date": "2026-04-02", "close": 11.27},
                {"date": "2026-04-03", "close": 11.12},
                {"date": "2026-04-07", "close": 11.06},
            ]
        )
        stock_filter = StockFilter.__new__(StockFilter)
        prev_close = stock_filter._resolve_intraday_prev_close(history_df, "2026-04-07")
        self.assertEqual(prev_close, 11.12)

    def test_normalize_intraday_source_frame_maps_avg_price(self):
        raw = pd.DataFrame(
            [
                {
                    "时间": "2026-04-07 09:30:00",
                    "开盘": "11.12",
                    "收盘": "11.13",
                    "最高": "11.14",
                    "最低": "11.11",
                    "成交量": "3850",
                    "成交额": "4281200",
                    "均价": "11.120",
                }
            ]
        )
        normalized = _normalize_intraday_source_frame(raw, "000001")
        self.assertEqual(
            normalized.columns.tolist(),
            ["time", "open", "close", "high", "low", "volume", "amount", "avg_price"],
        )
        self.assertAlmostEqual(float(normalized.iloc[0]["avg_price"]), 11.12, places=2)


if __name__ == "__main__":
    unittest.main()
