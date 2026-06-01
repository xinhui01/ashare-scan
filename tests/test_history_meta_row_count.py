"""history meta.row_count 应记录表里真实总行数，而非单次增量抓取的行数。

这是"第二轮更新缓存还是慢"的根因回归：save_history 按 (code, trade_date) upsert
累积，表里可能有几百行，但单次抓取的 df 只有几十行；若 row_count 写成 len(df)，
缓存新鲜度（要 row_count >= days）永远不达标 → 每轮重拉全市场。
"""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


def _ohlcv(dates):
    n = len(dates)
    return pd.DataFrame({
        "date": list(dates),
        "open": [10.0] * n,
        "close": [10.5] * n,
        "high": [10.8] * n,
        "low": [9.9] * n,
        "volume": [1000] * n,
        "amount": [10500.0] * n,
    })


class HistoryRowCountTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._db_path = Path(self._tmp) / "test.sqlite3"
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", self._db_path)
        self._patch_dir.start()
        self._patch_db.start()
        import stock_store
        stock_store._SCHEMA_INITIALIZED = False
        stock_store._SCHEMA_INITIALIZED_PATH = ""
        stock_store.reset_all_connections()
        stock_store.ensure_store_ready()

    def tearDown(self):
        self._patch_dir.stop()
        self._patch_db.stop()
        import stock_store
        stock_store._SCHEMA_INITIALIZED = False
        stock_store._SCHEMA_INITIALIZED_PATH = ""
        stock_store.reset_all_connections()

    def test_count_history_returns_table_total(self):
        import stock_store
        dates = [f"2026-01-{d:02d}" for d in range(1, 29)]  # 28 行
        stock_store.save_history("600000", _ohlcv(dates))
        self.assertEqual(stock_store.count_history("600000"), 28)
        # 增量再写 5 个新日期 → upsert 累积到 33
        stock_store.save_history("600000", _ohlcv(["2026-02-01", "2026-02-02",
                                                   "2026-02-03", "2026-02-04", "2026-02-05"]))
        self.assertEqual(stock_store.count_history("600000"), 33)
        # 重写已存在的日期 → 不增加
        stock_store.save_history("600000", _ohlcv(["2026-01-01", "2026-01-02"]))
        self.assertEqual(stock_store.count_history("600000"), 33)

    def test_count_history_unknown_code_is_zero(self):
        import stock_store
        self.assertEqual(stock_store.count_history("999999"), 0)

    def test_get_history_data_records_true_depth_not_increment(self):
        """表里已有 100 行；增量只抓 10 行；meta.row_count 应记 110，而不是 10。"""
        import stock_store
        import stock_data
        from data_source_models import HistoryRequestPlan

        code = "600000"
        # 预置 100 行历史（模拟之前累积下来的深度）
        base_dates = [f"2025-{(m):02d}-{(d):02d}"
                      for m in range(1, 6) for d in range(1, 21)][:100]
        stock_store.save_history(code, _ohlcv(base_dates))
        self.assertEqual(stock_store.count_history(code), 100)

        # 增量抓取只返回 10 个新日期（模拟 days+15 窗口只够 ~几十行）
        new_dates = [f"2026-03-{d:02d}" for d in range(1, 11)]
        incremental = _ohlcv(new_dates)

        plan = HistoryRequestPlan(
            mode="network",
            provider_sequence=("sina",),
            mirror_urls=(),
            reason="test",
        )
        fetcher = stock_data.StockDataFetcher()
        with mock.patch.object(stock_data, "_fetch_sina_hist_frame", return_value=incremental):
            fetcher.get_history_data(code, days=10, force_refresh=True, request_plan=plan)

        meta = stock_store.load_history_meta(code)
        self.assertIsNotNone(meta)
        # 关键断言：记的是表里真实总数 110，不是增量的 10
        self.assertEqual(stock_store.count_history(code), 110)
        self.assertEqual(meta["row_count"], 110)


if __name__ == "__main__":
    unittest.main()
