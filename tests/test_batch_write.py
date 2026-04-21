"""批量写入 save_history_rows_batch / save_history_meta_batch 的行为测试。

重点：
- 批大小切分
- 失败批隔离（不影响其他批）
- 返回 (success_codes, failed_codes)
- 空输入不炸
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


class BatchWriteTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", Path(self._tmp) / "test.sqlite3")
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

    # -------- save_history_rows_batch --------

    def test_batch_write_success(self):
        import stock_store
        rows_by_code = {
            "000001": pd.DataFrame([
                {"date": "2026-04-20", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8},
                {"date": "2026-04-21", "open": 10.5, "close": 11.0, "high": 11.2, "low": 10.3},
            ]),
            "000002": pd.DataFrame([
                {"date": "2026-04-20", "open": 20.0, "close": 20.5, "high": 21.0, "low": 19.8},
            ]),
        }
        success, failed = stock_store.save_history_rows_batch(rows_by_code, batch_size=500)
        self.assertEqual(set(success), {"000001", "000002"})
        self.assertEqual(failed, [])

        loaded = stock_store.load_history("000001")
        self.assertEqual(len(loaded), 2)

    def test_batch_write_empty_input(self):
        import stock_store
        s, f = stock_store.save_history_rows_batch({}, batch_size=500)
        self.assertEqual(s, [])
        self.assertEqual(f, [])

    def test_batch_write_skips_empty_df(self):
        import stock_store
        rows_by_code = {
            "000001": pd.DataFrame([
                {"date": "2026-04-20", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8},
            ]),
            "000003": pd.DataFrame(),  # 空 DF 不计入 success/failed
        }
        success, failed = stock_store.save_history_rows_batch(rows_by_code)
        self.assertEqual(success, ["000001"])
        self.assertEqual(failed, [])

    def test_batch_write_splits_by_size(self):
        """batch_size=2 时 5 个 code 应切成 3 批（2+2+1）。"""
        import stock_store
        rows_by_code = {
            f"00000{i}": pd.DataFrame([
                {"date": "2026-04-20", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8},
            ])
            for i in range(1, 6)
        }
        success, failed = stock_store.save_history_rows_batch(rows_by_code, batch_size=2)
        self.assertEqual(len(success), 5)
        self.assertEqual(len(failed), 0)

    def test_batch_write_failure_isolated(self):
        """模拟第二批写入失败：第一批应成功，第二批进 failed。"""
        import stock_store
        rows_by_code = {
            f"00000{i}": pd.DataFrame([
                {"date": "2026-04-20", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8},
            ])
            for i in range(1, 5)
        }
        original_retry = stock_store._retry_locked
        call_count = {"n": 0}

        def flaky_retry(fn, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise sqlite3.OperationalError("simulated batch 2 failure")
            return original_retry(fn, *args, **kwargs)

        with mock.patch("stock_store._retry_locked", side_effect=flaky_retry):
            success, failed = stock_store.save_history_rows_batch(rows_by_code, batch_size=2)

        self.assertEqual(len(success), 2)
        self.assertEqual(len(failed), 2)
        # 第一批成功的两只应能查到
        for code in success:
            self.assertIsNotNone(stock_store.load_history(code))

    def test_batch_write_does_not_swallow_programming_error(self):
        """非 sqlite3.Error 的编程错误必须原样抛出，不能被 failed 列表吞掉。"""
        import stock_store
        rows_by_code = {
            "000001": pd.DataFrame([
                {"date": "2026-04-20", "open": 10.0, "close": 10.5, "high": 11.0, "low": 9.8},
            ]),
        }
        with mock.patch("stock_store._retry_locked", side_effect=TypeError("bug")):
            with self.assertRaises(TypeError):
                stock_store.save_history_rows_batch(rows_by_code)

    # -------- save_history_meta_batch --------

    def test_meta_batch_success(self):
        import stock_store
        metas = [
            {"code": "000001", "latest_trade_date": "2026-04-22", "row_count": 60, "source": "tencent"},
            {"code": "000002", "latest_trade_date": "2026-04-22", "row_count": 60, "source": "sina",
             "partial_fields": "open,high", "needs_repair": 1},
        ]
        success, failed = stock_store.save_history_meta_batch(metas, batch_size=500)
        self.assertEqual(set(success), {"000001", "000002"})
        self.assertEqual(failed, [])

        m2 = stock_store.load_history_meta("000002")
        self.assertEqual(m2["partial_fields"], "open,high")
        self.assertEqual(m2["needs_repair"], 1)

    def test_meta_batch_empty(self):
        import stock_store
        s, f = stock_store.save_history_meta_batch([], batch_size=500)
        self.assertEqual(s, [])
        self.assertEqual(f, [])

    def test_meta_batch_missing_code_skipped(self):
        import stock_store
        metas = [
            {"code": "000001", "latest_trade_date": "2026-04-22", "row_count": 60},
            {"code": "", "latest_trade_date": "2026-04-22"},  # 空 code 被跳过
        ]
        success, failed = stock_store.save_history_meta_batch(metas)
        self.assertEqual(success, ["000001"])
        self.assertEqual(failed, [])


if __name__ == "__main__":
    unittest.main()
