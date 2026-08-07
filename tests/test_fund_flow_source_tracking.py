"""资金流来源标记：同花顺兜底的伪造拆分不得冒充东财大单数据。

同花顺榜单只有一个净额，代码把它复制进主力/大单两列让界面有值。此前缓存
判断只看"big_order_amount 有没有值"，会被这份副本骗过 —— 东财恢复后再也
不为补齐大单重拉。改为以 source 列为准。
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


def _em_rows(dates):
    n = len(dates)
    return pd.DataFrame({
        "date": list(dates),
        "close": [10.0] * n,
        "change_pct": [1.0] * n,
        "main_force_amount": [1_000_000.0] * n,
        "main_force_ratio": [5.0] * n,
        "big_order_amount": [600_000.0] * n,
        "big_order_ratio": [3.0] * n,
        "super_big_order_amount": [400_000.0] * n,
        "super_big_order_ratio": [2.0] * n,
    })


def _ths_rows(dates):
    """同花顺兜底形态：主力==大单（同一净额的副本），超大单为空。"""
    n = len(dates)
    return pd.DataFrame({
        "date": list(dates),
        "close": [10.0] * n,
        "main_force_amount": [-728_000_000.0] * n,
        "big_order_amount": [-728_000_000.0] * n,
        "super_big_order_amount": [None] * n,
    })


class FundFlowSourceTrackingTestCase(unittest.TestCase):
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

    def test_source_column_round_trips(self):
        import stock_store

        df = _em_rows(["2026-08-06", "2026-08-07"])
        df["source"] = "eastmoney"
        stock_store.save_fund_flow("000938", df)

        loaded = stock_store.load_fund_flow("000938")
        self.assertIsNotNone(loaded)
        self.assertIn("source", loaded.columns)
        self.assertEqual(set(loaded["source"]), {"eastmoney"})

    def test_missing_source_defaults_to_empty_not_crash(self):
        import stock_store

        stock_store.save_fund_flow("000938", _em_rows(["2026-08-07"]))
        loaded = stock_store.load_fund_flow("000938")
        self.assertEqual(list(loaded["source"]), [""])

    def test_migration_backfills_ths_rows_by_shape(self):
        """老库无 source 列：迁移后按"主力==大单且超大单空"识别同花顺行。"""
        import stock_store

        # 造一个"迁移前"的库：删掉 source 列
        with sqlite3.connect(self._db_path) as conn:
            conn.execute("DROP TABLE fund_flow")
            conn.execute(
                """
                CREATE TABLE fund_flow (
                    code TEXT NOT NULL, trade_date TEXT NOT NULL, close REAL, change_pct REAL,
                    main_force_amount REAL, main_force_ratio REAL, big_order_amount REAL,
                    big_order_ratio REAL, super_big_order_amount REAL, super_big_order_ratio REAL,
                    updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY (code, trade_date)
                )
                """
            )
            # 同花顺形态：主力==大单，超大单 NULL
            conn.execute(
                "INSERT INTO fund_flow VALUES ('000938','2026-08-07',10.0,1.0,"
                "-728000000.0,NULL,-728000000.0,NULL,NULL,NULL,'')"
            )
            # 东财形态：三档不同
            conn.execute(
                "INSERT INTO fund_flow VALUES ('000938','2026-08-06',10.0,1.0,"
                "1000000.0,5.0,600000.0,3.0,400000.0,2.0,'')"
            )
            conn.commit()

        stock_store._SCHEMA_INITIALIZED = False
        stock_store._SCHEMA_INITIALIZED_PATH = ""
        stock_store.reset_all_connections()
        stock_store.ensure_store_ready()

        loaded = stock_store.load_fund_flow("000938").set_index("date")
        self.assertEqual(loaded.loc["2026-08-07", "source"], "ths")
        self.assertEqual(loaded.loc["2026-08-06", "source"], "")


class HasRealBigOrderSplitTestCase(unittest.TestCase):
    def test_ths_rows_are_not_real_split(self):
        from stock_data import _has_real_big_order_split

        df = _ths_rows(["2026-08-07"])
        df["source"] = "ths"
        self.assertFalse(_has_real_big_order_split(df))

    def test_eastmoney_rows_are_real_split(self):
        from stock_data import _has_real_big_order_split

        df = _em_rows(["2026-08-07"])
        df["source"] = "eastmoney"
        self.assertTrue(_has_real_big_order_split(df))

    def test_legacy_rows_without_source_stay_trusted(self):
        """无 source 的老数据保持旧行为，避免全量重拉。"""
        from stock_data import _has_real_big_order_split

        self.assertTrue(_has_real_big_order_split(_em_rows(["2026-08-07"])))

    def test_mixed_cache_trusts_eastmoney_portion(self):
        from stock_data import _has_real_big_order_split

        em = _em_rows(["2026-08-06"])
        em["source"] = "eastmoney"
        ths = _ths_rows(["2026-08-07"])
        ths["source"] = "ths"
        self.assertTrue(_has_real_big_order_split(pd.concat([em, ths], ignore_index=True)))

    def test_empty_and_missing_column_are_false(self):
        from stock_data import _has_real_big_order_split

        self.assertFalse(_has_real_big_order_split(None))
        self.assertFalse(_has_real_big_order_split(pd.DataFrame()))
        self.assertFalse(_has_real_big_order_split(pd.DataFrame({"date": ["2026-08-07"]})))


if __name__ == "__main__":
    unittest.main()
