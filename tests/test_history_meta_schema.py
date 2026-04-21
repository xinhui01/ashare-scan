"""history_meta 新增字段的持久化与迁移测试。

覆盖：
- 新库直接创建时带 partial_fields / needs_repair / source_failure_streak
- 老库启动时自动 ALTER TABLE 补列
- save/load 能正确读写新字段
- load_all_history_meta_map() 批量读取
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class HistoryMetaSchemaTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._db_path = Path(self._tmp) / "test.sqlite3"
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", self._db_path)
        self._patch_dir.start()
        self._patch_db.start()
        # 重置 schema 初始化标志，保证 fixture 隔离
        import stock_store
        stock_store._SCHEMA_INITIALIZED = False
        stock_store._SCHEMA_INITIALIZED_PATH = ""
        stock_store.reset_all_connections()

    def tearDown(self):
        self._patch_dir.stop()
        self._patch_db.stop()
        import stock_store
        stock_store._SCHEMA_INITIALIZED = False
        stock_store._SCHEMA_INITIALIZED_PATH = ""
        stock_store.reset_all_connections()

    def test_fresh_db_has_new_columns(self):
        import stock_store
        stock_store.ensure_store_ready()
        with sqlite3.connect(self._db_path) as conn:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(history_meta)").fetchall()}
        self.assertIn("partial_fields", cols)
        self.assertIn("needs_repair", cols)
        self.assertIn("source_failure_streak", cols)

    def test_migration_from_old_schema(self):
        """手动建一个"老 schema"（缺三个新字段），再跑 ensure_store_ready 验证补列。"""
        with sqlite3.connect(self._db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE history_meta (
                    code TEXT PRIMARY KEY,
                    latest_trade_date TEXT NOT NULL DEFAULT '',
                    row_count INTEGER NOT NULL DEFAULT 0,
                    refreshed_at TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT ''
                );
                INSERT INTO history_meta(code, latest_trade_date, row_count, refreshed_at, source)
                VALUES ('000001', '2026-04-20', 60, '2026-04-20 15:30:00', 'tencent');
                """
            )
        import stock_store
        stock_store.ensure_store_ready()

        # 老数据应仍在，新字段应填默认值
        meta = stock_store.load_history_meta("000001")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["latest_trade_date"], "2026-04-20")
        self.assertEqual(meta["row_count"], 60)
        self.assertEqual(meta["source"], "tencent")
        self.assertEqual(meta["partial_fields"], "")
        self.assertEqual(meta["needs_repair"], 0)
        self.assertEqual(meta["source_failure_streak"], 0)

    def test_save_and_load_with_new_fields(self):
        import stock_store
        stock_store.ensure_store_ready()
        stock_store.save_history_meta(
            "000002", "2026-04-22", 60, "sina",
            partial_fields="open,high,low",
            needs_repair=1,
            source_failure_streak=2,
        )
        meta = stock_store.load_history_meta("000002")
        self.assertEqual(meta["partial_fields"], "open,high,low")
        self.assertEqual(meta["needs_repair"], 1)
        self.assertEqual(meta["source_failure_streak"], 2)

    def test_load_all_history_meta_map(self):
        import stock_store
        stock_store.ensure_store_ready()
        for code in ("000001", "000002", "600000"):
            stock_store.save_history_meta(code, "2026-04-22", 60, "tencent")
        mapping = stock_store.load_all_history_meta_map()
        self.assertEqual(set(mapping.keys()), {"000001", "000002", "600000"})
        self.assertEqual(mapping["000001"]["latest_trade_date"], "2026-04-22")
        # 6 位代码补零行为要体现在 map 的 key 上
        stock_store.save_history_meta("1", "2026-04-22", 10, "tencent")
        mapping = stock_store.load_all_history_meta_map()
        self.assertIn("000001", mapping)  # "1" 被 zfill(6)


if __name__ == "__main__":
    unittest.main()
