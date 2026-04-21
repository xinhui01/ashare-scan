"""db_admin_service 的直连测试：确认抽出后职责保持不变。

注意：stock_store 中的旧符号（backup_database 等）仍存在，只是转发。
这里直接走 src.services.db_admin_service，验证新模块本身。
"""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


class DBAdminServiceTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._db_path = Path(self._tmp) / "main.sqlite3"
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", self._db_path)
        self._patch_dir.start()
        self._patch_db.start()
        import stock_store
        stock_store.reset_all_connections()
        stock_store.ensure_store_ready()

    def tearDown(self):
        import stock_store
        stock_store.reset_all_connections()
        self._patch_dir.stop()
        self._patch_db.stop()


class TestDBAdminService(DBAdminServiceTestCase):
    def test_backup_database_creates_file(self):
        from src.services import db_admin_service
        import stock_store
        stock_store.save_universe(pd.DataFrame([
            {"code": "000001", "name": "A", "exchange": "", "board": "", "concepts": ""}
        ]))
        dest = db_admin_service.backup_database()
        self.assertTrue(dest.is_file())
        self.assertGreater(dest.stat().st_size, 0)

    def test_list_backups_returns_sorted_entries(self):
        from src.services import db_admin_service
        import stock_store
        stock_store.save_universe(pd.DataFrame([
            {"code": "000001", "name": "A", "exchange": "", "board": "", "concepts": ""}
        ]))
        db_admin_service.backup_database()
        entries = db_admin_service.list_backups()
        self.assertGreaterEqual(len(entries), 1)
        self.assertIn("path", entries[0])
        self.assertIn("size_mb", entries[0])

    def test_restore_missing_file_returns_false(self):
        from src.services import db_admin_service
        ok = db_admin_service.restore_database(str(Path(self._tmp) / "nope.sqlite3"))
        self.assertFalse(ok)

    def test_restore_replaces_data(self):
        from src.services import db_admin_service
        import stock_store

        stock_store.save_universe(pd.DataFrame([
            {"code": "111111", "name": "OLD", "exchange": "", "board": "", "concepts": ""}
        ]))
        # 构造一个备份文件
        backup = Path(self._tmp) / "alt.sqlite3"
        conn = sqlite3.connect(str(backup))
        conn.executescript(
            """
            CREATE TABLE universe (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                exchange TEXT NOT NULL DEFAULT '',
                board TEXT NOT NULL DEFAULT '',
                concepts TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
        conn.execute("INSERT INTO universe(code, name) VALUES (?, ?)", ("999999", "NEW"))
        conn.commit()
        conn.close()

        self.assertTrue(db_admin_service.restore_database(str(backup)))
        loaded = stock_store.load_universe()
        self.assertIsNotNone(loaded)
        self.assertIn("999999", list(loaded["code"]))
        self.assertNotIn("111111", list(loaded["code"]))

    def test_stock_store_forwarders_match_service(self):
        """stock_store.backup_database 应等价于直接调用 service。"""
        from src.services import db_admin_service
        import stock_store

        direct = db_admin_service.backup_database()
        via_forwarder = stock_store.backup_database()
        self.assertTrue(direct.is_file())
        self.assertTrue(via_forwarder.is_file())

    def test_cleanup_all_aggregates_per_table(self):
        from src.services import db_admin_service
        result = db_admin_service.cleanup_all()
        # 空库上 cleanup 应返回零值字典，不抛
        self.assertIn("history", result)
        self.assertIn("intraday", result)
        self.assertIn("scan_snapshots", result)


class TestWatchlistCSV(DBAdminServiceTestCase):
    def test_export_then_import_roundtrip(self):
        from src.services import db_admin_service
        import stock_store

        stock_store.save_watchlist_item({
            "code": "000001", "name": "A", "status": "watching",
            "note": "", "board": "主板", "score": 60,
        })
        csv_path = Path(self._tmp) / "watch.csv"
        exported = db_admin_service.export_watchlist_csv(str(csv_path))
        self.assertEqual(exported, 1)

        # 清掉自选股后再从 CSV 导入
        stock_store.delete_watchlist_item("000001")
        self.assertEqual(len(stock_store.load_watchlist()), 0)
        imported = db_admin_service.import_watchlist_csv(str(csv_path))
        self.assertEqual(imported, 1)
        self.assertEqual(len(stock_store.load_watchlist()), 1)


if __name__ == "__main__":
    unittest.main()
