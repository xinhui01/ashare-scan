"""restore_database 的一致性测试 —— 覆盖写锁、连接失效、schema 重建。"""
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


class RestoreTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self._db_path = Path(self._tmp) / "main.sqlite3"
        self._patch_dir = mock.patch("stock_store._DATA_DIR", Path(self._tmp))
        self._patch_db = mock.patch("stock_store._DB_PATH", self._db_path)
        self._patch_dir.start()
        self._patch_db.start()
        import stock_store
        # 必须在 patch 之后 reset，否则会残留之前运行缓存的连接
        stock_store.reset_all_connections()
        stock_store.ensure_store_ready()

    def tearDown(self):
        import stock_store
        stock_store.reset_all_connections()
        self._patch_dir.stop()
        self._patch_db.stop()


class TestResetAllConnections(RestoreTestCase):
    def test_reset_clears_thread_local_and_schema_flag(self):
        import stock_store
        # 触发一次连接创建
        stock_store.save_universe(pd.DataFrame([
            {"code": "000001", "name": "A", "exchange": "SZ", "board": "主板", "concepts": ""}
        ]))
        self.assertTrue(stock_store._SCHEMA_INITIALIZED)
        self.assertIsNotNone(getattr(stock_store._THREAD_LOCAL, "conn", None))

        stock_store.reset_all_connections()
        self.assertFalse(stock_store._SCHEMA_INITIALIZED)
        # 旧连接应已被关闭；再次使用需要能透明重建
        loaded = stock_store.load_universe()
        self.assertIsNotNone(loaded)
        self.assertEqual(len(loaded), 1)


class TestRestoreDatabase(RestoreTestCase):
    def _make_backup_with_row(self, code: str, name: str) -> Path:
        """在临时位置构造一个备份文件，含单条 universe 记录。"""
        backup_path = Path(self._tmp) / f"backup_{code}.sqlite3"
        conn = sqlite3.connect(str(backup_path))
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
        conn.execute(
            "INSERT INTO universe(code, name) VALUES (?, ?)",
            (code, name),
        )
        conn.commit()
        conn.close()
        return backup_path

    def test_restore_replaces_data(self):
        import stock_store
        stock_store.save_universe(pd.DataFrame([
            {"code": "111111", "name": "OLD", "exchange": "", "board": "", "concepts": ""}
        ]))
        backup = self._make_backup_with_row("999999", "NEW")

        ok = stock_store.restore_database(str(backup))
        self.assertTrue(ok)

        # 恢复后应读到新库内容，而不是旧库的 OLD 行
        loaded = stock_store.load_universe()
        self.assertIsNotNone(loaded)
        codes = list(loaded["code"])
        self.assertIn("999999", codes)
        self.assertNotIn("111111", codes)

    def test_restore_creates_pre_restore_backup(self):
        import stock_store
        stock_store.save_universe(pd.DataFrame([
            {"code": "111111", "name": "OLD", "exchange": "", "board": "", "concepts": ""}
        ]))
        backup = self._make_backup_with_row("222222", "X")

        before = stock_store.list_backups()
        stock_store.restore_database(str(backup))
        after = stock_store.list_backups()
        self.assertGreater(len(after), len(before))

    def test_restore_missing_file_returns_false(self):
        import stock_store
        ok = stock_store.restore_database(str(Path(self._tmp) / "does_not_exist.sqlite3"))
        self.assertFalse(ok)

    def test_restore_invalidates_existing_thread_local_connection(self):
        """恢复后同一线程再发起查询必须走新连接，否则读到的是旧 schema。"""
        import stock_store
        stock_store.save_universe(pd.DataFrame([
            {"code": "111111", "name": "OLD", "exchange": "", "board": "", "concepts": ""}
        ]))
        old_conn = stock_store._connect()
        backup = self._make_backup_with_row("333333", "Y")

        self.assertTrue(stock_store.restore_database(str(backup)))
        # 恢复后线程本地引用应被清掉
        self.assertIsNone(getattr(stock_store._THREAD_LOCAL, "conn", None))
        # 再次 _connect() 必须返回一个新的连接对象
        new_conn = stock_store._connect()
        self.assertIsNot(old_conn, new_conn)

    def test_restore_holds_write_lock(self):
        """恢复期间其他写入应被写锁阻塞，从而保证串行化。"""
        import stock_store
        backup = self._make_backup_with_row("444444", "Z")

        # 人为持锁 0.3s，再放开；restore 必须排队等待
        release = threading.Event()
        lock_held = threading.Event()

        def _hold_lock():
            with stock_store._DB_WRITE_LOCK:
                lock_held.set()
                release.wait(timeout=2.0)

        t = threading.Thread(target=_hold_lock, daemon=True)
        t.start()
        self.assertTrue(lock_held.wait(timeout=1.0))

        # restore 在后台线程开启；在释放之前它不应完成
        restore_done = threading.Event()

        def _do_restore():
            stock_store.restore_database(str(backup))
            restore_done.set()

        threading.Thread(target=_do_restore, daemon=True).start()
        # 给它一点时间去抢锁
        self.assertFalse(restore_done.wait(timeout=0.3))
        release.set()
        t.join(timeout=2.0)
        self.assertTrue(restore_done.wait(timeout=5.0))


if __name__ == "__main__":
    unittest.main()
