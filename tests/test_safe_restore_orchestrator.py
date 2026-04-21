"""SafeRestoreOrchestrator：验证 broadcast → wait threads → restore → reset 的顺序。"""
import threading
import unittest

from src.services.db_admin_service import SafeRestoreOrchestrator


class _FakeThread:
    def __init__(self, alive: bool = True, join_sleep: float = 0.0):
        self._alive = alive
        self._joined_with_timeout = None
        self._join_sleep = join_sleep

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout=None):
        self._joined_with_timeout = timeout
        self._alive = False  # 模拟成功退出


class TestSafeRestoreOrchestrator(unittest.TestCase):
    def test_happy_path_call_order(self):
        trace: list[str] = []

        def _broadcast():
            trace.append("broadcast")

        t1 = _FakeThread(alive=True)
        t2 = _FakeThread(alive=True)

        def _sources():
            trace.append("sources")
            return (t1, t2)

        def _restore(path):
            trace.append(f"restore:{path}")
            return True

        def _reset():
            trace.append("reset")

        orch = SafeRestoreOrchestrator(
            broadcast_cancel=_broadcast,
            thread_sources=_sources,
            wait_timeout_sec=2.0,
            reset_connections=_reset,
            restore_impl=_restore,
        )
        ok = orch.execute("/tmp/snapshot.sqlite3")
        self.assertTrue(ok)
        self.assertEqual(
            trace,
            ["broadcast", "sources", "restore:/tmp/snapshot.sqlite3", "reset"],
        )
        self.assertEqual(t1._joined_with_timeout, 2.0)
        self.assertEqual(t2._joined_with_timeout, 2.0)

    def test_skips_dead_or_none_threads(self):
        t_alive = _FakeThread(alive=True)
        t_dead = _FakeThread(alive=False)

        def _sources():
            return (None, t_dead, t_alive)

        orch = SafeRestoreOrchestrator(
            broadcast_cancel=lambda: None,
            thread_sources=_sources,
            wait_timeout_sec=1.0,
            reset_connections=lambda: None,
            restore_impl=lambda _p: True,
        )
        orch.execute("whatever")
        self.assertEqual(t_alive._joined_with_timeout, 1.0)
        self.assertIsNone(t_dead._joined_with_timeout)

    def test_failure_does_not_reset(self):
        reset_called = []

        orch = SafeRestoreOrchestrator(
            broadcast_cancel=lambda: None,
            thread_sources=lambda: (),
            wait_timeout_sec=0.5,
            reset_connections=lambda: reset_called.append(1),
            restore_impl=lambda _p: False,
        )
        ok = orch.execute("bad")
        self.assertFalse(ok)
        self.assertEqual(reset_called, [])

    def test_broadcast_runs_before_thread_join(self):
        """广播必须先发，确保线程能在 join 前看到取消信号。"""
        event_order = []
        evt = threading.Event()

        def _broadcast():
            event_order.append("broadcast")
            evt.set()

        class _WatchThread:
            def is_alive(self):
                return not evt.is_set()

            def join(self, timeout=None):
                event_order.append("join")

        orch = SafeRestoreOrchestrator(
            broadcast_cancel=_broadcast,
            thread_sources=lambda: (_WatchThread(),),
            wait_timeout_sec=0.2,
            reset_connections=lambda: None,
            restore_impl=lambda _p: True,
        )
        orch.execute("p")
        # broadcast 必须排在前
        self.assertEqual(event_order[0], "broadcast")


if __name__ == "__main__":
    unittest.main()
