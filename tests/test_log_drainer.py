"""LogDrainer：验证排队、主线程直写、关闭时短路、tick 重新调度。"""
import threading
import unittest

from src.gui.log_drainer import LogDrainer


class _FakeDispatcher:
    def __init__(self):
        self._closing = False
        self.scheduled = []  # [(delay_ms, callback), ...]

    @property
    def is_closing(self) -> bool:
        return self._closing

    def mark_closing(self):
        self._closing = True

    def safe_after(self, delay_ms, callback):
        if self._closing:
            return None
        self.scheduled.append((delay_ms, callback))
        return f"id-{len(self.scheduled)}"

    def post(self, cb):
        self.safe_after(0, cb)


class TestLogDrainer(unittest.TestCase):
    def _make(self, sink=None):
        sink_fn = sink or (lambda m: sink_calls.append(m))
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=sink_fn,
            poll_interval_ms=100,
        )
        return drainer, disp

    def test_main_thread_enqueue_calls_sink_immediately(self):
        calls = []
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=calls.append,
            poll_interval_ms=100,
        )
        drainer.enqueue("hello")
        self.assertEqual(calls, ["hello"])
        self.assertEqual(drainer.pending_count, 0)

    def test_other_thread_enqueue_buffers(self):
        calls = []
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=0,  # 假装主线程 id 是 0，当前线程 id 不会匹配
            sink=calls.append,
            poll_interval_ms=100,
        )
        drainer.enqueue("from-worker")
        self.assertEqual(calls, [])
        self.assertEqual(drainer.pending_count, 1)

        processed = drainer.drain_once()
        self.assertEqual(processed, 1)
        self.assertEqual(calls, ["from-worker"])

    def test_enqueue_is_noop_when_closing(self):
        calls = []
        disp = _FakeDispatcher()
        disp.mark_closing()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=calls.append,
            poll_interval_ms=100,
        )
        drainer.enqueue("after close")
        self.assertEqual(calls, [])

    def test_drain_once_is_noop_when_closing(self):
        calls = []
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=0,
            sink=calls.append,
            poll_interval_ms=100,
        )
        drainer.enqueue("queued")
        disp.mark_closing()
        self.assertEqual(drainer.drain_once(), 0)
        self.assertEqual(calls, [])

    def test_start_schedules_tick_and_is_idempotent(self):
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=lambda _m: None,
            poll_interval_ms=100,
        )
        drainer.start()
        drainer.start()  # 重复调用
        self.assertEqual(len(disp.scheduled), 1)
        self.assertEqual(disp.scheduled[0][0], 100)

    def test_tick_self_schedules_next(self):
        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=lambda _m: None,
            poll_interval_ms=100,
        )
        drainer.start()
        first_tick = disp.scheduled[0][1]
        # 手动跑一次 tick：它应该再往 dispatcher 调度一条
        first_tick()
        self.assertEqual(len(disp.scheduled), 2)

    def test_sink_exception_does_not_propagate(self):
        def bad_sink(_m):
            raise RuntimeError("sink broken")

        disp = _FakeDispatcher()
        drainer = LogDrainer(
            dispatcher=disp,
            main_thread_id=threading.get_ident(),
            sink=bad_sink,
            poll_interval_ms=100,
        )
        # 主线程路径
        try:
            drainer.enqueue("x")
        except Exception:  # pragma: no cover
            self.fail("enqueue should swallow sink errors")
        # drain_once 路径
        drainer2 = LogDrainer(
            dispatcher=_FakeDispatcher(),
            main_thread_id=0,
            sink=bad_sink,
            poll_interval_ms=100,
        )
        drainer2.enqueue("y")
        try:
            drainer2.drain_once()
        except Exception:  # pragma: no cover
            self.fail("drain_once should swallow sink errors")


if __name__ == "__main__":
    unittest.main()
