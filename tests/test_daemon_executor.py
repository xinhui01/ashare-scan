"""DaemonThreadPoolExecutor：验证 worker 是 daemon，常规 submit 行为未被破坏。"""
import threading
import time
import unittest

from src.utils.daemon_executor import DaemonThreadPoolExecutor


class TestDaemonExecutor(unittest.TestCase):
    def test_worker_threads_are_daemon(self):
        captured_threads: list[threading.Thread] = []

        def _record_current_thread():
            captured_threads.append(threading.current_thread())
            # 让 worker 稍等，让下一个任务也能跑到另一个 worker 上
            time.sleep(0.05)

        with DaemonThreadPoolExecutor(max_workers=2, thread_name_prefix="tdaemon") as ex:
            for _ in range(4):
                ex.submit(_record_current_thread)

        self.assertTrue(captured_threads)
        for t in captured_threads:
            self.assertTrue(t.daemon, f"worker {t.name!r} should be daemon")

    def test_submit_runs_and_returns_future_value(self):
        with DaemonThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(lambda x=i: x * x) for i in range(5)]
            results = sorted(f.result() for f in futs)
        self.assertEqual(results, [0, 1, 4, 9, 16])

    def test_exception_propagates_through_future(self):
        with DaemonThreadPoolExecutor(max_workers=1) as ex:
            def _boom():
                raise RuntimeError("kaboom")

            fut = ex.submit(_boom)
            with self.assertRaises(RuntimeError):
                fut.result(timeout=2.0)

    def test_thread_name_prefix_applied(self):
        seen_names: list[str] = []

        def _record_name():
            seen_names.append(threading.current_thread().name)

        with DaemonThreadPoolExecutor(max_workers=1, thread_name_prefix="testprefix") as ex:
            ex.submit(_record_name).result(timeout=2.0)

        self.assertTrue(seen_names)
        self.assertTrue(seen_names[0].startswith("testprefix"))

    def test_stock_data_reexport_is_same_class(self):
        import stock_data
        self.assertIs(
            stock_data.DaemonThreadPoolExecutor,
            DaemonThreadPoolExecutor,
        )


if __name__ == "__main__":
    unittest.main()
