"""验证 CancelToken 在 scan_all_stocks 和 _call_with_timeout 中真正生效。"""
import time
import unittest

import pandas as pd

from src.utils.cancel_token import CancelToken
from stock_filter import StockFilter


class _FakeFetcher:
    """最小实现：返回一个两只股票的股票池，history 用慢任务模拟。"""

    def __init__(self, delay: float = 0.0):
        self._delay = delay
        self.history_calls = 0

    def get_all_stocks(self, force_refresh=False):
        return pd.DataFrame(
            [
                {"code": "000001", "name": "A", "exchange": "SZ", "board": "主板"},
                {"code": "000002", "name": "B", "exchange": "SZ", "board": "主板"},
            ]
        )

    def get_history_cache_summary(self):
        return {}

    def get_runtime_diagnostics(self):
        return {"history_concurrency_limit": 1}

    def history_request_concurrency_limit(self):
        return 1

    def get_history_data(self, *args, **kwargs):
        self.history_calls += 1
        if self._delay:
            time.sleep(self._delay)
        return pd.DataFrame()

    def set_log_callback(self, cb):
        pass


def _build_filter(fetcher) -> StockFilter:
    stock_filter = StockFilter.__new__(StockFilter)
    stock_filter.fetcher = fetcher
    stock_filter.trend_days = 2
    stock_filter.ma_period = 2
    stock_filter.limit_up_lookback_days = 3
    stock_filter.volume_lookback_days = 2
    stock_filter.volume_expand_enabled = False
    stock_filter.volume_expand_factor = 2.0
    stock_filter.require_limit_up_within_days = False
    stock_filter._log = None
    return stock_filter


class TestScanCancellation(unittest.TestCase):
    def test_scan_returns_empty_when_cancelled_before_submit(self):
        token = CancelToken()
        token.cancel("before_start")

        stock_filter = _build_filter(_FakeFetcher())
        results = stock_filter.scan_all_stocks(
            max_stocks=2,
            max_workers=1,
            cancel_token=token,
        )
        self.assertEqual(results, [])

    def test_scan_exits_quickly_on_mid_run_cancel(self):
        """模拟：token 在扫描启动后被外部取消，应在合理时间内返回。"""
        fetcher = _FakeFetcher(delay=0.1)
        token = CancelToken()

        stock_filter = _build_filter(fetcher)

        import threading

        def _cancel_soon():
            time.sleep(0.05)
            token.cancel("mid_run")

        threading.Thread(target=_cancel_soon, daemon=True).start()
        t0 = time.time()
        stock_filter.scan_all_stocks(
            max_stocks=2,
            max_workers=1,
            cancel_token=token,
        )
        elapsed = time.time() - t0
        # 无论命中还是空，在取消后应快速返回；给一个相对宽松的上限
        self.assertLess(elapsed, 3.0, f"scan took {elapsed:.2f}s after cancel")


class TestCallWithTimeoutCancellation(unittest.TestCase):
    def test_cancel_token_short_circuits_before_submit(self):
        stock_filter = _build_filter(_FakeFetcher())
        token = CancelToken()
        token.cancel("pre")

        called = {"n": 0}

        def _task():
            called["n"] += 1
            return "real"

        result = stock_filter._call_with_timeout(
            _task, timeout_sec=5.0, fallback="fb", cancel_token=token
        )
        self.assertEqual(result, "fb")
        self.assertEqual(called["n"], 0, "task should not have been invoked")

    def test_cancel_during_wait_returns_fallback_quickly(self):
        stock_filter = _build_filter(_FakeFetcher())
        token = CancelToken()

        def _slow():
            time.sleep(2.0)
            return "real"

        import threading

        def _cancel_soon():
            time.sleep(0.1)
            token.cancel()

        threading.Thread(target=_cancel_soon, daemon=True).start()
        t0 = time.time()
        result = stock_filter._call_with_timeout(
            _slow, timeout_sec=5.0, fallback="fb", cancel_token=token
        )
        elapsed = time.time() - t0
        self.assertEqual(result, "fb")
        self.assertLess(elapsed, 1.0, f"wait returned after {elapsed:.2f}s")


if __name__ == "__main__":
    unittest.main()
