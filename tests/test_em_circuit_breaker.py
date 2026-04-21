"""EMCircuitBreaker：阈值、指数退避、跨线程一致性、reset 语义。"""
import threading
import unittest

from src.utils.em_circuit_breaker import EMCircuitBreaker


class _FakeClock:
    def __init__(self, start: float = 1000.0):
        self._t = float(start)

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += float(seconds)


class TestEMCircuitBreaker(unittest.TestCase):
    def _make(self, **kwargs):
        clock = _FakeClock()
        defaults = dict(
            fail_threshold=3,
            initial_cooldown_sec=30.0,
            max_cooldown_sec=600.0,
            clock=clock,
        )
        defaults.update(kwargs)
        cb = EMCircuitBreaker(**defaults)
        return cb, clock

    def test_initial_state_closed(self):
        cb, _ = self._make()
        self.assertFalse(cb.is_open())
        self.assertEqual(cb.fail_count(), 0)
        self.assertEqual(cb.consecutive_trips(), 0)

    def test_opens_after_threshold_failures(self):
        cb, _ = self._make()
        cb.record_failure()
        cb.record_failure()
        self.assertFalse(cb.is_open())  # 2 次还未到阈值
        cb.record_failure()
        self.assertTrue(cb.is_open())
        self.assertEqual(cb.consecutive_trips(), 1)

    def test_cooldown_expires_after_initial_window(self):
        cb, clock = self._make()
        for _ in range(3):
            cb.record_failure()
        self.assertTrue(cb.is_open())
        clock.advance(29.9)
        self.assertTrue(cb.is_open())
        clock.advance(0.2)
        self.assertFalse(cb.is_open())

    def test_success_resets_counters(self):
        cb, _ = self._make()
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        self.assertEqual(cb.fail_count(), 0)
        self.assertFalse(cb.is_open())

    def test_exponential_backoff_grows_each_trip(self):
        """每次越过阈值都让 consecutive_trips+1，冷却时长指数放大。"""
        cb, clock = self._make()
        # 第 1 次跨过阈值 → 冷却 30s
        for _ in range(3):
            cb.record_failure()
        self.assertAlmostEqual(cb.open_until() - clock(), 30.0, delta=0.01)
        # 再失败一次（fail_count 已 ≥ threshold，会再度触发）→ 冷却 60s
        cb.record_failure()
        self.assertAlmostEqual(cb.open_until() - clock(), 60.0, delta=0.01)
        # 再失败一次 → 冷却 120s
        cb.record_failure()
        self.assertAlmostEqual(cb.open_until() - clock(), 120.0, delta=0.01)

    def test_max_cooldown_is_respected(self):
        cb, clock = self._make(
            initial_cooldown_sec=100.0, max_cooldown_sec=200.0
        )
        # 连续触发若干次：每次 record_failure 都处于"已达阈值"状态，consecutive_trips
        # 单调增，冷却时长会指数放大但应被 max_cooldown_sec 截断。
        for _ in range(10):
            cb.record_failure()
        cooldown_now = cb.open_until() - clock()
        self.assertLessEqual(cooldown_now, 200.0 + 0.01)
        self.assertGreater(cb.consecutive_trips(), 2)  # 确实触发了多轮

    def test_reset_is_alias_for_success(self):
        cb, _ = self._make()
        cb.record_failure()
        cb.record_failure()
        cb.reset()
        self.assertEqual(cb.fail_count(), 0)
        self.assertEqual(cb.consecutive_trips(), 0)

    def test_thread_safe_concurrent_record(self):
        cb, _ = self._make(fail_threshold=100)

        def _worker():
            for _ in range(50):
                cb.record_failure()

        threads = [threading.Thread(target=_worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)
        self.assertEqual(cb.fail_count(), 4 * 50)

    def test_singleton_returns_same_instance(self):
        a = EMCircuitBreaker.instance()
        b = EMCircuitBreaker.instance()
        self.assertIs(a, b)


class TestModuleLevelFunctions(unittest.TestCase):
    """覆盖 is_open / record_failure / record_success 顶层函数（单例路径）。"""

    def setUp(self):
        from src.utils.em_circuit_breaker import EMCircuitBreaker
        EMCircuitBreaker.instance().reset()

    def tearDown(self):
        from src.utils.em_circuit_breaker import EMCircuitBreaker
        EMCircuitBreaker.instance().reset()

    def test_wrapper_functions_hit_singleton(self):
        from src.utils.em_circuit_breaker import (
            EMCircuitBreaker,
            is_open,
            record_failure,
            record_success,
        )
        self.assertFalse(is_open())
        for _ in range(3):
            record_failure()
        self.assertTrue(is_open())
        record_success()
        self.assertFalse(is_open())

    def test_stock_data_forwarders_use_same_singleton(self):
        """stock_data 里的 `_eastmoney_circuit_breaker_*` 转发函数应等价于顶层 API。"""
        import stock_data
        from src.utils.em_circuit_breaker import EMCircuitBreaker

        EMCircuitBreaker.instance().reset()
        self.assertFalse(stock_data._eastmoney_circuit_breaker_open())
        for _ in range(3):
            stock_data._eastmoney_circuit_breaker_record_failure()
        self.assertTrue(stock_data._eastmoney_circuit_breaker_open())
        stock_data._eastmoney_circuit_breaker_record_success()
        self.assertFalse(stock_data._eastmoney_circuit_breaker_open())


if __name__ == "__main__":
    unittest.main()
