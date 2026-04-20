"""CancelToken 基础行为 + 与 should_stop 合并的兼容性。"""
import threading
import time
import unittest

from src.utils.cancel_token import CancelToken, CancelledError, coerce_should_stop


class TestCancelToken(unittest.TestCase):
    def test_default_not_cancelled(self):
        token = CancelToken()
        self.assertFalse(token.is_cancelled())
        self.assertEqual(token.reason, "")

    def test_cancel_sets_state_and_reason(self):
        token = CancelToken()
        token.cancel("user_stop")
        self.assertTrue(token.is_cancelled())
        self.assertEqual(token.reason, "user_stop")

    def test_cancel_reason_is_sticky_on_first_set(self):
        token = CancelToken()
        token.cancel("first")
        token.cancel("second")
        self.assertEqual(token.reason, "first")

    def test_raise_if_cancelled(self):
        token = CancelToken()
        token.raise_if_cancelled()  # no-op
        token.cancel("boom")
        with self.assertRaises(CancelledError):
            token.raise_if_cancelled()

    def test_wait_returns_true_when_cancelled(self):
        token = CancelToken()

        def _cancel_later():
            time.sleep(0.05)
            token.cancel("late")

        threading.Thread(target=_cancel_later, daemon=True).start()
        t0 = time.time()
        fired = token.wait(timeout=2.0)
        elapsed = time.time() - t0
        self.assertTrue(fired)
        self.assertLess(elapsed, 1.5)

    def test_wait_times_out_when_not_cancelled(self):
        token = CancelToken()
        t0 = time.time()
        fired = token.wait(timeout=0.1)
        self.assertFalse(fired)
        self.assertGreaterEqual(time.time() - t0, 0.05)

    def test_reset_clears_state(self):
        token = CancelToken()
        token.cancel("x")
        token.reset()
        self.assertFalse(token.is_cancelled())
        self.assertEqual(token.reason, "")

    def test_as_should_stop_tracks_state(self):
        token = CancelToken()
        predicate = token.as_should_stop()
        self.assertFalse(predicate())
        token.cancel()
        self.assertTrue(predicate())


class TestCoerceShouldStop(unittest.TestCase):
    def test_none_inputs(self):
        self.assertIsNone(coerce_should_stop(None, None))

    def test_only_should_stop(self):
        flag = {"v": False}
        predicate = coerce_should_stop(None, lambda: flag["v"])
        self.assertFalse(predicate())
        flag["v"] = True
        self.assertTrue(predicate())

    def test_only_token(self):
        token = CancelToken()
        predicate = coerce_should_stop(token, None)
        self.assertFalse(predicate())
        token.cancel()
        self.assertTrue(predicate())

    def test_combines_both(self):
        token = CancelToken()
        flag = {"v": False}
        predicate = coerce_should_stop(token, lambda: flag["v"])
        self.assertFalse(predicate())
        flag["v"] = True
        self.assertTrue(predicate())
        flag["v"] = False
        token.cancel()
        self.assertTrue(predicate())


if __name__ == "__main__":
    unittest.main()
