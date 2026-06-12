"""ProgressThrottle 行为测试：纯逻辑，时间通过参数注入，无需真实时钟。

节流器的唯一职责：在高频进度事件中，决定"这一次是否应该真正刷新 UI"，
把后台每只股票一次的回调压成按时间间隔采样的少数几次，避免 root.after(0)
回调风暴冻结 Tk 主线程。
"""
import unittest

from src.gui.progress_throttle import ProgressThrottle


class TestProgressThrottle(unittest.TestCase):
    def test_first_call_always_emits(self):
        t = ProgressThrottle(interval_ms=120)
        self.assertTrue(t.should_emit(now_ms=1000.0))

    def test_suppressed_within_interval(self):
        t = ProgressThrottle(interval_ms=120)
        t.should_emit(now_ms=1000.0)  # 首次，推送
        self.assertFalse(t.should_emit(now_ms=1050.0))  # 50ms 后，间隔内，抑制
        self.assertFalse(t.should_emit(now_ms=1119.0))  # 119ms，仍在间隔内

    def test_emits_after_interval(self):
        t = ProgressThrottle(interval_ms=120)
        t.should_emit(now_ms=1000.0)
        self.assertTrue(t.should_emit(now_ms=1120.0))  # 恰好满 120ms，推送

    def test_final_always_emits_even_within_interval(self):
        t = ProgressThrottle(interval_ms=120)
        t.should_emit(now_ms=1000.0)
        # 5ms 后但是最后一只 → 必须推送，保证结束时显示 100% 和最终统计
        self.assertTrue(t.should_emit(now_ms=1005.0, is_final=True))

    def test_emit_resets_baseline(self):
        t = ProgressThrottle(interval_ms=120)
        t.should_emit(now_ms=1000.0)        # 推送，基准=1000
        self.assertTrue(t.should_emit(now_ms=1200.0))   # 推送，基准重置=1200
        self.assertFalse(t.should_emit(now_ms=1250.0))  # 距 1200 仅 50ms，抑制

    def test_burst_is_heavily_compressed(self):
        """模拟缓存更新：5000 只股票，每只间隔约 6ms 完成（总约 30 秒）。
        节流后真正推送的次数应远小于 5000（数量级压缩）。"""
        t = ProgressThrottle(interval_ms=120)
        emits = 0
        total = 5000
        for i in range(total):
            now = 1000.0 + i * 6.0  # 每只 6ms
            is_final = (i == total - 1)
            if t.should_emit(now_ms=now, is_final=is_final):
                emits += 1
        # 30 秒 / 120ms ≈ 250 次上限，给足余量断言 < 300，且远小于 5000
        self.assertLess(emits, 300)
        self.assertGreater(emits, 0)

    def test_final_emits_when_burst_ends_inside_interval(self):
        """最后一只即便紧跟前一次推送，也要确保推送（否则界面停在 99%）。"""
        t = ProgressThrottle(interval_ms=120)
        t.should_emit(now_ms=1000.0)  # 推送
        # 紧接着就是最后一只，距上次仅 10ms
        self.assertTrue(t.should_emit(now_ms=1010.0, is_final=True))


if __name__ == "__main__":
    unittest.main()
