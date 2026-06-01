"""host_health：软/硬冷却阶梯 + 单 host 在途并发闸 的回归测试。"""
import os
import unittest

from src.network import host_health as hh


class HostHealthCooldownTests(unittest.TestCase):
    def setUp(self):
        # 固定 base 冷却为 3600s，避免被外部环境变量影响
        self._old_env = os.environ.get("ASHARE_SCAN_HOST_COOLDOWN_SEC")
        os.environ["ASHARE_SCAN_HOST_COOLDOWN_SEC"] = "3600"
        # 清空全局状态
        hh._HOST_HEALTH.clear()
        hh._HOST_FAIL_COUNT.clear()
        hh._HOST_INFLIGHT_SEMAPHORES.clear()

    def tearDown(self):
        hh._HOST_HEALTH.clear()
        hh._HOST_FAIL_COUNT.clear()
        hh._HOST_INFLIGHT_SEMAPHORES.clear()
        if self._old_env is None:
            os.environ.pop("ASHARE_SCAN_HOST_COOLDOWN_SEC", None)
        else:
            os.environ["ASHARE_SCAN_HOST_COOLDOWN_SEC"] = self._old_env

    def _remaining(self, host):
        return hh.cooldown_remaining(host)

    def test_soft_ladder_escalates(self):
        host = "demo.soft.com"
        # 1次：60s
        hh.mark_failed(host)
        self.assertTrue(55 <= self._remaining(host) <= 60)
        # 2次：5min
        hh.mark_failed(host)
        self.assertTrue(295 <= self._remaining(host) <= 300)
        # 3次：30min
        hh.mark_failed(host)
        self.assertTrue(1795 <= self._remaining(host) <= 1800)
        # 4次：退化到长冷却 base*1 = 3600
        hh.mark_failed(host)
        self.assertTrue(3595 <= self._remaining(host) <= 3600)

    def test_mark_ok_resets_ladder(self):
        host = "demo.reset.com"
        hh.mark_failed(host)
        hh.mark_failed(host)  # 已到 5min 档
        hh.mark_ok(host)
        self.assertEqual(self._remaining(host), 0.0)
        # 复位后再失败应回到首档 60s
        hh.mark_failed(host)
        self.assertTrue(55 <= self._remaining(host) <= 60)

    def test_hard_keeps_long_cooldown(self):
        host = "demo.hard.com"
        # hard：首次失败就是 base*1 = 3600（保守，给易被封的源用，如东财）
        hh.mark_failed(host, hard=True)
        self.assertTrue(3595 <= self._remaining(host) <= 3600)
        hh.mark_failed(host, hard=True)  # 2次：base*2 = 7200
        self.assertTrue(7195 <= self._remaining(host) <= 7200)

    def test_inflight_cap_blocks_second(self):
        host = "demo.inflight.com"
        cm = hh.limit_host_inflight(host, default_limit=1)
        cm.__enter__()
        sem = hh._HOST_INFLIGHT_SEMAPHORES[host]
        self.assertFalse(sem.acquire(blocking=False))  # cap=1，第二个拿不到
        cm.__exit__(None, None, None)
        self.assertTrue(sem.acquire(blocking=False))  # 释放后可拿
        sem.release()

    def test_inflight_env_override(self):
        os.environ["ASHARE_SCAN_HOST_INFLIGHT_LIMIT"] = "4"
        try:
            host = "demo.override.com"
            with hh.limit_host_inflight(host, default_limit=1):
                sem = hh._HOST_INFLIGHT_SEMAPHORES[host]
                got = sum(1 for _ in range(10) if sem.acquire(blocking=False))
                self.assertEqual(got, 3)  # 已持有 1 个，env=4 还剩 3
        finally:
            os.environ.pop("ASHARE_SCAN_HOST_INFLIGHT_LIMIT", None)

    def test_blank_host_is_noop(self):
        with hh.limit_host_inflight("", default_limit=1):
            pass  # 不抛错即可


if __name__ == "__main__":
    unittest.main()
