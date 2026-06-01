"""BaoStock 持久登录：多只票连抓应只登录一次、成功不登出（不再刷屏 login/logout）。"""
import sys
import types
import unittest

from src.network import host_health
from src.sources import baostock as bsrc


class _FakeRS:
    def __init__(self, rows, fields):
        self._rows = rows
        self.fields = fields
        self._i = -1
        self.error_code = "0"
        self.error_msg = ""

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return self._rows[self._i]


class _FakeBS:
    def __init__(self):
        self.login_calls = 0
        self.logout_calls = 0

    def login(self):
        self.login_calls += 1
        return types.SimpleNamespace(error_code="0", error_msg="")

    def logout(self):
        self.logout_calls += 1
        return types.SimpleNamespace(error_code="0", error_msg="")

    def query_history_k_data_plus(self, symbol, fields, **kw):
        flds = fields.split(",")
        row = []
        for f in flds:
            if f == "date":
                row.append("2026-01-02")
            elif f == "code":
                row.append(symbol)
            elif f == "tradestatus":
                row.append("1")
            elif f in ("open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg"):
                row.append("10")
            else:
                row.append("")
        return _FakeRS([row], flds)


class BaostockPersistentLoginTests(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeBS()
        sys.modules["baostock"] = self.fake
        bsrc._LOGGED_IN = False
        bsrc._NEXT_REQUEST_AT = 0.0
        self._orig_throttle = bsrc.throttle
        bsrc.throttle = lambda: None  # 测试里不睡
        host_health.mark_ok(bsrc.HOST)  # 清掉可能的冷却

    def tearDown(self):
        sys.modules.pop("baostock", None)
        bsrc.throttle = self._orig_throttle
        bsrc._LOGGED_IN = False

    def test_login_once_no_logout_on_success(self):
        for code in ("600000", "600001", "600002"):
            out = bsrc.fetch_hist_frame(code, "20260101", "20260131")
            self.assertFalse(out.empty)
        self.assertEqual(self.fake.login_calls, 1, "持久登录：3 次抓取只 login 一次")
        self.assertEqual(self.fake.logout_calls, 0, "成功不应 logout")

    def test_failure_resets_session_for_relogin(self):
        # 先成功一次（登录）
        bsrc.fetch_hist_frame("600000", "20260101", "20260131")
        self.assertEqual(self.fake.login_calls, 1)
        # 让下一次查询失败 → 应登出重置
        def _boom(*a, **k):
            raise RuntimeError("query boom")
        self.fake.query_history_k_data_plus = _boom
        with self.assertRaises(Exception):
            bsrc.fetch_hist_frame("600001", "20260101", "20260131")
        self.assertEqual(self.fake.logout_calls, 1, "失败后应登出重置会话")
        self.assertFalse(bsrc._LOGGED_IN, "失败后登录态应清空，下次会重登")


if __name__ == "__main__":
    unittest.main()
