"""UIDispatcher 行为测试：无需真正的 Tk，只要 root.after/winfo_exists 行为可控即可。"""
import unittest

from src.gui.ui_dispatch import UIDispatcher


class _FakeRoot:
    """Stub Tk root：记录 after 调度，支持模拟 winfo_exists 返回值/抛异常。"""

    def __init__(self):
        self.scheduled = []
        self._exists = True
        self._winfo_raises = None
        self._after_raises = None
        self._after_counter = 0

    def after(self, delay_ms, callback):
        if self._after_raises is not None:
            raise self._after_raises
        self._after_counter += 1
        self.scheduled.append((delay_ms, callback))
        return f"after-id-{self._after_counter}"

    def winfo_exists(self):
        if self._winfo_raises is not None:
            raise self._winfo_raises
        return self._exists


class TestUIDispatcher(unittest.TestCase):
    def test_safe_after_schedules_when_alive(self):
        root = _FakeRoot()
        disp = UIDispatcher(root)
        called = []
        after_id = disp.safe_after(50, lambda: called.append(1))
        self.assertEqual(len(root.scheduled), 1)
        self.assertEqual(root.scheduled[0][0], 50)
        self.assertTrue(after_id)

    def test_safe_after_noop_after_mark_closing(self):
        root = _FakeRoot()
        disp = UIDispatcher(root)
        disp.mark_closing()
        result = disp.safe_after(0, lambda: None)
        self.assertIsNone(result)
        self.assertEqual(root.scheduled, [])

    def test_safe_after_noop_when_root_gone(self):
        root = _FakeRoot()
        root._exists = False
        disp = UIDispatcher(root)
        result = disp.safe_after(0, lambda: None)
        self.assertIsNone(result)
        self.assertEqual(root.scheduled, [])

    def test_safe_after_swallows_tcl_error(self):
        import tkinter as tk

        root = _FakeRoot()
        root._after_raises = tk.TclError("simulated")
        disp = UIDispatcher(root)
        result = disp.safe_after(0, lambda: None)
        self.assertIsNone(result)

    def test_safe_after_swallows_winfo_exists_error(self):
        import tkinter as tk

        root = _FakeRoot()
        root._winfo_raises = tk.TclError("simulated")
        disp = UIDispatcher(root)
        result = disp.safe_after(0, lambda: None)
        self.assertIsNone(result)

    def test_post_uses_zero_delay(self):
        root = _FakeRoot()
        disp = UIDispatcher(root)
        disp.post(lambda: None)
        self.assertEqual(root.scheduled[0][0], 0)

    def test_is_closing_property(self):
        root = _FakeRoot()
        disp = UIDispatcher(root)
        self.assertFalse(disp.is_closing)
        disp.mark_closing()
        self.assertTrue(disp.is_closing)


if __name__ == "__main__":
    unittest.main()
