"""线程 → Tk 主线程的安全派发组件。

把散落在 `StockMonitorApp` 里的 `_safe_after` / `_post_to_ui` / `_is_closing`
标记抽成独立组件，便于单独测试，也方便后续其他 GUI 模块复用。

关键点：Tkinter 的 `root.after(...)` 在窗口销毁后会抛 `TclError`。本组件统一：
1. 通过 `mark_closing()` 显式进入"关闭中"状态，之后所有 `safe_after/post` 直接丢弃
2. 调用 `root.after(...)` 前用 `winfo_exists()` 二次校验
3. 捕获 `TclError`，防止线程回调在关闭竞态里冒泡
"""

from __future__ import annotations

from typing import Callable, Optional, Protocol


class _RootLike(Protocol):
    def after(self, delay_ms: int, callback: Callable[[], None]) -> str: ...
    def winfo_exists(self) -> bool: ...


class UIDispatcher:
    """线程安全的 Tk 主线程派发器。

    Attributes:
        root: Tk root（任何实现了 after/winfo_exists 的对象即可，方便单测）
    """

    __slots__ = ("_root", "_closing")

    def __init__(self, root: _RootLike) -> None:
        self._root = root
        self._closing = False

    @property
    def is_closing(self) -> bool:
        return self._closing

    def mark_closing(self) -> None:
        """进入关闭流程。之后所有 safe_after/post 都不会再调度。"""
        self._closing = True

    def safe_after(self, delay_ms: int, callback: Callable[[], None]) -> Optional[str]:
        """窗口仍在时通过 root.after 调度 callback；否则丢弃。

        返回 Tk 的 after id（用于 after_cancel），关闭状态下返回 None。
        """
        if self._closing:
            return None
        # 捕获两类错误：winfo_exists 本身可能抛 TclError，after 也可能抛。
        try:
            if not self._root.winfo_exists():
                return None
            return self._root.after(delay_ms, callback)
        except Exception:
            return None

    def post(self, callback: Callable[[], None]) -> None:
        """0ms 推送 —— 后台线程把 UI 更新交给主线程的标准入口。"""
        self.safe_after(0, callback)
