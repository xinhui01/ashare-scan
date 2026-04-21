"""统一的取消令牌。

将散落在 GUI / filter 中的布尔停止标记整合为一个 `threading.Event` 包装，
保留 `should_stop`-callable 兼容接口，同时让后台代码可以 `wait(timeout)` 而不是
忙轮询。
"""

from __future__ import annotations

import threading
from typing import Callable, Optional


class CancelToken:
    __slots__ = ("_event", "_reason")

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: str = ""

    def cancel(self, reason: str = "") -> None:
        if reason and not self._reason:
            self._reason = reason
        self._event.set()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str:
        return self._reason

    def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise CancelledError(self._reason or "operation cancelled")

    def wait(self, timeout: float) -> bool:
        """Block up to `timeout` seconds, return True iff cancellation fired."""
        return self._event.wait(timeout)

    def as_should_stop(self) -> Callable[[], bool]:
        """向后兼容：返回 `() -> bool` 谓词。"""
        return self._event.is_set

    def reset(self) -> None:
        self._event.clear()
        self._reason = ""


class CancelledError(RuntimeError):
    """显式的取消异常；与标准库 concurrent.futures.CancelledError 区分。"""


def coerce_should_stop(
    token: Optional[CancelToken],
    should_stop: Optional[Callable[[], bool]],
) -> Optional[Callable[[], bool]]:
    """Combine an optional CancelToken with an optional should_stop callable."""
    if token is None and should_stop is None:
        return None
    if token is None:
        return should_stop
    if should_stop is None:
        return token.as_should_stop()
    return lambda: token.is_cancelled() or bool(should_stop())


class CancelTokenRegistry:
    """多个后台任务的取消令牌登记中心。

    用法：每个后台线程开工前 `issue()` 取一个 token；结束时 `retire(token)` 摘掉。
    关闭窗口 / 恢复数据库时调用 `broadcast_cancel(reason)`，一次性通知所有在登记
    的任务尽快退出。线程安全。
    """

    def __init__(self) -> None:
        self._tokens: "set[CancelToken]" = set()
        self._lock = threading.Lock()

    def issue(self, token: Optional[CancelToken] = None) -> CancelToken:
        tk_ = token if token is not None else CancelToken()
        with self._lock:
            self._tokens.add(tk_)
        return tk_

    def retire(self, token: CancelToken) -> None:
        with self._lock:
            self._tokens.discard(token)

    def broadcast_cancel(self, reason: str = "") -> None:
        with self._lock:
            snapshot = list(self._tokens)
        for tk_ in snapshot:
            tk_.cancel(reason)

    def active_count(self) -> int:
        with self._lock:
            return len(self._tokens)

    def clear(self) -> None:
        with self._lock:
            self._tokens.clear()
