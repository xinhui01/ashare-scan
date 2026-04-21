"""后台线程日志消息的排队 + 定时抽取到 UI 的组件。

将 `stock_gui.StockMonitorApp` 中分散的 `_log_queue / _log_async / _drain_log_queue`
收敛到一个独立的类，便于：
1. 独立单测（不需要真实 Tk）
2. 明确边界：UI 消息写入仍然由宿主对象负责（因为要操作具体的 Text widget），
   但排队/抽取/调度的机制完全内聚到这里

典型用法：
    drainer = LogDrainer(
        dispatcher=self._ui,
        main_thread_id=threading.get_ident(),
        sink=self._log,          # 在主线程执行的"写一行日志"实现
        poll_interval_ms=100,
    )
    drainer.start()      # 在 __init__ 末尾启动
    # 后台线程：drainer.enqueue("...")
    # 主线程：drainer.enqueue("...")  也会被识别并直接 sink
    # on_close 时会被 UIDispatcher.mark_closing() 间接停掉
"""

from __future__ import annotations

import queue
import threading
from typing import Callable, Optional


class LogDrainer:
    def __init__(
        self,
        dispatcher,
        main_thread_id: int,
        sink: Callable[[str], None],
        poll_interval_ms: int = 100,
    ) -> None:
        self._ui = dispatcher
        self._main_thread_id = main_thread_id
        self._sink = sink
        self._poll_interval_ms = int(poll_interval_ms)
        self._queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()
        self._started = False

    def enqueue(self, message: str) -> None:
        """线程安全的日志写入入口。主线程直接 sink，其它线程进队列。"""
        if self._ui.is_closing:
            return
        if threading.get_ident() == self._main_thread_id:
            try:
                self._sink(message)
            except Exception:
                # sink 本体异常不应冒泡，以免让 UI 线程挂掉
                pass
            return
        self._queue.put(message)

    def drain_once(self) -> int:
        """把队列里当前已有的消息全部 sink 掉，返回处理条数。"""
        if self._ui.is_closing:
            return 0
        count = 0
        while True:
            try:
                msg = self._queue.get_nowait()
            except queue.Empty:
                break
            try:
                self._sink(msg)
            except Exception:
                pass
            count += 1
        return count

    def _tick(self) -> None:
        if self._ui.is_closing:
            return
        self.drain_once()
        # 通过 UIDispatcher 重新调度下一轮；关闭状态下会直接丢弃，循环自然停止
        self._ui.safe_after(self._poll_interval_ms, self._tick)

    def start(self) -> None:
        """启动定时抽取循环。重复调用是幂等的。"""
        if self._started:
            return
        self._started = True
        self._ui.safe_after(self._poll_interval_ms, self._tick)

    @property
    def pending_count(self) -> int:
        """队列中待抽取的消息数量（近似值，读取无锁）。"""
        return self._queue.qsize()
