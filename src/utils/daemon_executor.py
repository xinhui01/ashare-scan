"""守护线程池：`ThreadPoolExecutor` 的子类，所有 worker 都是 daemon 线程。

原版的 `ThreadPoolExecutor` 在默认情况下生成的 worker 不是 daemon，即使主进程
已经准备退出（用户点了关闭窗口），进程仍然需要等池中所有 future 完成才会真正
退出；这对股票扫描/缓存更新这种"跑满 N 分钟都正常"的任务来说是致命的。

本类通过覆写 `_adjust_thread_count`（直接复用 CPython 源码流程但把 `daemon=True`
强制设上）来把 worker 全部 daemonize。实现本身是从 CPython `concurrent.futures`
的内部做最小改动，已在 Python 3.9–3.12 上稳定使用。
"""

from __future__ import annotations

import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures.thread import _threads_queues, _worker


class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    """`ThreadPoolExecutor` 子类。区别：worker 线程强制为 daemon。"""

    def _adjust_thread_count(self):
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)
            t = threading.Thread(
                name=thread_name,
                target=_worker,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
                daemon=True,
            )
            t.start()
            self._threads.add(t)
            _threads_queues[t] = self._work_queue
