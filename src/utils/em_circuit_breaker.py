"""东方财富请求熔断器。

`_gupiao_request_with_retry` 连续失败达到阈值时开启熔断，冷却期内所有东财相关
请求立即快速失败，避免无意义的超长等待和 IP 被进一步封禁。

对外暴露一个模块级的默认实例（用 `EMCircuitBreaker.instance()` 访问），以及
三个薄函数 `is_open / record_failure / record_success` —— 与此前 stock_data 中
散落的 `_eastmoney_circuit_breaker_*` 函数签名完全一致，便于 stock_data 做简单
的向后兼容转发。

测试场景可以通过 `reset()` 把状态清零，无需 monkeypatch 模块全局。
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from stock_logger import get_logger

logger = get_logger(__name__)


class EMCircuitBreaker:
    """Eastmoney 熔断器。线程安全，指数回退。

    触发条件：连续失败次数 >= `fail_threshold` 即开启熔断，冷却时间按"触发次数"
    指数放大（首次 `initial_cooldown_sec`，第 n 次 `initial_cooldown_sec * 2**(n-1)`，
    封顶 `max_cooldown_sec`）。
    """

    def __init__(
        self,
        *,
        fail_threshold: int = 3,
        initial_cooldown_sec: float = 30.0,
        max_cooldown_sec: float = 600.0,
        clock=None,
    ) -> None:
        self._lock = threading.Lock()
        self._fail_threshold = int(fail_threshold)
        self._initial_cooldown = float(initial_cooldown_sec)
        self._max_cooldown = float(max_cooldown_sec)
        self._clock = clock or time.time

        self._fail_count = 0
        self._open_until = 0.0
        self._consecutive_trips = 0

    # ---- 查询 ----
    def is_open(self) -> bool:
        with self._lock:
            return self._open_until > self._clock()

    def open_until(self) -> float:
        with self._lock:
            return self._open_until

    def consecutive_trips(self) -> int:
        with self._lock:
            return self._consecutive_trips

    def fail_count(self) -> int:
        with self._lock:
            return self._fail_count

    # ---- 突变 ----
    def record_failure(self) -> None:
        with self._lock:
            self._fail_count += 1
            if self._fail_count >= self._fail_threshold:
                self._consecutive_trips += 1
                cooldown = min(
                    self._initial_cooldown * (2 ** (self._consecutive_trips - 1)),
                    self._max_cooldown,
                )
                self._open_until = self._clock() + cooldown
                logger.warning(
                    "东方财富熔断器已开启：连续 %d 次失败，冷却 %.0fs（第 %d 次触发）",
                    self._fail_count, cooldown, self._consecutive_trips,
                )

    def record_success(self) -> None:
        with self._lock:
            self._fail_count = 0
            self._open_until = 0.0
            self._consecutive_trips = 0

    def reset(self) -> None:
        """测试专用：强制归零所有状态。"""
        self.record_success()

    # ---- 单例入口 ----
    _instance: "Optional[EMCircuitBreaker]" = None
    _instance_lock = threading.Lock()

    @classmethod
    def instance(cls) -> "EMCircuitBreaker":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance


# ---- 与旧 stock_data API 兼容的顶层函数 ----
def is_open() -> bool:
    return EMCircuitBreaker.instance().is_open()


def record_failure() -> None:
    EMCircuitBreaker.instance().record_failure()


def record_success() -> None:
    EMCircuitBreaker.instance().record_success()
