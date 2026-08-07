"""统一的同步重试原语。

此前 stock_data._retry_ak_call（2 次、仅瞬时网络错误、1s 线性退避）与
concept_index._retry_call（3 次、任意异常、0.5s 线性退避）是两套独立实现；
现合并为一个实现，两处保留薄封装、各自语义不变。
"""
from __future__ import annotations

import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def retry_call(
    fn: Callable[..., T],
    *args,
    max_attempts: int = 2,
    base_delay: float = 1.0,
    should_retry: Optional[Callable[[BaseException], bool]] = None,
    **kwargs,
) -> T:
    """线性退避重试：第 i 次失败后 sleep base_delay*(i+1) 再试。

    should_retry 返回 False 的异常立刻抛出（缺省任何异常都重试）；
    最后一次尝试的异常原样抛出。
    """
    for attempt in range(max_attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt < max_attempts - 1 and (should_retry is None or should_retry(exc)):
                time.sleep(base_delay * (attempt + 1))
                continue
            raise
    raise RuntimeError("retry_call: max_attempts 必须 >= 1")
