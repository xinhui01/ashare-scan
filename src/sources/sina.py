"""新浪财经历史 K 线源（通过 akshare 的 stock_zh_a_daily 接口）。

新浪对频率极敏感，本模块用独立节流阀保护，并接入全局主机冷却状态。
"""
from __future__ import annotations

import random
import threading
import time
from typing import Optional

import pandas as pd

from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import market_prefixed_code, normalize_history_frame


HISTORY_MIRRORS = [
    "https://finance.sina.com.cn",
    "https://hq.sinajs.cn",
]
_REQUEST_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0
_MIN_INTERVAL = 1.5  # 新浪对频率更敏感


def throttle() -> None:
    """新浪专用节流阀，确保请求间隔。"""
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL
                return
        time.sleep(min(wait, 0.5))


def fetch_hist_frame(stock_code: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """新浪历史日线：带 UA 随机化 + 独立节流 + 重试 + 全局主机健康管理。"""
    import akshare as ak

    if on_cooldown("finance.sina.com.cn"):
        remain = cooldown_remaining("finance.sina.com.cn")
        raise RuntimeError(f"sina host on cooldown ({int(remain)}s remaining)")

    symbol = market_prefixed_code(stock_code)
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            throttle()
            # sina 请求头已在 stock_data._apply_network_patches 的 Session.request 补丁中统一处理
            df = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust="",
            )
            mark_ok("finance.sina.com.cn")
            return normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.3, 1.0))

    mark_failed("finance.sina.com.cn")
    if last_error is not None:
        raise last_error
    return pd.DataFrame()
