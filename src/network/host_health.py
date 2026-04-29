"""全局主机健康管理器。

所有数据源（eastmoney / tencent / sina 等）共用同一个健康状态池：
任何主机失败一次即进入冷却，冷却期间所有调用方自动跳过该主机。

冷却时长由 ``ASHARE_SCAN_HOST_COOLDOWN_SEC``（兼容旧名 ``ASHARE_SCAN_HISTORY_HOST_COOLDOWN_SEC``）控制，
默认 1 小时；连续失败时按 1x / 2x / 3x 累乘，封顶 3 小时。
"""
from __future__ import annotations

import os
import re
import threading
import time
from typing import Dict, List, Optional


_HOST_HEALTH: Dict[str, float] = {}  # host → cooldown_until timestamp
_HOST_HEALTH_LOCK = threading.Lock()
_HOST_FAIL_COUNT: Dict[str, int] = {}  # host → consecutive fail count


def _normalize_host(url_or_host: str) -> str:
    return re.sub(r"^https?://", "", str(url_or_host or "").strip()).split("/", 1)[0].lower()


def cooldown_sec() -> float:
    """全局主机冷却时间，默认 1 小时。可通过 ASHARE_SCAN_HOST_COOLDOWN_SEC 环境变量配置。"""
    raw = os.environ.get("ASHARE_SCAN_HOST_COOLDOWN_SEC", "").strip()
    if not raw:
        raw = os.environ.get("ASHARE_SCAN_HISTORY_HOST_COOLDOWN_SEC", "").strip()
    try:
        value = float(raw) if raw else 3600.0
    except ValueError:
        value = 3600.0
    return max(60.0, min(value, 7200.0))


def mark_failed(url_or_host: str) -> None:
    """标记主机失败，进入冷却。连续失败次数越多，冷却时间越长（封顶 3 小时）。"""
    host = _normalize_host(url_or_host)
    if not host:
        return
    base_cooldown = cooldown_sec()
    with _HOST_HEALTH_LOCK:
        count = _HOST_FAIL_COUNT.get(host, 0) + 1
        _HOST_FAIL_COUNT[host] = count
        multiplier = min(count, 3)
        cooldown = min(base_cooldown * multiplier, 10800.0)
        _HOST_HEALTH[host] = time.time() + cooldown


def mark_ok(url_or_host: str) -> None:
    """标记主机成功，清除冷却状态和失败计数。"""
    host = _normalize_host(url_or_host)
    if not host:
        return
    with _HOST_HEALTH_LOCK:
        _HOST_HEALTH.pop(host, None)
        _HOST_FAIL_COUNT.pop(host, None)


def on_cooldown(url_or_host: str, now: Optional[float] = None) -> bool:
    """检查主机是否正在冷却中。"""
    host = _normalize_host(url_or_host)
    if not host:
        return False
    if now is None:
        now = time.time()
    with _HOST_HEALTH_LOCK:
        cooldown_until = _HOST_HEALTH.get(host, 0.0)
        if cooldown_until <= now:
            _HOST_HEALTH.pop(host, None)
            _HOST_FAIL_COUNT.pop(host, None)
            return False
        return True


def cooldown_remaining(url_or_host: str) -> float:
    """返回主机剩余冷却秒数，0 表示不在冷却中。"""
    host = _normalize_host(url_or_host)
    if not host:
        return 0.0
    now = time.time()
    with _HOST_HEALTH_LOCK:
        cooldown_until = _HOST_HEALTH.get(host, 0.0)
        return max(0.0, cooldown_until - now)


def filter_healthy_urls(urls: List[str]) -> List[str]:
    """从 URL 列表中过滤掉正在冷却的主机。"""
    now = time.time()
    return [u for u in urls if not on_cooldown(u, now)]
