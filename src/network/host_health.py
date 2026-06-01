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
from contextlib import contextmanager
from typing import Dict, Iterator, List, Optional


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


# 软冷却阶梯：偶发超时 / 5xx 不该一次就把整源锁死 1 小时，按连续失败次数递增。
# 一旦 mark_ok（成功）会清空失败计数，阶梯随之复位。
_SOFT_COOLDOWN_LADDER = (60.0, 300.0, 1800.0)  # 1min → 5min → 30min


def mark_failed(url_or_host: str, *, hard: bool = False) -> None:
    """标记主机失败，进入冷却。

    - ``hard=False``（默认，给易抖动的回退源用）：走软阶梯——首次失败只冷却 60s，
      连续失败升到 5min / 30min，再往后才退化到长冷却。避免一次偶发超时就把整源
      锁死 1 小时、后续全被 skip。
    - ``hard=True``（给容易被封、需要保守的源用，如东方财富）：沿用原长冷却，
      base 的 1x / 2x / 3x 累乘，封顶 3 小时。
    """
    host = _normalize_host(url_or_host)
    if not host:
        return
    base_cooldown = cooldown_sec()
    with _HOST_HEALTH_LOCK:
        count = _HOST_FAIL_COUNT.get(host, 0) + 1
        _HOST_FAIL_COUNT[host] = count
        if hard:
            cooldown = min(base_cooldown * min(count, 3), 10800.0)
        elif count <= len(_SOFT_COOLDOWN_LADDER):
            cooldown = _SOFT_COOLDOWN_LADDER[count - 1]
        else:
            # 软阶梯走完仍持续失败 → 退化到长冷却，按超出次数累乘
            over = count - len(_SOFT_COOLDOWN_LADDER)
            cooldown = min(base_cooldown * min(over, 3), 10800.0)
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
            # 注意：不要在这里清 _HOST_FAIL_COUNT。失败计数是软/硬冷却阶梯的依据
            # （连续失败 → 冷却越来越长）。只有真正成功（mark_ok）才清零；若一到期
            # 就清零，死源每个冷却窗口都被当"第一次失败"(只冷 60s)放出来重试，永远
            # 升不上去 → 死源被每 60s 反复调用、失败累积。
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


# ---- 单主机在途并发闸 ----
# 备用历史源（同花顺 / 网易 / 搜狐 等）原本只有"请求间隔节流"，没有"在途并发上限"，
# 多线程扫描/评分时会有 N 个请求同时压到同一个慢源上，导致 Read timeout / 502，
# 再被 mark_failed 拉进 1 小时冷却 → 整源停摆。这里给每个 host 加一道并发闸。
_HOST_INFLIGHT_SEMAPHORES: Dict[str, threading.Semaphore] = {}
_HOST_INFLIGHT_LOCK = threading.Lock()


def _global_inflight_override() -> Optional[int]:
    """``ASHARE_SCAN_HOST_INFLIGHT_LIMIT`` 若设置则全局覆盖各源的默认在途上限。"""
    raw = os.environ.get("ASHARE_SCAN_HOST_INFLIGHT_LIMIT", "").strip()
    if not raw:
        return None
    try:
        return max(1, min(int(raw), 16))
    except ValueError:
        return None


def _host_inflight_semaphore(host: str, default_limit: int) -> threading.Semaphore:
    """按 host 复用一个信号量；上限优先取全局环境变量，否则用调用方给的默认值。"""
    with _HOST_INFLIGHT_LOCK:
        sem = _HOST_INFLIGHT_SEMAPHORES.get(host)
        if sem is None:
            override = _global_inflight_override()
            limit = override if override is not None else max(1, min(int(default_limit), 16))
            sem = threading.Semaphore(limit)
            _HOST_INFLIGHT_SEMAPHORES[host] = sem
        return sem


@contextmanager
def limit_host_inflight(url_or_host: str, default_limit: int = 2) -> Iterator[None]:
    """限制单个主机的并发在途请求数，避免回退源被并发打爆。

    ``default_limit`` 为该源默认在途上限；设置环境变量 ``ASHARE_SCAN_HOST_INFLIGHT_LIMIT``
    可全局覆盖所有源。host 解析失败时不做限制，直接放行。
    """
    host = _normalize_host(url_or_host)
    if not host:
        yield
        return
    sem = _host_inflight_semaphore(host, default_limit)
    sem.acquire()
    try:
        yield
    finally:
        sem.release()
