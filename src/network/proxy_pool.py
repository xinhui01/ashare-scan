"""可选免费代理池。

启用方式：环境变量 ``ASHARE_SCAN_USE_PROXY_POOL=1`` 或项目根目录创建 ``USE_PROXY_POOL`` 文件。

代理池会从多个免费源拉取代理列表，随机抽样验证后轮换使用，降低被封 IP 的风险。
模块自包含——不依赖 stock_data 任何内部 helper。
"""
from __future__ import annotations

import os
import random
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional


_PROXY_POOL_LOCK = threading.Lock()
_PROXY_POOL: List[str] = []
_PROXY_POOL_REFRESHED_AT: float = 0.0
_PROXY_POOL_REFRESH_INTERVAL = 300.0
_PROXY_BLACKLIST: Dict[str, float] = {}

# 用于探测代理可用性，独立于其它源的 UA 池
_VALIDATION_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def _project_root() -> Path:
    # 项目根 = 仓库根。本文件在 src/network/，向上两级。
    return Path(__file__).resolve().parents[2]


def use_proxy_pool() -> bool:
    if os.environ.get("ASHARE_SCAN_USE_PROXY_POOL", "").strip().lower() in ("1", "true", "yes"):
        return True
    root = _project_root()
    return (root / "USE_PROXY_POOL").is_file() or (root / ".ashare_scan_proxy_pool").is_file()


def fetch_free_proxies(logger: Optional[Callable[[str], None]] = None) -> List[str]:
    """从多个免费代理源获取 HTTPS 代理列表。"""
    import requests
    proxies: List[str] = []
    sources = [
        ("https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=CN&ssl=yes&anonymity=all", "proxyscrape"),
        ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "github-speedx"),
        ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "github-clarketm"),
    ]
    for url, name in sources:
        try:
            resp = requests.get(url, timeout=8, headers={"User-Agent": random.choice(_VALIDATION_USER_AGENTS)})
            if resp.status_code == 200:
                lines = resp.text.strip().splitlines()
                for line in lines:
                    addr = line.strip()
                    if addr and ":" in addr and not addr.startswith("#"):
                        proxies.append(f"http://{addr}")
                if logger and lines:
                    logger(f"代理池: 从 {name} 获取 {len(lines)} 条")
        except Exception as e:
            if logger:
                logger(f"代理池: {name} 获取失败: {e}")
    return list(dict.fromkeys(proxies))


def validate_proxy(proxy: str, timeout: float = 5.0) -> bool:
    """快速验证代理是否可用（用百度做测试目标）。"""
    import requests
    try:
        resp = requests.get(
            "https://www.baidu.com",
            proxies={"http": proxy, "https": proxy},
            timeout=timeout,
            headers={"User-Agent": random.choice(_VALIDATION_USER_AGENTS)},
        )
        return resp.status_code == 200
    except Exception:
        return False


def refresh_proxy_pool(logger: Optional[Callable[[str], None]] = None) -> None:
    """刷新代理池：获取 → 随机抽样验证 → 缓存。"""
    global _PROXY_POOL, _PROXY_POOL_REFRESHED_AT
    raw = fetch_free_proxies(logger)
    if not raw:
        return
    sample = random.sample(raw, min(20, len(raw)))
    valid: List[str] = []
    for proxy in sample:
        if validate_proxy(proxy, timeout=4.0):
            valid.append(proxy)
            if len(valid) >= 8:
                break
    validated_set = set(valid)
    remaining = [p for p in raw if p not in validated_set]
    with _PROXY_POOL_LOCK:
        _PROXY_POOL = valid + remaining[:50]
        _PROXY_POOL_REFRESHED_AT = time.time()
    if logger:
        logger(f"代理池: 刷新完成，验证可用 {len(valid)} 个，总计 {len(_PROXY_POOL)} 个")


def get_proxy() -> Optional[str]:
    """获取一个可用代理地址。如果代理池为空或未启用，返回 None。"""
    if not use_proxy_pool():
        return None
    now = time.time()
    with _PROXY_POOL_LOCK:
        if now - _PROXY_POOL_REFRESHED_AT > _PROXY_POOL_REFRESH_INTERVAL:
            threading.Thread(target=refresh_proxy_pool, daemon=True).start()
        pool = [p for p in _PROXY_POOL if _PROXY_BLACKLIST.get(p, 0) <= now]
        if not pool:
            return None
        return random.choice(pool)


def blacklist_proxy(proxy: str) -> None:
    """将失败的代理加入黑名单 60 秒。"""
    with _PROXY_POOL_LOCK:
        _PROXY_BLACKLIST[proxy] = time.time() + 60.0
