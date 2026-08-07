"""东方财富请求 URL 规范化 + 历史镜像健康度排序。

- ``request_mirror_urls``：多节点轮换已下线——82.push2 等编号节点与主域同属
  一个集群，被网络出口按 TLS 指纹拦截时一起死（2026-08-06/07 两次实测），
  轮换只会把熔断计数打得更快。现在仅把编号节点规范到无编号主域后返回
  单元素列表，保持旧调用方的 for 循环签名不变。
- ``prioritize_history_mirrors``：从候选列表里剔除冷却中的主机，按健康度截断。
"""
from __future__ import annotations

import re
import time
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

from src.network.host_health import on_cooldown


_NUMBERED_PUSH_HOST = re.compile(
    r"^\d+\.(push2his|push2delay|push2)(\.eastmoney\.com)$", re.IGNORECASE
)


def request_mirror_urls(url: str) -> List[str]:
    """返回单元素列表；编号 push 节点（82.push2 等）规范到无编号主域。"""
    raw = url.strip()
    p = urlparse(raw)
    match = _NUMBERED_PUSH_HOST.match((p.netloc or "").strip().lower())
    if not match:
        return [raw]
    host = match.group(1) + match.group(2)
    return [urlunparse(("https", host, p.path or "/", "", p.query or "", ""))]


def prioritize_history_mirrors(
    mirror_urls: List[str],
    preferred_mirror: Optional[str] = None,
    max_count: int = 3,
) -> List[str]:
    """剔除冷却中的镜像，按健康度截断。"""
    now = time.time()
    seen: set[str] = set()

    candidates: List[str] = []
    if preferred_mirror:
        candidates.append(preferred_mirror)
    candidates.extend(mirror_urls)

    healthy: List[str] = []
    cooling: List[str] = []
    for url in candidates:
        clean = str(url or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        if on_cooldown(clean, now):
            cooling.append(clean)
        else:
            healthy.append(clean)

    # 冷却中的镜像直接剔除，避免"明知不可用还继续打"。
    return healthy[:max_count]
