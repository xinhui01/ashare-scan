"""新浪财经历史 K 线源（通过 akshare 的 stock_zh_a_daily 接口）。

新浪对频率极敏感，本模块用独立节流阀保护，并接入全局主机冷却状态。
"""
from __future__ import annotations

import random
import threading
import time
from typing import Optional

import pandas as pd

from src.network.headers import USER_AGENT_POOL
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


def fetch_intraday_1min(stock_code: str, logger=None) -> "pd.DataFrame":
    """新浪 1 分钟分时：容错解析，替代脆弱的 ``ak.stock_zh_a_minute``。

    akshare 的 ``stock_zh_a_minute`` 在新浪返回非 JSONP（被限流时的 HTTP 456
    拦截页、空响应等）时会执行 ``data_text.split("=(")[1]`` 直接抛
    ``IndexError: list index out of range``，日志难懂且打断回退。

    本函数直连同一接口，自己做"找 ``[...]`` 段 + json.loads"的宽松解析：
    - 非 200 / 找不到 JSON 数组 → 记一条清晰日志并返回空表（让上层干净回退）；
    - 真正的网络异常（超时/断连）照常抛出，交给外层重试。

    返回列 ``day/open/high/low/close/volume``（不复权），可直接喂给
    ``normalize_source_frame``（按列名映射，``day`` → ``time``）。
    """
    import json

    import requests

    from stock_data import _use_bypass_proxy, _use_insecure_ssl

    symbol = market_prefixed_code(stock_code)
    url = "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/CN_MarketDataService.getKLineData"
    params = {"symbol": symbol, "scale": "1", "ma": "no", "datalen": "1970"}

    throttle()
    req_kw = {
        "url": url,
        "params": params,
        "timeout": (5, 15),
        "headers": {
            "User-Agent": random.choice(USER_AGENT_POOL),
            "Referer": "https://finance.sina.com.cn/",
        },
    }
    if _use_insecure_ssl():
        req_kw["verify"] = False
    with requests.Session() as session:
        if _use_bypass_proxy():
            session.trust_env = False
        resp = session.get(**req_kw)

    if resp.status_code != 200:
        if logger:
            logger(f"分时行情(新浪) {stock_code} 被限流/拒绝 (HTTP {resp.status_code})，跳过新浪")
        return pd.DataFrame()

    text = resp.text or ""
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end <= start:
        if logger:
            logger(f"分时行情(新浪) {stock_code} 返回非预期格式(无数据)，跳过新浪")
        return pd.DataFrame()
    try:
        data = json.loads(text[start : end + 1])
    except (ValueError, TypeError):
        if logger:
            logger(f"分时行情(新浪) {stock_code} JSON 解析失败，跳过新浪")
        return pd.DataFrame()
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)
