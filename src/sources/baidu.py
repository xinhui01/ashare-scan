"""百度股市通历史 K 线源。

接口：``https://finance.pae.baidu.com/vapi/v1/getquotation``
返回结构层级较深，需要按 keys 探测多种字段路径。
"""
from __future__ import annotations

import random
import threading
import time

import pandas as pd

from src.network.headers import USER_AGENT_POOL
from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import normalize_history_frame


_REQUEST_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0
_MIN_INTERVAL = 1.2


def throttle() -> None:
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL + random.uniform(0.2, 0.8)
                return
        time.sleep(min(wait, 0.5))


def stock_code(code: str) -> str:
    """百度用 ab.sz000001 或 ab.sh600000 的格式。"""
    c = str(code).strip().zfill(6)
    market = "sh" if c.startswith(("5", "6", "9")) else "sz"
    return f"ab.{market}{c}"


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """百度股市通历史日线 API。"""
    import requests

    if on_cooldown("gushitong.baidu.com"):
        remain = cooldown_remaining("gushitong.baidu.com")
        raise RuntimeError(f"baidu host on cooldown ({int(remain)}s remaining)")

    baidu_code = stock_code(stock_code_in)
    url = "https://finance.pae.baidu.com/vapi/v1/getquotation"
    params = {
        "srcid": "5353",
        "pointType": "string",
        "group": "quotation_kline_ab",
        "query": baidu_code,
        "code": baidu_code,
        "market_type": "ab",
        "newFormat": "1",
        "is_498": "1",
        "ktype": "day",
        "finClientType": "pc",
    }

    last_error = None
    for attempt in range(3):
        try:
            throttle()
            resp = requests.get(
                url,
                params=params,
                timeout=(5, 12),
                headers={
                    "User-Agent": random.choice(USER_AGENT_POOL),
                    "Referer": "https://gushitong.baidu.com/",
                    "Accept": "application/json",
                },
            )
            if resp.status_code != 200:
                last_error = RuntimeError(f"baidu HTTP {resp.status_code}")
                time.sleep(1.0 + random.uniform(0.5, 1.5))
                continue

            data = resp.json()
            result = data.get("Result") or {}
            content_list = result.get("newMarketData") or result.get("priceinfo") or {}
            if isinstance(content_list, dict):
                content_list = content_list.get("marketData") or content_list.get("content") or {}
            if isinstance(content_list, dict):
                content_list = content_list.get("marketData") or ""

            kline_data = None
            if isinstance(result, dict):
                for key in ("newMarketData", "priceinfo"):
                    block = result.get(key)
                    if isinstance(block, dict):
                        md = block.get("marketData", "")
                        if isinstance(md, str) and md.strip():
                            kline_data = md
                            break
                        keys_data = block.get("keys", [])
                        if keys_data:
                            kline_data = block
                            break

            if not kline_data:
                last_error = RuntimeError("baidu: no kline data found")
                time.sleep(1.0)
                continue

            if isinstance(kline_data, str):
                lines = kline_data.strip().split("\n")
                rows = []
                for line in lines:
                    parts = line.split(",")
                    if len(parts) >= 7:
                        rows.append({
                            "date": parts[0].strip(),
                            "open": parts[1].strip(),
                            "close": parts[2].strip(),
                            "high": parts[3].strip(),
                            "low": parts[4].strip(),
                            "volume": parts[5].strip(),
                            "amount": parts[6].strip(),
                        })
                df = pd.DataFrame(rows)
            else:
                last_error = RuntimeError("baidu: unexpected data format")
                continue

            if df.empty:
                last_error = RuntimeError("baidu: empty result")
                continue

            for col in ("open", "close", "high", "low", "volume", "amount"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
            df = df.dropna(subset=["date", "close"])

            mark_ok("gushitong.baidu.com")
            return normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.3, 1.0))

    mark_failed("gushitong.baidu.com")
    if last_error is not None:
        raise last_error
    return pd.DataFrame()
