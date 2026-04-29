"""华尔街见闻 (WallstreetCN) 历史 K 线源。

接口：``https://api-ddc-wscn.awtmt.com/market/kline``
JSON 格式，国际 CDN 节点。
"""
from __future__ import annotations

import random
import threading
import time
from datetime import datetime as _dt

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
_MIN_INTERVAL = 0.5


def throttle() -> None:
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL + random.uniform(0.1, 0.4)
                return
        time.sleep(min(wait, 0.5))


def stock_code(code: str) -> str:
    """华尔街见闻用 000001.SZ / 600000.SS 格式。"""
    c = str(code).strip().zfill(6)
    market = "SS" if c.startswith(("5", "6", "9")) else "SZ"
    return f"{c}.{market}"


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """华尔街见闻历史日线 API：JSON 格式，国际 CDN 节点。"""
    import requests

    host = "api-ddc-wscn.awtmt.com"
    if on_cooldown(host):
        remain = cooldown_remaining(host)
        raise RuntimeError(f"wscn host on cooldown ({int(remain)}s remaining)")

    wscn_code = stock_code(stock_code_in)
    sd = start_date.replace("-", "")
    ed = end_date.replace("-", "")
    try:
        d1 = _dt.strptime(sd[:8], "%Y%m%d")
        d2 = _dt.strptime(ed[:8], "%Y%m%d")
        cal_days = (d2 - d1).days
    except Exception:
        cal_days = 120
    tick_count = max(30, int(cal_days * 0.75) + 10)

    url = f"https://{host}/market/kline"
    params = {
        "prod_code": wscn_code,
        "tick_count": str(tick_count),
        "period_type": "86400",
        "fields": "tick_at,open_px,close_px,high_px,low_px,turnover_volume,turnover_value",
    }

    last_error = None
    for attempt in range(3):
        try:
            throttle()
            resp = requests.get(
                url,
                params=params,
                timeout=(5, 12),
                verify=False,
                headers={
                    "User-Agent": random.choice(USER_AGENT_POOL),
                    "Referer": "https://wallstreetcn.com/",
                    "Origin": "https://wallstreetcn.com",
                },
            )
            if resp.status_code != 200:
                last_error = RuntimeError(f"wscn HTTP {resp.status_code}")
                time.sleep(1.0 + random.uniform(0.5, 1.0))
                continue

            data = resp.json()
            if data.get("code") != 20000:
                last_error = RuntimeError(f"wscn API error: {data.get('message', 'unknown')}")
                continue

            candle = data.get("data", {}).get("candle", {})
            stock_block = candle.get(wscn_code, {})
            lines = stock_block.get("lines", [])
            if not lines:
                last_error = RuntimeError("wscn: no kline data")
                continue

            # fields order: open_px, close_px, high_px, low_px, turnover_volume, turnover_value, tick_at
            rows = []
            for line in lines:
                if len(line) < 7:
                    continue
                ts = line[6]
                try:
                    dt = _dt.fromtimestamp(ts)
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception:
                    continue
                rows.append({
                    "date": date_str,
                    "open": line[0],
                    "close": line[1],
                    "high": line[2],
                    "low": line[3],
                    "volume": line[4],
                    "amount": line[5],
                })

            if not rows:
                last_error = RuntimeError("wscn: empty parsed result")
                continue

            df = pd.DataFrame(rows)
            for col in ("open", "close", "high", "low", "volume", "amount"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
            df = df.dropna(subset=["date", "close"])
            df = df[df["close"] > 0]

            sd_fmt = f"{sd[:4]}-{sd[4:6]}-{sd[6:8]}"
            ed_fmt = f"{ed[:4]}-{ed[4:6]}-{ed[6:8]}"
            df = df[(df["date"] >= sd_fmt) & (df["date"] <= ed_fmt)]

            mark_ok(host)
            return normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.3, 1.0))

    mark_failed(host)
    if last_error is not None:
        raise last_error
    return pd.DataFrame()
