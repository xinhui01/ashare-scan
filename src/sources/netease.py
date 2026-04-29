"""网易财经历史 K 线源。

CSV 下载格式（GBK 编码），反爬较弱，是相对稳定的次级源。
URL: ``https://quotes.money.163.com/service/chddata.html``
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
_MIN_INTERVAL = 1.0


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
    """网易用 0+沪市代码 或 1+深市代码 的格式。"""
    c = str(code).strip().zfill(6)
    if c.startswith(("5", "6", "9")):
        return f"0{c}"
    return f"1{c}"


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """网易财经历史日线：CSV 格式下载，反爬较弱。"""
    import io

    import requests

    if on_cooldown("quotes.money.163.com"):
        remain = cooldown_remaining("quotes.money.163.com")
        raise RuntimeError(f"netease host on cooldown ({int(remain)}s remaining)")

    netease_code = stock_code(stock_code_in)
    url = "https://quotes.money.163.com/service/chddata.html"
    params = {
        "code": netease_code,
        "start": start_date,
        "end": end_date,
        "fields": "TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER",
    }

    last_error = None
    for attempt in range(3):
        try:
            throttle()
            resp = requests.get(
                url,
                params=params,
                timeout=(5, 15),
                headers={
                    "User-Agent": random.choice(USER_AGENT_POOL),
                    "Referer": "https://quotes.money.163.com/",
                },
            )
            if resp.status_code != 200:
                last_error = RuntimeError(f"netease HTTP {resp.status_code}")
                time.sleep(1.0 + random.uniform(0.5, 1.5))
                continue

            text = resp.content.decode("gbk", errors="replace")
            df = pd.read_csv(io.StringIO(text), dtype=str)
            if df.empty:
                last_error = RuntimeError("netease: empty CSV")
                continue

            col_map = {
                "日期": "date",
                "收盘价": "close",
                "最高价": "high",
                "最低价": "low",
                "开盘价": "open",
                "前收盘": "prev_close",
                "涨跌额": "change_amount",
                "涨跌幅": "change_pct",
                "换手率": "turnover_rate",
                "成交量": "volume",
                "成交金额": "amount",
            }
            df.columns = [c.strip().strip("'\"") for c in df.columns]
            df = df.rename(columns=col_map)

            for col in ("open", "close", "high", "low", "volume", "amount", "change_pct", "change_amount", "turnover_rate"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.strip(), errors="coerce")

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"].astype(str).str.strip(), errors="coerce").dt.date.astype(str)

            df = df.dropna(subset=["date", "close"])
            df = df[df["close"] > 0]

            mark_ok("quotes.money.163.com")
            return normalize_history_frame(df)
        except Exception as e:
            last_error = e
            time.sleep(1.5 * (attempt + 1) + random.uniform(0.3, 1.0))

    mark_failed("quotes.money.163.com")
    if last_error is not None:
        raise last_error
    return pd.DataFrame()
