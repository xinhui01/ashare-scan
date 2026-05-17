"""同花顺 (THS / 10jqka) 数据源。

接口：``https://d.10jqka.com.cn/v6/line/{ths_code}/01/{year}.js``
JSONP 格式，按年请求后合并筛选。
"""
from __future__ import annotations

import json as _json
import random
import threading
import time

import akshare as ak
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
_MIN_INTERVAL = 0.6


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
    """同花顺用 hs_000001 格式。"""
    c = str(code).strip().zfill(6)
    return f"hs_{c}"


def fetch_fund_flow_frame(stock_code_in: str) -> "pd.DataFrame":
    """同花顺个股资金流补位。

    当前优先复用 akshare 的同花顺资金流榜单接口，从“即时”榜单中过滤目标股票。
    该源通常只提供最新一笔聚合数据，因此作为东方财富失败时的兜底返回单行结果。
    """
    code = str(stock_code_in or "").strip().zfill(6)
    if not code:
        return pd.DataFrame()

    df = ak.stock_fund_flow_individual(symbol="即时")
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    if "股票代码" not in out.columns:
        return pd.DataFrame()

    out["股票代码"] = out["股票代码"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    out = out[out["股票代码"] == code].copy()
    if out.empty:
        return pd.DataFrame()

    today_text = pd.Timestamp.now().strftime("%Y-%m-%d")
    out["日期"] = today_text

    # 同花顺即时榜单只给出汇总净额，没有东财那样的主力/大单/超大单拆分；
    # 这里保留可用列，并把“净额”映射到统一层最常用的大单净额字段以便界面有值可展示。
    if "净额" in out.columns and "大单净额" not in out.columns:
        out["大单净额"] = out["净额"]
    if "净额" in out.columns and "主力净额" not in out.columns:
        out["主力净额"] = out["净额"]
    if "最新价" in out.columns and "收盘价" not in out.columns:
        out["收盘价"] = out["最新价"]

    return out.reset_index(drop=True)


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """同花顺 CDN 历史日线：JSONP 格式，按年请求后合并筛选。"""
    import requests

    host = "d.10jqka.com.cn"
    if on_cooldown(host):
        remain = cooldown_remaining(host)
        raise RuntimeError(f"ths host on cooldown ({int(remain)}s remaining)")

    ths_code = stock_code(stock_code_in)
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years = list(range(start_year, end_year + 1))

    all_rows = []
    last_error = None
    for year in years:
        for attempt in range(2):
            try:
                throttle()
                url = f"https://{host}/v6/line/{ths_code}/01/{year}.js"
                resp = requests.get(
                    url,
                    timeout=(5, 12),
                    verify=False,
                    headers={
                        "User-Agent": random.choice(USER_AGENT_POOL),
                        "Referer": "https://www.10jqka.com.cn/",
                    },
                )
                if resp.status_code != 200:
                    last_error = RuntimeError(f"ths HTTP {resp.status_code}")
                    time.sleep(0.5 + random.uniform(0.3, 0.8))
                    continue

                text = resp.text
                lp = text.find("(")
                rp = text.rfind(")")
                if lp < 0 or rp <= lp:
                    last_error = RuntimeError("ths: invalid JSONP response")
                    continue
                data = _json.loads(text[lp + 1 : rp])
                raw = data.get("data", "")
                if not raw:
                    last_error = RuntimeError("ths: empty data field")
                    continue

                # 格式: date,open,high,low,close,volume,amount,turnover_rate,,,flag;...
                for line in raw.split(";"):
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(",")
                    if len(parts) < 7:
                        continue
                    all_rows.append({
                        "date": parts[0],
                        "open": parts[1],
                        "high": parts[2],
                        "low": parts[3],
                        "close": parts[4],
                        "volume": parts[5],
                        "amount": parts[6],
                        "turnover_rate": parts[7] if len(parts) > 7 and parts[7] else None,
                    })
                break
            except Exception as e:
                last_error = e
                time.sleep(1.0 + random.uniform(0.3, 0.8))

    if not all_rows:
        mark_failed(host)
        if last_error is not None:
            raise last_error
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    for col in ("open", "close", "high", "low", "volume", "amount", "turnover_rate"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce").dt.date.astype(str)
    df = df.dropna(subset=["date", "close"])
    df = df[df["close"] > 0]

    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
    df = df[(df["date"] >= sd) & (df["date"] <= ed)]

    mark_ok(host)
    return normalize_history_frame(df)
