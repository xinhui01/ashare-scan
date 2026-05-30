"""BaoStock 历史日线源。

BaoStock 不需要 token，但需要登录会话。这里按请求粒度登录/登出，避免
长时间 GUI 进程里残留不可控连接状态；外层更新缓存会做每源并发限制。
"""
from __future__ import annotations

import random
import threading
import time
from typing import List

import pandas as pd

from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import infer_market, normalize_history_frame


HOST = "baostock.com"
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
    c = str(code or "").strip().zfill(6)
    return f"{infer_market(c)}.{c}"


def _date_text(value: str) -> str:
    raw = str(value or "").strip().replace("/", "-").replace(".", "-")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def normalize_baostock_history_frame(df: "pd.DataFrame") -> "pd.DataFrame":
    """把 BaoStock 日K返回规整为项目统一 history schema。"""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "tradestatus" in out.columns:
        out = out[out["tradestatus"].astype(str).str.strip().replace("", "0") == "1"].copy()
    if out.empty:
        return pd.DataFrame()
    for col in ("open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pctChg"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col].astype(str).str.strip(), errors="coerce")
    if "preclose" in out.columns:
        out["change_amount"] = out["close"] - out["preclose"]
    if "pctChg" in out.columns:
        out["change_pct"] = out["pctChg"]
    if "turn" in out.columns:
        out["turnover_rate"] = out["turn"]
    if {"high", "low", "preclose"} <= set(out.columns):
        base = out["preclose"].where(out["preclose"].notna() & (out["preclose"] != 0), out["close"])
        out["amplitude"] = ((out["high"] - out["low"]) / base) * 100.0
    return normalize_history_frame(out)


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """BaoStock 历史日线。"""
    if on_cooldown(HOST):
        remain = cooldown_remaining(HOST)
        raise RuntimeError(f"baostock host on cooldown ({int(remain)}s remaining)")

    try:
        import baostock as bs  # type: ignore
    except Exception as exc:
        raise RuntimeError("baostock package is not installed") from exc

    symbol = stock_code(stock_code_in)
    fields = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "turn,tradestatus,pctChg"
    )
    throttle()
    logged_in = False
    try:
        login_result = bs.login()
        logged_in = True
        if str(getattr(login_result, "error_code", "0")) != "0":
            raise RuntimeError(f"baostock login failed: {getattr(login_result, 'error_msg', '')}")
        rs = bs.query_history_k_data_plus(
            symbol,
            fields,
            start_date=_date_text(start_date),
            end_date=_date_text(end_date),
            frequency="d",
            adjustflag="3",
        )
        if str(getattr(rs, "error_code", "0")) != "0":
            raise RuntimeError(f"baostock query failed: {getattr(rs, 'error_msg', '')}")
        rows: List[List[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            raise RuntimeError("baostock: empty history")
        df = pd.DataFrame(rows, columns=list(rs.fields))
        out = normalize_baostock_history_frame(df)
        if out.empty:
            raise RuntimeError("baostock: empty normalized history")
        mark_ok(HOST)
        return out
    except Exception:
        mark_failed(HOST)
        raise
    finally:
        if logged_in:
            try:
                bs.logout()
            except Exception:
                pass
