"""BaoStock 历史日线源。

BaoStock 不需要 token，但需要登录会话。会话是**全局单例**：若每抓一只票都
login/logout，会往 stdout 刷一堆 `login success! / logout success!`、拖慢，
且并发时多个线程的 login/logout 会互相踩掉对方的会话。

这里改为**全局持久登录 + 串行访问**：首次用时登录一次、之后复用；只有查询失败
（疑似会话过期）才登出重置、下次重新登录；进程退出时兜底登出。
"""
from __future__ import annotations

import atexit
import os
import random
import socket
import threading
import time
from contextlib import contextmanager
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

# baostock 会话全局单例：串行化访问 + 持久登录，避免刷屏/并发互踩。
_SESSION_LOCK = threading.RLock()
_LOGGED_IN = False


def _bs_timeout() -> float:
    """baostock 没有 timeout 参数，用 socket 默认超时兜底，避免卡死十几分钟。"""
    try:
        return max(3.0, min(float(os.environ.get("ASHARE_BAOSTOCK_TIMEOUT_SEC", "15") or "15"), 60.0))
    except ValueError:
        return 15.0


@contextmanager
def _socket_timeout(seconds: float):
    """临时设进程级 socket 默认超时（baostock 串行访问，窗口很短）。

    其它源走 requests/显式 timeout，不受默认值影响；这里只是给 baostock 的
    无超时 socket 兜个底。
    """
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(old)


def _ensure_logged_in(bs) -> None:
    """惰性登录：已登录则直接复用，不重复 login（避免刷屏）。"""
    global _LOGGED_IN
    if _LOGGED_IN:
        return
    res = bs.login()
    if str(getattr(res, "error_code", "0")) != "0":
        raise RuntimeError(f"baostock login failed: {getattr(res, 'error_msg', '')}")
    _LOGGED_IN = True


def _logout_and_reset(bs) -> None:
    """登出并清登录态（仅在查询失败/进程退出时调用）。"""
    global _LOGGED_IN
    try:
        bs.logout()
    except Exception:
        pass
    _LOGGED_IN = False


@atexit.register
def _atexit_logout() -> None:
    global _LOGGED_IN
    if not _LOGGED_IN:
        return
    try:
        import baostock as bs  # type: ignore

        bs.logout()
    except Exception:
        pass
    _LOGGED_IN = False


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
    # 串行化 baostock 访问：会话是全局单例，并发查询会互相踩。
    with _SESSION_LOCK:
        try:
            # 所有网络 I/O（登录 + 查询 + 翻页）都罩在 socket 超时窗口内，
            # 否则 baostock 无超时会把 worker 卡死十几分钟，连"停止"都得干等。
            with _socket_timeout(_bs_timeout()):
                _ensure_logged_in(bs)
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
            # 失败可能是会话过期：登出重置，下次调用会重新登录。
            _logout_and_reset(bs)
            raise
