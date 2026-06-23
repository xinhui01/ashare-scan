"""Experimental finshare historical daily bars source."""
from __future__ import annotations

import os
import random
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import infer_market, normalize_history_frame


HOST = "finshare"
_REQUEST_LOCK = threading.Lock()
_IMPORT_LOCK = threading.Lock()
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
    norm = str(code or "").strip().zfill(6)
    return f"{norm}.{infer_market(norm).upper()}"


def _date_text(value: str) -> str:
    raw = str(value or "").strip().replace("/", "-").replace(".", "-")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _runtime_base_dir() -> Path:
    configured = str(os.environ.get("ASHARE_FINSHARE_RUNTIME_DIR") or "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "data" / "finshare_runtime"


@contextmanager
def _finshare_import_env():
    base = _runtime_base_dir()
    appdata = base / "AppData" / "Roaming"
    base.mkdir(parents=True, exist_ok=True)
    appdata.mkdir(parents=True, exist_ok=True)
    keys = ("USERPROFILE", "HOME", "APPDATA")
    old_values = {key: os.environ.get(key) for key in keys}
    os.environ["USERPROFILE"] = str(base)
    os.environ["HOME"] = str(base)
    os.environ["APPDATA"] = str(appdata)
    try:
        yield
    finally:
        for key, value in old_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _import_finshare_package():
    with _IMPORT_LOCK:
        with _finshare_import_env():
            import finshare as fs  # type: ignore

    return fs


def normalize_finshare_history_frame(df: "pd.DataFrame") -> "pd.DataFrame":
    """Normalize finshare K-line output to the shared history schema."""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out = out.rename(
        columns={
            "trade_date": "date",
            "open_price": "open",
            "close_price": "close",
            "high_price": "high",
            "low_price": "low",
            "vol": "volume",
            "turnover": "amount",
            "pct_chg": "change_pct",
            "change_percent": "change_pct",
            "change": "change_amount",
        }
    )
    return normalize_history_frame(out)


def fetch_hist_frame(stock_code_in: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """Fetch daily bars from finshare and normalize them into the shared history schema."""
    if on_cooldown(HOST):
        remain = cooldown_remaining(HOST)
        raise RuntimeError(f"finshare host on cooldown ({int(remain)}s remaining)")

    try:
        fs = _import_finshare_package()
    except Exception as exc:
        raise RuntimeError("finshare package is not installed") from exc

    try:
        throttle()
        raw = fs.get_historical_data(
            stock_code(stock_code_in),
            start=_date_text(start_date),
            end=_date_text(end_date),
            period="daily",
            adjust=None,
        )
        out = normalize_finshare_history_frame(raw)
        if out.empty:
            raise RuntimeError("finshare: empty normalized history")
        mark_ok(HOST)
        return out
    except Exception:
        mark_failed(HOST)
        raise
