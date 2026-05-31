"""TongDaXin historical daily bars via the optional xmtdx backend."""
from __future__ import annotations

import importlib.util
import random
import threading
import time
from typing import Any, Iterable

import pandas as pd

from src.network.host_health import (
    cooldown_remaining,
    mark_failed,
    mark_ok,
    on_cooldown,
)
from src.sources._common import infer_market, normalize_history_frame


HOST = "tdx"
_REQUEST_LOCK = threading.Lock()
_NEXT_REQUEST_AT = 0.0
_MIN_INTERVAL = 0.35
_BEST_HOST_LOCK = threading.Lock()
_BEST_HOST: str | None = None


def is_available() -> bool:
    """Return whether an installed TDX backend can be used."""
    return importlib.util.find_spec("xmtdx") is not None


def throttle() -> None:
    global _NEXT_REQUEST_AT
    while True:
        with _REQUEST_LOCK:
            now = time.time()
            wait = _NEXT_REQUEST_AT - now
            if wait <= 0:
                _NEXT_REQUEST_AT = now + _MIN_INTERVAL + random.uniform(0.1, 0.3)
                return
        time.sleep(min(wait, 0.5))


def _bar_value(bar: Any, *names: str) -> Any:
    for name in names:
        if isinstance(bar, dict):
            value = bar.get(name)
        else:
            value = getattr(bar, name, None)
        if value is not None and str(value).strip() != "":
            return value
    return None


def _bar_date_text(bar: Any) -> str:
    raw = _bar_value(bar, "date", "datetime", "time")
    if raw is not None:
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.notna(parsed):
            return str(parsed.date())
    year = _bar_value(bar, "year")
    month = _bar_value(bar, "month")
    day = _bar_value(bar, "day")
    if year is None or month is None or day is None:
        return ""
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except (TypeError, ValueError):
        return ""


def _date_text(value: str) -> str:
    raw = str(value or "").strip().replace("/", "-").replace(".", "-")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _bar_count(start_date: str, end_date: str) -> int:
    start = pd.to_datetime(_date_text(start_date), errors="coerce")
    end = pd.to_datetime(_date_text(end_date), errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return 160
    days = max(1, int((end - start).days) + 1)
    return min(800, max(60, int(days * 2.2) + 20))


def _tdx_market(Market: Any, code: str) -> Any:
    market_name = infer_market(str(code or "").zfill(6)).upper()
    market = getattr(Market, market_name, None)
    if market is None:
        raise RuntimeError(f"tdx backend does not support market {market_name}")
    return market


def _client_host(client: Any) -> str:
    return str(getattr(client, "host", None) or getattr(client, "_host", "") or "").strip()


def _best_host_client(TdxClient: Any) -> tuple[Any, str]:
    global _BEST_HOST
    with _BEST_HOST_LOCK:
        cached_host = _BEST_HOST
    if cached_host:
        return TdxClient(cached_host, timeout=10.0), cached_host

    try:
        client = TdxClient.from_best_host(timeout=10.0, ping_timeout=3.0)
    except TypeError:
        client = TdxClient.from_best_host()
    selected_host = _client_host(client)
    if selected_host:
        with _BEST_HOST_LOCK:
            _BEST_HOST = selected_host
    return client, selected_host


def _forget_best_host(host: str) -> None:
    global _BEST_HOST
    if not host:
        return
    with _BEST_HOST_LOCK:
        if _BEST_HOST == host:
            _BEST_HOST = None


def normalize_tdx_history_bars(bars: Iterable[Any]) -> "pd.DataFrame":
    rows = []
    for bar in bars or []:
        rows.append(
            {
                "date": _bar_date_text(bar),
                "open": _bar_value(bar, "open"),
                "close": _bar_value(bar, "close"),
                "high": _bar_value(bar, "high"),
                "low": _bar_value(bar, "low"),
                "volume": _bar_value(bar, "volume", "vol"),
                "amount": _bar_value(bar, "amount"),
            }
        )
    if not rows:
        return pd.DataFrame()
    return normalize_history_frame(pd.DataFrame(rows))


def fetch_hist_frame(stock_code: str, start_date: str, end_date: str) -> "pd.DataFrame":
    """Fetch daily bars from TDX and normalize them into the shared history schema."""
    if on_cooldown(HOST):
        remain = cooldown_remaining(HOST)
        raise RuntimeError(f"tdx host on cooldown ({int(remain)}s remaining)")
    if not is_available():
        raise RuntimeError("xmtdx package is not installed")

    try:
        from xmtdx import KlineCategory, Market, TdxClient  # type: ignore
    except Exception as exc:
        raise RuntimeError("xmtdx package is not available") from exc

    code = str(stock_code or "").strip().zfill(6)
    market = _tdx_market(Market, code)
    count = _bar_count(start_date, end_date)
    start_text = _date_text(start_date)
    end_text = _date_text(end_date)

    try:
        throttle()
        client_cm, selected_host = _best_host_client(TdxClient)
        with client_cm as client:
            bars = client.get_security_bars(market, code, KlineCategory.DAY, 0, count)
        out = normalize_tdx_history_bars(bars)
        if out.empty:
            return pd.DataFrame()
        out = out[(out["date"] >= start_text) & (out["date"] <= end_text)]
        if out.empty:
            return pd.DataFrame()
        mark_ok(HOST)
        return out.reset_index(drop=True)
    except Exception:
        _forget_best_host(locals().get("selected_host", ""))
        mark_failed(HOST)
        raise
