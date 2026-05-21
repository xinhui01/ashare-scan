"""分时数据查询服务（intraday query chain）。

模块级函数（参数注入模式），覆盖 stock_filter.get_stock_intraday 的全部逻辑：

- resolve_intraday_prev_close: 从历史 K 线解析昨收
- get_stock_intraday: 公开 API，并行拉取分时 + 历史昨收

依赖：StockDataFetcher（fetcher 参数）+ 注入的 call_with_timeout_fn。
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def resolve_intraday_prev_close(
    history_df: Optional[pd.DataFrame],
    selected_trade_date: str,
) -> Optional[float]:
    if history_df is None or history_df.empty or "close" not in history_df.columns:
        return None

    df = history_df.copy()
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.strip()
    else:
        df["date"] = ""
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    if df.empty:
        return None

    target_date = str(selected_trade_date or "").strip()
    if target_date:
        previous_rows = df[df["date"] < target_date]
        if not previous_rows.empty:
            return float(previous_rows.iloc[-1]["close"])

    if len(df) >= 2:
        return float(df.iloc[-2]["close"])
    return float(df.iloc[-1]["close"])


def get_stock_intraday(
    stock_code: str,
    day_offset: int = 0,
    target_trade_date: str = "",
    *,
    fetcher,
    call_with_timeout_fn: Callable[..., Any],
) -> Dict[str, Any]:
    code = str(stock_code).strip().zfill(6)

    # ---- 并行获取：分时数据 + 历史(昨收)同时发起 ----
    intraday_payload = {}
    history_df = None

    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="intraday") as pool:
        fut_intraday = pool.submit(
            call_with_timeout_fn,
            lambda: fetcher.get_intraday_data(
                code, day_offset=day_offset,
                target_trade_date=target_trade_date, include_meta=True,
            ),
            12.0, {}, f"分时 {code}",
        )
        fut_history = pool.submit(
            call_with_timeout_fn,
            lambda: fetcher.get_history_data(code, days=20),
            6.0, None, f"分时昨收 {code}",
        )
        try:
            intraday_payload = fut_intraday.result() or {}
        except Exception as exc:
            logger.debug("分时数据获取失败 %s: %s", code, exc)
            intraday_payload = {}
        try:
            history_df = fut_history.result()
        except Exception as exc:
            logger.debug("历史数据获取失败 %s: %s", code, exc)
            history_df = None

    intraday_df = None
    selected_trade_date = ""
    available_trade_dates: List[str] = []
    applied_day_offset = 0
    auction_snapshot = None
    if isinstance(intraday_payload, dict):
        intraday_df = intraday_payload.get("intraday")
        selected_trade_date = str(intraday_payload.get("selected_trade_date") or "")
        available_trade_dates = [str(d) for d in (intraday_payload.get("available_trade_dates") or [])]
        raw_auction = intraday_payload.get("auction")
        if isinstance(raw_auction, dict):
            auction_snapshot = raw_auction
        try:
            applied_day_offset = int(intraday_payload.get("applied_day_offset") or 0)
        except (TypeError, ValueError):
            applied_day_offset = 0

    prev_close = resolve_intraday_prev_close(history_df, selected_trade_date)
    return {
        "code": code,
        "intraday": intraday_df,
        "prev_close": prev_close,
        "selected_trade_date": selected_trade_date,
        "available_trade_dates": available_trade_dates,
        "applied_day_offset": applied_day_offset,
        "auction": auction_snapshot,
    }
