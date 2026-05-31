"""Helpers for deriving an auction snapshot from intraday source frames."""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from src.sources._common import first_existing_column


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        out = float(text)
    except (TypeError, ValueError):
        return None
    return out if pd.notna(out) else None


def _row_float(row: "pd.Series", *candidates: str) -> Optional[float]:
    col = first_existing_column([str(c) for c in row.index.tolist()], list(candidates))
    if col is None:
        return None
    value = _safe_float(row.get(col))
    return value if value is not None and value > 0 else None


def snapshot_from_intraday_frame(
    frame: "pd.DataFrame",
    *,
    stock_code: str = "",
    source: str = "",
) -> Optional[Dict[str, Any]]:
    """Parse the real 09:25 row from an intraday frame as an auction snapshot.

    This intentionally requires an exact 09:25 row. A 09:30 row is the regular
    open, not the call-auction match price, so callers should keep it separate.
    """
    if frame is None or getattr(frame, "empty", True):
        return None

    columns = [str(col) for col in frame.columns.tolist()]
    time_col = first_existing_column(columns, ["time", "day", "datetime", "日期时间", "时间"])
    if time_col is None:
        return None

    work = frame.copy()
    work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
    work = work.dropna(subset=[time_col]).sort_values(time_col).reset_index(drop=True)
    if work.empty:
        return None

    rows = work[work[time_col].dt.strftime("%H:%M") == "09:25"].reset_index(drop=True)
    if rows.empty:
        return None

    row = rows.iloc[-1]
    price = (
        _row_float(row, "close", "收盘", "最新价")
        or _row_float(row, "open", "开盘")
        or _row_float(row, "avg_price", "均价")
        or _row_float(row, "high", "最高")
        or _row_float(row, "low", "最低")
    )
    if price is None:
        return None

    ts = row[time_col]
    return {
        "trade_date": ts.date().isoformat(),
        "time": ts,
        "price": price,
        "open": _row_float(row, "open", "开盘"),
        "high": _row_float(row, "high", "最高"),
        "low": _row_float(row, "low", "最低"),
        "avg_price": _row_float(row, "avg_price", "均价"),
        "volume": _row_float(row, "volume", "成交量"),
        "amount": _row_float(row, "amount", "成交额", "money"),
        "source": str(source or "").strip(),
        "code": str(stock_code or "").strip().zfill(6) if stock_code else "",
    }
