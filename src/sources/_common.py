"""数据源共享工具：市场前缀、历史 K 线 frame 标准化、列名匹配。

所有 ``src.sources.*`` 子模块依赖这里的纯函数。本模块只依赖 pandas，
不依赖 stock_data，避免循环 import。
"""
from __future__ import annotations

from typing import List, Optional

import pandas as pd


def first_existing_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    """从 ``columns`` 里找第一个匹配 ``candidates`` 的列名（按候选顺序优先）。"""
    normalized = {str(col).strip(): col for col in columns}
    for name in candidates:
        key = str(name).strip()
        if key in normalized:
            return normalized[key]
    return None


def infer_market(code: str) -> str:
    c = str(code).strip().zfill(6)
    if c.startswith(("4", "8")):
        return "bj"
    return "sh" if c.startswith(("5", "6", "9")) else "sz"


def market_prefixed_code(code: str) -> str:
    norm = str(code or "").strip().zfill(6)
    return f"{infer_market(norm)}{norm}"


def normalize_history_frame(df: "pd.DataFrame") -> "pd.DataFrame":
    """把任意来源的历史 K 线 DataFrame 转成统一 schema：
    ``date / open / close / high / low / volume / amount / amplitude / change_pct / change_amount / turnover_rate``。
    """
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    rename_map = {
        "日期": "date",
        "时间": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "change_pct",
        "涨跌额": "change_amount",
        "换手率": "turnover_rate",
    }
    out = out.rename(columns=rename_map)
    if "date" not in out.columns:
        return pd.DataFrame()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)
    for col in ("open", "close", "high", "low", "volume", "amount", "amplitude", "change_pct", "change_amount", "turnover_rate"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "close" not in out.columns:
        return pd.DataFrame()
    close_series = pd.to_numeric(out["close"], errors="coerce")
    prev_close = close_series.shift(1)
    if "change_amount" not in out.columns:
        out["change_amount"] = close_series - prev_close
    if "change_pct" not in out.columns:
        out["change_pct"] = ((close_series / prev_close) - 1.0) * 100.0
    if "volume" not in out.columns and "amount" in out.columns:
        out["volume"] = pd.to_numeric(out["amount"], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = pd.Series([None] * len(out), dtype="float64")
    if "amplitude" not in out.columns and {"high", "low", "close"} <= set(out.columns):
        base_close = prev_close.where(prev_close.notna() & (prev_close != 0), close_series)
        out["amplitude"] = ((pd.to_numeric(out["high"], errors="coerce") - pd.to_numeric(out["low"], errors="coerce")) / base_close) * 100.0
    if "turnover_rate" not in out.columns:
        out["turnover_rate"] = pd.Series([None] * len(out), dtype="float64")
    keep_cols = [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "change_pct",
        "change_amount",
        "turnover_rate",
    ]
    return out[keep_cols].dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
