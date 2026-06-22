"""Relative strength scoring against Shanghai/Shenzhen index history."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pandas as pd

import stock_store

Fetcher = Callable[[str, str, str], Optional[pd.DataFrame]]


INDEX_NAMES = {
    "sh000001": "上证",
    "sz399001": "深成指",
}


def _normalize_index_symbol(symbol: Any) -> str:
    text = str(symbol or "").strip().lower()
    if text in {"sh000001", "000001.sh", "1.000001"}:
        return "sh000001"
    if text in {"sz399001", "399001.sz", "0.399001"}:
        return "sz399001"
    return text


def _normalize_stock_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return digits.zfill(6) if digits else ""


def benchmark_symbol_for_stock(code: Any) -> str:
    """Use Shanghai index for SH-listed stocks, Shenzhen index otherwise."""
    c = _normalize_stock_code(code)
    if c.startswith(("60", "68", "90")):
        return "sh000001"
    return "sz399001"


def _date_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = text.replace("-", "").replace("/", "")
    return digits if len(digits) == 8 and digits.isdigit() else ""


def _date_dash(value: Any) -> str:
    key = _date_key(value)
    if not key:
        return ""
    return f"{key[:4]}-{key[4:6]}-{key[6:]}"


def _start_date_for(target_key: str, calendar_days: int = 240) -> str:
    try:
        end = datetime.strptime(target_key, "%Y%m%d")
    except ValueError:
        return ""
    return (end - timedelta(days=calendar_days)).strftime("%Y%m%d")


def _normalize_history(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or getattr(df, "empty", True):
        return None
    if "date" not in df.columns or "close" not in df.columns:
        return None
    work = df[["date", "close"]].copy()
    work["date_key"] = work["date"].map(_date_key)
    work["date"] = work["date_key"].map(_date_dash)
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work.dropna(subset=["close"])
    work = work[work["date_key"].astype(str).str.len() == 8]
    if work.empty:
        return None
    work = work.drop_duplicates(subset=["date_key"], keep="last")
    return work.sort_values("date_key").reset_index(drop=True)


def _cache_key(symbol: str) -> str:
    return f"index_history_{symbol}"


def _cache_to_history(payload: Any) -> Tuple[Optional[pd.DataFrame], str, str]:
    if not isinstance(payload, dict):
        return None, "", ""
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return None, "", ""
    df = _normalize_history(pd.DataFrame(rows))
    return df, str(payload.get("source") or "cache"), str(payload.get("fetched_at") or "")


def _save_cache(symbol: str, df: pd.DataFrame, source: str) -> None:
    rows = [
        {"date": str(row["date"]), "close": float(row["close"])}
        for _, row in df.iterrows()
    ]
    payload = {
        "symbol": symbol,
        "source": source,
        "latest_trade_date": str(df.iloc[-1]["date"]),
        "row_count": len(rows),
        "rows": rows,
        "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    stock_store.save_app_config(_cache_key(symbol), payload)


def _history_covers(df: Optional[pd.DataFrame], target_key: str) -> bool:
    if df is None or df.empty or not target_key:
        return False
    return target_key in set(df["date_key"].astype(str))


def _fetch_index_daily_em(symbol: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    import stock_data  # noqa: F401 - applies project network patches before akshare use
    import akshare as ak

    return ak.stock_zh_index_daily_em(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
    )


def _fetch_index_daily_sina(symbol: str, _start_date: str, _end_date: str) -> Optional[pd.DataFrame]:
    import stock_data  # noqa: F401
    import akshare as ak

    return ak.stock_zh_index_daily(symbol=symbol)


def _fetch_index_daily_tx(symbol: str, _start_date: str, _end_date: str) -> Optional[pd.DataFrame]:
    import stock_data  # noqa: F401
    import akshare as ak

    return ak.stock_zh_index_daily_tx(symbol=symbol)


def _default_fetchers() -> List[Tuple[str, Fetcher]]:
    return [
        ("eastmoney", _fetch_index_daily_em),
        ("sina", _fetch_index_daily_sina),
        ("tencent", _fetch_index_daily_tx),
    ]


def load_index_history(
    symbol: str,
    *,
    target_date: str,
    fetchers: Optional[Iterable[Tuple[str, Fetcher]]] = None,
    force_refresh: bool = False,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Load index history covering target_date, fetching via fallback sources if needed."""
    normalized_symbol = _normalize_index_symbol(symbol)
    target_key = _date_key(target_date)
    if normalized_symbol not in INDEX_NAMES:
        return {
            "ok": False,
            "symbol": normalized_symbol,
            "history": None,
            "source": "",
            "warning": f"未知指数代码: {symbol}",
        }
    if not target_key:
        return {
            "ok": False,
            "symbol": normalized_symbol,
            "history": None,
            "source": "",
            "warning": f"指数目标日期无效: {target_date}",
        }

    cached_df = None
    cached_source = ""
    if not force_refresh:
        cached_df, cached_source, _fetched_at = _cache_to_history(
            stock_store.load_app_config(_cache_key(normalized_symbol), default=None)
        )
        if _history_covers(cached_df, target_key):
            return {
                "ok": True,
                "symbol": normalized_symbol,
                "history": cached_df,
                "source": cached_source or "cache",
                "warning": "",
            }

    start_key = _start_date_for(target_key)
    errors: List[str] = []
    latest_seen = ""
    latest_source = ""
    latest_df: Optional[pd.DataFrame] = None
    for source, fetcher in list(fetchers or _default_fetchers()):
        try:
            df = _normalize_history(fetcher(normalized_symbol, start_key, target_key))
        except Exception as exc:
            errors.append(f"{source}: {exc}")
            if log_fn:
                log_fn(f"指数历史 {INDEX_NAMES[normalized_symbol]}({source}) 拉取失败: {exc}")
            continue
        if df is None or df.empty:
            errors.append(f"{source}: empty")
            continue
        df = df[df["date_key"] <= target_key].reset_index(drop=True)
        if df.empty:
            errors.append(f"{source}: no rows before target")
            continue
        latest_seen = str(df.iloc[-1]["date_key"])
        latest_source = source
        latest_df = df
        try:
            _save_cache(normalized_symbol, df, source)
        except Exception as exc:
            if log_fn:
                log_fn(f"指数历史 {INDEX_NAMES[normalized_symbol]} 缓存失败: {exc}")
        if _history_covers(df, target_key):
            return {
                "ok": True,
                "symbol": normalized_symbol,
                "history": df,
                "source": source,
                "warning": "",
            }
        errors.append(f"{source}: latest={latest_seen}")

    if latest_df is not None:
        latest_dash = _date_dash(latest_seen)
        target_dash = _date_dash(target_key)
        return {
            "ok": False,
            "symbol": normalized_symbol,
            "history": None,
            "source": latest_source,
            "warning": (
                f"{INDEX_NAMES[normalized_symbol]}指数历史未覆盖目标日{target_dash}"
                f"（最新{latest_dash}），强弱因子已跳过"
            ),
        }

    return {
        "ok": False,
        "symbol": normalized_symbol,
        "history": None,
        "source": "",
        "warning": (
            f"{INDEX_NAMES[normalized_symbol]}指数历史拉取失败，强弱因子已跳过: "
            + "；".join(errors[:3])
        ),
    }


def build_relative_strength_context(
    trade_date: str,
    *,
    fetchers: Optional[Iterable[Tuple[str, Fetcher]]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """Fetch both benchmark histories and return fields mergeable into compare_context."""
    histories: Dict[str, pd.DataFrame] = {}
    meta: Dict[str, Dict[str, Any]] = {}
    warnings: List[str] = []
    for symbol in ("sh000001", "sz399001"):
        result = load_index_history(
            symbol,
            target_date=trade_date,
            fetchers=fetchers,
            log_fn=log_fn,
        )
        meta[symbol] = {
            "ok": bool(result.get("ok")),
            "source": result.get("source") or "",
            "warning": result.get("warning") or "",
        }
        if result.get("ok") and result.get("history") is not None:
            histories[symbol] = result["history"]
        elif result.get("warning"):
            warnings.append(str(result["warning"]))
    return {
        "relative_strength_index_history": histories,
        "relative_strength_index_meta": meta,
        "relative_strength_warnings": warnings,
    }


def _window_return(values: pd.Series, window: int) -> Optional[float]:
    if len(values) <= window:
        return None
    prev = values.iloc[-window - 1]
    latest = values.iloc[-1]
    if pd.isna(prev) or pd.isna(latest) or float(prev) <= 0:
        return None
    return (float(latest) / float(prev) - 1.0) * 100


def _category_weight(category: str, boards: int = 0) -> float:
    cat = str(category or "").strip().lower()
    if cat in {"cont", "continuation"}:
        return 0.75 if boards >= 4 else 1.0
    if cat in {"trend"}:
        return 1.0
    if cat in {"first", "followthrough"}:
        return 0.75
    if cat in {"fresh"}:
        return 0.65
    if cat in {"wrap"}:
        return 0.7
    return 0.75


def score_stock_relative_strength(
    code: Any,
    stock_history: pd.DataFrame,
    index_history: Optional[pd.DataFrame],
    *,
    category: str = "",
    boards: int = 0,
) -> Dict[str, Any]:
    """Score true relative leadership; never fabricates a neutral score on missing data."""
    benchmark = benchmark_symbol_for_stock(code)
    stock = _normalize_history(stock_history)
    index = _normalize_history(index_history)
    if stock is None or stock.empty:
        return {
            "available": False,
            "score": None,
            "benchmark": benchmark,
            "warning": "个股历史缺失，强弱因子已跳过",
            "reasons": [],
            "metrics": {},
        }
    target_key = str(stock.iloc[-1]["date_key"])
    target_dash = _date_dash(target_key)
    if not _history_covers(index, target_key):
        latest = ""
        if index is not None and not index.empty:
            latest = _date_dash(str(index.iloc[-1]["date_key"]))
        detail = f"（最新{latest}）" if latest else ""
        return {
            "available": False,
            "score": None,
            "benchmark": benchmark,
            "warning": f"指数历史未覆盖目标日{target_dash}{detail}，强弱因子已跳过",
            "reasons": [],
            "metrics": {"relative_strength_target_date": target_dash},
        }

    merged = stock[["date_key", "date", "close"]].rename(columns={"close": "stock_close"}).merge(
        index[["date_key", "close"]].rename(columns={"close": "index_close"}),
        on="date_key",
        how="inner",
    )
    merged = merged.sort_values("date_key").reset_index(drop=True)
    if len(merged) < 6 or str(merged.iloc[-1]["date_key"]) != target_key:
        return {
            "available": False,
            "score": None,
            "benchmark": benchmark,
            "warning": f"指数/个股历史交易日对齐不足，强弱因子已跳过",
            "reasons": [],
            "metrics": {"relative_strength_target_date": target_dash},
        }

    raw_score = 0.0
    reasons: List[str] = []
    metrics: Dict[str, Any] = {
        "relative_strength_target_date": target_dash,
        "relative_strength_benchmark": benchmark,
    }

    stock_close = pd.to_numeric(merged["stock_close"], errors="coerce")
    index_close = pd.to_numeric(merged["index_close"], errors="coerce")
    rs_line = stock_close / index_close

    for window in (10, 20, 30):
        stock_ret = _window_return(stock_close, window)
        index_ret = _window_return(index_close, window)
        if stock_ret is None or index_ret is None:
            continue
        excess = round(stock_ret - index_ret, 1)
        metrics[f"relative_strength_excess_{window}d"] = excess
        if window == 10:
            if excess >= 8:
                raw_score += 4
                reasons.append(f"10日强于指数{excess:+.1f}%+4")
            elif excess >= 4:
                raw_score += 2
                reasons.append(f"10日强于指数{excess:+.1f}%+2")
            elif excess <= -5:
                raw_score -= 3
                reasons.append(f"10日弱于指数{excess:+.1f}%-3")
        if window == 20:
            if excess >= 12:
                raw_score += 7
                reasons.append(f"启动前强20日{excess:+.1f}%+7")
            elif excess >= 8:
                raw_score += 5
                reasons.append(f"启动前强20日{excess:+.1f}%+5")
            elif excess >= 5:
                raw_score += 3
                reasons.append(f"20日强于指数{excess:+.1f}%+3")
            elif excess <= -8:
                raw_score -= 5
                reasons.append(f"20日弱于指数{excess:+.1f}%-5")
        if window == 30:
            if excess >= 15:
                raw_score += 5
                reasons.append(f"30日强势沉淀{excess:+.1f}%+5")
            elif excess >= 9:
                raw_score += 3
                reasons.append(f"30日强于指数{excess:+.1f}%+3")
            elif excess <= -10:
                raw_score -= 5
                reasons.append(f"30日弱于指数{excess:+.1f}%-5")

    rs20 = _window_return(rs_line, 20)
    if rs20 is not None:
        metrics["relative_strength_line_20d"] = round(rs20, 1)
        if rs20 >= 8:
            raw_score += 4
            reasons.append(f"强弱线20日上行{rs20:+.1f}%+4")
        elif rs20 >= 4:
            raw_score += 2
            reasons.append(f"强弱线20日抬头{rs20:+.1f}%+2")
        elif rs20 <= -5:
            raw_score -= 3
            reasons.append(f"强弱线20日走弱{rs20:+.1f}%-3")

    if len(rs_line.dropna()) >= 20:
        recent = rs_line.dropna().iloc[-30:]
        if not recent.empty and float(recent.iloc[-1]) >= float(recent.max()) * 0.995:
            raw_score += 3
            reasons.append("强弱线近30日新高+3")

    daily_stock = stock_close.pct_change() * 100
    daily_index = index_close.pct_change() * 100
    recent = pd.DataFrame({"stock": daily_stock, "index": daily_index}).dropna().tail(20)
    down_days = recent[recent["index"] < 0]
    if len(down_days) >= 3:
        outperform_rate = float((down_days["stock"] >= down_days["index"]).mean())
        positive_rate = float((down_days["stock"] > 0).mean())
        metrics["relative_strength_down_day_outperform_rate"] = round(outperform_rate * 100, 1)
        if outperform_rate >= 0.7 and positive_rate >= 0.25:
            raw_score += 3
            reasons.append(f"弱市抗跌{outperform_rate*100:.0f}%+3")
        elif outperform_rate < 0.45:
            raw_score -= 3
            reasons.append(f"弱市跑输{outperform_rate*100:.0f}%-3")

    stock_10d = _window_return(stock_close, 10)
    stock_30d = _window_return(stock_close, 30)
    if stock_10d is not None and stock_10d > 35:
        raw_score -= 4
        reasons.append(f"10日涨{stock_10d:+.1f}%过热-4")
    elif stock_30d is not None and stock_30d > 65:
        raw_score -= 4
        reasons.append(f"30日涨{stock_30d:+.1f}%过热-4")

    raw_score = max(-10.0, min(16.0, raw_score))
    weight = _category_weight(category, int(boards or 0))
    final_score = int(round(raw_score * weight))
    metrics["relative_strength_raw_score"] = round(raw_score, 2)
    metrics["relative_strength_weight"] = weight
    metrics["relative_strength_score"] = final_score

    if not reasons:
        reasons.append("相对指数中性+0")
    return {
        "available": True,
        "score": final_score,
        "benchmark": benchmark,
        "warning": "",
        "reasons": reasons,
        "metrics": metrics,
    }
