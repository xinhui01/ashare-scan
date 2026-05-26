"""股票详情服务（detail chain）。

模块级函数（参数注入模式），覆盖 stock_filter.get_stock_detail* 的全部逻辑：

身份解析：
- resolve_stock_identity

analysis 增强：
- enrich_analysis_with_history_snapshot
- enrich_analysis_with_indicators

payload 组装：
- build_stock_detail_payload

公开 API：
- get_stock_detail_quick
- get_stock_detail
- get_stock_detail_history

依赖：StockDataFetcher（fetcher 参数）+ 注入的 analyze_history_fn /
call_with_timeout_fn / FilterSettings 派生参数。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

import pandas as pd

from scan_models import FilterSettings
from stock_store import load_limit_up_stock_meta

logger = logging.getLogger(__name__)


def _fetch_history_with_sina_fallback(
    code: str,
    *,
    days: int,
    fetcher,
    call_with_timeout_fn: Callable[..., Any],
    task_label: str,
    primary_timeout: float = 15.0,
    sina_timeout: float = 15.0,
) -> Optional[pd.DataFrame]:
    """主源（默认 auto，东财优先）15s 超时/失败后，再单独走一次新浪。"""
    history = call_with_timeout_fn(
        lambda: fetcher.get_history_data(code, days=days),
        timeout_sec=primary_timeout,
        fallback=None,
        task_name=task_label,
    )
    if history is not None and not getattr(history, "empty", True):
        return history
    try:
        sina_plan = fetcher.build_history_request_plan(source="sina")
    except Exception as exc:
        logger.debug("构建新浪 plan 失败: %s", exc)
        return history
    return call_with_timeout_fn(
        lambda: fetcher.get_history_data(code, days=days, request_plan=sina_plan),
        timeout_sec=sina_timeout,
        fallback=None,
        task_name=f"{task_label}(新浪兜底)",
    )


def resolve_stock_identity(
    universe: Optional[pd.DataFrame],
    stock_code: str,
) -> Dict[str, str]:
    code = str(stock_code or "").strip().zfill(6)
    cached_meta = load_limit_up_stock_meta(code) or {}
    if universe is None or universe.empty or not code:
        return {
            "name": str(cached_meta.get("name", "") or ""),
            "board": "",
            "exchange": "",
            "industry": str(cached_meta.get("industry", "") or ""),
            "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
        }
    try:
        match = universe[universe["code"].astype(str).str.zfill(6) == code]
    except Exception:
        match = pd.DataFrame()
    if match.empty:
        return {
            "name": str(cached_meta.get("name", "") or ""),
            "board": "",
            "exchange": "",
            "industry": str(cached_meta.get("industry", "") or ""),
            "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
        }
    row = match.iloc[0]
    return {
        "name": str(row.get("name", "") or "") or str(cached_meta.get("name", "") or ""),
        "board": str(row.get("board", "") or ""),
        "exchange": str(row.get("exchange", "") or ""),
        "industry": str(cached_meta.get("industry", "") or ""),
        "last_limit_up_trade_date": str(cached_meta.get("last_limit_up_trade_date", "") or ""),
    }


def enrich_analysis_with_history_snapshot(
    analysis: Dict[str, Any],
    history: Optional[pd.DataFrame],
) -> None:
    if history is not None and not history.empty:
        latest_row = history.iloc[-1]
        analysis["latest_volume"] = latest_row.get("volume")
        analysis["latest_amount"] = latest_row.get("amount")
        analysis["quote_time"] = str(latest_row.get("date", "") or "")
        return
    analysis["latest_volume"] = None
    analysis["latest_amount"] = None
    analysis["quote_time"] = ""


def enrich_analysis_with_indicators(
    analysis: Dict[str, Any],
    history: Optional[pd.DataFrame],
) -> None:
    """在 analysis 字典中追加 MACD/KDJ/RSI/BOLL 最新值。"""
    if history is None or history.empty or "close" not in history.columns:
        analysis["macd_dif"] = None
        analysis["macd_dea"] = None
        analysis["macd_bar"] = None
        analysis["kdj_k"] = None
        analysis["kdj_d"] = None
        analysis["kdj_j"] = None
        analysis["rsi_6"] = None
        analysis["rsi_12"] = None
        analysis["boll_upper"] = None
        analysis["boll_mid"] = None
        analysis["boll_lower"] = None
        return
    try:
        from stock_indicators import calc_macd, calc_kdj, calc_rsi, calc_boll
        close = pd.to_numeric(history["close"], errors="coerce")
        m = calc_macd(close)
        analysis["macd_dif"] = round(float(m["dif"].iloc[-1]), 3) if not pd.isna(m["dif"].iloc[-1]) else None
        analysis["macd_dea"] = round(float(m["dea"].iloc[-1]), 3) if not pd.isna(m["dea"].iloc[-1]) else None
        analysis["macd_bar"] = round(float(m["macd"].iloc[-1]), 3) if not pd.isna(m["macd"].iloc[-1]) else None

        if all(c in history.columns for c in ("high", "low")):
            k = calc_kdj(history["high"], history["low"], close)
            analysis["kdj_k"] = round(float(k["k"].iloc[-1]), 2) if not pd.isna(k["k"].iloc[-1]) else None
            analysis["kdj_d"] = round(float(k["d"].iloc[-1]), 2) if not pd.isna(k["d"].iloc[-1]) else None
            analysis["kdj_j"] = round(float(k["j"].iloc[-1]), 2) if not pd.isna(k["j"].iloc[-1]) else None
        else:
            analysis["kdj_k"] = analysis["kdj_d"] = analysis["kdj_j"] = None

        r = calc_rsi(close, periods=(6, 12))
        analysis["rsi_6"] = round(float(r["rsi_6"].iloc[-1]), 2) if not pd.isna(r["rsi_6"].iloc[-1]) else None
        analysis["rsi_12"] = round(float(r["rsi_12"].iloc[-1]), 2) if not pd.isna(r["rsi_12"].iloc[-1]) else None

        b = calc_boll(close)
        analysis["boll_upper"] = round(float(b["upper"].iloc[-1]), 2) if not pd.isna(b["upper"].iloc[-1]) else None
        analysis["boll_mid"] = round(float(b["mid"].iloc[-1]), 2) if not pd.isna(b["mid"].iloc[-1]) else None
        analysis["boll_lower"] = round(float(b["lower"].iloc[-1]), 2) if not pd.isna(b["lower"].iloc[-1]) else None
    except Exception as exc:
        logger.debug("技术指标计算失败: %s", exc)


def build_stock_detail_payload(
    stock_code: str,
    stock_identity: Dict[str, str],
    history: Optional[pd.DataFrame],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "code": str(stock_code).strip().zfill(6),
        "name": str(stock_identity.get("name", "") or ""),
        "board": str(stock_identity.get("board", "") or ""),
        "exchange": str(stock_identity.get("exchange", "") or ""),
        "industry": str(stock_identity.get("industry", "") or ""),
        "last_limit_up_trade_date": str(stock_identity.get("last_limit_up_trade_date", "") or ""),
        "history": history,
        "analysis": analysis,
    }


def get_stock_detail_quick(
    stock_code: str,
    *,
    settings: FilterSettings,
    fetcher,
    analyze_history_fn: Callable[..., Dict[str, Any]],
    call_with_timeout_fn: Callable[..., Any],
) -> Dict[str, Any]:
    code = str(stock_code).strip().zfill(6)
    history_days = max(
        80,
        settings.trend_days + settings.limit_up_lookback_days + settings.ma_period + 20,
    )
    history = _fetch_history_with_sina_fallback(
        code,
        days=history_days,
        fetcher=fetcher,
        call_with_timeout_fn=call_with_timeout_fn,
        task_label=f"详情历史 {code}",
    )
    # cached_meta 里通常已存 name / industry / last_limit_up_trade_date，
    # 秒开时就用上，避免要等 full 路径才闪出行业。
    stock_identity = resolve_stock_identity(None, code)
    analysis = analyze_history_fn(
        history,
        settings.trend_days,
        settings.ma_period,
        settings.limit_up_lookback_days,
        settings.volume_lookback_days,
        settings.volume_expand_enabled,
        settings.volume_expand_factor,
        stock_name=stock_identity["name"],
        stock_code=stock_code,
    )
    enrich_analysis_with_history_snapshot(analysis, history)
    enrich_analysis_with_indicators(analysis, history)
    return build_stock_detail_payload(code, stock_identity, history, analysis)


def get_stock_detail(
    stock_code: str,
    preloaded_history: Optional[pd.DataFrame] = None,
    *,
    settings: FilterSettings,
    fetcher,
    analyze_history_fn: Callable[..., Dict[str, Any]],
    call_with_timeout_fn: Callable[..., Any],
) -> Dict[str, Any]:
    code = str(stock_code).strip().zfill(6)
    history_days = max(
        80,
        settings.trend_days + settings.limit_up_lookback_days + settings.ma_period + 20,
    )

    # quick 路径已拉 history 并完整 analyze 过；这一段只补 universe → 行业/板块身份。
    history = preloaded_history
    universe = None
    if history is None:
        history = _fetch_history_with_sina_fallback(
            code,
            days=history_days,
            fetcher=fetcher,
            call_with_timeout_fn=call_with_timeout_fn,
            task_label=f"详情历史 {code}",
        )
    try:
        universe = call_with_timeout_fn(
            lambda: fetcher.get_all_stocks(),
            8.0, None, f"详情股票池 {code}",
        )
    except Exception as exc:
        logger.debug("预取股票池异常: %s", exc)

    stock_identity = resolve_stock_identity(universe, code)
    analysis = analyze_history_fn(
        history,
        settings.trend_days,
        settings.ma_period,
        settings.limit_up_lookback_days,
        settings.volume_lookback_days,
        settings.volume_expand_enabled,
        settings.volume_expand_factor,
        board=stock_identity["board"],
        stock_name=stock_identity["name"],
        stock_code=stock_code,
    )
    enrich_analysis_with_history_snapshot(analysis, history)
    enrich_analysis_with_indicators(analysis, history)
    return build_stock_detail_payload(code, stock_identity, history, analysis)


def get_stock_detail_history(
    stock_code: str,
    days: int,
    *,
    fetcher,
    call_with_timeout_fn: Callable[..., Any],
) -> Optional[pd.DataFrame]:
    code = str(stock_code).strip().zfill(6)
    history_days = max(60, int(days))
    return _fetch_history_with_sina_fallback(
        code,
        days=history_days,
        fetcher=fetcher,
        call_with_timeout_fn=call_with_timeout_fn,
        task_label=f"补充详情历史 {code}",
    )
