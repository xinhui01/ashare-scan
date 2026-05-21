"""filter_stock 链路（单股过滤 + analyze_history）。

模块级函数（参数注入模式），覆盖 stock_filter.filter_stock 的全部协调逻辑：

历史分析：
- resolve_analysis_config / build_analysis_service
- analyze_history（公开 API）
- check_close_above_ma / limit_up_threshold
- calculate_limit_up_streak / calculate_trade_score

filter_stock 链：
- build_filter_result_shell / resolve_filter_history_days
- attach_filter_analysis / apply_limit_up_requirement_failure
- apply_strong_followthrough_failure / build_strong_ft_failure_reason
- finalize_filter_result
- filter_stock（公开 API）

排序：
- result_sort_key

依赖：StockDataFetcher（fetcher 参数）+ FilterSettings 衍生参数。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from scan_models import FilterSettings, HistoryRequestPlan
from src.models.analysis_models import HistoryAnalysisConfig
from src.services.history_analysis_service import HistoryAnalysisService


def resolve_analysis_config(
    settings: FilterSettings,
    *,
    streak_days: Optional[int] = None,
    ma_period: Optional[int] = None,
    limit_up_lookback_days: Optional[int] = None,
    volume_lookback_days: Optional[int] = None,
    volume_expand_enabled: Optional[bool] = None,
    volume_expand_factor: Optional[float] = None,
) -> HistoryAnalysisConfig:
    return HistoryAnalysisConfig.from_filter_settings(
        settings,
        trend_days=streak_days,
        ma_period=ma_period,
        limit_up_lookback_days=limit_up_lookback_days,
        volume_lookback_days=volume_lookback_days,
        volume_expand_enabled=volume_expand_enabled,
        volume_expand_factor=volume_expand_factor,
    )


def build_analysis_service(
    settings: FilterSettings,
    *,
    streak_days: Optional[int] = None,
    ma_period: Optional[int] = None,
    limit_up_lookback_days: Optional[int] = None,
    volume_lookback_days: Optional[int] = None,
    volume_expand_enabled: Optional[bool] = None,
    volume_expand_factor: Optional[float] = None,
) -> HistoryAnalysisService:
    config = resolve_analysis_config(
        settings,
        streak_days=streak_days,
        ma_period=ma_period,
        limit_up_lookback_days=limit_up_lookback_days,
        volume_lookback_days=volume_lookback_days,
        volume_expand_enabled=volume_expand_enabled,
        volume_expand_factor=volume_expand_factor,
    )
    return HistoryAnalysisService(config)


def limit_up_threshold(
    settings: FilterSettings,
    board: str = "",
    stock_name: str = "",
) -> float:
    return build_analysis_service(settings).limit_up_threshold(
        board=board,
        stock_name=stock_name,
    )


def check_close_above_ma(
    settings: FilterSettings,
    history_data: pd.DataFrame,
    streak_days: int,
    ma_period: int,
) -> bool:
    return build_analysis_service(settings).check_close_above_ma(
        history_data,
        streak_days=streak_days,
        ma_period=ma_period,
    )


def calculate_limit_up_streak(mask: pd.Series) -> int:
    """计算从最新交易日往前数的连续涨停天数。"""
    streak = 0
    for flag in reversed(mask.tolist()):
        if bool(flag):
            streak += 1
        else:
            break
    return streak


def calculate_trade_score(
    settings: FilterSettings,
    result: Dict[str, Any],
    streak_days: int,
    ma_period: int,
    volume_enabled: bool,
) -> Tuple[int, str]:
    return build_analysis_service(
        settings,
        streak_days=streak_days,
        ma_period=ma_period,
        volume_expand_enabled=volume_enabled,
    ).calculate_trade_score(result)


def analyze_history(
    settings: FilterSettings,
    history_data: pd.DataFrame,
    streak_days: Optional[int] = None,
    ma_period: Optional[int] = None,
    limit_up_lookback_days: Optional[int] = None,
    volume_lookback_days: Optional[int] = None,
    volume_expand_enabled: Optional[bool] = None,
    volume_expand_factor: Optional[float] = None,
    board: str = "",
    stock_name: str = "",
    stock_code: str = "",
) -> Dict[str, Any]:
    return build_analysis_service(
        settings,
        streak_days=streak_days,
        ma_period=ma_period,
        limit_up_lookback_days=limit_up_lookback_days,
        volume_lookback_days=volume_lookback_days,
        volume_expand_enabled=volume_expand_enabled,
        volume_expand_factor=volume_expand_factor,
    ).analyze_history(
        history_data,
        board=board,
        stock_name=stock_name,
        stock_code=stock_code,
    )


def build_filter_result_shell(
    stock_code: str,
    stock_name: str,
    board: str,
    exchange: str,
) -> Dict[str, Any]:
    result = {
        "code": str(stock_code).strip().zfill(6),
        "name": stock_name or "",
        "passed": False,
        "reasons": [],
        "data": {},
    }
    if board:
        result["data"]["board"] = board
    if exchange:
        result["data"]["exchange"] = exchange
    return result


def resolve_filter_history_days(settings: FilterSettings) -> int:
    return max(
        14,
        settings.trend_days + settings.ma_period + 4,
        settings.limit_up_lookback_days + settings.ma_period + 4,
        settings.volume_lookback_days + 4,
    )


def attach_filter_analysis(
    settings: FilterSettings,
    result: Dict[str, Any],
    history_data: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    board: str,
) -> Dict[str, Any]:
    analysis = analyze_history(
        settings,
        history_data,
        settings.trend_days,
        settings.ma_period,
        settings.limit_up_lookback_days,
        settings.volume_lookback_days,
        settings.volume_expand_enabled,
        settings.volume_expand_factor,
        board=board,
        stock_name=stock_name,
        stock_code=stock_code,
    )
    result["data"]["analysis"] = analysis
    result["data"]["history_tail"] = history_data.tail(
        max(settings.trend_days, settings.limit_up_lookback_days)
    ).copy()
    return analysis


def apply_limit_up_requirement_failure(
    settings: FilterSettings,
    result: Dict[str, Any],
    analysis: Dict[str, Any],
) -> bool:
    if not settings.require_limit_up_within_days or analysis.get("limit_up_within_days"):
        return False
    analysis["summary"] = (
        f"{analysis['summary']}；未命中过去{settings.limit_up_lookback_days}个交易日涨停条件"
        if analysis.get("summary")
        else f"未命中过去{settings.limit_up_lookback_days}个交易日涨停条件"
    )
    result["reasons"].append(analysis["summary"])
    return True


def apply_strong_followthrough_failure(
    settings: FilterSettings,
    result: Dict[str, Any],
    analysis: Dict[str, Any],
) -> bool:
    """当开启"承接强势"过滤时，未命中形态的股票直接淘汰。"""
    if not settings.strong_ft_enabled:
        return False
    ft = analysis.get("strong_followthrough") or {}
    if ft.get("has_strong_followthrough"):
        return False
    reason = build_strong_ft_failure_reason(settings, ft)
    analysis["summary"] = (
        f"{analysis['summary']}；{reason}" if analysis.get("summary") else reason
    )
    result["reasons"].append(reason)
    return True


def build_strong_ft_failure_reason(
    settings: FilterSettings,
    ft: Dict[str, Any],
) -> str:
    """把 followthrough 结果翻译成人类友好的失败原因。"""
    if ft.get("limit_up_is_today"):
        return f"{ft.get('limit_up_date')} 刚涨停，次日走势还未出现，无法判断承接"
    if not ft.get("limit_up_date"):
        return f"近{settings.limit_up_lookback_days}日未找到可承接的涨停日"
    parts = [f"{ft['limit_up_date']} 涨停后"]
    if not ft.get("is_pullback_day"):
        parts.append("次日未回落（未形成承接形态）")
    if not ft.get("pullback_within_limit"):
        parts.append(
            f"回撤过深（{ft.get('pullback_pct', 0):.1f}% > {settings.strong_ft_max_pullback_pct:.1f}%）"
        )
    if not ft.get("volume_shrunk"):
        parts.append(
            f"未缩量（次日量比 {ft.get('pullback_volume_ratio', 0):.0%} > {settings.strong_ft_max_volume_ratio:.0%}）"
        )
    if not ft.get("holds_above_pullback_low"):
        parts.append("后续跌破回落日最低价")
    elif ft.get("hold_days", 0) < ft.get("min_hold_days", 0):
        parts.append(f"站稳天数不足（{ft.get('hold_days', 0)} < {ft.get('min_hold_days', 0)}）")
    return "；".join(parts)


def finalize_filter_result(
    result: Dict[str, Any],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:
    result["passed"] = bool(analysis.get("passed"))
    result["reasons"].append(analysis["summary"])
    return result


def filter_stock(
    settings: FilterSettings,
    *,
    fetcher,
    stock_code: str,
    stock_name: str = "",
    board: str = "",
    exchange: str = "",
    history_mirror: Optional[str] = None,
    mirror_pool: Optional[List[str]] = None,
    history_plan: Optional[HistoryRequestPlan] = None,
) -> Dict[str, Any]:
    result = build_filter_result_shell(stock_code, stock_name, board, exchange)
    history_data = fetcher.get_history_data(
        stock_code,
        days=resolve_filter_history_days(settings),
        preferred_mirror=history_mirror,
        mirror_pool=mirror_pool,
        request_plan=history_plan,
    )
    result["data"]["history"] = history_data
    if history_data is None or history_data.empty:
        result["reasons"].append("无法获取历史数据")
        return result

    analysis = attach_filter_analysis(
        settings, result, history_data, stock_code, stock_name, board
    )
    if apply_limit_up_requirement_failure(settings, result, analysis):
        return result
    if apply_strong_followthrough_failure(settings, result, analysis):
        return result
    return finalize_filter_result(result, analysis)


def result_sort_key(item: Dict[str, Any]):
    analysis = (item.get("data", {}) or {}).get("analysis") or {}
    five_day_return = analysis.get("five_day_return")
    volume_expand_ratio = analysis.get("volume_expand_ratio")
    latest_change_pct = analysis.get("latest_change_pct")
    return (
        five_day_return if five_day_return is not None else float("-inf"),
        volume_expand_ratio if volume_expand_ratio is not None else float("-inf"),
        1 if analysis.get("limit_up_within_days") else 0,
        latest_change_pct if latest_change_pct is not None else float("-inf"),
        str(item.get("code", "")),
    )
