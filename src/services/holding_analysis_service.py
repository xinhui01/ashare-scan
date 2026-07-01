"""Single-stock holding analysis.

The service is intentionally pure: it reads an existing detail payload's history
and analysis dict, then returns a structured recommendation without persistence.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd


UNKNOWN_RESULT = {
    "advice": "无法判断",
    "risk_level": "高",
    "score": 0,
    "reasons": ["历史数据不足，无法形成持有判断"],
    "key_levels": {},
    "summary": "无法判断：历史数据不足，无法形成持有判断。",
}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        converted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(converted):
        return None
    return converted


def _round_or_none(value: Any, digits: int = 2) -> Optional[float]:
    number = _safe_float(value)
    if number is None:
        return None
    return round(number, digits)


def _advice_for_score(score: int) -> str:
    if score >= 75:
        return "继续持有"
    if score >= 60:
        return "谨慎持有"
    if score >= 45:
        return "减仓观察"
    return "止盈或离场观察"


def _risk_for_score(score: int) -> str:
    if score >= 75:
        return "低"
    if score >= 45:
        return "中"
    return "高"


def _append_reason(reasons: List[str], text: str) -> None:
    if text and text not in reasons:
        reasons.append(text)


def analyze_holding(history: Any, analysis: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analyze whether a currently viewed stock is worth holding.

    Args:
        history: DataFrame-like K-line data with at least a ``close`` column.
        analysis: Existing detail analysis dict. Optional keys such as
            ``strong_followthrough`` and ``broken_limit_up`` refine the score.

    Returns:
        Dict with ``advice``, ``risk_level``, ``score``, ``reasons``,
        ``key_levels`` and ``summary``.
    """
    if history is None or getattr(history, "empty", True):
        return dict(UNKNOWN_RESULT)
    if "close" not in history.columns:
        return dict(UNKNOWN_RESULT)

    df = history.copy()
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 10:
        return dict(UNKNOWN_RESULT)

    analysis = analysis or {}
    latest_close = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    ma5 = close.rolling(window=5, min_periods=5).mean().iloc[-1]
    ma10 = close.rolling(window=10, min_periods=10).mean().iloc[-1]
    score = 50.0
    reasons: List[str] = []

    if latest_close >= ma5:
        score += 12
        _append_reason(reasons, f"收盘价站上MA5({ma5:.2f})")
    else:
        score -= 16
        _append_reason(reasons, f"收盘价跌破MA5({ma5:.2f})")

    if latest_close >= ma10:
        score += 10
        _append_reason(reasons, f"收盘价站上MA10({ma10:.2f})")
    else:
        score -= 12
        _append_reason(reasons, f"收盘价跌破MA10({ma10:.2f})")

    if ma5 >= ma10:
        score += 8
        _append_reason(reasons, "MA5高于MA10，短线趋势未破")
    else:
        score -= 8
        _append_reason(reasons, "MA5低于MA10，短线趋势转弱")

    base_idx = max(0, len(close) - 6)
    base_close = float(close.iloc[base_idx])
    five_day_return = (latest_close / base_close - 1.0) * 100.0 if base_close > 0 else 0.0
    if five_day_return > 20:
        score -= 4
        _append_reason(reasons, f"近5日涨幅{five_day_return:.1f}%，短线偏透支")
    elif five_day_return >= 0:
        score += 6
        _append_reason(reasons, f"近5日涨幅{five_day_return:.1f}%，走势仍为正")
    else:
        score -= 8
        _append_reason(reasons, f"近5日跌幅{abs(five_day_return):.1f}%，短线承压")

    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else None
    latest_volume = None
    avg_volume = None
    if volume is not None and len(volume.dropna()) >= 6:
        latest_volume = _safe_float(volume.iloc[-1])
        avg_volume = _safe_float(volume.iloc[-6:-1].mean())
        if latest_close < prev_close and latest_volume and avg_volume and latest_volume >= avg_volume * 1.5:
            score -= 18
            _append_reason(reasons, "放量下跌，疑似资金离场")
        elif latest_close >= prev_close and latest_volume and avg_volume and latest_volume >= avg_volume * 1.2:
            score += 5
            _append_reason(reasons, "上涨伴随温和放量")

    strong_followthrough = analysis.get("strong_followthrough") or {}
    if isinstance(strong_followthrough, dict):
        if strong_followthrough.get("has_strong_followthrough"):
            score += 14
            _append_reason(reasons, "涨停后承接强势成立")
        elif strong_followthrough.get("limit_up_date"):
            score -= 8
            _append_reason(reasons, "涨停后承接未确认")

    if analysis.get("broken_limit_up"):
        score -= 16
        _append_reason(reasons, "存在断板走弱信号")

    macd_bar = _safe_float(analysis.get("macd_bar"))
    if macd_bar is not None:
        if macd_bar > 0:
            score += 4
            _append_reason(reasons, "MACD柱线为正")
        elif macd_bar < 0:
            score -= 4
            _append_reason(reasons, "MACD柱线为负")

    kdj_k = _safe_float(analysis.get("kdj_k"))
    kdj_d = _safe_float(analysis.get("kdj_d"))
    if kdj_k is not None and kdj_d is not None:
        if kdj_k >= kdj_d:
            score += 3
            _append_reason(reasons, "KDJ仍保持多头")
        else:
            score -= 3
            _append_reason(reasons, "KDJ出现转弱")

    final_score = max(0, min(100, int(round(score))))
    advice = _advice_for_score(final_score)
    risk_level = _risk_for_score(final_score)
    key_levels = {
        "latest_close": _round_or_none(latest_close),
        "ma5": _round_or_none(ma5),
        "ma10": _round_or_none(ma10),
        "five_day_return_pct": _round_or_none(five_day_return),
        "latest_volume": _round_or_none(latest_volume, 0),
        "avg_volume_5d": _round_or_none(avg_volume, 0),
    }
    reasons = reasons[:6] or ["当前信号不充分，建议继续观察"]
    return {
        "advice": advice,
        "risk_level": risk_level,
        "score": final_score,
        "reasons": reasons,
        "key_levels": key_levels,
        "summary": f"{advice}：风险{risk_level}，评分{final_score}/100。",
    }
