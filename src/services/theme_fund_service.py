"""题材/板块资金潜伏与爆发评分。"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring.trend import _score_accumulation_signal


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return digits.zfill(6) if digits else ""


def _concept_breakout_score(concept: Dict[str, Any]) -> int:
    phase = str(concept.get("phase") or "")
    trend = str(concept.get("trend") or "")
    today = int(concept.get("today_count") or 0)
    total = int(concept.get("total_limit_ups") or concept.get("member_count") or 0)
    opportunity = int(concept.get("opportunity_score") or 0)

    score = 0
    score += {"萌芽": 10, "主升": 14, "末期": -6, "退潮": -12}.get(phase, 0)
    score += {"rising": 10, "flat": 0, "declining": -8}.get(trend, 0)
    if today >= 5:
        score += 14
    elif today >= 3:
        score += 10
    elif today >= 2:
        score += 5
    if total >= 10:
        score += 6
    elif total >= 6:
        score += 3
    if opportunity >= 80:
        score += 6
    elif opportunity >= 65:
        score += 3
    return max(0, min(40, score))


def build_theme_fund_context(
    concepts: List[Dict[str, Any]],
    *,
    fetcher,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    max_members_per_theme: int = 20,
) -> Dict[str, Any]:
    """按题材聚合资金潜伏分和爆发分，返回可直接 merge 到 compare_context 的字段。"""
    theme_accumulation: Dict[str, int] = {}
    theme_breakout: Dict[str, int] = {}
    theme_fund_score: Dict[str, int] = {}
    code_theme_fund_score: Dict[str, int] = {}
    industry_theme_fund_score: Dict[str, int] = {}
    theme_member_stats: Dict[str, Dict[str, Any]] = {}

    for concept in concepts or []:
        if not isinstance(concept, dict):
            continue
        name = str(concept.get("name") or "").strip()
        source = str(concept.get("source") or "").strip()
        if not name or source == "行业":
            continue

        members = [
            m for m in (concept.get("members") or [])
            if isinstance(m, dict) and _normalize_code(m.get("code"))
        ][:max_members_per_theme]
        if not members:
            continue

        member_scores: Dict[str, int] = {}
        high_count = 0
        for member in members:
            code = _normalize_code(member.get("code"))
            if not code:
                continue
            try:
                request_plan = (
                    build_local_cache_history_plan_fn(reason="theme-fund-cache-only")
                    if build_local_cache_history_plan_fn is not None
                    else None
                )
                history = fetcher.get_history_data(
                    code, days=120, force_refresh=False, request_plan=request_plan,
                )
            except Exception:
                history = None
            if history is None or getattr(history, "empty", True) or len(history) < 31:
                continue
            df = history.sort_values("date").reset_index(drop=True)
            close = pd.to_numeric(df["close"], errors="coerce")
            volume = (
                pd.to_numeric(df.get("volume"), errors="coerce")
                if "volume" in df.columns
                else pd.Series(dtype=float)
            )
            raw_score, risk_penalty, _reasons, _metrics = _score_accumulation_signal(
                close, volume, len(df) - 1,
            )
            score = max(0, int(raw_score + risk_penalty))
            member_scores[code] = score
            if score >= 12:
                high_count += 1

        if not member_scores:
            continue
        avg_score = round(sum(member_scores.values()) / len(member_scores))
        breadth_bonus = min(6, high_count * 2)
        accumulation = max(0, min(30, int(avg_score + breadth_bonus)))
        breakout = _concept_breakout_score(concept)
        fund_score = max(0, min(100, int(accumulation * 1.5 + breakout)))

        theme_accumulation[name] = accumulation
        theme_breakout[name] = breakout
        theme_fund_score[name] = fund_score
        theme_member_stats[name] = {
            "member_count": len(member_scores),
            "high_accumulation_count": high_count,
            "avg_accumulation_score": avg_score,
        }
        for code in member_scores:
            code_theme_fund_score[code] = max(
                int(code_theme_fund_score.get(code, 0)),
                fund_score,
            )
        for item in concept.get("related_industries") or []:
            if not isinstance(item, dict):
                continue
            industry = str(item.get("name") or "").strip()
            if industry:
                industry_theme_fund_score[industry] = max(
                    int(industry_theme_fund_score.get(industry, 0)),
                    fund_score,
                )

    theme_sentiment_delta = 0
    if theme_fund_score:
        top_score = max(theme_fund_score.values())
        hot_themes = sum(1 for v in theme_fund_score.values() if v >= 55)
        if top_score >= 70:
            theme_sentiment_delta += 8
        elif top_score >= 55:
            theme_sentiment_delta += 5
        if hot_themes >= 3:
            theme_sentiment_delta += 4
        elif hot_themes == 0:
            theme_sentiment_delta -= 4

    return {
        "theme_fund_accumulation_map": theme_accumulation,
        "theme_breakout_map": theme_breakout,
        "theme_fund_score_map": theme_fund_score,
        "code_theme_fund_score": code_theme_fund_score,
        "industry_theme_fund_score": industry_theme_fund_score,
        "theme_member_fund_stats": theme_member_stats,
        "theme_sentiment_delta": max(-10, min(12, int(theme_sentiment_delta))),
    }
