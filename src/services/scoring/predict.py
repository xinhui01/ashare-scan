"""涨停预测主编排（predict）。

2 个模块级函数（参数注入模式）：
- predict_limit_up_candidates: 主编排，整合所有 scorer 模块，输出涨停候选预测结果
- build_compare_market_context: 从最近几组涨停对比中提炼市场环境

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn / build_local_cache_history_plan_fn。
内部直接调用 scoring 包内的各 scorer 模块（cont / first / fresh / wrap / trend / first_board /
classifiers / shared）。
"""
from __future__ import annotations

import logging
from concurrent.futures import TimeoutError as FutureTimeoutError, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from src.services.scoring import classifiers as _classifiers
from src.services.scoring import cont as _cont
from src.services.scoring import first as _first
from src.services.scoring import first_board as _first_board
from src.services.scoring import fresh as _fresh
from src.services.scoring import fresh_calibration as _fresh_calibration
from src.services.scoring import shared as _shared
from src.services.scoring import trend as _trend
from src.services.scoring import wrap as _wrap
from src.services import popularity_rank_service as _popularity_rank_service
from src.services.market_focus_advice_service import (
    build_market_focus_advice,
    format_market_focus_advice_lines,
)
from src.utils.codes import is_bse_code

logger = logging.getLogger(__name__)
_BLANK_INDUSTRY_VALUES = {"", "-", "--", "nan", "none", "null", "未知", "其他"}
MIN_PREDICT_LOOKBACK_DAYS = 2
DEFAULT_PREDICT_LOOKBACK_DAYS = 25
MAX_PREDICT_LOOKBACK_DAYS = 60

_PREDICTION_CANDIDATE_KEYS: Dict[str, str] = {
    "cont": "continuation_candidates",
    "first": "first_board_candidates",
    "fresh": "fresh_first_board_candidates",
    "wrap": "broken_board_wrap_candidates",
    "trend": "trend_limit_up_candidates",
}

_RETREAT_LIMITS = {
    "cont": 1,
    "first": 1,
    "fresh": 2,
    "wrap": 8,
    "trend": 4,
}
_RETREAT_TOTAL_LIMIT = 15

_ICE_POINT_LIMITS = {
    "cont": 0,
    "first": 0,
    "fresh": 1,
    "wrap": 4,
    "trend": 1,
}
_ICE_POINT_TOTAL_LIMIT = 5


def normalize_predict_lookback(value: Any) -> int:
    try:
        raw = int(value if value not in (None, "") else DEFAULT_PREDICT_LOOKBACK_DAYS)
    except (TypeError, ValueError):
        raw = DEFAULT_PREDICT_LOOKBACK_DAYS
    return max(MIN_PREDICT_LOOKBACK_DAYS, min(raw, MAX_PREDICT_LOOKBACK_DAYS))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value not in (None, "") else default)
    except (TypeError, ValueError):
        return default


def _text_matches_name(value: Any, name: str) -> bool:
    left = str(value or "").strip()
    right = str(name or "").strip()
    if not left or not right:
        return False
    return left == right or left in right or right in left


def _concept_strength_score(raw: Dict[str, Any]) -> int:
    today_count = _safe_int(raw.get("today_count"))
    active_days = _safe_int(raw.get("active_days"))
    opportunity_score = _safe_int(raw.get("opportunity_score"))
    total_limit_ups = _safe_int(raw.get("total_limit_ups") or raw.get("member_count"))
    duration = _safe_int(raw.get("duration"))
    source = str(raw.get("source") or "").strip()
    source_bonus = 8 if source and source != "行业" else 0
    return (
        opportunity_score
        + today_count * 8
        + min(active_days, 20) * 4
        + min(total_limit_ups, 80)
        + min(duration, 30)
        + source_bonus
    )


def _build_theme_data_quality(
    *,
    hype_stats: Dict[str, Any],
    real_concepts: List[Dict[str, Any]],
    industry_concepts_count: int,
    code_theme_map: Dict[str, str],
) -> Dict[str, Any]:
    concept_pairs = _safe_int(hype_stats.get("concept_pairs"))
    concept_covered_codes = _safe_int(hype_stats.get("concept_covered_codes"))
    llm_cache_days = _safe_int(hype_stats.get("llm_cache_days"))
    real_theme_count = len(real_concepts or [])
    direct_code_count = len(code_theme_map or {})
    fine_theme_available = bool(
        real_theme_count > 0
        and (concept_pairs > 0 or concept_covered_codes > 0 or llm_cache_days > 0 or direct_code_count > 0)
    )
    if fine_theme_available:
        level = "fine_theme"
        warning = ""
    elif industry_concepts_count > 0:
        level = "industry_fallback"
        warning = "细题材覆盖不足，当前主要使用行业主线兜底；非主线候选不会因为行业名被伪装成题材而加分。"
    else:
        level = "missing"
        warning = "细题材和行业主线都缺失，本次题材因子只做显式跳过。"
    return {
        "quality_level": level,
        "fine_theme_available": fine_theme_available,
        "real_theme_count": real_theme_count,
        "industry_theme_count": int(industry_concepts_count or 0),
        "direct_code_count": direct_code_count,
        "concept_pairs": concept_pairs,
        "concept_covered_codes": concept_covered_codes,
        "llm_cache_days": llm_cache_days,
        "warning": warning,
    }


def _count_missing_industries(df: Optional[pd.DataFrame]) -> int:
    if df is None or "所属行业" not in df.columns:
        return 0
    industries = df["所属行业"].fillna("").astype(str).str.strip()
    return int(industries.str.lower().isin(_BLANK_INDUSTRY_VALUES).sum())


def _drop_bse_rows(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    code_col = ""
    for candidate in ("代码", "code"):
        if candidate in df.columns:
            code_col = candidate
            break
    if not code_col:
        return df
    mask = df[code_col].astype(str).str.strip().str.zfill(6).map(is_bse_code)
    if not mask.any():
        return df
    return df.loc[~mask].reset_index(drop=True)


@dataclass
class _AsOfHistoryFetcher:
    """历史回放专用 fetcher 代理：所有日线读取都截断到 as-of 当天。"""
    base_fetcher: Any
    as_of_trade_date: str

    def __getattr__(self, name: str) -> Any:
        return getattr(self.base_fetcher, name)

    def get_history_data(
        self,
        stock_code: str,
        days: int = 10,
        force_refresh: bool = False,
        preferred_mirror: Optional[str] = None,
        mirror_pool: Optional[List[str]] = None,
        request_plan: Optional[Any] = None,
    ):
        requested_days = int(days or 0)
        effective_days = max(requested_days, 120)

        def _fetch(use_days: int):
            try:
                return self.base_fetcher.get_history_data(
                    stock_code,
                    days=use_days,
                    force_refresh=force_refresh,
                    preferred_mirror=preferred_mirror,
                    mirror_pool=mirror_pool,
                    request_plan=request_plan,
                    as_of_trade_date=self.as_of_trade_date,
                )
            except TypeError:
                return self.base_fetcher.get_history_data(
                    stock_code,
                    days=use_days,
                    force_refresh=force_refresh,
                    preferred_mirror=preferred_mirror,
                    mirror_pool=mirror_pool,
                    request_plan=request_plan,
                )

        df = _fetch(effective_days)
        # store_facade.load_history 在 cache 行数 < min_rows 时整体返回 None，
        # 新股 / 短历史票会因此被误判为"零 K 线"。这里用调用方实际请求的 days
        # 再试一次，让短历史票也能命中缓存。
        if (df is None or df.empty) and requested_days and requested_days < effective_days:
            df = _fetch(requested_days)
        if df is None or df.empty or "date" not in df.columns:
            return df

        as_of = str(self.as_of_trade_date or "").strip()
        if len(as_of) == 8 and as_of.isdigit():
            as_of = f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}"

        date_col = (
            df["date"].astype(str).str.replace("/", "-", regex=False).str.replace(".", "-", regex=False)
        )
        trimmed = df.loc[date_col <= as_of].copy()
        if trimmed.empty:
            return trimmed
        return trimmed.tail(days).reset_index(drop=True)


def build_compare_market_context(
    trade_date: str,
    lookback_days: int,
    *,
    fetcher,
) -> Dict[str, Any]:
    """从最近几组涨停对比中提炼市场环境。

    迁自 StockFilter._build_compare_market_context；回溯窗口统一按预测合同标准化。
    """
    lookback_days = normalize_predict_lookback(lookback_days)
    window_days = max(2, int(lookback_days or 2) + 1)
    trade_dates = fetcher._recent_trade_dates(trade_date, window_days)
    pair_stats: List[Dict[str, Any]] = []

    for idx in range(1, len(trade_dates)):
        prev_date = trade_dates[idx - 1]
        cur_date = trade_dates[idx]
        try:
            compare = fetcher.compare_limit_up_pools(cur_date, prev_date)
        except Exception as exc:
            logger.debug("涨停预测获取涨停对比 %s/%s 失败: %s", cur_date, prev_date, exc)
            continue

        yesterday_first = compare.get("yesterday_first", []) or []
        continued = compare.get("continued_codes", []) or []
        lost = compare.get("lost_codes", []) or []
        first_count = len(yesterday_first)
        rate = round(len(continued) / first_count * 100, 1) if first_count else None
        pair_stats.append({
            "today_date": cur_date,
            "yesterday_date": prev_date,
            "yesterday_first_count": first_count,
            "continued_count": len(continued),
            "lost_count": len(lost),
            "continuation_rate": rate,
            "today_first_count": len(compare.get("today_first", []) or []),
        })

    valid_rates = [item["continuation_rate"] for item in pair_stats if item.get("continuation_rate") is not None]
    avg_rate = round(sum(valid_rates) / len(valid_rates), 1) if valid_rates else None
    latest_rate = pair_stats[-1]["continuation_rate"] if pair_stats else None
    latest_first_count = pair_stats[-1]["today_first_count"] if pair_stats else 0

    return {
        "trade_dates": trade_dates,
        "pair_stats": pair_stats,
        "pair_count": len(pair_stats),
        "avg_continuation_rate": avg_rate,
        "latest_continuation_rate": latest_rate,
        "latest_first_count": latest_first_count,
    }


def _apply_market_sentiment_context(
    compare_context: Dict[str, Any],
    data_quality: Dict[str, Any],
    sent: Dict[str, Any],
) -> None:
    """把市场情绪服务结果写入 compare_context，供所有 scorer 复用。"""
    base_sentiment_score = int(sent.get("score", 50))
    theme_sentiment_delta = int(compare_context.get("theme_sentiment_delta") or 0)
    final_sentiment_score = max(0, min(100, base_sentiment_score + theme_sentiment_delta))
    market_state = sent.get("market_state") or {}
    market_strategy = market_state.get("strategy") or {}
    raw = sent.get("raw") or {}
    rotation = raw.get("rotation") or {}

    compare_context["sentiment_base_score"] = base_sentiment_score
    compare_context["sentiment_score"] = final_sentiment_score
    compare_context["sentiment_label"] = (
        (sent.get("position_suggest") or {}).get("label", "")
    )
    compare_context["market_state"] = market_state
    compare_context["market_state_label"] = str(market_state.get("label") or "")
    compare_context["market_state_strategy"] = market_strategy
    compare_context["market_rotation"] = rotation

    sentiment_quality = data_quality.setdefault("sentiment", {})
    sentiment_quality["loaded"] = True
    sentiment_quality["score"] = final_sentiment_score
    sentiment_quality["base_score"] = base_sentiment_score
    sentiment_quality["theme_delta"] = theme_sentiment_delta
    sentiment_quality["label"] = compare_context.get("sentiment_label", "")
    sentiment_quality["market_state"] = compare_context.get("market_state_label", "")
    sentiment_quality["strategy_label"] = market_strategy.get("label", "")
    sentiment_quality["rotation_score"] = rotation.get("rotation_score", "")

    sent_external = (raw.get("external") or {})
    if not sent_external.get("ok", True):
        sentiment_quality["degraded"] = True
        data_quality.setdefault("warnings", []).append(
            "市场情绪外部数据降级：跌停池/上证指数未完整拉到"
        )


def _compute_timing_hint(trade_date: str, historical_mode: bool) -> str:
    """预测时机提示：盘中 / 盘后 / 历史模式 reason 数据完整度差异。

    复盘网（涨停 reason 主源）当天数据通常盘后 16:00+ 才发布完整。盘中跑预测
    时 reason 字段会走概念兜底（[xxx / yyy]），不是真实涨停原因。提示用户
    "什么时候跑预测最准"。
    """
    try:
        now = datetime.now()
        today_key = now.strftime("%Y%m%d")
    except Exception:
        return ""
    td = str(trade_date or "").strip()
    if not td:
        return ""

    if historical_mode or td < today_key:
        return (
            "历史模式：复盘网应已有完整 reason 数据；行情走本地缓存合成，结果稳定。"
        )

    if td == today_key:
        hour = now.hour
        if hour < 15:
            return (
                f"盘中预测（{now.strftime('%H:%M')}）：复盘网 reason 数据通常 "
                f"16:00 后才更新完整，当前 reason 字段将走概念标签兜底（不是真实涨停原因）。"
                f"建议盘后 16:30+ 重跑一次获取真实 reason。"
            )
        if hour < 16:
            return (
                f"刚收盘（{now.strftime('%H:%M')}）：复盘网正在更新中，部分 reason "
                f"可能仍为空 / 概念兜底。16:30 后重跑可拿到完整数据。"
            )
        return (
            f"盘后预测（{now.strftime('%H:%M')}）：复盘网 reason 数据应已稳定，预测精度最佳。"
        )

    return f"未来日期（{td}）：暂无数据，请检查 trade_date 设置。"


def _derive_board_strength_from_spot(spot_df: Optional[pd.DataFrame]) -> Dict[str, float]:
    """历史模式无 akshare 行业实时接口，从合成 spot 按 "所属行业" 聚合平均涨跌幅。

    覆盖范围受 limit_up_stock_meta 行业字段限制（只覆盖曾涨停过的票），
    无行业字段的票自动跳过；返回 dict: 行业名 → 平均涨跌幅 %。
    """
    if spot_df is None or not isinstance(spot_df, pd.DataFrame) or spot_df.empty:
        return {}
    if "所属行业" not in spot_df.columns or "涨跌幅" not in spot_df.columns:
        return {}
    df = spot_df[["所属行业", "涨跌幅"]].copy()
    df["所属行业"] = df["所属行业"].astype(str).str.strip()
    df = df[df["所属行业"] != ""]
    df["涨跌幅"] = pd.to_numeric(df["涨跌幅"], errors="coerce")
    df = df.dropna(subset=["涨跌幅"])
    if df.empty:
        return {}
    agg = df.groupby("所属行业")["涨跌幅"].mean()
    return {str(k): float(round(v, 2)) for k, v in agg.items()}


def _trade_date_digits(value: str) -> str:
    text = str(value or "").strip().replace("-", "").replace("/", "")
    return text[:8] if len(text) >= 8 and text[:8].isdigit() else ""


def _trade_date_dash(value: str) -> str:
    digits = _trade_date_digits(value)
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}" if digits else str(value or "").strip()


def _valid_spot_codes(spot_df: Optional[pd.DataFrame]) -> set[str]:
    if spot_df is None or not isinstance(spot_df, pd.DataFrame) or spot_df.empty:
        return set()
    if "代码" not in spot_df.columns:
        return set()
    work = spot_df.copy()
    work["代码"] = work["代码"].astype(str).str.strip().str.zfill(6)
    if "最新价" in work.columns:
        close = pd.to_numeric(work["最新价"], errors="coerce")
        work = work[close.notna() & (close > 0)]
    return {
        code for code in work["代码"].tolist()
        if code and not is_bse_code(code)
    }


def _select_strong_main_line(concepts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Pick the strongest sustained line for regime advice, allowing industry fallback."""
    candidates: List[Dict[str, Any]] = []
    for raw in concepts or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        phase = str(raw.get("phase") or "").strip()
        trend = str(raw.get("trend") or "").strip()
        if not name or phase != "主升":
            continue
        if trend == "declining":
            continue
        try:
            today_count = int(raw.get("today_count") or 0)
        except (TypeError, ValueError):
            today_count = 0
        try:
            active_days = int(raw.get("active_days") or 0)
        except (TypeError, ValueError):
            active_days = 0
        try:
            opportunity_score = int(raw.get("opportunity_score") or 0)
        except (TypeError, ValueError):
            opportunity_score = 0
        if today_count < 2:
            continue
        if active_days < 3 and opportunity_score < 60:
            continue
        line = {
            "name": name,
            "source": str(raw.get("source") or "").strip(),
            "phase": phase,
            "trend": trend,
            "today_count": today_count,
            "active_days": active_days,
            "duration": int(raw.get("duration") or 0),
            "opportunity_score": opportunity_score,
            "total_limit_ups": int(raw.get("total_limit_ups") or 0),
        }
        line["strength_score"] = _concept_strength_score(line)
        candidates.append(line)
    if not candidates:
        return {}
    candidates.sort(
        key=lambda item: (
            -int(item.get("strength_score") or 0),
            -int(item.get("opportunity_score") or 0),
            -int(item.get("today_count") or 0),
            -int(item.get("active_days") or 0),
            str(item.get("name") or ""),
        )
    )
    return candidates[0]


def _compact_concept_hype_topics(concepts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep lightweight concept evidence for downstream regime wording."""
    topics: List[Dict[str, Any]] = []
    for raw in concepts or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        if not name:
            continue
        try:
            today_count = int(raw.get("today_count") or 0)
        except (TypeError, ValueError):
            today_count = 0
        try:
            active_days = int(raw.get("active_days") or 0)
        except (TypeError, ValueError):
            active_days = 0
        topics.append({
            "name": name,
            "source": str(raw.get("source") or "").strip(),
            "phase": str(raw.get("phase") or "").strip(),
            "trend": str(raw.get("trend") or "").strip(),
            "today_count": today_count,
            "active_days": active_days,
        })
    return topics[:80]


def _declining_line_decay_score(raw: Dict[str, Any]) -> int:
    phase = str(raw.get("phase") or "").strip()
    trend = str(raw.get("trend") or "").strip()
    today_count = _safe_int(raw.get("today_count"))
    active_days = _safe_int(raw.get("active_days"))
    total_limit_ups = _safe_int(raw.get("total_limit_ups") or raw.get("member_count"))
    peak_count = _safe_int(raw.get("peak_count"))
    score = min(active_days, 20) * 4 + min(total_limit_ups, 80) + min(peak_count, 20) * 4
    if trend == "declining":
        score += 30
    if phase == "末期":
        score += 20
    elif phase == "退潮":
        score += 32
    score -= min(today_count, 10) * 2
    return max(0, score)


def _select_declining_main_lines(concepts: List[Dict[str, Any]], *, limit: int = 3) -> List[Dict[str, Any]]:
    """Return formerly-active lines that are no longer suitable for mainline chasing."""
    out: List[Dict[str, Any]] = []
    for raw in concepts or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        phase = str(raw.get("phase") or "").strip()
        trend = str(raw.get("trend") or "").strip()
        if not name:
            continue
        if phase not in {"末期", "退潮"} and trend != "declining":
            continue
        today_count = _safe_int(raw.get("today_count"))
        active_days = _safe_int(raw.get("active_days"))
        total_limit_ups = _safe_int(raw.get("total_limit_ups") or raw.get("member_count"))
        peak_count = _safe_int(raw.get("peak_count"))
        if active_days < 3 and total_limit_ups < 8 and peak_count < 3:
            continue
        item = {
            "name": name,
            "source": str(raw.get("source") or "").strip(),
            "phase": phase,
            "trend": trend,
            "today_count": today_count,
            "active_days": active_days,
            "duration": _safe_int(raw.get("duration")),
            "opportunity_score": _safe_int(raw.get("opportunity_score")),
            "total_limit_ups": total_limit_ups,
            "peak_count": peak_count,
            "decay_score": _declining_line_decay_score(raw),
        }
        out.append(item)
    out.sort(
        key=lambda item: (
            -int(item.get("decay_score") or 0),
            -int(item.get("active_days") or 0),
            -int(item.get("total_limit_ups") or 0),
            str(item.get("name") or ""),
        )
    )
    return out[: max(1, int(limit or 1))]


def _candidate_score(rec: Dict[str, Any]) -> float:
    for key in ("calibrated_score", "score", "total_score", "final_score"):
        if key in rec:
            return _safe_float(rec.get(key))
    return 0.0


def _candidate_matches_strong_line(
    category: str,
    rec: Dict[str, Any],
    compare_context: Dict[str, Any],
) -> bool:
    strong_line = compare_context.get("strong_main_line") or {}
    if not isinstance(strong_line, dict):
        return False
    name = str(strong_line.get("name") or "").strip()
    phase = str(strong_line.get("phase") or "").strip()
    if not name or phase != "主升":
        return False
    code = str(rec.get("code") or "").strip().zfill(6)
    theme_name = str(
        rec.get("theme")
        or rec.get("theme_name")
        or (compare_context.get("code_theme_map") or {}).get(code)
        or ""
    ).strip()
    industry = str(rec.get("industry") or "").strip()
    return _text_matches_name(theme_name, name) or _text_matches_name(industry, name)


def _candidate_declining_line(
    rec: Dict[str, Any],
    compare_context: Dict[str, Any],
) -> Dict[str, Any]:
    code = str(rec.get("code") or "").strip().zfill(6)
    theme_name = str(
        rec.get("theme")
        or rec.get("theme_name")
        or (compare_context.get("code_theme_map") or {}).get(code)
        or ""
    ).strip()
    industry = str(rec.get("industry") or "").strip()
    for raw in compare_context.get("declining_main_lines") or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        if name and (_text_matches_name(theme_name, name) or _text_matches_name(industry, name)):
            return raw
    return {}


def _candidate_has_fine_theme(rec: Dict[str, Any], compare_context: Dict[str, Any]) -> bool:
    code = str(rec.get("code") or "").strip().zfill(6)
    theme_name = str(
        rec.get("theme")
        or rec.get("theme_name")
        or (compare_context.get("code_theme_map") or {}).get(code)
        or ""
    ).strip()
    return bool(theme_name)


def _priority_adjustment(
    category: str,
    rec: Dict[str, Any],
    compare_context: Dict[str, Any],
    theme_quality: Dict[str, Any],
) -> Tuple[float, List[str]]:
    state_label = str(compare_context.get("market_state_label") or "").strip()
    strong_line = compare_context.get("strong_main_line") or {}
    strong_line_name = str(strong_line.get("name") or "").strip() if isinstance(strong_line, dict) else ""
    is_main_line = _candidate_matches_strong_line(category, rec, compare_context)
    fine_theme_available = bool((theme_quality or {}).get("fine_theme_available"))
    cat = str(category or "").strip()
    delta = 0.0
    reasons: List[str] = []
    declining_line = _candidate_declining_line(rec, compare_context)

    if is_main_line:
        if cat in {"first", "trend", "fresh"}:
            delta += 28.0
        elif cat == "wrap":
            delta += 18.0
        elif cat == "cont":
            delta += 8.0
        reasons.append(f"强主线{strong_line_name}+{int(delta)}")
    elif not fine_theme_available and strong_line_name:
        delta -= 3.0
        reasons.append("细题材不足，非强主线-3")

    if fine_theme_available and _candidate_has_fine_theme(rec, compare_context):
        delta += 6.0
        reasons.append("细题材命中+6")

    if declining_line:
        line_name = str(declining_line.get("name") or "").strip()
        phase = str(declining_line.get("phase") or "").strip()
        trend = str(declining_line.get("trend") or "").strip()
        penalty = -30.0
        if cat in {"cont", "first", "trend"}:
            penalty = -36.0
        elif cat == "fresh":
            penalty = -24.0
        elif cat == "wrap":
            penalty = -12.0
        delta += penalty
        detail = phase or trend or "转弱"
        reasons.append(f"衰退主线{line_name}({detail}){int(penalty)}")

    if state_label == "退潮日":
        if cat == "wrap":
            delta += 18.0
            reasons.append("退潮日反包修复优先+18")
        elif cat == "trend":
            penalty = -10.0 if is_main_line else -20.0
            delta += penalty
            reasons.append(f"退潮日趋势收缩{int(penalty)}")
        elif cat in {"cont", "first"}:
            penalty = -10.0 if is_main_line else -18.0
            delta += penalty
            reasons.append(f"退潮日接力收缩{int(penalty)}")
        elif cat == "fresh":
            penalty = -6.0 if is_main_line else -12.0
            delta += penalty
            reasons.append(f"退潮日首板试错收缩{int(penalty)}")
    elif state_label == "冰点日":
        if cat == "wrap":
            delta += 12.0
            reasons.append("冰点日只保留超跌修复+12")
        else:
            delta -= 24.0
            reasons.append("冰点日非修复大幅降权-24")
    elif state_label == "轮动日" and strong_line_name:
        if not is_main_line and cat in {"first", "trend", "cont"}:
            delta -= 8.0
            reasons.append("轮动日偏离强主线-8")
        elif not is_main_line and cat == "fresh":
            delta -= 4.0
            reasons.append("轮动日非主线首板降权-4")
    elif state_label == "过渡日" and strong_line_name:
        if is_main_line and cat in {"first", "trend", "fresh", "wrap"}:
            delta += 8.0
            reasons.append(f"过渡日强主线{strong_line_name}+8")

    hit_rate = rec.get("calibrated_hit_rate")
    if isinstance(hit_rate, (int, float)):
        if hit_rate >= 30:
            delta += 6.0
            reasons.append(f"历史命中段{hit_rate:.0f}%+6")
        elif hit_rate < 12:
            delta -= 6.0
            reasons.append(f"历史命中段{hit_rate:.0f}%-6")

    return delta, reasons


def _limit_candidates_for_state(
    ranked: Dict[str, List[Dict[str, Any]]],
    compare_context: Dict[str, Any],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    state_label = str(compare_context.get("market_state_label") or "").strip()
    if state_label == "退潮日":
        limits = dict(_RETREAT_LIMITS)
        total_limit = _RETREAT_TOTAL_LIMIT
        reason = f"退潮日缩量：只保留反包修复、强主线低位机会和少量试错，最多{total_limit}只"
    elif state_label == "冰点日":
        limits = dict(_ICE_POINT_LIMITS)
        total_limit = _ICE_POINT_TOTAL_LIMIT
        reason = f"冰点日缩量：原则空仓，仅保留极少修复观察，最多{total_limit}只"
    else:
        return ranked, {"limited": False, "limit_reason": ""}

    per_category = {
        cat: list(ranked.get(cat, []))[: limits.get(cat, len(ranked.get(cat, [])))]
        for cat in _PREDICTION_CANDIDATE_KEYS
    }
    flat: List[Dict[str, Any]] = []
    for cat, rows in per_category.items():
        for rec in rows:
            flat.append({**rec, "rank_category": cat})
    flat.sort(
        key=lambda rec: (
            -_safe_float(rec.get("final_rank_score")),
            -_candidate_score(rec),
            str(rec.get("code") or ""),
        )
    )
    keep_keys = {
        (str(rec.get("rank_category") or ""), str(rec.get("code") or "").zfill(6))
        for rec in flat[:total_limit]
    }
    limited = {
        cat: [
            rec for rec in rows
            if (cat, str(rec.get("code") or "").zfill(6)) in keep_keys
        ]
        for cat, rows in per_category.items()
    }
    return limited, {"limited": True, "limit_reason": reason}


def _rank_and_limit_prediction_candidates(
    candidates_by_category: Dict[str, List[Dict[str, Any]]],
    compare_context: Dict[str, Any],
    *,
    theme_quality: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    theme_quality = theme_quality or {}
    ranked: Dict[str, List[Dict[str, Any]]] = {}
    before_counts: Dict[str, int] = {}
    for cat in _PREDICTION_CANDIDATE_KEYS:
        rows = list((candidates_by_category or {}).get(cat) or [])
        before_counts[cat] = len(rows)
        enriched: List[Dict[str, Any]] = []
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            rec = raw
            base = _candidate_score(rec)
            delta, reasons = _priority_adjustment(cat, rec, compare_context or {}, theme_quality)
            rec["final_rank_score"] = round(base + delta, 2)
            rec["final_rank_reasons"] = reasons
            rec["rank_category"] = cat
            enriched.append(rec)
        enriched.sort(
            key=lambda rec: (
                -_safe_float(rec.get("final_rank_score")),
                -_candidate_score(rec),
                str(rec.get("code") or ""),
            )
        )
        ranked[cat] = enriched

    ranked, limit_stats = _limit_candidates_for_state(ranked, compare_context or {})
    flat: List[Dict[str, Any]] = []
    for cat, rows in ranked.items():
        for rec in rows:
            flat.append(rec)
    flat.sort(
        key=lambda rec: (
            -_safe_float(rec.get("final_rank_score")),
            -_candidate_score(rec),
            str(rec.get("code") or ""),
        )
    )
    after_counts = {cat: len(ranked.get(cat, [])) for cat in _PREDICTION_CANDIDATE_KEYS}
    stats = {
        "before_counts": before_counts,
        "after_counts": after_counts,
        "before_total": sum(before_counts.values()),
        "after_total": sum(after_counts.values()),
        "top_priority_candidates": flat[:5],
        **limit_stats,
    }
    return ranked, stats


def _universe_codes_for_history_fill(fetcher: Any, log_fn: Optional[Callable[[str], None]]) -> List[str]:
    try:
        universe = fetcher.get_all_stocks(force_refresh=False)
    except TypeError:
        universe = fetcher.get_all_stocks()
    except Exception as exc:
        if log_fn:
            log_fn(f"涨停预测[历史模式]：读取股票池失败，无法自动补齐历史快照: {exc}")
        return []
    if universe is None or not isinstance(universe, pd.DataFrame) or universe.empty:
        return []
    code_col = "code" if "code" in universe.columns else "代码" if "代码" in universe.columns else ""
    if not code_col:
        return []
    codes = (
        universe[code_col]
        .astype(str)
        .str.strip()
        .str.zfill(6)
        .dropna()
        .tolist()
    )
    out: List[str] = []
    seen: set[str] = set()
    for code in codes:
        if not code or code in seen or is_bse_code(code):
            continue
        seen.add(code)
        out.append(code)
    return out


def _make_force_history_plan(fetcher: Any) -> Optional[Any]:
    source = str(getattr(fetcher, "_default_history_source", "auto") or "auto")
    try:
        return fetcher.build_history_request_plan(source=source, force_refresh=True)
    except Exception:
        return None


def _ensure_historical_spot_snapshot(
    trade_date: str,
    *,
    fetcher: Any,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """历史模式跨机器一致性：用在线历史日线补齐目标日全市场快照。"""
    import stock_store as _stock_store
    from stock_data import DaemonThreadPoolExecutor

    target_digits = _trade_date_digits(trade_date)
    target_dash = _trade_date_dash(trade_date)
    stats: Dict[str, Any] = {
        "target_date": target_digits,
        "universe_rows": 0,
        "initial_rows": 0,
        "initial_coverage": 0.0,
        "missing_before_fill": 0,
        "filled": 0,
        "failed": 0,
        "final_rows": 0,
        "final_coverage": 0.0,
        "missing_after_fill": 0,
    }

    spot_df = _drop_bse_rows(_stock_store.load_spot_snapshot_at(target_digits or trade_date))
    existing_codes = _valid_spot_codes(spot_df)
    stats["initial_rows"] = len(existing_codes)

    universe_codes = _universe_codes_for_history_fill(fetcher, log_fn)
    stats["universe_rows"] = len(universe_codes)
    if not universe_codes:
        return spot_df, stats

    missing_codes = [code for code in universe_codes if code not in existing_codes]
    stats["initial_coverage"] = len(existing_codes) / len(universe_codes)
    stats["missing_before_fill"] = len(missing_codes)
    if not missing_codes:
        stats["final_rows"] = len(existing_codes)
        stats["final_coverage"] = stats["initial_coverage"]
        return spot_df, stats

    if log_fn:
        log_fn(
            f"涨停预测[历史模式]：{target_digits} 本机历史快照覆盖 "
            f"{len(existing_codes)}/{len(universe_codes)}，在线补齐缺失 {len(missing_codes)} 只..."
        )

    request_plan = _make_force_history_plan(fetcher)
    try:
        history_limit = int(fetcher.history_request_concurrency_limit())
    except Exception:
        history_limit = 2
    workers = max(1, min(history_limit, 4, len(missing_codes)))
    completed = 0
    filled = 0
    failed = 0

    def _fetch_one(code: str) -> bool:
        df = fetcher.get_history_data(
            code,
            days=120,
            force_refresh=True,
            request_plan=request_plan,
            as_of_trade_date=target_digits,
        )
        if df is None or getattr(df, "empty", True) or "date" not in df.columns:
            return False
        dates = (
            df["date"].astype(str)
            .str.replace("/", "-", regex=False)
            .str.replace(".", "-", regex=False)
        )
        return bool((dates == target_dash).any())

    executor = DaemonThreadPoolExecutor(max_workers=workers, thread_name_prefix="hist-spot-fill")
    try:
        futures = {executor.submit(_fetch_one, code): code for code in missing_codes}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                ok = bool(fut.result())
            except Exception as exc:
                ok = False
                logger.debug("历史快照补齐 %s 失败: %s", code, exc)
            completed += 1
            if ok:
                filled += 1
            else:
                failed += 1
            if progress_callback and (completed == 1 or completed % 20 == 0 or completed == len(missing_codes)):
                progress_callback(completed, len(missing_codes), f"历史快照补齐 {code}")
    finally:
        executor.shutdown(wait=True)

    stats["filled"] = filled
    stats["failed"] = failed
    spot_df = _drop_bse_rows(_stock_store.load_spot_snapshot_at(target_digits or trade_date))
    final_codes = _valid_spot_codes(spot_df)
    stats["final_rows"] = len(final_codes)
    stats["missing_after_fill"] = max(0, len(universe_codes) - len(final_codes))
    stats["final_coverage"] = len(final_codes) / len(universe_codes)
    if log_fn:
        log_fn(
            f"涨停预测[历史模式]：{target_digits} 历史快照补齐完成，"
            f"新增成功 {filled} 只，失败 {failed} 只，最终覆盖 "
            f"{len(final_codes)}/{len(universe_codes)}。"
        )
    return spot_df, stats


def _is_new_stock_history(history: pd.DataFrame, today: datetime) -> bool:
    """K 线行数不足 10 但最早 K 线日期在最近 30 个自然日内 → 视为新股。

    用 history 自己最早日期判定，不依赖外部上市日期接口，避免每只票多一次网络。
    """
    if history is None or history.empty or "date" not in history.columns:
        return False
    try:
        earliest = str(history["date"].astype(str).iloc[0]).strip()
    except Exception:
        return False
    if not earliest:
        return False
    norm = earliest.replace("/", "-").replace(".", "-")
    if len(norm) == 8 and norm.isdigit():
        norm = f"{norm[:4]}-{norm[4:6]}-{norm[6:8]}"
    try:
        earliest_dt = datetime.strptime(norm[:10], "%Y-%m-%d")
    except Exception:
        return False
    return (today - earliest_dt).days <= 30


def _load_cached_history_for_prereq(code: str, limit: int = 10) -> Optional[pd.DataFrame]:
    """Read raw SQLite history for prerequisite checks.

    StockDataFetcher intentionally returns None when cached rows are fewer than
    requested. The prereq check still needs those short rows to identify newly
    listed stocks and avoid blocking prediction incorrectly.
    """
    try:
        from stock_store import load_history

        return load_history(code, limit=limit)
    except Exception:
        return None


def _trim_history_as_of(history: Optional[pd.DataFrame], fetcher: Any) -> Optional[pd.DataFrame]:
    if history is None or history.empty or "date" not in history.columns:
        return history
    as_of = str(getattr(fetcher, "as_of_trade_date", "") or "").strip()
    if len(as_of) == 8 and as_of.isdigit():
        as_of = f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}"
    if not as_of:
        return history
    date_col = (
        history["date"].astype(str).str.replace("/", "-", regex=False).str.replace(".", "-", regex=False)
    )
    trimmed = history.loc[date_col <= as_of].copy()
    return trimmed.reset_index(drop=True)


def _check_prerequisites(
    *,
    historical_mode: bool,
    pool_source: str,
    concept_themes_count: int,
    industry_groups_count: int = 0,
    board_strength: Dict[str, Any],
    sentiment_degraded: bool,
    zt_codes: set,
    fetcher,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> List[str]:
    """硬校验所有预测必备数据是否就位。返回缺失项列表（空列表 = 通过）。

    用户原则：宁可不出预测，也别用兜底数据骗用户。任一项缺失，predict 直接
    返回"中止结果"，并把这份清单显示给用户去逐项修复。
    """
    missing: List[str] = []
    zt_codes = {
        str(code).strip().zfill(6)
        for code in (zt_codes or set())
        if str(code).strip() and not is_bse_code(str(code).strip().zfill(6))
    }

    # 1. 涨停池来源必须可信
    trusted_sources = {"cache_db", "eastmoney", "cache_memory"}
    if pool_source not in trusted_sources:
        missing.append(
            f"❌ 涨停池数据源不可信（当前 = {pool_source!r}）→ "
            f"请检查东财涨停池接口 / 网络 / 清缓存重试"
        )

    # 2. 概念炒作分析必须至少识别出细题材或行业主线。
    # 细题材缺失时不把行业伪装成题材，但行业主线仍可作为兜底参与预测。
    if concept_themes_count <= 0 and industry_groups_count <= 0:
        missing.append(
            "❌ 概念炒作分析未识别出题材 → "
            "可能原因：本地已缓存交易日的涨停池数据不足 / 概念库未刷新。"
            "请到「概念炒作」tab 检查是否能看到题材列表，"
            "若题材列表为空请先补足涨停池历史 + 刷新概念库"
        )

    # 3. 板块强度（仅实时模式必备；历史模式从合成 spot 兜底）
    if not historical_mode and not board_strength:
        missing.append(
            "❌ 板块强度（行业涨跌榜）数据缺失 → 请检查东财板块接口"
        )

    # 4. 市场情绪不得降级
    if sentiment_degraded:
        missing.append(
            "❌ 市场情绪数据降级（跌停池 / 上证指数未完整拉到）→ "
            "请检查网络 / akshare 接口，或点「刷新（强制重拉外部数据）」重试"
        )

    # 5. 所有涨停股个股历史 K 线 ≥ 10 行（cont scorer 必备）
    # 新股豁免：若 K 线不足 10 行但最早一根 K 线在最近 30 个自然日内，视为新股
    # （cont scorer 已自带 len(history) >= 10 守卫，新股进流程会自动降级评分而不会崩）
    missing_kline: List[str] = []
    new_stock_skipped: List[str] = []
    if zt_codes:
        try:
            request_plan = (
                build_local_cache_history_plan_fn(
                    reason="predict-prereq-check-cache-only",
                )
                if build_local_cache_history_plan_fn is not None
                else None
            )
        except Exception:
            request_plan = None
        today = datetime.now()
        for code in sorted(zt_codes):
            # 关键：这里只校验 ≥ 10 行；days 要直接传 10，否则 store_facade.load_history
            # 在 cache_only 模式下会因 min_rows 不达标而整体返回 None，把 60 行历史的票
            # 也误判成"K 线缺失"。
            try:
                history = fetcher.get_history_data(
                    code, days=10, force_refresh=False,
                    request_plan=request_plan,
                )
            except Exception:
                history = None
            if history is None or history.empty:
                history = _trim_history_as_of(_load_cached_history_for_prereq(code, 10), fetcher)
            if (history is None or getattr(history, "empty", True)) and not historical_mode:
                # 全新涨停股本地完全没缓存（0 行）：新股豁免至少要有 1 根 K 才能判定，
                # 否则会被当成"K 线缺失"硬中止整批预测。涨停池很小，这里对这几只 0 缓存的票
                # 定向补拉一次历史（走默认 auto 回退链），再回查缓存重判。
                # 历史/回测模式不补拉，避免拉到非 as-of 数据。
                if log_fn:
                    log_fn(f"涨停预测：{code} 本地无历史缓存，定向补拉一次…")
                try:
                    fetcher.get_history_data(code, days=60, force_refresh=True)
                except Exception:
                    pass
                history = _trim_history_as_of(_load_cached_history_for_prereq(code, 10), fetcher)
            if history is None or history.empty:
                missing_kline.append(code)
                continue
            if len(history) >= 10:
                continue
            if _is_new_stock_history(history, today):
                new_stock_skipped.append(code)
            else:
                missing_kline.append(code)
    if missing_kline:
        preview = "、".join(missing_kline[:10])
        tail = f" 等 {len(missing_kline)} 只" if len(missing_kline) > 10 else ""
        missing.append(
            f"❌ 个股历史 K 线不足 10 行: {preview}{tail} → "
            f"请在「K 线缓存」tab 用「批量补历史」更新这些票"
        )
    if new_stock_skipped and log_fn:
        preview = "、".join(new_stock_skipped[:10])
        tail = f" 等 {len(new_stock_skipped)} 只" if len(new_stock_skipped) > 10 else ""
        log_fn(f"涨停预测：识别到新股（K 线 < 10 行但最早 K 线在 30 日内），不阻断预测：{preview}{tail}")

    return missing


def predict_limit_up_candidates(
    trade_date: str,
    lookback_days: int = DEFAULT_PREDICT_LOOKBACK_DAYS,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    historical_mode: bool = False,
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    classify_limit_up_pattern_fn: Optional[Callable[..., Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """基于涨停对比 + 二波接力数据预测明日涨停候选。

    步骤：
    1. 回看最近 N 日涨停对比，统计昨日首板晋级率等环境数据
    2. 保留涨停候选：对今日涨停股按封板质量 + 近期晋级环境评分
    3. 二波接力候选：近期涨停过 + 今日已启动 + 收盘强势的接力候选

    返回字段沿用旧结构，便于 GUI 直接复用：
        profile: 兼容旧 UI，现固定为空
        continuation_candidates: 保留涨停/连板候选
        first_board_candidates: 二波接力候选（字段名沿用 first_board_*）
        hot_industries: 今日涨停行业分布
        summary: 文字摘要

    `historical_mode=True`：回测模式。spot_df 从 history 表合成（"as of 收盘"）
    而非实时快照；板块强度从合成 spot 按行业聚合涨跌幅。涨停池仍用
    get_limit_up_pool（其本身有 SQLite 缓存）。仅在需要"对任意历史日期回放
    预测"的批量回测场景下使用。

    迁自 StockFilter.predict_limit_up_candidates；回溯窗口统一按预测合同标准化。
    """
    lookback_days = normalize_predict_lookback(lookback_days)
    # 这里 import 是为了避免顶层 import 循环（stock_store / llm_theme_clustering 等老模块）
    from stock_store import (
        load_all_limit_up_industries,
        save_last_limit_up_prediction,
        save_limit_up_prediction_record,
    )
    from stock_data import DaemonThreadPoolExecutor

    # ===== auto-promote historical_mode =====
    # 若 trade_date 早于今日，强制切到历史模式。原因：实时 spot / 板块强度
    # 等接口都不带 date 参数，永远返回"当前时刻"，跨天调用会拉到错误
    # 日期的盘中数据，导致两台机器（或同一台机器不同时间）跑同一历史日期
    # 结果不一致。auto-promote 后历史 spot 从本地 history 表合成，结果稳定。
    if not historical_mode and trade_date:
        today_key = datetime.now().strftime("%Y%m%d")
        td_digits = str(trade_date).strip().replace("-", "").replace("/", "")
        if td_digits and td_digits.isdigit() and len(td_digits) == 8 \
                and td_digits < today_key:
            historical_mode = True
            if log_fn:
                log_fn(
                    f"涨停预测：trade_date={trade_date} ≠ 今日({today_key})，"
                    f"自动切到历史模式（spot 从本地 history 合成，板块强度走"
                    f"合成 spot 兜底，保证两台机器结果一致）"
                )
    if progress_callback:
        progress_callback(0, 1, "统计最近涨停对比环境...")

    # 数据健康度收集：每个数据源在哪个分支被读到、是否降级，全部记一笔
    # 用户看 UI 时能直接判断"这次预测哪些维度是真数据、哪些是 fallback"
    data_quality: Dict[str, Any] = {
        "historical_mode": bool(historical_mode),
        "trade_date": trade_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "spot": {"rows": 0, "industry_missing": 0, "source": "none"},
        "limit_up_pool": {"rows": 0, "source": "none"},
        "themes": {"loaded": False, "themes": 0, "covered_codes": 0},
        "board_strength": {"loaded": False, "rows": 0},
        "sentiment": {"loaded": False, "score": None, "degraded": False},
        "relative_strength": {"loaded": False, "benchmarks": 0, "warnings": []},
        "warnings": [],
        "timing_hint": _compute_timing_hint(trade_date, historical_mode),
    }
    if data_quality["timing_hint"] and log_fn:
        log_fn(f"涨停预测：{data_quality['timing_hint']}")

    profile: Dict[str, Any] = {}
    feature_samples: List[Dict[str, Any]] = []
    compare_context = build_compare_market_context(trade_date, lookback_days, fetcher=fetcher)
    scoring_fetcher = (
        _AsOfHistoryFetcher(fetcher, trade_date)
        if historical_mode
        else fetcher
    )
    try:
        from src.services.relative_strength_service import build_relative_strength_context
        relative_strength_context = build_relative_strength_context(
            trade_date,
            log_fn=log_fn,
        )
    except Exception as exc:
        logger.debug("加载相对指数强弱数据失败: %s", exc)
        relative_strength_context = {
            "relative_strength_index_history": {},
            "relative_strength_index_meta": {},
            "relative_strength_warnings": [f"指数历史加载失败，强弱因子已跳过: {exc}"],
        }
    compare_context.update(relative_strength_context)
    relative_strength_histories = (
        relative_strength_context.get("relative_strength_index_history") or {}
    )
    relative_strength_warnings = [
        str(x) for x in (relative_strength_context.get("relative_strength_warnings") or [])
        if str(x).strip()
    ]
    data_quality["relative_strength"] = {
        "loaded": bool(relative_strength_histories),
        "benchmarks": len(relative_strength_histories),
        "warnings": relative_strength_warnings,
    }
    for warning in relative_strength_warnings:
        data_quality["warnings"].append(warning)
    if log_fn:
        if relative_strength_histories:
            names = {
                "sh000001": "上证",
                "sz399001": "深成指",
            }
            loaded = " / ".join(
                names.get(symbol, symbol) for symbol in relative_strength_histories
            )
            log_fn(f"涨停预测：相对指数强弱数据已加载（{loaded}）")
        else:
            log_fn("涨停预测：相对指数强弱数据不可用，强弱因子将显示为空")

    # 阶段2：获取今日涨停池 + 全市场行情
    if log_fn:
        log_fn(f"涨停预测：阶段2 - 获取 {trade_date} 涨停池 + 全市场行情...")
    if progress_callback:
        progress_callback(0, 1, "获取今日涨停池...")

    # 并行获取涨停池和全市场行情快照
    today_pool_df: Optional[pd.DataFrame] = None
    spot_df: Optional[pd.DataFrame] = None
    zt_codes: set = set()

    def _fetch_pool():
        nonlocal today_pool_df
        today_pool_df = fetcher.get_limit_up_pool(trade_date)

    if historical_mode:
        # 历史模式：spot_df 从目标日历史日线合成。为保证跨电脑一致，
        # 本机缺目标日 history 行时先按 as-of 日期在线补齐，再合成快照。
        try:
            spot_df, fill_stats = _ensure_historical_spot_snapshot(
                trade_date,
                fetcher=fetcher,
                progress_callback=progress_callback,
                log_fn=log_fn,
            )
            raw_cnt = 0 if spot_df is None else len(spot_df)
            spot_df = _drop_bse_rows(spot_df)
            cnt = 0 if spot_df is None else len(spot_df)
            data_quality["spot"]["source"] = "historical_online_filled" if int(fill_stats.get("filled") or 0) else "local_history"
            data_quality["spot"]["rows"] = int(cnt)
            data_quality["spot"]["industry_missing"] = _count_missing_industries(spot_df)
            data_quality["spot"]["coverage"] = {
                "universe_rows": int(fill_stats.get("universe_rows") or 0),
                "initial_rows": int(fill_stats.get("initial_rows") or 0),
                "initial_coverage": float(fill_stats.get("initial_coverage") or 0.0),
                "missing_before_fill": int(fill_stats.get("missing_before_fill") or 0),
                "filled": int(fill_stats.get("filled") or 0),
                "failed": int(fill_stats.get("failed") or 0),
                "final_rows": int(fill_stats.get("final_rows") or cnt),
                "final_coverage": float(fill_stats.get("final_coverage") or 0.0),
                "missing_after_fill": int(fill_stats.get("missing_after_fill") or 0),
            }
            if cnt == 0:
                data_quality["warnings"].append(
                    f"历史 spot 为空：本地 history 表无 {trade_date} 数据，"
                    f"且在线补齐未成功，首板候选将无法筛选"
                )
            elif int(fill_stats.get("missing_after_fill") or 0) > 0:
                data_quality["warnings"].append(
                    f"历史 spot 覆盖不完整：{trade_date} 最终覆盖 "
                    f"{int(fill_stats.get('final_rows') or cnt)}/"
                    f"{int(fill_stats.get('universe_rows') or 0)}，"
                    f"仍缺 {int(fill_stats.get('missing_after_fill') or 0)} 只"
                )
            if log_fn:
                log_fn(f"涨停预测[历史模式]：从 history 合成 spot 快照 {cnt} 行")
                dropped = raw_cnt - cnt
                if dropped > 0:
                    log_fn(f"涨停预测[历史模式]：已过滤北交所 spot {dropped} 行")
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：合成 spot 失败: {e}")
            spot_df = None
            data_quality["warnings"].append(f"历史 spot 合成异常: {e}")
        # 涨停池仍然走 get_limit_up_pool —— 本地 SQLite 命中即可，未命中才联网
        try:
            _fetch_pool()
        except Exception as e:
            if log_fn:
                log_fn(f"涨停预测[历史模式]：获取涨停池失败: {e}")
            data_quality["warnings"].append(f"涨停池拉取失败: {e}")
    else:
        def _fetch_spot():
            nonlocal spot_df
            spot_df = _first_board.fetch_spot_snapshot(log_fn=log_fn)

        # 使用线程池并行获取两个数据源。这里不能用 `with`，否则退出上下文时会 wait=True，
        # 即使 result(timeout=...) 超时了，仍然会继续等待后台任务跑完。
        executor = DaemonThreadPoolExecutor(max_workers=2, thread_name_prefix="stage2")
        try:
            future_pool = executor.submit(_fetch_pool)
            future_spot = executor.submit(_fetch_spot)

            try:
                # 涨停池最多 15 秒（底层 _ashare_request_with_retry 有 20s deadline 兜底）
                future_pool.result(timeout=15.0)
            except FutureTimeoutError as e:
                if log_fn:
                    log_fn(f"涨停预测：获取涨停池超时 (get_limit_up_pool): {e}")
            except Exception as e:
                if log_fn:
                    log_fn(f"涨停预测：获取涨停池失败 (get_limit_up_pool): {e}")

            try:
                # 全市场行情上限 60s：东财快路径 5s 内 return；
                # 东财 RST/熔断时，需要给"东财重试 ~15s + 新浪 ~30s"留足时间，否则
                # 主线程会在新浪刚开始时就超时放弃，导致首板候选筛选每次都被跳过。
                future_spot.result(timeout=60.0)
            except FutureTimeoutError as e:
                if log_fn:
                    log_fn(f"涨停预测：获取全市场行情超时 (5000+只股票): {e}")
                    log_fn("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
            except Exception as e:
                if log_fn:
                    log_fn(f"涨停预测：获取全市场行情失败: {e}")
                    log_fn("涨停预测：将跳过首板候选筛选，继续执行连板延续分析")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        raw_spot_cnt = 0 if spot_df is None else len(spot_df)
        spot_df = _drop_bse_rows(spot_df)

        # 记一笔实时 spot 行数（实时模式下行业字段由源接口直接给出）
        if spot_df is not None:
            data_quality["spot"]["source"] = "realtime"
            data_quality["spot"]["rows"] = int(len(spot_df))
            data_quality["spot"]["industry_missing"] = _count_missing_industries(spot_df)
            dropped = raw_spot_cnt - len(spot_df)
            if dropped > 0 and log_fn:
                log_fn(f"涨停预测：已过滤北交所 spot {dropped} 行")

    raw_pool_cnt = 0 if today_pool_df is None else len(today_pool_df)
    today_pool_df = _drop_bse_rows(today_pool_df)
    if raw_pool_cnt and today_pool_df is not None:
        dropped = raw_pool_cnt - len(today_pool_df)
        if dropped > 0 and log_fn:
            log_fn(f"涨停预测：已过滤北交所涨停池 {dropped} 行")

    if today_pool_df is None or today_pool_df.empty:
        non_trading = False
        try:
            from datetime import datetime as _dt2
            from src.utils.trade_calendar import _get_trade_calendar, _is_trading_day
            parsed = _dt2.strptime(str(trade_date).strip(), "%Y%m%d").date()
            non_trading = not _is_trading_day(parsed, _get_trade_calendar())
        except Exception:
            pass
        summary = (
            f"{trade_date} 非交易日（周末/节假日），无涨停池数据"
            if non_trading
            else f"{trade_date} 未获取到涨停池数据"
        )
        data_quality["warnings"].append(summary)
        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": [],
            "first_board_candidates": [],
            "fresh_first_board_candidates": [],
            "broken_board_wrap_candidates": [],
            "trend_limit_up_candidates": [],
            "hot_industries": {},
            "summary": summary,
            "data_quality": data_quality,
        }
        try:
            save_last_limit_up_prediction(result)
        except Exception:
            pass
        try:
            save_limit_up_prediction_record(result)
        except Exception:
            pass
        return result

    # 涨停池就绪 → 记录来源 + 行数
    data_quality["limit_up_pool"]["rows"] = int(len(today_pool_df))
    try:
        # fetcher 内部维护了 _last_pool_source[date]，标识 cache_memory / cache_db /
        # eastmoney / spot_fallback / empty，便于 UI 显示"涨停池数据来自哪里"。
        td_norm = fetcher._normalize_trade_date(trade_date)
        data_quality["limit_up_pool"]["source"] = (
            getattr(fetcher, "_last_pool_source", {}).get(td_norm, "unknown")
        )
    except Exception:
        data_quality["limit_up_pool"]["source"] = "unknown"

    all_pool_records = _shared.parse_full_pool(today_pool_df)
    if log_fn:
        log_fn(f"涨停预测：解析涨停池完成，共 {len(all_pool_records)} 只")
    hot_industries = _shared.count_pool_industries(today_pool_df)
    if log_fn:
        log_fn(f"涨停预测：统计热门行业完成，共 {len(hot_industries)} 个行业")

    if not today_pool_df.empty and "代码" in today_pool_df.columns:
        zt_codes = {
            code for code in today_pool_df["代码"].astype(str).str.strip().str.zfill(6)
            if not is_bse_code(code)
        }
        if log_fn:
            log_fn(f"涨停预测：提取涨停股代码 {len(zt_codes)} 只")

    # 阶段2.5：概念炒作分析（concept_hype 服务）
    # 这是题材维度的【唯一入口】：内部已合并三源（涨停池行业 + 概念库反查 +
    # LLM 题材缓存若有），同时给出题材阶段（萌芽/主升/末期/退潮）。
    # 不再单独调 llm_theme_clustering（涨停对比 tab 已下线，那条入口已 dead）。
    try:
        from src.services.concept_hype_service import analyze_concept_hype
        hype = analyze_concept_hype(trade_date, log=log_fn)
    except Exception as exc:
        logger.debug("加载概念炒作分析失败: %s", exc)
        hype = {}

    concepts = hype.get("concepts") or []
    code_industry_map: Dict[str, str] = {
        r["code"]: r.get("industry", "") for r in all_pool_records
    }
    code_theme_map: Dict[str, str] = {}        # code → 该票最具代表性的题材名
    theme_size_map: Dict[str, int] = {}        # 题材名 → 成员数
    industry_theme_heat: Dict[str, int] = {}   # 行业 → 关联到的最大题材规模
    code_to_phase: Dict[str, str] = {}         # code → 该票所在题材的最佳阶段
    phase_priority = {"萌芽": 4, "主升": 3, "末期": 2, "退潮": 1}

    for c in concepts:
        try:
            name = str(c.get("name") or "").strip()
            source = str(c.get("source") or "").strip()
            phase = str(c.get("phase") or "")
            members = c.get("members") or []
            codes_in_theme = [
                str(m.get("code") or "").strip().zfill(6)
                for m in members if m.get("code")
            ]
            codes_in_theme = [x for x in codes_in_theme if x]
        except Exception:
            continue
        if not name or source == "行业" or len(codes_in_theme) < 2:
            continue
        size = len(codes_in_theme)
        # 题材规模取最大命中（同名题材跨多 source 时合并）
        if theme_size_map.get(name, 0) < size:
            theme_size_map[name] = size
        inds_in_theme: set = set()
        for code in codes_in_theme:
            # code → theme：选 size 较大的题材作为该票代表
            existing = code_theme_map.get(code)
            if not existing or theme_size_map.get(existing, 0) < size:
                code_theme_map[code] = name
            # code → phase：萌芽/主升 优先
            existing_phase = code_to_phase.get(code)
            if (not existing_phase
                    or phase_priority.get(phase, 0)
                    > phase_priority.get(existing_phase, 0)):
                code_to_phase[code] = phase
            ind = code_industry_map.get(code) or ""
            if ind:
                inds_in_theme.add(ind)
        for ind in inds_in_theme:
            if industry_theme_heat.get(ind, 0) < size:
                industry_theme_heat[ind] = size

    # 把题材信息塞进 compare_context，所有 scorer 共用
    compare_context["industry_theme_heat"] = industry_theme_heat
    compare_context["code_theme_map"] = code_theme_map
    compare_context["theme_size_map"] = theme_size_map
    compare_context["code_to_concept_phase"] = code_to_phase
    compare_context["concept_hype_topics"] = _compact_concept_hype_topics(concepts)
    strong_main_line = _select_strong_main_line(concepts)
    if strong_main_line:
        compare_context["strong_main_line"] = strong_main_line
    declining_main_lines = _select_declining_main_lines(concepts)
    if declining_main_lines:
        compare_context["declining_main_lines"] = declining_main_lines

    try:
        from src.services.theme_fund_service import build_theme_fund_context
        theme_fund_context = build_theme_fund_context(
            concepts,
            fetcher=scoring_fetcher,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        )
    except Exception as exc:
        logger.debug("计算题材资金潜伏/爆发失败: %s", exc)
        theme_fund_context = {}
        data_quality["warnings"].append(f"题材资金潜伏/爆发评分失败: {exc}")
    compare_context.update(theme_fund_context)

    real_concepts = [
        c for c in concepts
        if isinstance(c, dict) and str(c.get("source") or "").strip() != "行业"
    ]
    industry_concepts_count = max(0, len(concepts) - len(real_concepts))
    hype_stats = hype.get("stats") or {}
    theme_lookback_label = str(
        hype.get("lookback_label") or hype_stats.get("lookback_label") or ""
    ).strip()
    try:
        theme_lookback_days = int(
            hype.get("lookback_days")
            or hype_stats.get("lookback_days")
            or len(hype.get("trade_dates") or [])
            or 0
        )
    except (TypeError, ValueError):
        theme_lookback_days = 0
    theme_window_start = str(hype.get("start_date") or "").strip()
    theme_window_end = str(hype.get("end_date") or "").strip()
    theme_lookback_mode = str(
        hype.get("lookback_mode") or hype_stats.get("lookback_mode") or ""
    ).strip()
    data_quality["themes"]["loaded"] = bool(real_concepts)
    data_quality["themes"]["themes"] = len(real_concepts)
    data_quality["themes"]["industry_groups"] = industry_concepts_count
    data_quality["themes"]["covered_codes"] = len(code_theme_map)
    data_quality["themes"]["source"] = "concept_hype"
    data_quality["themes"]["lookback_label"] = theme_lookback_label
    data_quality["themes"]["lookback_days"] = theme_lookback_days
    data_quality["themes"]["lookback_mode"] = theme_lookback_mode
    data_quality["themes"]["start_date"] = theme_window_start
    data_quality["themes"]["end_date"] = theme_window_end
    data_quality["themes"]["concept_pairs"] = int(hype_stats.get("concept_pairs") or 0)
    data_quality["themes"]["concept_covered_codes"] = int(
        hype_stats.get("concept_covered_codes") or 0
    )
    data_quality["themes"]["llm_cache_days"] = int(hype_stats.get("llm_cache_days") or 0)
    theme_quality = _build_theme_data_quality(
        hype_stats=hype_stats,
        real_concepts=real_concepts,
        industry_concepts_count=industry_concepts_count,
        code_theme_map=code_theme_map,
    )
    data_quality["themes"].update(theme_quality)
    compare_context["theme_data_quality"] = theme_quality
    data_quality["themes"]["fund_themes"] = len(
        theme_fund_context.get("theme_fund_score_map") or {}
    )
    data_quality["themes"]["sentiment_delta"] = int(
        theme_fund_context.get("theme_sentiment_delta") or 0
    )
    data_quality["themes"]["industry_fallback"] = (
        theme_quality.get("quality_level") == "industry_fallback"
    )
    data_quality["themes"]["declining_main_lines"] = len(declining_main_lines)
    if theme_lookback_label:
        compare_context["theme_lookback_label"] = theme_lookback_label
        compare_context["theme_lookback_days"] = theme_lookback_days
        compare_context["theme_window_start"] = theme_window_start
        compare_context["theme_window_end"] = theme_window_end
        compare_context["theme_lookback_mode"] = theme_lookback_mode
    if theme_quality.get("warning"):
        data_quality["warnings"].append(str(theme_quality["warning"]))

    if log_fn:
        if theme_lookback_label:
            window_text = (
                f"，窗口 {theme_window_start}~{theme_window_end}"
                if theme_window_start and theme_window_end
                else ""
            )
            log_fn(f"涨停预测：题材判断周期 {theme_lookback_label}{window_text}")
        if real_concepts:
            log_fn(
                f"涨停预测：概念炒作识别 {len(real_concepts)} 个真实题材，"
                f"覆盖 {len(code_theme_map)} 只涨停股 / "
                f"{len(industry_theme_heat)} 个行业 / "
                f"阶段映射 {len(code_to_phase)} 只 "
                f"(萌芽 {sum(1 for v in code_to_phase.values() if v == '萌芽')} / "
                f"主升 {sum(1 for v in code_to_phase.values() if v == '主升')} / "
                f"末期 {sum(1 for v in code_to_phase.values() if v == '末期')} / "
                f"退潮 {sum(1 for v in code_to_phase.values() if v == '退潮')})"
            )
        elif concepts:
            log_fn(
                f"涨停预测：概念炒作仅识别到 {industry_concepts_count} 个行业主线，"
                "未命中概念库/LLM细题材；预测将使用行业主线兜底，题材列不会用行业名填充"
            )
        else:
            log_fn(
                "涨停预测：概念炒作分析未识别出题材"
                "（本地已缓存 limit_up_pool 数据可能不足）"
            )
        if strong_main_line:
            log_fn(
                "涨停预测：持续主线 "
                f"{strong_main_line.get('name')}({strong_main_line.get('source')}) "
                f"{strong_main_line.get('phase')}，今日 "
                f"{strong_main_line.get('today_count')} 只，活跃 "
                f"{strong_main_line.get('active_days')} 日"
            )
        if declining_main_lines:
            preview = "、".join(
                f"{x.get('name')}({x.get('phase') or x.get('trend')})"
                for x in declining_main_lines[:3]
            )
            log_fn(f"涨停预测：衰退主线警示 {preview}")

    # 阶段2.6：加载板块强度（失败不影响预测）
    # 板块强度 fallback 链：
    #   - 历史模式：东财历史日 K → 同花顺历史日 K → 合成 spot 按行业聚合
    #   - 实时模式：东财行业列表 → 同花顺当日日 K → 合成 spot 按行业聚合
    #   THS 命名跟东财不一致，下游 lookup miss 一部分，但聊胜于无
    if log_fn:
        log_fn("涨停预测：正在加载板块强度...")

    if historical_mode:
        try:
            board_strength = _first_board.load_industry_board_strength_for_date(
                trade_date, log_fn=log_fn,
            )
        except Exception as exc:
            logger.debug("历史行业板块强度（东财）拉取异常: %s", exc)
            board_strength = {}
        if not board_strength:
            try:
                board_strength = (
                    _first_board.load_industry_board_strength_for_date_ths(
                        trade_date, log_fn=log_fn,
                    )
                )
                if board_strength and log_fn:
                    log_fn(
                        f"涨停预测[历史模式]：东财死 → 同花顺行业 K 线 "
                        f"{len(board_strength)} 个板块（命名跟 EM 不一致，部分 lookup 会 miss）"
                    )
            except Exception as exc:
                logger.debug("历史行业板块强度（同花顺）拉取异常: %s", exc)
        if not board_strength:
            board_strength = _derive_board_strength_from_spot(spot_df)
            if log_fn:
                log_fn(
                    f"涨停预测[历史模式]：东财 + THS 均空 → "
                    f"合成 spot 兜底聚合 {len(board_strength)} 个板块"
                )
        elif log_fn and not board_strength.get("__source_already_logged__"):
            log_fn(
                f"涨停预测[历史模式]：板块强度共 {len(board_strength)} 个板块"
            )
    else:
        try:
            board_strength = _first_board.load_industry_board_strength(log_fn=log_fn)
        except Exception as exc:
            logger.debug("板块涨跌幅（东财）拉取异常: %s", exc)
            board_strength = {}
        if not board_strength:
            # 实时 THS：用今日做 trade_date
            from datetime import datetime as _dt_now
            today_key = _dt_now.now().strftime("%Y%m%d")
            try:
                board_strength = (
                    _first_board.load_industry_board_strength_for_date_ths(
                        today_key, log_fn=log_fn,
                    )
                )
                if board_strength and log_fn:
                    log_fn(
                        f"涨停预测：东财板块强度空 → 同花顺当日 K "
                        f"{len(board_strength)} 个板块（命名跟 EM 不一致，部分 lookup 会 miss）"
                    )
            except Exception as exc:
                logger.debug("板块涨跌幅（同花顺）拉取异常: %s", exc)
        if not board_strength:
            board_strength = _derive_board_strength_from_spot(spot_df)
            if log_fn:
                log_fn(
                    f"涨停预测：东财 + THS 均空 → "
                    f"合成 spot 兜底聚合 {len(board_strength)} 个板块"
                )

    data_quality["board_strength"]["loaded"] = bool(board_strength)
    data_quality["board_strength"]["rows"] = len(board_strength)

    compare_context["board_strength"] = board_strength
    if log_fn:
        top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
        board_summary = "、".join(f"{k}({v:.1f}%)" for k, v in top_boards)
        log_fn(f"涨停预测：板块强弱榜 TOP5 {board_summary}")

    # 市场情绪评分（供二波接力等评分调节，冰点情绪下降权）
    try:
        from src.services.market_sentiment_service import analyze_market_sentiment
        sent = analyze_market_sentiment(
            trade_date, fetch_external=True, log=log_fn,
        )
        _apply_market_sentiment_context(compare_context, data_quality, sent)
        if log_fn:
            state_label = compare_context.get("market_state_label", "")
            strategy_label = (compare_context.get("market_state_strategy") or {}).get("label", "")
            state_part = f"；状态 {state_label} → {strategy_label}" if state_label else ""
            log_fn(
                f"涨停预测：市场情绪 {compare_context['sentiment_score']}/100"
                f"（基础{compare_context['sentiment_base_score']}, "
                f"题材{int(compare_context.get('theme_sentiment_delta') or 0):+d}）"
                f" → {compare_context['sentiment_label']}"
                f"{state_part}"
            )
    except Exception as exc:
        logger.debug("接入市场情绪评分失败: %s", exc)
        compare_context["sentiment_score"] = 50
        data_quality["sentiment"]["degraded"] = True
        data_quality["warnings"].append(f"市场情绪评分失败: {exc}")

    # ============== 预测前置硬校验 ==============
    # 所有必备数据必须就位才允许预测。任一项缺失 → 直接中止 + 列出待修复清单。
    # 原则：宁可不出预测，也不让用户被兜底数据骗。
    prereq_missing = _check_prerequisites(
        historical_mode=historical_mode,
        pool_source=data_quality["limit_up_pool"].get("source", "unknown"),
        concept_themes_count=int(data_quality["themes"].get("themes") or 0),
        industry_groups_count=int(data_quality["themes"].get("industry_groups") or 0),
        board_strength=board_strength,
        sentiment_degraded=bool(data_quality["sentiment"].get("degraded")),
        zt_codes=zt_codes,
        fetcher=scoring_fetcher,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        log_fn=log_fn,
    )
    if prereq_missing:
        data_quality["blocked"] = True
        data_quality["missing"] = prereq_missing
        if log_fn:
            log_fn(f"涨停预测：前置校验失败，中止预测（{len(prereq_missing)} 项待修复）")
            for m in prereq_missing:
                log_fn(f"  {m}")
        summary = (
            f"❌ 预测中止 — {trade_date} 数据未就位（{len(prereq_missing)} 项待修复）\n\n"
            + "\n\n".join(prereq_missing)
            + "\n\n修复后请重新点「开始预测」。"
        )
        result = {
            "trade_date": trade_date,
            "profile": profile,
            "profile_samples": feature_samples,
            "continuation_candidates": [],
            "first_board_candidates": [],
            "fresh_first_board_candidates": [],
            "broken_board_wrap_candidates": [],
            "trend_limit_up_candidates": [],
            "hot_industries": {},
            "compare_context": compare_context,
            "summary": summary,
            "data_quality": data_quality,
        }
        try:
            save_last_limit_up_prediction(result)
        except Exception:
            pass
        # 注意：中止结果不写入 prediction_record 历史表，避免污染历史
        return result

    # 龙头身份预算：同板块今日最高板数 + 持有该板数的代码集合
    # 让 cont 评分识别"板块独苗高板龙头"vs"高位跟风票"
    industry_max_boards: Dict[str, int] = {}
    industry_top_codes: Dict[str, set] = {}
    market_max_boards = 0
    market_top_codes: set = set()
    for r in all_pool_records:
        ind = str(r.get("industry") or "").strip()
        if not ind:
            continue
        try:
            b = int(r.get("consecutive_boards") or 1)
        except (TypeError, ValueError):
            b = 1
        code = str(r.get("code") or "").strip()
        if b > market_max_boards:
            market_max_boards = b
            market_top_codes = {code} if code else set()
        elif b == market_max_boards and code:
            market_top_codes.add(code)
        cur_max = industry_max_boards.get(ind, 0)
        if b > cur_max:
            industry_max_boards[ind] = b
            industry_top_codes[ind] = {code}
        elif b == cur_max:
            industry_top_codes.setdefault(ind, set()).add(code)
    compare_context["industry_max_boards"] = industry_max_boards
    compare_context["industry_top_codes"] = industry_top_codes
    compare_context["market_max_boards"] = market_max_boards
    compare_context["market_top_codes"] = market_top_codes

    # 资金接入型首板的板块联动：候选 spot 行业是 universe（证监会粗命名），跟涨停池
    # （东财窄命名）0% 对得上；limit_up_stock_meta 的 industry 与涨停池 100% 同命名，
    # 覆盖所有曾涨停过的票。建一份 {code: 东财行业} 供 fresh 把候选行业映射到涨停池命名。
    try:
        compare_context["em_industry_map"] = load_all_limit_up_industries()
    except Exception as exc:
        logger.debug("加载涨停股行业映射失败: %s", exc)
        compare_context["em_industry_map"] = {}

    # 题材阶段映射已在阶段 2.5（concept_hype）一并完成，无需重复调用。

    # 阶段3：统一预取所有需要的历史数据（一次搞定）
    if log_fn:
        log_fn("涨停预测：阶段3 - 统一预取历史数据...")

    # 收集所有需要历史数据的股票代码
    pool_codes = [r["code"] for r in all_pool_records]
    candidate_codes: List[str] = []
    if spot_df is not None and not spot_df.empty:
        if log_fn:
            log_fn(f"涨停预测：开始筛选强势股（全市场 {len(spot_df)} 只）...")
        strong = _first_board.filter_strong_stocks(spot_df, zt_codes)
        if log_fn:
            log_fn(f"涨停预测：筛选强势股完成，共 {len(strong)} 只")
        pullback = _first_board.filter_ma5_pullback_stocks(spot_df, zt_codes)
        if log_fn:
            log_fn(f"涨停预测：筛选回踩MA5完成，共 {len(pullback)} 只")
        seen = set()
        for rec in strong + pullback:
            if rec["code"] not in seen:
                seen.add(rec["code"])
                candidate_codes.append(rec["code"])
    else:
        if log_fn:
            log_fn("涨停预测：无全市场行情，跳过强势股筛选（首板候选将不可用）")

    all_codes = [
        code for code in set(pool_codes + candidate_codes)
        if code and not is_bse_code(str(code).strip().zfill(6))
    ]
    if log_fn:
        log_fn(f"涨停预测：统一预取 {len(all_codes)} 只股票历史数据"
               f"（涨停池{len(pool_codes)} + 候选{len(candidate_codes)}）")
    # 实时模式保持只读本地缓存；历史模式为了跨电脑回放一致，允许按 as-of 日期补齐候选历史。
    _classifiers.prefetch_history_for_pool(
        scoring_fetcher, all_codes, 65, progress_callback, not historical_mode, log_fn=log_fn,
    )
    if log_fn:
        log_fn("涨停预测：阶段3完成 - 历史数据预取结束")

    # 阶段4：保留涨停 / 连板延续候选评分
    if log_fn:
        log_fn(f"涨停预测：阶段4 - 分析 {len(all_pool_records)} 只涨停股的保留涨停潜力...")

    continuation_candidates = []
    for idx, rec in enumerate(all_pool_records):
        classify_fn = classify_limit_up_pattern_fn
        if historical_mode:
            classify_fn = (
                lambda stock_code, stock_name="", board="":
                _classifiers.classify_limit_up_pattern(
                    scoring_fetcher,
                    stock_code,
                    stock_name=stock_name,
                    board=board,
                    log_fn=log_fn,
                )
            )
        score_info = _cont.score_continuation_by_compare(
            rec, hot_industries, compare_context,
            fetcher=scoring_fetcher,
            log_fn=log_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            classify_limit_up_pattern_fn=classify_fn,
        )
        # 门槛 40→50：30 天数据显示 0-49 段命中仅 15.2%（n=277），50+ 段才有
        # 22.5% 区分度，进一步过滤减少 false positive
        if score_info["score"] >= 50:
            continuation_candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, len(all_pool_records),
                              f"保留涨停分析 {rec['code']} {rec.get('name', '')}")
    continuation_candidates.sort(key=lambda x: -x["score"])

    # 阶段5：二波接力候选（历史数据 + 行情都已缓存）
    if log_fn:
        log_fn("涨停预测：阶段5 - 识别二波接力候选...")

    first_board_candidates = _first.scan_followthrough_candidates_cached(
        hot_industries, spot_df, zt_codes, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        lookback_days=lookback_days,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_strong_stocks_fn=_first_board.filter_strong_stocks,
    )

    # 阶段6：首板涨停候选（最近 N 日未涨停、今日量价启动）
    if log_fn:
        log_fn("涨停预测：阶段6 - 识别首板涨停候选...")
    try:
        fresh_rules = _fresh_calibration.load_fresh_calibration_rules(
            lookback_dates=20, min_samples=20, success_field="hit_strict",
        )
    except Exception as exc:
        logger.debug("首板涨停校准规则加载失败: %s", exc)
        fresh_rules = {}
    compare_context["fresh_calibration_rules"] = fresh_rules
    if log_fn:
        eligible_rules = [
            item for item in fresh_rules.values()
            if float(item.get("rate") or 0.0) >= 10.0
        ]
        if eligible_rules:
            log_fn(
                f"涨停预测：首板 V2 严格涨停校准规则 {len(fresh_rules)} 条，"
                f"其中 10%+ 涨停高置信规则 {len(eligible_rules)} 条"
            )
        else:
            log_fn("涨停预测：首板 V2 严格涨停校准规则样本不足，回退原始分数排序")
    fresh_first_board_candidates = _fresh.scan_fresh_first_board_candidates_cached(
        spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_candidates_fn=_first_board.filter_capital_inflow_candidates,
    )

    # 阶段7：断板反包候选（近期涨停被打掉，今日逼近反包）
    if log_fn:
        log_fn("涨停预测：阶段7 - 识别断板反包候选...")
    broken_board_wrap_candidates = _wrap.scan_broken_board_wrap_candidates_cached(
        spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_wrap_candidate_stocks_fn=_first_board.filter_wrap_candidate_stocks,
    )
    popularity_stats = _popularity_rank_service.enrich_wrap_candidates_with_popularity(
        broken_board_wrap_candidates, trade_date, log_fn=log_fn,
    )
    data_quality["popularity_rank"] = {
        "source": _popularity_rank_service.EASTMONEY_STOCK_RANK_SOURCE,
        "mode": "per_stock_history_after_wrap",
        "target_trade_date": trade_date,
        **popularity_stats,
    }

    # 阶段8：趋势涨停候选（多头排列、稳健上行）
    if log_fn:
        log_fn("涨停预测：阶段8 - 识别趋势涨停候选...")
    trend_limit_up_candidates = _trend.scan_trend_limit_up_candidates_cached(
        spot_df, zt_codes, hot_industries, compare_context, progress_callback,
        fetcher=scoring_fetcher,
        log_fn=log_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        filter_strong_stocks_fn=_first_board.filter_strong_stocks,
        filter_ma5_pullback_stocks_fn=_first_board.filter_ma5_pullback_stocks,
    )

    ranked_candidates, candidate_priority = _rank_and_limit_prediction_candidates(
        {
            "cont": continuation_candidates,
            "first": first_board_candidates,
            "fresh": fresh_first_board_candidates,
            "wrap": broken_board_wrap_candidates,
            "trend": trend_limit_up_candidates,
        },
        compare_context,
        theme_quality=data_quality["themes"],
    )
    continuation_candidates = ranked_candidates["cont"]
    first_board_candidates = ranked_candidates["first"]
    fresh_first_board_candidates = ranked_candidates["fresh"]
    broken_board_wrap_candidates = ranked_candidates["wrap"]
    trend_limit_up_candidates = ranked_candidates["trend"]
    compare_context["candidate_priority"] = candidate_priority
    data_quality["candidate_priority"] = {
        "limited": bool(candidate_priority.get("limited")),
        "limit_reason": str(candidate_priority.get("limit_reason") or ""),
        "before_total": int(candidate_priority.get("before_total") or 0),
        "after_total": int(candidate_priority.get("after_total") or 0),
        "before_counts": dict(candidate_priority.get("before_counts") or {}),
        "after_counts": dict(candidate_priority.get("after_counts") or {}),
    }

    try:
        from src.services.prediction_theme_service import build_theme_prediction_groups
        theme_prediction = build_theme_prediction_groups(
            {
                "continuation_candidates": continuation_candidates,
                "first_board_candidates": first_board_candidates,
                "fresh_first_board_candidates": fresh_first_board_candidates,
                "broken_board_wrap_candidates": broken_board_wrap_candidates,
                "trend_limit_up_candidates": trend_limit_up_candidates,
            },
            hype_result=hype,
            compare_context=compare_context,
        )
    except Exception as exc:
        logger.debug("构建题材优先候选失败: %s", exc)
        theme_prediction = {
            "groups": [],
            "ungrouped": {},
            "role_order": [],
            "role_labels": {},
            "stats": {
                "theme_count": 0,
                "total_candidates": 0,
                "grouped_candidates": 0,
                "ungrouped_candidates": 0,
            },
        }

    category_counts = {
        "cont": len(continuation_candidates),
        "first": len(first_board_candidates),
        "fresh": len(fresh_first_board_candidates),
        "wrap": len(broken_board_wrap_candidates),
        "trend": len(trend_limit_up_candidates),
    }
    market_focus_advice = build_market_focus_advice(compare_context, category_counts)
    if market_focus_advice:
        compare_context["market_focus_advice"] = market_focus_advice

    # 摘要
    summary_lines = [
        f"预测日期：基于 {trade_date} 数据预测次日涨停候选",
        f"环境样本：最近 {compare_context.get('pair_count', 0)} 组首板晋级对比",
        f"今日涨停总数：{len(all_pool_records)} 只",
        f"保留涨停候选：{len(continuation_candidates)} 只（得分>=40）",
        f"二波接力候选：{len(first_board_candidates)} 只（得分>=50）",
        f"首板涨停候选：{len(fresh_first_board_candidates)} 只（5日未涨停，得分>=45）",
        f"反包候选：{len(broken_board_wrap_candidates)} 只（≥2 板涨停被打掉，T0 在 -10.5%~+3% 区间，得分>=70）",
        f"趋势涨停候选：{len(trend_limit_up_candidates)} 只（多头排列稳健上行，得分>=65）",
    ]
    if candidate_priority.get("limited"):
        summary_lines.append(
            f"候选缩量：{candidate_priority.get('limit_reason')}，"
            f"{int(candidate_priority.get('before_total') or 0)} → "
            f"{int(candidate_priority.get('after_total') or 0)} 只"
        )
    top_priority = candidate_priority.get("top_priority_candidates") or []
    if top_priority:
        parts = []
        for rec in top_priority[:5]:
            code = str(rec.get("code") or "").strip()
            name = str(rec.get("name") or "").strip()
            rank_score = _safe_float(rec.get("final_rank_score"))
            parts.append(f"{code}{name}({rank_score:.0f})")
        summary_lines.append(f"前排重点：{'、'.join(parts)}")
    latest_cont_rate = compare_context.get("latest_continuation_rate")
    avg_cont_rate = compare_context.get("avg_continuation_rate")
    if latest_cont_rate is not None:
        summary_lines.append(f"昨日首板最新晋级率：{latest_cont_rate:.1f}%")
    if avg_cont_rate is not None:
        summary_lines.append(f"近{compare_context.get('pair_count', 0)}组平均晋级率：{avg_cont_rate:.1f}%")
    theme_cycle_label = str(data_quality["themes"].get("lookback_label") or "").strip()
    if theme_cycle_label:
        theme_cycle_text = theme_cycle_label
        theme_start = str(data_quality["themes"].get("start_date") or "").strip()
        theme_end = str(data_quality["themes"].get("end_date") or "").strip()
        theme_days = int(data_quality["themes"].get("lookback_days") or 0)
        if theme_start and theme_end:
            theme_cycle_text += f"（{theme_start}~{theme_end}"
            if theme_days:
                theme_cycle_text += f"，实际{theme_days}日"
            theme_cycle_text += "）"
        summary_lines.append(f"题材判断周期：{theme_cycle_text}")
    theme_quality_warning = str(data_quality["themes"].get("warning") or "").strip()
    if theme_quality_warning:
        summary_lines.append(f"题材数据质量：{theme_quality_warning}")
    declining_main_lines = compare_context.get("declining_main_lines") or []
    if declining_main_lines:
        parts = []
        for item in declining_main_lines[:3]:
            name = str(item.get("name") or "").strip()
            phase = str(item.get("phase") or item.get("trend") or "转弱").strip()
            today_count = int(item.get("today_count") or 0)
            active_days = int(item.get("active_days") or 0)
            if name:
                parts.append(f"{name}({phase}，今{today_count}只，活跃{active_days}日)")
        if parts:
            summary_lines.append(f"主线衰退警示：{'、'.join(parts)}")
    if market_focus_advice:
        summary_lines.extend(format_market_focus_advice_lines(market_focus_advice))
    state_label = str(compare_context.get("market_state_label") or "").strip()
    if state_label:
        strategy_label = str(
            (compare_context.get("market_state_strategy") or {}).get("label") or ""
        ).strip()
        rotation_score = (compare_context.get("market_rotation") or {}).get("rotation_score")
        state_line = f"基础环境判断：{state_label}"
        if strategy_label:
            state_line += f" → {strategy_label}"
        if isinstance(rotation_score, (int, float)):
            state_line += f"（轮动分{int(rotation_score):+d}）"
        summary_lines.append(state_line)
    if hot_industries:
        top3 = sorted(hot_industries.items(), key=lambda x: -x[1])[:3]
        summary_lines.append(f"热门行业：{'、'.join(f'{k}({v})' for k, v in top3)}")
    if theme_size_map:
        top_themes = sorted(theme_size_map.items(), key=lambda x: -x[1])[:3]
        summary_lines.append(
            f"题材识别：{'、'.join(f'{k}({v}只)' for k, v in top_themes)}"
        )
    theme_fund_score_map = compare_context.get("theme_fund_score_map") or {}
    if theme_fund_score_map:
        top_fund_themes = sorted(
            theme_fund_score_map.items(), key=lambda x: -int(x[1])
        )[:3]
        summary_lines.append(
            "题材资金："
            + "、".join(f"{name}({int(score)})" for name, score in top_fund_themes)
        )
    strong_main_line = compare_context.get("strong_main_line") or {}
    if strong_main_line and not theme_size_map:
        summary_lines.append(
            "行业主线："
            f"{strong_main_line.get('name')}("
            f"{strong_main_line.get('phase')}，"
            f"今{int(strong_main_line.get('today_count') or 0)}只，"
            f"活跃{int(strong_main_line.get('active_days') or 0)}日)"
        )
    theme_groups = theme_prediction.get("groups") or []
    if theme_groups:
        role_labels = theme_prediction.get("role_labels") or {}
        role_order = theme_prediction.get("role_order") or []
        parts = []
        for group in theme_groups[:3]:
            role_parts = []
            for role in role_order:
                cnt = int((group.get("counts") or {}).get(role) or 0)
                if cnt:
                    role_parts.append(f"{role_labels.get(role, role)}{cnt}")
            role_text = "/".join(role_parts) or "候选0"
            parts.append(f"{group.get('name')}({role_text})")
        summary_lines.append(f"主线题材候选：{'、'.join(parts)}")
    if board_strength:
        top_boards = sorted(board_strength.items(), key=lambda x: -x[1])[:5]
        summary_lines.append(
            f"强势板块 TOP5：{'、'.join(f'{k}({v:+.1f}%)' for k, v in top_boards)}"
        )

    result = {
        "trade_date": trade_date,
        "profile": profile,
        "profile_samples": feature_samples,
        "continuation_candidates": continuation_candidates,
        "first_board_candidates": first_board_candidates,
        "fresh_first_board_candidates": fresh_first_board_candidates,
        "broken_board_wrap_candidates": broken_board_wrap_candidates,
        "trend_limit_up_candidates": trend_limit_up_candidates,
        "hot_industries": hot_industries,
        "compare_context": compare_context,
        "concept_hype_result": hype,
        "theme_prediction": theme_prediction,
        "market_focus_advice": market_focus_advice,
        "summary": "\n".join(summary_lines),
        "data_quality": data_quality,
    }
    try:
        save_last_limit_up_prediction(result)
    except Exception:
        pass
    try:
        save_limit_up_prediction_record(result)
    except Exception:
        pass
    return result
