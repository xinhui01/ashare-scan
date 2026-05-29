"""趋势涨停（trend）评分。

2 个模块级函数（参数注入模式）：
- scan_trend_limit_up_candidates_cached: 从强势股池 + MA5 回踩股池扫候选并逐只评分
- score_trend_limit_up: 主评分（多头排列 + MA20 抬头 + 60 日位置过滤）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_pct_fn / build_local_cache_history_plan_fn /
filter_strong_stocks_fn / filter_ma5_pullback_stocks_fn。

设计说明：趋势分支与"首板涨停候选"区别：
- 首板候选要求最近 10 日无涨停（冷启动），关注从沉寂到爆发
- 趋势候选关注已经在多头排列里"温和上攻"的票，可能有过近期涨停
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


def scan_trend_limit_up_candidates_cached(
    spot_df: Optional[pd.DataFrame],
    zt_codes: set,
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    filter_strong_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    filter_ma5_pullback_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """识别"趋势涨停"候选：均线多头排列、稳健上行的票，明日有望趋势加速涨停。

    迁自 StockFilter._scan_trend_limit_up_candidates_cached；行为零变化。
    """
    if spot_df is None or spot_df.empty:
        return []

    if filter_strong_stocks_fn is None or filter_ma5_pullback_stocks_fn is None:
        # 没有注入筛选函数时无法继续；保持原行为（原方法必然依赖 self._filter_*）
        return []

    seen: set = set()
    merged: List[Dict[str, Any]] = []
    for rec in filter_strong_stocks_fn(spot_df, zt_codes):
        if rec["code"] in seen:
            continue
        seen.add(rec["code"])
        merged.append(rec)
    for rec in filter_ma5_pullback_stocks_fn(spot_df, zt_codes):
        if rec["code"] in seen:
            continue
        seen.add(rec["code"])
        merged.append(rec)
    if not merged:
        return []

    candidates: List[Dict[str, Any]] = []
    total = len(merged)
    for idx, rec in enumerate(merged):
        score_info = score_trend_limit_up(
            rec, hot_industries, compare_context,
            fetcher=fetcher,
            log_fn=log_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        )
        # 门槛从 50 提到 65：30 天数据显示 50-60 分段命中率仅 5.9%（n=222）几乎没有
        # 区分度；65+ 才能把"次日实质上行"的标的过滤出来
        if score_info is not None and score_info["score"] >= 65:
            candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, total, f"趋势筛选 {rec['code']} {rec.get('name', '')}")

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:50]


def score_trend_limit_up(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    """对趋势涨停候选评分。

    触发条件（强制）：
    1. MA5 > MA10 > MA20 多头排列
    2. 今日收盘 ≥ MA5（在趋势之上）
    3. MA20 5 日斜率 > 0（中期趋势抬头）
    4. 60 日位置 40~92（中位偏上、避开极顶）

    迁自 StockFilter._score_trend_limit_up；行为零变化。
    """
    code = rec["code"]
    name = rec.get("name", "")
    change_pct = rec.get("change_pct")
    turnover = rec.get("turnover")
    industry = rec.get("industry", "")

    try:
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-trend-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code, days=120, force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("趋势涨停预测获取历史 %s 失败: %s", code, exc)
        history = None

    if history is None or history.empty or len(history) < 25:
        return None

    df = history.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

    t = len(df) - 1
    latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else rec.get("close")
    if latest_close is None or latest_close <= 0:
        return None

    ma5 = close.rolling(5, min_periods=5).mean()
    ma10 = close.rolling(10, min_periods=10).mean()
    ma20 = close.rolling(20, min_periods=20).mean()
    ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
    ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
    ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None
    if ma5_val is None or ma10_val is None or ma20_val is None:
        return None

    # 1) 多头排列
    if not (ma5_val > ma10_val > ma20_val):
        return None
    # 2) 站上 MA5
    if latest_close < ma5_val * 0.995:
        return None
    # 3) MA20 抬头
    if t < 25 or pd.isna(ma20.iloc[t - 5]):
        return None
    ma20_slope = float(ma20.iloc[t]) - float(ma20.iloc[t - 5])
    if ma20_slope <= 0:
        return None
    ma20_slope_pct = round(ma20_slope / float(ma20.iloc[t - 5]) * 100, 2) if float(ma20.iloc[t - 5]) > 0 else 0.0

    # 4) 60 日位置
    position_60d = None
    if t >= 60:
        window60 = close.iloc[t - 60:t + 1].dropna()
        if not window60.empty:
            hi = float(window60.max())
            lo = float(window60.min())
            if hi > lo:
                position_60d = round((latest_close - lo) / (hi - lo) * 100, 1)
    if position_60d is None or position_60d < 40 or position_60d > 92:
        return None

    score = 0.0
    reasons: List[str] = []

    # 多头排列强度（MA5/MA20 的开口）
    ma_spread_pct = round((ma5_val - ma20_val) / ma20_val * 100, 2)
    if ma_spread_pct >= 8:
        score += 22
        reasons.append(f"多头开口{ma_spread_pct:.1f}%+22")
    elif ma_spread_pct >= 4:
        score += 18
        reasons.append(f"多头开口{ma_spread_pct:.1f}%+18")
    elif ma_spread_pct >= 1.5:
        score += 12
        reasons.append(f"多头排列{ma_spread_pct:.1f}%+12")
    else:
        score += 5
        reasons.append(f"刚刚多头{ma_spread_pct:.1f}%+5")

    # 距 MA5：紧贴 MA5 (0~3%) 是甜区，太远扣分
    dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)
    if 0 <= dist_ma5_pct <= 3:
        score += 14
        reasons.append(f"贴MA5({dist_ma5_pct:+.1f}%)+14")
    elif 3 < dist_ma5_pct <= 6:
        score += 8
        reasons.append(f"距MA5 {dist_ma5_pct:.1f}%+8")
    elif dist_ma5_pct > 10:
        score -= 8
        reasons.append(f"距MA5 {dist_ma5_pct:.1f}%过远-8")

    # 今日动能
    if change_pct is not None:
        if change_pct >= 7.0:
            score += 22
            reasons.append(f"今涨{change_pct:.1f}%临界涨停+22")
        elif change_pct >= 4.0:
            score += 14
            reasons.append(f"今涨{change_pct:.1f}%上攻+14")
        elif change_pct >= 1.5:
            score += 8
            reasons.append(f"今涨{change_pct:.1f}%稳健+8")
        elif change_pct < -1.5:
            score -= 6
            reasons.append(f"今跌{change_pct:.1f}%-6")

    # 量比：温和放量 (1.2~2.5) 最好；爆量(>3) 反而要警惕加速顶。叠加 20 日量比避免假放量
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if 1.2 <= vol_ratio <= 2.5:
            score += 14
            reasons.append(f"量比{vol_ratio:.1f}x健康+14")
        elif 2.5 < vol_ratio <= 3.5:
            score += 8
            reasons.append(f"量比{vol_ratio:.1f}x偏热+8")
        elif vol_ratio > 3.5:
            score -= 4
            reasons.append(f"量比{vol_ratio:.1f}x过热-4")
        elif vol_ratio < 0.7:
            score -= 8
            reasons.append(f"量比{vol_ratio:.1f}x缩量-8")
        else:
            score += 4
            reasons.append(f"量比{vol_ratio:.1f}x+4")

        if vol_ratio >= 1.2 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 6
            reasons.append(f"5d量比{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-6")

    # 5/10 日趋势
    trend_5d = None
    trend_10d = None
    if t >= 5 and not pd.isna(close.iloc[t - 5]) and float(close.iloc[t - 5]) > 0:
        trend_5d = round((latest_close / float(close.iloc[t - 5]) - 1) * 100, 1)
    if t >= 10 and not pd.isna(close.iloc[t - 10]) and float(close.iloc[t - 10]) > 0:
        trend_10d = round((latest_close / float(close.iloc[t - 10]) - 1) * 100, 1)
    if trend_5d is not None:
        if 2 <= trend_5d <= 12:
            score += 8
            reasons.append(f"5日{trend_5d:+.1f}%稳健+8")
        elif 12 < trend_5d <= 22:
            score += 4
            reasons.append(f"5日{trend_5d:+.1f}%偏快+4")
        elif trend_5d > 25:
            score -= 8
            reasons.append(f"5日{trend_5d:+.1f}%过急-8")

    # 60 日位置：50~80 中位偏上为最佳
    if 50 <= position_60d <= 80:
        score += 10
        reasons.append(f"60日位置{position_60d:.0f}%中位+10")
    elif 80 < position_60d <= 90:
        score += 4
        reasons.append(f"60日位置{position_60d:.0f}%偏高+4")
    else:
        score += 2
        reasons.append(f"60日位置{position_60d:.0f}%+2")

    # MA20 抬头
    if ma20_slope_pct >= 1.0:
        score += 8
        reasons.append(f"MA20 5日抬头{ma20_slope_pct:.1f}%+8")
    elif ma20_slope_pct >= 0.3:
        score += 4
        reasons.append(f"MA20 5日抬头{ma20_slope_pct:.1f}%+4")

    # 行业
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 10
        reasons.append(f"热门板块({hot_industries[industry]}只)+10")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 5
        reasons.append(f"板块联动({hot_industries[industry]}只)+5")

    # 题材热度（来自 AI 题材聚类缓存）
    theme_bonus, theme_reason = _shared.theme_bonus(code, industry, compare_context)
    if theme_bonus > 0:
        score += theme_bonus
        if theme_reason:
            reasons.append(theme_reason)

    # 资金面：龙虎榜 + 北向 + 板块强弱
    flow_bonus, flow_reasons = _shared.capital_flow_bonus(code, compare_context, industry=industry)
    if flow_bonus != 0:
        score += flow_bonus
        reasons.extend(flow_reasons)

    # 换手率
    if turnover is not None:
        if 3 <= turnover <= 12:
            score += 6
            reasons.append(f"换手{turnover:.1f}%健康+6")
        elif 12 < turnover <= 22:
            score += 2
            reasons.append(f"换手{turnover:.1f}%偏高+2")
        elif turnover > 30:
            score -= 6
            reasons.append(f"换手{turnover:.1f}%过热-6")

    # 大盘环境
    latest_cont_rate = compare_context.get("latest_continuation_rate")
    if latest_cont_rate is not None:
        if latest_cont_rate >= 60:
            score += 4
            reasons.append(f"晋级率{latest_cont_rate:.0f}%+4")
        elif latest_cont_rate < 25:
            score -= 4
            reasons.append(f"晋级率{latest_cont_rate:.0f}%-4")

    final_score = max(0, min(100, int(round(score))))
    return {
        "code": code,
        "name": name,
        "industry": industry,
        "close": latest_close,
        "change_pct": change_pct,
        "turnover": turnover,
        "ma5": ma5_val,
        "ma10": ma10_val,
        "ma20": ma20_val,
        "ma_spread_pct": ma_spread_pct,
        "ma20_slope_pct": ma20_slope_pct,
        "dist_ma5_pct": dist_ma5_pct,
        "position_60d": position_60d,
        "trend_5d": trend_5d,
        "trend_10d": trend_10d,
        "volume_ratio": vol_ratio,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": "趋势涨停",
    }
