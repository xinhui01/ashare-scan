"""反包/承接（wrap）评分。

2 个模块级函数（参数注入模式）：
- scan_broken_board_wrap_candidates_cached: 从今日强势股池 + MA5 回踩股池扫候选并逐只评分
- score_broken_board_wrap: 主评分（含路径划分 wrap/hold_strong + 历史反包加分）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_pct_fn / build_local_cache_history_plan_fn /
filter_strong_stocks_fn / filter_ma5_pullback_stocks_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring.helpers import _count_historical_wrap

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


def scan_broken_board_wrap_candidates_cached(
    spot_df: Optional[pd.DataFrame],
    zt_codes: set,
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    fetcher,
    lookback_days: int = 5,
    drop_threshold_pct: float = -3.0,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    filter_strong_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
    filter_ma5_pullback_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """识别"断板反包"候选：近 lookback_days 内有过涨停被打掉(>=3%阴线)，
    今日价格接近前涨停日收盘，明日有望以涨停反包之前的下跌。

    迁自 StockFilter._scan_broken_board_wrap_candidates_cached；行为零变化。
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
        score_info = score_broken_board_wrap(
            rec, hot_industries, compare_context,
            fetcher=fetcher,
            lookback_days=lookback_days, drop_threshold_pct=drop_threshold_pct,
            log_fn=log_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        )
        if score_info is not None and score_info["score"] >= 50:
            candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, total, f"反包筛选 {rec['code']} {rec.get('name', '')}")

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:50]


def score_broken_board_wrap(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    lookback_days: int = 5,
    drop_threshold_pct: float = -3.0,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    """对断板反包 / 强势承接候选评分。

    近 lookback_days 内出现过涨停（不含今日），按今日相对前涨停价的位置分两路：

    路径 A · 经典反包（今日收盘 < 前涨停价）：
    1. 涨停日与今日之间至少出现一根 ≤ drop_threshold_pct 的阴线
    2. 反包缺口 ≤ 11%（明日单板可覆盖）

    路径 B · 强势承接 / 不破前涨停价（今日收盘 ≥ 前涨停价）：
    1. 今日最低价 ≥ 前涨停价 × 0.99（盘中没真正跌破涨停价）
    2. 今日收盘距前涨停价上方 ≤ 12%（再高就是趋势加速，归趋势分支）
    3. 不要求中间出现阴线（昨涨停今承接也算）

    迁自 StockFilter._score_broken_board_wrap；行为零变化。
    """
    threshold_fn = limit_up_threshold_pct_fn or _default_limit_up_threshold_pct

    code = rec["code"]
    name = rec.get("name", "")
    change_pct = rec.get("change_pct")
    turnover = rec.get("turnover")
    industry = rec.get("industry", "")

    try:
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-broken-wrap-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code, days=120, force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("反包预测获取历史 %s 失败: %s", code, exc)
        history = None

    if history is None or history.empty or len(history) < 8:
        return None

    df = history.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    low = pd.to_numeric(df.get("low"), errors="coerce") if "low" in df.columns else pd.Series(dtype=float)
    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

    t = len(df) - 1
    latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else rec.get("close")
    if latest_close is None or latest_close <= 0:
        return None
    latest_low = float(low.iloc[t]) if not low.empty and not pd.isna(low.iloc[t]) else None

    threshold = threshold_fn(code)

    # 1) 在 [t-lookback_days, t-1] 找最近一次涨停
    start = max(1, t - lookback_days)
    prior_lu_idx: Optional[int] = None
    for i in range(t - 1, start - 1, -1):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            prior_lu_idx = i
            break
    if prior_lu_idx is None:
        return None

    prior_lu_close = float(close.iloc[prior_lu_idx])
    if prior_lu_close <= 0:
        return None

    # 2) 统计涨停日到今日之间的阴线（信息项，按路径决定是否强制）
    worst_drop: Optional[float] = None
    bearish_days = 0
    for j in range(prior_lu_idx + 1, t):
        if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
            continue
        chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
        if chg_j <= drop_threshold_pct:
            bearish_days += 1
            if worst_drop is None or chg_j < worst_drop:
                worst_drop = chg_j

    # 3) 路径划分
    # wrap_gap_pct > 0 → 今日 < 前涨停价（经典反包）
    # wrap_gap_pct ≤ 0 → 今日 ≥ 前涨停价（强势承接）
    wrap_gap_pct = (prior_lu_close / latest_close - 1) * 100

    if wrap_gap_pct > 0:
        pattern_kind = "wrap"
        predict_type = "断板反包"
        # 经典反包硬性条件
        if worst_drop is None:
            return None
        if wrap_gap_pct > 11.0:
            return None
    else:
        pattern_kind = "hold_strong"
        predict_type = "强势承接"
        # 强势承接硬性条件：低点没破前涨停价
        if latest_low is None or latest_low < prior_lu_close * 0.99:
            return None
        # 离前涨停价上方过远（≥12%）就归趋势分支
        if -wrap_gap_pct > 12.0:
            return None

    score = 0.0
    reasons: List[str] = []

    # ---- 位置/缺口分（按路径分别打分）----
    if pattern_kind == "wrap":
        # 反包缺口（甜区 1~5%）
        if wrap_gap_pct <= 2.0:
            score += 26
            reasons.append(f"距前涨停{wrap_gap_pct:.1f}%临界+26")
        elif wrap_gap_pct <= 5.0:
            score += 22
            reasons.append(f"距前涨停{wrap_gap_pct:.1f}%甜区+22")
        elif wrap_gap_pct <= 8.0:
            score += 12
            reasons.append(f"距前涨停{wrap_gap_pct:.1f}%可达+12")
        else:
            score += 4
            reasons.append(f"距前涨停{wrap_gap_pct:.1f}%偏远+4")
    else:
        # 强势承接：站在前涨停价上方多少
        above_pct = -wrap_gap_pct
        if above_pct <= 1.0:
            score += 28
            reasons.append(f"顶住前涨停价({above_pct:.1f}%)+28")
        elif above_pct <= 3.0:
            score += 24
            reasons.append(f"涨停价上方{above_pct:.1f}%+24")
        elif above_pct <= 6.0:
            score += 18
            reasons.append(f"涨停价上方{above_pct:.1f}%+18")
        else:
            score += 10
            reasons.append(f"涨停价上方{above_pct:.1f}%偏远+10")
        # 盘中是否回踩涨停价但不破（最低价贴近前涨停价）
        if latest_low is not None and latest_low <= prior_lu_close * 1.01:
            score += 8
            reasons.append("盘中回踩涨停价不破+8")

    # ---- 今日动能（共用）----
    if change_pct is not None:
        if change_pct >= 6.0:
            score += 20
            reasons.append(f"今涨{change_pct:.1f}%放量上攻+20")
        elif change_pct >= 3.0:
            score += 12
            reasons.append(f"今涨{change_pct:.1f}%企稳+12")
        elif change_pct >= 1.0:
            score += 6
            reasons.append(f"今涨{change_pct:.1f}%温和+6")
        elif change_pct < -1.0:
            score -= 10
            reasons.append(f"今跌{change_pct:.1f}%-10")

    # ---- 量比（5 日 + 20 日双校验）----
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if pattern_kind == "wrap":
            if vol_ratio >= 2.0:
                score += 18
                reasons.append(f"量比{vol_ratio:.1f}x爆量+18")
            elif vol_ratio >= 1.4:
                score += 10
                reasons.append(f"量比{vol_ratio:.1f}x放量+10")
            elif vol_ratio < 0.8:
                score -= 8
                reasons.append(f"量比{vol_ratio:.1f}x缩量-8")
        else:
            # 强势承接更看重缩量止跌（盘面没抛压）；爆量出货反而要警惕
            if 0.6 <= vol_ratio <= 1.3:
                score += 14
                reasons.append(f"量比{vol_ratio:.1f}x缩量承接+14")
            elif 1.3 < vol_ratio <= 2.0:
                score += 10
                reasons.append(f"量比{vol_ratio:.1f}x放量+10")
            elif vol_ratio > 3.0:
                score -= 6
                reasons.append(f"量比{vol_ratio:.1f}x巨量警惕-6")
            elif vol_ratio < 0.4:
                score -= 4
                reasons.append(f"量比{vol_ratio:.1f}x极缩-4")

        # 5 日量比看似放量、但 20 日量比仍 <0.9 → 假放量（缩量调整里的小反弹）
        if vol_ratio >= 1.4 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 6
            reasons.append(f"5d量比{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-6")

    # ---- 阴线深度（仅经典反包）----
    if pattern_kind == "wrap" and worst_drop is not None:
        if worst_drop <= -7.0:
            score += 8
            reasons.append(f"前阴{worst_drop:.1f}%深坑+8")
        elif worst_drop <= -5.0:
            score += 4
            reasons.append(f"前阴{worst_drop:.1f}%+4")

    # ---- 距前涨停天数（共用，但 hold_strong 时 1 天最佳）----
    days_since_lu = t - prior_lu_idx
    if pattern_kind == "hold_strong":
        if days_since_lu == 1:
            score += 15
            reasons.append("昨涨停今承接+15")
        elif days_since_lu in (2, 3):
            score += 10
            reasons.append(f"前{days_since_lu}日涨停今承接+10")
        elif days_since_lu <= 5:
            score += 5
            reasons.append(f"前{days_since_lu}日涨停+5")
    else:
        if days_since_lu == 1:
            score += 8
            reasons.append("昨涨停今回踩+8")
        elif days_since_lu in (2, 3):
            score += 12
            reasons.append(f"前{days_since_lu}日涨停+12")
        elif days_since_lu <= 5:
            score += 5
            reasons.append(f"前{days_since_lu}日涨停+5")

    # ---- 行业（共用）----
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 10
        reasons.append(f"热门板块({hot_industries[industry]}只)+10")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 5
        reasons.append(f"板块联动({hot_industries[industry]}只)+5")

    # ---- 题材热度（来自 AI 题材聚类缓存）----
    theme_bonus, theme_reason = _shared.theme_bonus(code, industry, compare_context)
    if theme_bonus > 0:
        score += theme_bonus
        if theme_reason:
            reasons.append(theme_reason)

    # ---- 资金面：龙虎榜 + 北向 3 日加仓 ----
    flow_bonus, flow_reasons = _shared.capital_flow_bonus(code, compare_context)
    if flow_bonus != 0:
        score += flow_bonus
        reasons.extend(flow_reasons)

    # ---- 大盘环境（共用）----
    latest_cont_rate = compare_context.get("latest_continuation_rate")
    if latest_cont_rate is not None:
        if latest_cont_rate >= 60:
            score += 4
            reasons.append(f"晋级率{latest_cont_rate:.0f}%+4")
        elif latest_cont_rate < 25:
            score -= 4
            reasons.append(f"晋级率{latest_cont_rate:.0f}%-4")

    # === 历史同类形态加分：近 90 日内的反包/承接成功次数 ===
    occ_count, last_hit_days = _count_historical_wrap(
        history, code, lookback_days=90, window=5, drop_threshold=-3.0,
        threshold_fn=threshold_fn,
    )
    if occ_count >= 3:
        bonus = 8
    elif occ_count >= 2:
        bonus = 5
    elif occ_count >= 1:
        bonus = 2
    else:
        bonus = 0

    if bonus > 0:
        _label = "反包" if pattern_kind == "wrap" else "承接"
        if last_hit_days is not None and last_hit_days <= 30:
            bonus = min(bonus + 2, 10)
            reasons.append(f"近90日{occ_count}次{_label}成功(最近{last_hit_days}日内)+{bonus}")
        else:
            reasons.append(f"近90日{occ_count}次{_label}成功+{bonus}")
        score += bonus

    final_score = max(0, min(100, int(round(score))))

    prior_lu_date: Optional[str] = None
    if "date" in df.columns:
        try:
            prior_lu_date = str(df["date"].iloc[prior_lu_idx])
        except Exception:
            prior_lu_date = None

    return {
        "code": code,
        "name": name,
        "industry": industry,
        "close": latest_close,
        "change_pct": change_pct,
        "turnover": turnover,
        "prior_lu_date": prior_lu_date,
        "prior_lu_close": prior_lu_close,
        "days_since_lu": days_since_lu,
        # wrap_gap_pct 沿用旧含义：>0 表示今日低于前涨停价（经典反包缺口）；
        # <0 表示今日高于前涨停价（强势承接），数值越接近 0 越紧贴涨停价。
        "wrap_gap_pct": round(wrap_gap_pct, 2),
        "worst_drop": round(worst_drop, 2) if worst_drop is not None else None,
        "bearish_days": bearish_days,
        "volume_ratio": vol_ratio,
        "pattern_kind": pattern_kind,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": predict_type,
    }
