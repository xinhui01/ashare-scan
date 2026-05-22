"""反包（wrap）评分。

2 个模块级函数（参数注入模式）：
- scan_broken_board_wrap_candidates_cached: 从反包候选池（chg ∈ [-10.5%, +3%)）扫候选并逐只评分
- score_broken_board_wrap: 主评分（要求前置连板数 ≥ 2 + 反包缺口 ≤ 11% + 历史反包加分）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_pct_fn / build_local_cache_history_plan_fn /
filter_wrap_candidate_stocks_fn。
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
    filter_wrap_candidate_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """识别"断板反包"候选：近 lookback_days 内有过 ≥2 板涨停被打掉，
    今日 T0 形态在 [-10.5%, +3%) 区间，明日有望反包涨停。

    候选池：filter_wrap_candidate_stocks 形成的 T0 形态池；
    "前置连板数 ≥ 2" 由 score_broken_board_wrap 内部硬性过滤（1 板反包率仅 3.97%）。
    +3%~+9.95% 强势上涨区间已剥离到 trend / fresh 通道。
    """
    if spot_df is None or spot_df.empty:
        return []

    if filter_wrap_candidate_stocks_fn is None:
        return []

    merged: List[Dict[str, Any]] = list(filter_wrap_candidate_stocks_fn(spot_df, zt_codes))
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
    """对断板反包候选评分。

    硬性条件：
    1. 前 lookback_days 内至少存在 ≥2 板连续涨停（streak ≥ 2）
    2. 该连板被打掉（涨停日到今日之间至少一根 ≤ drop_threshold_pct 的阴线）
    3. 今日收盘 < 前涨停价（必有反包缺口）
    4. 反包缺口 ≤ 11%（明日单板可覆盖）
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

    # 1.5) 硬性条件：前置连板数 ≥ 2（从 prior_lu_idx 往前数连续涨停日）。
    # 回测 91185 个事件：1 板反包率 3.97%、2 板 6.53%、3 板 7.92%、≥4 板 8.80%，
    # 单板反包动力不足，砍掉可消除 ~84% 噪音同时把基线从 4.42% 拉到 6.5%+。
    streak = 1
    j = prior_lu_idx - 1
    while j >= 1:
        if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
            break
        chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
        if chg_j < threshold - 0.3:
            break
        streak += 1
        j -= 1
    if streak < 2:
        return None

    # 2) 统计涨停日到今日（含今日）之间的阴线
    # 注：range 必须含 t 自己——"今日就是断板日"是最经典的反包前夜形态，
    # 旧版 range(prior_lu_idx+1, t) 排除 t 会让"4 连板今天 -7% 断板"这种典型
    # 反包候选 worst_drop=None 直接被 return None，导致评分漏判。
    worst_drop: Optional[float] = None
    bearish_days = 0
    for j in range(prior_lu_idx + 1, t + 1):
        if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
            continue
        chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
        if chg_j <= drop_threshold_pct:
            bearish_days += 1
            if worst_drop is None or chg_j < worst_drop:
                worst_drop = chg_j

    # 3) 反包硬性条件：今日 < 前涨停价 + 缺口 ≤ 11% + 期间有断板阴线
    wrap_gap_pct = (prior_lu_close / latest_close - 1) * 100
    if wrap_gap_pct <= 0:
        return None
    if wrap_gap_pct > 11.0:
        return None
    if worst_drop is None:
        return None
    pattern_kind = "wrap"
    predict_type = "断板反包"

    score = 0.0
    reasons: List[str] = []

    # ---- 前置连板加分（核心因子）----
    # 端到端回测：2板 9.3%、3板 11.9%、≥4板 15.5%（反包率随连板线性提升）
    if streak >= 4:
        score += 20
        reasons.append(f"{streak}连板被砸+20")
    elif streak == 3:
        score += 12
        reasons.append("3连板被砸+12")
    else:  # streak == 2 (硬性已 ≥ 2)
        score += 5
        reasons.append("2连板被砸+5")

    # ---- 反包缺口分（缺口 1~5% 最佳，越远越难单板覆盖）----
    if wrap_gap_pct <= 2.0:
        score += 22
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%临界+22")
    elif wrap_gap_pct <= 5.0:
        score += 18
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%甜区+18")
    elif wrap_gap_pct <= 8.0:
        score += 18
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%可达+18")
    else:  # 8~11%
        score += 12
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%深缺口+12")

    # ---- 今日 T0 形态（U 形：硬阴线/微红高，消化区低）----
    # 回测 T+1 反包率：
    #   -10.5~-7% 6.06%，-7~-5% 6.53%（硬阴线带）
    #   -5~-3% 4.85%，-3~+3% 3.10-4.05%（消化区低谷）
    #   +5%以上才再次走高（被剥离到 trend/fresh）
    if change_pct is not None:
        if change_pct <= -7.0:
            score += 15
            reasons.append(f"今跌{change_pct:.1f}%深坑+15")
        elif change_pct <= -5.0:
            score += 12
            reasons.append(f"今跌{change_pct:.1f}%硬阴+12")
        elif change_pct <= -3.0:
            score += 6
            reasons.append(f"今跌{change_pct:.1f}%小阴+6")
        elif change_pct <= -1.0:
            score += 3
            reasons.append(f"今跌{change_pct:.1f}%弱消化+3")
        elif change_pct >= 6.0:
            score += 20
            reasons.append(f"今涨{change_pct:.1f}%放量上攻+20")
        elif change_pct >= 3.0:
            score += 12
            reasons.append(f"今涨{change_pct:.1f}%企稳+12")
        elif change_pct >= 1.0:
            score += 6
            reasons.append(f"今涨{change_pct:.1f}%温和+6")
        # 平淡区 (-1~+1) 不加不扣

    # ---- 量比（5 日 + 20 日双校验，弱权重——回测显示影响仅 1-2 个百分点）----
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if vol_ratio >= 2.0:
            score += 8
            reasons.append(f"量比{vol_ratio:.1f}x爆量+8")
        elif vol_ratio >= 1.4:
            score += 5
            reasons.append(f"量比{vol_ratio:.1f}x放量+5")
        elif vol_ratio < 0.5:
            score -= 2
            reasons.append(f"量比{vol_ratio:.1f}x极缩-2")
        # 0.5~1.4 中性区不加不扣（缩量止跌是中性偏好）

        # 5 日量比看似放量、但 20 日量比仍 <0.9 → 假放量（缩量调整里的小反弹）
        if vol_ratio >= 1.4 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 4
            reasons.append(f"5d量比{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-4")

    # ---- 阴线深度（瞬时或历史最深阴线，权重提高）----
    if worst_drop is not None:
        if worst_drop <= -7.0:
            score += 12
            reasons.append(f"最深阴{worst_drop:.1f}%深坑+12")
        elif worst_drop <= -5.0:
            score += 8
            reasons.append(f"最深阴{worst_drop:.1f}%+8")

    # ---- 距前涨停天数 ----
    days_since_lu = t - prior_lu_idx
    if days_since_lu == 1:
        score += 8
        reasons.append("昨涨停今回踩+8")
    elif days_since_lu in (2, 3):
        score += 12
        reasons.append(f"前{days_since_lu}日涨停+12")
    elif days_since_lu <= 5:
        score += 5
        reasons.append(f"前{days_since_lu}日涨停+5")

    # ---- 联合因子：高连板 × 硬阴线（深坑反包黄金组合）----
    if streak >= 3 and change_pct is not None and change_pct <= -5.0:
        score += 10
        reasons.append(f"{streak}连板+今跌{change_pct:.1f}%深坑反包+10")

    # ---- 联合因子：高连板 × 深缺口（≥3板 + 缺口5-11% = 砸出来的黄金坑）----
    if streak >= 3 and wrap_gap_pct >= 5.0:
        score += 6
        reasons.append(f"{streak}连板+{wrap_gap_pct:.1f}%深缺口+6")

    # ---- 换手率（反包前夜的多空换手强度）----
    if turnover is not None:
        if turnover >= 10.0:
            score += 6
            reasons.append(f"换手{turnover:.1f}%高活跃+6")
        elif turnover >= 6.0:
            score += 3
            reasons.append(f"换手{turnover:.1f}%+3")
        elif turnover < 1.5:
            score -= 3
            reasons.append(f"换手{turnover:.1f}%极冷-3")

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

    # === 历史同类形态加分：近 90 日内的反包成功次数 ===
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
        if last_hit_days is not None and last_hit_days <= 30:
            bonus = min(bonus + 2, 10)
            reasons.append(f"近90日{occ_count}次反包成功(最近{last_hit_days}日内)+{bonus}")
        else:
            reasons.append(f"近90日{occ_count}次反包成功+{bonus}")
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
        # wrap_gap_pct > 0：今日低于前涨停价，数值即反包缺口大小（%）
        "wrap_gap_pct": round(wrap_gap_pct, 2),
        "worst_drop": round(worst_drop, 2) if worst_drop is not None else None,
        "bearish_days": bearish_days,
        "volume_ratio": vol_ratio,
        "pattern_kind": pattern_kind,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": predict_type,
    }
