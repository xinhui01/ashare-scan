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
from src.services.scoring.trend import _score_accumulation_signal

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
        # 反包分支优先追求命中率，默认只保留高置信候选。
        if score_info is not None and score_info["score"] >= 70:
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

    # 1) 在更合理的历史窗口里找“最近一次满足前置连板条件的涨停”。
    # lookback_days 仍保留为对“近期性”的偏好参数，但实际反包前置涨停
    # 可能比 5 日更早；如果把窗口卡死，会漏掉典型的中期断板反包。
    search_days = max(int(lookback_days or 0), 20)
    start = max(1, t - search_days)
    prior_lu_idx: Optional[int] = None
    streak = 0
    for i in range(t - 1, start - 1, -1):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            streak = 1
            j = i - 1
            while j >= 1:
                if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
                    break
                chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
                if chg_j < threshold - 0.3:
                    break
                streak += 1
                j -= 1
            if streak >= 2:
                prior_lu_idx = i
                break
    if prior_lu_idx is None:
        return None

    prior_lu_close = float(close.iloc[prior_lu_idx])
    if prior_lu_close <= 0:
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

    # 3) 反包硬性条件：今日 < 前涨停价 + 缺口 ≤ 11%
    wrap_gap_pct = (prior_lu_close / latest_close - 1) * 100
    if wrap_gap_pct <= 0:
        return None
    if wrap_gap_pct > 11.0:
        return None
    pattern_kind = "wrap"
    predict_type = "断板反包"

    score = 0.0
    reasons: List[str] = []

    raw_accumulation_score, raw_accumulation_risk_penalty, accumulation_reasons, accumulation_metrics = (
        _score_accumulation_signal(close, volume, t)
    )
    accumulation_score = int(round(raw_accumulation_score * 0.4))
    accumulation_risk_penalty = int(round(raw_accumulation_risk_penalty * 0.4))
    accumulation_metrics["accumulation_raw_score"] = raw_accumulation_score
    accumulation_metrics["accumulation_weight"] = 0.4
    if accumulation_score or accumulation_risk_penalty:
        score += accumulation_score + accumulation_risk_penalty
        if accumulation_score > 0:
            reasons.append(f"30日潜伏铺垫x0.4+{accumulation_score}")
        reasons.extend(accumulation_reasons)

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

    # 市场板位：当前连板如果已经是全市场最高板，资金更偏向继续做高标，
    # 反包优先级应下降；如果不是市场最高板，修复型反包更有性价比。
    market_max = int(compare_context.get("market_max_boards") or 0)
    if market_max > 0 and streak >= 2:
        if streak == market_max and streak >= 3:
            score -= 8
            reasons.append(f"已是市场最高{streak}板-8")
        elif market_max - streak >= 2:
            score += 4
            reasons.append(f"非市场最高板，修复空间大+4")
        elif market_max - streak == 1 and streak >= 3:
            score -= 4
            reasons.append(f"接近市场最高板{streak}板-4")

    # ---- 市场情绪：冰点更容易走超跌修复/反包，火热时更偏向追高标 ----
    sent_score = int(compare_context.get("sentiment_score") or 50)
    if sent_score < 35:
        score += 10
        reasons.append(f"情绪冰点{sent_score}超跌反包+10")
        if change_pct is not None and change_pct <= -3.0:
            score += 4
            reasons.append("冰点深跌修复+4")
    elif sent_score < 50:
        score += 4
        reasons.append(f"情绪偏冷{sent_score}修复+4")
    elif sent_score >= 70:
        score -= 4
        reasons.append(f"情绪火热{sent_score}追高优先-4")

    # ---- 反包缺口分（缺口 1~5% 最佳，越远越难单板覆盖）----
    if wrap_gap_pct <= 2.0:
        score += 18
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%临界+18")
    elif wrap_gap_pct <= 5.0:
        score += 16
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%甜区+16")
    elif wrap_gap_pct <= 8.0:
        score += 14
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%可达+14")
    else:  # 8~11%
        score += 10
        reasons.append(f"距前涨停{wrap_gap_pct:.1f}%深缺口+10")

    # ---- 量能优先（反包成功更依赖放量承接）----
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if vol_ratio >= 3.0:
            score += 28
            reasons.append(f"量比{vol_ratio:.1f}x爆量承接+28")
        elif vol_ratio >= 2.0:
            score += 22
            reasons.append(f"量比{vol_ratio:.1f}x强放量+22")
        elif vol_ratio >= 1.4:
            score += 16
            reasons.append(f"量比{vol_ratio:.1f}x放量+16")
        elif vol_ratio >= 1.0:
            score += 8
            reasons.append(f"量比{vol_ratio:.1f}x温和+8")
        elif vol_ratio < 0.7:
            score -= 5
            reasons.append(f"量比{vol_ratio:.1f}x过弱-5")

        if vol_ratio >= 1.4 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 6
            reasons.append(f"5d量比{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-6")

    # ---- 今日 T0 形态（U 形：硬阴线/微红高，消化区低）----
    # 断板涨幅只做低权重修正，不再作为硬门槛。
    if change_pct is not None:
        if change_pct <= -7.0:
            score += 5
            reasons.append(f"今跌{change_pct:.1f}%深坑+5")
        elif change_pct <= -5.0:
            score += 4
            reasons.append(f"今跌{change_pct:.1f}%硬阴+4")
        elif change_pct <= -3.0:
            score += 2
            reasons.append(f"今跌{change_pct:.1f}%小阴+2")
        elif change_pct <= -1.0:
            score += 1
            reasons.append(f"今跌{change_pct:.1f}%弱消化+1")
        elif change_pct >= 6.0:
            score += 4
            reasons.append(f"今涨{change_pct:.1f}%放量上攻+4")
        elif change_pct >= 3.0:
            score += 2
            reasons.append(f"今涨{change_pct:.1f}%企稳+2")
        elif change_pct >= 1.0:
            score += 1
            reasons.append(f"今涨{change_pct:.1f}%温和+1")
        # 平淡区 (-1~+1) 不加不扣

    # ---- 弱断板修正：没有出现 -3% 以上的深跌，也允许进候选池 ----
    if worst_drop is not None:
        if worst_drop <= -7.0:
            score += 4
            reasons.append(f"最深阴{worst_drop:.1f}%深坑+4")
        elif worst_drop <= -5.0:
            score += 3
            reasons.append(f"最深阴{worst_drop:.1f}%+3")
        elif worst_drop <= -3.0:
            score += 1
            reasons.append(f"最深阴{worst_drop:.1f}%+1")

    # 高量弱断板：核心过滤特征
    if vol_ratio is not None and vol_ratio >= 2.0 and (change_pct is None or change_pct > -3.0):
        score += 8
        reasons.append(f"高量弱断板+8")

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
        score += 5
        reasons.append(f"{streak}连板+今跌{change_pct:.1f}%深坑反包+5")

    # ---- 联合因子：高连板 × 深缺口（≥3板 + 缺口5-11% = 砸出来的黄金坑）----
    if streak >= 3 and wrap_gap_pct >= 5.0:
        score += 4
        reasons.append(f"{streak}连板+{wrap_gap_pct:.1f}%深缺口+4")

    # ---- 联合因子：高连板 × 放量 ----
    if streak >= 3 and vol_ratio is not None and vol_ratio >= 2.0:
        score += 6
        reasons.append(f"{streak}连板+{vol_ratio:.1f}x放量+6")

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

    theme_fund_bonus, theme_fund_reasons = _shared.theme_fund_bonus(
        code, industry, compare_context
    )
    if theme_fund_bonus:
        score += min(theme_fund_bonus, 4)
        reasons.extend(theme_fund_reasons)

    # ---- 板块联动（行业涨跌幅加分） ----
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
        "accumulation_score": accumulation_score,
        "accumulation_risk_penalty": accumulation_risk_penalty,
        **accumulation_metrics,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": predict_type,
    }
