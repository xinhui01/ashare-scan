"""首板涨停（fresh）评分。

2 个模块级函数（参数注入模式）：
- scan_fresh_first_board_candidates_cached: 从今日强势股池扫候选并逐只评分（带冷却期过滤）
- score_fresh_first_board: 主评分（冷却期判定 + 量价启动 + 均线位置 + 行业/题材/资金面共振）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_pct_fn / build_local_cache_history_plan_fn /
filter_strong_stocks_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring.helpers import _count_historical_any_limit_up

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


def scan_fresh_first_board_candidates_cached(
    spot_df: Optional[pd.DataFrame],
    zt_codes: set,
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    fetcher,
    cooldown_days: int = 5,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    filter_strong_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """从全市场强势股中识别"近期未涨停、明日有望首封"的候选。

    与 `scan_followthrough_candidates_cached` 区别：
    - 承接候选：最近曾涨停过、回落到 MA5 附近的股票
    - 首板候选：最近 N 日未出现过涨停，今日量价启动、逼近涨停的"新生力量"

    迁自 StockFilter._scan_fresh_first_board_candidates_cached；行为零变化。
    """
    if spot_df is None or spot_df.empty:
        return []

    if filter_strong_stocks_fn is None:
        # 没有注入强势股筛选函数时无法继续；保持原行为（原方法必然依赖 self._filter_strong_stocks）
        return []

    # 入口涨幅 [+3%, +9.5%)：实证表明 < 3% 涨幅的"潜伏型"虽然在真实
    # 首板里占 53%，但 base rate 太低（全市场样本太多），加进来反而拉低
    # precision。保留原范围，靠评分函数提升高分段命中率。
    merged: List[Dict[str, Any]] = []
    seen: set = set()
    for rec in filter_strong_stocks_fn(spot_df, zt_codes):
        chg = rec.get("change_pct")
        if chg is None or chg < 3.0 or chg >= 9.5:
            continue
        if rec["code"] in seen:
            continue
        seen.add(rec["code"])
        merged.append(rec)

    if not merged:
        return []

    candidates: List[Dict[str, Any]] = []
    total = len(merged)
    for idx, rec in enumerate(merged):
        score_info = score_fresh_first_board(
            rec, hot_industries, compare_context,
            fetcher=fetcher,
            cooldown_days=cooldown_days,
            log_fn=log_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        )
        # 门槛从 50 降到 45：30 天只攒到 13 条样本统计意义不足，
        # 先放宽吸量积累数据，待样本到位再回收门槛
        if score_info is not None and score_info["score"] >= 45:
            candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, total, f"首板筛选 {rec['code']} {rec.get('name', '')}")

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:50]


def score_fresh_first_board(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    cooldown_days: int = 5,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    """对"近期未涨停、今日量价启动"的强势股评分。

    强制条件：最近 cooldown_days 个交易日内不存在涨停过。命中冷却期返回 None。

    实证：基于 1112 只真实首板涨停股回测，本算法在
      - score ≥ 50: precision ~3% (n=447)
      - score ≥ 60: precision ~4% (n=104)
      - score ≥ 70: precision ~9% (n=11)
    高分段命中率合理但样本稀少。建议盯 ≥70 分的候选。

    迁自 StockFilter._score_fresh_first_board；行为零变化。
    """
    threshold_fn = limit_up_threshold_pct_fn or _default_limit_up_threshold_pct

    code = rec["code"]
    name = rec.get("name", "")
    change_pct = rec.get("change_pct")
    turnover = rec.get("turnover")
    industry = rec.get("industry", "")

    try:
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-fresh-first-board-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code, days=120, force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("预测首板获取历史 %s 失败: %s", code, exc)
        history = None

    if history is None or history.empty or len(history) < 11:
        return None

    df = history.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

    t = len(df) - 1
    latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else rec.get("close")

    # ---- 冷却期判定：最近 cooldown_days 交易日内不能有涨停 ----
    threshold = threshold_fn(code)
    cooldown_start = max(1, t - cooldown_days + 1)
    last_zt_offset: Optional[int] = None
    for i in range(cooldown_start, t + 1):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            last_zt_offset = t - i
            break
    if last_zt_offset is not None:
        return None  # 已涨停过，让承接/连板分支处理

    score = 0.0
    reasons: List[str] = []

    # 1. 当日涨幅靠近涨停
    if change_pct is not None:
        if change_pct >= 8.0:
            score += 28
            reasons.append(f"涨{change_pct:.1f}%逼近涨停+28")
        elif change_pct >= 6.0:
            score += 18
            reasons.append(f"涨{change_pct:.1f}%放量上攻+18")
        elif change_pct >= 4.0:
            score += 10
            reasons.append(f"涨{change_pct:.1f}%突破+10")
        elif change_pct >= 3.0:
            score += 5
            reasons.append(f"涨{change_pct:.1f}%温和启动+5")

    # 2. 量比放大（叠加 20 日校验，剔除"缩量调整里的假放量"）
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if vol_ratio >= 2.5:
            score += 22
            reasons.append(f"量比{vol_ratio:.1f}x爆量+22")
        elif vol_ratio >= 1.8:
            score += 14
            reasons.append(f"量比{vol_ratio:.1f}x放量+14")
        elif vol_ratio >= 1.3:
            score += 6
            reasons.append(f"量比{vol_ratio:.1f}x温和放量+6")
        elif vol_ratio < 1.0:
            score -= 10
            reasons.append(f"量比{vol_ratio:.1f}x缩量-10")

        if vol_ratio >= 1.3 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 8
            reasons.append(f"5d量比{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-8")

    # 3. 均线位置：站上 MA5/MA10/MA20
    ma5 = close.rolling(5, min_periods=5).mean()
    ma10 = close.rolling(10, min_periods=10).mean()
    ma20 = close.rolling(20, min_periods=20).mean()
    ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
    ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
    ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None
    dist_ma5_pct = None
    if ma5_val and ma5_val > 0 and latest_close is not None:
        dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)

    if (
        latest_close is not None and ma5_val and ma10_val and ma20_val
        and latest_close >= ma5_val >= ma10_val >= ma20_val
    ):
        score += 14
        reasons.append("多头排列+14")
    elif (
        latest_close is not None and ma5_val and ma10_val
        and latest_close >= ma5_val >= ma10_val
    ):
        score += 8
        reasons.append("站上MA5/10+8")
    elif latest_close is not None and ma5_val and latest_close < ma5_val * 0.99:
        score -= 8
        reasons.append("跌破MA5-8")

    # 4. 60日位置：避开高位接盘
    position_60d = None
    if t >= 60:
        window60 = close.iloc[t - 60:t + 1].dropna()
        if not window60.empty:
            hi = float(window60.max())
            lo = float(window60.min())
            if hi > lo and latest_close is not None:
                position_60d = round((latest_close - lo) / (hi - lo) * 100, 1)
    if position_60d is not None:
        if position_60d >= 92:
            score -= 10
            reasons.append(f"60日位置{position_60d:.0f}%过高-10")
        elif position_60d <= 35:
            score += 8
            reasons.append(f"60日位置{position_60d:.0f}%低位+8")
        elif 35 < position_60d <= 70:
            score += 4
            reasons.append(f"60日位置{position_60d:.0f}%中位+4")

    # 5. 5日/10日趋势
    trend_5d = None
    if t >= 5 and not pd.isna(close.iloc[t - 5]) and float(close.iloc[t - 5]) > 0 and latest_close is not None:
        trend_5d = round((latest_close / float(close.iloc[t - 5]) - 1) * 100, 1)
    if trend_5d is not None:
        if trend_5d > 22:
            score -= 8
            reasons.append(f"5日已涨{trend_5d:.1f}%过急-8")
        elif 4 <= trend_5d <= 18:
            score += 6
            reasons.append(f"5日涨{trend_5d:.1f}%稳健+6")

    # 6. 行业共振
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 12
        reasons.append(f"热门板块({hot_industries[industry]}只)+12")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 6
        reasons.append(f"板块联动({hot_industries[industry]}只)+6")

    # 6b. 题材热度（来自 AI 题材聚类缓存）
    theme_bonus, theme_reason = _shared.theme_bonus(code, industry, compare_context)
    if theme_bonus > 0:
        score += theme_bonus
        if theme_reason:
            reasons.append(theme_reason)

    # 6c. 资金面：龙虎榜
    flow_bonus, flow_reasons = _shared.capital_flow_bonus(code, compare_context)
    if flow_bonus != 0:
        score += flow_bonus
        reasons.extend(flow_reasons)

    # 7. 换手率
    if turnover is not None:
        if 5 <= turnover <= 15:
            score += 6
            reasons.append(f"换手{turnover:.1f}%健康+6")
        elif 15 < turnover <= 25:
            score += 2
            reasons.append(f"换手{turnover:.1f}%偏高+2")
        elif turnover > 30:
            score -= 6
            reasons.append(f"换手{turnover:.1f}%过热-6")
        elif turnover < 1.5:
            score -= 4
            reasons.append(f"换手{turnover:.1f}%偏冷-4")

    # 8. 大盘环境调节：晋级率高时稍加分，低时减分
    latest_cont_rate = compare_context.get("latest_continuation_rate")
    if latest_cont_rate is not None:
        if latest_cont_rate >= 60:
            score += 5
            reasons.append(f"昨日晋级率{latest_cont_rate:.0f}%+5")
        elif latest_cont_rate < 25:
            score -= 5
            reasons.append(f"昨日晋级率{latest_cont_rate:.0f}%-5")

    # 9. 股性活跃度（近 60 日任意涨停次数）：有涨停记录的股更易再次涨停，僵尸股惩罚
    occ_count, last_hit_days = _count_historical_any_limit_up(
        history, code, lookback_days=60, threshold_fn=threshold_fn,
    )
    if occ_count >= 5:
        stock_bonus, label = 6, "妖股性"
    elif occ_count >= 3:
        stock_bonus, label = 4, "股性活跃"
    elif occ_count >= 1:
        stock_bonus, label = 2, "曾涨停"
    else:
        stock_bonus, label = -3, "僵尸股"
    if stock_bonus > 0 and last_hit_days is not None and last_hit_days <= 20:
        stock_bonus = min(stock_bonus + 1, 6)
        reasons.append(f"近60日{occ_count}次涨停{label}(最近{last_hit_days}日){stock_bonus:+d}")
    elif stock_bonus > 0:
        reasons.append(f"近60日{occ_count}次涨停{label}{stock_bonus:+d}")
    else:
        reasons.append(f"近60日无涨停{label}{stock_bonus:+d}")
    score += stock_bonus

    final_score = max(0, min(100, int(round(score))))
    return {
        "code": code,
        "name": name,
        "industry": industry,
        "close": latest_close,
        "change_pct": change_pct,
        "turnover": turnover,
        "ma5": ma5_val,
        "dist_ma5_pct": dist_ma5_pct,
        "volume_ratio": vol_ratio,
        "trend_5d": trend_5d,
        "position_60d": position_60d,
        "cooldown_days": cooldown_days,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": "首板涨停",
    }
