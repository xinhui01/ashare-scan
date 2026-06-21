"""首板涨停（fresh）评分 —— "资金接入型"策略（2026-06 改造）。

2 个模块级函数（参数注入模式）：
- scan_fresh_first_board_candidates_cached: 从"资金接入型"入口池扫候选并逐只评分（带冷却期过滤）
- score_fresh_first_board: 主评分（冷却期 + 资金接入(量能) + 止跌 + 板块题材联动 + 股性/盘子）

策略要点：按"资金有没有进来"选票，不按"位置高低"选票。入口 D-1 涨幅 [-4%,+5%]
（覆盖实测 81% 前一天不强势的首板），主信号=资金接入(量能)+止跌，强加权=板块题材联动。

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_pct_fn / build_local_cache_history_plan_fn /
filter_candidates_fn（注入 first_board.filter_capital_inflow_candidates）。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring import fresh_calibration as _fresh_calibration
from src.services.scoring.helpers import (
    _count_historical_any_limit_up,
    detect_stop_falling,
    detect_volume_ignition,
)
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
    filter_candidates_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """识别"资金接入型首板"候选：近期未涨停、今日止跌 + 资金进场、明日有望首封。

    策略（2026-06：从"强势突破型"改造为"资金接入型"）：实测 81% 的真实首板，
    涨停前一天涨幅 < +3%（中位 -0.3%），旧的 +3~9.5% 强势入口漏掉八成。改为
    按"资金有没有进来"选票、不按"位置高低"选票——入口放宽到 D-1 涨幅 [-4%,+5%]
    （由 filter_candidates_fn 注入，见 first_board.filter_capital_inflow_candidates），
    评分以"资金接入(量能) + 止跌"为主、"板块题材联动"为强加权。
    """
    if spot_df is None or spot_df.empty:
        return []

    if filter_candidates_fn is None:
        # 未注入入口筛选函数时无法继续（上层 predict.py 负责注入）
        return []

    merged: List[Dict[str, Any]] = []
    seen: set = set()
    for rec in filter_candidates_fn(spot_df, zt_codes):
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

    calibration_rules = compare_context.get("fresh_calibration_rules") or {}
    if isinstance(calibration_rules, dict) and calibration_rules:
        candidates = [
            _fresh_calibration.calibrate_fresh_candidate(
                item, calibration_rules, min_samples=20,
            )
            for item in candidates
        ]
        candidates.sort(
            key=lambda x: (
                str(x.get("confidence") or "") == "涨停高置信",
                float(x.get("calibrated_hit_rate") or 0.0),
                int(x.get("calibrated_score") or x.get("score") or 0),
                int(x.get("score") or 0),
            ),
            reverse=True,
        )
    else:
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
    """资金接入型首板评分：近期未涨停、今日"止跌 + 资金进场"、明日有望首封。

    强制条件：最近 cooldown_days 个交易日内不存在涨停过（才算"首板"）。命中冷却期返回 None。

    2026-06 策略改造（从"强势突破型"翻成"资金接入型"）：
    旧 fresh 模"今日 +3~9.5% 强势 + 多头排列"，但实测最近 30 天 81% 的真实首板，
    涨停前一天涨幅 < +3%（中位 -0.3%），旧入口漏掉八成；"超跌+资金接入"型首板
    占比一个月 23%→最近一周 56%，旧逻辑还给这类空头/破位票扣分。

    新评分按"资金有没有进来"选票、不按"位置高低"选票：
    - 主信号：资金接入（量比放大 / 地量启动）+ 止跌（不创新低 / 长下影·十字星 / 收复MA5）
    - 强加权：板块题材联动（同板块今日涨停家数）+ 题材发酵阶段
    - 次加权：股性（曾涨停，不再惩罚僵尸冷票）、流通盘适中、换手健康
    - 降权：高位放量（出货嫌疑）、大盘情绪冰点；位置只用于扣分不做主筛

    注：fund_flow 表覆盖率不足（每日仅 ~8 只、且数据陈旧），资金接入退化为纯量能口径。
    评估口径见复盘定位——主看"识别形态 vs 当日真实首板形态吻合度"，非 avg_oc PnL。
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

    # === 主信号①：资金接入（纯量能口径——fund_flow 覆盖率不足已弃用）===
    vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t)
    if vol_ratio is not None:
        if vol_ratio >= 2.0:
            score += 14
            reasons.append(f"放量资金进{vol_ratio:.1f}x+14")
        elif vol_ratio >= 1.5:
            score += 10
            reasons.append(f"放量资金进{vol_ratio:.1f}x+10")
        elif vol_ratio >= 1.2:
            score += 5
            reasons.append(f"温和资金进{vol_ratio:.1f}x+5")
        elif vol_ratio < 0.8:
            score -= 8
            reasons.append(f"缩量无资金{vol_ratio:.1f}x-8")
        # 5d 放量但 20d 仍缩 = 相对前 5 天的假放量
        if vol_ratio >= 1.3 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
            score -= 6
            reasons.append(f"5d{vol_ratio:.1f}x但20d仅{vol_ratio_20:.1f}x假放量-6")

    ignition = detect_volume_ignition(history)
    if ignition["ignited"]:
        score += 8
        reasons.append(ignition["label"] + "+8")

    # === 主信号②：止跌企稳（先走弱、今日见止跌迹象——资金低位介入的时机）===
    stop = detect_stop_falling(history)
    if stop["stabilizing"]:
        # 有反转 K 线(锤子/十字/收复MA5)才给满分；仅"不创新低"无反转形态止跌偏弱，降权
        strong_turn = stop["hammer"] or stop["doji"] or stop["reclaim_ma5"]
        stab = min(10 + (3 if stop["reclaim_ma5"] else 0), 13) if strong_turn else 5
        score += stab
        reasons.append((stop["label"] or "止跌企稳") + f"+{stab}")
    elif stop["prior_weak"] and not stop["no_new_low"]:
        # 仍在下跌且今日续创新低 = 接飞刀
        score -= 6
        reasons.append("下跌中续创新低-6")

    # === 强加权：板块题材联动（同板块今日涨停家数——比单票资金更难造假）===
    # 候选 industry 来自 spot（universe 证监会粗命名），跟 hot_industries（涨停池东财
    # 窄命名）实测 0% 对得上；改用 limit_up_stock_meta 的东财行业（与涨停池 100% 同命名、
    # 覆盖曾涨停过的有股性票）映射后再查，否则板块联动信号全是死的。
    em_industry_map = compare_context.get("em_industry_map") or {}
    link_industry = em_industry_map.get(code) or industry
    hot = hot_industries.get(link_industry, 0) if link_industry else 0
    if hot >= 4:
        score += 16
        reasons.append(f"同板块今日{hot}涨停+16")
    elif hot == 3:
        score += 12
        reasons.append(f"同板块今日{hot}涨停+12")
    elif hot == 2:
        score += 8
        reasons.append(f"同板块今日{hot}涨停+8")

    theme_bonus, theme_reason = _shared.theme_bonus(code, link_industry, compare_context)
    if theme_bonus > 0:
        score += theme_bonus
        if theme_reason:
            reasons.append(theme_reason)

    # 题材发酵阶段（萌芽/主升 顺风、末期/退潮 逆风）——之前算好但 fresh 没用上
    phase = (compare_context.get("code_to_concept_phase") or {}).get(code)
    if phase in ("萌芽", "主升"):
        score += 4
        reasons.append(f"题材{phase}期+4")
    elif phase in ("末期", "退潮"):
        score -= 4
        reasons.append(f"题材{phase}期-4")

    # === 次加权：股性（曾涨停加分；不再对僵尸冷票惩罚——资金接入型常是冷票）===
    occ_count, last_hit_days = _count_historical_any_limit_up(
        history, code, lookback_days=60, threshold_fn=threshold_fn,
    )
    if occ_count >= 3:
        stock_bonus, label = 8, "股性活跃"
    elif occ_count >= 1:
        stock_bonus, label = 4, "曾涨停"
    else:
        stock_bonus, label = 0, "冷票"
    if stock_bonus > 0 and last_hit_days is not None and last_hit_days <= 20:
        stock_bonus = min(stock_bonus + 2, 10)
    if stock_bonus > 0:
        score += stock_bonus
        reasons.append(f"近60日{occ_count}次涨停{label}+{stock_bonus}")

    # === 次加权：流通盘（游资做首板偏中小盘，百亿大盘难封）===
    float_mcap = rec.get("float_mcap")
    if float_mcap:
        yi = float_mcap / 1e8
        if yi <= 50:
            score += 4
            reasons.append(f"小盘{yi:.0f}亿易封+4")
        elif yi <= 150:
            score += 2
            reasons.append(f"盘子{yi:.0f}亿适中+2")
        elif yi >= 400:
            score -= 4
            reasons.append(f"大盘{yi:.0f}亿难封-4")

    # === 次：换手健康度 ===
    if turnover is not None:
        if 3 <= turnover <= 15:
            score += 4
            reasons.append(f"换手{turnover:.1f}%健康+4")
        elif turnover > 30:
            score -= 6
            reasons.append(f"换手{turnover:.1f}%过热-6")
        elif turnover < 1.0:
            score -= 3
            reasons.append(f"换手{turnover:.1f}%枯竭-3")

    # === 降权：位置只用于扣分（不做主筛、不正向奖励低位）===
    ma5 = close.rolling(5, min_periods=5).mean()
    ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
    dist_ma5_pct = None
    if ma5_val and ma5_val > 0 and latest_close is not None:
        dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)

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
            score -= 8
            reasons.append(f"60日位置{position_60d:.0f}%过高-8")
        elif position_60d >= 85 and vol_ratio is not None and vol_ratio >= 1.5:
            score -= 8
            reasons.append(f"高位{position_60d:.0f}%放量出货嫌疑-8")

    # === 降权：大盘情绪冰点 / 晋级率低 ===
    sent_score = int(compare_context.get("sentiment_score") or 50)
    if sent_score < 35:
        score -= 10
        reasons.append(f"情绪冰点{sent_score}-10")
    elif sent_score < 50:
        score -= 5
        reasons.append(f"情绪偏冷{sent_score}-5")

    latest_cont_rate = compare_context.get("latest_continuation_rate")
    if latest_cont_rate is not None and latest_cont_rate < 25:
        score -= 5
        reasons.append(f"昨日晋级率{latest_cont_rate:.0f}%低-5")

    # trend_5d 仅用于输出展示（不参与评分）
    trend_5d = None
    if t >= 5 and not pd.isna(close.iloc[t - 5]) and float(close.iloc[t - 5]) > 0 and latest_close is not None:
        trend_5d = round((latest_close / float(close.iloc[t - 5]) - 1) * 100, 1)

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
        "stabilizing": stop["stabilizing"],
        "volume_ignited": ignition["ignited"],
        "accumulation_score": accumulation_score,
        "accumulation_risk_penalty": accumulation_risk_penalty,
        **accumulation_metrics,
        "score": final_score,
        "reasons": " / ".join(reasons[:10]),
        "predict_type": "首板涨停",
    }
