"""保留涨停（cont）评分。

2 个模块级函数（参数注入模式）：
- score_continuation: 主评分（涨停股技术形态 + 板块热度 + 量价 + 历史命中）
- score_continuation_by_compare: 基于涨停对比的辅助评分（环境定盘）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_fn / build_local_cache_history_plan_fn /
classify_limit_up_pattern_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring.helpers import _count_historical_continuation
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


def score_continuation(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
) -> Dict[str, Any]:
    """对涨停股进行连板延续评分。满分100。

    迁自 StockFilter._score_continuation；行为零变化。
    """
    code = rec["code"]
    name = rec.get("name", "")
    score = 0.0
    reasons: List[str] = []

    boards = rec.get("consecutive_boards", 1)
    break_count = rec.get("break_count", 0)
    board_amount = rec.get("board_amount")
    first_time = rec.get("first_board_time", "")
    industry = rec.get("industry", "")
    turnover = rec.get("turnover")
    accumulation_score = 0
    accumulation_risk_penalty = 0
    accumulation_metrics: Dict[str, Any] = {"accumulation_days": 30}

    # 1. 连板数基础分
    # 数据反馈：cont 主类 17.7% 命中，首板 (cont_1to2) 仅 14.5%；
    # 多连板（boards>=2）命中率显著更高（ratio 2.10 正指），加大区分度
    if boards >= 5:
        score += 35
        reasons.append(f"{boards}连板+35")
    elif boards >= 3:
        score += 30
        reasons.append(f"{boards}连板+30")
    elif boards == 2:
        score += 25
        reasons.append("2连板+25")
    else:
        score += 5
        reasons.append("首板+5")

    # 1b. 开盘溢价惩罚 —— 2026-05-29 实盘"开盘价买入"约束：
    # 用户实盘买不到盘中低点，按开盘价追买是真实可达口径。
    # 27 天回测 avg_oc (按板数)：
    #   1板 -0.31% / 2板 -0.70% / 3板 -2.33% / 4板 +0.14%(n=16噪声) / 5+板 -3.15%
    # strict 命中率（次日涨停）随板数单调上升 14%→43%，但开盘溢价 3-6%
    # 把涨停红利全吃掉甚至倒亏。按板数单调减分对冲基础分的"强势加分"，
    # 让最终分数反映可达 PnL 而非纯命中率。
    if boards >= 5:
        score -= 15
        reasons.append(f"{boards}板开盘溢价大-15")
    elif boards == 4:
        score -= 10
        reasons.append("4板开盘溢价偏大-10")
    elif boards == 3:
        score -= 8
        reasons.append("3板开盘溢价偏大-8")
    elif boards == 2:
        score -= 3
        reasons.append("2板开盘溢价小-3")
    # 1 板 avg_oc -0.31% 几乎可忽略，不减分

    # 2. 封板强度（炸板次数少、封板时间早）
    if break_count == 0:
        score += 15
        reasons.append("未炸板+15")
    elif break_count == 1:
        score += 8
        reasons.append("炸板1次+8")
    else:
        score -= 5
        reasons.append(f"炸板{break_count}次-5")

    if first_time:
        try:
            parts = first_time.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            seal_minutes = hour * 60 + minute
            if seal_minutes <= 9 * 60 + 35:
                score += 15
                reasons.append("秒板/早封+15")
            elif seal_minutes <= 10 * 60:
                score += 10
                reasons.append("上午早封+10")
            elif seal_minutes <= 11 * 60 + 30:
                score += 5
                reasons.append("上午封板+5")
            elif seal_minutes >= 14 * 60 + 30:
                # 2026-05-29 -5 → -15：cont_1to2 ≥60 分明细几乎全带"尾盘封板"标签
                # 仍能攒到 60+ 分，说明 -5 不足以压住。尾盘封板=资金不愿打板/可能被埋，
                # 是连板延续最重要的负向信号。
                score -= 15
                reasons.append("尾盘封板-15")
        except (ValueError, IndexError):
            pass

    # 3. 板块热度加分（数据反馈：板块热是 cont 类正指，hit ratio 1.29）
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 13
        reasons.append(f"板块热({hot_industries[industry]}只)+13")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 7
        reasons.append(f"板块有{hot_industries[industry]}只+7")

    # 4. 换手率
    if turnover is not None:
        if 3 <= turnover <= 15:
            score += 5
            reasons.append(f"换手{turnover:.1f}%适中+5")
        elif turnover > 30:
            score -= 5
            reasons.append(f"换手{turnover:.1f}%过高-5")

    # 5. 量能和均线（历史数据已预取到缓存，直接读取）
    try:
        # 只使用本地缓存，不发起网络请求
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-continuation-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code, days=120, force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("预测续板获取历史 %s 失败: %s", code, exc)
        history = None
    if history is not None and not history.empty and len(history) >= 10:
        df = history.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()
        latest_ma5 = float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None
        latest_ma10 = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
        latest_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None

        if (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
                and latest_ma5 > latest_ma10 > latest_ma20):
            score += 10
            reasons.append("多头排列+10")

        # 量能（5 日 + 20 日双校验）
        # 数据反馈：1.0~3.0 量比适中 +5 无区分度（hit 27% / miss 74% 都满足），
        # 取消加分，只保留过大/假放量的负向信号
        t_idx = len(close) - 1
        vol_ratio, vol_ratio_20 = _shared.vol_ratio_with_baseline(volume, t_idx)
        if vol_ratio is not None:
            if vol_ratio > 5.0:
                score -= 5
                reasons.append(f"量比{vol_ratio:.1f}过大-5")
            if vol_ratio >= 1.5 and vol_ratio_20 is not None and vol_ratio_20 < 0.9:
                score -= 4
                reasons.append(f"5d量比{vol_ratio:.1f}x但20d{vol_ratio_20:.1f}x假放量-4")

        raw_accumulation_score, raw_accumulation_risk_penalty, accumulation_reasons, accumulation_metrics = (
            _score_accumulation_signal(close, volume, t_idx)
        )
        accumulation_score = int(round(raw_accumulation_score * 0.8))
        accumulation_risk_penalty = int(round(raw_accumulation_risk_penalty * 0.8))
        accumulation_metrics["accumulation_raw_score"] = raw_accumulation_score
        accumulation_metrics["accumulation_weight"] = 0.8
        if accumulation_score or accumulation_risk_penalty:
            score += accumulation_score + accumulation_risk_penalty
            if accumulation_score > 0:
                reasons.append(f"30日潜伏铺垫x0.8+{accumulation_score}")
            reasons.extend(accumulation_reasons)

    # === 历史同类形态加分：近 90 日内的连板成功次数 ===
    threshold_fn = limit_up_threshold_pct_fn or _default_limit_up_threshold_pct
    occ_count, last_hit_days = _count_historical_continuation(
        history, code, lookback_days=90,
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
            reasons.append(f"近90日{occ_count}次连板成功(最近{last_hit_days}日内)+{bonus}")
        else:
            reasons.append(f"近90日{occ_count}次连板成功+{bonus}")
        score += bonus

    final_score = max(0, min(100, int(round(score))))
    return {
        "code": code,
        "name": name,
        "industry": industry,
        "consecutive_boards": boards,
        "close": rec.get("close"),
        "change_pct": rec.get("change_pct"),
        "turnover": turnover,
        "break_count": break_count,
        "first_board_time": first_time,
        "board_amount": board_amount,
        "accumulation_score": accumulation_score,
        "accumulation_risk_penalty": accumulation_risk_penalty,
        **accumulation_metrics,
        "score": final_score,
        "reasons": " / ".join(reasons[:8]),
        "predict_type": "连板延续",
    }


def score_continuation_by_compare(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    log_fn: Optional[Callable[[str], None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    classify_limit_up_pattern_fn: Optional[Callable[..., Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """结合最近涨停对比环境，对今日涨停股评估次日保留涨停概率。

    迁自 StockFilter._score_continuation_by_compare；行为零变化。
    """
    base = score_continuation(
        rec, hot_industries,
        fetcher=fetcher,
        log_fn=log_fn,
        build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
    )
    score = float(base.get("score", 0))
    reasons = [r for r in str(base.get("reasons", "")).split(" / ") if r]

    boards = int(rec.get("consecutive_boards") or 1)
    latest_rate = compare_context.get("latest_continuation_rate")
    avg_rate = compare_context.get("avg_continuation_rate")

    ref_rate = latest_rate if latest_rate is not None else avg_rate
    if ref_rate is not None:
        if boards == 1:
            if ref_rate >= 35:
                score += 15
                reasons.append(f"首板晋级环境强({ref_rate:.1f}%)+15")
            elif ref_rate >= 25:
                score += 8
                reasons.append(f"首板晋级环境尚可({ref_rate:.1f}%)+8")
            elif ref_rate < 15:
                # 数据反馈：弱市首板继续涨停极少，加大惩罚
                score -= 15
                reasons.append(f"首板晋级环境弱({ref_rate:.1f}%)-15")
        else:
            if ref_rate >= 30:
                score += 8
                reasons.append(f"连板接力环境偏强({ref_rate:.1f}%)+8")
            elif ref_rate < 15:
                # 大盘连板延续率 <15% 是典型见顶信号，权重要压过单股技术面。
                # 高位（boards>=3）逆环境继续涨停的概率更低，再叠一刀。
                score -= 10
                reasons.append(f"接力环境偏冷({ref_rate:.1f}%)-10")
                if boards >= 3:
                    score -= 5
                    reasons.append(f"{boards}连板逆冷环境-5")

    if boards == 1:
        if classify_limit_up_pattern_fn is not None:
            pattern = classify_limit_up_pattern_fn(
                rec["code"],
                stock_name=rec.get("name", ""),
            ).get("pattern", "")
        else:
            pattern = ""
        # 数据反馈：cont 类样本中"趋势加速涨停"是最强反指（hit 9% vs miss 61%，
        # ratio 0.15, n=34）。高位趋势股的涨停往往是"诱多顶"，次日大概率冲高回落
        if pattern == "趋势加速涨停":
            score -= 10
            reasons.append("趋势加速首板诱多顶-10")
        elif pattern in {"回踩MA5涨停", "突破平台涨停"}:
            score += 8
            reasons.append(f"{pattern}+8")
        elif pattern == "暴量涨停":
            score -= 5
            reasons.append("暴量首板次日分歧-5")

    # 题材热度加分（来自 AI 题材聚类缓存）
    theme_bonus, theme_reason = _shared.theme_bonus(
        rec.get("code", ""), rec.get("industry", ""), compare_context
    )
    if theme_bonus > 0:
        score += theme_bonus
        if theme_reason:
            reasons.append(theme_reason)

    theme_fund_bonus, theme_fund_reasons = _shared.theme_fund_bonus(
        rec.get("code", ""), rec.get("industry", ""), compare_context
    )
    if theme_fund_bonus:
        score += theme_fund_bonus
        reasons.extend(theme_fund_reasons)

    # 板块联动（行业涨跌幅加分）
    flow_bonus, flow_reasons = _shared.capital_flow_bonus(
        rec.get("code", ""), compare_context,
        industry=rec.get("industry", ""), boards=boards,
    )
    if flow_bonus != 0:
        score += flow_bonus
        reasons.extend(flow_reasons)

    # ============== 游资视角增强信号 ==============
    # 1. 龙头身份：板块独苗最高板 +10；并列最高 +5
    ind = str(rec.get("industry") or "").strip()
    ind_max = compare_context.get("industry_max_boards", {}).get(ind, 0)
    code = str(rec.get("code") or "").strip()
    if ind and ind_max > 0 and boards == ind_max and boards >= 2:
        top_codes = compare_context.get("industry_top_codes", {}).get(ind) or set()
        if len(top_codes) == 1 and code in top_codes:
            score += 10
            reasons.append(f"板块独苗{boards}板龙头+10")
        elif len(top_codes) >= 2 and code in top_codes:
            score += 5
            reasons.append(f"板块并列{boards}板龙头({len(top_codes)}只)+5")

    # 1.5. 市场板位：当前是否就是全市场最高板
    market_max = int(compare_context.get("market_max_boards") or 0)
    if market_max > 0 and boards >= 2:
        if boards == market_max:
            score += 8
            reasons.append(f"市场最高{boards}板+8")
        elif market_max - boards >= 2:
            score -= 4
            reasons.append(f"距市场最高板差{market_max - boards}板-4")

    # 2. 题材阶段：萌芽/主升 加分；末期/退潮 重扣
    phase = (compare_context.get("code_to_concept_phase") or {}).get(code, "")
    if phase == "萌芽":
        score += 5
        reasons.append("题材萌芽阶段+5")
    elif phase == "主升":
        score += 3
        reasons.append("题材主升阶段+3")
    elif phase == "末期":
        score -= 8
        reasons.append("题材末期阶段-8")
    elif phase == "退潮":
        score -= 15
        reasons.append("题材退潮阶段-15")

    # 3. 情绪定盘 —— 2026-05-29 把"火爆 +5"改成分档：
    # cont_1to2 ≥60 分明细几乎每条都带"情绪火爆 94 +5"但 strict 命中率仅 6.5%（vs <60 14.5%）。
    # 95+ 通常是市场顶部信号，把这部分翻转为减分。
    sent_score = int(compare_context.get("sentiment_score") or 50)
    if sent_score < 35:
        score -= 15
        reasons.append(f"情绪冰点{sent_score}-15")
    elif sent_score < 50:
        score -= 7
        reasons.append(f"情绪偏冷{sent_score}-7")
    elif sent_score >= 95:
        score -= 5
        reasons.append(f"情绪过热{sent_score}-5")
    elif sent_score >= 85:
        score += 0
        reasons.append(f"情绪火爆{sent_score}+0")
    elif sent_score >= 70:
        score += 3
        reasons.append(f"情绪温热{sent_score}+3")

    final_score = max(0, min(100, int(round(score))))
    base["score"] = final_score
    base["reasons"] = " / ".join(reasons[:12])
    base["predict_type"] = "保留涨停"
    return base
