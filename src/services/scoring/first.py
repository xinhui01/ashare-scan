"""二波接力（first）评分。

2 个模块级函数（参数注入模式）：
- scan_followthrough_candidates_cached: 从今日强势股池扫候选并逐只评分
- score_followthrough_candidate: 主评分（含历史命中、量比、距 MA5、前涨停日形态等多维度）

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
limit_up_threshold_fn / build_local_cache_history_plan_fn /
filter_strong_stocks_fn（P10 才迁到 first_board.py，本阶段仍通过参数注入）。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.services.scoring import shared as _shared
from src.services.scoring.helpers import _count_historical_followthrough

logger = logging.getLogger(__name__)


def _default_limit_up_threshold_pct(code: str) -> float:
    """A股各板块涨停阈值（百分比）。fallback 用，与 stock_filter._limit_up_threshold_pct 同。"""
    c = (code or "").strip()
    if c.startswith(("30", "68")):
        return 19.5
    if c.startswith(("43", "83", "87", "88", "92")):
        return 29.5
    return 9.5


def scan_followthrough_candidates_cached(
    hot_industries: Dict[str, int],
    spot_df: Optional[pd.DataFrame],
    zt_codes: set,
    compare_context: Dict[str, Any],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    fetcher,
    lookback_days: int = 5,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
    filter_strong_stocks_fn: Optional[Callable[..., List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    """从今日强势股中识别二波接力候选。

    新规则要求当日涨幅 ≥+4%，所以只取 filter_strong_stocks（3.0%~9.95%）
    作为输入，不再合并回踩股票（那些 change_pct < +4 必然被硬过滤掉）。

    迁自 StockFilter._scan_followthrough_candidates_cached；行为零变化。
    """
    if spot_df is None or spot_df.empty:
        return []

    if filter_strong_stocks_fn is None:
        # 没有注入强势股筛选函数时无法继续；保持原行为（原方法必然依赖 self._filter_strong_stocks）
        return []
    merged = filter_strong_stocks_fn(spot_df, zt_codes)
    if not merged:
        return []

    candidates: List[Dict[str, Any]] = []
    total = len(merged)
    for idx, rec in enumerate(merged):
        score_info = score_followthrough_candidate(
            rec,
            hot_industries,
            compare_context,
            fetcher=fetcher,
            lookback_days=lookback_days,
            log_fn=log_fn,
            limit_up_threshold_pct_fn=limit_up_threshold_pct_fn,
            build_local_cache_history_plan_fn=build_local_cache_history_plan_fn,
        )
        if score_info is not None and score_info["score"] >= 50:
            candidates.append(score_info)
        if progress_callback:
            progress_callback(idx + 1, total, f"二波接力 {rec['code']} {rec.get('name', '')}")

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:50]


def score_followthrough_candidate(
    rec: Dict[str, Any],
    hot_industries: Dict[str, int],
    compare_context: Dict[str, Any],
    *,
    fetcher,
    lookback_days: int = 5,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_pct_fn: Optional[Callable[[str], float]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    """二波接力候选评分：识别"近期涨停过 + 今日处于接力窗口"形态。

    设计目标：预测**次日涨停**。

    基于 1118 只历史涨停股 T 日特征统计的发现：
    - 真实涨停前一日，67% 涨幅在 -5%~+4%（潜伏/小阴）
    - 89% 量比 < 2（缩量或温和放量）
    - 82% 收盘强势 ≥ 当日高点 × 0.96
    - 28% 距前涨停 ≤ 5 日（这部分就是"二波接力"的目标）

    所以放宽硬过滤，靠评分识别真正强的：

    硬性过滤（缺一不返回）：
    1. 当日涨幅 ∈ [-3%, +9.5%)（覆盖潜伏与启动两类，>9.5% 让保留涨停处理）
    2. 距前涨停日 ∈ [1, 5]（这是接力窗口的核心定义）
    3. 距 MA5 ∈ [-5%, +12%]（不能跌破太深也不能位置过高）
    4. 量比 ≥ 0.7（避免极度缩量阴跌）
    5. 收盘 ≥ 当日最高 × 0.93（不要尾盘跳水）

    迁自 StockFilter._score_followthrough_candidate；行为零变化。
    """
    threshold_fn = limit_up_threshold_pct_fn or _default_limit_up_threshold_pct

    code = rec["code"]
    name = rec.get("name", "")
    change_pct = rec.get("change_pct")
    turnover = rec.get("turnover")
    industry = rec.get("industry", "")
    score = 0.0
    reasons: List[str] = []

    try:
        request_plan = (
            build_local_cache_history_plan_fn(reason="predict-followthrough-cache-only")
            if build_local_cache_history_plan_fn is not None
            else None
        )
        history = fetcher.get_history_data(
            code,
            days=120,
            force_refresh=False,
            request_plan=request_plan,
        )
    except Exception as exc:
        logger.debug("预测二波接力获取历史 %s 失败: %s", code, exc)
        history = None

    latest_close = rec.get("close")
    latest_high = None
    latest_low = None
    ma5_val = None
    ma10_val = None
    ma20_val = None
    dist_ma5_pct = None
    volume_ratio_today = None  # 今日量比（基于前 5 日均量）
    prior_lu_idx = None        # 最近一次收盘涨停的索引
    days_since_prior_lu = None
    prior_lu_date = ""
    had_limit_up_60d = False
    breakout_20d = False       # 是否突破近 20 日新高
    above_ma10 = False
    position_60d = None

    if history is not None and not history.empty and len(history) >= 10:
        df = history.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"], errors="coerce")
        high = pd.to_numeric(df.get("high"), errors="coerce") if "high" in df.columns else pd.Series(dtype=float)
        low = pd.to_numeric(df.get("low"), errors="coerce") if "low" in df.columns else pd.Series(dtype=float)
        volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)

        t = len(df) - 1
        latest_close = float(close.iloc[t]) if not pd.isna(close.iloc[t]) else latest_close
        latest_high = float(high.iloc[t]) if not high.empty and not pd.isna(high.iloc[t]) else None
        latest_low = float(low.iloc[t]) if not low.empty and not pd.isna(low.iloc[t]) else None

        ma5 = close.rolling(5, min_periods=5).mean()
        ma10 = close.rolling(10, min_periods=10).mean()
        ma20 = close.rolling(20, min_periods=20).mean()
        ma5_val = float(ma5.iloc[t]) if not pd.isna(ma5.iloc[t]) else None
        ma10_val = float(ma10.iloc[t]) if not pd.isna(ma10.iloc[t]) else None
        ma20_val = float(ma20.iloc[t]) if not pd.isna(ma20.iloc[t]) else None

        if latest_close is not None and ma5_val is not None and ma5_val > 0:
            dist_ma5_pct = round((latest_close / ma5_val - 1) * 100, 2)
        if latest_close is not None and ma10_val is not None:
            above_ma10 = latest_close >= ma10_val

        # 今日量比（基于前 5 日均量）
        if not volume.empty and not pd.isna(volume.iloc[t]) and t >= 5:
            prev_vol = volume.iloc[t - 5:t].dropna()
            if not prev_vol.empty and float(prev_vol.mean()) > 0:
                volume_ratio_today = round(float(volume.iloc[t]) / float(prev_vol.mean()), 2)

        # 突破近 20 日新高（不含今日）
        if t >= 20 and latest_close is not None and not high.empty:
            prior_20d_high = float(high.iloc[t - 20:t].max())
            if not pd.isna(prior_20d_high) and latest_close > prior_20d_high:
                breakout_20d = True

        # 60 日相对位置
        if t >= 30 and latest_close is not None and not low.empty and not high.empty:
            lo60 = float(low.iloc[max(0, t - 60):t + 1].min())
            hi60 = float(high.iloc[max(0, t - 60):t + 1].max())
            if hi60 > lo60:
                position_60d = round((latest_close - lo60) / (hi60 - lo60) * 100, 1)

        # 近 7 日内最近一次收盘涨停（用作"前涨停日"，决定接力窗口）
        zt_threshold = threshold_fn(code)
        scan_start = max(1, t - 7)
        for i in range(t - 1, scan_start - 1, -1):
            if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]):
                continue
            prev_c = float(close.iloc[i - 1])
            if prev_c <= 0:
                continue
            chg_i = (float(close.iloc[i]) / prev_c - 1) * 100
            if chg_i >= zt_threshold - 0.3:
                prior_lu_idx = i
                days_since_prior_lu = t - i
                prior_lu_date = str(df.iloc[i].get("date", "") or "")
                break

        # 近 60 日内是否有过任意收盘涨停
        if prior_lu_idx is not None:
            had_limit_up_60d = True
        else:
            zt_start = max(1, t - 60)
            for i in range(zt_start, t):
                if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]):
                    continue
                prev_c = float(close.iloc[i - 1])
                if prev_c <= 0:
                    continue
                chg_i = (float(close.iloc[i]) / prev_c - 1) * 100
                if chg_i >= zt_threshold - 0.3:
                    had_limit_up_60d = True
                    break

        # ============== 二波接力增强信号 ==============
        # A. 前一波"高度"：从 prior_lu_idx 往前数连续涨停的板数（顶 3 板见顶概率高）
        if prior_lu_idx is not None and prior_lu_idx >= 1:
            prior_wave_boards = 1
            for j in range(prior_lu_idx - 1, max(-1, prior_lu_idx - 8), -1):
                if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]):
                    break
                prev_c = float(close.iloc[j - 1])
                if prev_c <= 0:
                    break
                chg = (float(close.iloc[j]) / prev_c - 1) * 100
                if chg >= zt_threshold - 0.3:
                    prior_wave_boards += 1
                else:
                    break
        else:
            prior_wave_boards = 0

        # B/C. 前涨停日的高/低，用于回调深度 + 是否破前低
        prior_lu_high = None
        prior_lu_low = None
        prior_lu_open = None
        if prior_lu_idx is not None:
            if not high.empty and not pd.isna(high.iloc[prior_lu_idx]):
                prior_lu_high = float(high.iloc[prior_lu_idx])
            if not low.empty and not pd.isna(low.iloc[prior_lu_idx]):
                prior_lu_low = float(low.iloc[prior_lu_idx])
            if "open" in df.columns:
                o_val = pd.to_numeric(df["open"], errors="coerce").iloc[prior_lu_idx]
                if not pd.isna(o_val):
                    prior_lu_open = float(o_val)

        pullback_pct = None
        if prior_lu_high and prior_lu_high > 0 and latest_close is not None:
            pullback_pct = round((latest_close / prior_lu_high - 1) * 100, 2)
        broken_prior_lu_low = (
            latest_low is not None and prior_lu_low is not None
            and prior_lu_low > 0 and latest_low < prior_lu_low
        )

        # E. 窗口期内涨停次数（[t-5, t-1] 不含今日）
        window_lu_count = 0
        for j in range(max(1, t - 5), t):
            if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]):
                continue
            prev_c = float(close.iloc[j - 1])
            if prev_c <= 0:
                continue
            chg = (float(close.iloc[j]) / prev_c - 1) * 100
            if chg >= zt_threshold - 0.3:
                window_lu_count += 1

        # F. 前涨停日形态（一字 / T 字 / 普通）—— 烂板需分时数据，暂跳过
        prior_lu_pattern = "normal"
        if (prior_lu_idx is not None and prior_lu_open is not None
                and prior_lu_high is not None and prior_lu_low is not None
                and prior_lu_high > 0):
            p_close = float(close.iloc[prior_lu_idx])
            close_at_high = abs(p_close - prior_lu_high) / prior_lu_high < 0.005
            open_at_high = abs(prior_lu_open - prior_lu_high) / prior_lu_high < 0.005
            low_far_from_high = (prior_lu_high - prior_lu_low) / prior_lu_high > 0.05
            if close_at_high and open_at_high and not low_far_from_high:
                prior_lu_pattern = "一字"
            elif close_at_high and open_at_high and low_far_from_high:
                prior_lu_pattern = "T字"
    else:
        # 历史数据不足时给保守默认（不阻塞硬过滤；scoring 块会跳过这些维度）
        prior_wave_boards = 0
        pullback_pct = None
        broken_prior_lu_low = False
        window_lu_count = 0
        prior_lu_pattern = "normal"

    # ---- 硬性过滤（基于 1118 只历史涨停股 T 日特征统计放宽）----
    if change_pct is None or change_pct < -3.0 or change_pct >= 9.5:
        return None
    if days_since_prior_lu is None or not (1 <= days_since_prior_lu <= 5):
        return None
    if dist_ma5_pct is None or not (-5.0 <= dist_ma5_pct <= 12.0):
        return None
    if volume_ratio_today is None or volume_ratio_today < 0.7:
        return None
    # 不要尾盘跳水（高点回落超过 7% 排除）
    if latest_high is not None and latest_close is not None:
        if latest_high > 0 and latest_close < latest_high * 0.93:
            return None

    # 评分维度
    is_strong_close = (
        latest_high is not None and latest_close is not None
        and latest_high > 0 and latest_close >= latest_high * 0.96
    )

    # 1. 接力窗口（max 25，最重要的信号）
    relay_score_map = {1: 25, 2: 20, 3: 15, 4: 8, 5: 3}
    relay_bonus = relay_score_map.get(days_since_prior_lu, 0)
    if relay_bonus:
        score += relay_bonus
        reasons.append(f"距前涨停{days_since_prior_lu}日+{relay_bonus}")

    # 2. 当日表现 —— 2026-05-29 校准翻转：
    # 旧版"潜伏+收强 +22"基于 22 条早期样本（命中 30%+），但累积 589 条 first 样本后：
    #   - 潜伏+收强 (-1~+4% + 收强): hit 10.7% (n=122) ← 实际反指 -5.4%
    #   - 启动+收强 (4-9.5% + 收强):  hit 17.3% (n=255) ← 实际正指 +4.1%
    # 直接对调：启动+收强升至 +18（最强单项），潜伏+收强降到接近 0。
    if 4.0 <= change_pct < 9.5 and is_strong_close:
        score += 18
        reasons.append(f"启动+收强{change_pct:+.1f}%+18")
    elif -1.0 <= change_pct < 4.0 and is_strong_close:
        score += 0
        reasons.append(f"潜伏+收强{change_pct:+.1f}%+0")
    elif 4.0 <= change_pct < 9.5:
        score += 3
        reasons.append(f"启动但收弱{change_pct:+.1f}%+3")
    elif -3.0 <= change_pct < -1.0 and is_strong_close:
        score += 8
        reasons.append(f"小阴+收强{change_pct:+.1f}%+8")
    else:
        score += 0
        reasons.append(f"涨幅{change_pct:+.1f}%+0")

    # 3. 距涨停可达 —— 2026-05-29 收紧：
    #   - ≤4%: hit 19.1% (n=220) ← 全文最强正指 +6.6%，保留 15
    #   - 4-6%: hit 12.0% (n=283) ← 反指 -5.6%，归零
    #   - 6-8%: 样本里更弱，归零
    # 真正可达涨停才有意义，4% 以外的距离已经是"远"。
    lu_threshold = threshold_fn(code)
    room_to_lu = lu_threshold - change_pct
    if room_to_lu <= 4.0:
        score += 15
        reasons.append(f"距涨停剩{room_to_lu:.1f}%+15")
    elif room_to_lu <= 6.0:
        score += 0
        reasons.append(f"距涨停剩{room_to_lu:.1f}%+0")
    elif room_to_lu <= 8.0:
        score += 0
        reasons.append(f"距涨停剩{room_to_lu:.1f}%+0")

    # 4. 量价配合（max 12）— 爆量与"高涨幅"叠加时打折，防止"加速顶"被堆出高分
    if volume_ratio_today >= 3.0:
        if change_pct >= 4.0:
            score += 4
            reasons.append(f"爆量{volume_ratio_today:.1f}x+涨{change_pct:+.1f}%(透支)+4")
        else:
            score += 12
            reasons.append(f"爆量{volume_ratio_today:.1f}x+12")
    elif volume_ratio_today >= 1.5:
        score += 6
        reasons.append(f"温和放量{volume_ratio_today:.1f}x+6")
    elif 0.7 <= volume_ratio_today < 1.0 and is_strong_close:
        score += 5
        reasons.append(f"强势缩量整理{volume_ratio_today:.1f}x+5")
    # 1.0~1.5 不加分

    # 5. 情绪共振 —— 2026-05-29 减半：
    # "热门板块 +12" 实测反指 -3.7% (n=85)、"题材龙头≥20只" 反指 -3.4% (n=59)。
    # 高情绪共振往往出现在加速顶/见顶日，跟单股技术面冲突时往往是顶部信号。
    if industry and hot_industries.get(industry, 0) >= 3:
        score += 6
        reasons.append(f"热门板块({hot_industries[industry]}只)+6")
    elif industry and hot_industries.get(industry, 0) >= 2:
        score += 3
        reasons.append(f"板块联动({hot_industries[industry]}只)+3")

    theme_bonus, theme_reason = _shared.theme_bonus(code, industry, compare_context)
    if theme_bonus > 0:
        # 题材龙头数 ≥20 只时反指，封顶 4 避免过度加权
        score += min(theme_bonus, 4)
        if theme_reason:
            reasons.append(theme_reason)

    flow_bonus, flow_reasons = _shared.capital_flow_bonus(code, compare_context)
    if flow_bonus != 0:
        score += flow_bonus
        reasons.extend(flow_reasons)

    # 6. 形态加分 —— 2026-05-29 调整：
    # "突破20日新高" 实测反指 -3.8% (n=215)，归零（加速顶常见特征，不再加分）。
    # "站稳MA10" 无显著方向性 (+1.2% diff, p>0.05)，保留 +4。
    if breakout_20d:
        score += 0
        reasons.append("突破20日新高+0")
    if above_ma10:
        score += 4
        reasons.append("站稳MA10+4")

    # 7. 减分项
    if position_60d is not None and position_60d >= 95:
        score -= 8
        reasons.append(f"60日位置{position_60d:.0f}%过高-8")
    if change_pct <= -2.0:
        score -= 5
        reasons.append(f"当日跌{change_pct:+.1f}%-5")
    # 动能堆叠：高涨幅 + 突破20日 + 爆量 同时出现，多为"加速顶"信号
    # 数据反馈：满足全部三项的样本在 70+ 分段中 0 命中
    if (change_pct >= 4.0 and breakout_20d
            and volume_ratio_today is not None and volume_ratio_today >= 3.0):
        score -= 8
        reasons.append("启动+突破+爆量三连(加速顶)-8")

    # 换手率
    if turnover is not None:
        if 3 <= turnover <= 20:
            score += 4
            reasons.append(f"换手{turnover:.1f}%适中+4")
        elif turnover > 35:
            score -= 5
            reasons.append(f"换手{turnover:.1f}%过高-5")

    # ============== 二波接力增强信号评分 ==============
    # A. 前一波"高度"：1 板 +5 / 2 板 0 / ≥3 板 -5
    if prior_wave_boards == 1:
        score += 5
        reasons.append("前波首板+5")
    elif prior_wave_boards >= 3:
        score -= 5
        reasons.append(f"前波{prior_wave_boards}板高位-5")

    # B. 回调深度：-8~-3% 良性洗盘 +6 / -15~-8% 深回调 +3 / <-15% 形态破坏 -5
    if pullback_pct is not None:
        if -8.0 <= pullback_pct <= -3.0:
            score += 6
            reasons.append(f"良性回调{pullback_pct:+.1f}%+6")
        elif -15.0 <= pullback_pct < -8.0:
            score += 3
            reasons.append(f"深回调{pullback_pct:+.1f}%+3")
        elif pullback_pct < -15.0:
            score -= 5
            reasons.append(f"回调过深{pullback_pct:+.1f}%-5")

    # C. 破前涨停日最低价：短线趋势走坏
    if broken_prior_lu_low:
        score -= 8
        reasons.append("破前涨停日低点-8")

    # E. 窗口期涨停次数 ≥2：已是连板股，不该走二波逻辑
    if window_lu_count >= 2:
        score -= 8
        reasons.append(f"5日内涨停{window_lu_count}次(连板股)-8")

    # F. 前涨停日形态：一字板抢筹激烈 -5 / T 字板 -2
    if prior_lu_pattern == "一字":
        score -= 5
        reasons.append("前波一字板抢筹激烈-5")
    elif prior_lu_pattern == "T字":
        score -= 2
        reasons.append("前波T字板-2")

    # D. 情绪定盘联动 —— 2026-05-29 把"火爆 +5"改成分档：
    # 实测含"情绪火爆"标签 n=85 hit 11.8% vs 不含 hit 15.5%（diff -3.7%）。
    # cont_1to2 ≥60 分明细几乎每条都带"情绪火爆 94"，>=95 通常是市场顶部反指。
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

    # === 历史同类形态加分：近 90 日内的二波接力成功次数 ===
    occ_count, last_hit_days = _count_historical_followthrough(
        history, code, lookback_days=90, window=5,
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
            reasons.append(f"近90日{occ_count}次二波接力成功(最近{last_hit_days}日内)+{bonus}")
        else:
            reasons.append(f"近90日{occ_count}次二波接力成功+{bonus}")
        score += bonus

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
        "volume_ratio": volume_ratio_today,
        # 兼容旧字段名（GUI 仍使用 burst_date / days_since_burst）
        "burst_date": prior_lu_date,
        "days_since_burst": days_since_prior_lu,
        "prior_lu_date": prior_lu_date,
        "days_since_prior_lu": days_since_prior_lu,
        "is_strong_close": is_strong_close,
        "breakout_20d": breakout_20d,
        "position_60d": position_60d,
        "prior_wave_boards": prior_wave_boards,
        "pullback_pct": pullback_pct,
        "broken_prior_lu_low": broken_prior_lu_low,
        "window_lu_count": window_lu_count,
        "prior_lu_pattern": prior_lu_pattern,
        "score": final_score,
        "reasons": " / ".join(reasons[:10]),
        "predict_type": "二波接力",
    }
