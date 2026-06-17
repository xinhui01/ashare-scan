"""K 线历史形态统计 helper：识别"成功二波接力/连板/反包"形态 + 任意涨停次数，
以及"资金接入型首板"用的止跌企稳 / 地量启动单日信号。

- _count_historical_* 4 个：输入 history DataFrame + 配置，输出 (occurrence_count, days_since_last_hit)。
- detect_stop_falling / detect_volume_ignition：输入 history DataFrame，输出当日信号 dict。
所有函数无状态。
"""
from __future__ import annotations

from typing import Callable, Optional, Tuple

import pandas as pd


def _count_historical_continuation(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    threshold_fn=None,
):
    """扫历史 K 线统计成功连板次数（涨停 → T+1 继续涨停）。

    跳过最后一行（today），避免今日数据自计。

    返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1  # today index (skip)
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    occ = 0
    last_hit_idx = None
    for i in range(cutoff_idx, t - 1):
        # i: 涨停日? i+1: 次日继续涨停?
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]):
            continue
        if float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i < threshold - 0.3:
            continue
        if pd.isna(close.iloc[i + 1]) or float(close.iloc[i]) <= 0:
            continue
        chg_next = (float(close.iloc[i + 1]) / float(close.iloc[i]) - 1) * 100
        if chg_next >= threshold - 0.3:
            occ += 1
            last_hit_idx = i + 1
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def _count_historical_any_limit_up(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 60,
    threshold_fn=None,
):
    """扫历史 K 线统计近 N 日内任意涨停次数（不要求 T+1 连板），作为"股性活跃度"代理。

    用于首板评分：凡历史涨停过的股更易再次涨停；长期不涨停的"僵尸股"首板成功率低。
    与 _count_historical_continuation 的区别：这里只统计单次涨停事件，不要求 T+1 继续涨停。

    跳过最后一行（today），避免今日数据自计。

    返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 2:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1  # today index (skip)
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    occ = 0
    last_hit_idx = None
    for i in range(cutoff_idx, t):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]):
            continue
        if float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            occ += 1
            last_hit_idx = i
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def _count_historical_followthrough(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    threshold_fn=None,
):
    """扫历史 K 线统计成功二波接力次数（涨停 → window 日内另一次涨停）。

    跳过最后一行（today）。返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    # 找出所有涨停日 idx
    lu_indices = []
    for i in range(cutoff_idx, t):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            lu_indices.append(i)
    # 对每个涨停日 i，看 [i+1, min(i+window, t-1)] 内是否有再次涨停（i 之外的）
    occ = 0
    last_hit_idx = None
    for i in lu_indices:
        end = min(i + int(window), t - 1)
        for j in range(i + 1, end + 1):
            if j in lu_indices:
                occ += 1
                last_hit_idx = j
                break
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def _count_historical_wrap(
    history_df: "pd.DataFrame",
    code: str,
    lookback_days: int = 90,
    window: int = 5,
    drop_threshold: float = -3.0,
    threshold_fn=None,
):
    """扫历史 K 线统计成功反包次数（涨停 → window 日内 ≤drop 阴线 → 再涨停）。

    跳过最后一行（today）。返回 (occurrence_count, days_since_last_hit)。
    """
    if history_df is None or len(history_df) < 3:
        return (0, None)
    if threshold_fn is None:
        def threshold_fn(_c):
            return 10.0
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    n = len(df)
    t = n - 1
    threshold = float(threshold_fn(code))
    cutoff_idx = max(1, t - int(lookback_days))
    # 找出所有涨停日 idx
    lu_indices = []
    for i in range(cutoff_idx, t):
        if pd.isna(close.iloc[i]) or pd.isna(close.iloc[i - 1]) or float(close.iloc[i - 1]) <= 0:
            continue
        chg_i = (float(close.iloc[i]) / float(close.iloc[i - 1]) - 1) * 100
        if chg_i >= threshold - 0.3:
            lu_indices.append(i)
    if len(lu_indices) < 2:
        return (0, None)
    # 对每对相邻涨停日 (a, b)，b - a ≤ window，且 (a, b) 之间至少一根 ≤ drop_threshold 阴线
    occ = 0
    last_hit_idx = None
    for k in range(len(lu_indices) - 1):
        a = lu_indices[k]
        b = lu_indices[k + 1]
        if b - a > int(window):
            continue
        # a 与 b 之间有阴线 ≤ drop_threshold
        has_drop = False
        for j in range(a + 1, b):
            if pd.isna(close.iloc[j]) or pd.isna(close.iloc[j - 1]) or float(close.iloc[j - 1]) <= 0:
                continue
            chg_j = (float(close.iloc[j]) / float(close.iloc[j - 1]) - 1) * 100
            if chg_j <= drop_threshold:
                has_drop = True
                break
        if has_drop:
            occ += 1
            last_hit_idx = b
    last_days = (t - last_hit_idx) if last_hit_idx is not None else None
    return (occ, last_days)


def detect_stop_falling(history_df: "pd.DataFrame", lookback: int = 5) -> dict:
    """止跌企稳信号（today = 最后一行）。

    用于"资金接入型首板"评分：先有一段走弱（prior_weak），今天出现止跌迹象
    （不创新低 / 长下影锤子 / 十字星 / 收复 MA5）→ 资金可能正在低位介入。

    返回 dict（缺数据时各项 False）：
      no_new_low / hammer / doji / reclaim_ma5 / prior_weak / stabilizing / label
    其中 ``stabilizing = prior_weak and (任一止跌迹象)``，是主判定；
    label 是给评分理由用的中文标签。
    """
    empty = {
        "no_new_low": False, "hammer": False, "doji": False,
        "reclaim_ma5": False, "prior_weak": False, "stabilizing": False,
        "label": "",
    }
    if history_df is None or len(history_df) < lookback + 2:
        return empty
    df = history_df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else pd.Series(dtype=float)
    high = pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else pd.Series(dtype=float)
    low = pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else pd.Series(dtype=float)
    t = len(df) - 1
    if pd.isna(close.iloc[t]):
        return empty
    c = float(close.iloc[t])
    o = float(open_.iloc[t]) if not open_.empty and not pd.isna(open_.iloc[t]) else c
    h = float(high.iloc[t]) if not high.empty and not pd.isna(high.iloc[t]) else c
    lo = float(low.iloc[t]) if not low.empty and not pd.isna(low.iloc[t]) else c
    rng = h - lo
    body = abs(c - o)
    lower_shadow = min(o, c) - lo

    # 前期走弱：近 10 日收益为负（下跌中 / 超跌背景，但不作硬条件）
    prior_weak = False
    if t >= 10 and not pd.isna(close.iloc[t - 10]) and float(close.iloc[t - 10]) > 0:
        prior_weak = (c / float(close.iloc[t - 10]) - 1) < 0.0

    # 不创新低：今日最低 > 前 lookback 日最低
    prior_low_win = low.iloc[max(0, t - lookback):t].dropna()
    no_new_low = bool(not prior_low_win.empty and lo > float(prior_low_win.min()))

    # 长下影（锤子）：下影 ≥ 2×实体 且收盘落在当日上半区
    hammer = bool(rng > 0 and lower_shadow >= 2 * body and (c - lo) >= 0.5 * rng)

    # 十字星：实体 ≤ 30% 振幅
    doji = bool(rng > 0 and body <= 0.3 * rng)

    # 收复 MA5：今日收 > MA5 且昨日收 < 昨日 MA5
    reclaim_ma5 = False
    if t >= 5:
        ma5 = close.rolling(5, min_periods=5).mean()
        if (not pd.isna(ma5.iloc[t]) and not pd.isna(ma5.iloc[t - 1])
                and not pd.isna(close.iloc[t - 1])):
            reclaim_ma5 = bool(
                c > float(ma5.iloc[t]) and float(close.iloc[t - 1]) < float(ma5.iloc[t - 1])
            )

    turn = hammer or doji or reclaim_ma5 or no_new_low
    stabilizing = bool(prior_weak and turn)

    labels = []
    if reclaim_ma5:
        labels.append("收复MA5")
    if hammer:
        labels.append("长下影止跌")
    if doji:
        labels.append("十字星企稳")
    if no_new_low and not (hammer or doji):
        labels.append("不创新低")
    return {
        "no_new_low": no_new_low, "hammer": hammer, "doji": doji,
        "reclaim_ma5": reclaim_ma5, "prior_weak": prior_weak,
        "stabilizing": stabilizing, "label": "+".join(labels),
    }


def detect_volume_ignition(
    history_df: "pd.DataFrame",
    dry_window: int = 5,
    baseline_window: int = 20,
    ignite_mult: float = 1.5,
    dry_ratio: float = 0.85,
) -> dict:
    """地量启动信号（today = 最后一行）：近期缩量(地量) + 今日明显放量。

    比单纯"量比 > X"更准地抓"资金从地量里刚进场那一刻"：要求今天之前的
    ``dry_window`` 天均量明显低于更早 ``baseline_window`` 段（缩量蓄势），
    且今天放量 ≥ ``ignite_mult`` 倍于那段地量。

    返回 dict：today_ratio / was_dry / ignited / label
    """
    empty = {"today_ratio": None, "was_dry": False, "ignited": False, "label": ""}
    if history_df is None or len(history_df) < baseline_window + 2:
        return empty
    df = history_df.sort_values("date").reset_index(drop=True)
    vol = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
    if vol.empty:
        return empty
    t = len(df) - 1
    if pd.isna(vol.iloc[t]) or float(vol.iloc[t]) <= 0:
        return empty
    cur = float(vol.iloc[t])
    recent = vol.iloc[max(0, t - dry_window):t].dropna()        # 今日之前 dry_window 天
    base = vol.iloc[max(0, t - baseline_window):t - dry_window].dropna()
    if recent.empty or float(recent.mean()) <= 0:
        return empty
    today_ratio = round(cur / float(recent.mean()), 2)
    was_dry = bool(
        not base.empty and float(base.mean()) > 0
        and float(recent.mean()) < float(base.mean()) * dry_ratio
    )
    ignited = bool(was_dry and today_ratio >= ignite_mult)
    return {
        "today_ratio": today_ratio, "was_dry": was_dry, "ignited": ignited,
        "label": f"地量启动{today_ratio:.1f}x" if ignited else "",
    }
