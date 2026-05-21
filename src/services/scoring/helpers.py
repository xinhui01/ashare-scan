"""K 线历史形态统计 helper：识别"成功二波接力/连板/反包"形态。

3 个函数无状态，输入是 history DataFrame + 配置参数，输出是 (occurrence_count, days_since_last_hit) 元组。
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
