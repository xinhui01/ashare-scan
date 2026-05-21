"""Pre-limit-up 特征提取与 profile 聚合。

3 个模块级函数（参数注入）：
- extract_pre_limit_up_features: 单股涨停日前特征提取
- analyze_pre_limit_up_profile: 批量历史涨停股 profile 分析
- aggregate_profile: 批量样本聚合统计

依赖：StockDataFetcher（fetcher 参数）+ 可选 log_fn /
prefetch_history_for_pool_fn / build_local_cache_history_plan_fn。
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def extract_pre_limit_up_features(
    fetcher,
    code: str,
    limit_up_date_idx: int,
    df: pd.DataFrame,
    close: pd.Series,
    volume: pd.Series,
    amount: pd.Series,
    change_pct: pd.Series,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """提取某只股票在涨停日前一天（T-1）的特征快照。

    返回特征字典，数据不足则返回 None。

    迁自 StockFilter._extract_pre_limit_up_features；行为零变化。
    """
    t = limit_up_date_idx  # 涨停日在 df 中的行索引
    if t < 6:
        return None  # 至少需要前6天数据

    feat: Dict[str, Any] = {}

    # --- T-1 日（涨停前一天）的特征 ---
    prev = t - 1

    # 涨跌幅
    feat["change_pct_t1"] = float(change_pct.iloc[prev]) if not pd.isna(change_pct.iloc[prev]) else None

    # 收盘价
    prev_close = float(close.iloc[prev]) if not pd.isna(close.iloc[prev]) else None
    feat["close_t1"] = prev_close

    # 成交量 / 成交额
    feat["volume_t1"] = float(volume.iloc[prev]) if not pd.isna(volume.iloc[prev]) else None
    feat["amount_t1"] = float(amount.iloc[prev]) if not pd.isna(amount.iloc[prev]) else None

    # 量比：T-1 成交量 / 前5日均量
    vol_window = volume.iloc[max(0, prev - 5):prev].dropna()
    if feat["volume_t1"] and not vol_window.empty and float(vol_window.mean()) > 0:
        feat["vol_ratio_t1"] = round(feat["volume_t1"] / float(vol_window.mean()), 2)
    else:
        feat["vol_ratio_t1"] = None

    # 额比：T-1 成交额 / 前5日均额
    amt_window = amount.iloc[max(0, prev - 5):prev].dropna()
    if feat["amount_t1"] and not amt_window.empty and float(amt_window.mean()) > 0:
        feat["amt_ratio_t1"] = round(feat["amount_t1"] / float(amt_window.mean()), 2)
    else:
        feat["amt_ratio_t1"] = None

    # 缩量比：前3日均量 / 前5日均量（判断是否蓄势）
    vol_3 = volume.iloc[max(0, prev - 3):prev].dropna()
    vol_5 = volume.iloc[max(0, prev - 5):prev].dropna()
    if not vol_3.empty and not vol_5.empty and float(vol_5.mean()) > 0:
        feat["shrink_ratio_t1"] = round(float(vol_3.mean()) / float(vol_5.mean()), 2)
    else:
        feat["shrink_ratio_t1"] = None

    # 均线距离
    ma5 = close.rolling(5, min_periods=5).mean()
    ma10 = close.rolling(10, min_periods=10).mean()
    ma20 = close.rolling(20, min_periods=20).mean()

    ma5_val = float(ma5.iloc[prev]) if not pd.isna(ma5.iloc[prev]) else None
    ma10_val = float(ma10.iloc[prev]) if not pd.isna(ma10.iloc[prev]) else None
    ma20_val = float(ma20.iloc[prev]) if not pd.isna(ma20.iloc[prev]) else None

    if prev_close and ma5_val and ma5_val > 0:
        feat["dist_ma5_pct"] = round((prev_close / ma5_val - 1) * 100, 2)
    else:
        feat["dist_ma5_pct"] = None
    if prev_close and ma10_val and ma10_val > 0:
        feat["dist_ma10_pct"] = round((prev_close / ma10_val - 1) * 100, 2)
    else:
        feat["dist_ma10_pct"] = None

    # 多头排列
    feat["ma_bullish"] = bool(
        ma5_val is not None and ma10_val is not None and ma20_val is not None
        and ma5_val > ma10_val > ma20_val
    )

    # 站上 MA5
    feat["above_ma5"] = bool(prev_close is not None and ma5_val is not None and prev_close > ma5_val)

    # 回踩MA5：收盘接近或略低于MA5（距MA5在 -3%~+1% 之间），且前几日曾在MA5之上
    feat["ma5_pullback"] = False
    if prev_close is not None and ma5_val is not None and ma5_val > 0:
        dist = (prev_close / ma5_val - 1) * 100
        if -3.0 <= dist <= 1.0:
            # 检查前3~5日是否曾站上MA5（确认是回踩而非下跌趋势）
            was_above = False
            for lookback in range(2, min(6, prev + 1)):
                idx_back = prev - lookback
                if idx_back >= 0 and not pd.isna(close.iloc[idx_back]) and not pd.isna(ma5.iloc[idx_back]):
                    if float(close.iloc[idx_back]) > float(ma5.iloc[idx_back]) * 1.01:
                        was_above = True
                        break
            feat["ma5_pullback"] = was_above

    # 5日涨幅
    if prev >= 5 and not pd.isna(close.iloc[prev - 5]) and close.iloc[prev - 5] > 0:
        feat["trend_5d"] = round((prev_close / float(close.iloc[prev - 5]) - 1) * 100, 2) if prev_close else None
    else:
        feat["trend_5d"] = None

    # 10日涨幅
    if prev >= 10 and not pd.isna(close.iloc[prev - 10]) and close.iloc[prev - 10] > 0:
        feat["trend_10d"] = round((prev_close / float(close.iloc[prev - 10]) - 1) * 100, 2) if prev_close else None
    else:
        feat["trend_10d"] = None

    # 60日位置分位
    window = close.iloc[max(0, prev - 59):prev + 1].dropna()
    if len(window) >= 10 and prev_close is not None:
        feat["position_60d"] = round(float((window < prev_close).sum()) / len(window) * 100, 1)
    else:
        feat["position_60d"] = None

    # 近10日振幅（波动率）
    recent_close = close.iloc[max(0, prev - 10):prev].dropna()
    if len(recent_close) >= 5 and recent_close.mean() > 0:
        feat["volatility_10d"] = round(float(recent_close.std() / recent_close.mean() * 100), 2)
    else:
        feat["volatility_10d"] = None

    # 换手率
    turnover = pd.to_numeric(df.get("turnover_rate"), errors="coerce") if "turnover_rate" in df.columns else pd.Series(dtype=float)
    feat["turnover_t1"] = float(turnover.iloc[prev]) if not turnover.empty and prev < len(turnover) and not pd.isna(turnover.iloc[prev]) else None

    return feat


def analyze_pre_limit_up_profile(
    fetcher,
    lookback_days: int = 5,
    trade_date: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    prefetch_history_for_pool_fn: Optional[Callable[..., None]] = None,
    build_local_cache_history_plan_fn: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """回溯分析最近 N 个交易日涨停股在涨停前的特征。

    步骤：
    1. 获取最近 N 天的涨停池
    2. 对每只首板涨停股，拉取历史数据，提取涨停前 T-1 日特征
    3. 汇总统计各特征的分布（中位数、均值、分位）

    返回:
        feature_samples: 每只涨停股的特征样本列表
        profile: 聚合的特征画像（中位数/均值/分位）
        sample_count: 样本数量
        trade_dates: 回溯的交易日期列表

    迁自 StockFilter.analyze_pre_limit_up_profile；行为零变化。
    """
    if log_fn:
        log_fn(f"涨停画像：回溯最近 {lookback_days} 个交易日涨停股特征...")

    # 获取最近 N 个交易日
    from datetime import datetime as _dt
    base_trade_date = str(trade_date or "").strip() or _dt.now().strftime("%Y%m%d")
    trade_dates = fetcher._recent_trade_dates(base_trade_date, lookback_days)
    if not trade_dates:
        return {"feature_samples": [], "profile": {}, "sample_count": 0, "trade_dates": []}

    # 收集所有首板涨停股（加总超时保护，防止东财不可达时无限阻塞）
    import time as _time
    _pool_deadline = _time.time() + 45.0  # 涨停池获取总时限 45 秒
    all_first_board: List[Dict[str, Any]] = []
    for d in trade_dates:
        if _time.time() > _pool_deadline:
            if log_fn:
                log_fn(f"涨停画像：获取涨停池超时（已超 45s），使用已获取的数据继续")
            break
        try:
            pool = fetcher.get_limit_up_pool(d)
        except Exception as e:
            if log_fn:
                log_fn(f"涨停画像：获取 {d} 涨停池失败: {e}，跳过该日")
            continue
        if pool is None or pool.empty:
            continue
        if "连板数" in pool.columns:
            first = pool[pool["连板数"] == 1]
        else:
            first = pool
        for _, row in first.iterrows():
            code = str(row.get("代码", "")).strip().zfill(6)
            name = str(row.get("名称", ""))
            industry = str(row.get("所属行业", ""))
            if "ST" in name.upper():
                continue
            all_first_board.append({
                "code": code, "name": name, "industry": industry,
                "limit_up_date": d,
            })

    if not all_first_board:
        return {"feature_samples": [], "profile": {}, "sample_count": 0, "trade_dates": trade_dates}

    if log_fn:
        log_fn(f"涨停画像：共 {len(all_first_board)} 只首板涨停股，正在提取涨停前特征...")

    # 预取历史数据（只使用本地缓存，不发起网络请求）
    codes = list({r["code"] for r in all_first_board})
    if prefetch_history_for_pool_fn is not None:
        prefetch_history_for_pool_fn(codes, days=65, progress_callback=progress_callback, cache_only=True)
    if build_local_cache_history_plan_fn is not None:
        local_cache_plan = build_local_cache_history_plan_fn(reason="predict-profile-cache-only")
    else:
        local_cache_plan = None

    # 逐只提取特征（只从本地缓存读取）
    feature_samples: List[Dict[str, Any]] = []
    total = len(all_first_board)
    prepared_history: Dict[str, Optional[Tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series, pd.Series]]] = {}
    for idx, rec in enumerate(all_first_board):
        code = rec["code"]
        limit_date = rec["limit_up_date"]

        if code not in prepared_history:
            try:
                # 只使用本地缓存，不发起网络请求
                history = fetcher.get_history_data(
                    code,
                    days=65,
                    force_refresh=False,
                    request_plan=local_cache_plan,
                )
            except Exception as exc:
                logger.debug("涨停分类获取历史 %s 失败: %s", code, exc)
                history = None

            if history is None or history.empty or len(history) < 10:
                prepared_history[code] = None
            else:
                df = history.sort_values("date").reset_index(drop=True)
                df["date"] = df["date"].astype(str).str.strip().str.replace("-", "")
                close = pd.to_numeric(df["close"], errors="coerce")
                volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
                amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)
                change_pct = pd.to_numeric(df.get("change_pct"), errors="coerce") if "change_pct" in df.columns else pd.Series(dtype=float)
                prepared_history[code] = (df, close, volume, amount, change_pct)

        prepared = prepared_history.get(code)
        if prepared is None:
            continue

        df, close, volume, amount, change_pct = prepared

        # 找到涨停日在历史中的位置
        match_idx = df.index[df["date"] == limit_date].tolist()
        if not match_idx:
            continue
        t = match_idx[0]

        feat = extract_pre_limit_up_features(fetcher, code, t, df, close, volume, amount, change_pct, log_fn=log_fn)
        if feat is None:
            continue
        feat["code"] = code
        feat["name"] = rec["name"]
        feat["industry"] = rec["industry"]
        feat["limit_up_date"] = limit_date
        feature_samples.append(feat)

        if progress_callback:
            progress_callback(idx + 1, total, f"画像 {code} {rec['name']}")

    # 汇总统计
    profile = aggregate_profile(feature_samples)

    if log_fn:
        log_fn(f"涨停画像：成功提取 {len(feature_samples)} 个样本的涨停前特征")

    return {
        "feature_samples": feature_samples,
        "profile": profile,
        "sample_count": len(feature_samples),
        "trade_dates": trade_dates,
    }


def aggregate_profile(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    """汇总特征样本，计算各指标的中位数/均值/分位数分布。

    迁自 StockFilter._aggregate_profile（去 @staticmethod 装饰器）；行为零变化。
    """
    if not samples:
        return {}

    numeric_keys = [
        "change_pct_t1", "vol_ratio_t1", "amt_ratio_t1", "shrink_ratio_t1",
        "dist_ma5_pct", "dist_ma10_pct", "trend_5d", "trend_10d",
        "position_60d", "volatility_10d", "turnover_t1",
    ]
    bool_keys = ["ma_bullish", "above_ma5", "ma5_pullback"]

    profile: Dict[str, Any] = {}
    for key in numeric_keys:
        values = [s[key] for s in samples if s.get(key) is not None]
        if not values:
            profile[key] = {"median": None, "mean": None, "p25": None, "p75": None, "count": 0}
            continue
        sorted_v = sorted(values)
        n = len(sorted_v)
        profile[key] = {
            "median": round(sorted_v[n // 2], 2),
            "mean": round(sum(sorted_v) / n, 2),
            "p25": round(sorted_v[max(0, n // 4)], 2),
            "p75": round(sorted_v[max(0, n * 3 // 4)], 2),
            "min": round(sorted_v[0], 2),
            "max": round(sorted_v[-1], 2),
            "count": n,
        }

    for key in bool_keys:
        true_count = sum(1 for s in samples if s.get(key))
        profile[key] = {
            "true_count": true_count,
            "total": len(samples),
            "ratio": round(true_count / max(len(samples), 1) * 100, 1),
        }

    return profile
