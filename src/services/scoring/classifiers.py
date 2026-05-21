"""涨停形态分类与批量分类编排。

3 个模块级函数（参数注入模式）：
- classify_limit_up_pattern: 单股技术形态分类（连板/反包/突破/超跌等）
- prefetch_history_for_pool: 批量预取涨停池股票历史数据到本地缓存
- classify_limit_up_pool: 涨停池每只股票批量分类

依赖：StockDataFetcher（通过 fetcher 参数注入）+ 可选 log_fn /
limit_up_threshold_fn / call_with_timeout_fn。
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from src.utils.daemon_executor import DaemonThreadPoolExecutor

logger = logging.getLogger(__name__)


def _default_calculate_limit_up_streak(mask: pd.Series) -> int:
    """计算从最新交易日往前数的连续涨停天数（迁自 StockFilter._calculate_limit_up_streak）。"""
    streak = 0
    for flag in reversed(mask.tolist()):
        if bool(flag):
            streak += 1
        else:
            break
    return streak


def classify_limit_up_pattern(
    fetcher,
    stock_code: str,
    *,
    board: str = "",
    stock_name: str = "",
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[..., float]] = None,
    call_with_timeout_fn: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """对涨停股进行技术形态分类，返回形态标签和详细指标。

    形态类型:
    - 回踩MA5涨停: 前一日或前两日收盘接近/低于MA5，涨停日拉回
    - 超跌反弹涨停: 近10日跌幅>10%，或收盘在MA20以下
    - 趋势加速涨停: MA5>MA10>MA20 多头排列，涨停加速
    - 高位连板: 连板数>=2
    - 断板反包: 近5日内有过涨停但被打掉（>=3%阴线），今日重新涨停反包
    - 突破平台涨停: 近10日振幅小（横盘），涨停突破
    - 首板低位涨停: 股价在近60日低位（<30%分位）
    - 其他涨停: 不符合以上任何分类

    迁自 StockFilter.classify_limit_up_pattern；行为零变化。
    """
    code = str(stock_code).strip().zfill(6)
    result: Dict[str, Any] = {
        "code": code,
        "pattern": "其他涨停",
        "pattern_detail": "",
        "ma5": None,
        "ma10": None,
        "ma20": None,
        "close": None,
        "prev_close": None,
        "change_pct": None,
        "distance_ma5_pct": None,
        "trend_10d_pct": None,
        "position_60d_pct": None,
        "volatility_10d": None,
        "volume_burst_ratio": None,
        "amount_burst_ratio": None,
        "is_volume_burst": False,
        "consecutive_boards": 0,
    }

    if call_with_timeout_fn is not None:
        history = call_with_timeout_fn(
            lambda: fetcher.get_history_data(code, days=65),
            timeout_sec=10.0,
            fallback=None,
            task_name=f"涨停分类 {code}",
        )
    else:
        try:
            history = fetcher.get_history_data(code, days=65)
        except Exception as exc:
            if log_fn:
                log_fn(f"涨停分类 {code} 失败: {exc}")
            history = None
    if history is None or history.empty or len(history) < 10:
        result["pattern"] = "数据不足"
        return result

    df = history.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce")
    change_pct = pd.to_numeric(df.get("change_pct"), errors="coerce") if "change_pct" in df.columns else pd.Series(dtype=float)
    volume = pd.to_numeric(df.get("volume"), errors="coerce") if "volume" in df.columns else pd.Series(dtype=float)
    amount = pd.to_numeric(df.get("amount"), errors="coerce") if "amount" in df.columns else pd.Series(dtype=float)

    ma5 = close.rolling(5, min_periods=5).mean()
    ma10 = close.rolling(10, min_periods=10).mean()
    ma20 = close.rolling(20, min_periods=20).mean()

    latest_close = float(close.iloc[-1]) if not pd.isna(close.iloc[-1]) else None
    prev_close = float(close.iloc[-2]) if len(close) >= 2 and not pd.isna(close.iloc[-2]) else None
    latest_ma5 = float(ma5.iloc[-1]) if not pd.isna(ma5.iloc[-1]) else None
    latest_ma10 = float(ma10.iloc[-1]) if not pd.isna(ma10.iloc[-1]) else None
    latest_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else None
    prev_ma5 = float(ma5.iloc[-2]) if len(ma5) >= 2 and not pd.isna(ma5.iloc[-2]) else None

    result["close"] = latest_close
    result["prev_close"] = prev_close
    result["ma5"] = latest_ma5
    result["ma10"] = latest_ma10
    result["ma20"] = latest_ma20
    if not change_pct.empty and not pd.isna(change_pct.iloc[-1]):
        result["change_pct"] = float(change_pct.iloc[-1])

    # 距MA5百分比
    if latest_close and latest_ma5 and latest_ma5 > 0:
        result["distance_ma5_pct"] = round((latest_close / latest_ma5 - 1) * 100, 2)

    # 10日涨跌幅
    if len(close) >= 11 and not pd.isna(close.iloc[-11]) and close.iloc[-11] > 0:
        result["trend_10d_pct"] = round((float(close.iloc[-1]) / float(close.iloc[-11]) - 1) * 100, 2)

    # 60日位置分位
    if len(close) >= 20:
        window = close.tail(min(60, len(close)))
        window_valid = window.dropna()
        if len(window_valid) >= 10 and latest_close is not None:
            rank = float((window_valid < latest_close).sum()) / len(window_valid) * 100
            result["position_60d_pct"] = round(rank, 1)

    # 近10日振幅（用于判断横盘）
    if len(close) >= 11:
        recent_10 = close.iloc[-11:-1].dropna()
        if len(recent_10) >= 5 and recent_10.mean() > 0:
            result["volatility_10d"] = round(float(recent_10.std() / recent_10.mean() * 100), 2)

    # 暴量倍数：当日量/额 相对前5日均值
    if len(volume) >= 6 and not pd.isna(volume.iloc[-1]):
        prev_volume = volume.iloc[-6:-1].dropna()
        if not prev_volume.empty and float(prev_volume.mean()) > 0:
            result["volume_burst_ratio"] = round(float(volume.iloc[-1]) / float(prev_volume.mean()), 2)
    if len(amount) >= 6 and not pd.isna(amount.iloc[-1]):
        prev_amount = amount.iloc[-6:-1].dropna()
        if not prev_amount.empty and float(prev_amount.mean()) > 0:
            result["amount_burst_ratio"] = round(float(amount.iloc[-1]) / float(prev_amount.mean()), 2)
    volume_burst = result["volume_burst_ratio"]
    amount_burst = result["amount_burst_ratio"]
    result["is_volume_burst"] = bool(
        (volume_burst is not None and volume_burst >= 2.5)
        or (amount_burst is not None and amount_burst >= 2.5)
    )

    # 连板数
    if limit_up_threshold_fn is not None:
        threshold = limit_up_threshold_fn(board=board, stock_name=stock_name)
    else:
        threshold = 10.0
    if not change_pct.empty:
        mask = (change_pct >= (threshold - 0.2)).fillna(False)
        streak = _default_calculate_limit_up_streak(mask)
        result["consecutive_boards"] = streak

    # ---- 分类逻辑（优先级从高到低）----
    streak = result["consecutive_boards"]
    dist_ma5 = result["distance_ma5_pct"]
    trend_10d = result["trend_10d_pct"]
    pos_60d = result["position_60d_pct"]
    vol_10d = result["volatility_10d"]
    volume_burst = result["volume_burst_ratio"]
    amount_burst = result["amount_burst_ratio"]

    # 断板反包检测：近期涨停被打回（明显阴线），今日重新涨停反包
    short_board_wrap_detail: Optional[str] = None
    if (
        streak == 1
        and not change_pct.empty
        and len(close) >= 8
        and latest_close is not None
    ):
        threshold_low = threshold - 0.2
        lookback = min(5, len(close) - 1)
        start = max(0, len(close) - 1 - lookback)
        prior_lu_idx: Optional[int] = None
        for i in range(len(close) - 2, start - 1, -1):
            cp = change_pct.iloc[i]
            if pd.notna(cp) and float(cp) >= threshold_low:
                prior_lu_idx = i
                break
        if prior_lu_idx is not None and prior_lu_idx < len(close) - 2:
            worst_drop: Optional[float] = None
            for j in range(prior_lu_idx + 1, len(close) - 1):
                cp = change_pct.iloc[j]
                if pd.notna(cp) and float(cp) <= -3.0:
                    if worst_drop is None or float(cp) < worst_drop:
                        worst_drop = float(cp)
            if worst_drop is not None:
                prior_lu_close = float(close.iloc[prior_lu_idx])
                if prior_lu_close > 0 and latest_close >= prior_lu_close * 0.99:
                    gap_days = len(close) - 1 - prior_lu_idx
                    short_board_wrap_detail = (
                        f"前{gap_days}日涨停后被打回({worst_drop:.1f}%)，今反包"
                    )

    if streak >= 2:
        result["pattern"] = "高位连板"
        result["pattern_detail"] = f"连板{streak}板"

    elif short_board_wrap_detail:
        result["pattern"] = "断板反包"
        result["pattern_detail"] = short_board_wrap_detail

    elif result["is_volume_burst"]:
        result["pattern"] = "暴量涨停"
        parts = []
        if volume_burst is not None:
            parts.append(f"量比前5日均量 {volume_burst:.2f}倍")
        if amount_burst is not None:
            parts.append(f"额比前5日均额 {amount_burst:.2f}倍")
        result["pattern_detail"] = "，".join(parts)

    elif (prev_close is not None and prev_ma5 is not None and prev_close <= prev_ma5 * 1.01
          and dist_ma5 is not None and dist_ma5 > 0):
        result["pattern"] = "回踩MA5涨停"
        detail_parts = []
        if prev_close and prev_ma5:
            detail_parts.append(f"前日收盘{prev_close:.2f}/MA5 {prev_ma5:.2f}")
        result["pattern_detail"] = "，".join(detail_parts)

    elif (trend_10d is not None and trend_10d < -10) or (
        latest_close is not None and latest_ma20 is not None and latest_close < latest_ma20
    ):
        result["pattern"] = "超跌反弹涨停"
        parts = []
        if trend_10d is not None:
            parts.append(f"10日跌{trend_10d:.1f}%")
        if latest_close and latest_ma20 and latest_close < latest_ma20:
            parts.append(f"低于MA20({latest_ma20:.2f})")
        result["pattern_detail"] = "，".join(parts)

    elif (latest_ma5 is not None and latest_ma10 is not None and latest_ma20 is not None
          and latest_ma5 > latest_ma10 > latest_ma20):
        result["pattern"] = "趋势加速涨停"
        result["pattern_detail"] = f"MA5({latest_ma5:.2f})>MA10({latest_ma10:.2f})>MA20({latest_ma20:.2f})"

    elif vol_10d is not None and vol_10d < 2.0:
        result["pattern"] = "突破平台涨停"
        result["pattern_detail"] = f"近10日波动率仅{vol_10d:.2f}%，横盘后突破"

    elif pos_60d is not None and pos_60d < 30:
        result["pattern"] = "首板低位涨停"
        result["pattern_detail"] = f"60日分位{pos_60d:.0f}%"

    else:
        result["pattern"] = "其他涨停"
        parts = []
        if dist_ma5 is not None:
            parts.append(f"距MA5 {dist_ma5:+.1f}%")
        if trend_10d is not None:
            parts.append(f"10日{trend_10d:+.1f}%")
        result["pattern_detail"] = "，".join(parts) if parts else ""

    return result


def prefetch_history_for_pool(
    fetcher,
    codes: List[str],
    days: int = 65,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    cache_only: bool = False,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """批量预取涨停池股票的历史数据到本地缓存。
    已有缓存的跳过，缺失的并行拉取，确保后续分类时不再逐只走网络。

    参数:
        cache_only: True=只使用本地缓存，不发起网络请求；False=允许网络请求补全缓存

    迁自 StockFilter._prefetch_history_for_pool；行为零变化。
    """
    from stock_data import _is_history_cache_fresh
    need_fetch: List[str] = []
    cached_count = 0
    for code in codes:
        c = str(code).strip().zfill(6)
        # 检查缓存是否新鲜（至少需要10行数据）
        if _is_history_cache_fresh(c, min(10, days)):
            cached_count += 1
            continue
        # 再检查 SQLite 有没有足够行数
        from stock_store import load_history as _load_h
        cached = _load_h(c, limit=days)
        if cached is not None and not cached.empty and len(cached) >= min(10, days):
            cached_count += 1
        else:
            need_fetch.append(c)

    if log_fn:
        log_fn(f"涨停分类：缓存命中 {cached_count}/{len(codes)}，需预取 {len(need_fetch)} 只")

    if not need_fetch:
        if progress_callback:
            progress_callback(len(codes), len(codes), "全部已有缓存")
        return

    # 如果只使用缓存，跳过网络请求
    if cache_only:
        if log_fn:
            log_fn(f"涨停分类：cache-only 模式，跳过 {len(need_fetch)} 只无缓存股票的网络请求")
        if progress_callback:
            progress_callback(len(codes) - len(need_fetch), len(codes), f"cache-only: {len(need_fetch)}只无缓存")
        return

    total = len(need_fetch)
    if log_fn:
        log_fn(f"涨停分类：需预取 {total}/{len(codes)} 只股票的历史数据")

    completed = 0
    completed_lock = threading.Lock()

    def _fetch_one(code: str) -> None:
        nonlocal completed
        try:
            fetcher.get_history_data(code, days=days, force_refresh=False)
        except Exception as exc:
            logger.debug("预取历史 %s 失败: %s", code, exc)
        finally:
            with completed_lock:
                completed += 1
            if progress_callback:
                progress_callback(completed, total, f"预取 {code}")

    # 根据股票数量动态调整并发数，上限不超过历史接口并发限制
    history_limit = fetcher.history_request_concurrency_limit()
    workers = min(max(4, total // 3), history_limit, 8)
    workers = max(1, min(workers, total))

    executor = DaemonThreadPoolExecutor(max_workers=workers, thread_name_prefix="zt-prefetch")
    try:
        futures = [executor.submit(_fetch_one, c) for c in need_fetch]
        from concurrent.futures import as_completed
        for fut in as_completed(futures):
            try:
                fut.result(timeout=15.0)  # 单只股票最多15秒
            except Exception as exc:
                logger.debug("预取 future 异常: %s", exc)
    finally:
        executor.shutdown(wait=True)  # 等待所有任务完成，不取消


def classify_limit_up_pool(
    fetcher,
    pool_records: List[Dict[str, Any]],
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    *,
    log_fn: Optional[Callable[[str], None]] = None,
    limit_up_threshold_fn: Optional[Callable[..., float]] = None,
    call_with_timeout_fn: Optional[Callable[..., Any]] = None,
) -> List[Dict[str, Any]]:
    """对涨停池中的每只股票进行技术形态分类。
    自动先批量预取缺失的历史数据，再逐只分类。

    迁自 StockFilter.classify_limit_up_pool；行为零变化。
    """
    codes = [str(r.get("code", "")).strip().zfill(6) for r in pool_records]

    # 阶段1：批量预取历史数据（缓存已有的秒过）
    prefetch_history_for_pool(
        fetcher, codes, days=65,
        progress_callback=lambda c, t, info:
            progress_callback(c, t, f"[预取] {info}") if progress_callback else None,
        log_fn=log_fn,
    )

    # 阶段2：逐只分类（全部从本地缓存读取，秒出）
    results: List[Dict[str, Any]] = []
    total = len(pool_records)
    for idx, rec in enumerate(pool_records):
        code = str(rec.get("code", "")).strip().zfill(6)
        name = str(rec.get("name", ""))
        industry = str(rec.get("industry", ""))
        classification = classify_limit_up_pattern(
            fetcher,
            code,
            board=rec.get("board", ""),
            stock_name=name,
            log_fn=log_fn,
            limit_up_threshold_fn=limit_up_threshold_fn,
            call_with_timeout_fn=call_with_timeout_fn,
        )
        classification["name"] = name
        classification["industry"] = industry
        for key in ("amount", "market_cap", "turnover", "first_board_time",
                     "last_board_time", "break_count", "board_amount",
                     "limit_up_reason", "limit_up_reason_detail", "strong_tag"):
            if key in rec:
                classification[key] = rec[key]
        results.append(classification)
        if progress_callback:
            progress_callback(idx + 1, total, f"{code} {name}")
    return results
