"""市场状态分类的回测验证。

对历史 N 天的 limit_up_pool，每天：
1. 算出当日市场状态
2. 按 4 种策略筛选 D 日候选股 (保留涨停 / 打首板 / 接力 2+ / 反包-炸过板)
3. 模拟 D+1 open 买入、close 卖出，记录涨幅
4. 聚合: 每个 (状态, 策略) 的 (mean_return, hit_rate, n)

用法：
    from src.services import market_state_backtest_service as bt
    result = bt.backtest_market_state(start_date="20260408")
    bt.print_summary(result)
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

import stock_store
from stock_logger import get_logger
from src.services import market_sentiment_service as mss
from src.services.prediction_accuracy_service import (
    _evaluate_candidate,
    _to_dash_date,
    _next_trading_day_yyyymmdd,
)

logger = get_logger(__name__)

STRATEGY_KEYS: List[str] = ["保留涨停", "打首板", "接力(2+)", "反包(炸过板)"]


def _stocks_by_strategy(
    pool_df: Optional[pd.DataFrame],
) -> Dict[str, List[Tuple[str, str]]]:
    """从 D 日 pool 筛选 4 类候选股 [(code, name), ...]。"""
    out: Dict[str, List[Tuple[str, str]]] = {k: [] for k in STRATEGY_KEYS}
    if pool_df is None or pool_df.empty:
        return out

    codes = (
        pool_df["代码"].astype(str).str.zfill(6).tolist()
        if "代码" in pool_df.columns else []
    )
    names = (
        pool_df["名称"].astype(str).tolist()
        if "名称" in pool_df.columns else codes
    )
    boards = (
        pool_df["连板数"].astype(int).tolist()
        if "连板数" in pool_df.columns else [1] * len(codes)
    )
    breaks = (
        pool_df["炸板次数"].astype(int).tolist()
        if "炸板次数" in pool_df.columns else [0] * len(codes)
    )

    for i, code in enumerate(codes):
        item = (code, names[i] if i < len(names) else code)
        out["保留涨停"].append(item)
        if boards[i] == 1:
            out["打首板"].append(item)
        else:
            out["接力(2+)"].append(item)
        if breaks[i] > 0:
            out["反包(炸过板)"].append(item)
    return out


def _eval_t1(
    code: str,
    name: str,
    d_key: str,
    next_d_key: str,
) -> Optional[Dict[str, Any]]:
    """查 code 在 d_key → next_d_key 之间的 T+1 表现（用 history 本地缓存）。"""
    df = stock_store.load_history(code)
    if df is None or df.empty:
        return None
    d_dash = _to_dash_date(d_key)
    n_dash = _to_dash_date(next_d_key)
    return _evaluate_candidate(code, name, df, d_dash, n_dash)


def _empty_strategy_stat() -> Dict[str, Any]:
    return {
        "n": 0, "n_total": 0, "n_skipped": 0,
        "mean_pct": 0.0, "median_pct": 0.0,
        "hit_loose_rate": 0.0, "hit_strict_rate": 0.0,
    }


def backtest_market_state(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    *,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """跑历史回测。

    Returns:
        rows: List[per-day dict]
        summary_state_strategy: Dict[(state, strategy)] -> stat
        summary_overall:        Dict[strategy] -> stat (不分状态)
        adaptive_vs_fixed:      Dict 对比"按状态切换"和"固定策略"的累计表现
    """
    def _l(m: str) -> None:
        if log:
            try:
                log(m)
            except Exception:
                pass
        logger.info(m)

    all_dates = stock_store.list_limit_up_pool_trade_dates() or []
    if start_date:
        all_dates = [d for d in all_dates if d >= start_date]
    if end_date:
        all_dates = [d for d in all_dates if d <= end_date]
    if not all_dates:
        return {
            "rows": [], "summary_state_strategy": {}, "summary_overall": {},
            "adaptive_vs_fixed": {},
        }

    rows: List[Dict[str, Any]] = []
    state_strat_returns: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    state_strat_hits: Dict[Tuple[str, str], List[bool]] = defaultdict(list)
    overall_returns: Dict[str, List[float]] = defaultdict(list)
    overall_hits: Dict[str, List[bool]] = defaultdict(list)
    # 每天每策略的平均涨幅，用于"adaptive vs fixed"对比
    daily_strategy_mean: List[Tuple[str, str, Dict[str, float]]] = []  # (date, state, {strat: mean})

    _l(f"市场状态回测: {len(all_dates)} 天 ({all_dates[0]} ~ {all_dates[-1]})")

    for d in all_dates:
        next_d = _next_trading_day_yyyymmdd(d)
        if not next_d:
            continue

        try:
            r = mss.analyze_market_sentiment(
                d, fetch_external=False, log=lambda m: None
            )
        except Exception as e:
            _l(f"  {d}: 状态分析失败 {e}")
            continue
        state = (r.get("market_state") or {}).get("label", "?")
        score = int(r.get("score", 0))

        pool_df = stock_store.load_limit_up_pool(d)
        if pool_df is None or pool_df.empty:
            continue
        groups = _stocks_by_strategy(pool_df)

        day_row: Dict[str, Any] = {
            "trade_date": d,
            "state": state,
            "score": score,
            "next_trade_date": next_d,
            "strategies": {},
        }
        day_strat_mean: Dict[str, float] = {}

        for strat, items in groups.items():
            returns: List[float] = []
            hits_loose: List[bool] = []
            hits_strict: List[bool] = []
            skipped = 0
            for code, name in items:
                ev = _eval_t1(code, name, d, next_d)
                if ev is None or ev.get("t1_open_close_pct") is None:
                    skipped += 1
                    continue
                if not ev.get("hit_buyable"):
                    skipped += 1  # 一字板买不到
                    continue
                returns.append(float(ev["t1_open_close_pct"]))
                hits_loose.append(bool(ev.get("hit_loose")))
                hits_strict.append(bool(ev.get("hit_strict")))

            n = len(returns)
            if n == 0:
                day_row["strategies"][strat] = {
                    **_empty_strategy_stat(),
                    "n_total": len(items), "n_skipped": skipped,
                }
                day_strat_mean[strat] = 0.0
                continue
            mean_pct = sum(returns) / n
            hr_loose = sum(hits_loose) / n
            hr_strict = sum(hits_strict) / n
            stat = {
                "n": n, "n_total": len(items), "n_skipped": skipped,
                "mean_pct": round(mean_pct, 3),
                "median_pct": round(sorted(returns)[n // 2], 3),
                "hit_loose_rate": round(hr_loose, 3),
                "hit_strict_rate": round(hr_strict, 3),
            }
            day_row["strategies"][strat] = stat
            day_strat_mean[strat] = mean_pct

            state_strat_returns[(state, strat)].extend(returns)
            state_strat_hits[(state, strat)].extend(hits_loose)
            overall_returns[strat].extend(returns)
            overall_hits[strat].extend(hits_loose)

        rows.append(day_row)
        daily_strategy_mean.append((d, state, day_strat_mean))

    def _summarize(rmap, hmap):
        out: Dict[Any, Dict[str, Any]] = {}
        for k, rets in rmap.items():
            n = len(rets)
            if n == 0:
                out[k] = _empty_strategy_stat()
                continue
            hits = hmap.get(k, [])
            srt = sorted(rets)
            out[k] = {
                "n": n,
                "mean_pct": round(sum(rets) / n, 3),
                "median_pct": round(srt[n // 2], 3),
                "hit_loose_rate": round(sum(hits) / n, 3) if hits else 0.0,
            }
        return out

    summary_state_strategy = _summarize(state_strat_returns, state_strat_hits)
    summary_overall = _summarize(overall_returns, overall_hits)

    # ===== Adaptive vs Fixed 对比 =====
    # adaptive: 每天按状态推荐的策略走 (用 _STATE_STRATEGIES 里的第一个 pool 映射)
    state_to_recommended_strat = {
        "接力日": "接力(2+)",
        "轮动日": "打首板",
        "退潮日": "反包(炸过板)",
        "冰点日": None,  # 空仓 → 当天收益 0%
        "过渡日": "打首板",
    }
    adaptive_returns: List[float] = []
    fixed_returns: Dict[str, List[float]] = {s: [] for s in STRATEGY_KEYS}
    for d, state, smap in daily_strategy_mean:
        rec = state_to_recommended_strat.get(state, "打首板")
        if rec is None:
            adaptive_returns.append(0.0)
        else:
            adaptive_returns.append(smap.get(rec, 0.0))
        for s in STRATEGY_KEYS:
            fixed_returns[s].append(smap.get(s, 0.0))

    def _stat(arr):
        if not arr:
            return {"mean_daily": 0.0, "cum_pct": 0.0, "days": 0}
        cum = 1.0
        for r in arr:
            cum *= (1 + r / 100.0)
        return {
            "mean_daily": round(sum(arr) / len(arr), 3),
            "cum_pct": round((cum - 1) * 100, 2),
            "days": len(arr),
        }

    adaptive_vs_fixed = {
        "adaptive": _stat(adaptive_returns),
        "fixed": {s: _stat(fixed_returns[s]) for s in STRATEGY_KEYS},
    }

    return {
        "rows": rows,
        "summary_state_strategy": summary_state_strategy,
        "summary_overall": summary_overall,
        "adaptive_vs_fixed": adaptive_vs_fixed,
    }


# ============== 报告打印 ==============

def print_summary(result: Dict[str, Any]) -> None:
    """打印对比报表。"""
    rows = result.get("rows", [])
    if not rows:
        print("无回测数据")
        return

    print(f"\n回测覆盖: {len(rows)} 天 ({rows[0]['trade_date']} ~ {rows[-1]['trade_date']})\n")

    # 1. 整体策略平均 (不分状态)
    print("=" * 90)
    print("【整体】不分状态，4 种策略的平均 T+1 表现：")
    print("-" * 90)
    print(f"{'策略':<14} {'样本数':>6} {'平均涨幅':>10} {'中位数':>8} {'≥5%命中率':>10}")
    overall = result.get("summary_overall", {})
    for s in STRATEGY_KEYS:
        st = overall.get(s, _empty_strategy_stat())
        print(
            f"{s:<14} {st['n']:>6} {st['mean_pct']:>+9.2f}% "
            f"{st.get('median_pct', 0):>+7.2f}% {st['hit_loose_rate']*100:>9.0f}%"
        )

    # 2. 按状态 × 策略矩阵
    print("\n" + "=" * 90)
    print("【按状态分组】每个状态下，哪个策略 T+1 表现最好：")
    print("-" * 90)
    sss = result.get("summary_state_strategy", {})
    states = sorted({k[0] for k in sss.keys()})
    for state in states:
        print(f"\n  >> {state}")
        print(f"    {'策略':<14} {'样本数':>6} {'平均涨幅':>10} {'≥5%命中率':>10}")
        best_strat = None
        best_mean = -999.0
        for s in STRATEGY_KEYS:
            st = sss.get((state, s), _empty_strategy_stat())
            mark = ""
            if st["n"] > 0 and st["mean_pct"] > best_mean:
                best_mean = st["mean_pct"]
                best_strat = s
            print(
                f"    {s:<14} {st['n']:>6} {st['mean_pct']:>+9.2f}% "
                f"{st['hit_loose_rate']*100:>9.0f}%"
            )
        if best_strat:
            print(f"    >>> 该状态下最佳策略: 【{best_strat}】 (平均 {best_mean:+.2f}%)")

    # 3. Adaptive vs Fixed
    print("\n" + "=" * 90)
    print("【自适应 vs 固定策略】 (Adaptive = 按当日状态切换推荐策略)")
    print("-" * 90)
    avf = result.get("adaptive_vs_fixed", {})
    adp = avf.get("adaptive", {})
    print(
        f"  {'按状态切换':<16} "
        f"日均 {adp.get('mean_daily', 0):>+6.2f}% · "
        f"累计 {adp.get('cum_pct', 0):>+7.2f}% · "
        f"{adp.get('days', 0)} 天"
    )
    for s in STRATEGY_KEYS:
        fx = avf.get("fixed", {}).get(s, {})
        print(
            f"  {'固定[' + s + ']':<16} "
            f"日均 {fx.get('mean_daily', 0):>+6.2f}% · "
            f"累计 {fx.get('cum_pct', 0):>+7.2f}% · "
            f"{fx.get('days', 0)} 天"
        )
    print()
