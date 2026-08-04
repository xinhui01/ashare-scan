"""反事实测算：在已落库的真实预测结果上，模拟不同筛选策略的命中率。

不需要重跑预测（历史回放单次要 10+ 分钟），直接在 limit_up_prediction_accuracy
上做"如果当时只买其中一部分候选，结果会怎样"的推演。

命中口径与线上一致：cont 走 hit_strict，其余走 hit_loose，均要求 hit_buyable。
收益口径：t1_open_close_pct（次日开盘买入、收盘卖出）。
"""
from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

DB = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "stock_store.sqlite3"))

CONT_SUB = {"cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"}
CAT_LABEL = {
    "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
    "wrap": "反包", "trend": "趋势涨停",
}


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def primary_hit(row: Dict[str, Any]) -> int:
    if not int(row["hit_buyable"] or 0):
        return 0
    cat = str(row["category"] or "")
    if cat == "cont" or cat in CONT_SUB:
        return int(row["hit_strict"] or 0)
    return int(row["hit_loose"] or 0)


def base_category(row: Dict[str, Any]) -> str:
    cat = str(row["category"] or "")
    return "cont" if cat in CONT_SUB else cat


def load_rows(c: sqlite3.Connection, lookback_dates: Optional[int]) -> Dict[str, List[Dict[str, Any]]]:
    dates = [
        r["trade_date"]
        for r in c.execute(
            "SELECT DISTINCT trade_date FROM limit_up_prediction_accuracy "
            "ORDER BY trade_date DESC"
        ).fetchall()
    ]
    if lookback_dates:
        dates = dates[:lookback_dates]
    if not dates:
        return {}
    placeholders = ",".join("?" * len(dates))
    rows = c.execute(
        f"SELECT * FROM limit_up_prediction_accuracy WHERE trade_date IN ({placeholders})",
        dates,
    ).fetchall()

    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = dict(r)
        if not int(d.get("hit_buyable") or 0):
            continue  # 一字板买不到，直接排除出可买池
        d["_hit"] = primary_hit(d)
        d["_cat"] = base_category(d)
        try:
            d["_score"] = float(d.get("predicted_score") or 0)
        except (TypeError, ValueError):
            d["_score"] = 0.0
        ret = d.get("t1_open_close_pct")
        d["_ret"] = float(ret) if ret is not None else None
        by_date[d["trade_date"]].append(d)
    return by_date


def evaluate_strategy(
    by_date: Dict[str, List[Dict[str, Any]]],
    selector: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    picked_total = 0
    hits = 0
    rets: List[float] = []
    days_with_pick = 0
    for _date, rows in by_date.items():
        picked = selector(list(rows))
        if picked:
            days_with_pick += 1
        picked_total += len(picked)
        for r in picked:
            hits += r["_hit"]
            if r["_ret"] is not None:
                rets.append(r["_ret"])
    n_days = len(by_date) or 1
    return {
        "picked": picked_total,
        "per_day": picked_total / n_days,
        "hits": hits,
        "hit_rate": (hits / picked_total * 100.0) if picked_total else 0.0,
        "avg_ret": (sum(rets) / len(rets)) if rets else 0.0,
        "total_ret": sum(rets),
        "days_with_pick": days_with_pick,
    }


def top_n(n: int) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    def _sel(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(rows, key=lambda r: -r["_score"])[:n]
    return _sel


def cats_only(cats: set) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    def _sel(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [r for r in rows if r["_cat"] in cats]
    return _sel


def cats_top_n(cats: set, n: int) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    def _sel(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        f = [r for r in rows if r["_cat"] in cats]
        return sorted(f, key=lambda r: -r["_score"])[:n]
    return _sel


def score_min(v: float) -> Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]:
    def _sel(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [r for r in rows if r["_score"] >= v]
    return _sel


def print_table(title: str, results: List[tuple]) -> None:
    print(f"\n{'=' * 92}")
    print(title)
    print("=" * 92)
    print(
        f"{'策略':<28} | {'日均选股':>8} | {'总选中':>6} | {'命中':>5} | "
        f"{'命中率':>7} | {'均收益':>8} | {'累计收益':>9}"
    )
    print("-" * 92)
    for name, s in results:
        print(
            f"{name:<28} | {s['per_day']:>8.1f} | {s['picked']:>6} | {s['hits']:>5} | "
            f"{s['hit_rate']:>6.2f}% | {s['avg_ret']:>+7.2f}% | {s['total_ret']:>+8.1f}%"
        )


def run_window(by_date: Dict[str, List[Dict[str, Any]]], window_label: str) -> None:
    if not by_date:
        print(f"[{window_label}] 无数据")
        return
    n_days = len(by_date)
    print(f"\n\n{'#' * 92}")
    print(f"# 窗口：{window_label}（{n_days} 个交易日）")
    print("#" * 92)

    baseline = evaluate_strategy(by_date, lambda rows: rows)

    # --- 1. top-N 截断 ---
    res = [("基线：全部候选（现状）", baseline)]
    for n in (3, 5, 8, 10, 15, 20, 30):
        res.append((f"top-{n}（按分数）", evaluate_strategy(by_date, top_n(n))))
    print_table("策略 A：候选数截断（解决热门日名单爆炸）", res)

    # --- 2. 分数门槛 ---
    res = [("基线：全部候选（现状）", baseline)]
    for v in (50, 55, 60, 65, 70, 75, 80, 85, 90):
        res.append((f"predicted_score >= {v}", evaluate_strategy(by_date, score_min(v))))
    print_table("策略 B：抬高分数门槛", res)

    # --- 2b. 分数分层命中率：验证评分本身有没有区分度 ---
    bands = [(40, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 101)]
    res = []
    for lo, hi in bands:
        s = evaluate_strategy(
            by_date, lambda rows, lo=lo, hi=hi: [r for r in rows if lo <= r["_score"] < hi]
        )
        if s["picked"]:
            res.append((f"分数 [{lo}, {hi if hi <= 100 else 100}]", s))
    print_table("诊断 B2：分数分层命中率（评分区分度检验）", res)

    # --- 3. 类别筛选 ---
    res = [("基线：全部候选（现状）", baseline)]
    combos = [
        ("只买 反包", {"wrap"}),
        ("只买 反包+二波", {"wrap", "first"}),
        ("只买 反包+二波+保留", {"wrap", "first", "cont"}),
        ("只买 反包+保留", {"wrap", "cont"}),
        ("剔除 首板+趋势", {"wrap", "first", "cont"}),
    ]
    for label, cats in combos:
        res.append((label, evaluate_strategy(by_date, cats_only(cats))))
    print_table("策略 C：只买高胜率类别", res)

    # --- 4. 组合策略 ---
    res = [("基线：全部候选（现状）", baseline)]
    for cats, clabel in (
        ({"wrap", "first"}, "反包+二波"),
        ({"wrap", "first", "cont"}, "反包+二波+保留"),
    ):
        for n in (3, 5, 10):
            res.append((f"{clabel} 且 top-{n}", evaluate_strategy(by_date, cats_top_n(cats, n))))
    print_table("策略 D：类别筛选 + 数量截断（组合）", res)

    # --- 5. 各类别单独表现 ---
    res = []
    for cat in ("wrap", "first", "cont", "fresh", "trend"):
        s = evaluate_strategy(by_date, cats_only({cat}))
        if s["picked"]:
            res.append((f"{CAT_LABEL[cat]}（{cat}）", s))
    res.sort(key=lambda x: -x[1]["hit_rate"])
    print_table("参考：各类别单独表现（按命中率排序）", res)

    # --- 6. 类别 × 平均分 交叉：检查"高分类别是否真的高胜率" ---
    print(f"\n{'=' * 92}")
    print("诊断 F：类别平均分 vs 实际命中率（评分标定是否错位）")
    print("=" * 92)
    print(f"{'类别':<18} | {'样本':>5} | {'平均分':>7} | {'命中率':>7} | {'均收益':>8} | 标定判断")
    print("-" * 92)
    rank_rows = []
    for cat in ("wrap", "first", "cont", "fresh", "trend"):
        picks = [r for rows in by_date.values() for r in rows if r["_cat"] == cat]
        if not picks:
            continue
        avg_score = sum(r["_score"] for r in picks) / len(picks)
        hr = sum(r["_hit"] for r in picks) / len(picks) * 100.0
        rets = [r["_ret"] for r in picks if r["_ret"] is not None]
        rank_rows.append((cat, len(picks), avg_score, hr, (sum(rets) / len(rets)) if rets else 0.0))
    if rank_rows:
        by_score = sorted(rank_rows, key=lambda x: -x[2])
        by_hit = sorted(rank_rows, key=lambda x: -x[3])
        score_rank = {r[0]: i + 1 for i, r in enumerate(by_score)}
        hit_rank = {r[0]: i + 1 for i, r in enumerate(by_hit)}
        for cat, n, avg_score, hr, ar in by_score:
            gap = score_rank[cat] - hit_rank[cat]
            flag = "一致" if gap == 0 else (f"高估 {abs(gap)} 位" if gap < 0 else f"低估 {gap} 位")
            print(
                f"{CAT_LABEL[cat] + '(' + cat + ')':<18} | {n:>5} | {avg_score:>7.1f} | "
                f"{hr:>6.2f}% | {ar:>+7.2f}% | 分数第{score_rank[cat]} / 命中第{hit_rank[cat]} → {flag}"
            )


def main() -> int:
    if not os.path.exists(DB):
        print(f"数据库不存在: {DB}")
        return 1
    c = conn()
    try:
        for label, n in (("近 20 个交易日", 20), ("近 60 个交易日", 60), ("全部历史", None)):
            run_window(load_rows(c, n), label)
    finally:
        c.close()

    print("\n\n说明：")
    print("  - 命中口径与线上一致（cont=hit_strict，其余=hit_loose，均要求可买）")
    print("  - 收益口径：次日开盘买入、收盘卖出（t1_open_close_pct），等权、未计交易成本")
    print("  - 累计收益 = 每笔收益直接相加（等权单笔，不代表复利账户曲线）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
