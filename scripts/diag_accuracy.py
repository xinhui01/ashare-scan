"""诊断：从 limit_up_prediction_accuracy 表直接计算真实命中率。
避免引入重型依赖，直连 sqlite3。
"""
import sqlite3
import os

DB = os.path.join(os.path.dirname(__file__), "..", "data", "stock_store.sqlite3")
DB = os.path.abspath(DB)

CONT_SUB = {"cont_1to2", "cont_2to3", "cont_3to4", "cont_4to5", "cont_5plus"}


def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c


def distinct_dates(c, n=None):
    sql = "SELECT DISTINCT trade_date FROM limit_up_prediction_accuracy ORDER BY trade_date DESC"
    if n:
        sql += f" LIMIT {int(n)}"
    return [r["trade_date"] for r in c.execute(sql).fetchall()]


def primary_hit(row):
    cat = str(row["category"] or "")
    if cat == "cont" or cat in CONT_SUB:
        return int(row["hit_strict"] or 0)
    return int(row["hit_loose"] or 0)


def classify_failure(row):
    if not int(row["hit_buyable"] or 0):
        return None
    if primary_hit(row):
        return None
    t_close = row["t_close"]
    t1_open = row["t1_open"]
    t1_high = row["t1_high"]
    t1_close = row["t1_close"]
    t1_pct = row["t1_pct"]
    if t1_pct is None or t1_open is None or t1_close is None or t_close is None:
        return "其他"
    if t1_pct <= -5:
        return "大跌/跌停"
    if t1_high is not None and t_close and t_close > 0:
        intraday = (t1_high - t_close) / t_close * 100.0
        if intraday >= 3 and t1_close <= t_close:
            return "冲高回落"
    if t_close > 0:
        open_pct = (t1_open - t_close) / t_close * 100.0
        if open_pct <= -1 and t1_close <= t1_open:
            return "低开低走"
    if -2 <= t1_pct <= 2:
        return "弱势震荡"
    return "其他"


def stats_for(c, date_list, cat=None):
    where = ["trade_date IN ({})".format(",".join("?" * len(date_list)))]
    params = list(date_list)
    if cat:
        where.append("category = ?")
        params.append(cat)
    else:
        where.append("category NOT IN ({})".format(",".join("?" * len(CONT_SUB))))
        params.extend(list(CONT_SUB))
    rows = c.execute(
        "SELECT * FROM limit_up_prediction_accuracy WHERE " + " AND ".join(where),
        params,
    ).fetchall()
    total = len(rows)
    dates = {r["trade_date"] for r in rows}
    buyable = [r for r in rows if int(r["hit_buyable"] or 0)]
    b = len(buyable)
    hit = sum(primary_hit(r) for r in buyable)
    pcts = []
    for r in buyable:
        oc = r["t1_open_close_pct"]
        if oc is not None:
            pcts.append(float(oc))
        elif r["t1_pct"] is not None:
            pcts.append(float(r["t1_pct"]))
    avg_pct = sum(pcts) / len(pcts) if pcts else 0.0
    return {
        "dates": len(dates), "total": total, "buyable": b, "hit": hit,
        "primary_rate": (hit / b * 100.0) if b else 0.0,
        "avg_pct": avg_pct,
    }


def main():
    c = conn()
    print("=" * 70)
    print("真实命中率诊断  (表: limit_up_prediction_accuracy)")
    print("=" * 70)

    all_dates = distinct_dates(c)
    print(f"\n已评估交易日总数: {len(all_dates)}  最新: {all_dates[0] if all_dates else 'N/A'}  最早: {all_dates[-1] if all_dates else 'N/A'}")

    print("\n--- 整体命中率（按最近 N 个交易日的 buyable 候选聚合）---")
    print(f"{'窗口':<10}{'交易日':<8}{'候选':<8}{'可买':<8}{'命中':<8}{'命中率':<10}{'均收益%':<10}")
    for n in (5, 10, 20, 30, 60, len(all_dates)):
        d = distinct_dates(c, n)
        s = stats_for(c, d)
        label = f"近{n}" if n != len(all_dates) else "全部"
        print(f"{label:<10}{s['dates']:<8}{s['total']:<8}{s['buyable']:<8}{s['hit']:<8}{s['primary_rate']:>7.1f}%{s['avg_pct']:>9.2f}%")

    print("\n--- 按类别命中率（近20交易日）---")
    d20 = distinct_dates(c, 20)
    cats = {
        "cont": "保留涨停", "first": "二波接力", "fresh": "首板涨停",
        "wrap": "反包", "trend": "趋势涨停",
    }
    print(f"{'类别':<12}{'候选':<8}{'可买':<8}{'命中':<8}{'命中率':<10}{'均收益%':<10}")
    for k, label in cats.items():
        s = stats_for(c, d20, cat=k)
        print(f"{label:<12}{s['total']:<8}{s['buyable']:<8}{s['hit']:<8}{s['primary_rate']:>7.1f}%{s['avg_pct']:>9.2f}%")
    s_all = stats_for(c, d20)
    print(f"{'全部':<12}{s_all['total']:<8}{s_all['buyable']:<8}{s_all['hit']:<8}{s_all['primary_rate']:>7.1f}%{s_all['avg_pct']:>9.2f}%")

    print("\n--- 按预测分桶命中率（近20交易日，全类别）---")
    d20 = distinct_dates(c, 20)
    rows = c.execute(
        "SELECT * FROM limit_up_prediction_accuracy WHERE trade_date IN ({}) "
        "AND category NOT IN ({})".format(
            ",".join("?" * len(d20)), ",".join("?" * len(CONT_SUB))
        ),
        list(d20) + list(CONT_SUB),
    ).fetchall()
    buckets = [("0-49", 0, 49), ("50-59", 50, 59), ("60-69", 60, 69), ("70-79", 70, 79), ("80-100", 80, 100)]
    print(f"{'分数段':<10}{'可买':<8}{'命中':<8}{'命中率':<10}{'均收益%':<10}")
    for label, lo, hi in buckets:
        bk = [r for r in rows if lo <= int(r["predicted_score"] or 0) <= hi]
        bu = [r for r in bk if int(r["hit_buyable"] or 0)]
        h = sum(primary_hit(r) for r in bu)
        pcts = [float(r["t1_open_close_pct"]) if r["t1_open_close_pct"] is not None else float(r["t1_pct"]) for r in bu if (r["t1_open_close_pct"] is not None or r["t1_pct"] is not None)]
        ap = sum(pcts) / len(pcts) if pcts else 0.0
        print(f"{label:<10}{len(bu):<8}{h:<8}{(h/len(bu)*100 if bu else 0):>7.1f}%{ap:>9.2f}%")

    print("\n--- 失败归因（近20交易日，未命中的可买候选）---")
    misses = [r for r in rows if classify_failure(r)]
    total_miss = len(misses)
    from collections import Counter
    cnt = Counter(classify_failure(r) for r in misses)
    for reason, n in cnt.most_common():
        print(f"  {reason:<10} {n:>4}  ({n/total_miss*100:.1f}%)")

    print("\n--- 每日候选数量趋势（验证热门市场是否候选膨胀）---")
    for dt in all_dates[:15]:
        s = stats_for(c, [dt])
        print(f"  {dt}: 候选 {s['total']:<4} 可买 {s['buyable']:<4} 命中 {s['hit']:<3} 命中率 {s['primary_rate']:.1f}%")

    c.close()


if __name__ == "__main__":
    main()
