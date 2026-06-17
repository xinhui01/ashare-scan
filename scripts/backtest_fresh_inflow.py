"""资金接入型 fresh 精度回测（本地，无网络）。

方法（无前视偏差）：
  对最近 N 个交易日 D（每个都有 T+1）：
   1. 候选池 = history 中当日 chg ∈ [-4%,+5%]、成交额 ≥ 5000万、非 ST/北交所、
      不在 D 当日涨停池(zt_codes) 的全部股票（= filter_capital_inflow_candidates 口径）。
   2. 对每只候选用 as-of 截到 D 的历史调 score_fresh_first_board 打分；
      hot_industries = D 当日涨停池按 所属行业 计数（复盘时已知，无前视）。
   3. 评估 T+1：该候选自身 K 线 D 的下一根（D+1）是否涨停（hit）；
      avg_oc = (close(D+1)-open(D+1))/open(D+1)。
  按分数段汇总 precision(涨停占比) / avg_oc / 候选数。

局限（诚实标注）：
  - compare_context 为空 → 题材/阶段/情绪 加减分不参与（评分略偏低，核心信号不受影响）
  - float_mcap 不可得（history 无市值）→ 流通盘加分不参与
  - hot_industries(EM命名) vs 候选 industry(universe命名) 存在命名错配，会低估板块联动命中
"""
import sqlite3
import json
import sys

import pandas as pd

sys.path.insert(0, ".")
from src.services.scoring.fresh import score_fresh_first_board

DB = "data/stock_store.sqlite3"
N_DAYS = 20
THRESHOLD = 45  # 对齐生产 scan_fresh 的出表门槛(fresh.py: score>=45)

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
c = con.cursor()


def is_bse(code):
    return code[:1] in ("4", "8") or code[:2] == "92"


def lu_threshold(code):
    return 19.5 if code.startswith(("30", "68")) else 9.7


# --- universe: code -> (name, industry 证监会命名) ---
uni_name, uni_ind = {}, {}
for r in c.execute("SELECT code, name, industry FROM universe"):
    code = str(r["code"]).zfill(6)
    uni_name[code] = str(r["name"] or "")
    uni_ind[code] = str(r["industry"] or "").strip()

# --- limit_up_stock_meta: code -> 东财行业（与涨停池 100% 同命名，用于板块联动）---
meta_ind = {}
for r in c.execute("SELECT code, industry FROM limit_up_stock_meta WHERE industry != ''"):
    meta_ind[str(r["code"]).zfill(6)] = str(r["industry"]).strip()

# --- per-code history cache (cols renamed to scorer's expectation) ---
hist_cache = {}


def get_hist(code):
    if code not in hist_cache:
        rows = c.execute(
            "SELECT trade_date AS date, open, close, high, low, volume, change_pct, "
            "turnover_rate FROM history WHERE code=? ORDER BY trade_date", (code,)
        ).fetchall()
        hist_cache[code] = pd.DataFrame([dict(r) for r in rows])
    return hist_cache[code]


class AsOfFetcher:
    def __init__(self, code, d_dash):
        self.code, self.d = code, d_dash

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None, **kw):
        df = get_hist(self.code)
        if df.empty:
            return df
        return df[df["date"] <= self.d].tail(130).reset_index(drop=True)


def load_pool(d_yyyymmdd):
    row = c.execute(
        "SELECT data_json FROM limit_up_pool WHERE trade_date=? AND pool_type='today'",
        (d_yyyymmdd,),
    ).fetchone()
    if not row:
        return set(), {}
    zt, hot = set(), {}
    for it in json.loads(row["data_json"]):
        code = str(it.get("代码", "")).zfill(6)
        zt.add(code)
        ind = str(it.get("所属行业", "")).strip()
        if ind:
            hot[ind] = hot.get(ind, 0) + 1
    return zt, hot


pool_dates = [r["trade_date"] for r in c.execute(
    "SELECT DISTINCT trade_date FROM limit_up_pool WHERE pool_type='today' ORDER BY trade_date"
).fetchall()]
# 取倒数第 2..N+1 个（最后一个无 T+1）
test_dates = pool_dates[-(N_DAYS + 1):-1]

buckets = {"45-49": [0, 0, []], "50-59": [0, 0, []], "60-69": [0, 0, []], "70+": [0, 0, []]}
near_miss = [0, 0]  # [40,45) 诊断用：生产不出表，单列不计入头条
pool_total = 0
pool_hit = 0
ind_lookup_hit = 0
ind_lookup_total = 0


def bucket_of(s):
    if s >= 70:
        return "70+"
    if s >= 60:
        return "60-69"
    if s >= 50:
        return "50-59"
    return "45-49"


for D in test_dates:
    d_dash = f"{D[:4]}-{D[4:6]}-{D[6:]}"
    zt, hot = load_pool(D)
    rows = c.execute(
        "SELECT code, change_pct, amount FROM history WHERE trade_date=?", (d_dash,)
    ).fetchall()
    for r in rows:
        code = str(r["code"]).zfill(6)
        chg, amt = r["change_pct"], r["amount"]
        if is_bse(code) or code in zt:
            continue
        if chg is None or not (-4.0 <= chg <= 5.0):
            continue
        if amt is None or amt < 5000_0000:
            continue
        name = uni_name.get(code, "")
        if "ST" in name.upper():
            continue
        industry = uni_ind.get(code, "")
        link_industry = meta_ind.get(code) or industry  # 板块联动用东财命名(meta)优先
        ind_lookup_total += 1
        if link_industry and link_industry in hot:
            ind_lookup_hit += 1

        df = get_hist(code)
        # 找 D 在该票历史里的位置，确认有 D+1
        ds = df["date"].tolist()
        try:
            i = ds.index(d_dash)
        except ValueError:
            continue
        if i + 1 >= len(df) or i < 10:  # i<10: D 含在 as-of 切片内, i+1>=11 对齐 scorer 的 len>=11
            continue

        # T+1 评估（先剔除停牌/占位行：open<=0 或 close<=0 无真实结果，不计入样本）
        nxt = df.iloc[i + 1]
        nclose, nopen, npct = nxt["close"], nxt["open"], nxt["change_pct"]
        if nopen is None or nopen <= 0 or nclose is None or nclose <= 0:
            continue
        boarded = npct is not None and npct >= lu_threshold(code)
        oc = (nclose - nopen) / nopen * 100

        rec = {"code": code, "name": name, "change_pct": chg,
               "turnover": None, "industry": industry, "float_mcap": None}
        try:
            out = score_fresh_first_board(
                rec, hot, {"em_industry_map": meta_ind},
                fetcher=AsOfFetcher(code, d_dash),
            )
        except Exception:
            continue
        if out is None:
            continue

        pool_total += 1
        if boarded:
            pool_hit += 1
        s = out["score"]
        if s >= THRESHOLD:
            b = buckets[bucket_of(s)]
            b[0] += 1
            if boarded:
                b[1] += 1
            b[2].append(oc)
        elif s >= 40:  # [40,45) 近门槛诊断，生产不出表，不计入头条
            near_miss[0] += 1
            if boarded:
                near_miss[1] += 1

con.close()

if not test_dates:
    print("无可回测日期（limit_up_pool 数据不足）")
    sys.exit(0)

print(f"回测天数: {len(test_dates)} ({test_dates[0]}~{test_dates[-1]})")
print(f"入口候选池总数: {pool_total}  T+1 涨停基准率: {pool_hit}/{pool_total} "
      f"({pool_hit/max(pool_total,1)*100:.1f}%)")
print(f"板块联动命名命中: {ind_lookup_hit}/{ind_lookup_total} "
      f"({ind_lookup_hit/max(ind_lookup_total,1)*100:.0f}%)  ← 越低说明 EM/universe 命名错配越重\n")
print(f"{'分数段':<8}{'候选':>6}{'涨停':>6}{'precision':>11}{'avg_oc%':>9}")
surf_n = surf_hit = 0
for lab in ("45-49", "50-59", "60-69", "70+"):
    n, h, ocs = buckets[lab]
    surf_n += n
    surf_hit += h
    prec = h / n * 100 if n else 0
    avgoc = sum(ocs) / len(ocs) if ocs else 0
    print(f"{lab:<8}{n:>6}{h:>6}{prec:>10.1f}%{avgoc:>+8.2f}%")
base = pool_hit / max(pool_total, 1)
print(f"\n出表合计(score≥{THRESHOLD}): {surf_hit}/{surf_n} = {surf_hit/max(surf_n,1)*100:.1f}% precision "
      f"vs 基准 {base*100:.1f}%  → lift {surf_hit/max(surf_n,1)/max(base,1e-9):.2f}x")
print(f"[诊断] 近门槛 [40,45) 生产不出表: {near_miss[1]}/{near_miss[0]} "
      f"({near_miss[1]/max(near_miss[0],1)*100:.1f}%)，不计入上面头条")
