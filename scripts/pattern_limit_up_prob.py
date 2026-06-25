# -*- coding: utf-8 -*-
"""统计：什么样的"图形"（今日 EOD 可判定的形态）次日涨停概率最高。

口径：
- 对全市场每个交易日 T，用截至 T 收盘的数据提取形态特征（EOD 可判定，符合复盘语境）。
- 标签 y = 次日 T+1 是否涨停（同一只股票的紧邻下一交易日）。
- 涨停阈值：主板 change_pct>=9.8；创业板/科创板(300/301/302/688/689) >=19.5。
- 排除 ST（universe.name 含 ST）。库里只有 0/3/6 开头，无北交所。
- turnover_rate 绝大多数为空 -> 不用换手率。

输出：基准涨停率、单因子概率表、命名"图形"概率排名（全周期 + 近窗口对比）。
"""
from __future__ import annotations
import sqlite3
import numpy as np
import pandas as pd

DB = "data/stock_store.sqlite3"
RECENT_TD = 90  # 近窗口（交易日数），用于对比当前市场环境

BIG_BOARD = ("300", "301", "302", "688", "689")  # 20cm
NUM_COLS = ["open", "close", "high", "low", "volume", "amount", "change_pct"]
LOAD_CHUNKSIZE = 200_000


def _prepare_history_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    df.dropna(subset=["close", "change_pct"], inplace=True)
    return df


def load() -> pd.DataFrame:
    query = """
        SELECT h.code, h.trade_date, h.open, h.close, h.high, h.low,
               h.volume, h.amount, h.change_pct
        FROM history AS h
        LEFT JOIN universe AS u ON u.code = h.code
        WHERE u.name IS NULL OR upper(u.name) NOT LIKE '%ST%'
        """
    con = sqlite3.connect(DB)
    chunks = []
    for chunk in pd.read_sql(query, con, chunksize=LOAD_CHUNKSIZE):
        chunks.append(_prepare_history_chunk(chunk))
    con.close()
    df = pd.concat(chunks, ignore_index=True)
    df.sort_values(["code", "trade_date"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    pre3 = df["code"].str[:3]
    df["thr"] = np.where(pre3.isin(BIG_BOARD), 19.5, 9.8)
    df["big"] = pre3.isin(BIG_BOARD)
    df["is_lu"] = df["change_pct"] >= df["thr"]

    g = df.groupby("code", sort=False)
    c = df["close"]

    # 标签：次日涨停
    df["y"] = g["is_lu"].shift(-1).astype("float")

    # 均线
    df["ma5"] = g["close"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    df["ma10"] = g["close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["ma20"] = g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())

    # 量比：今日量 / 前5日均量（不含今日）
    vmean5_prev = g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean().shift(1))
    df["vol_ratio"] = df["volume"] / vmean5_prev
    # 蓄势缩量：前3日均量 / 前5日均量
    v3 = g["volume"].transform(lambda s: s.rolling(3, min_periods=2).mean())
    v5 = g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["vol_shrink"] = v3 / v5

    # 实体 / 振幅
    prev_close = g["close"].shift(1)
    df["body_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["amp_pct"] = (df["high"] - df["low"]) / prev_close * 100

    # 均线关系
    df["ma_bullish"] = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
    df["above_ma5"] = df["close"] > df["ma5"]
    df["dist_ma5"] = (df["close"] / df["ma5"] - 1) * 100
    df["dist_ma20"] = (df["close"] / df["ma20"] - 1) * 100

    # 趋势
    df["trend5"] = (c / g["close"].shift(5) - 1) * 100
    df["trend10"] = (c / g["close"].shift(10) - 1) * 100
    df["trend20"] = (c / g["close"].shift(20) - 1) * 100

    # 60日位置（0=区间底 100=区间顶）
    min60 = g["low"].transform(lambda s: s.rolling(60, min_periods=20).min())
    max60 = g["high"].transform(lambda s: s.rolling(60, min_periods=20).max())
    rng = (max60 - min60).replace(0, np.nan)
    df["pos60"] = (df["close"] - min60) / rng * 100

    # 突破：今日收盘 >= 前20日最高（不含今日）
    high20_prev = g["high"].transform(lambda s: s.rolling(20, min_periods=10).max().shift(1))
    df["new_high20"] = df["close"] >= high20_prev * 0.995
    # 平台紧凑度：前20日箱体振幅（不含今日）
    h20p = g["high"].transform(lambda s: s.rolling(20, min_periods=10).max().shift(1))
    l20p = g["low"].transform(lambda s: s.rolling(20, min_periods=10).min().shift(1))
    df["box20_pct"] = (h20p - l20p) / l20p * 100

    # 股性：前20日涨停次数（不含今日）
    df["lu20_prev"] = g["is_lu"].transform(lambda s: s.rolling(20, min_periods=1).sum().shift(1))
    df["lu60_prev"] = g["is_lu"].transform(lambda s: s.rolling(60, min_periods=1).sum().shift(1))

    # 连续上涨天数（今日含）
    up = (df["change_pct"] > 0).astype(int)
    # 组内连续计数
    grp_id = (up == 0).groupby(df["code"]).cumsum()
    df["up_streak"] = up.groupby([df["code"], grp_id]).cumsum()

    return df


def rate(sub: pd.DataFrame) -> tuple:
    s = sub["y"].dropna()
    n = len(s)
    p = s.mean() * 100 if n else float("nan")
    return n, p


def rate_y(s: pd.Series) -> tuple:
    y = s.dropna()
    n = len(y)
    p = y.mean() * 100 if n else float("nan")
    return n, p


def pct_table(df: pd.DataFrame, col: str, bins, labels, base: float) -> str:
    out = [f"  [{col}]"]
    cat = pd.cut(df[col], bins=bins, labels=labels)
    grouped = df["y"].groupby(cat, observed=False).agg(["count", "mean"])
    for lab in labels:
        if lab in grouped.index:
            n = int(grouped.at[lab, "count"])
            p = grouped.at[lab, "mean"] * 100 if n else float("nan")
        else:
            n, p = 0, float("nan")
        if n < 200:
            out.append(f"    {str(lab):>14} : 样本{n:>7}  概率   --  (样本不足)")
            continue
        lift = p / base if base else float("nan")
        out.append(f"    {str(lab):>14} : 样本{n:>7}  涨停率 {p:5.2f}%  Lift {lift:4.2f}x")
    return "\n".join(out)


def setups(df: pd.DataFrame):
    """命名'图形'集合：(名称, 布尔mask)。仅用 T 日 EOD 可判定特征。"""
    d = df
    not_lu = ~d["is_lu"]
    return [
        # ===== 非涨停形态（首板/启动预测）=====
        ("放量突破20日新高(中阳)",
         not_lu & d["new_high20"] & d["change_pct"].between(3, 9.7) & (d["vol_ratio"] >= 2)),
        ("缩量回踩均线(多头整理)",
         not_lu & d["ma_bullish"] & d["dist_ma5"].between(-4, 2) & (d["vol_shrink"] < 0.85) & (d["trend10"] > 5)),
        ("多头排列+温和放量中阳",
         not_lu & d["ma_bullish"] & d["change_pct"].between(3, 7) & d["vol_ratio"].between(1.2, 3)),
        ("股性活跃(近20日涨过停)+今日异动",
         not_lu & (d["lu20_prev"] >= 1) & d["change_pct"].between(3, 9.7) & (d["vol_ratio"] >= 1.5)),
        ("低位首次放量启动",
         not_lu & (d["pos60"] < 25) & (d["change_pct"] >= 5) & (d["vol_ratio"] >= 2.5)),
        ("高位放量加速",
         not_lu & (d["pos60"] > 90) & (d["change_pct"] >= 5) & (d["vol_ratio"] >= 2)),
        ("紧凑平台突破(箱体<25%)",
         not_lu & (d["box20_pct"] < 25) & (d["change_pct"] > 5) & d["new_high20"]),
        ("连续上涨3日以上(强趋势)",
         not_lu & (d["up_streak"] >= 3) & d["ma_bullish"]),
        ("普通中阳(对照)",
         not_lu & d["change_pct"].between(3, 7)),
        # ===== 涨停后(连板预测)=====
        ("今日首板(前20日无涨停)",
         d["is_lu"] & (d["lu20_prev"] == 0)),
        ("今日连板(前20日有涨停)",
         d["is_lu"] & (d["lu20_prev"] >= 1)),
        ("今日涨停+放量(量比>=2)",
         d["is_lu"] & (d["vol_ratio"] >= 2)),
        ("今日涨停+缩量/一字(量比<0.7)",
         d["is_lu"] & (d["vol_ratio"] < 0.7)),
    ]


def report(df: pd.DataFrame, title: str) -> str:
    valid = df.dropna(subset=["y"])
    base = valid["y"].mean() * 100
    lines = [f"\n{'='*72}", f"【{title}】", f"{'='*72}"]
    lines.append(f"样本(股票-交易日数, 有次日标签): {len(valid):,}")
    lines.append(f"全样本基准次日涨停率: {base:.2f}%  (随机一只票次日涨停的概率)")

    # 单因子
    lines.append("\n--- 单因子 → 次日涨停率 ---")
    lines.append(pct_table(valid, "change_pct",
                           [-50, 0, 3, 5, 7, 9.7, 100],
                           ["跌", "0~3%", "3~5%", "5~7%", "7~9.7%", ">=涨停"], base))
    lines.append(pct_table(valid, "vol_ratio",
                           [0, 0.7, 1, 1.5, 2, 3, 5, 1e9],
                           ["<0.7缩量", "0.7~1", "1~1.5", "1.5~2", "2~3", "3~5", ">5暴量"], base))
    lines.append(pct_table(valid, "pos60",
                           [-1, 20, 40, 60, 80, 95, 101],
                           ["底20%", "20~40", "40~60", "60~80", "80~95", "顶部95+"], base))
    lines.append(pct_table(valid, "lu20_prev",
                           [-1, 0, 1, 2, 3, 100],
                           ["近20无涨停", "1次", "2次", "3次", "4次+"], base))
    lines.append(pct_table(valid, "trend10",
                           [-100, -5, 0, 5, 15, 30, 1e9],
                           ["<-5%", "-5~0", "0~5", "5~15", "15~30", ">30%"], base))

    # 命名图形排名
    lines.append("\n--- 命名'图形' → 次日涨停率（按概率排序）---")
    rows = []
    for name, mask in setups(df):
        aligned = mask.reindex(valid.index, fill_value=False)
        n, p = rate_y(valid.loc[aligned, "y"])
        if n < 100:
            rows.append((name, n, float("nan"), float("nan")))
        else:
            rows.append((name, n, p, p / base))
    rows.sort(key=lambda r: (np.isnan(r[2]), -(r[2] if not np.isnan(r[2]) else 0)))
    lines.append(f"  {'图形':<28}{'样本':>9}{'涨停率':>9}{'Lift':>8}")
    for name, n, p, lift in rows:
        if np.isnan(p):
            lines.append(f"  {name:<28}{n:>9}{'  --':>9}{'  (少)':>8}")
        else:
            lines.append(f"  {name:<28}{n:>9,}{p:>8.2f}%{lift:>7.2f}x")
    return "\n".join(lines)


def main():
    print("加载 history ...", flush=True)
    df = load()
    print(f"  原始有效行: {len(df):,}  股票数: {df['code'].nunique()}", flush=True)
    print("构建特征 ...", flush=True)
    df = build_features(df)

    # 全周期
    print(report(df, "全周期 2023-04 ~ 2026-06 (全部 0/3/6 板块，剔除ST)"))

    # 近窗口
    dates = sorted(df["trade_date"].unique())
    if len(dates) > RECENT_TD:
        cut = dates[-RECENT_TD]
        recent = df[df["trade_date"] >= cut]
        print(report(recent, f"近 {RECENT_TD} 交易日 ({cut} ~ {dates[-1]}) —— 当前市场环境"))

    # 分板块（全周期）
    print(report(df[~df["big"]], "仅主板(10cm) 全周期"))
    print(report(df[df["big"]], "仅创业板/科创板(20cm) 全周期"))


if __name__ == "__main__":
    main()
