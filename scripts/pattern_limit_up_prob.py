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
FEATURE_WARMUP_TD = 70  # 供60日位置、20日箱体、均线等特征使用

BIG_BOARD = ("300", "301", "302", "688", "689")  # 20cm
NUM_COLS = ["open", "close", "high", "low", "volume", "amount", "change_pct"]
LOAD_CHUNKSIZE = 1_000
DISPLAY_STOCK_LIMIT = 1


FILTERED_SQL = """
    FROM history AS h
    LEFT JOIN universe AS u ON u.code = h.code
    WHERE u.name IS NULL OR upper(u.name) NOT LIKE '%ST%'
"""


def _prepare_history_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df["code_id"] = pd.to_numeric(df["code_id"], errors="coerce").astype("int32")
    df["trade_date"] = pd.to_numeric(df["trade_date"], errors="coerce").astype("int32")
    df["big"] = df["big"].astype("bool")
    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    df.dropna(subset=["close", "change_pct"], inplace=True)
    return df


def as_float32(s: pd.Series) -> pd.Series:
    return s.astype("float32")


def shift_by_code(values: pd.Series, codes: pd.Series, periods: int) -> pd.Series:
    """Groupwise shift for data already sorted by code, without pandas groupby indexers."""
    if periods == 0:
        return values.astype("float32")

    n = len(values)
    out = np.full(n, np.nan, dtype="float32")
    src = values.to_numpy(dtype="float32", copy=False)
    code_values = codes.to_numpy(copy=False)

    if periods > 0:
        same_code = code_values[periods:] == code_values[:-periods]
        target = out[periods:]
        target[same_code] = src[:-periods][same_code]
    else:
        p = -periods
        same_code = code_values[:-p] == code_values[p:]
        target = out[:-p]
        target[same_code] = src[p:][same_code]

    return pd.Series(out, index=values.index, name=values.name)


def load() -> pd.DataFrame:
    query = """
        WITH filtered AS (
            SELECT h.code, h.trade_date, h.open, h.close, h.high, h.low,
                   h.volume, h.amount, h.change_pct
            {filtered_sql}
        ),
        codes AS (
            SELECT code, row_number() OVER (ORDER BY code) - 1 AS code_id
            FROM (SELECT DISTINCT code FROM filtered)
        )
        SELECT c.code_id,
               CAST(REPLACE(f.trade_date, '-', '') AS INTEGER) AS trade_date,
               CASE
                   WHEN substr(f.code, 1, 3) IN ('300', '301', '302', '688', '689') THEN 1
                   ELSE 0
               END AS big,
               f.open, f.close, f.high, f.low, f.volume, f.amount, f.change_pct
        FROM filtered AS f
        JOIN codes AS c ON c.code = f.code
        ORDER BY c.code_id, trade_date
        """.format(filtered_sql=FILTERED_SQL)
    con = sqlite3.connect(DB)
    chunks = []
    for chunk in pd.read_sql(query, con, chunksize=LOAD_CHUNKSIZE):
        chunks.append(_prepare_history_chunk(chunk))
    con.close()
    df = pd.concat(chunks, ignore_index=True)
    df.reset_index(drop=True, inplace=True)
    return df


def load_code_lookup() -> dict[int, str]:
    query = """
        SELECT h.code, COALESCE(MAX(u.name), '') AS name
        {filtered_sql}
        GROUP BY h.code
        ORDER BY h.code
        """.format(filtered_sql=FILTERED_SQL)
    con = sqlite3.connect(DB)
    codes = pd.read_sql(query, con)
    con.close()
    lookup = {}
    for code_id, row in enumerate(codes.itertuples(index=False)):
        name = str(row.name).strip()
        lookup[code_id] = f"{row.code} {name}".strip()
    return lookup


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df["thr"] = np.where(df["big"], 19.5, 9.8).astype("float32")
    df["is_lu"] = df["change_pct"] >= df["thr"]

    g = df.groupby("code_id", sort=False)
    c = df["close"]
    code = df["code_id"]

    # 标签：次日涨停
    df["y"] = shift_by_code(df["is_lu"], code, -1)

    # 均线
    ma5 = as_float32(g["close"].transform(lambda s: s.rolling(5, min_periods=5).mean()))
    ma10 = as_float32(g["close"].transform(lambda s: s.rolling(10, min_periods=10).mean()))
    ma20 = as_float32(g["close"].transform(lambda s: s.rolling(20, min_periods=20).mean()))

    # 量比：今日量 / 前5日均量（不含今日）
    vmean5_prev = as_float32(g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean().shift(1)))
    df["vol_ratio"] = as_float32(df["volume"] / vmean5_prev)
    # 蓄势缩量：前3日均量 / 前5日均量
    v3 = as_float32(g["volume"].transform(lambda s: s.rolling(3, min_periods=2).mean()))
    v5 = as_float32(g["volume"].transform(lambda s: s.rolling(5, min_periods=3).mean()))
    df["vol_shrink"] = as_float32(v3 / v5)

    # 均线关系
    df["ma_bullish"] = (ma5 > ma10) & (ma10 > ma20)
    df["dist_ma5"] = as_float32((df["close"] / ma5 - 1) * 100)

    # 趋势
    df["trend10"] = as_float32((c / shift_by_code(df["close"], code, 10) - 1) * 100)

    # 60日位置（0=区间底 100=区间顶）
    min60 = as_float32(g["low"].transform(lambda s: s.rolling(60, min_periods=20).min()))
    max60 = as_float32(g["high"].transform(lambda s: s.rolling(60, min_periods=20).max()))
    rng = (max60 - min60).replace(0, np.nan)
    df["pos60"] = as_float32((df["close"] - min60) / rng * 100)

    # 突破：今日收盘 >= 前20日最高（不含今日）
    high20_prev = as_float32(g["high"].transform(lambda s: s.rolling(20, min_periods=10).max().shift(1)))
    df["new_high20"] = df["close"] >= high20_prev * 0.995
    # 平台紧凑度：前20日箱体振幅（不含今日）
    h20p = high20_prev
    l20p = as_float32(g["low"].transform(lambda s: s.rolling(20, min_periods=10).min().shift(1)))
    df["box20_pct"] = as_float32((h20p - l20p) / l20p * 100)

    # 股性：前20日涨停次数（不含今日）
    df["lu20_prev"] = as_float32(g["is_lu"].transform(lambda s: s.rolling(20, min_periods=1).sum().shift(1)))

    # 连续上涨天数（今日含）
    up = (df["change_pct"] > 0).astype(int)
    # 组内连续计数
    grp_id = (up == 0).groupby(df["code_id"]).cumsum()
    df["up_streak"] = up.groupby([df["code_id"], grp_id]).cumsum()

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


def format_stock_matches(code_ids: pd.Series, code_lookup: dict[int, str], limit: int) -> str:
    ids = [int(code_id) for code_id in code_ids]
    if not ids:
        return "无"
    shown = [code_lookup.get(code_id, str(code_id)) for code_id in ids[:limit]]
    text = "、".join(shown)
    if len(ids) > limit:
        text += f" ... 共{len(ids)}只"
    return text


def latest_setup_stock_lines(
    df: pd.DataFrame,
    code_lookup: dict[int, str],
    stock_limit: int = DISPLAY_STOCK_LIMIT,
) -> list[str]:
    if df.empty:
        return []

    latest_date = int(df["trade_date"].max())
    current = df[df["trade_date"] == latest_date]
    lines = [f"\n--- 最新交易日命中股票（{latest_date}，每类最多显示{stock_limit}只）---"]
    for name, mask in setups(current):
        code_ids = current.loc[mask.fillna(False), "code_id"]
        lines.append(f"  {name:<28}: {format_stock_matches(code_ids, code_lookup, stock_limit)}")
    return lines


def recent_window(df: pd.DataFrame, recent_td: int = RECENT_TD) -> tuple[str, pd.DataFrame]:
    dates = sorted(df["trade_date"].unique())
    if not dates:
        return f"近 {recent_td} 交易日 (无数据) —— 当前市场环境", df
    cut = dates[-recent_td] if len(dates) > recent_td else dates[0]
    recent = df[df["trade_date"] >= cut]
    title = f"近 {min(recent_td, len(dates))} 交易日 ({cut} ~ {dates[-1]}) —— 当前市场环境"
    return title, recent


def feature_window(
    df: pd.DataFrame,
    recent_td: int = RECENT_TD,
    warmup_td: int = FEATURE_WARMUP_TD,
) -> pd.DataFrame:
    dates = sorted(df["trade_date"].unique())
    keep_days = recent_td + warmup_td
    if len(dates) <= keep_days:
        return df
    cut = dates[-keep_days]
    return df[df["trade_date"] >= cut].copy()


def report(
    df: pd.DataFrame,
    title: str,
    code_lookup: dict[int, str] | None = None,
    stock_limit: int = DISPLAY_STOCK_LIMIT,
) -> str:
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
    if code_lookup:
        lines.extend(latest_setup_stock_lines(df, code_lookup, stock_limit))
    return "\n".join(lines)


def main():
    print("加载 history ...", flush=True)
    df = load()
    code_lookup = load_code_lookup()
    print(f"  原始有效行: {len(df):,}  股票数: {df['code_id'].nunique()}", flush=True)
    df = feature_window(df)
    print("构建特征 ...", flush=True)
    df = build_features(df)

    title, recent = recent_window(df)
    print(report(recent, title, code_lookup))


if __name__ == "__main__":
    main()
