"""从本地 history 表反推历史 limit_up_pool，用于扩展回测样本。

派生规则：
- 严格过滤：close == high (封板) + 按板块阈值过滤 change_pct
- ST 股按 5% 限幅
- JOIN universe 取 name + industry
- 连板数：递推查找 D-1 pool（先按时间顺序回填，保证递推可用）

字段限制：
- 炸板次数 = 0 (反推没分钟数据，无法判定)
- 首次/最后封板时间, 封板资金, 流通市值, 总市值 = NaN

不覆盖已缓存的 limit_up_pool，只填补缺失日期。
"""
from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Set

import pandas as pd

import stock_store
from stock_logger import get_logger

logger = get_logger(__name__)

# 反推涨停的 SQL：close==high 且按板块/ST 区分限幅
_DERIVE_SQL = """
SELECT h.code,
       u.name,
       u.industry,
       h.close, h.open, h.high, h.low,
       h.change_pct,
       h.amount, h.volume,
       h.turnover_rate
FROM history h
LEFT JOIN universe u ON u.code = h.code
WHERE h.trade_date = ?
  AND h.close = h.high
  AND (
       (INSTR(COALESCE(u.name, ''), 'ST') > 0 AND h.change_pct >= 4.7) OR
       (INSTR(COALESCE(u.name, ''), 'ST') = 0 AND substr(h.code,1,2) IN ('30','68') AND h.change_pct >= 19.5) OR
       (INSTR(COALESCE(u.name, ''), 'ST') = 0 AND substr(h.code,1,1) IN ('4','8') AND h.change_pct >= 29.5) OR
       (INSTR(COALESCE(u.name, ''), 'ST') = 0 AND substr(h.code,1,1) IN ('6','0') AND h.change_pct >= 9.7)
  )
ORDER BY h.change_pct DESC, h.code
"""


def _date_dash_to_key(date_dash: str) -> str:
    return str(date_dash or "").replace("-", "")


def _date_key_to_dash(date_key: str) -> str:
    s = str(date_key or "").replace("-", "")
    return f"{s[:4]}-{s[4:6]}-{s[6:]}" if len(s) == 8 else s


def list_history_dates() -> List[str]:
    """history 表里所有 distinct trade_date，升序 (YYYY-MM-DD)。"""
    with stock_store._connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT trade_date FROM history ORDER BY trade_date"
        ).fetchall()
    return [str(r[0]) for r in rows]


def derive_pool_for_date(date_dash: str) -> Optional[pd.DataFrame]:
    """反推单日涨停股 DataFrame（不含连板数；由调用方填充）。"""
    with stock_store._connect() as conn:
        df = pd.read_sql_query(_DERIVE_SQL, conn, params=[date_dash])
    if df is None or df.empty:
        return None
    df = df.rename(columns={
        "code": "代码",
        "name": "名称",
        "industry": "所属行业",
        "close": "最新价",
        "change_pct": "涨跌幅",
        "amount": "成交额",
        "turnover_rate": "换手率",
    })
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    df["名称"] = df["名称"].fillna("").astype(str)
    df["所属行业"] = df["所属行业"].fillna("").astype(str)
    # 反推无法得到的字段：填 0 / NaN
    df["炸板次数"] = 0
    df["首次封板时间"] = pd.NA
    df["最后封板时间"] = pd.NA
    df["封板资金"] = pd.NA
    df["流通市值"] = pd.NA
    df["总市值"] = pd.NA
    df["涨停统计"] = "1/1"
    # 序号 + 列顺序对齐原 limit_up_pool
    df["序号"] = range(1, len(df) + 1)
    columns = [
        "序号", "代码", "名称", "涨跌幅", "最新价", "成交额",
        "流通市值", "总市值", "换手率",
        "封板资金", "首次封板时间", "最后封板时间", "炸板次数",
        "涨停统计", "连板数", "所属行业",
    ]
    if "连板数" not in df.columns:
        df["连板数"] = 1
    df = df[columns]
    return df


def backfill_derived_pools(
    *,
    overwrite: bool = False,
    log: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """遍历 history 全部日期，反推缺失的 limit_up_pool 并入库。

    Args:
        overwrite: True 时覆盖已缓存的（默认 False，仅填补缺失）

    Returns: {"derived": N, "skipped": N, "empty": N, "errors": N}
    """
    def _l(m: str) -> None:
        if log:
            try: log(m)
            except Exception: pass
        logger.info(m)

    all_dates_dash = list_history_dates()
    if not all_dates_dash:
        return {"derived": 0, "skipped": 0, "empty": 0, "errors": 0}

    cached: Set[str] = set(stock_store.list_limit_up_pool_trade_dates() or [])
    _l(f"反推开始: history 表共 {len(all_dates_dash)} 个交易日, "
       f"已缓存 {len(cached)} 个 limit_up_pool")

    stats = {"derived": 0, "skipped": 0, "empty": 0, "errors": 0}
    start_t = time.time()

    for i, d_dash in enumerate(all_dates_dash, 1):
        d_key = _date_dash_to_key(d_dash)
        if not overwrite and d_key in cached:
            stats["skipped"] += 1
            continue
        try:
            df = derive_pool_for_date(d_dash)
            if df is None or df.empty:
                stats["empty"] += 1
                continue
            # 派生连板数：查上一交易日的 pool
            prev_key = _previous_history_date(d_dash, all_dates_dash, i)
            prev_boards: Dict[str, int] = {}
            if prev_key:
                prev_df = stock_store.load_limit_up_pool(prev_key)
                if prev_df is not None and not prev_df.empty and "代码" in prev_df.columns:
                    prev_codes = prev_df["代码"].astype(str).str.zfill(6)
                    prev_b = (
                        prev_df["连板数"].astype(int)
                        if "连板数" in prev_df.columns
                        else pd.Series([1] * len(prev_df))
                    )
                    prev_boards = dict(zip(prev_codes, prev_b))
            df["连板数"] = df["代码"].map(lambda c: prev_boards.get(c, 0) + 1).astype(int)

            stock_store.save_limit_up_pool(d_key, df)
            cached.add(d_key)
            stats["derived"] += 1

            if stats["derived"] % 20 == 0:
                elapsed = time.time() - start_t
                _l(f"  进度 {i}/{len(all_dates_dash)} "
                   f"(已派生 {stats['derived']}, 跳过 {stats['skipped']}, "
                   f"用时 {elapsed:.0f}s)")
        except Exception as exc:
            stats["errors"] += 1
            _l(f"  {d_dash} 失败: {exc}")

    elapsed = time.time() - start_t
    _l(f"反推完成: 派生 {stats['derived']} · 跳过 {stats['skipped']} · "
       f"空 {stats['empty']} · 错 {stats['errors']} · 用时 {elapsed:.1f}s")
    return stats


def _previous_history_date(
    d_dash: str,
    all_dates_dash: List[str],
    current_index_1based: int,
) -> str:
    """从已排序列表里找 d_dash 的上一个交易日，返回 YYYYMMDD。"""
    idx = current_index_1based - 1  # 0-based
    if idx <= 0:
        return ""
    return _date_dash_to_key(all_dates_dash[idx - 1])
