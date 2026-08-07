"""按类别给出 T+1 卖出时机建议（竞价走 vs 留到收盘）。

依据是本地 accuracy 表的实证：同样 T 日收盘买入，比较"T+1 竞价卖"与
"持有到 T+1 收盘"两种卖法的平均收益。0408~0805 全样本实测——

    类别      样本   竞价卖    收盘卖    结论
    二波      823   -0.63%   +0.26%   留到收盘
    反包      387   -0.48%   +0.69%   留到收盘
    趋势     1587   -0.17%   +0.73%   留到收盘
    首板      626   -0.46%   -0.19%   留到收盘
    连板      687   +2.80%   +2.22%   竞价可走（开盘后平均倒亏 0.52%）

连板与其余类别的最优卖点相反，用一套卖法会在其中一边持续亏钱。

统计每次调用现算，市场风格切换后结论自动跟着变；样本不足或读库失败时
回退到上表的默认结论。
"""
from __future__ import annotations

from typing import Any, Callable, Dict, NamedTuple, Optional

HOLD_TO_CLOSE = "留到收盘"
SELL_AT_AUCTION = "竞价可走"

# 样本不足/读库失败时的兜底（0408~0805 实证，见模块 docstring）
_DEFAULT_ADVICE: Dict[str, str] = {
    "cont": SELL_AT_AUCTION,
    "first": HOLD_TO_CLOSE,
    "wrap": HOLD_TO_CLOSE,
    "fresh": HOLD_TO_CLOSE,
    "trend": HOLD_TO_CLOSE,
}

_MIN_SAMPLES = 30
# 收益差在正负 0.15 个百分点内视为没有区分度，按"留到收盘"处理（卖出少一次冲击成本）
_FLAT_BAND_PCT = 0.15


class ExitHint(NamedTuple):
    advice: str          # HOLD_TO_CLOSE / SELL_AT_AUCTION
    edge_pct: float      # 收盘卖 - 竞价卖，正数=留到收盘更划算（百分点）
    samples: int         # 参与统计的样本数；0 表示用的是兜底默认值

    @property
    def from_data(self) -> bool:
        return self.samples > 0

    def describe(self) -> str:
        if not self.from_data:
            return f"{self.advice}(默认)"
        return f"{self.advice}({self.edge_pct:+.2f}%/{self.samples}样本)"


def _advice_for(edge_pct: float) -> str:
    return SELL_AT_AUCTION if edge_pct < -_FLAT_BAND_PCT else HOLD_TO_CLOSE


def compute_exit_hints(
    *,
    lookback_days: int = 120,
    min_samples: int = _MIN_SAMPLES,
    load_rows_fn: Optional[Callable[[int], Any]] = None,
) -> Dict[str, ExitHint]:
    """按类别算 T+1 两种卖法的收益差。

    只统计能真正成交的样本：排除停牌与一字板（竞价买不到也卖不掉）。
    """
    rows = []
    try:
        loader = load_rows_fn or _load_rows
        rows = list(loader(lookback_days) or [])
    except Exception:
        rows = []

    grouped: Dict[str, list] = {}
    for row in rows:
        try:
            category = str(row[0] or "").strip()
            t_close = float(row[1])
            t1_open = float(row[2])
            t1_close = float(row[3])
        except (TypeError, ValueError, IndexError):
            continue
        if not category or t_close <= 0 or t1_open <= 0:
            continue
        auction_pct = (t1_open / t_close - 1.0) * 100.0
        close_pct = (t1_close / t_close - 1.0) * 100.0
        grouped.setdefault(category, []).append(close_pct - auction_pct)

    hints: Dict[str, ExitHint] = {}
    for category, default_advice in _DEFAULT_ADVICE.items():
        edges = grouped.get(category) or []
        if len(edges) < min_samples:
            hints[category] = ExitHint(default_advice, 0.0, 0)
            continue
        edge = sum(edges) / len(edges)
        hints[category] = ExitHint(_advice_for(edge), edge, len(edges))
    return hints


def _load_rows(lookback_days: int):
    """读 accuracy 表里可成交的 T+1 样本。"""
    from datetime import datetime, timedelta

    import stock_store

    start = (datetime.now() - timedelta(days=max(1, int(lookback_days)))).strftime("%Y%m%d")
    sql = """
        SELECT category, t_close, t1_open, t1_close
        FROM limit_up_prediction_accuracy
        WHERE trade_date >= ?
          AND t_close > 0 AND t1_open > 0 AND t1_close > 0
          AND COALESCE(t1_suspended, 0) = 0
          AND COALESCE(t1_one_word, 0) = 0
    """
    with stock_store._connect() as conn:
        return conn.execute(sql, (start,)).fetchall()
