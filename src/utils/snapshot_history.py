"""全市场行情快照 → history 行的规范化转换。

把 akshare `stock_zh_a_spot_em` / `stock_zh_a_spot` 返回的单行记录转成
可写入 `history` 表的字典，并**在盘中态（INTRADAY）硬拦截**，避免把
实时价当成收盘价写库。

快照来源常见字段名：
    东财（em）: 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅
                最高 最低 今开 昨收 换手率
    新浪     : 代码 名称 最新价 涨跌额 涨跌幅 买入 卖出 昨收 今开
                最高 最低 成交量 成交额

本模块只负责转换，不做入库。调用方拿到 dict 后喂给
`stock_store.save_history_rows_batch({code: DataFrame})`。
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional

from .trade_calendar import TradePhase

try:
    import pandas as _pd  # type: ignore

    _PD_ISNA = _pd.isna
except Exception:  # pandas 不存在时退化为无 NaN 检查
    _PD_ISNA = None  # type: ignore

# 依赖完整 OHLC 的字段列表（决定 partial_fields 产出）
_REQUIRED_OHLC = ("open", "high", "low")

# 各来源字段名到统一内部名的映射。多个别名指向同一内部字段，按顺序取第一个非空。
_FIELD_ALIASES: Dict[str, tuple] = {
    "code": ("代码", "code"),
    "close": ("最新价", "close", "收盘"),
    "open": ("今开", "open", "开盘"),
    "high": ("最高", "high"),
    "low": ("最低", "low"),
    "volume": ("成交量", "volume"),
    "amount": ("成交额", "amount"),
    "amplitude": ("振幅", "amplitude"),
    "change_pct": ("涨跌幅", "change_pct"),
    "change_amount": ("涨跌额", "change_amount"),
    "turnover_rate": ("换手率", "turnover_rate"),
}


def _is_nan(v: Any) -> bool:
    if _PD_ISNA is None:
        return False
    try:
        result = _PD_ISNA(v)
    except (TypeError, ValueError):
        return False
    # pandas.isna 对数组输入返回数组；这里只处理标量
    return bool(result) if isinstance(result, bool) else False


def _pick(row: Mapping[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row:
            v = row[k]
            if v is None or _is_nan(v):
                continue
            return v
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or _is_nan(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _normalize_code(raw: Any) -> str:
    """快照代码常带交易所前缀（sh600000），统一剥离并补零到 6 位。"""
    text = str(raw or "").strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    return text.strip().zfill(6) if text else ""


def snapshot_row_to_history_row(
    row: Mapping[str, Any],
    target_date: str,
    phase: TradePhase,
) -> Optional[Dict[str, Any]]:
    """把一行快照转成 history 行 dict。

    - `phase == INTRADAY` 时返回 None（硬门禁：不能把实时价当收盘价写库）。
    - `code`/`close` 任一缺失时返回 None。
    - 缺失的 OHLC 字段不会补零，而是通过返回值里的 `partial_fields` 字段报告。
    - `target_date` 作为写入日期，不使用快照自带日期字段（快照常常没有）。

    返回 dict 结构与 `save_history` / `save_history_rows_batch` 期望的列一致，
    额外带两个辅助键：`partial_fields`、`needs_repair`（调用方用于更新 meta）。
    """
    if phase is TradePhase.INTRADAY:
        return None

    code = _normalize_code(_pick(row, *_FIELD_ALIASES["code"]))
    if not code:
        return None

    close = _to_float(_pick(row, *_FIELD_ALIASES["close"]))
    if close is None:
        return None

    values: Dict[str, Optional[float]] = {"close": close}
    for name in ("open", "high", "low", "volume", "amount",
                 "amplitude", "change_pct", "change_amount", "turnover_rate"):
        values[name] = _to_float(_pick(row, *_FIELD_ALIASES[name]))

    missing = [f for f in _REQUIRED_OHLC if values.get(f) is None]

    return {
        "code": code,
        "date": str(target_date).strip(),
        "open": values["open"],
        "close": values["close"],
        "high": values["high"],
        "low": values["low"],
        "volume": values["volume"],
        "amount": values["amount"],
        "amplitude": values["amplitude"],
        "change_pct": values["change_pct"],
        "change_amount": values["change_amount"],
        "turnover_rate": values["turnover_rate"],
        "partial_fields": ",".join(missing),
        "needs_repair": 1 if missing else 0,
    }


def snapshot_rows_to_history_rows(
    rows: Iterable[Mapping[str, Any]],
    target_date: str,
    phase: TradePhase,
) -> List[Dict[str, Any]]:
    """批量版本，跳过所有返回 None 的行。INTRADAY 态整体返回空列表。"""
    if phase is TradePhase.INTRADAY:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        converted = snapshot_row_to_history_row(row, target_date, phase)
        if converted is not None:
            out.append(converted)
    return out
