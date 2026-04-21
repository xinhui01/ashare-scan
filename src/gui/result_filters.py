"""扫描结果列表的客户端过滤谓词。

GUI 扫完出来的一个 dict 列表，用户经常需要在这几百条里再细分。本模块只放**纯谓词**
——不依赖 Tk，所有输入都是普通 Python 值，方便单测。

每个 `filter_*` 函数签名：`(item: dict, threshold_or_flag) -> bool`
返回 True 表示**保留**该 item。

组合使用时只需在 stock_gui 里把门槛从 Tk var 读出来，挑需要启用的谓词依次做
`[x for x in items if predicate(x, t)]`。
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _analysis(item: Dict[str, Any]) -> Dict[str, Any]:
    return (item.get("data", {}) or {}).get("analysis") or {}


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 搜索（代码或名称子串匹配）
# ---------------------------------------------------------------------------

def matches_search(item: Dict[str, Any], needle: str) -> bool:
    """代码或名称包含 needle 子串（大小写不敏感）；空 needle 不过滤。"""
    needle = (needle or "").strip()
    if not needle:
        return True
    needle_lower = needle.lower()
    code = str(item.get("code", "") or "").strip().lower()
    name = str(item.get("name", "") or "").strip().lower()
    return needle_lower in code or needle_lower in name


# ---------------------------------------------------------------------------
# 数值下限：评分 / 5 日涨幅 / 放量倍数 / 连板数
# ---------------------------------------------------------------------------

def at_least_score(item: Dict[str, Any], min_score: Optional[float]) -> bool:
    if min_score is None:
        return True
    value = _to_float(_analysis(item).get("score"))
    return value is not None and value >= float(min_score)


def at_least_five_day_return(item: Dict[str, Any], min_pct: Optional[float]) -> bool:
    if min_pct is None:
        return True
    value = _to_float(_analysis(item).get("five_day_return"))
    return value is not None and value >= float(min_pct)


def at_least_volume_ratio(item: Dict[str, Any], min_ratio: Optional[float]) -> bool:
    if min_ratio is None:
        return True
    value = _to_float(_analysis(item).get("volume_expand_ratio"))
    return value is not None and value >= float(min_ratio)


def at_least_limit_up_streak(item: Dict[str, Any], min_streak: Optional[int]) -> bool:
    if min_streak is None or int(min_streak) <= 0:
        return True
    value = _analysis(item).get("limit_up_streak") or 0
    try:
        return int(value) >= int(min_streak)
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# 布尔"只显示 X"
# ---------------------------------------------------------------------------

def only_in_watchlist(
    item: Dict[str, Any],
    enabled: bool,
    watchlist_codes: set,
) -> bool:
    """勾选时只保留在自选池里的股票。"""
    if not enabled:
        return True
    code = str(item.get("code", "") or "").strip().zfill(6)
    return code in watchlist_codes


def _analysis_bool(item: Dict[str, Any], key: str) -> bool:
    return bool(_analysis(item).get(key))


def only_limit_up(item: Dict[str, Any], enabled: bool) -> bool:
    return (not enabled) or _analysis_bool(item, "limit_up")


def only_broken_limit_up(item: Dict[str, Any], enabled: bool) -> bool:
    return (not enabled) or _analysis_bool(item, "broken_limit_up")


def only_volume_expand(item: Dict[str, Any], enabled: bool) -> bool:
    return (not enabled) or _analysis_bool(item, "volume_expand")


def only_strong_followthrough(item: Dict[str, Any], enabled: bool) -> bool:
    if not enabled:
        return True
    ft = _analysis(item).get("strong_followthrough") or {}
    return bool(ft.get("has_strong_followthrough"))


# ---------------------------------------------------------------------------
# 价格区间（保留原语义：支持 min/max 任一或同时为 None）
# ---------------------------------------------------------------------------

def within_price_range(
    item: Dict[str, Any],
    min_price: Optional[float],
    max_price: Optional[float],
) -> bool:
    if min_price is None and max_price is None:
        return True
    latest = _to_float(_analysis(item).get("latest_close"))
    if latest is None:
        # 没有最新价时：只要设置了任一门槛就过滤掉（保留旧行为）
        return False
    if min_price is not None and latest < float(min_price):
        return False
    if max_price is not None and latest > float(max_price):
        return False
    return True
