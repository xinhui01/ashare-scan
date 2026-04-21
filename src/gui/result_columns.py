"""扫描结果表格列的声明式注册表。

之前 stock_gui 里"一列"的信息被切成 4 份散在不同地方（列 ID tuple、表头字典、
默认可见 tuple、默认降序 set），另外 `_format_result_row_values` / `_sort_value_for_column`
还把每列的字段提取逻辑抄了两遍。每增一列要动 6 处，容易出"显示对、排序错"这种
难查的 bug。

这里把一列的**全部真相**收敛成一条 `ResultColumn` 记录。GUI 侧只负责把结果字典
喂给 `format_cell` / `sort_key`，其它都走声明式配置。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


Extractor = Callable[[Dict[str, Any], Dict[str, Any]], Any]
"""(result_item, context) -> 原始值。context 是 `{"watchlist_items": {...}}` 这类 GUI 状态。"""

CellFormatter = Callable[[Any], str]
SortKey = Callable[[Any, Dict[str, Any]], Any]


NEG_INF = float("-inf")
POS_INF = float("inf")


def _walk_path(item: Dict[str, Any], path: str) -> Any:
    """按 `a.b.c` 走字典取值，任何一环 None/缺失就返回 None。"""
    obj: Any = item
    for part in path.split("."):
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(part)
        else:
            return None
    return obj


def _fmt_float(value: Any, suffix: str = "", decimals: int = 2, placeholder: str = "-") -> str:
    if value is None:
        return placeholder
    try:
        return f"{float(value):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return placeholder


def _fmt_ratio_as_pct(value: Any, placeholder: str = "-") -> str:
    """把小数比值格式化为"%"显示,例如 0.62 → "62%"。"""
    if value is None:
        return placeholder
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return placeholder


def _fmt_int(value: Any, placeholder: str = "0") -> str:
    if value is None:
        return placeholder
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return placeholder


def _fmt_bool_cn(value: Any) -> str:
    return "是" if bool(value) else "否"


def _fmt_recent_closes(value: Any) -> str:
    if not value:
        return ""
    parts: List[str] = []
    for v in value:
        if v is None:
            parts.append("-")
            continue
        try:
            parts.append(f"{float(v):.2f}")
        except (TypeError, ValueError):
            parts.append("-")
    return ", ".join(parts)


def _sort_number(value: Any, *, default: float = NEG_INF) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sort_int_bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _sort_recent_closes(value: Any) -> tuple:
    if not value:
        return ()
    parts = []
    for v in value:
        if v is None:
            parts.append("")
            continue
        try:
            parts.append(f"{float(v):010.4f}")
        except (TypeError, ValueError):
            parts.append("")
    return tuple(parts)


# --- 几个非平凡列的自定义取值 / 格式化 ---

def _extract_code(item: Dict[str, Any], _ctx: Dict[str, Any]) -> str:
    return str(item.get("code", "") or "").strip().zfill(6)


def _extract_watch_flag(item: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    watchlist = ctx.get("watchlist_items") or {}
    code = _extract_code(item, ctx)
    return code in watchlist


def _extract_board_or_exchange(item: Dict[str, Any], _ctx: Dict[str, Any]) -> str:
    data = item.get("data", {}) or {}
    return str(data.get("board") or data.get("exchange") or "")


def _fmt_code(value: Any) -> str:
    s = str(value or "").strip()
    return s or "-"


def _fmt_str(value: Any, placeholder: str = "-") -> str:
    s = str(value or "").strip()
    return s or placeholder


def _fmt_watch(value: Any) -> str:
    return "自选" if bool(value) else ""


@dataclass(frozen=True)
class ResultColumn:
    """一列的全部真相。

    `kind` 决定 format / sort 的默认行为；当默认不够用时用 `format_override`
    或 `sort_override` 精调。需要读外部 GUI 状态（比如自选池）就给 `extract`。
    """
    id: str
    label: str
    width: int = 90
    path: Optional[str] = None
    extract: Optional[Extractor] = None
    kind: str = "float"             # "float" | "float-x" | "float-pct" | "int" | "bool-cn" | "str" | "code" | "watch" | "recent-closes"
    default_visible: bool = True
    sort_desc_by_default: bool = False
    anchor: str = "center"
    format_override: Optional[CellFormatter] = None
    sort_override: Optional[SortKey] = None

    def extract_value(self, item: Dict[str, Any], context: Dict[str, Any]) -> Any:
        if self.extract is not None:
            return self.extract(item, context)
        if self.path:
            return _walk_path(item, self.path)
        return None

    def format_cell(self, item: Dict[str, Any], context: Dict[str, Any]) -> str:
        raw = self.extract_value(item, context)
        if self.format_override is not None:
            return self.format_override(raw)
        return _default_format(raw, self.kind)

    def sort_key(self, item: Dict[str, Any], context: Dict[str, Any]) -> Any:
        raw = self.extract_value(item, context)
        if self.sort_override is not None:
            return self.sort_override(raw, context)
        return _default_sort_key(raw, self.kind)


def _default_format(value: Any, kind: str) -> str:
    if kind == "float":
        return _fmt_float(value)
    if kind == "float-x":
        return _fmt_float(value, suffix="x")
    if kind == "float-pct":
        return _fmt_float(value, suffix="%")
    if kind == "ratio-pct":
        return _fmt_ratio_as_pct(value)
    if kind == "int":
        return _fmt_int(value)
    if kind == "bool-cn":
        return _fmt_bool_cn(value)
    if kind == "str":
        return _fmt_str(value)
    if kind == "code":
        return _fmt_code(value)
    if kind == "watch":
        return _fmt_watch(value)
    if kind == "recent-closes":
        return _fmt_recent_closes(value)
    # 兜底：strify
    return "-" if value is None else str(value)


def _default_sort_key(value: Any, kind: str) -> Any:
    if kind in ("float", "float-x", "float-pct", "ratio-pct"):
        return _sort_number(value)
    if kind == "int":
        return _sort_number(value, default=0)
    if kind in ("bool-cn", "watch"):
        return _sort_int_bool(value)
    if kind in ("str", "code"):
        return str(value or "")
    if kind == "recent-closes":
        return _sort_recent_closes(value)
    return value


# --- 注册表 ---
# 顺序即列在表格里的默认顺序；`default_visible=False` 的列不在默认视图里显示，
# 但可在"结果列配置"里勾上。

RESULT_COLUMNS: List[ResultColumn] = [
    ResultColumn(
        id="code", label="代码", width=90, kind="code", extract=_extract_code,
    ),
    ResultColumn(
        id="name", label="名称", width=140, path="name", kind="str", anchor="w",
    ),
    ResultColumn(
        id="watch", label="自选", width=60, kind="watch",
        extract=_extract_watch_flag, sort_desc_by_default=True,
    ),
    ResultColumn(
        id="score", label="评分", width=70, path="data.analysis.score",
        kind="int", sort_desc_by_default=True,
    ),
    ResultColumn(
        id="board", label="板块", width=120, kind="str",
        extract=_extract_board_or_exchange,
    ),
    ResultColumn(
        id="latest_close", label="最新收盘", width=100,
        path="data.analysis.latest_close", kind="float", sort_desc_by_default=True,
    ),
    ResultColumn(
        id="latest_ma", label="MA", width=100,
        path="data.analysis.latest_ma", kind="float", sort_desc_by_default=True,
    ),
    ResultColumn(
        id="five_day_return", label="5日涨幅", width=90,
        path="data.analysis.five_day_return", kind="float-pct",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="limit_up_streak", label="连板数", width=80,
        path="data.analysis.limit_up_streak", kind="int",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="broken_limit_up", label="断板", width=70,
        path="data.analysis.broken_limit_up", kind="bool-cn",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="volume_expand_ratio", label="放量倍数", width=90,
        path="data.analysis.volume_expand_ratio", kind="float-x",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="volume_expand", label="放量", width=70,
        path="data.analysis.volume_expand", kind="bool-cn",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="volume_break_limit_up", label="放量断板", width=90,
        path="data.analysis.volume_break_limit_up", kind="bool-cn",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="after_two_limit_up", label="二连板后", width=90,
        path="data.analysis.after_two_limit_up", kind="bool-cn",
        sort_desc_by_default=True,
    ),
    ResultColumn(
        id="limit_up", label="涨停", width=70,
        path="data.analysis.limit_up", kind="bool-cn",
        sort_desc_by_default=True,
    ),
    # --- 承接强势形态（默认不展示，用户按需勾选）---
    ResultColumn(
        id="strong_followthrough", label="承接形态", width=90,
        path="data.analysis.strong_followthrough.has_strong_followthrough",
        kind="bool-cn", default_visible=False, sort_desc_by_default=True,
    ),
    ResultColumn(
        id="strong_ft_limit_up_date", label="涨停日", width=100,
        path="data.analysis.strong_followthrough.limit_up_date",
        kind="str", default_visible=False,
    ),
    ResultColumn(
        id="strong_ft_pullback_pct", label="次日回撤%", width=90,
        path="data.analysis.strong_followthrough.pullback_pct",
        kind="float-pct", default_visible=False,
    ),
    ResultColumn(
        id="strong_ft_volume_ratio", label="次日量能", width=90,
        path="data.analysis.strong_followthrough.pullback_volume_ratio",
        # 用 "62%" 而不是 "0.62x"，和过滤失败原因里的 `:.0%` 保持一致
        kind="ratio-pct", default_visible=False,
    ),
    ResultColumn(
        id="strong_ft_hold_days", label="站稳天数", width=80,
        path="data.analysis.strong_followthrough.hold_days",
        kind="int", default_visible=False, sort_desc_by_default=True,
    ),
    # --- 最近 N 日收盘（放最后，宽列）---
    ResultColumn(
        id="recent_closes", label="最近收盘", width=220,
        path="data.analysis.recent_closes", kind="recent-closes",
        default_visible=False,
    ),
]


# --- 便捷查询函数 ---

def columns_by_id() -> Dict[str, ResultColumn]:
    return {col.id: col for col in RESULT_COLUMNS}


def default_visible_ids() -> tuple:
    return tuple(col.id for col in RESULT_COLUMNS if col.default_visible)


def all_column_ids() -> tuple:
    return tuple(col.id for col in RESULT_COLUMNS)


def desc_by_default_ids() -> set:
    return {col.id for col in RESULT_COLUMNS if col.sort_desc_by_default}
