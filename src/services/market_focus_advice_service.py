"""Market-state based focus advice for limit-up prediction categories."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence


CATEGORY_LABELS: Dict[str, str] = {
    "cont": "保留涨停/连板",
    "first": "二波接力",
    "fresh": "首板涨停",
    "wrap": "反包",
    "trend": "趋势涨停",
}

CATEGORY_ORDER = ("cont", "first", "fresh", "wrap", "trend")

PREDICTION_CATEGORY_KEYS: Dict[str, str] = {
    "cont": "continuation_candidates",
    "first": "first_board_candidates",
    "fresh": "fresh_first_board_candidates",
    "wrap": "broken_board_wrap_candidates",
    "trend": "trend_limit_up_candidates",
}

_CATEGORY_ALIASES = {
    "continuation": "cont",
    "relay": "first",
    "followthrough": "first",
    "first_board": "fresh",
    "first-board": "fresh",
    "fresh_first_board": "fresh",
    "broken_board_wrap": "wrap",
    "broken-board-wrap": "wrap",
    "trend_limit_up": "trend",
    "trend-limit-up": "trend",
}

_STATE_FOCUS: Dict[str, Dict[str, Any]] = {
    "接力日": {
        "primary": ["cont"],
        "secondary": ["first", "fresh"],
        "avoid": ["wrap", "trend"],
        "reason": "连板/二波接力有赚钱效应，首板只看主线补涨。",
    },
    "轮动日": {
        "primary": ["fresh"],
        "secondary": ["wrap"],
        "avoid": ["cont", "first", "trend"],
        "reason": "首板新题材优先，反包只做修复，老主线接力降权。",
    },
    "退潮日": {
        "primary": ["wrap"],
        "secondary": ["fresh"],
        "avoid": ["cont", "first", "trend"],
        "reason": "不追高位接力，优先反包修复，首板仅轻仓试错。",
    },
    "冰点日": {
        "primary": [],
        "secondary": ["wrap"],
        "avoid": ["cont", "first", "fresh", "trend"],
        "reason": "原则空仓观望，最多极少量试探超跌反包。",
        "wait_text": "空仓观望",
    },
    "过渡日": {
        "primary": ["fresh"],
        "secondary": ["first", "wrap"],
        "avoid": ["cont", "trend"],
        "reason": "状态未定，首板试错优先，二波/反包只做确认后的备选。",
    },
}


def _canonical_category(value: Any) -> str:
    text = str(value or "").strip().lower()
    return _CATEGORY_ALIASES.get(text, text)


def _safe_count(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _unique_categories(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for value in values or []:
        cat = _canonical_category(value)
        if cat in CATEGORY_LABELS and cat not in out:
            out.append(cat)
    return out


def _items(categories: Sequence[str], counts: Mapping[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "category": cat,
            "label": CATEGORY_LABELS[cat],
            "count": _safe_count(counts.get(cat)),
        }
        for cat in categories
        if cat in CATEGORY_LABELS
    ]


def _format_items(items: Sequence[Mapping[str, Any]], *, warn_zero: bool = False) -> str:
    parts: List[str] = []
    for item in items:
        count = _safe_count(item.get("count"))
        text = f"{item.get('label', '')}({count}只)"
        if warn_zero and count == 0:
            text = f"{item.get('label', '')}(0只，宁可空仓不硬买)"
        parts.append(text)
    return "、".join(parts)


def _sentiment_score(compare_context: Mapping[str, Any]) -> int | None:
    raw_score = compare_context.get("sentiment_score")
    if raw_score is None:
        sentiment = compare_context.get("sentiment") or {}
        if isinstance(sentiment, Mapping):
            raw_score = sentiment.get("score") or sentiment.get("sentiment_score")
    try:
        return int(raw_score)
    except (TypeError, ValueError):
        return None


def _execution_rules(compare_context: Mapping[str, Any], primary: Sequence[str]) -> List[str]:
    state_label = str(compare_context.get("market_state_label") or "").strip()
    score = _sentiment_score(compare_context)
    has_fresh_focus = "fresh" in primary
    if not has_fresh_focus:
        return []

    rules = [
        "执行规则：谁所在板块最强、谁先主动放量上板，优先做谁；没有板块共振，一个都不做。"
    ]
    if state_label in {"轮动日", "退潮日", "过渡日"}:
        rules.append("执行规则：弱情绪日只做确认，不做预测型埋伏。")
    if score is not None and score < 30:
        rules.append(
            "弱情绪过滤：市场情绪低于30分时，首板池只作为观察名单；必须等板块共振 + 个股主动上板确认。"
        )
    return rules


def _qualified_strong_main_line(compare_context: Mapping[str, Any]) -> Dict[str, Any]:
    line = compare_context.get("strong_main_line") or {}
    if not isinstance(line, Mapping):
        return {}
    name = str(line.get("name") or "").strip()
    phase = str(line.get("phase") or "").strip()
    trend = str(line.get("trend") or "").strip()
    try:
        today_count = int(line.get("today_count") or 0)
    except (TypeError, ValueError):
        today_count = 0
    try:
        active_days = int(line.get("active_days") or 0)
    except (TypeError, ValueError):
        active_days = 0
    try:
        opportunity_score = int(line.get("opportunity_score") or 0)
    except (TypeError, ValueError):
        opportunity_score = 0
    if not name or phase != "主升" or trend == "declining" or today_count < 2:
        return {}
    if active_days < 3 and opportunity_score < 60:
        return {}
    return dict(line)


def prediction_category_counts(prediction: Mapping[str, Any]) -> Dict[str, int]:
    """Return candidate counts in the five scoring categories."""
    counts: Dict[str, int] = {}
    for category, key in PREDICTION_CATEGORY_KEYS.items():
        rows = prediction.get(key) if isinstance(prediction, Mapping) else []
        try:
            counts[category] = len(rows or [])
        except TypeError:
            counts[category] = 0
    return counts


def build_market_focus_advice(
    compare_context: Mapping[str, Any],
    category_counts: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Translate market state into category focus advice.

    The scoring layer already uses market state as a factor. This helper makes
    the same regime decision explicit for the user-facing summary.
    """
    if not isinstance(compare_context, Mapping):
        return {}

    market_state = compare_context.get("market_state") or {}
    state_label = str(compare_context.get("market_state_label") or market_state.get("label") or "").strip()
    strategy = compare_context.get("market_state_strategy") or market_state.get("strategy") or {}
    counts = {cat: _safe_count((category_counts or {}).get(cat)) for cat in CATEGORY_ORDER}
    if not state_label and not strategy:
        return {}

    spec = _STATE_FOCUS.get(state_label)
    if spec is None:
        pools = _unique_categories((strategy or {}).get("pools") or [])
        primary = pools[:1]
        secondary = pools[1:]
        avoid = [cat for cat in CATEGORY_ORDER if cat not in pools]
        reason = str((strategy or {}).get("notes") or (strategy or {}).get("label") or "按市场情绪推荐池优先观察。")
        wait_text = ""
    else:
        primary = _unique_categories(spec.get("primary") or [])
        secondary = _unique_categories(spec.get("secondary") or [])
        avoid = _unique_categories(spec.get("avoid") or [])
        reason = str(spec.get("reason") or "")
        wait_text = str(spec.get("wait_text") or "")

    strong_line = _qualified_strong_main_line(compare_context)
    if strong_line and state_label in {"轮动日", "过渡日"}:
        primary = ["first", "trend"]
        secondary = ["fresh", "cont"]
        avoid = ["wrap"]
        name = str(strong_line.get("name") or "").strip()
        source = str(strong_line.get("source") or "").strip() or "主线"
        active_days = int(strong_line.get("active_days") or 0)
        today_count = int(strong_line.get("today_count") or 0)
        reason = (
            f"已有持续主线{name}（{source}，{active_days}个活跃日，今日{today_count}只），"
            "优先做主线内二波/趋势和补涨；偏离主线的新首板不作为主策略。"
        )
        wait_text = ""

    primary_items = _items(primary, counts)
    secondary_items = _items(secondary, counts)
    avoid_items = _items(avoid, counts)

    primary_text = _format_items(primary_items, warn_zero=True)
    secondary_text = _format_items(secondary_items, warn_zero=True)
    if not primary_text and wait_text:
        focus_text = wait_text
        if secondary_text:
            focus_text = f"{focus_text}；极少试探：{secondary_text}"
    elif primary_text:
        focus_text = primary_text
    else:
        focus_text = secondary_text

    avoid_text = _format_items(avoid_items)
    state_text = state_label or str((strategy or {}).get("label") or "市场状态")
    execution_rules = _execution_rules(compare_context, primary)
    if strong_line and state_label in {"轮动日", "过渡日"}:
        line_name = str(strong_line.get("name") or "").strip()
        execution_rules.insert(
            0,
            f"执行规则：先看{line_name}主线内是否继续强于大盘；不在主线内、也没有板块共振的新首板不做。",
        )

    return {
        "state_label": state_label,
        "strategy_label": str((strategy or {}).get("label") or ""),
        "primary": primary_items,
        "secondary": secondary_items,
        "avoid": avoid_items,
        "focus_text": focus_text,
        "secondary_text": secondary_text,
        "avoid_text": avoid_text,
        "execution_rules": execution_rules,
        "reason": reason,
        "summary": f"行情打法建议：{state_text} → {reason}" if reason else f"行情打法建议：{state_text}",
    }


def resolve_market_focus_advice(prediction: Mapping[str, Any]) -> Dict[str, Any]:
    """Use precomputed advice when present, otherwise build it from prediction."""
    if not isinstance(prediction, Mapping):
        return {}
    advice = prediction.get("market_focus_advice")
    if isinstance(advice, dict) and advice:
        return advice
    ctx = prediction.get("compare_context") or {}
    if isinstance(ctx, Mapping):
        advice = ctx.get("market_focus_advice")
        if isinstance(advice, dict) and advice:
            return advice
    return build_market_focus_advice(ctx, prediction_category_counts(prediction))


def format_market_focus_advice_lines(advice: Mapping[str, Any]) -> List[str]:
    """Format focus advice as short display lines for summary, GUI and Excel."""
    if not isinstance(advice, Mapping) or not advice:
        return []
    lines = [str(advice.get("summary") or "").strip()]
    focus_text = str(advice.get("focus_text") or "").strip()
    secondary_text = str(advice.get("secondary_text") or "").strip()
    avoid_text = str(advice.get("avoid_text") or "").strip()
    if focus_text:
        lines.append(f"今日重点池：{focus_text}")
    execution_rules = advice.get("execution_rules") or []
    if isinstance(execution_rules, Sequence) and not isinstance(execution_rules, (str, bytes)):
        lines.extend(str(rule).strip() for rule in execution_rules if str(rule).strip())
    if secondary_text:
        lines.append(f"备选观察：{secondary_text}")
    if avoid_text:
        lines.append(f"谨慎/回避池：{avoid_text}")
    return [line for line in lines if line]
