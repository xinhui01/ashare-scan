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

SEMICONDUCTOR_THEME_NAME = "芯片/半导体"
SEMICONDUCTOR_BOARD_NAMES = ("半导体", "电子化学品")
SEMICONDUCTOR_TOPIC_KEYWORDS = (
    "半导体",
    "芯片",
    "集成电路",
    "先进封装",
    "封装",
    "存储",
    "光刻",
    "晶圆",
    "硅片",
)

PREDICTION_CATEGORY_KEYS: Dict[str, str] = {
    "cont": "continuation_candidates",
    "first": "first_board_candidates",
    "fresh": "fresh_first_board_candidates",
    "wrap": "broken_board_wrap_candidates",
    "trend": "trend_limit_up_candidates",
}

THEME_SOURCE_PRIORITY = {
    "概念": 0,
    "LLM题材": 1,
    "题材": 1,
}

THEME_PHASE_PRIORITY = {
    "主升": 0,
    "萌芽": 1,
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
        "primary": [],
        "secondary": ["wrap"],
        "avoid": ["cont", "first", "fresh", "trend"],
        "reason": "退潮日以防守为先，原则不操作；不追高、不低吸、不做新方向试错，等情绪修复再出手。",
        "wait_text": "空仓观望",
        "no_trade": True,
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


def _retreat_stage_info(compare_context: Mapping[str, Any]) -> Dict[str, Any]:
    stage = compare_context.get("market_retreat_stage")
    if not isinstance(stage, Mapping):
        market_state = compare_context.get("market_state") or {}
        if isinstance(market_state, Mapping):
            stage = market_state.get("retreat_stage")
    return dict(stage) if isinstance(stage, Mapping) else {}


def _retreat_stage_label(
    compare_context: Mapping[str, Any],
    *,
    default: str = "退潮",
) -> str:
    stage = _retreat_stage_info(compare_context)
    return str(stage.get("label") or default).strip() or default


def _retreat_stage_allows_wrap(compare_context: Mapping[str, Any]) -> bool:
    return bool(_retreat_stage_info(compare_context).get("allow_wrap"))


def _source_priority(source: Any) -> int:
    return THEME_SOURCE_PRIORITY.get(str(source or "").strip(), 9)


def _phase_priority(phase: Any) -> int:
    return THEME_PHASE_PRIORITY.get(str(phase or "").strip(), 9)


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _iter_topic_names(compare_context: Mapping[str, Any]) -> Iterable[str]:
    for raw in compare_context.get("concept_hype_topics") or []:
        if isinstance(raw, Mapping):
            name = str(raw.get("name") or raw.get("theme") or "").strip()
        else:
            name = str(raw or "").strip()
        if name:
            yield name


def _infer_local_theme(compare_context: Mapping[str, Any]) -> Dict[str, Any]:
    board_strength = compare_context.get("board_strength") or {}
    if not isinstance(board_strength, Mapping):
        board_strength = {}

    board_hits: List[Dict[str, Any]] = []
    for name in SEMICONDUCTOR_BOARD_NAMES:
        value = _safe_float(board_strength.get(name))
        if value is not None and value >= 3.0:
            board_hits.append({"name": name, "value": value})

    topic_hits = [
        name for name in _iter_topic_names(compare_context)
        if any(keyword in name for keyword in SEMICONDUCTOR_TOPIC_KEYWORDS)
    ]

    # Avoid translating broad electronics buckets into "芯片" without narrow
    # semiconductor evidence from both a board and a theme/topic source.
    if not board_hits or not topic_hits:
        return {}

    board_text = "、".join(f"{hit['name']}(+{hit['value']:.1f}%)" for hit in board_hits[:3])
    topic_text = "、".join(topic_hits[:3])
    return {
        "name": SEMICONDUCTOR_THEME_NAME,
        "board_evidence": board_hits,
        "topic_evidence": topic_hits[:5],
        "reason": f"{board_text}，概念证据：{topic_text}",
    }


def _theme_item(raw: Mapping[str, Any]) -> Dict[str, Any]:
    name = str(raw.get("name") or raw.get("theme") or "").strip()
    source = str(raw.get("source") or "").strip() or "题材"
    phase = str(raw.get("phase") or "").strip()
    trend = str(raw.get("trend") or "").strip()
    return {
        "name": name,
        "source": source,
        "phase": phase,
        "trend": trend,
        "today_count": _safe_count(raw.get("today_count")),
        "candidate_count": _safe_count(raw.get("candidate_count")),
        "opportunity_score": _safe_count(raw.get("opportunity_score")),
    }


def _usable_next_theme(item: Mapping[str, Any]) -> bool:
    name = str(item.get("name") or "").strip()
    source = str(item.get("source") or "").strip()
    phase = str(item.get("phase") or "").strip()
    trend = str(item.get("trend") or "").strip()
    if not name or source == "行业":
        return False
    if trend == "declining" or phase in {"末期", "退潮"}:
        return False
    if phase and phase not in THEME_PHASE_PRIORITY:
        return False
    return True


def _format_theme_item(item: Mapping[str, Any]) -> str:
    name = str(item.get("name") or "").strip()
    phase = str(item.get("phase") or "").strip()
    today_count = _safe_count(item.get("today_count"))
    candidate_count = _safe_count(item.get("candidate_count"))
    details: List[str] = []
    if phase:
        details.append(phase)
    if today_count:
        details.append(f"今{today_count}只")
    elif candidate_count:
        details.append(f"候选{candidate_count}只")
    if not details:
        return name
    return f"{name}({'，'.join(details)})"


def _next_theme_text(
    compare_context: Mapping[str, Any],
    *,
    state_label: str,
    local_theme: Mapping[str, Any] | None = None,
    theme_prediction: Mapping[str, Any] | None = None,
) -> str:
    if state_label == "退潮日":
        stage_label = _retreat_stage_label(compare_context)
        if not _retreat_stage_allows_wrap(compare_context):
            return f"{stage_label}观望，不新增题材操作"
    if state_label == "冰点日":
        return "冰点观望，不新增题材操作"

    candidates: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def add(raw: Mapping[str, Any]) -> None:
        item = _theme_item(raw)
        name = str(item.get("name") or "").strip()
        if not name or name in seen or not _usable_next_theme(item):
            return
        candidates.append(item)
        seen.add(name)

    if isinstance(local_theme, Mapping) and local_theme.get("name"):
        add({
            "name": local_theme.get("name"),
            "source": "题材",
            "phase": "主升",
        })

    if isinstance(theme_prediction, Mapping):
        for group in theme_prediction.get("groups") or []:
            if isinstance(group, Mapping):
                add(group)

    strong_line = compare_context.get("strong_main_line") or {}
    if isinstance(strong_line, Mapping):
        add(strong_line)

    for raw in compare_context.get("concept_hype_topics") or []:
        if isinstance(raw, Mapping):
            add(raw)

    if not candidates:
        if state_label == "退潮日":
            stage_label = _retreat_stage_label(compare_context)
            return f"{stage_label}只看最先共振的近期主线，未确认不操作"
        return "暂无明确细题材，等次日板块共振确认"

    candidates.sort(
        key=lambda item: (
            _source_priority(item.get("source")),
            _phase_priority(item.get("phase")),
            -_safe_count(item.get("opportunity_score")),
            -_safe_count(item.get("candidate_count")),
            -_safe_count(item.get("today_count")),
            str(item.get("name") or ""),
        )
    )
    theme_text = "、".join(_format_theme_item(item) for item in candidates[:2])
    if state_label == "退潮日":
        stage_label = _retreat_stage_label(compare_context)
        return f"{stage_label}只看确认型反包：{theme_text}"
    return theme_text


def _execution_rules(compare_context: Mapping[str, Any], primary: Sequence[str]) -> List[str]:
    state_label = str(compare_context.get("market_state_label") or "").strip()
    if state_label == "退潮日":
        if _retreat_stage_allows_wrap(compare_context):
            stage_label = _retreat_stage_label(compare_context)
            return [
                f"执行规则：{stage_label}只做确认型反包；必须等竞价/开盘确认、题材共振和个股主动转强，不确认就空仓。"
            ]
        stage_label = _retreat_stage_label(compare_context, default="退潮日")
        return [
            f"执行规则：{stage_label}不操作；不追高、不低吸、不做反包、不做预测型试错，等情绪修复后再重新选择题材。"
        ]
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
    theme_prediction: Mapping[str, Any] | None = None,
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
        no_trade = False
    else:
        primary = _unique_categories(spec.get("primary") or [])
        secondary = _unique_categories(spec.get("secondary") or [])
        avoid = _unique_categories(spec.get("avoid") or [])
        reason = str(spec.get("reason") or "")
        wait_text = str(spec.get("wait_text") or "")
        no_trade = bool(spec.get("no_trade"))

    retreat_stage = _retreat_stage_info(compare_context)
    if state_label == "退潮日":
        stage_label = str(retreat_stage.get("label") or "退潮").strip()
        if _retreat_stage_allows_wrap(compare_context):
            primary = []
            secondary = ["wrap"]
            avoid = ["cont", "first", "fresh", "trend"]
            reason = (
                f"{stage_label}不是全面进攻，只允许确认型反包；"
                "没有竞价/开盘转强和题材共振就继续空仓。"
            )
            wait_text = "空仓等确认"
            no_trade = False
        else:
            primary = []
            secondary = ["wrap"]
            avoid = ["cont", "first", "fresh", "trend"]
            reason = (
                f"{stage_label}以防守为先，原则不操作；"
                "反包只作复盘观察，不作为买入建议。"
            )
            wait_text = "空仓观望"
            no_trade = True

    strong_line = _qualified_strong_main_line(compare_context)
    local_theme = _infer_local_theme(compare_context)
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
    if state_label == "退潮日" and _retreat_stage_allows_wrap(compare_context):
        focus_text = wait_text or "空仓等确认"
        if secondary_text:
            focus_text = f"{focus_text}；确认型{secondary_text}（需竞价/开盘确认）"
    elif no_trade and wait_text:
        focus_text = wait_text
    elif not primary_text and wait_text:
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
        line_name = str((local_theme or {}).get("name") or strong_line.get("name") or "").strip()
        execution_rules.insert(
            0,
            f"执行规则：先看{line_name}主线内是否继续强于大盘；不在主线内、也没有板块共振的新首板不做。",
        )
    if local_theme:
        if no_trade:
            execution_rules.insert(
                0,
                f"执行规则：{local_theme['name']}仅作观察，不作为买入方向；等市场情绪修复后再重新评估。",
            )
        else:
            execution_rules.insert(
                0,
                f"执行规则：{local_theme['name']}是推断的局部强方向，必须同时看板块强度、题材证据和个股梯队，不能只按粗行业名交易。",
            )

    summary = f"行情打法建议：{state_text} → {reason}" if reason else f"行情打法建议：{state_text}"
    if local_theme:
        if no_trade:
            summary += f" 局部强方向：{local_theme['name']}（{local_theme['reason']}），仅作观察。"
        else:
            summary += f" 局部强方向：{local_theme['name']}（{local_theme['reason']}）。"
    next_theme_text = _next_theme_text(
        compare_context,
        state_label=state_label,
        local_theme=local_theme,
        theme_prediction=theme_prediction,
    )

    return {
        "state_label": state_label,
        "strategy_label": str((strategy or {}).get("label") or ""),
        "retreat_stage": retreat_stage,
        "local_theme": local_theme,
        "no_trade": no_trade,
        "primary": primary_items,
        "secondary": secondary_items,
        "avoid": avoid_items,
        "focus_text": focus_text,
        "secondary_text": secondary_text,
        "avoid_text": avoid_text,
        "next_theme_text": next_theme_text,
        "execution_rules": execution_rules,
        "reason": reason,
        "summary": summary,
    }


def resolve_market_focus_advice(prediction: Mapping[str, Any]) -> Dict[str, Any]:
    """Use precomputed advice when present, otherwise build it from prediction."""
    if not isinstance(prediction, Mapping):
        return {}
    legacy_advice: Dict[str, Any] = {}
    advice = prediction.get("market_focus_advice")
    if isinstance(advice, dict) and advice:
        if advice.get("next_theme_text"):
            return advice
        legacy_advice = advice
    ctx = prediction.get("compare_context") or {}
    if isinstance(ctx, Mapping):
        advice = ctx.get("market_focus_advice")
        if isinstance(advice, dict) and advice:
            if advice.get("next_theme_text"):
                return advice
            legacy_advice = legacy_advice or advice
    rebuilt = build_market_focus_advice(
        ctx,
        prediction_category_counts(prediction),
        prediction.get("theme_prediction") or {},
    )
    return rebuilt or legacy_advice


def format_market_focus_advice_lines(advice: Mapping[str, Any]) -> List[str]:
    """Format focus advice as short display lines for summary, GUI and Excel."""
    if not isinstance(advice, Mapping) or not advice:
        return []
    lines = [str(advice.get("summary") or "").strip()]
    focus_text = str(advice.get("focus_text") or "").strip()
    secondary_text = str(advice.get("secondary_text") or "").strip()
    avoid_text = str(advice.get("avoid_text") or "").strip()
    next_theme_text = str(advice.get("next_theme_text") or "").strip()
    no_trade = bool(advice.get("no_trade"))
    if next_theme_text:
        lines.append(f"明日题材方向：{next_theme_text}")
    if focus_text:
        lines.append(f"今日重点池：{focus_text}")
    execution_rules = advice.get("execution_rules") or []
    if isinstance(execution_rules, Sequence) and not isinstance(execution_rules, (str, bytes)):
        lines.extend(str(rule).strip() for rule in execution_rules if str(rule).strip())
    if secondary_text:
        if no_trade:
            lines.append(f"仅观察池：{secondary_text}（不作为买入建议）")
        else:
            lines.append(f"备选观察：{secondary_text}")
    if avoid_text:
        lines.append(f"谨慎/回避池：{avoid_text}")
    return [line for line in lines if line]
