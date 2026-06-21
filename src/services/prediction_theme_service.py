"""Theme-first grouping for limit-up prediction candidates.

The prediction engine still produces candidates by shape (continuation, wrap,
fresh first board, etc.).  This service turns those shape buckets into the
view the user actually wants: theme first, role second.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROLE_ORDER = ["core", "relay", "repair", "replenish", "watch"]
ROLE_LABELS = {
    "core": "连板核心",
    "relay": "二波接力",
    "repair": "反包修复",
    "replenish": "首板补涨",
    "watch": "趋势观察",
}

CATEGORY_SPECS = [
    ("continuation_candidates", "cont", "core"),
    ("first_board_candidates", "first", "relay"),
    ("broken_board_wrap_candidates", "wrap", "repair"),
    ("fresh_first_board_candidates", "fresh", "replenish"),
    ("trend_limit_up_candidates", "trend", "watch"),
]

SOURCE_PRIORITY = {
    "概念": 0,
    "LLM题材": 1,
    "行业": 2,
}


def _source_priority(source: Any) -> int:
    return SOURCE_PRIORITY.get(str(source or "").strip(), 9)


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return digits.zfill(6) if digits else ""


def _score_of(record: Dict[str, Any]) -> float:
    for key in ("calibrated_score", "score", "total_score", "final_score"):
        try:
            value = record.get(key)
            if value is not None:
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _empty_roles() -> Dict[str, List[Dict[str, Any]]]:
    return {role: [] for role in ROLE_ORDER}


def _empty_counts() -> Dict[str, int]:
    return {role: 0 for role in ROLE_ORDER}


def _concept_sort_key(concept: Dict[str, Any]) -> Tuple[int, int, int, int]:
    return (
        _source_priority(concept.get("source")),
        -int(concept.get("opportunity_score") or 0),
        -int(concept.get("today_count") or 0),
        -int(concept.get("total_limit_ups") or concept.get("member_count") or 0),
    )


def _better_concept(candidate: Dict[str, Any], current: Optional[Dict[str, Any]]) -> bool:
    if current is None:
        return True
    return _concept_sort_key(candidate) < _concept_sort_key(current)


def _iter_prediction_records(prediction: Dict[str, Any]) -> Iterable[Tuple[str, str, Dict[str, Any]]]:
    for key, category, role in CATEGORY_SPECS:
        for rec in prediction.get(key) or []:
            if isinstance(rec, dict):
                yield category, role, rec


def _build_theme_indexes(
    hype_result: Optional[Dict[str, Any]],
    compare_context: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, str]]:
    concepts = list((hype_result or {}).get("concepts") or [])
    compare_context = compare_context or {}

    concept_meta: Dict[str, Dict[str, Any]] = {}
    code_to_theme: Dict[str, str] = {}
    industry_to_theme: Dict[str, str] = {}

    for raw in concepts:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        if not name:
            continue
        source = str(raw.get("source") or "").strip() or "题材"
        if source == "行业":
            continue
        meta = {
            "name": name,
            "source": source,
            "phase": str(raw.get("phase") or "").strip(),
            "trend": str(raw.get("trend") or "").strip(),
            "opportunity_score": int(raw.get("opportunity_score") or 0),
            "today_count": int(raw.get("today_count") or 0),
            "total_limit_ups": int(raw.get("total_limit_ups") or 0),
            "member_count": int(raw.get("member_count") or 0),
        }
        if _better_concept(meta, concept_meta.get(name)):
            concept_meta[name] = meta

    theme_size_map = compare_context.get("theme_size_map") or {}
    for name, size in theme_size_map.items():
        theme_name = str(name or "").strip()
        if not theme_name or theme_name not in concept_meta:
            continue
        meta = concept_meta[theme_name]
        try:
            meta["member_count"] = max(int(meta.get("member_count") or 0), int(size or 0))
        except (TypeError, ValueError):
            pass

    for code, name in (compare_context.get("code_theme_map") or {}).items():
        c = _normalize_code(code)
        theme_name = str(name or "").strip()
        if c and theme_name in concept_meta:
            code_to_theme[c] = theme_name

    for raw in concepts:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name") or "").strip()
        meta = concept_meta.get(name)
        if not name or not meta:
            continue
        for member in raw.get("members") or []:
            if not isinstance(member, dict):
                continue
            code = _normalize_code(member.get("code"))
            if not code:
                continue
            current_name = code_to_theme.get(code)
            current_meta = concept_meta.get(current_name or "")
            if current_name is None or _better_concept(meta, current_meta):
                code_to_theme[code] = name
        for item in raw.get("related_industries") or []:
            if not isinstance(item, dict):
                continue
            industry = str(item.get("name") or "").strip()
            if not industry:
                continue
            current_name = industry_to_theme.get(industry)
            current_meta = concept_meta.get(current_name or "")
            if current_name is None or _better_concept(meta, current_meta):
                industry_to_theme[industry] = name

    return concept_meta, code_to_theme, industry_to_theme


def _group_sort_score(group: Dict[str, Any]) -> int:
    counts = group.get("counts") or {}
    return (
        int(counts.get("core") or 0) * 100
        + int(counts.get("repair") or 0) * 45
        + int(counts.get("relay") or 0) * 30
        + int(counts.get("replenish") or 0) * 20
        + int(counts.get("watch") or 0) * 10
    )


def build_theme_prediction_groups(
    prediction: Dict[str, Any],
    *,
    hype_result: Optional[Dict[str, Any]] = None,
    compare_context: Optional[Dict[str, Any]] = None,
    max_per_role: int = 8,
) -> Dict[str, Any]:
    """Build theme-first candidate groups from shape-first prediction output."""
    prediction = prediction or {}
    concept_meta, code_to_theme, industry_to_theme = _build_theme_indexes(
        hype_result, compare_context,
    )

    groups_by_name: Dict[str, Dict[str, Any]] = {}
    ungrouped = {
        "name": "非主线观察",
        "source": "",
        "phase": "",
        "opportunity_score": 0,
        "roles": _empty_roles(),
        "counts": _empty_counts(),
        "candidate_count": 0,
    }
    total_candidates = 0

    for category, role, raw_rec in _iter_prediction_records(prediction):
        code = _normalize_code(raw_rec.get("code") or raw_rec.get("代码"))
        if not code:
            continue
        total_candidates += 1
        industry = str(raw_rec.get("industry") or raw_rec.get("所属行业") or "").strip()
        theme_name = code_to_theme.get(code)
        match = "个股命中"
        if not theme_name and industry:
            theme_name = industry_to_theme.get(industry)
            match = "行业关联" if theme_name else ""

        rec = deepcopy(raw_rec)
        rec["code"] = code
        rec["category"] = category
        rec["role"] = role
        rec["role_label"] = ROLE_LABELS[role]
        rec["theme_name"] = theme_name or ""
        rec["theme_match"] = match or "未命中"

        if theme_name:
            meta = concept_meta.setdefault(
                theme_name,
                {
                    "name": theme_name,
                    "source": "题材",
                    "phase": "",
                    "trend": "",
                    "opportunity_score": 0,
                    "today_count": 0,
                    "total_limit_ups": 0,
                    "member_count": 0,
                },
            )
            group = groups_by_name.setdefault(
                theme_name,
                {
                    **meta,
                    "roles": _empty_roles(),
                    "counts": _empty_counts(),
                    "candidate_count": 0,
                },
            )
        else:
            group = ungrouped

        group["roles"][role].append(rec)
        group["counts"][role] += 1
        group["candidate_count"] += 1

    for group in list(groups_by_name.values()) + [ungrouped]:
        for role in ROLE_ORDER:
            group["roles"][role].sort(
                key=lambda item: (-_score_of(item), str(item.get("code") or "")),
            )
            if max_per_role > 0:
                group["roles"][role] = group["roles"][role][:max_per_role]

    groups = sorted(
        groups_by_name.values(),
        key=lambda item: (
            _source_priority(item.get("source")),
            -int(item.get("opportunity_score") or 0),
            -_group_sort_score(item),
            -int(item.get("candidate_count") or 0),
            str(item.get("name") or ""),
        ),
    )
    grouped_candidates = sum(int(g.get("candidate_count") or 0) for g in groups)

    return {
        "groups": groups,
        "ungrouped": ungrouped,
        "role_order": list(ROLE_ORDER),
        "role_labels": dict(ROLE_LABELS),
        "stats": {
            "theme_count": len(groups),
            "total_candidates": total_candidates,
            "grouped_candidates": grouped_candidates,
            "ungrouped_candidates": int(ungrouped.get("candidate_count") or 0),
        },
    }
