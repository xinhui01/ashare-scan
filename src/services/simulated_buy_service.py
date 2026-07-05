"""Simulated buy selection and result statistics for prediction candidates."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Tuple


CATEGORY_CONFIG: Tuple[Tuple[str, str, str], ...] = (
    ("cont", "保留涨停", "continuation_candidates"),
    ("first", "二波接力", "first_board_candidates"),
    ("fresh", "首板涨停", "fresh_first_board_candidates"),
    ("wrap", "反包", "broken_board_wrap_candidates"),
    ("trend", "趋势涨停", "trend_limit_up_candidates"),
)


def _number(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _score_value(rec: Mapping[str, Any]) -> float:
    if rec.get("calibrated_score") is not None:
        return _number(rec.get("calibrated_score"))
    return _number(rec.get("score"))


def _relative_strength_value(rec: Mapping[str, Any]) -> float:
    for key in ("relative_strength", "relative_strength_score", "rs_score"):
        if rec.get(key) is not None:
            return _number(rec.get(key))
    return 0.0


def _confirmation_rank(rec: Mapping[str, Any]) -> int:
    confirmation = rec.get("opening_confirmation") or {}
    status = str(confirmation.get("status") or "").strip()
    return {
        "可买": 4,
        "观察": 3,
        "放弃": 1,
        "风险过高": 0,
    }.get(status, 2)


def _buy_status(rec: Mapping[str, Any]) -> str:
    confirmation = rec.get("opening_confirmation") or {}
    return str(confirmation.get("status") or "待确认").strip() or "待确认"


def _in_bucket(score: float, bucket: Optional[Tuple[int, int]]) -> bool:
    if bucket is None:
        return False
    lo, hi = bucket
    return float(lo) <= score <= float(hi)


def _rank_tuple(
    rec: Mapping[str, Any],
    category: str,
    best_buckets: Mapping[str, Optional[Tuple[int, int]]],
) -> Tuple[float, float, float, float, float, str]:
    score = _score_value(rec)
    return (
        float(_confirmation_rank(rec)),
        1.0 if _in_bucket(score, best_buckets.get(category)) else 0.0,
        score,
        _number(rec.get("accumulation_score")),
        _relative_strength_value(rec),
        str(rec.get("code") or "").zfill(6),
    )


def build_simulated_buy_picks(
    prediction_result: Mapping[str, Any],
    *,
    best_buckets: Optional[Mapping[str, Optional[Tuple[int, int]]]] = None,
    limit: int = 2,
) -> List[Dict[str, Any]]:
    """Return the top unique simulated buys across all prediction categories."""
    best_buckets = best_buckets or {}
    candidates: List[Tuple[Tuple[float, float, float, float, float, str], Dict[str, Any]]] = []
    trade_date = str(prediction_result.get("trade_date") or "").strip()
    for category, category_label, payload_key in CATEGORY_CONFIG:
        for rec in prediction_result.get(payload_key) or []:
            if not isinstance(rec, Mapping):
                continue
            code = str(rec.get("code") or "").strip().zfill(6)
            if not code or code == "000000":
                continue
            score = _score_value(rec)
            pick = {
                "trade_date": trade_date,
                "code": code,
                "name": str(rec.get("name") or ""),
                "industry": str(rec.get("industry") or ""),
                "theme": str(rec.get("theme") or ""),
                "category": category,
                "category_label": category_label,
                "score": score,
                "buy_status": _buy_status(rec),
                "reasons": str(rec.get("reasons") or ""),
            }
            candidates.append((_rank_tuple(rec, category, best_buckets), pick))

    selected: List[Dict[str, Any]] = []
    seen_codes = set()
    for _, pick in sorted(candidates, key=lambda item: item[0], reverse=True):
        code = pick["code"]
        if code in seen_codes:
            continue
        selected.append(dict(pick))
        seen_codes.add(code)
        if len(selected) >= max(0, int(limit)):
            break
    return selected


def _is_hit(category: str, row: Mapping[str, Any]) -> bool:
    if not int(row.get("hit_buyable") or 0):
        return False
    if category == "cont" or category.startswith("cont_"):
        return bool(int(row.get("hit_strict") or 0))
    return bool(int(row.get("hit_loose") or 0))


def _profit_pct(row: Mapping[str, Any]) -> Optional[float]:
    pct = row.get("t1_open_close_pct")
    if pct is None:
        pct = row.get("t1_pct")
    if pct is None:
        return None
    return _number(pct)


def summarize_simulated_buy_picks(
    picks: List[Mapping[str, Any]],
    results_map: Mapping[Tuple[str, str], Mapping[str, Any]],
) -> Dict[str, Any]:
    """Attach T+1 evaluation info and calculate win/profit stats."""
    enriched: List[Dict[str, Any]] = []
    evaluated = 0
    wins = 0
    total_profit = 0.0

    for pick in picks or []:
        item = dict(pick)
        code = str(item.get("code") or "").zfill(6)
        category = str(item.get("category") or "")
        row = results_map.get((code, category))
        if row:
            hit = _is_hit(category, row)
            profit = _profit_pct(row)
            if profit is not None:
                evaluated += 1
                total_profit += profit
            if hit:
                wins += 1
            item["result_text"] = "命中" if hit else "未中"
            item["profit_pct"] = profit
            item["verify_date"] = str(row.get("verify_date") or "")
        else:
            item["result_text"] = "待回填"
            item["profit_pct"] = None
            item["verify_date"] = ""
        enriched.append(item)

    win_rate = (wins / evaluated * 100.0) if evaluated else 0.0
    avg_profit = (total_profit / evaluated) if evaluated else 0.0
    return {
        "picks": enriched,
        "total": len(enriched),
        "evaluated": evaluated,
        "wins": wins,
        "win_rate": win_rate,
        "total_profit_pct": total_profit,
        "avg_profit_pct": avg_profit,
    }
