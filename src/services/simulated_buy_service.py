"""Simulated buy selection and result statistics for prediction candidates."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import pandas as pd


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


def build_trade_snapshot(pick: Mapping[str, Any]) -> Dict[str, Any]:
    """Convert a selected pick into an immutable pending trade snapshot."""
    return {
        "prediction_date": str(pick.get("trade_date") or "").strip(),
        "trade_date": "",
        "code": str(pick.get("code") or "").strip().zfill(6),
        "name": str(pick.get("name") or ""),
        "industry": str(pick.get("industry") or ""),
        "theme": str(pick.get("theme") or ""),
        "category": str(pick.get("category") or ""),
        "category_label": str(pick.get("category_label") or ""),
        "score": _score_value(pick),
        "buy_status": str(pick.get("buy_status") or ""),
        "reasons": str(pick.get("reasons") or ""),
        "buy_price": None,
        "sell_price": None,
        "profit_pct": None,
        "is_buyable": 0,
        "is_hit": 0,
        "trade_status": "pending",
        "unavailable_reason": "",
    }


def apply_accuracy_result(
    trade: Mapping[str, Any],
    result: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Return a trade copy with its T+1 execution result attached."""
    item = dict(trade)
    if not result:
        return item
    item["trade_date"] = str(result.get("verify_date") or "").strip()
    item["buy_price"] = result.get("t1_open")
    item["sell_price"] = result.get("t1_close")
    item["profit_pct"] = result.get("t1_open_close_pct")
    item["is_buyable"] = int(bool(result.get("hit_buyable")))
    item["is_hit"] = int(_is_hit(str(item.get("category") or ""), result))
    if result.get("t1_one_word"):
        item["trade_status"] = "one_word"
        item["unavailable_reason"] = "一字板不可买"
    elif result.get("t1_suspended"):
        item["trade_status"] = "suspended"
        item["unavailable_reason"] = "停牌"
    elif item["buy_price"] is None or item["sell_price"] is None:
        item["trade_status"] = "missing_price"
        item["unavailable_reason"] = "开盘价或收盘价缺失"
    elif not item["is_buyable"]:
        item["trade_status"] = "unbuyable"
        item["unavailable_reason"] = "不可买"
    else:
        item["trade_status"] = "completed"
        item["unavailable_reason"] = ""
    return item


def build_account_curve(
    trades: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Build an equal-weight daily, continuously compounded equity curve."""
    daily_returns: Dict[str, List[float]] = {}
    for trade in trades or []:
        if str(trade.get("trade_status") or "") != "completed":
            continue
        if not int(trade.get("is_buyable") or 0):
            continue
        trade_date = str(trade.get("trade_date") or "").strip()
        profit = trade.get("profit_pct")
        if not trade_date or profit is None:
            continue
        try:
            daily_returns.setdefault(trade_date, []).append(float(profit))
        except (TypeError, ValueError):
            continue

    equity = 1.0
    curve: List[Dict[str, Any]] = []
    for trade_date in sorted(daily_returns):
        values = daily_returns[trade_date]
        daily_return_pct = sum(values) / len(values)
        equity *= 1.0 + daily_return_pct / 100.0
        curve.append({
            "trade_date": trade_date,
            "daily_return_pct": daily_return_pct,
            "equity": equity,
            "cumulative_return_pct": (equity - 1.0) * 100.0,
        })
    return curve


def build_intraday_return_curve(
    intraday_df,
    *,
    buy_price: float,
) -> Dict[str, List[Any]]:
    """Convert intraday prices to percentage returns from the opening buy."""
    if intraday_df is None or getattr(intraday_df, "empty", True):
        return {"times": [], "returns_pct": []}
    try:
        normalized_buy_price = float(buy_price)
    except (TypeError, ValueError):
        return {"times": [], "returns_pct": []}
    if normalized_buy_price <= 0:
        return {"times": [], "returns_pct": []}

    time_col = next((c for c in ("time", "datetime") if c in intraday_df.columns), None)
    price_col = next((c for c in ("price", "close") if c in intraday_df.columns), None)
    if not time_col or not price_col:
        return {"times": [], "returns_pct": []}
    frame = intraday_df[[time_col, price_col]].copy()
    frame[price_col] = pd.to_numeric(frame[price_col], errors="coerce")
    frame = frame.dropna(subset=[price_col]).sort_values(time_col)
    if frame.empty:
        return {"times": [], "returns_pct": []}
    returns = (frame[price_col] / normalized_buy_price - 1.0) * 100.0
    return {
        "times": frame[time_col].astype(str).tolist(),
        "returns_pct": returns.astype(float).tolist(),
    }


def sync_simulated_buy_history(
    prediction_results: Iterable[Mapping[str, Any]],
    results_maps_by_date: Mapping[str, Mapping[Tuple[str, str], Mapping[str, Any]]],
    *,
    save_trades_fn,
    update_result_fn,
    limit: int = 2,
) -> Dict[str, int]:
    """Idempotently backfill saved predictions and update their T+1 results."""
    inserted = 0
    updated = 0
    for prediction in prediction_results or []:
        if not isinstance(prediction, Mapping):
            continue
        picks = build_simulated_buy_picks(prediction, limit=limit)
        snapshots = [build_trade_snapshot(pick) for pick in picks]
        inserted += int(save_trades_fn(snapshots) or 0)
        prediction_date = str(prediction.get("trade_date") or "").strip()
        result_map = results_maps_by_date.get(prediction_date, {})
        for snapshot in snapshots:
            result = result_map.get((snapshot["code"], snapshot["category"]))
            if not result:
                continue
            evaluated = apply_accuracy_result(snapshot, result)
            changed = update_result_fn(
                snapshot["prediction_date"],
                snapshot["code"],
                trade_date=evaluated["trade_date"],
                buy_price=evaluated["buy_price"],
                sell_price=evaluated["sell_price"],
                profit_pct=evaluated["profit_pct"],
                is_buyable=bool(evaluated["is_buyable"]),
                is_hit=bool(evaluated["is_hit"]),
                trade_status=evaluated["trade_status"],
                unavailable_reason=evaluated["unavailable_reason"],
            )
            updated += int(bool(changed))
    return {"inserted": inserted, "updated": updated}


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


def summarize_historical_simulated_buy_picks(
    prediction_results: Iterable[Mapping[str, Any]],
    results_maps_by_date: Mapping[str, Mapping[Tuple[str, str], Mapping[str, Any]]],
    *,
    limit: int = 2,
) -> Dict[str, Any]:
    """Rebuild daily simulated buys from saved predictions and total their results."""
    total = 0
    evaluated = 0
    wins = 0
    total_profit = 0.0
    pending = 0

    for prediction in prediction_results or []:
        if not isinstance(prediction, Mapping):
            continue
        trade_date = str(prediction.get("trade_date") or "").strip()
        picks = build_simulated_buy_picks(prediction, limit=limit)
        if not picks:
            continue
        daily_summary = summarize_simulated_buy_picks(
            picks,
            results_maps_by_date.get(trade_date, {}),
        )
        daily_total = int(daily_summary.get("total") or 0)
        daily_evaluated = int(daily_summary.get("evaluated") or 0)
        total += daily_total
        evaluated += daily_evaluated
        wins += int(daily_summary.get("wins") or 0)
        total_profit += float(daily_summary.get("total_profit_pct") or 0.0)
        pending += max(0, daily_total - daily_evaluated)

    win_rate = (wins / evaluated * 100.0) if evaluated else 0.0
    avg_profit = (total_profit / evaluated) if evaluated else 0.0
    return {
        "total": total,
        "evaluated": evaluated,
        "pending": pending,
        "wins": wins,
        "win_rate": win_rate,
        "total_profit_pct": total_profit,
        "avg_profit_pct": avg_profit,
    }
