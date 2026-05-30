"""Historical hit-rate calibration for fresh first-board candidates."""
from __future__ import annotations

from itertools import combinations
from typing import Any, Dict, Iterable, List, Optional, Tuple

import stock_store


FRESH_CALIBRATION_FEATURES: Tuple[str, ...] = (
    "温和启动",
    "突破",
    "放量上攻",
    "高位滞涨",
    "爆量",
    "放量",
    "温和放量",
    "多头排列",
    "站上MA5/10",
    "低位",
    "中位",
    "过高",
    "热门板块",
    "题材族群",
    "昨日晋级率",
    "僵尸股",
    "曾涨停",
    "股性活跃",
)

RuleKey = Tuple[str, ...]
RuleStats = Dict[str, Any]


def extract_fresh_features(reasons: Any) -> Tuple[str, ...]:
    text = str(reasons or "")
    found: List[str] = []
    for feature in FRESH_CALIBRATION_FEATURES:
        if feature in text:
            found.append(feature)
    return tuple(found)


def build_fresh_calibration_rules(
    rows: Iterable[Dict[str, Any]],
    *,
    min_samples: int = 20,
    max_combo_size: int = 3,
    success_field: str = "hit_strict",
) -> Dict[RuleKey, RuleStats]:
    counters: Dict[RuleKey, Dict[str, int]] = {}
    max_size = max(1, int(max_combo_size or 1))
    sample_floor = max(1, int(min_samples or 1))

    for row in rows:
        if int(row.get("hit_buyable") or 0) != 1:
            continue
        features = extract_fresh_features(row.get("reasons"))
        if not features:
            continue
        hit = 1 if int(row.get(success_field) or 0) else 0
        for size in range(1, min(max_size, len(features)) + 1):
            for combo in combinations(features, size):
                stat = counters.setdefault(combo, {"buyable": 0, "hit": 0})
                stat["buyable"] += 1
                stat["hit"] += hit

    rules: Dict[RuleKey, RuleStats] = {}
    for combo, stat in counters.items():
        buyable = int(stat["buyable"])
        if buyable < sample_floor:
            continue
        hit = int(stat["hit"])
        rules[combo] = {
            "buyable": buyable,
            "hit": hit,
            "rate": round(hit / buyable * 100.0, 1),
        }
    return rules


def load_fresh_calibration_rules(
    *,
    lookback_dates: int = 20,
    min_samples: int = 20,
    success_field: str = "hit_strict",
) -> Dict[RuleKey, RuleStats]:
    dates = stock_store.list_prediction_accuracy_dates()
    recent_dates = dates[: max(1, int(lookback_dates or 1))]
    rows: List[Dict[str, Any]] = []
    for trade_date in recent_dates:
        for row in stock_store.load_prediction_accuracy_by_date(trade_date):
            if str(row.get("category") or "") == "fresh":
                rows.append(dict(row))
    return build_fresh_calibration_rules(
        rows, min_samples=min_samples, success_field=success_field,
    )


def best_fresh_calibration_rule(
    reasons: Any,
    rules: Dict[RuleKey, RuleStats],
    *,
    min_samples: int = 20,
) -> Optional[Tuple[RuleKey, RuleStats]]:
    features = set(extract_fresh_features(reasons))
    if not features:
        return None
    sample_floor = max(1, int(min_samples or 1))
    matches: List[Tuple[RuleKey, RuleStats]] = []
    for combo, stat in rules.items():
        if int(stat.get("buyable") or 0) < sample_floor:
            continue
        if all(item in features for item in combo):
            matches.append((combo, stat))
    if not matches:
        return None
    return sorted(
        matches,
        key=lambda item: (
            float(item[1].get("rate") or 0.0),
            len(item[0]),
            int(item[1].get("buyable") or 0),
        ),
        reverse=True,
    )[0]


def calibrate_fresh_candidate(
    candidate: Dict[str, Any],
    rules: Dict[RuleKey, RuleStats],
    *,
    min_samples: int = 20,
    high_confidence_rate: float = 10.0,
) -> Dict[str, Any]:
    out = dict(candidate)
    best = best_fresh_calibration_rule(
        out.get("reasons"), rules, min_samples=min_samples,
    )
    base_score = int(out.get("score") or 0)
    if best is None:
        out.update({
            "calibrated_hit_rate": None,
            "calibrated_sample_size": 0,
            "calibrated_rule": "",
            "confidence": "观察",
            "calibrated_score": base_score,
        })
        return out

    combo, stat = best
    rate = float(stat.get("rate") or 0.0)
    sample_size = int(stat.get("buyable") or 0)
    bonus = max(-10, min(20, round((rate - 14.0) * 1.5)))
    out.update({
        "calibrated_hit_rate": round(rate, 1),
        "calibrated_sample_size": sample_size,
        "calibrated_rule": "+".join(combo),
        "confidence": "涨停高置信" if rate >= float(high_confidence_rate) else "涨停观察",
        "calibrated_score": max(0, min(100, base_score + int(bonus))),
    })
    return out
