from src.services.scoring.fresh_calibration import (
    build_fresh_calibration_rules,
    calibrate_fresh_candidate,
    load_fresh_calibration_rules,
)
from src.services.scoring import fresh as fresh_scoring
import pandas as pd
from unittest.mock import patch


def _row(reasons, hit=False, score=55):
    return {
        "category": "fresh",
        "predicted_score": score,
        "hit_buyable": 1,
        "hit_strict": 1 if hit else 0,
        "hit_loose": 1 if hit else 0,
        "reasons": reasons,
    }


def _loose_only_row(reasons, score=55):
    return {
        "category": "fresh",
        "predicted_score": score,
        "hit_buyable": 1,
        "hit_strict": 0,
        "hit_loose": 1,
        "reasons": reasons,
    }


def test_build_rules_finds_twenty_percent_feature_combo():
    rows = []
    for i in range(40):
        rows.append(
            _row(
                "涨7.0%放量上攻+4 / 量比2.0x放量+14 / 60日位置95%过高-10",
                hit=i < 10,
            )
        )
    for i in range(40):
        rows.append(
            _row(
                "涨4.5%突破+16 / 量比1.1x温和放量+6 / 60日位置50%中位+4",
                hit=i < 4,
            )
        )

    rules = build_fresh_calibration_rules(rows, min_samples=20)
    key = ("放量", "过高")

    assert key in rules
    assert rules[key]["buyable"] == 40
    assert rules[key]["hit"] == 10
    assert rules[key]["rate"] == 25.0


def test_build_rules_default_success_is_strict_limit_up_not_loose_profit():
    rows = [
        _loose_only_row(
            "涨7.0%放量上攻+4 / 量比2.0x放量+14 / 60日位置95%过高-10"
        )
        for _ in range(30)
    ]
    rows.extend(
        _row(
            "涨7.0%放量上攻+4 / 量比2.0x放量+14 / 60日位置95%过高-10",
            hit=True,
        )
        for _ in range(3)
    )

    rules = build_fresh_calibration_rules(rows, min_samples=20)

    assert rules[("放量", "过高")]["hit"] == 3
    assert rules[("放量", "过高")]["buyable"] == 33
    assert rules[("放量", "过高")]["rate"] == 9.1


def test_calibrate_candidate_marks_high_confidence_from_best_rule():
    rules = {
        ("放量",): {"rate": 16.0, "buyable": 50, "hit": 8},
        ("放量", "过高"): {"rate": 25.0, "buyable": 40, "hit": 10},
    }
    candidate = {
        "score": 52,
        "reasons": "涨7.0%放量上攻+4 / 量比2.0x放量+14 / 60日位置95%过高-10",
    }

    out = calibrate_fresh_candidate(candidate, rules, min_samples=20)

    assert out["calibrated_hit_rate"] == 25.0
    assert out["calibrated_sample_size"] == 40
    assert out["calibrated_rule"] == "放量+过高"
    assert out["confidence"] == "涨停高置信"
    assert out["calibrated_score"] > out["score"]


def test_scan_fresh_candidates_sorts_by_calibrated_confidence(monkeypatch):
    rec_high = {"code": "000001", "name": "高置信", "change_pct": 6.5}
    rec_plain = {"code": "000002", "name": "普通", "change_pct": 6.5}

    def fake_score(rec, *_args, **_kwargs):
        if rec["code"] == "000001":
            return {
                "code": rec["code"],
                "name": rec["name"],
                "score": 50,
                "reasons": "量比2.0x放量+14 / 60日位置95%过高-10",
            }
        return {
            "code": rec["code"],
            "name": rec["name"],
            "score": 65,
            "reasons": "涨4.5%突破+16 / 60日位置50%中位+4",
        }

    monkeypatch.setattr(fresh_scoring, "score_fresh_first_board", fake_score)

    result = fresh_scoring.scan_fresh_first_board_candidates_cached(
        spot_df=pd.DataFrame([{"代码": "000001"}]),
        zt_codes=set(),
        hot_industries={},
        compare_context={
            "fresh_calibration_rules": {
                ("放量", "过高"): {"rate": 25.0, "buyable": 40, "hit": 10}
            }
        },
        fetcher=object(),
        filter_strong_stocks_fn=lambda *_args: [rec_plain, rec_high],
    )

    assert [r["code"] for r in result] == ["000001", "000002"]
    assert result[0]["confidence"] == "涨停高置信"
    assert result[0]["calibrated_hit_rate"] == 25.0
    assert result[1]["confidence"] == "观察"


def test_load_fresh_calibration_rules_reads_recent_accuracy_rows():
    rows = [
        _row(
            "涨7.0%放量上攻+4 / 量比2.0x放量+14 / 60日位置95%过高-10",
            hit=i < 10,
        )
        for i in range(40)
    ]

    with (
        patch("src.services.scoring.fresh_calibration.stock_store.list_prediction_accuracy_dates", return_value=["20260530"]),
        patch("src.services.scoring.fresh_calibration.stock_store.load_prediction_accuracy_by_date", return_value=rows),
    ):
        rules = load_fresh_calibration_rules(lookback_dates=20, min_samples=20)

    assert rules[("放量", "过高")]["rate"] == 25.0
