from src.services.scoring.fresh_calibration import (
    build_fresh_calibration_rules,
    calibrate_fresh_candidate,
    load_fresh_calibration_rules,
)
from src.services.scoring import fresh as fresh_scoring
import pandas as pd
from unittest.mock import patch


# 2026-06 资金接入型改造后的 reason 词表（放量资金进 / 出货嫌疑 / 同板块今日 / 温和资金进 / 萌芽期…）
_REASON_A = "放量资金进2.0x+14 / 出货嫌疑-8 / 情绪偏冷-5"      # features: 放量资金进, 出货嫌疑, 情绪偏冷
_REASON_B = "温和资金进1.3x+5 / 同板块今日3涨停+12 / 萌芽期+4"  # features: 温和资金进, 同板块今日, 萌芽期


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
        rows.append(_row(_REASON_A, hit=i < 10))
    for i in range(40):
        rows.append(_row(_REASON_B, hit=i < 4))

    rules = build_fresh_calibration_rules(rows, min_samples=20)
    key = ("放量资金进", "出货嫌疑")

    assert key in rules
    assert rules[key]["buyable"] == 40
    assert rules[key]["hit"] == 10
    assert rules[key]["rate"] == 25.0


def test_build_rules_default_success_is_strict_limit_up_not_loose_profit():
    rows = [_loose_only_row(_REASON_A) for _ in range(30)]
    rows.extend(_row(_REASON_A, hit=True) for _ in range(3))

    rules = build_fresh_calibration_rules(rows, min_samples=20)

    assert rules[("放量资金进", "出货嫌疑")]["hit"] == 3
    assert rules[("放量资金进", "出货嫌疑")]["buyable"] == 33
    assert rules[("放量资金进", "出货嫌疑")]["rate"] == 9.1


def test_calibrate_candidate_marks_high_confidence_from_best_rule():
    rules = {
        ("放量资金进",): {"rate": 16.0, "buyable": 50, "hit": 8},
        ("放量资金进", "出货嫌疑"): {"rate": 25.0, "buyable": 40, "hit": 10},
    }
    candidate = {"score": 52, "reasons": _REASON_A}

    out = calibrate_fresh_candidate(candidate, rules, min_samples=20)

    assert out["calibrated_hit_rate"] == 25.0
    assert out["calibrated_sample_size"] == 40
    assert out["calibrated_rule"] == "放量资金进+出货嫌疑"
    assert out["confidence"] == "涨停高置信"
    assert out["calibrated_score"] > out["score"]


def test_scan_fresh_candidates_sorts_by_calibrated_confidence(monkeypatch):
    rec_high = {"code": "000001", "name": "高置信", "change_pct": 2.0}
    rec_plain = {"code": "000002", "name": "普通", "change_pct": 2.0}

    def fake_score(rec, *_args, **_kwargs):
        if rec["code"] == "000001":
            return {"code": rec["code"], "name": rec["name"], "score": 50, "reasons": _REASON_A}
        return {"code": rec["code"], "name": rec["name"], "score": 65, "reasons": _REASON_B}

    monkeypatch.setattr(fresh_scoring, "score_fresh_first_board", fake_score)

    result = fresh_scoring.scan_fresh_first_board_candidates_cached(
        spot_df=pd.DataFrame([{"代码": "000001"}]),
        zt_codes=set(),
        hot_industries={},
        compare_context={
            "fresh_calibration_rules": {
                ("放量资金进", "出货嫌疑"): {"rate": 25.0, "buyable": 40, "hit": 10}
            }
        },
        fetcher=object(),
        filter_candidates_fn=lambda *_args: [rec_plain, rec_high],
    )

    assert [r["code"] for r in result] == ["000001", "000002"]
    assert result[0]["confidence"] == "涨停高置信"
    assert result[0]["calibrated_hit_rate"] == 25.0
    assert result[1]["confidence"] == "观察"


def test_load_fresh_calibration_rules_reads_recent_accuracy_rows():
    rows = [_row(_REASON_A, hit=i < 10) for i in range(40)]

    with (
        patch("src.services.scoring.fresh_calibration.stock_store.list_prediction_accuracy_dates", return_value=["20260530"]),
        patch("src.services.scoring.fresh_calibration.stock_store.load_prediction_accuracy_by_date", return_value=rows),
    ):
        rules = load_fresh_calibration_rules(lookback_dates=20, min_samples=20)

    assert rules[("放量资金进", "出货嫌疑")]["rate"] == 25.0
