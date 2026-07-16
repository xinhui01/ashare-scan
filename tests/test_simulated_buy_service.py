import pytest
import pandas as pd

from src.services.simulated_buy_service import (
    apply_accuracy_result,
    build_account_curve,
    build_intraday_return_curve,
    build_simulated_buy_picks,
    build_trade_snapshot,
    summarize_historical_simulated_buy_picks,
    summarize_simulated_buy_picks,
    sync_simulated_buy_history,
)


def test_build_simulated_buy_picks_selects_two_unique_best_candidates():
    result = {
        "trade_date": "20260703",
        "continuation_candidates": [
            {
                "code": "000001",
                "name": "Alpha",
                "score": 80,
                "consecutive_boards": 1,
                "opening_confirmation": {"status": "观察"},
            },
        ],
        "first_board_candidates": [
            {
                "code": "000002",
                "name": "Beta",
                "score": 72,
                "accumulation_score": 5,
                "relative_strength_score": 10,
                "opening_confirmation": {"status": "可买"},
            },
            {
                "code": "000001",
                "name": "Alpha Again",
                "score": 90,
                "opening_confirmation": {"status": "可买"},
            },
        ],
        "trend_limit_up_candidates": [
            {
                "code": "000003",
                "name": "Gamma",
                "score": 70,
                "opening_confirmation": {"status": "风险过高"},
            },
        ],
    }
    best_buckets = {"first": (70, 79)}

    picks = build_simulated_buy_picks(result, best_buckets=best_buckets)

    assert [p["code"] for p in picks] == ["000002", "000001"]
    assert [p["category"] for p in picks] == ["first", "first"]
    assert picks[0]["buy_status"] == "可买"


def test_summarize_simulated_buy_picks_uses_category_hit_and_profit_rows():
    picks = [
        {"code": "000001", "category": "cont"},
        {"code": "000002", "category": "first"},
        {"code": "000003", "category": "trend"},
    ]
    results_map = {
        ("000001", "cont"): {
            "hit_buyable": 1,
            "hit_strict": 1,
            "hit_loose": 1,
            "t1_open_close_pct": 9.8,
        },
        ("000002", "first"): {
            "hit_buyable": 1,
            "hit_strict": 0,
            "hit_loose": 1,
            "t1_open_close_pct": 5.2,
        },
        ("000003", "trend"): {
            "hit_buyable": 1,
            "hit_strict": 0,
            "hit_loose": 0,
            "t1_open_close_pct": -2.0,
        },
    }

    summary = summarize_simulated_buy_picks(picks, results_map)

    assert summary["evaluated"] == 3
    assert summary["wins"] == 2
    assert summary["win_rate"] == 66.66666666666666
    assert summary["total_profit_pct"] == 13.0
    assert summary["avg_profit_pct"] == 13.0 / 3
    assert [p["result_text"] for p in summary["picks"]] == ["命中", "命中", "未中"]


def test_summarize_historical_simulated_buy_picks_rebuilds_daily_picks_and_totals_profit():
    predictions = [
        {
            "trade_date": "20260701",
            "first_board_candidates": [
                {"code": "000001", "name": "Alpha", "score": 95, "opening_confirmation": {"status": "可买"}},
                {"code": "000002", "name": "Beta", "score": 80, "opening_confirmation": {"status": "观察"}},
            ],
        },
        {
            "trade_date": "20260702",
            "first_board_candidates": [
                {"code": "000003", "name": "Gamma", "score": 99, "opening_confirmation": {"status": "可买"}},
                {"code": "000004", "name": "Delta", "score": 70, "opening_confirmation": {"status": "观察"}},
            ],
        },
    ]
    results_by_date = {
        "20260701": {
            ("000001", "first"): {
                "hit_buyable": 1,
                "hit_loose": 1,
                "t1_open_close_pct": 6.0,
            },
            ("000002", "first"): {
                "hit_buyable": 1,
                "hit_loose": 0,
                "t1_open_close_pct": -2.5,
            },
        },
        "20260702": {
            ("000003", "first"): {
                "hit_buyable": 1,
                "hit_loose": 0,
                "t1_open_close_pct": -1.0,
            },
        },
    }

    summary = summarize_historical_simulated_buy_picks(predictions, results_by_date)

    assert summary["total"] == 4
    assert summary["evaluated"] == 3
    assert summary["pending"] == 1
    assert summary["wins"] == 1
    assert summary["win_rate"] == pytest.approx(100.0 / 3)
    assert summary["total_profit_pct"] == 2.5
    assert summary["avg_profit_pct"] == pytest.approx(2.5 / 3)


def test_build_trade_snapshot_and_apply_completed_result():
    pick = {
        "trade_date": "20260701",
        "code": "000001",
        "category": "first",
        "category_label": "二波接力",
        "score": 88,
    }

    snapshot = build_trade_snapshot(pick)
    updated = apply_accuracy_result(snapshot, {
        "verify_date": "20260702",
        "t1_open": 10.0,
        "t1_close": 10.5,
        "t1_open_close_pct": 5.0,
        "hit_buyable": 1,
    })

    assert updated["prediction_date"] == "20260701"
    assert updated["trade_status"] == "completed"
    assert updated["profit_pct"] == 5.0
    assert updated["is_buyable"] == 1


@pytest.mark.parametrize(
    ("result", "status", "reason"),
    [
        ({"t1_one_word": 1, "hit_buyable": 0}, "one_word", "一字板不可买"),
        ({"t1_suspended": 1, "hit_buyable": 0}, "suspended", "停牌"),
        ({"hit_buyable": 1, "t1_open": None, "t1_close": 10.0}, "missing_price", "开盘价或收盘价缺失"),
    ],
)
def test_apply_accuracy_result_maps_unavailable_states(result, status, reason):
    updated = apply_accuracy_result(
        build_trade_snapshot({"trade_date": "20260701", "code": "000001"}),
        {"verify_date": "20260702", **result},
    )

    assert updated["trade_status"] == status
    assert updated["unavailable_reason"] == reason


def test_build_account_curve_equal_weights_and_compounds():
    rows = [
        {"trade_date": "20260702", "trade_status": "completed", "is_buyable": 1, "profit_pct": 10.0},
        {"trade_date": "20260702", "trade_status": "completed", "is_buyable": 1, "profit_pct": -2.0},
        {"trade_date": "20260703", "trade_status": "completed", "is_buyable": 1, "profit_pct": 5.0},
        {"trade_date": "20260703", "trade_status": "one_word", "is_buyable": 0, "profit_pct": 99.0},
    ]

    curve = build_account_curve(rows)

    assert curve[0]["daily_return_pct"] == pytest.approx(4.0)
    assert curve[0]["cumulative_return_pct"] == pytest.approx(4.0)
    assert curve[1]["cumulative_return_pct"] == pytest.approx(9.2)


def test_build_intraday_return_curve_uses_open_buy_price():
    df = pd.DataFrame({"time": ["09:30", "15:00"], "price": [10.0, 10.5]})

    points = build_intraday_return_curve(df, buy_price=10.0)

    assert points["times"] == ["09:30", "15:00"]
    assert points["returns_pct"] == pytest.approx([0.0, 5.0])


def test_sync_simulated_buy_history_is_idempotent_and_updates_results():
    predictions = [{
        "trade_date": "20260701",
        "first_board_candidates": [
            {"code": "000001", "score": 90, "opening_confirmation": {"status": "可买"}},
        ],
    }]
    results = {"20260701": {("000001", "first"): {
        "verify_date": "20260702",
        "t1_open": 10.0,
        "t1_close": 10.5,
        "t1_open_close_pct": 5.0,
        "hit_buyable": 1,
    }}}
    saved_keys = set()
    updated = []

    def save(records):
        inserted = 0
        for record in records:
            key = (record["prediction_date"], record["code"])
            if key not in saved_keys:
                saved_keys.add(key)
                inserted += 1
        return inserted

    def update(prediction_date, code, **fields):
        updated.append((prediction_date, code, fields))
        return True

    first = sync_simulated_buy_history(predictions, results, save_trades_fn=save, update_result_fn=update)
    second = sync_simulated_buy_history(predictions, results, save_trades_fn=save, update_result_fn=update)

    assert first == {"inserted": 1, "updated": 1}
    assert second == {"inserted": 0, "updated": 1}
    assert updated[-1][2]["trade_status"] == "completed"
