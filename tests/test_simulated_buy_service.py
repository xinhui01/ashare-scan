import pytest

from src.services.simulated_buy_service import (
    build_simulated_buy_picks,
    summarize_historical_simulated_buy_picks,
    summarize_simulated_buy_picks,
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
