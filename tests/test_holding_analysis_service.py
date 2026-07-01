import pandas as pd

from src.services.holding_analysis_service import analyze_holding


def _history(closes, volumes=None):
    volumes = volumes or [1000] * len(closes)
    return pd.DataFrame({
        "date": pd.date_range("2026-06-01", periods=len(closes)).strftime("%Y-%m-%d"),
        "open": closes,
        "high": [v * 1.03 for v in closes],
        "low": [v * 0.97 for v in closes],
        "close": closes,
        "volume": volumes,
    })


def test_continue_holding_when_trend_and_followthrough_are_strong():
    result = analyze_holding(
        _history([10, 10.2, 10.4, 10.6, 10.9, 11.2, 11.5, 11.8, 12.1, 12.4]),
        {"strong_followthrough": {"has_strong_followthrough": True}},
    )

    assert result["advice"] == "继续持有"
    assert result["risk_level"] == "低"
    assert result["score"] >= 75
    assert result["reasons"]


def test_exit_observation_when_price_breaks_short_averages_on_heavy_volume():
    result = analyze_holding(
        _history(
            [10, 10.3, 10.6, 10.8, 11.0, 10.7, 10.3, 9.9, 9.5, 9.1],
            [1000, 950, 980, 990, 1000, 1100, 1250, 1500, 1800, 2300],
        ),
        {"broken_limit_up": True},
    )

    assert result["advice"] == "止盈或离场观察"
    assert result["risk_level"] == "高"
    assert result["score"] < 45


def test_mixed_signals_return_watchful_advice():
    result = analyze_holding(
        _history([10, 10.1, 10.2, 10.4, 10.5, 10.45, 10.4, 10.35, 10.38, 10.42]),
        {},
    )

    assert result["advice"] in {"谨慎持有", "减仓观察"}
    assert 45 <= result["score"] < 75


def test_missing_history_returns_unknown():
    result = analyze_holding(pd.DataFrame(), {})

    assert result["advice"] == "无法判断"
    assert result["risk_level"] == "高"
    assert result["score"] == 0
    assert result["reasons"]
