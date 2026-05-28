from __future__ import annotations

from src.services import market_sentiment_service as svc


def test_explicit_date_does_not_silently_fallback(monkeypatch):
    state = {"calls": 0}

    def _list_dates():
        state["calls"] += 1
        return ["20260522"]

    monkeypatch.setattr(svc.stock_store, "list_limit_up_pool_trade_dates", _list_dates)
    monkeypatch.setattr(svc, "_ensure_pool_dates_ready", lambda date_keys, log: ["20260525"])

    result = svc.analyze_market_sentiment("20260525", fetch_external=False, log=lambda _s: None)

    assert result["trade_date"] == "20260525"
    assert "20260525 情绪依赖数据不完整" in result["summary"]
    assert result["raw"]["missing_pool_dates"] == ["20260525"]


def test_default_date_still_uses_latest_cached(monkeypatch):
    monkeypatch.setattr(svc.stock_store, "list_limit_up_pool_trade_dates", lambda: ["20260522"])
    monkeypatch.setattr(svc, "_ensure_pool_dates_ready", lambda date_keys, log: [])
    monkeypatch.setattr(
        svc,
        "_load_pool_aggregates",
        lambda d: {
            "lu_count": 10,
            "broken_count": 2,
            "broken_total_times": 2,
            "max_boards": 3,
            "high_board_count_4plus": 0,
            "codes": ["000001"],
        },
    )
    monkeypatch.setattr(
        svc, "_avg_lu_count_5d",
        lambda end_date: (10.0, ["20260521"], {"20260521": 10}),
    )
    monkeypatch.setattr(svc, "_previous_pool_date", lambda end_date: "")

    result = svc.analyze_market_sentiment(None, fetch_external=False, log=lambda _s: None)

    assert result["trade_date"] == "20260522"
