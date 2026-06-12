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


def test_result_includes_previous_day_sentiment(monkeypatch):
    aggs = {
        # 昨日：宽度+高度极弱 → 冰点日
        "20260610": {
            "lu_count": 20,
            "broken_count": 5,
            "broken_total_times": 6,
            "max_boards": 2,
            "high_board_count_4plus": 0,
            "codes": ["000001", "000002"],
            "top_industries": [("汽车", 5), ("化工", 3)],
            "hhi": 0.12,
        },
        # 今日：高度+晋级双强 + 主线延续 → 接力日
        "20260611": {
            "lu_count": 80,
            "broken_count": 10,
            "broken_total_times": 12,
            "max_boards": 6,
            "high_board_count_4plus": 3,
            "codes": ["000001", "600519"],
            "top_industries": [("汽车", 20), ("机械", 10)],
            "hhi": 0.15,
        },
    }
    prev_map = {"20260611": "20260610", "20260610": "20260609"}

    monkeypatch.setattr(
        svc.stock_store, "list_limit_up_pool_trade_dates",
        lambda: ["20260610", "20260611"],
    )
    monkeypatch.setattr(svc, "_ensure_pool_dates_ready", lambda date_keys, log: [])
    monkeypatch.setattr(svc, "_load_pool_aggregates", lambda d: aggs.get(d))
    monkeypatch.setattr(
        svc, "_avg_lu_count_5d",
        lambda end_date: (40.0, ["20260609"], {"20260609": 40}),
    )
    monkeypatch.setattr(svc, "_previous_pool_date", lambda end_date: prev_map.get(end_date, ""))

    result = svc.analyze_market_sentiment("20260611", fetch_external=False, log=lambda _s: None)

    prev = result.get("previous")
    assert prev is not None, "结果应包含昨日情绪 previous 字段"
    assert prev["trade_date"] == "20260610"
    assert isinstance(prev["score"], int)
    assert prev["market_state"]["label"] == "冰点日"
    assert result["market_state"]["label"] == "接力日"
    # 昨日结果不再继续嵌套，避免递归
    assert "previous" not in prev


def test_previous_omitted_when_yesterday_unavailable(monkeypatch):
    monkeypatch.setattr(
        svc.stock_store, "list_limit_up_pool_trade_dates", lambda: ["20260611"],
    )
    monkeypatch.setattr(svc, "_ensure_pool_dates_ready", lambda date_keys, log: [])
    monkeypatch.setattr(
        svc, "_load_pool_aggregates",
        lambda d: {
            "lu_count": 30,
            "broken_count": 5,
            "broken_total_times": 5,
            "max_boards": 3,
            "high_board_count_4plus": 0,
            "codes": ["000001"],
        } if d == "20260611" else None,
    )
    monkeypatch.setattr(
        svc, "_avg_lu_count_5d",
        lambda end_date: (30.0, ["20260610"], {"20260610": 30}),
    )
    monkeypatch.setattr(svc, "_previous_pool_date", lambda end_date: "")

    result = svc.analyze_market_sentiment("20260611", fetch_external=False, log=lambda _s: None)

    assert result.get("previous") is None


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
