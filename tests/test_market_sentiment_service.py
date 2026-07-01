from __future__ import annotations

from src.services import market_sentiment_service as svc


def test_retreat_state_strategy_is_no_trade():
    strategy = svc._STATE_STRATEGIES["退潮日"]["strategy"]

    assert strategy["label"] == "空仓观望 / 不操作"
    assert strategy["pools"] == []
    assert strategy["position_cap"] == 0.0
    assert "不操作" in strategy["notes"]


def test_retreat_state_local_focus_stays_observation_only():
    state = {
        "label": "退潮日",
        "strategy": {
            "label": "空仓观望 / 不操作",
            "pools": [],
            "position_cap": 0.0,
            "notes": "退潮日不操作。",
        },
    }

    out = svc._apply_local_focus_to_market_state(
        state,
        {"name": "芯片/半导体", "reason": "细题材证据：先进封装(4只)"},
    )

    notes = out["strategy"]["notes"]
    assert "仅作观察" in notes
    assert "优先看该方向内二波/趋势核心" not in notes


def test_early_retreat_stage_forbids_wrap_operation():
    state = svc._classify_market_state(
        score=28,
        today_agg={
            "lu_count": 38,
            "max_boards": 3,
            "high_board_count_4plus": 0,
        },
        rotation={"main_line_status": "broken", "rotation_score": 42},
        yest_lu=60,
        today_continued=3,
    )

    stage = state["retreat_stage"]
    assert state["label"] == "退潮日"
    assert stage["code"] == "early_retreat"
    assert stage["allow_wrap"] is False
    assert state["strategy"]["pools"] == []
    assert state["strategy"]["position_cap"] == 0.0
    assert "退潮初期" in state["strategy"]["label"]
    assert "不做反包" in state["strategy"]["notes"]


def test_broad_low_height_rotation_is_not_early_retreat():
    state = svc._classify_market_state(
        score=50,
        today_agg={
            "lu_count": 151,
            "max_boards": 3,
            "high_board_count_4plus": 0,
        },
        rotation={"main_line_status": "broken", "rotation_score": 80},
        yest_lu=140,
        today_continued=23,
    )

    assert state["label"] == "轮动日"
    assert "低位扩散" in state["reason"]
    assert "retreat_stage" not in state
    assert state["strategy"]["position_cap"] > 0


def test_retreat_repair_stage_allows_only_confirmed_wrap():
    state = svc._classify_market_state(
        score=48,
        today_agg={
            "lu_count": 36,
            "max_boards": 3,
            "high_board_count_4plus": 0,
        },
        rotation={"main_line_status": "continued", "rotation_score": 8},
        yest_lu=40,
        today_continued=6,
    )

    stage = state["retreat_stage"]
    assert state["label"] == "退潮日"
    assert stage["code"] == "repair_watch"
    assert stage["allow_wrap"] is True
    assert state["strategy"]["pools"] == ["wrap"]
    assert state["strategy"]["position_cap"] <= 0.15
    assert "确认型反包" in state["strategy"]["label"]
    assert "确认型反包" in state["strategy"]["notes"]


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


def test_market_index_signal_uses_shenzhen_composite_when_available(monkeypatch):
    monkeypatch.setattr(svc.stock_store, "list_limit_up_pool_trade_dates", lambda: ["20260612"])
    monkeypatch.setattr(svc, "_ensure_pool_dates_ready", lambda date_keys, log: [])
    monkeypatch.setattr(
        svc,
        "_load_pool_aggregates",
        lambda d: {
            "lu_count": 40,
            "broken_count": 10,
            "broken_total_times": 10,
            "max_boards": 4,
            "high_board_count_4plus": 1,
            "codes": ["000001"],
            "top_industries": [("银行", 10)],
            "hhi": 0.25,
        },
    )
    monkeypatch.setattr(
        svc, "_avg_lu_count_5d",
        lambda end_date: (40.0, ["20260611"], {"20260611": 40}),
    )
    monkeypatch.setattr(svc, "_previous_pool_date", lambda end_date: "")
    monkeypatch.setattr(
        svc,
        "_fetch_external",
        lambda date_key, log: {
            "down_limit_count": 0,
            "sh_index_pct": -2.0,
            "sz_index_pct": 2.0,
            "index_composite_pct": 0.0,
        },
    )

    result = svc.analyze_market_sentiment("20260612", fetch_external=True, log=lambda _s: None)

    market_signal = next(s for s in result["signals"] if s["name"] == "大盘")
    assert market_signal["value"] == "+0.00%"
    assert market_signal["delta"] == 0
    assert "上证 -2.00%" in market_signal["note"]
    assert "深成指 +2.00%" in market_signal["note"]
    assert "大盘 +0.00%" in result["summary"]


def test_local_focus_infers_semiconductor_only_from_fine_theme_evidence():
    focus = svc._infer_local_focus_from_concepts([
        {"name": "先进封装", "source": "概念", "today_count": 4, "phase": "主升"},
        {"name": "存储芯片", "source": "概念", "today_count": 3, "phase": "萌芽"},
        {
            "name": "计算机、通信和其他电子设备制造业",
            "source": "行业",
            "today_count": 18,
            "phase": "主升",
        },
    ])

    assert focus["name"] == "芯片/半导体"
    assert "先进封装(4只)" in focus["reason"]
    assert "存储芯片(3只)" in focus["reason"]


def test_local_focus_does_not_relabel_broad_electronics_industry_as_chip():
    focus = svc._infer_local_focus_from_concepts([
        {
            "name": "计算机、通信和其他电子设备制造业",
            "source": "行业",
            "today_count": 18,
            "phase": "主升",
        },
        {"name": "通信设备", "source": "行业", "today_count": 3, "phase": "萌芽"},
    ])

    assert focus == {}
