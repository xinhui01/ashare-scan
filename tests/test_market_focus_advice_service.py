from src.services.market_focus_advice_service import (
    build_market_focus_advice,
    format_market_focus_advice_lines,
)


def test_rotation_day_focuses_fresh_board_and_avoids_relay_pools():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "market_rotation": {"rotation_score": 80},
        },
        {"cont": 4, "first": 2, "fresh": 0, "wrap": 1, "trend": 3},
    )

    assert [item["category"] for item in advice["primary"]] == ["fresh"]
    assert [item["category"] for item in advice["secondary"]] == ["wrap"]
    assert [item["category"] for item in advice["avoid"]] == ["cont", "first", "trend"]
    assert "首板涨停(0只" in advice["focus_text"]
    assert "宁可空仓" in advice["focus_text"]
    assert "保留涨停/连板(4只)" in advice["avoid_text"]
    assert "二波接力(2只)" in advice["avoid_text"]


def test_ice_point_focus_prefers_waiting_over_forced_candidates():
    advice = build_market_focus_advice(
        {
            "market_state_label": "冰点日",
            "market_state_strategy": {"label": "空仓观望 / 极少试探超跌反包"},
        },
        {"cont": 1, "first": 1, "fresh": 2, "wrap": 0, "trend": 1},
    )

    assert advice["primary"] == []
    assert [item["category"] for item in advice["secondary"]] == ["wrap"]
    assert "空仓观望" in advice["focus_text"]
    assert "反包(0只" in advice["focus_text"]
    assert "保留涨停/连板(1只)" in advice["avoid_text"]


def test_market_focus_advice_formats_summary_lines_for_ui_and_excel():
    advice = build_market_focus_advice(
        {"market_state_label": "过渡日"},
        {"cont": 3, "first": 2, "fresh": 5, "wrap": 1, "trend": 0},
    )

    lines = format_market_focus_advice_lines(advice)

    assert lines[0].startswith("行情打法建议：")
    assert "今日重点池：首板涨停(5只)" in lines
    assert "备选观察：二波接力(2只)、反包(1只)" in lines
    assert "谨慎/回避池：保留涨停/连板(3只)、趋势涨停(0只)" in lines
