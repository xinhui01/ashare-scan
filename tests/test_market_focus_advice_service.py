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


def test_market_focus_advice_prefers_established_main_line_over_new_theme_rotation():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "strong_main_line": {
                "name": "半导体",
                "source": "行业",
                "phase": "主升",
                "today_count": 4,
                "active_days": 8,
                "opportunity_score": 72,
            },
        },
        {"cont": 2, "first": 5, "fresh": 12, "wrap": 1, "trend": 3},
    )

    assert "半导体" in advice["reason"]
    assert "首板新题材优先" not in advice["reason"]
    assert [item["category"] for item in advice["primary"]] == ["first", "trend"]
    assert "二波接力(5只)" in advice["focus_text"]
    assert "趋势涨停(3只)" in advice["focus_text"]


def test_weak_rotation_day_formats_confirmation_execution_rules():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "sentiment_score": 24,
        },
        {"cont": 1, "first": 13, "fresh": 8, "wrap": 27, "trend": 43},
    )

    lines = format_market_focus_advice_lines(advice)

    assert "执行规则：谁所在板块最强、谁先主动放量上板，优先做谁；没有板块共振，一个都不做。" in lines
    assert "弱情绪过滤：市场情绪低于30分时，首板池只作为观察名单；必须等板块共振 + 个股主动上板确认。" in lines
