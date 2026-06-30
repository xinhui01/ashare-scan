from src.services.market_focus_advice_service import (
    build_market_focus_advice,
    format_market_focus_advice_lines,
    resolve_market_focus_advice,
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


def test_retreat_day_focuses_waiting_and_formats_no_trade_rule():
    advice = build_market_focus_advice(
        {
            "market_state_label": "退潮日",
            "market_state_strategy": {"label": "空仓观望 / 不操作"},
        },
        {"cont": 3, "first": 2, "fresh": 4, "wrap": 8, "trend": 1},
    )

    lines = format_market_focus_advice_lines(advice)

    assert advice["primary"] == []
    assert [item["category"] for item in advice["secondary"]] == ["wrap"]
    assert "空仓观望" in advice["focus_text"]
    assert advice["next_theme_text"] == "退潮观望，不新增题材操作"
    assert "明日题材方向：退潮观望，不新增题材操作" in lines
    assert "今日重点池：空仓观望" in lines
    assert any("退潮日不操作" in line for line in lines)


def test_retreat_day_local_theme_is_observation_only():
    advice = build_market_focus_advice(
        {
            "market_state_label": "退潮日",
            "board_strength": {"半导体": 5.0},
            "concept_hype_topics": [
                {"name": "先进封装", "source": "概念", "phase": "主升", "today_count": 4},
            ],
        },
        {"cont": 1, "first": 1, "fresh": 1, "wrap": 2, "trend": 1},
    )

    assert "局部强方向：芯片/半导体" in advice["summary"]
    assert "仅作观察" in advice["summary"]
    assert "优先看" not in advice["summary"]


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


def test_market_focus_advice_formats_next_day_theme_from_active_topics():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "concept_hype_topics": [
                {
                    "name": "机器人",
                    "source": "概念",
                    "phase": "主升",
                    "today_count": 4,
                    "opportunity_score": 82,
                },
                {
                    "name": "固态电池",
                    "source": "LLM题材",
                    "phase": "萌芽",
                    "today_count": 3,
                    "opportunity_score": 76,
                },
                {
                    "name": "专用设备制造业",
                    "source": "行业",
                    "phase": "主升",
                    "today_count": 11,
                    "opportunity_score": 91,
                },
            ],
        },
        {"cont": 1, "first": 2, "fresh": 6, "wrap": 1, "trend": 0},
    )

    lines = format_market_focus_advice_lines(advice)

    assert advice["next_theme_text"] == "机器人(主升，今4只)、固态电池(萌芽，今3只)"
    assert "明日题材方向：机器人(主升，今4只)、固态电池(萌芽，今3只)" in lines


def test_market_focus_advice_uses_theme_prediction_groups_for_next_day_theme():
    advice = build_market_focus_advice(
        {
            "market_state_label": "过渡日",
            "market_state_strategy": {"label": "首板为主，谨慎接力"},
        },
        {"cont": 1, "first": 2, "fresh": 6, "wrap": 1, "trend": 0},
        {
            "groups": [
                {
                    "name": "机器人",
                    "source": "概念",
                    "phase": "主升",
                    "today_count": 4,
                    "candidate_count": 5,
                },
                {
                    "name": "汽车制造业",
                    "source": "行业",
                    "phase": "主升",
                    "today_count": 9,
                    "candidate_count": 8,
                },
                {
                    "name": "固态电池",
                    "source": "LLM题材",
                    "phase": "萌芽",
                    "today_count": 2,
                    "candidate_count": 3,
                },
            ]
        },
    )

    assert advice["next_theme_text"] == "机器人(主升，今4只)、固态电池(萌芽，今2只)"


def test_resolve_market_focus_advice_refreshes_old_payload_without_next_theme():
    advice = resolve_market_focus_advice({
        "market_focus_advice": {
            "state_label": "轮动日",
            "summary": "行情打法建议：轮动日",
        },
        "compare_context": {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
        },
        "fresh_first_board_candidates": [{"code": "300001"}],
        "theme_prediction": {
            "groups": [
                {
                    "name": "机器人",
                    "source": "概念",
                    "phase": "主升",
                    "candidate_count": 1,
                }
            ]
        },
    })

    assert advice["next_theme_text"] == "机器人(主升，候选1只)"


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


def test_market_focus_advice_does_not_promote_declining_main_line():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "strong_main_line": {
                "name": "机器人",
                "source": "概念",
                "phase": "主升",
                "trend": "declining",
                "today_count": 5,
                "active_days": 10,
                "opportunity_score": 82,
            },
        },
        {"cont": 2, "first": 5, "fresh": 12, "wrap": 1, "trend": 3},
    )

    assert [item["category"] for item in advice["primary"]] == ["fresh"]
    assert "首板新题材优先" in advice["reason"]
    assert "机器人" not in advice["reason"]


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


def test_market_focus_advice_names_semiconductor_line_only_with_evidence():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "strong_main_line": {
                "name": "计算机、通信和其他电子设备制造业",
                "source": "行业",
                "phase": "主升",
                "trend": "rising",
                "today_count": 18,
                "active_days": 5,
                "opportunity_score": 100,
            },
            "board_strength": {
                "半导体": 5.0,
                "电子化学品": 4.4,
                "元件": 3.8,
            },
            "concept_hype_topics": [
                {"name": "先进封装", "today_count": 4, "phase": "主升"},
                {"name": "存储芯片", "today_count": 3, "phase": "萌芽"},
            ],
        },
        {"cont": 2, "first": 8, "fresh": 12, "wrap": 1, "trend": 6},
    )

    assert advice["local_theme"]["name"] == "芯片/半导体"
    assert "局部强方向：芯片/半导体" in advice["summary"]
    assert "半导体(+5.0%)" in advice["summary"]
    assert "先进封装" in advice["summary"]
    assert any("芯片/半导体" in rule for rule in advice["execution_rules"])


def test_market_focus_advice_keeps_broad_electronics_name_without_theme_evidence():
    advice = build_market_focus_advice(
        {
            "market_state_label": "轮动日",
            "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
            "strong_main_line": {
                "name": "计算机、通信和其他电子设备制造业",
                "source": "行业",
                "phase": "主升",
                "trend": "rising",
                "today_count": 18,
                "active_days": 5,
                "opportunity_score": 100,
            },
            "board_strength": {"元件": 3.8, "通信设备": 2.0},
        },
        {"cont": 2, "first": 8, "fresh": 12, "wrap": 1, "trend": 6},
    )

    assert advice.get("local_theme") == {}
    assert "芯片" not in advice["summary"]
    assert "计算机、通信和其他电子设备制造业" in advice["summary"]
