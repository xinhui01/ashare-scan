from src.services.prediction_theme_service import build_theme_prediction_groups


def test_groups_candidates_by_mainline_theme_and_role():
    prediction = {
        "continuation_candidates": [
            {
                "code": "300001",
                "name": "机甲龙头",
                "industry": "通用设备",
                "score": 92,
                "consecutive_boards": 3,
            }
        ],
        "broken_board_wrap_candidates": [
            {
                "code": "300002",
                "name": "机甲修复",
                "industry": "通用设备",
                "score": 84,
            }
        ],
        "fresh_first_board_candidates": [
            {
                "code": "300003",
                "name": "机甲补涨",
                "industry": "通用设备",
                "score": 61,
            }
        ],
        "first_board_candidates": [
            {
                "code": "300004",
                "name": "机甲二波",
                "industry": "通用设备",
                "score": 67,
            }
        ],
        "trend_limit_up_candidates": [
            {
                "code": "600001",
                "name": "冷门趋势",
                "industry": "煤炭开采",
                "score": 72,
            }
        ],
    }
    hype = {
        "concepts": [
            {
                "name": "机器人",
                "source": "概念",
                "phase": "主升",
                "trend": "rising",
                "opportunity_score": 78,
                "today_count": 4,
                "total_limit_ups": 10,
                "members": [
                    {"code": "300001", "name": "机甲龙头"},
                    {"code": "300002", "name": "机甲修复"},
                ],
                "related_industries": [{"name": "通用设备", "count": 5}],
            }
        ],
    }
    compare_context = {
        "code_theme_map": {
            "300001": "机器人",
            "300002": "机器人",
        },
        "theme_size_map": {"机器人": 10},
        "code_to_concept_phase": {
            "300001": "主升",
            "300002": "主升",
        },
    }

    result = build_theme_prediction_groups(
        prediction,
        hype_result=hype,
        compare_context=compare_context,
    )

    assert result["groups"][0]["name"] == "机器人"
    assert result["groups"][0]["source"] == "概念"
    assert result["groups"][0]["phase"] == "主升"
    assert result["groups"][0]["counts"] == {
        "core": 1,
        "relay": 1,
        "repair": 1,
        "replenish": 1,
        "watch": 0,
    }
    assert [x["code"] for x in result["groups"][0]["roles"]["core"]] == ["300001"]
    assert [x["code"] for x in result["groups"][0]["roles"]["repair"]] == ["300002"]
    assert [x["code"] for x in result["groups"][0]["roles"]["replenish"]] == ["300003"]
    assert result["groups"][0]["roles"]["replenish"][0]["theme_match"] == "行业关联"
    assert [x["code"] for x in result["ungrouped"]["roles"]["watch"]] == ["600001"]


def test_direct_code_theme_overrides_industry_fallback():
    prediction = {
        "fresh_first_board_candidates": [
            {
                "code": "300099",
                "name": "跨界补涨",
                "industry": "传媒",
                "score": 70,
            }
        ],
    }
    hype = {
        "concepts": [
            {
                "name": "机器人",
                "source": "概念",
                "phase": "萌芽",
                "opportunity_score": 82,
                "members": [{"code": "300099", "name": "跨界补涨"}],
                "related_industries": [{"name": "传媒", "count": 3}],
            },
            {
                "name": "传媒",
                "source": "行业",
                "phase": "主升",
                "opportunity_score": 60,
                "members": [],
                "related_industries": [{"name": "传媒", "count": 9}],
            },
        ],
    }
    compare_context = {
        "code_theme_map": {"300099": "机器人"},
        "theme_size_map": {"机器人": 4, "传媒": 9},
    }

    result = build_theme_prediction_groups(
        prediction,
        hype_result=hype,
        compare_context=compare_context,
    )

    assert result["groups"][0]["name"] == "机器人"
    assert result["groups"][0]["roles"]["replenish"][0]["theme_match"] == "个股命中"
