from src.services.scoring import predict as scoring_predict
from src.gui.tabs.predict import PredictTab


def _candidate(code, score, category, industry="", theme="", **extra):
    row = {
        "code": code,
        "name": code,
        "score": score,
        "industry": industry,
        "theme": theme,
        "predict_type": category,
        "reasons": "",
    }
    row.update(extra)
    return row


def test_strong_main_line_selects_best_sustained_line_not_first_match():
    concepts = [
        {
            "name": "机器人",
            "source": "概念",
            "phase": "主升",
            "today_count": 2,
            "active_days": 3,
            "opportunity_score": 61,
            "total_limit_ups": 6,
        },
        {
            "name": "半导体",
            "source": "行业",
            "phase": "主升",
            "today_count": 6,
            "active_days": 9,
            "opportunity_score": 88,
            "total_limit_ups": 38,
        },
    ]

    line = scoring_predict._select_strong_main_line(concepts)

    assert line["name"] == "半导体"
    assert line["strength_score"] > 100


def test_declining_main_line_is_not_selected_as_strong_line():
    concepts = [
        {
            "name": "机器人",
            "source": "概念",
            "phase": "主升",
            "trend": "declining",
            "today_count": 7,
            "active_days": 11,
            "opportunity_score": 91,
            "total_limit_ups": 42,
        },
        {
            "name": "半导体",
            "source": "概念",
            "phase": "主升",
            "trend": "rising",
            "today_count": 4,
            "active_days": 6,
            "opportunity_score": 76,
            "total_limit_ups": 22,
        },
    ]

    line = scoring_predict._select_strong_main_line(concepts)

    assert line["name"] == "半导体"


def test_declining_main_line_candidates_are_cut_in_final_ranking():
    ranker = getattr(scoring_predict, "_rank_and_limit_prediction_candidates", None)
    assert callable(ranker)
    buckets = {
        "trend": [
            _candidate("600001", 96, "趋势涨停", industry="机器人"),
            _candidate("600002", 82, "趋势涨停", industry="半导体"),
        ],
    }
    context = {
        "market_state_label": "轮动日",
        "strong_main_line": {
            "name": "半导体",
            "source": "概念",
            "phase": "主升",
            "trend": "rising",
            "today_count": 4,
            "active_days": 6,
            "opportunity_score": 76,
        },
        "declining_main_lines": [
            {
                "name": "机器人",
                "phase": "末期",
                "trend": "declining",
                "today_count": 1,
                "active_days": 11,
                "decay_score": 81,
            }
        ],
    }

    ranked, _stats = ranker(buckets, context, theme_quality={"quality_level": "fine_theme"})

    assert ranked["trend"][0]["code"] == "600002"
    assert ranked["trend"][1]["code"] == "600001"
    assert any("衰退主线" in reason for reason in ranked["trend"][1]["final_rank_reasons"])


def test_final_priority_ranking_lifts_strong_mainline_candidate_above_raw_score():
    ranker = getattr(scoring_predict, "_rank_and_limit_prediction_candidates", None)
    assert callable(ranker)
    buckets = {
        "trend": [
            _candidate("600001", 72, "趋势涨停", industry="半导体"),
            _candidate("600002", 91, "趋势涨停", industry="游戏"),
        ],
    }
    context = {
        "market_state_label": "轮动日",
        "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
        "strong_main_line": {
            "name": "半导体",
            "source": "行业",
            "phase": "主升",
            "today_count": 6,
            "active_days": 9,
            "opportunity_score": 88,
        },
    }

    ranked, stats = ranker(buckets, context, theme_quality={"quality_level": "industry_fallback"})

    assert ranked["trend"][0]["code"] == "600001"
    assert ranked["trend"][0]["final_rank_score"] > ranked["trend"][1]["final_rank_score"]
    assert any("强主线" in reason for reason in ranked["trend"][0]["final_rank_reasons"])
    assert stats["top_priority_candidates"][0]["code"] == "600001"


def test_retreat_day_limits_candidate_count_and_prefers_repair_pool():
    ranker = getattr(scoring_predict, "_rank_and_limit_prediction_candidates", None)
    assert callable(ranker)
    buckets = {
        "cont": [_candidate(f"60010{i}", 80 - i, "保留涨停") for i in range(3)],
        "first": [_candidate(f"60020{i}", 82 - i, "二波接力") for i in range(5)],
        "fresh": [_candidate(f"60030{i}", 78 - i, "首板涨停") for i in range(4)],
        "wrap": [
            _candidate(f"60040{i}", 88 - i, "断板反包", theme="机器人")
            for i in range(12)
        ],
        "trend": [_candidate(f"60050{i}", 95 - i, "趋势涨停") for i in range(12)],
    }
    context = {
        "market_state_label": "退潮日",
        "market_state_strategy": {"label": "空仓观望 / 不操作"},
        "sentiment_score": 21,
    }

    ranked, stats = ranker(buckets, context, theme_quality={"quality_level": "fine_theme"})

    assert stats["limited"] is True
    assert stats["limit_reason"].startswith("退潮日")
    assert "观察池" in stats["limit_reason"]
    assert "只保留反包修复" not in stats["limit_reason"]
    assert sum(len(rows) for rows in ranked.values()) <= 15
    assert len(ranked["wrap"]) <= 8
    assert len(ranked["trend"]) <= 4
    assert len(ranked["first"]) <= 1
    assert ranked["wrap"][0]["final_rank_score"] > ranked["trend"][0]["final_rank_score"]
    assert any("退潮日反包观察" in reason for reason in ranked["wrap"][0]["final_rank_reasons"])


def test_non_fresh_non_trend_candidates_must_match_recent_theme():
    ranker = getattr(scoring_predict, "_rank_and_limit_prediction_candidates", None)
    assert callable(ranker)
    buckets = {
        "cont": [
            _candidate("600101", 90, "保留涨停", theme="机器人"),
            _candidate("600102", 99, "保留涨停", industry="银行"),
            _candidate("600103", 98, "保留涨停", industry="银行", theme="银行"),
        ],
        "first": [
            _candidate("600201", 88, "二波接力"),
            _candidate("600202", 92, "二波接力", industry="煤炭"),
        ],
        "wrap": [
            _candidate("600301", 87, "断板反包", theme_name="固态电池"),
            _candidate("600302", 94, "断板反包", industry="地产"),
        ],
        "fresh": [
            _candidate("600401", 71, "首板涨停", industry="家电"),
        ],
        "trend": [
            _candidate("600501", 73, "趋势涨停", industry="食品饮料"),
        ],
    }
    context = {
        "market_state_label": "轮动日",
        "code_theme_map": {"600201": "机器人"},
    }

    ranked, stats = ranker(buckets, context, theme_quality={"quality_level": "fine_theme"})

    assert [row["code"] for row in ranked["cont"]] == ["600101"]
    assert [row["code"] for row in ranked["first"]] == ["600201"]
    assert [row["code"] for row in ranked["wrap"]] == ["600301"]
    assert [row["code"] for row in ranked["fresh"]] == ["600401"]
    assert [row["code"] for row in ranked["trend"]] == ["600501"]
    assert stats["theme_filter"]["removed_counts"] == {
        "cont": 2,
        "first": 1,
        "fresh": 0,
        "wrap": 1,
        "trend": 0,
    }


def test_theme_data_quality_marks_industry_only_fallback_as_low_quality():
    builder = getattr(scoring_predict, "_build_theme_data_quality", None)
    assert callable(builder)

    quality = builder(
        hype_stats={"concept_pairs": 0, "concept_covered_codes": 0, "llm_cache_days": 0},
        real_concepts=[],
        industry_concepts_count=4,
        code_theme_map={},
    )

    assert quality["quality_level"] == "industry_fallback"
    assert quality["fine_theme_available"] is False
    assert "细题材" in quality["warning"]

    good = builder(
        hype_stats={"concept_pairs": 18, "concept_covered_codes": 23, "llm_cache_days": 3},
        real_concepts=[{"name": "先进封装", "source": "概念"}],
        industry_concepts_count=2,
        code_theme_map={"600001": "先进封装"},
    )
    assert good["quality_level"] == "fine_theme"
    assert good["fine_theme_available"] is True


def test_compact_concept_hype_topics_keeps_lightweight_theme_evidence():
    compact = getattr(scoring_predict, "_compact_concept_hype_topics", None)
    assert callable(compact)

    topics = compact([
        {
            "name": "先进封装",
            "source": "概念",
            "phase": "主升",
            "trend": "rising",
            "today_count": 4,
            "members": [{"code": "600001"}],
        },
        {
            "name": "计算机、通信和其他电子设备制造业",
            "source": "行业",
            "phase": "主升",
            "today_count": 18,
            "members": [{"code": "600002"}],
        },
    ])

    assert topics == [
        {
            "name": "先进封装",
            "source": "概念",
            "phase": "主升",
            "trend": "rising",
            "today_count": 4,
            "active_days": 0,
        },
        {
            "name": "计算机、通信和其他电子设备制造业",
            "source": "行业",
            "phase": "主升",
            "trend": "",
            "today_count": 18,
            "active_days": 0,
        },
    ]


def test_gui_score_sort_uses_final_rank_score_when_available():
    assert PredictTab._sort_value(
        {"score": 90, "final_rank_score": 66},
        "score",
    ) == 66.0
    assert PredictTab._sort_value({"score": 90}, "score") == 90.0
