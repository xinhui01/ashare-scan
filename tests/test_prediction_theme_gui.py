import inspect

from src.gui.tabs.predict import PredictTab
from src.services.scoring import predict as scoring_predict


def test_prediction_summary_renders_theme_first_groups():
    apply_src = inspect.getsource(PredictTab._apply_result)

    assert 'result.get("theme_prediction")' in apply_src
    assert "主线题材候选" in apply_src
    assert "连板核心" in apply_src
    assert "首板补涨" in apply_src


def test_prediction_result_persists_concept_hype_payload():
    predict_src = inspect.getsource(scoring_predict.predict_limit_up_candidates)

    assert '"concept_hype_result": hype' in predict_src


def test_prediction_result_uses_concept_hype_payload_for_candidate_themes():
    apply_src = inspect.getsource(PredictTab._apply_result)

    assert 'result.get("concept_hype_result")' in apply_src
    assert "self._rebuild_candidate_theme_index" in apply_src


def test_prediction_ui_keeps_concept_hype_workspace_inside_prediction_tab():
    build_src = inspect.getsource(PredictTab._build)

    assert "_setup_concept_hype_subtab" in build_src


def test_prediction_toolbar_does_not_duplicate_theme_analysis_entry():
    build_src = inspect.getsource(PredictTab._build)

    assert 'text="题材分析"' not in build_src
    assert "_run_theme_analysis_from_toolbar" not in build_src


def test_concept_hype_tab_exposes_concept_index_refresh_entry():
    setup_src = inspect.getsource(PredictTab._setup_concept_hype_subtab)

    assert 'text="刷新概念库"' in setup_src
    assert "_refresh_concept_index_from_predict" in setup_src


def test_prediction_candidate_tables_show_industry_and_theme_columns():
    build_src = inspect.getsource(PredictTab._build)
    render_src = inspect.getsource(PredictTab._render_trees)

    assert '"industry", "theme"' in build_src
    assert '"industry": ("行业"' in build_src
    assert '"theme": ("题材"' in build_src
    assert 'rec.get("industry", "")' in render_src
    assert "_candidate_theme_label" in render_src


def test_prediction_reason_cell_text_wraps_at_readable_boundaries():
    text = "4连板+30 / 4板开盘溢价偏大-10 / 未炸板+15 / 竞价高开+8"

    wrapped = PredictTab._wrap_reason_cell_text(text, max_units=28, max_lines=3)

    assert "\n" in wrapped
    assert "4连板+30" in wrapped
    assert "未炸板+15" in wrapped
    assert all(
        PredictTab._display_text_units(line) <= 28
        for line in wrapped.splitlines()
    )
    assert PredictTab._wrap_reason_cell_text("短句", max_units=36) == "短句"


def test_prediction_reason_wrap_units_use_visible_tail_width():
    class FakeTree:
        def __init__(self):
            self._columns = (
                "code", "name", "industry", "theme", "change_pct",
                "volume_ratio", "dist_ma5", "trend_5d", "score",
                "confirm", "auction", "result", "reasons",
            )
            self._widths = {
                "code": 70, "name": 85, "industry": 85, "theme": 110,
                "change_pct": 75, "volume_ratio": 60, "dist_ma5": 65,
                "trend_5d": 70, "score": 65, "confirm": 70,
                "auction": 115, "result": 90, "reasons": 260,
            }

        def cget(self, key):
            return self._columns if key == "columns" else ""

        def column(self, col, option):
            return self._widths[col] if option == "width" else None

        def winfo_width(self):
            return 1150

    units = PredictTab._reason_wrap_units_for_tree(FakeTree())

    assert units < 40
    assert units <= 30


def test_prediction_candidate_tables_use_multiline_reason_cells():
    build_src = inspect.getsource(PredictTab._build)
    render_src = inspect.getsource(PredictTab._render_trees)

    assert "PredictCandidate.Treeview" in build_src
    assert "rowheight=56" in build_src
    assert "_reason_cell_text" in render_src


def test_prediction_candidate_tables_have_horizontal_scrollbars():
    build_src = inspect.getsource(PredictTab._build)

    assert "orient=tk.HORIZONTAL" in build_src
    assert "xscrollcommand" in build_src
    assert ".xview" in build_src
    assert "stretch=False" in build_src


def test_prediction_candidate_tabs_use_short_titles():
    assert PredictTab._candidate_tab_title("fresh", 9, 9) == "首板(9)"
    assert PredictTab._candidate_tab_title("first", 21, 23) == "二波(21/23)"
    assert PredictTab._candidate_tab_title("concept", 0, 0) == "概念"

    render_src = inspect.getsource(PredictTab._render_trees)

    assert "_candidate_tab_title" in render_src
    assert 'self.table_nb.tab(2, text=self._candidate_tab_title("fresh"' in render_src


def test_prediction_accuracy_text_is_compact_for_header_label():
    text = PredictTab._accuracy_header_text(
        "首板涨停",
        "0.0% (0/1)",
        20,
        16.0,
        71,
        444,
        0.74,
    )

    assert "昨日 0.0% (0/1)" in text
    assert "近20日 16.0% (71/444)" in text
    assert "均涨 +0.74%" in text
    assert "昨日命中率" not in text
    assert "平均次日涨幅" not in text


def test_prediction_accuracy_labels_have_wrapping_space():
    build_src = inspect.getsource(PredictTab._build)

    assert "wraplength=900" in build_src
    assert "justify=tk.LEFT" in build_src


def test_prediction_filter_bar_shows_theme_data_status_label():
    build_src = inspect.getsource(PredictTab._build)
    apply_src = inspect.getsource(PredictTab._apply_result)
    concept_src = inspect.getsource(PredictTab._apply_concept_hype_result)

    assert "theme_data_status_label" in build_src
    assert "_refresh_theme_data_status_label" in apply_src
    assert "_refresh_theme_data_status_label" in concept_src


def test_concept_hype_backfill_rebuilds_candidate_theme_groups():
    concept_src = inspect.getsource(PredictTab._apply_concept_hype_result)
    helper_src = inspect.getsource(PredictTab._rebuild_theme_prediction_from_hype)
    apply_src = inspect.getsource(PredictTab._apply_result)

    assert "_rebuild_theme_prediction_from_hype" in concept_src
    assert "build_theme_prediction_groups" in helper_src
    assert "self.lists =" in apply_src
    assert "_sync_concept_hype_for_result" in apply_src


def test_prediction_theme_column_does_not_relabel_industry_as_theme():
    tab = object.__new__(PredictTab)

    tab._rebuild_candidate_theme_index(
        {
            "groups": [
                {
                    "name": "专用设备制造业",
                    "source": "行业",
                    "roles": {"core": [{"code": "001234"}]},
                },
                {
                    "name": "机器人",
                    "source": "概念",
                    "roles": {"core": [{"code": "005678"}]},
                },
            ],
        },
        {
            "concepts": [
                {
                    "name": "汽车制造业",
                    "source": "行业",
                    "members": [{"code": "000111"}],
                    "related_industries": [{"name": "汽车制造业"}],
                },
                {
                    "name": "液冷服务器",
                    "source": "LLM题材",
                    "members": [{"code": "000222"}],
                    "related_industries": [{"name": "通信设备"}],
                },
            ],
        },
        {"code_theme_map": {"000333": "通用设备制造业"}},
    )

    assert tab._candidate_theme_label({"code": "001234", "industry": "专用设备制造业"}) == ""
    assert tab._candidate_theme_label({"code": "000111", "industry": "汽车制造业"}) == ""
    assert tab._candidate_theme_label({"code": "000333", "industry": "通用设备制造业"}) == ""
    assert tab._candidate_theme_label({"code": "005678"}) == "机器人"
    assert tab._candidate_theme_label({"code": "000222"}) == "液冷服务器"


def test_prediction_theme_column_ignores_stale_industry_theme_fields():
    tab = object.__new__(PredictTab)
    tab._rebuild_candidate_theme_index(
        {
            "groups": [
                {
                    "name": "通用设备制造业",
                    "source": "行业",
                    "roles": {"core": [{"code": "001234"}]},
                }
            ],
        },
        {
            "concepts": [
                {
                    "name": "电气机械和器材",
                    "source": "行业",
                    "members": [{"code": "300252"}],
                }
            ],
        },
        {},
    )

    assert tab._candidate_theme_label(
        {"code": "001234", "industry": "通用设备制造业", "theme": "通用设备制造业"}
    ) == ""
    assert tab._candidate_theme_label(
        {"code": "300252", "industry": "电气机械和器材", "theme_name": "电气机械和器材"}
    ) == ""


def test_concept_hype_warns_when_only_industry_sources_are_available():
    industry_only = {
        "concepts": [
            {"name": "专用设备制造业", "source": "行业"},
            {"name": "通用设备制造业", "source": "行业"},
        ],
        "stats": {
            "concept_pairs": 0,
            "concept_covered_codes": 0,
            "llm_cache_days": 0,
        },
    }
    real_theme = {
        "concepts": [
            {"name": "机器人", "source": "概念"},
            {"name": "专用设备制造业", "source": "行业"},
        ],
        "stats": {
            "concept_pairs": 12,
            "concept_covered_codes": 5,
            "llm_cache_days": 0,
        },
    }

    hint = PredictTab._concept_hype_theme_source_hint(industry_only)

    assert "只有行业来源" in hint
    assert "刷新概念库" in hint
    assert PredictTab._concept_hype_theme_source_hint(real_theme) == ""


def test_prediction_summary_explains_empty_theme_column_when_only_industry_source():
    text = PredictTab._theme_column_status_text(
        {
            "concepts": [
                {"name": "电气机械和器材", "source": "行业"},
                {"name": "计算机、通信和其他电子设备制造业", "source": "行业"},
            ],
            "stats": {
                "concept_pairs": 0,
                "concept_covered_codes": 0,
                "llm_cache_days": 0,
            },
        },
        {},
        {},
    )

    assert "题材列" in text
    assert "概念库为空" in text
    assert "刷新概念库" in text
