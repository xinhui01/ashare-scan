import inspect

from src.gui.tabs.predict import PredictTab


class _FalseVar:
    def get(self):
        return False


def _predict_tab_for_sorting() -> PredictTab:
    tab = PredictTab.__new__(PredictTab)
    tab.trend_sort_column = "score"
    tab.trend_sort_reverse = True
    tab.sort_by_hit_bucket = _FalseVar()
    tab.bucket_rates_cache = {}
    return tab


def test_trend_heading_click_toggles_dedicated_sort_state():
    tab = _predict_tab_for_sorting()
    # _on_heading_click 末尾走 `if self.result: self._apply_result(...)`，
    # result 置 None 即可跳过重渲染，无需挂真实 UI
    tab.result = None

    tab._on_heading_click("trend", "ma_spread")
    assert tab.trend_sort_column == "ma_spread"
    assert tab.trend_sort_reverse is True

    tab._on_heading_click("trend", "ma_spread")
    assert tab.trend_sort_column == "ma_spread"
    assert tab.trend_sort_reverse is False


def test_trend_records_use_dedicated_sort_state():
    tab = _predict_tab_for_sorting()
    records = [
        {"code": "000001", "score": 68, "ma_spread_pct": 4.0, "ma20_slope_pct": 0.6},
        {"code": "000002", "score": 82, "ma_spread_pct": 2.0, "ma20_slope_pct": 0.4},
    ]

    sorted_records = tab._sort_records(records, "trend")

    assert [item["code"] for item in sorted_records] == ["000002", "000001"]


def test_trend_candidates_are_wired_from_payload_to_gui_table():
    apply_src = inspect.getsource(PredictTab._apply_result)
    render_src = inspect.getsource(PredictTab._render_trees)
    accuracy_src = inspect.getsource(PredictTab._refresh_accuracy_async)
    bucket_src = inspect.getsource(PredictTab._refresh_best_bucket_labels)

    assert 'result.get("trend_limit_up_candidates", [])' in apply_src
    assert '"trend": trend_list' in apply_src
    assert '_result_cell("trend"' in render_src
    assert "self.trend_tree.insert" in render_src
    assert '"trend",' in accuracy_src
    assert '("cont", "first", "fresh", "wrap", "trend")' in bucket_src


def test_prediction_tab_has_excel_export_action():
    build_src = inspect.getsource(PredictTab._build)
    export_src = inspect.getsource(PredictTab.export_prediction_excel)

    assert 'text="导出Excel"' in build_src
    assert "export_prediction_to_excel" in export_src


def test_prediction_tab_wires_simulated_buy_subtab():
    build_src = inspect.getsource(PredictTab._build)
    apply_src = inspect.getsource(PredictTab._apply_result)
    accuracy_src = inspect.getsource(PredictTab._apply_accuracy)
    render_src = inspect.getsource(PredictTab._render_simulated_buy_picks)
    history_src = inspect.getsource(PredictTab._load_historical_simulated_buy_summary)

    assert "模拟买入" in build_src
    assert "build_simulated_buy_picks" in apply_src
    assert "_render_simulated_buy_picks" in apply_src
    assert "_render_simulated_buy_picks" in accuracy_src
    assert "_render_simulated_buy_history" in render_src
    assert "summarize_historical_simulated_buy_picks" in history_src


def test_prediction_tab_syncs_trade_ledger_and_saves_new_picks():
    init_src = inspect.getsource(PredictTab._start_simulated_buy_history_sync)
    apply_src = inspect.getsource(PredictTab._apply_result)
    accuracy_src = inspect.getsource(PredictTab._apply_accuracy)

    assert "sync_simulated_buy_history" in init_src
    assert "save_simulated_buy_trades" in apply_src
    assert "_start_simulated_buy_history_sync" in accuracy_src


def test_simulated_buy_tab_has_history_and_account_curve():
    build_src = inspect.getsource(PredictTab._build)
    render_src = inspect.getsource(PredictTab._render_simulated_buy_history)

    assert "simulated_account_canvas" in build_src
    assert '"buy_price"' in build_src
    assert '"sell_price"' in build_src
    assert "build_account_curve" in render_src
    assert "load_simulated_buy_trades" in render_src
