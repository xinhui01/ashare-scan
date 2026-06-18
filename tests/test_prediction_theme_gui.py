import inspect

from src.gui.tabs.predict import PredictTab


def test_prediction_summary_renders_theme_first_groups():
    apply_src = inspect.getsource(PredictTab._apply_result)

    assert 'result.get("theme_prediction")' in apply_src
    assert "主线题材候选" in apply_src
    assert "连板核心" in apply_src
    assert "首板补涨" in apply_src
