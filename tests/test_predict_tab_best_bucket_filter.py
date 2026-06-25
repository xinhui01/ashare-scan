"""「仅最优分段」筛选条件的判定逻辑单测。

_row_in_best_bucket 必须与 best_bucket 金色高亮（_row_tag）用同一套判定：
落在该分类历史最优分数段内才算"最优股票"。
"""
from src.gui.tabs.predict import PredictTab


def _tab() -> PredictTab:
    tab = PredictTab.__new__(PredictTab)
    tab.best_buckets = {
        "trend": (50, 70),
        "first": None,          # 样本不足，无最优段
        "fresh": (70, 79),
        "cont": (80, 100),
        "cont_1to2": (50, 59),
    }
    return tab


def test_in_best_bucket_true_on_range_bounds():
    tab = _tab()
    assert tab._row_in_best_bucket("trend", {"score": 50}) is True
    assert tab._row_in_best_bucket("trend", {"score": 60}) is True
    assert tab._row_in_best_bucket("trend", {"score": 70}) is True


def test_out_of_best_bucket_is_false():
    tab = _tab()
    assert tab._row_in_best_bucket("trend", {"score": 49}) is False
    assert tab._row_in_best_bucket("trend", {"score": 71}) is False


def test_no_best_bucket_category_is_false():
    tab = _tab()
    # first 样本不足 → 无最优段 → 任何分数都不算最优
    assert tab._row_in_best_bucket("first", {"score": 60}) is False


def test_invalid_or_missing_score_is_false():
    tab = _tab()
    assert tab._row_in_best_bucket("trend", {}) is False
    assert tab._row_in_best_bucket("trend", {"score": "abc"}) is False


def test_fresh_uses_raw_score_like_gold_highlight():
    tab = _tab()
    # 显示分 52 不在段内，但原始 score 76 在 (70,79) 内 → 与金色高亮同判定
    assert tab._row_in_best_bucket("fresh", {"score": 76, "calibrated_score": 52}) is True


def test_cont_prefers_subcategory_best_bucket():
    tab = _tab()
    # 1板=cont_1to2，用子类段 (50,59)；55 命中
    assert tab._row_in_best_bucket("cont", {"score": 55, "consecutive_boards": 1}) is True
    # 子类无段时回退总类段 (80,100)；85 命中
    assert tab._row_in_best_bucket("cont", {"score": 85, "consecutive_boards": 9}) is True
    # 1板用子类段 (50,59)，85 不在该段内 → 不算最优
    assert tab._row_in_best_bucket("cont", {"score": 85, "consecutive_boards": 1}) is False
