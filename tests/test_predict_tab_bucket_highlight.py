from src.gui.tabs.predict import PredictTab


class _TrueVar:
    def get(self):
        return True


def _predict_tab_with_buckets() -> PredictTab:
    tab = PredictTab.__new__(PredictTab)
    tab.best_buckets = {
        "cont": (80, 100),
        "cont_1to2": (50, 59),
        "cont_2to3": None,
    }
    tab.bucket_rates_cache = {
        "cont": {
            (50, 59): {"rate": 5.0, "buyable": 20, "hit": 1, "eligible": True},
            (80, 100): {"rate": 30.0, "buyable": 10, "hit": 3, "eligible": True},
        },
        "cont_1to2": {
            (50, 59): {"rate": 40.0, "buyable": 20, "hit": 8, "eligible": True},
            (80, 100): {"rate": 0.0, "buyable": 6, "hit": 0, "eligible": True},
        },
        "cont_2to3": {
            (50, 59): {"rate": 0.0, "buyable": 2, "hit": 0, "eligible": False},
        },
    }
    tab.sort_by_hit_bucket = _TrueVar()
    return tab


def test_cont_row_highlight_uses_subcategory_best_bucket_first():
    tab = _predict_tab_with_buckets()

    assert tab._row_tag("cont", None, 55, {"consecutive_boards": 1}) == "best_bucket"
    assert tab._row_tag("cont", None, 85, {"consecutive_boards": 1}) == "score_high"


def test_cont_row_highlight_falls_back_to_total_when_subcategory_has_no_best_bucket():
    tab = _predict_tab_with_buckets()

    assert tab._row_tag("cont", None, 85, {"consecutive_boards": 2}) == "best_bucket"


def test_cont_bucket_sort_priority_uses_subcategory_rates_first():
    tab = _predict_tab_with_buckets()

    priority = tab._bucket_priority_for("cont")

    assert priority({"score": 55, "consecutive_boards": 1}) == 40.0
    assert priority({"score": 85, "consecutive_boards": 1}) == 0.0


def test_fresh_row_highlight_uses_raw_score_for_historical_bucket():
    tab = PredictTab.__new__(PredictTab)
    tab.best_buckets = {"fresh": (70, 79)}

    record = {"score": 76, "calibrated_score": 52}

    assert tab._row_tag("fresh", None, 52, record) == "best_bucket"
