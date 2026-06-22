import inspect

from src.services.scoring import first, fresh, trend, wrap


def test_all_candidate_scorers_expose_relative_strength_fields():
    sources = [
        inspect.getsource(first.score_followthrough_candidate),
        inspect.getsource(fresh.score_fresh_first_board),
        inspect.getsource(wrap.score_broken_board_wrap),
        inspect.getsource(trend.score_trend_limit_up),
    ]

    for source in sources:
        assert "relative_strength_bonus" in source
        assert "relative_strength_score" in source
        assert "relative_strength_available" in source
