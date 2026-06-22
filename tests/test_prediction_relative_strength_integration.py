import inspect

from src.services.scoring import predict as scoring_predict


def test_prediction_pipeline_loads_relative_strength_context_and_quality():
    source = inspect.getsource(scoring_predict.predict_limit_up_candidates)

    assert "build_relative_strength_context" in source
    assert "relative_strength_index_history" in source
    assert '"relative_strength"' in source
    assert "relative_strength_warnings" in source
