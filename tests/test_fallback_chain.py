import pandas as pd

from src.sources.fallback_chain import run_fallback_chain


def test_first_valid_result_short_circuits():
    called = []

    def a():
        called.append("a")
        return "A"

    def b():
        called.append("b")
        return "B"

    result = run_fallback_chain([("源A", a), ("源B", b)])

    assert result.value == "A"
    assert result.source == "源A"
    assert result.last_error is None
    assert called == ["a"]


def test_exception_falls_through_and_records_last_error():
    boom = RuntimeError("boom")

    def a():
        raise boom

    def b():
        return 42

    logs = []
    result = run_fallback_chain(
        [("源A", a), ("源B", b)],
        log_fn=logs.append,
        chain_name="测试链",
    )

    assert result.value == 42
    assert result.source == "源B"
    assert result.last_error is boom
    assert logs == ["测试链 源A 失败: boom"]


def test_is_valid_rejects_empty_frames():
    def a():
        return pd.DataFrame()

    def b():
        return pd.DataFrame([{"x": 1}])

    result = run_fallback_chain(
        [("源A", a), ("源B", b)],
        is_valid=lambda df: df is not None and not df.empty,
    )

    assert result.source == "源B"
    assert len(result.value) == 1


def test_all_steps_fail_returns_empty_result():
    def a():
        raise ValueError("a-err")

    def b():
        return None

    result = run_fallback_chain([("源A", a), ("源B", b)])

    assert result.value is None
    assert result.source == ""
    assert isinstance(result.last_error, ValueError)
