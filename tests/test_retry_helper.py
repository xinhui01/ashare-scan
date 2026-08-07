import pytest

import src.utils.retry as retry_mod
from src.utils.retry import retry_call


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(retry_mod.time, "sleep", lambda _s: None)


def test_retries_until_success():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("transient")
        return "ok"

    assert retry_call(flaky, max_attempts=3, base_delay=0.01) == "ok"
    assert len(calls) == 3


def test_raises_after_max_attempts():
    calls = []

    def always_fail():
        calls.append(1)
        raise ValueError("nope")

    with pytest.raises(ValueError):
        retry_call(always_fail, max_attempts=2, base_delay=0.01)
    assert len(calls) == 2


def test_should_retry_false_raises_immediately():
    calls = []

    def fail_fatal():
        calls.append(1)
        raise ValueError("fatal")

    with pytest.raises(ValueError):
        retry_call(
            fail_fatal,
            max_attempts=3,
            base_delay=0.01,
            should_retry=lambda exc: isinstance(exc, ConnectionError),
        )
    assert len(calls) == 1


def test_passes_args_and_kwargs():
    def add(a, b, *, c=0):
        return a + b + c

    assert retry_call(add, 1, 2, c=3) == 6
