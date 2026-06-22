from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.services.scoring.cont import score_continuation_by_compare


class _HistoryFetcher:
    def __init__(self, history: pd.DataFrame) -> None:
        self._history = history

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return self._history.copy()


def _series_history(stock_strong: bool = True) -> pd.DataFrame:
    start = date(2026, 5, 1)
    closes = []
    volumes = []
    for i in range(45):
        base = 10.0 + i * (0.22 if stock_strong else 0.02)
        if i == 44:
            base = closes[-1] * 1.1
        closes.append(round(base, 2))
        volumes.append(900_000 + i * 12_000)
    return pd.DataFrame(
        {
            "date": [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(45)],
            "close": closes,
            "volume": volumes,
        }
    )


def _flat_index_like(stock_history: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for i, d in enumerate(stock_history["date"].tolist()):
        rows.append({"date": d, "close": 100.0 + i * 0.03})
    return pd.DataFrame(rows)


def _rec() -> dict:
    return {
        "code": "600001",
        "name": "强弱样本",
        "industry": "测试行业",
        "consecutive_boards": 2,
        "close": 20.0,
        "change_pct": 10.0,
        "turnover": 8.0,
        "break_count": 0,
        "first_board_time": "09:35",
    }


def test_continuation_score_adds_relative_strength_when_index_available():
    stock_history = _series_history(stock_strong=True)

    result = score_continuation_by_compare(
        _rec(),
        hot_industries={"测试行业": 2},
        compare_context={
            "latest_continuation_rate": 30,
            "relative_strength_index_history": {
                "sh000001": _flat_index_like(stock_history),
            },
        },
        fetcher=_HistoryFetcher(stock_history),
    )

    assert result["relative_strength_available"] is True
    assert result["relative_strength_score"] > 0
    assert "强弱" in result["reasons"] or "强于指数" in result["reasons"]


def test_continuation_score_marks_relative_strength_missing_without_index():
    stock_history = _series_history(stock_strong=True)

    result = score_continuation_by_compare(
        _rec(),
        hot_industries={"测试行业": 2},
        compare_context={
            "latest_continuation_rate": 30,
            "relative_strength_index_history": {},
        },
        fetcher=_HistoryFetcher(stock_history),
    )

    assert result["relative_strength_available"] is False
    assert result["relative_strength_score"] is None
    assert "强弱因子已跳过" in result["relative_strength_note"]
