from __future__ import annotations

import pandas as pd
from datetime import date, timedelta

from src.services.scoring.trend import score_trend_limit_up


class _HistoryFetcher:
    def __init__(self, history: pd.DataFrame) -> None:
        self._history = history

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return self._history.copy()


def _trend_history_with_accumulation() -> pd.DataFrame:
    closes = []
    volumes = []
    for i in range(80):
        if i < 24:
            close = 10.0 + i * 0.25
        elif i < 32:
            close = 16.0 - (i - 24) * 0.48
        else:
            close = 12.2 + (i - 32) * 0.066
        if i >= 72:
            close += (i - 71) * 0.04
        closes.append(round(close, 2))

        base_volume = 1000 + i * 8
        if i >= 50:
            base_volume += (i - 49) * 18
        if i >= 72 and i % 2 == 0:
            base_volume += 180
        volumes.append(int(base_volume))

    start = date(2026, 1, 1)
    return pd.DataFrame(
        {
            "date": [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(80)],
            "close": closes,
            "volume": volumes,
        }
    )


def test_trend_score_exposes_accumulation_only_for_trend_candidates():
    history = _trend_history_with_accumulation()
    rec = {
        "code": "000001",
        "name": "趋势样本",
        "industry": "测试行业",
        "change_pct": 3.2,
        "turnover": 6.0,
    }

    result = score_trend_limit_up(
        rec,
        hot_industries={},
        compare_context={},
        fetcher=_HistoryFetcher(history),
    )

    assert result is not None
    assert result["predict_type"] == "趋势涨停"
    assert result["accumulation_score"] > 0
    assert result["accumulation_days"] == 30
    assert result["accumulation_risk_penalty"] <= 0
    assert "潜伏" in result["reasons"]
