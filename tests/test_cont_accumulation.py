from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.services.scoring.cont import score_continuation_by_compare


class _HistoryFetcher:
    def __init__(self, history: pd.DataFrame) -> None:
        self._history = history

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return self._history.copy()


def _limit_up_history_with_accumulation() -> pd.DataFrame:
    start = date(2026, 1, 1)
    closes = []
    volumes = []
    for i in range(80):
        if i < 24:
            close = 9.0 + i * 0.2
        elif i < 34:
            close = 13.8 - (i - 24) * 0.34
        else:
            close = 10.7 + (i - 34) * 0.055
        if i == 79:
            close = round(closes[-1] * 1.1, 2)
        closes.append(round(close, 2))

        volume = 900_000 + i * 7_000
        if i >= 50:
            volume += (i - 49) * 16_000
        if i == 79:
            volume = int(volume * 1.35)
        volumes.append(int(volume))

    return pd.DataFrame(
        {
            "date": [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(80)],
            "close": closes,
            "volume": volumes,
        }
    )


def test_continuation_score_uses_30_day_accumulation_signal():
    rec = {
        "code": "000001",
        "name": "保留样本",
        "industry": "测试行业",
        "consecutive_boards": 1,
        "close": 14.0,
        "change_pct": 10.0,
        "turnover": 8.0,
        "break_count": 0,
        "first_board_time": "09:35",
    }

    result = score_continuation_by_compare(
        rec,
        hot_industries={"测试行业": 3},
        compare_context={"latest_continuation_rate": 30},
        fetcher=_HistoryFetcher(_limit_up_history_with_accumulation()),
    )

    assert result["predict_type"] == "保留涨停"
    assert result["accumulation_score"] > 0
    assert result["accumulation_days"] == 30
    assert "潜伏" in result["reasons"]
