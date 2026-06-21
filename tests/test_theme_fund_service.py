from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from src.services.theme_fund_service import build_theme_fund_context


class _Fetcher:
    def __init__(self, history: pd.DataFrame) -> None:
        self._history = history

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None):
        return self._history.copy()


def _accumulating_history() -> pd.DataFrame:
    start = date(2026, 1, 1)
    closes = []
    volumes = []
    for i in range(80):
        close = 10 + i * 0.045
        if i > 65:
            close += (i - 65) * 0.035
        closes.append(round(close, 2))
        volume = 1_000_000 + i * 8_000
        if i > 50:
            volume += (i - 50) * 18_000
        volumes.append(volume)
    return pd.DataFrame(
        {
            "date": [(start + timedelta(days=i)).strftime("%Y%m%d") for i in range(80)],
            "close": closes,
            "volume": volumes,
        }
    )


def test_build_theme_fund_context_scores_accumulation_and_breakout():
    concepts = [
        {
            "name": "机器人",
            "source": "概念",
            "phase": "主升",
            "trend": "rising",
            "today_count": 4,
            "total_limit_ups": 9,
            "opportunity_score": 82,
            "members": [{"code": "300001"}, {"code": "300002"}],
            "related_industries": [{"name": "通用设备"}],
        }
    ]

    result = build_theme_fund_context(
        concepts,
        fetcher=_Fetcher(_accumulating_history()),
    )

    assert result["theme_fund_accumulation_map"]["机器人"] > 0
    assert result["theme_breakout_map"]["机器人"] > 0
    assert result["theme_fund_score_map"]["机器人"] >= 40
    assert result["code_theme_fund_score"]["300001"] >= 40
    assert result["industry_theme_fund_score"]["通用设备"] >= 40
    assert result["theme_sentiment_delta"] > 0
