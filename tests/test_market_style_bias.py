"""Market-state style bias for limit-up prediction scoring."""
import pandas as pd

from src.services.scoring import cont as cont_scoring
from src.services.scoring import fresh as fresh_scoring
from src.services.scoring import shared as shared_scoring


class _NoHistoryFetcher:
    def get_history_data(self, *_args, **_kwargs):
        return None


class _HistoryFetcher:
    def __init__(self, df):
        self._df = df

    def get_history_data(self, *_args, **_kwargs):
        return self._df


def _ohlc(closes, vols, last_candle=None):
    rows = []
    for i, (close, volume) in enumerate(zip(closes, vols)):
        rows.append({
            "date": f"2026-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}",
            "open": close,
            "close": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "volume": volume,
        })
    if last_candle:
        rows[-1].update(last_candle)
    return pd.DataFrame(rows)


def _rotation_context():
    return {
        "sentiment_score": 86,
        "market_state_label": "轮动日",
        "market_state_strategy": {"label": "首板新题材 / 避开老主线"},
        "market_rotation": {
            "rotation_score": 42,
            "main_line_status": "broken",
            "new_industries": ["半导体", "机器人"],
        },
        "code_to_concept_phase": {
            "300001": "萌芽",
            "600001": "主升",
        },
        "code_theme_map": {
            "300001": "机器人",
            "600001": "有色金属",
        },
    }


def test_market_style_bias_prefers_new_theme_first_board_on_rotation_day():
    fresh_bonus, fresh_reasons = shared_scoring.market_style_bias(
        "fresh", "300001", "半导体", _rotation_context()
    )
    cont_bonus, cont_reasons = shared_scoring.market_style_bias(
        "cont", "600001", "工业金属", _rotation_context(), boards=3
    )

    assert fresh_bonus >= 8
    assert any("轮动日首板新题材" in reason for reason in fresh_reasons)
    assert cont_bonus <= -12
    assert any("轮动日老主线接力降权" in reason for reason in cont_reasons)


def test_market_style_bias_does_not_treat_confirmed_strong_line_as_old_rotation_risk():
    context = _rotation_context()
    context["strong_main_line"] = {
        "name": "半导体",
        "source": "行业",
        "phase": "主升",
        "today_count": 4,
        "active_days": 8,
        "opportunity_score": 72,
    }

    first_bonus, first_reasons = shared_scoring.market_style_bias(
        "first", "600010", "半导体", context
    )
    cont_bonus, cont_reasons = shared_scoring.market_style_bias(
        "cont", "600011", "半导体", context, boards=3
    )

    assert first_bonus > 0
    assert any("主线" in reason for reason in first_reasons)
    assert cont_bonus > -10
    assert any("主线" in reason for reason in cont_reasons)


def test_continuation_scoring_is_cut_on_rotation_day_old_mainline():
    rec = {
        "code": "600001",
        "name": "老主线",
        "industry": "工业金属",
        "consecutive_boards": 3,
        "break_count": 0,
        "first_board_time": "09:32",
        "turnover": 8.0,
        "close": 10.0,
        "change_pct": 10.0,
    }
    neutral_context = {"sentiment_score": 86}
    rotation_context = _rotation_context()

    neutral = cont_scoring.score_continuation_by_compare(
        rec, {"工业金属": 8}, neutral_context, fetcher=_NoHistoryFetcher()
    )
    rotation = cont_scoring.score_continuation_by_compare(
        rec, {"工业金属": 8}, rotation_context, fetcher=_NoHistoryFetcher()
    )

    assert neutral["score"] - rotation["score"] >= 12
    assert "轮动日老主线接力降权" in rotation["reasons"]


def test_fresh_scoring_rewards_rotation_day_new_theme_candidate():
    closes = [8 + 0.16 * i for i in range(31)] + [12.8 - 0.10 * i for i in range(1, 35)]
    vols = [5000] * 59 + [2000] * 5 + [8000]
    df = _ohlc(closes, vols, last_candle={"open": 9.6, "close": 9.4, "high": 9.65, "low": 8.6})
    rec = {
        "code": "300001",
        "name": "新题材",
        "change_pct": 1.0,
        "turnover": 8.0,
        "industry": "半导体",
        "float_mcap": 30e8,
    }

    neutral = fresh_scoring.score_fresh_first_board(
        rec, {"半导体": 2}, {"sentiment_score": 86}, fetcher=_HistoryFetcher(df)
    )
    rotation = fresh_scoring.score_fresh_first_board(
        rec, {"半导体": 2}, _rotation_context(), fetcher=_HistoryFetcher(df)
    )

    assert neutral is not None
    assert rotation is not None
    assert rotation["score"] - neutral["score"] >= 8
    assert "轮动日首板新题材" in rotation["reasons"]


def test_sentiment_context_propagates_market_state_and_rotation_to_compare_context():
    from src.services.scoring.predict import _apply_market_sentiment_context

    compare_context = {"theme_sentiment_delta": 4}
    data_quality = {"sentiment": {}, "warnings": []}
    sent = {
        "score": 82,
        "position_suggest": {"label": "谨慎参与"},
        "market_state": {
            "label": "轮动日",
            "strategy": {"label": "首板新题材 / 避开老主线"},
        },
        "raw": {
            "rotation": {
                "rotation_score": 42,
                "main_line_status": "broken",
                "new_industries": ["半导体"],
            },
            "external": {"ok": True},
        },
    }

    _apply_market_sentiment_context(compare_context, data_quality, sent)

    assert compare_context["sentiment_score"] == 86
    assert compare_context["market_state_label"] == "轮动日"
    assert compare_context["market_state_strategy"]["label"] == "首板新题材 / 避开老主线"
    assert compare_context["market_rotation"]["new_industries"] == ["半导体"]
    assert data_quality["sentiment"]["market_state"] == "轮动日"
    assert data_quality["sentiment"]["strategy_label"] == "首板新题材 / 避开老主线"
    assert data_quality["sentiment"]["rotation_score"] == 42
