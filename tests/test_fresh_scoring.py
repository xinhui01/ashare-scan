"""资金接入型 score_fresh_first_board 行为锁定测试。"""
import pandas as pd

from src.services.scoring import fresh as fresh_scoring


class _FakeFetcher:
    def __init__(self, df):
        self._df = df

    def get_history_data(self, code, days=120, force_refresh=False, request_plan=None, **_kw):
        return self._df


def _ohlc(closes, vols, last_candle=None):
    rows = []
    for i, (c, v) in enumerate(zip(closes, vols)):
        rows.append({
            "date": f"2026-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}",
            "open": c, "close": c, "high": c * 1.01, "low": c * 0.99, "volume": v,
        })
    if last_candle:
        rows[-1].update(last_candle)
    return pd.DataFrame(rows)


def test_capital_inflow_stabilizing_stock_scores_via_new_signals():
    # 先涨后跌（prior_weak、低位）+ 近5日地量 + 今日放量锤子止跌 + 同板块联动
    closes = [8 + 0.16 * i for i in range(31)] + [12.8 - 0.10 * i for i in range(1, 35)]
    vols = [5000] * 59 + [2000] * 5 + [8000]
    df = _ohlc(closes, vols, last_candle={"open": 9.6, "close": 9.4, "high": 9.65, "low": 8.6})

    rec = {"code": "000001", "name": "资金接入", "change_pct": 1.0,
           "turnover": 8.0, "industry": "半导体", "float_mcap": 30e8}
    out = fresh_scoring.score_fresh_first_board(
        rec, {"半导体": 3}, {},
        fetcher=_FakeFetcher(df),
    )

    assert out is not None
    assert out["stabilizing"] is True
    assert out["volume_ignited"] is True
    assert "放量资金进" in out["reasons"]
    assert "止跌" in out["reasons"]
    assert "同板块今日" in out["reasons"]
    assert out["score"] >= 40
    assert out["predict_type"] == "首板涨停"


def test_high_position_stock_is_penalized_and_not_stabilizing():
    # 持续上行到今日最高位（无前期走弱）→ 不止跌 + 高位扣分
    closes = [8 + (14 - 8) * i / 64 for i in range(65)]
    vols = [5000] * 64 + [8000]
    df = _ohlc(closes, vols)

    rec = {"code": "000002", "name": "高位", "change_pct": 2.0,
           "turnover": 10.0, "industry": "白酒", "float_mcap": 80e8}
    out = fresh_scoring.score_fresh_first_board(
        rec, {}, {},
        fetcher=_FakeFetcher(df),
    )

    assert out is not None
    assert out["stabilizing"] is False
    assert "60日位置" in out["reasons"]


def test_cooldown_recent_limit_up_returns_none():
    # 最近 5 日内有涨停 → 不算首板，返回 None
    closes = [10.0] * 60 + [10.0, 11.0, 11.0, 11.0, 11.0]  # 第61天 +10% 涨停
    vols = [4000] * 65
    df = _ohlc(closes, vols)
    rec = {"code": "000003", "name": "刚涨停", "change_pct": 1.0, "industry": "X"}
    out = fresh_scoring.score_fresh_first_board(rec, {}, {}, fetcher=_FakeFetcher(df))
    assert out is None
