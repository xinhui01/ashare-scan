import pandas as pd
import pytest

from scripts import pattern_limit_up_prob
from scripts.pattern_limit_up_prob import pct_table


class NoBooleanSliceDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return NoBooleanSliceDataFrame

    def __getitem__(self, key):
        if isinstance(key, (pd.Series, list, tuple)) and pd.api.types.is_bool_dtype(key):
            raise AssertionError("pct_table should not boolean-slice the full DataFrame")
        return super().__getitem__(key)


def test_pct_table_aggregates_y_without_boolean_slicing_full_dataframe():
    df = NoBooleanSliceDataFrame(
        {
            "change_pct": [-1.0, 1.0, 4.0, 6.0, 8.0, 10.0],
            "y": [0.0, 1.0, 0.0, 1.0, 1.0, 0.0],
            "extra_float": [1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
        }
    )

    try:
        text = pct_table(
            df,
            "change_pct",
            [-50, 0, 3, 5, 7, 9.7, 100],
            ["跌", "0~3%", "3~5%", "5~7%", "7~9.7%", ">=涨停"],
            base=50.0,
        )
    except AssertionError as exc:
        pytest.fail(str(exc))

    assert "0~3%" in text
    assert "样本不足" in text


def test_load_filters_st_in_sql_and_reads_history_in_chunks(monkeypatch):
    def fake_read_sql(sql, con, chunksize=None):
        assert "NOT LIKE '%ST%'" in sql.upper()
        assert chunksize == pattern_limit_up_prob.LOAD_CHUNKSIZE
        yield NoBooleanSliceDataFrame(
            {
                "code_id": [0, 1],
                "trade_date": [20260624, 20260625],
                "big": [0, 0],
                "open": [10, 11],
                "close": [10.5, 11.5],
                "high": [10.6, 11.6],
                "low": [9.9, 10.9],
                "volume": [1000, 1100],
                "amount": [10000, 11000],
                "change_pct": [1.2, 2.3],
            }
        )

    class FakeConnection:
        def close(self):
            pass

    monkeypatch.setattr(pattern_limit_up_prob.sqlite3, "connect", lambda _: FakeConnection())
    monkeypatch.setattr(pattern_limit_up_prob.pd, "read_sql", fake_read_sql)

    df = pattern_limit_up_prob.load()

    assert list(df["code_id"]) == [0, 1]
    assert str(df["code_id"].dtype) == "int32"


def test_shift_by_code_matches_groupby_shift_without_groupby_indexer():
    df = pd.DataFrame(
        {
            "code": ["000001", "000001", "000001", "000002", "000002"],
            "code_id": [0, 0, 0, 1, 1],
            "close": [10.0, 11.0, 12.0, 20.0, 21.0],
        }
    )

    prev = pattern_limit_up_prob.shift_by_code(df["close"], df["code_id"], 1)
    nxt = pattern_limit_up_prob.shift_by_code(df["close"], df["code_id"], -1)

    pd.testing.assert_series_equal(prev, df.groupby("code_id")["close"].shift(1).astype("float32"))
    pd.testing.assert_series_equal(nxt, df.groupby("code_id")["close"].shift(-1).astype("float32"))


def test_report_shows_latest_stocks_that_match_each_setup():
    df = pd.DataFrame(
        {
            "code_id": [0, 1],
            "trade_date": [20260626, 20260626],
            "big": [False, False],
            "is_lu": [False, False],
            "new_high20": [False, False],
            "change_pct": [4.0, -1.0],
            "vol_ratio": [1.0, 1.0],
            "ma_bullish": [False, False],
            "dist_ma5": [0.0, 0.0],
            "vol_shrink": [1.0, 1.0],
            "trend10": [0.0, 0.0],
            "lu20_prev": [0.0, 0.0],
            "pos60": [50.0, 50.0],
            "box20_pct": [50.0, 50.0],
            "up_streak": [1, 0],
            "y": [1.0, 0.0],
        }
    )
    lookup = {0: "000001 平安银行", 1: "000002 万科A"}

    text = pattern_limit_up_prob.report(df, "测试", lookup, stock_limit=10)

    assert "最新交易日命中股票" in text
    assert "普通中阳(对照)" in text
    assert "000001 平安银行" in text


def test_format_stock_matches_defaults_to_one_visible_stock():
    lookup = {
        0: "000001 平安银行",
        1: "000002 万科A",
    }

    text = pattern_limit_up_prob.format_stock_matches(
        pd.Series([0, 1]), lookup, pattern_limit_up_prob.DISPLAY_STOCK_LIMIT
    )

    assert text == "000001 平安银行 ... 共2只"


def test_recent_report_title_uses_latest_90_trading_days():
    df = pd.DataFrame({"trade_date": [20260624, 20260625, 20260626]})

    title, recent = pattern_limit_up_prob.recent_window(df, recent_td=2)

    assert title == "近 2 交易日 (20260625 ~ 20260626) —— 当前市场环境"
    assert list(recent["trade_date"]) == [20260625, 20260626]


def test_feature_window_keeps_warmup_before_recent_days():
    df = pd.DataFrame({"trade_date": [1, 2, 3, 4, 5]})

    window = pattern_limit_up_prob.feature_window(df, recent_td=2, warmup_td=1)

    assert list(window["trade_date"]) == [3, 4, 5]
