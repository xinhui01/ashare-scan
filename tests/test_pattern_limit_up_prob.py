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
                "code": ["000001", "000002"],
                "trade_date": ["2026-06-24", "2026-06-25"],
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

    assert list(df["code"]) == ["000001", "000002"]
