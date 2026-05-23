"""测试 StockDataFetcher._derive_limit_up_pool_from_spot 派生今日涨停池逻辑。

兜底链：东财涨停池失败 → 全市场 spot（含新浪兜底）→ 过滤涨停股 → 递推连板数。
本测试只覆盖派生逻辑（_derive_limit_up_pool_from_spot），不联网。
"""
from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import patch

from stock_data import StockDataFetcher


@pytest.fixture
def eod():
    """构造一个不联网的 StockDataFetcher 实例（绕开 __init__ 里的代理线程等副作用）。"""
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    return instance


def _make_spot(rows):
    """rows: [(代码, 涨跌幅, 最新价, 换手率, 所属行业, 名称), ...]"""
    return pd.DataFrame([
        {"代码": r[0], "涨跌幅": r[1], "最新价": r[2],
         "换手率": r[3], "所属行业": r[4], "名称": r[5]}
        for r in rows
    ])


class TestDeriveFromSpot:
    def test_none_spot_returns_empty(self, eod):
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=None):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert df.empty

    def test_empty_spot_returns_empty(self, eod):
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=pd.DataFrame()):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert df.empty

    def test_filter_to_limit_up_only(self, eod):
        # 主板：+10% 阈值；3 只达 / 不达
        spot = _make_spot([
            ("600000", 10.0, 5.5, 3.0, "银行", "浦发银行"),     # +10%, 主板涨停
            ("600001", 9.7, 5.2, 2.5, "钢铁", "邯郸钢铁"),      # +9.7%, 边界（>= threshold-0.3=9.7）算涨停
            ("600002", 5.0, 5.0, 2.0, "钢铁", "齐鲁石化"),      # +5%, 不算
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 2
        assert set(df["代码"].astype(str).tolist()) == {"600000", "600001"}

    def test_growth_board_20pct_threshold(self, eod):
        # 创业板 300xxx 阈值 20%，+11% 不算
        spot = _make_spot([
            ("300001", 11.0, 22.0, 5.0, "电子", "ABC"),
            ("300002", 20.0, 24.0, 8.0, "电子", "DEF"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 1
        assert df.iloc[0]["代码"] == "300002"

    def test_beijing_board_30pct_threshold(self, eod):
        # 北交所 8xxxxx 阈值 30%
        spot = _make_spot([
            ("830001", 29.0, 13.0, 5.0, "材料", "BJ1"),
            ("830002", 30.0, 13.0, 5.0, "材料", "BJ2"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        assert len(df) == 1
        assert df.iloc[0]["代码"] == "830002"

    def test_consecutive_boards_inferred_from_prev_pool(self, eod):
        # 昨日 pool：A 连板=2, B 连板=1。今日 A、B、C 都涨停 → A=3 / B=2 / C=1
        prev_pool = pd.DataFrame([
            {"代码": "600100", "连板数": 2, "名称": "A"},
            {"代码": "600200", "连板数": 1, "名称": "B"},
        ])
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "X", "A"),
            ("600200", 10.0, 11.0, 5.0, "X", "B"),
            ("600300", 10.0, 11.0, 5.0, "X", "C"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520", prev_pool_df=prev_pool)
        df_indexed = df.set_index("代码")
        assert int(df_indexed.loc["600100", "连板数"]) == 3
        assert int(df_indexed.loc["600200", "连板数"]) == 2
        assert int(df_indexed.loc["600300", "连板数"]) == 1

    def test_no_prev_pool_defaults_to_one(self, eod):
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "X", "A"),
            ("600200", 10.0, 11.0, 5.0, "X", "B"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520", prev_pool_df=None)
        assert (df["连板数"] == 1).all()

    def test_required_columns_present(self, eod):
        spot = _make_spot([
            ("600100", 10.0, 11.0, 5.0, "银行", "A"),
        ])
        with patch.object(eod, "_fetch_spot_with_fallback", return_value=spot):
            df = eod._derive_limit_up_pool_from_spot("20260520")
        required = {"代码", "名称", "最新价", "涨跌幅", "换手率", "连板数", "所属行业"}
        assert required.issubset(set(df.columns))


class TestLimitUpPoolEmptyRetry:
    def _build_fetcher(self):
        instance = StockDataFetcher.__new__(StockDataFetcher)
        instance._log = lambda msg: None
        instance._limit_up_pool_cache = {}
        instance._prev_limit_up_pool_cache = {}
        instance._last_pool_source = {}
        instance._last_prev_pool_source = {}
        return instance

    def test_empty_result_is_not_cached_and_second_call_retries_network(self, monkeypatch):
        fetcher = self._build_fetcher()
        calls = {"count": 0}

        monkeypatch.setattr(fetcher, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)

        def _fake_retry(_fn, date=None):
            calls["count"] += 1
            if calls["count"] == 1:
                return pd.DataFrame()
            return pd.DataFrame([{"代码": "600000", "名称": "浦发银行", "连板数": 1}])

        monkeypatch.setattr("stock_data._retry_ak_call", _fake_retry)

        first = fetcher.get_limit_up_pool("20260520")
        second = fetcher.get_limit_up_pool("20260520")

        assert first.empty
        assert not second.empty
        assert calls["count"] == 2

    def test_empty_memory_cache_does_not_block_retry(self, monkeypatch):
        fetcher = self._build_fetcher()
        fetcher._limit_up_pool_cache["20260520"] = pd.DataFrame()

        monkeypatch.setattr(fetcher, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            "stock_data._retry_ak_call",
            lambda _fn, date=None: pd.DataFrame([{"代码": "600001", "名称": "测试股", "连板数": 1}]),
        )

        df = fetcher.get_limit_up_pool("20260520")

        assert not df.empty
        assert "20260520" in fetcher._limit_up_pool_cache
        assert not fetcher._limit_up_pool_cache["20260520"].empty


def test_gui_refresh_clears_limit_up_pool_cache_and_triggers_retry():
    from src.gui.app import StockMonitorApp
    from src.gui.tabs.predict import PredictTab

    # 构造 minimal app + predict tab 实例，绕开完整初始化
    app = StockMonitorApp.__new__(StockMonitorApp)
    predict = PredictTab.__new__(PredictTab)
    predict.app = app
    app.predict = predict

    class Var:
        def __init__(self, v):
            self.v = v
        def get(self):
            return self.v
        def set(self, val):
            self.v = val

    class LabelStub:
        def config(self, **_kwargs):
            pass

    # 构造 fetcher 与 stock_filter
    class FetcherStub:
        def __init__(self):
            self._limit_up_pool_cache = {"20260520": pd.DataFrame()}
            self._prev_limit_up_pool_cache = {"20260520": pd.DataFrame()}
        def _normalize_trade_date(self, d):
            return "20260520"

    class FilterStub:
        def __init__(self, fetcher):
            self.fetcher = fetcher
            self._log = lambda msg: None

    fetcher = FetcherStub()
    app.stock_filter = FilterStub(fetcher)
    predict.history_var = Var("")
    predict.date_var = Var("20260520")
    predict.status_label = LabelStub()

    called = {"start": False, "historical_mode": None}
    def _start(historical_mode=False):
        called["start"] = True
        called["historical_mode"] = historical_mode
    predict.start = _start

    # 执行刷新
    predict.refresh_selected_date()

    # 断言缓存被清除且触发重跑
    assert "20260520" not in fetcher._limit_up_pool_cache
    assert "20260520" not in fetcher._prev_limit_up_pool_cache
    assert called["start"] is True
    assert called["historical_mode"] is True
