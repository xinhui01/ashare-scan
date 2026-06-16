"""今日涨停池盘中实时刷新：trade_date==今天 且未收盘(<15:30)时，
get_limit_up_pool 应绕过 内存/SQLite 缓存，强制重抓东财，
使实时视图随盘推进刷新；联网失败时回退已有缓存而非清空。

历史日期 / 收盘后 仍走缓存，行为不变。
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from stock_data import StockDataFetcher
import src.sources.limit_up_pool_service as pool_svc


def _build_fetcher():
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._limit_up_pool_cache = {}
    instance._prev_limit_up_pool_cache = {}
    instance._last_pool_source = {}
    instance._last_prev_pool_source = {}
    return instance


def _df(n):
    return pd.DataFrame(
        [{"代码": f"60000{i}", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0} for i in range(n)]
    )


def _common_mocks(monkeypatch, fetcher, *, eastmoney_df, breaker_open=False, db_df=None):
    monkeypatch.setattr(fetcher, "_normalize_trade_date", lambda d: str(d).replace("-", ""))
    monkeypatch.setattr(fetcher, "_sanitize_limit_up_pool", lambda df, **kwargs: df)
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: breaker_open)
    monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *a, **k: db_df)
    monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *a, **k: None)
    monkeypatch.setattr("stock_data._retry_ak_call", lambda _fn, date=None: eastmoney_df)


class TestTodayPoolIntradayRefresh:
    def test_intraday_today_bypasses_cache_and_refetches(self, monkeypatch):
        """盘中(10:00) 今日池：内存里有残池(2只)，应被忽略，重抓东财(50只)。"""
        f = _build_fetcher()
        # 当前时间固定为 今日 10:00（未收盘）
        monkeypatch.setattr(pool_svc, "_current_dt", lambda: datetime(2026, 6, 16, 10, 0, 0))
        f._limit_up_pool_cache["20260616"] = _df(2)  # 开盘抓到的残池
        _common_mocks(monkeypatch, f, eastmoney_df=_df(50), db_df=None)

        out = f.get_limit_up_pool("20260616")

        assert len(out) == 50, "盘中应重抓东财完整池，而不是返回内存残池"
        assert f.get_pool_source("20260616") == "eastmoney"

    def test_postclose_today_uses_cache(self, monkeypatch):
        """收盘后(16:00) 今日池：缓存视为最终态，直接复用，不再联网。"""
        f = _build_fetcher()
        monkeypatch.setattr(pool_svc, "_current_dt", lambda: datetime(2026, 6, 16, 16, 0, 0))
        f._limit_up_pool_cache["20260616"] = _df(80)

        def _boom(*a, **k):
            raise AssertionError("收盘后不应再联网重抓")

        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: str(d).replace("-", ""))
        monkeypatch.setattr("stock_data._retry_ak_call", _boom)

        out = f.get_limit_up_pool("20260616")
        assert len(out) == 80
        assert f.get_pool_source("20260616") == "cache_memory"

    def test_historical_date_always_uses_cache(self, monkeypatch):
        """历史日期(非今天)：永远走缓存，盘中也不重抓。"""
        f = _build_fetcher()
        monkeypatch.setattr(pool_svc, "_current_dt", lambda: datetime(2026, 6, 16, 10, 0, 0))
        f._limit_up_pool_cache["20260520"] = _df(3)

        def _boom(*a, **k):
            raise AssertionError("历史日期不应联网重抓")

        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: str(d).replace("-", ""))
        monkeypatch.setattr("stock_data._retry_ak_call", _boom)

        out = f.get_limit_up_pool("20260520")
        assert len(out) == 3
        assert f.get_pool_source("20260520") == "cache_memory"

    def test_intraday_refetch_empty_falls_back_to_cache(self, monkeypatch):
        """盘中重抓东财返空(瞬时故障)：应回退已有缓存，而不是把实时视图清空。"""
        f = _build_fetcher()
        monkeypatch.setattr(pool_svc, "_current_dt", lambda: datetime(2026, 6, 16, 10, 0, 0))
        f._limit_up_pool_cache["20260616"] = _df(7)
        _common_mocks(monkeypatch, f, eastmoney_df=pd.DataFrame(), db_df=None)

        out = f.get_limit_up_pool("20260616")
        assert len(out) == 7, "联网返空时应沿用已有缓存"
        assert f.get_pool_source("20260616") == "cache_memory"
