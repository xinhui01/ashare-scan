"""测试 StockDataFetcher._last_pool_source 跟踪 + get_pool_source 公开接口。"""
from __future__ import annotations

import pandas as pd
import pytest
from unittest.mock import patch

from stock_data import StockDataFetcher


def _build_fetcher():
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._limit_up_pool_cache = {}
    instance._prev_limit_up_pool_cache = {}
    instance._last_pool_source = {}
    instance._last_prev_pool_source = {}
    return instance


class TestPoolSourceTracking:
    def test_get_pool_source_unknown_by_default(self):
        f = _build_fetcher()
        assert f.get_pool_source("20260520") == "unknown"
        assert f.get_pool_source("20260520", previous=True) == "unknown"

    def test_memory_cache_hit_source(self, monkeypatch):
        f = _build_fetcher()
        f._limit_up_pool_cache["20260520"] = pd.DataFrame([{"代码": "600000"}])
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "cache_memory"

    def test_db_cache_hit_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr(
            f,
            "_sanitize_limit_up_pool",
            lambda df: df,
        )
        monkeypatch.setattr(
            "stock_store.load_limit_up_pool",
            lambda *args, **kwargs: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "cache_db"

    def test_eastmoney_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr(
            f,
            "_sanitize_limit_up_pool",
            lambda df: df,
        )
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            "stock_data._retry_ak_call",
            lambda _fn, date=None: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "eastmoney"

    def test_spot_fallback_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr(
            f,
            "_sanitize_limit_up_pool",
            lambda df: df,
        )
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)  # 熔断
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr("stock_store.save_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(f, "_recent_trade_dates", lambda d, n: ["20260519", "20260520"])
        # mock 派生返回非空
        monkeypatch.setattr(
            f, "_derive_limit_up_pool_from_spot",
            lambda *args, **kwargs: pd.DataFrame([{"代码": "600000", "连板数": 1, "最新价": 11.0, "涨跌幅": 10.0}]),
        )
        f.get_limit_up_pool("20260520")
        assert f.get_pool_source("20260520") == "spot_fallback"

    def test_empty_source(self, monkeypatch):
        f = _build_fetcher()
        monkeypatch.setattr(f, "_normalize_trade_date", lambda d: "20260520")
        monkeypatch.setattr(
            f,
            "_sanitize_limit_up_pool",
            lambda df: df,
        )
        monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
        monkeypatch.setattr("stock_store.load_limit_up_pool", lambda *args, **kwargs: None)
        monkeypatch.setattr(
            "stock_data._retry_ak_call",
            lambda _fn, date=None: pd.DataFrame(),  # 东财返空
        )
        df = f.get_limit_up_pool("20260520")
        assert df.empty
        assert f.get_pool_source("20260520") == "empty"
