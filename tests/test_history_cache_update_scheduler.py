import threading
import time
from pathlib import Path
from unittest import mock

import pandas as pd

from data_source_models import HistoryRequestPlan
from stock_data import StockDataFetcher
from src.utils.trade_calendar import SyncTarget, TradePhase


def _fetcher_with_universe(universe: pd.DataFrame) -> StockDataFetcher:
    fetcher = StockDataFetcher.__new__(StockDataFetcher)
    fetcher._log = lambda msg: None
    fetcher._default_history_source = "auto"
    fetcher.get_all_stocks = lambda force_refresh=False: universe.copy()  # type: ignore[method-assign]
    return fetcher


def test_update_history_cache_limits_parallelism_per_source(monkeypatch):
    universe = pd.DataFrame(
        {"code": [f"00000{i}" for i in range(1, 10)], "name": [f"S{i}" for i in range(1, 10)]}
    )
    fetcher = _fetcher_with_universe(universe)
    plans = [
        HistoryRequestPlan(mode="network", provider_sequence=("sina",), reason="multi-source-sina"),
        HistoryRequestPlan(mode="network", provider_sequence=("netease",), reason="multi-source-netease"),
        HistoryRequestPlan(mode="network", provider_sequence=("sohu",), reason="multi-source-sohu"),
    ]
    fetcher._build_multi_source_plans = lambda source: plans  # type: ignore[method-assign]

    active = {name: 0 for name in ("sina", "netease", "sohu")}
    max_active = {name: 0 for name in active}
    lock = threading.Lock()

    def fake_get_history_data(code, *, request_plan, **kwargs):
        provider = request_plan.provider_sequence[0]
        with lock:
            active[provider] += 1
            max_active[provider] = max(max_active[provider], active[provider])
        time.sleep(0.03)
        with lock:
            active[provider] -= 1
        return pd.DataFrame(
            [{"date": "2026-04-22", "open": 1, "close": 1, "high": 1, "low": 1}]
        )

    fetcher.get_history_data = fake_get_history_data  # type: ignore[method-assign]
    monkeypatch.setattr("stock_data._is_history_cache_fresh", lambda *args, **kwargs: False)
    monkeypatch.setattr("stock_data._history_per_source_concurrency", lambda: 1, raising=False)
    monkeypatch.setattr("stock_data._history_total_concurrency_cap", lambda: 12, raising=False)

    result = fetcher.update_history_cache(
        days=1,
        workers=9,
        source="auto",
        fast_daily_append=False,
    )

    assert result["updated"] == 9
    assert max(max_active.values()) == 1
    assert sum(1 for value in max_active.values() if value == 1) == 3


def test_non_eastmoney_history_sources_do_not_use_global_em_semaphore(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    plan = HistoryRequestPlan(mode="network", provider_sequence=("sina",), reason="test-sina")
    df = pd.DataFrame(
        [{"date": "2026-04-22", "open": 10, "close": 11, "high": 12, "low": 9, "amount": 1000}]
    )

    monkeypatch.setattr("stock_data._fetch_sina_hist_frame", lambda *args, **kwargs: df)
    monkeypatch.setattr(
        "stock_data._history_retry_ak_call",
        mock.Mock(side_effect=AssertionError("non-EM source should not use EM semaphore")),
    )
    monkeypatch.setattr("stock_data._save_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr("stock_data.save_history_meta_store", lambda *args, **kwargs: None)

    result = fetcher.get_history_data("000001", days=1, force_refresh=True, request_plan=plan)

    assert result is not None
    assert len(result) == 1


def test_update_history_cache_fast_appends_snapshot_before_network(tmp_path, monkeypatch):
    import stock_store
    import src.utils.cache_freshness as cache_freshness

    monkeypatch.setattr(stock_store, "_DATA_DIR", Path(tmp_path))
    monkeypatch.setattr(stock_store, "_DB_PATH", Path(tmp_path) / "test.sqlite3")
    stock_store._SCHEMA_INITIALIZED = False
    stock_store._SCHEMA_INITIALIZED_PATH = ""
    stock_store.reset_all_connections()
    stock_store.ensure_store_ready()

    universe = pd.DataFrame(
        {"code": ["000001", "000002"], "name": ["A", "B"], "board": ["主板", "主板"]}
    )
    fetcher = _fetcher_with_universe(universe)
    fetcher._fetch_history_cache_spot_snapshot = lambda: pd.DataFrame(  # type: ignore[attr-defined]
        [
            {
                "代码": "000001",
                "名称": "A",
                "最新价": 10.5,
                "今开": 10.0,
                "最高": 10.8,
                "最低": 9.9,
                "成交量": 1000,
                "成交额": 1050000,
            },
            {
                "代码": "000002",
                "名称": "B",
                "最新价": 20.5,
                "今开": 20.0,
                "最高": 20.8,
                "最低": 19.9,
                "成交量": 2000,
                "成交额": 2050000,
            },
        ]
    )
    target = SyncTarget("2026-04-22", TradePhase.CLOSED, calendar_degraded=False)
    monkeypatch.setattr("stock_data.resolve_sync_target_trade_date", lambda: target, raising=False)
    monkeypatch.setattr(cache_freshness, "estimate_last_trade_date", lambda: "2026-04-22")

    network_calls = []

    def fake_get_history_data(code, **kwargs):
        network_calls.append(code)
        return pd.DataFrame(
            [{"date": "2026-04-22", "open": 1, "close": 1, "high": 1, "low": 1}]
        )

    fetcher.get_history_data = fake_get_history_data  # type: ignore[method-assign]

    result = fetcher.update_history_cache(
        days=1,
        workers=2,
        source="auto",
        fast_daily_append=True,
    )

    assert network_calls == []
    assert result["snapshot_appended"] == 2
    assert result["skipped"] == 2
    assert stock_store.load_history("000001") is not None
