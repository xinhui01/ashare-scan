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
    fetcher._build_multi_source_plans = lambda source, excluded_providers=None: plans  # type: ignore[method-assign]

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


def test_history_cache_workers_do_not_expand_to_source_channel_count(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    plans = [
        HistoryRequestPlan(mode="network", provider_sequence=(provider,), reason=f"multi-source-{provider}")
        for provider in ("sina", "netease", "sohu", "ths", "wscn", "baostock")
    ]

    monkeypatch.setattr("stock_data._history_per_source_concurrency", lambda: 1, raising=False)
    monkeypatch.setattr("stock_data._history_total_concurrency_cap", lambda: 12, raising=False)
    monkeypatch.setattr("stock_data._history_request_concurrency", lambda: 2, raising=False)

    worker_count, per_source_limit, channel_count = fetcher._resolve_history_update_worker_count(
        requested_workers=9,
        plans=plans,
    )

    assert channel_count == 6
    assert per_source_limit == 1
    assert worker_count == 2


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


def test_non_eastmoney_history_sources_use_single_inflight_slot(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    provider_to_host = {
        "sina": "finance.sina.com.cn",
        "netease": "quotes.money.163.com",
        "sohu": "q.stock.sohu.com",
        "ths": "d.10jqka.com.cn",
        "wscn": "api-ddc-wscn.awtmt.com",
        "baostock": "baostock.com",
        "tdx": "tdx",
    }
    seen_limits = {}

    class _DummyLimit:
        def __init__(self, host, default_limit):
            seen_limits[host] = default_limit

        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr("stock_data._limit_host_inflight", lambda host, default_limit=2: _DummyLimit(host, default_limit))
    monkeypatch.setattr("stock_data._save_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr("stock_data.save_history_meta_store", lambda *args, **kwargs: None)

    for provider in provider_to_host:
        df = pd.DataFrame(
            [{"date": "2026-04-22", "open": 10, "close": 11, "high": 12, "low": 9, "amount": 1000}]
        )
        monkeypatch.setattr(f"stock_data._fetch_{provider}_hist_frame", lambda *args, _df=df, **kwargs: _df)
        plan = HistoryRequestPlan(mode="network", provider_sequence=(provider,), reason=f"test-{provider}")
        result = fetcher.get_history_data("000001", days=1, force_refresh=True, request_plan=plan)
        assert result is not None

    assert seen_limits == {host: 1 for host in provider_to_host.values()}


def test_auto_history_batch_prefers_eastmoney_and_keeps_fallback_inline(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    monkeypatch.setattr(
        fetcher,
        "get_available_history_mirrors",
        lambda force_refresh=False: ["https://em1.example/api", "https://em2.example/api"],
    )
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: True)

    plans = fetcher._build_multi_source_plans("auto")

    assert len(plans) == 2
    assert all(plan.provider_sequence[0] == "eastmoney" for plan in plans)
    assert all(plan.provider_sequence != ("sina",) for plan in plans)
    assert all("sina" in plan.provider_sequence for plan in plans)


def test_auto_history_plan_excludes_sina_from_default_fallback_chain(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    monkeypatch.setattr(
        fetcher,
        "get_available_history_mirrors",
        lambda force_refresh=False: ["https://em1.example/api"],
    )
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: True)

    plan = fetcher.build_history_request_plan(source="auto", force_refresh=True)

    assert plan.provider_sequence[0] == "eastmoney"
    assert "sina" in plan.provider_sequence


def test_auto_history_batch_can_temporarily_exclude_failed_providers(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    monkeypatch.setattr(
        fetcher,
        "get_available_history_mirrors",
        lambda force_refresh=False: ["https://em1.example/api", "https://em2.example/api"],
    )
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: True)

    plans = fetcher._build_multi_source_plans("auto", excluded_providers={"sina", "sohu"})

    assert len(plans) == 2
    assert all(plan.provider_sequence[0] == "eastmoney" for plan in plans)
    assert all("sina" not in plan.provider_sequence for plan in plans)
    assert all("sohu" not in plan.provider_sequence for plan in plans)
    assert all("ths" in plan.provider_sequence for plan in plans)


def test_auto_history_batch_returns_cache_only_when_all_fallbacks_excluded(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    monkeypatch.setattr(fetcher, "get_available_history_mirrors", lambda: [])
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: False)

    plans = fetcher._build_multi_source_plans(
        "auto",
        excluded_providers={"sina", "ths", "netease", "sohu", "wscn", "baostock"},
    )

    assert len(plans) == 1
    assert plans[0].cache_only
    assert plans[0].provider_sequence == ()
    assert "no-healthy-fallback" in plans[0].reason


def test_auto_history_plan_cache_only_when_eastmoney_open_and_fallbacks_cooling(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    cooldown_hosts = {
        "finance.sina.com.cn",
        "d.10jqka.com.cn",
        "quotes.money.163.com",
        "q.stock.sohu.com",
        "api-ddc-wscn.awtmt.com",
        "baostock.com",
    }

    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: host in cooldown_hosts)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: False)

    plan = fetcher.build_history_request_plan(source="auto", force_refresh=True)

    assert plan.cache_only
    assert plan.provider_sequence == ()
    assert "no-healthy-fallback" in plan.reason


def test_auto_history_plan_filters_cooling_fallback_provider(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())

    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: host == "baostock.com")
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: False)

    plan = fetcher.build_history_request_plan(source="auto", force_refresh=True)

    assert not plan.cache_only
    assert "baostock" not in plan.provider_sequence
    assert "sina" in plan.provider_sequence


def test_explicit_cooling_history_provider_uses_cache_only(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())

    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: host == "baostock.com")

    plan = fetcher.build_history_request_plan(source="baostock", force_refresh=True)

    assert plan.cache_only
    assert plan.provider_sequence == ("baostock",)


def test_update_history_cache_uses_probe_results_to_filter_auto_sources(monkeypatch):
    universe = pd.DataFrame({"code": ["000001"], "name": ["A"]})
    fetcher = _fetcher_with_universe(universe)
    seen = {}

    monkeypatch.setattr("stock_data._is_history_cache_fresh", lambda *args, **kwargs: False)
    monkeypatch.setattr(fetcher, "_probe_auto_history_providers", lambda rows, days: {"sina", "sohu"})

    def fake_build_plans(source, excluded_providers=None):
        seen["source"] = source
        seen["excluded"] = set(excluded_providers or set())
        return [HistoryRequestPlan(mode="network", provider_sequence=("eastmoney",), reason="em")]

    fetcher._build_multi_source_plans = fake_build_plans  # type: ignore[method-assign]
    fetcher.get_history_data = lambda *args, **kwargs: pd.DataFrame(  # type: ignore[method-assign]
        [{"date": "2026-04-22", "open": 1, "close": 1, "high": 1, "low": 1}]
    )

    result = fetcher.update_history_cache(days=1, workers=1, source="auto", fast_daily_append=False)

    assert result["updated"] == 1
    assert seen == {"source": "auto", "excluded": {"sina", "sohu"}}


def test_update_history_cache_drops_bse_before_network(monkeypatch):
    universe = pd.DataFrame(
        {
            "code": ["920225", "000001", "830799"],
            "name": ["BSE-A", "A", "BSE-B"],
            "board": ["北交所", "主板", "北交所"],
        }
    )
    fetcher = _fetcher_with_universe(universe)
    called_codes = []

    monkeypatch.setattr("stock_data._is_history_cache_fresh", lambda *args, **kwargs: False)

    def fake_get_history_data(code, **kwargs):
        called_codes.append(code)
        return pd.DataFrame(
            [{"date": "2026-04-22", "open": 1, "close": 1, "high": 1, "low": 1}]
        )

    fetcher.get_history_data = fake_get_history_data  # type: ignore[method-assign]

    result = fetcher.update_history_cache(
        days=1,
        workers=1,
        source="sina",
        fast_daily_append=False,
    )

    assert called_codes == ["000001"]
    assert result["total"] == 1
    assert result["updated"] == 1


def test_auto_provider_probe_ignores_bse_sample_codes(monkeypatch):
    fetcher = _fetcher_with_universe(pd.DataFrame())
    seen_codes = []

    monkeypatch.setattr("stock_data._tdx_source_available", lambda: False)

    def fake_get_history_data(code, **kwargs):
        seen_codes.append(code)
        return pd.DataFrame(
            [{"date": "2026-04-22", "open": 1, "close": 1, "high": 1, "low": 1}]
        )

    fetcher.get_history_data = fake_get_history_data  # type: ignore[method-assign]

    excluded = fetcher._probe_auto_history_providers(
        [{"code": "920225"}, {"code": "830799"}, {"code": "000001"}],
        days=20,
    )

    assert excluded == set()
    assert seen_codes == ["000001"] * 6


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
