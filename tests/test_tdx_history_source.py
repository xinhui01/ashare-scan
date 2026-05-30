import pandas as pd

from data_source_models import DATA_SOURCE_OPTIONS
from stock_data import StockDataFetcher


def _fetcher() -> StockDataFetcher:
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._default_history_source = "auto"
    return instance


def test_tdx_is_available_history_source():
    assert "tdx" in DATA_SOURCE_OPTIONS["history"]


def test_auto_history_plans_include_tdx_when_backend_available(monkeypatch):
    fetcher = _fetcher()
    monkeypatch.setattr(fetcher, "get_available_history_mirrors", lambda: [])
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: True)

    plans = fetcher._build_multi_source_plans("auto")

    assert any(plan.provider_sequence == ("tdx",) for plan in plans)


def test_auto_history_plans_skip_tdx_when_backend_missing(monkeypatch):
    fetcher = _fetcher()
    monkeypatch.setattr(fetcher, "get_available_history_mirrors", lambda: [])
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: False)

    plans = fetcher._build_multi_source_plans("auto")

    assert all(plan.provider_sequence != ("tdx",) for plan in plans)


def test_tdx_normalization_produces_unified_history_fields():
    from src.sources.tdx import normalize_tdx_history_bars

    class Bar:
        year = 2026
        month = 4
        day = 21
        open = 10.0
        close = 10.5
        high = 10.8
        low = 9.9
        vol = 123456.0
        amount = 129628800.0

    out = normalize_tdx_history_bars([Bar()])

    assert list(out.columns) == [
        "date",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "change_pct",
        "change_amount",
        "turnover_rate",
    ]
    assert out.iloc[0]["date"] == "2026-04-21"
    assert out.iloc[0]["amount"] == 129628800.0


def test_get_history_data_uses_tdx_provider(monkeypatch):
    fetcher = _fetcher()
    df = pd.DataFrame(
        [
            {
                "date": "2026-04-21",
                "open": 10.0,
                "high": 10.8,
                "low": 9.9,
                "close": 10.5,
                "volume": 123456.0,
                "amount": 129628800.0,
            }
        ]
    )
    monkeypatch.setattr("stock_data._fetch_tdx_hist_frame", lambda *args: df)
    monkeypatch.setattr("stock_data._save_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr("stock_data.save_history_meta_store", lambda *args, **kwargs: None)

    plan = fetcher.build_history_request_plan(source="tdx", force_refresh=True)
    out = fetcher.get_history_data("600000", days=1, force_refresh=True, request_plan=plan)

    assert out is not None
    assert len(out) == 1
