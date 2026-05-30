import pandas as pd

from data_source_models import DATA_SOURCE_OPTIONS
from stock_data import StockDataFetcher


def _fetcher() -> StockDataFetcher:
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._default_history_source = "auto"
    return instance


def test_baostock_is_available_history_source():
    assert "baostock" in DATA_SOURCE_OPTIONS["history"]


def test_auto_history_plans_include_baostock(monkeypatch):
    fetcher = _fetcher()
    monkeypatch.setattr(fetcher, "get_available_history_mirrors", lambda: [])
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: True)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)

    plans = fetcher._build_multi_source_plans("auto")

    assert any(plan.provider_sequence == ("baostock",) for plan in plans)


def test_baostock_normalization_produces_unified_history_fields():
    from src.sources.baostock import normalize_baostock_history_frame

    raw = pd.DataFrame(
        [
            {
                "date": "2026-04-21",
                "code": "sh.600000",
                "open": "10.00",
                "high": "10.80",
                "low": "9.90",
                "close": "10.50",
                "preclose": "10.00",
                "volume": "123456",
                "amount": "129628800.00",
                "turn": "1.23",
                "pctChg": "5.00",
                "tradestatus": "1",
            }
        ]
    )

    out = normalize_baostock_history_frame(raw)

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
    assert out.iloc[0]["amount"] == 129628800.00
    assert out.iloc[0]["turnover_rate"] == 1.23
    assert out.iloc[0]["change_pct"] == 5.00
    assert round(float(out.iloc[0]["change_amount"]), 2) == 0.50


def test_get_history_data_uses_baostock_provider(monkeypatch):
    fetcher = _fetcher()
    df = pd.DataFrame(
        [
            {
                "date": "2026-04-21",
                "open": 10.0,
                "high": 10.8,
                "low": 9.9,
                "close": 10.5,
                "amount": 1000.0,
            }
        ]
    )
    monkeypatch.setattr("stock_data._fetch_baostock_hist_frame", lambda *args: df)
    monkeypatch.setattr("stock_data._save_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr("stock_data.save_history_meta_store", lambda *args, **kwargs: None)

    plan = fetcher.build_history_request_plan(source="baostock", force_refresh=True)
    out = fetcher.get_history_data("600000", days=1, force_refresh=True, request_plan=plan)

    assert out is not None
    assert len(out) == 1
