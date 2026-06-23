import builtins
import os
import sys
import types

import pandas as pd

from data_source_models import DATA_SOURCE_OPTIONS
from stock_data import StockDataFetcher


def _fetcher() -> StockDataFetcher:
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = lambda msg: None
    instance._default_history_source = "auto"
    return instance


def test_finshare_is_available_history_source():
    assert "finshare" in DATA_SOURCE_OPTIONS["history"]


def test_auto_history_plan_does_not_include_finshare_by_default(monkeypatch):
    fetcher = _fetcher()
    monkeypatch.setattr(
        fetcher,
        "get_available_history_mirrors",
        lambda force_refresh=False: ["https://em.example/api"],
    )
    monkeypatch.setattr("stock_data._eastmoney_circuit_breaker_open", lambda: False)
    monkeypatch.setattr("stock_data._global_host_on_cooldown", lambda host: False)
    monkeypatch.setattr("stock_data._tdx_source_available", lambda: True)

    plan = fetcher.build_history_request_plan(source="auto", force_refresh=True)

    assert "finshare" not in plan.provider_sequence


def test_finshare_normalization_produces_unified_history_fields():
    from src.sources.finshare import normalize_finshare_history_frame

    raw = pd.DataFrame(
        [
            {
                "trade_date": "2026-04-21",
                "open_price": "10.00",
                "high_price": "10.80",
                "low_price": "9.90",
                "close_price": "10.50",
                "volume": "123456",
                "amount": "129628800.00",
                "amplitude": "9.00",
                "change_pct": "5.00",
                "change_amount": "0.50",
                "turnover_rate": "1.23",
            }
        ]
    )

    out = normalize_finshare_history_frame(raw)

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


def test_finshare_fetch_uses_market_suffix_and_date_range(monkeypatch):
    from src.sources import finshare

    calls = {}

    def fake_get_historical_data(code, start=None, end=None, period="daily", adjust=None):
        calls.update(
            {
                "code": code,
                "start": start,
                "end": end,
                "period": period,
                "adjust": adjust,
            }
        )
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-04-21",
                    "open_price": 10.0,
                    "high_price": 10.8,
                    "low_price": 9.9,
                    "close_price": 10.5,
                    "amount": 1000.0,
                }
            ]
        )

    monkeypatch.setattr(finshare, "throttle", lambda: None)
    monkeypatch.setitem(
        sys.modules,
        "finshare",
        types.SimpleNamespace(get_historical_data=fake_get_historical_data),
    )

    out = finshare.fetch_hist_frame("600000", "20260401", "20260421")

    assert calls == {
        "code": "600000.SH",
        "start": "2026-04-01",
        "end": "2026-04-21",
        "period": "daily",
        "adjust": None,
    }
    assert len(out) == 1
    assert out.iloc[0]["close"] == 10.5


def test_finshare_import_uses_project_runtime_dir(monkeypatch, tmp_path):
    from src.sources import finshare

    captured = {}
    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "finshare":
            captured.update(
                {
                    "USERPROFILE": os.environ.get("USERPROFILE"),
                    "HOME": os.environ.get("HOME"),
                    "APPDATA": os.environ.get("APPDATA"),
                }
            )
            return types.SimpleNamespace(
                get_historical_data=lambda *args, **kwargs: pd.DataFrame(
                    [
                            {
                                "trade_date": "2026-04-21",
                                "open_price": 10.0,
                                "high_price": 10.8,
                                "low_price": 9.9,
                                "close_price": 10.5,
                                "amount": 1000.0,
                            }
                    ]
                )
            )
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setenv("USERPROFILE", r"C:\blocked-home")
    monkeypatch.setenv("HOME", r"C:\blocked-home")
    monkeypatch.setenv("APPDATA", r"C:\blocked-appdata")
    monkeypatch.setenv("ASHARE_FINSHARE_RUNTIME_DIR", str(tmp_path))
    monkeypatch.delitem(sys.modules, "finshare", raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(finshare, "throttle", lambda: None)

    out = finshare.fetch_hist_frame("000001", "20260401", "20260421")

    assert len(out) == 1
    assert captured == {
        "USERPROFILE": str(tmp_path),
        "HOME": str(tmp_path),
        "APPDATA": str(tmp_path / "AppData" / "Roaming"),
    }
    assert os.environ["USERPROFILE"] == r"C:\blocked-home"
    assert os.environ["HOME"] == r"C:\blocked-home"
    assert os.environ["APPDATA"] == r"C:\blocked-appdata"


def test_get_history_data_uses_finshare_provider(monkeypatch):
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
    monkeypatch.setattr("stock_data._fetch_finshare_hist_frame", lambda *args: df)
    monkeypatch.setattr("stock_data._save_history_store", lambda *args, **kwargs: None)
    monkeypatch.setattr("stock_data.save_history_meta_store", lambda *args, **kwargs: None)

    plan = fetcher.build_history_request_plan(source="finshare", force_refresh=True)
    out = fetcher.get_history_data("600000", days=1, force_refresh=True, request_plan=plan)

    assert out is not None
    assert len(out) == 1
