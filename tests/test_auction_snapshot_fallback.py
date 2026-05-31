import pandas as pd

from stock_data import StockDataFetcher
from src.sources.auction_snapshot import snapshot_from_intraday_frame


def _fetcher():
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = None
    return instance


def test_snapshot_from_sina_intraday_uses_real_925_row():
    raw = pd.DataFrame(
        [
            {
                "day": "2026-06-01 09:24:00",
                "open": "10.10",
                "high": "10.10",
                "low": "10.10",
                "close": "10.10",
                "volume": "2000",
            },
            {
                "day": "2026-06-01 09:25:00",
                "open": "10.28",
                "high": "10.35",
                "low": "10.20",
                "close": "10.32",
                "volume": "5000",
            },
        ]
    )

    snapshot = snapshot_from_intraday_frame(raw, stock_code="600000", source="sina")

    assert snapshot is not None
    assert snapshot["trade_date"] == "2026-06-01"
    assert snapshot["source"] == "sina"
    assert snapshot["price"] == 10.32
    assert snapshot["open"] == 10.28
    assert snapshot["high"] == 10.35
    assert snapshot["low"] == 10.20
    assert snapshot["volume"] == 5000.0
    assert snapshot["amount"] is None


def test_snapshot_from_intraday_ignores_non_925_rows():
    raw = pd.DataFrame(
        [
            {
                "day": "2026-06-01 09:30:00",
                "open": "10.28",
                "high": "10.35",
                "low": "10.20",
                "close": "10.32",
                "volume": "5000",
            },
        ]
    )

    assert snapshot_from_intraday_frame(raw, stock_code="600000", source="sina") is None


def test_get_auction_snapshot_falls_back_to_sina_925(monkeypatch):
    raw = pd.DataFrame(
        [
            {
                "day": "2026-06-01 09:25:00",
                "open": "10.28",
                "high": "10.35",
                "low": "10.20",
                "close": "10.32",
                "volume": "5000",
            },
        ]
    )
    calls = []

    def fake_retry(fn, *args, **kwargs):
        calls.append(fn.__name__)
        return fn(*args, **kwargs)

    def fake_eastmoney(*args, **kwargs):
        return None

    def fake_sina(*args, **kwargs):
        return raw

    monkeypatch.setattr("stock_data._retry_ak_call", fake_retry)
    monkeypatch.setattr("stock_data._fetch_eastmoney_auction_snapshot", fake_eastmoney)
    monkeypatch.setattr("stock_data._fetch_sina_intraday_1min", fake_sina)

    snapshot = _fetcher().get_auction_snapshot("600000")

    assert snapshot is not None
    assert snapshot["source"] == "sina"
    assert snapshot["price"] == 10.32
    assert calls == ["fake_eastmoney", "fake_sina"]


def test_get_auction_snapshot_prefers_eastmoney_when_available(monkeypatch):
    sina_called = False

    def fake_retry(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def fake_eastmoney(*args, **kwargs):
        return {"trade_date": "2026-06-01", "price": 10.30}

    def fake_sina(*args, **kwargs):
        nonlocal sina_called
        sina_called = True
        return pd.DataFrame()

    monkeypatch.setattr("stock_data._retry_ak_call", fake_retry)
    monkeypatch.setattr("stock_data._fetch_eastmoney_auction_snapshot", fake_eastmoney)
    monkeypatch.setattr("stock_data._fetch_sina_intraday_1min", fake_sina)

    snapshot = _fetcher().get_auction_snapshot("600000")

    assert snapshot == {"trade_date": "2026-06-01", "price": 10.30, "source": "eastmoney"}
    assert sina_called is False
