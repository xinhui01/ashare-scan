import requests
import pandas as pd

import stock_data
from stock_data import StockDataFetcher
from src.network import host_health
from src.sources.eastmoney import intraday as em_intraday
from src.sources.auction_snapshot import snapshot_from_intraday_frame
from src.utils import em_circuit_breaker


def _fetcher():
    instance = StockDataFetcher.__new__(StockDataFetcher)
    instance._log = None
    return instance


def _reset_eastmoney_auction_state(monkeypatch):
    host_health._HOST_HEALTH.clear()
    host_health._HOST_FAIL_COUNT.clear()
    host_health._HOST_INFLIGHT_SEMAPHORES.clear()
    em_circuit_breaker.reset()
    stock_data._clear_intraday_mem_cache()
    monkeypatch.setattr(em_intraday, "_AUCTION_NEXT_REQUEST_AT", 0.0)
    monkeypatch.setattr(em_intraday, "_auction_min_interval_sec", lambda: 0.01)
    monkeypatch.setattr(em_intraday, "_auction_timeout", lambda: (0.5, 0.5))


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


def test_get_intraday_data_derives_auction_from_eastmoney_raw(monkeypatch):
    _reset_eastmoney_auction_state(monkeypatch)
    raw = pd.DataFrame(
        [
            {
                "时间": "2026-06-04 09:25:00",
                "开盘": "10.20",
                "收盘": "10.32",
                "最高": "10.35",
                "最低": "10.20",
                "成交量": "5000",
                "成交额": "52000",
                "均价": "10.30",
            },
            {
                "时间": "2026-06-04 09:30:00",
                "开盘": "10.40",
                "收盘": "10.45",
                "最高": "10.50",
                "最低": "10.38",
                "成交量": "8000",
                "成交额": "83000",
                "均价": "10.42",
            },
        ]
    )
    sina_called = False

    def fake_retry(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def fake_intraday(*args, **kwargs):
        return raw

    def fake_auction(*args, **kwargs):
        return None

    def fake_sina(*args, **kwargs):
        nonlocal sina_called
        sina_called = True
        return pd.DataFrame()

    monkeypatch.setattr("stock_data._today_ymd", lambda: "2026-06-04")
    monkeypatch.setattr("stock_data._intraday_market_closed_now", lambda: False)
    monkeypatch.setattr("stock_data._retry_ak_call", fake_retry)
    monkeypatch.setattr("stock_data._fetch_eastmoney_intraday_1min", fake_intraday)
    monkeypatch.setattr("stock_data._fetch_eastmoney_auction_snapshot", fake_auction)
    monkeypatch.setattr("stock_data._fetch_sina_intraday_1min", fake_sina)

    payload = StockDataFetcher().get_intraday_data(
        "600000",
        source="eastmoney",
        include_meta=True,
    )

    assert payload["auction"] is not None
    assert payload["auction"]["source"] == "eastmoney_intraday"
    assert payload["auction"]["price"] == 10.32
    assert sina_called is False


def test_get_intraday_data_falls_back_to_sina_auction(monkeypatch):
    _reset_eastmoney_auction_state(monkeypatch)
    eastmoney_raw = pd.DataFrame(
        [
            {
                "时间": "2026-06-04 09:30:00",
                "开盘": "10.40",
                "收盘": "10.45",
                "最高": "10.50",
                "最低": "10.38",
                "成交量": "8000",
                "成交额": "83000",
                "均价": "10.42",
            },
        ]
    )
    sina_raw = pd.DataFrame(
        [
            {
                "day": "2026-06-04 09:25:00",
                "open": "10.20",
                "high": "10.35",
                "low": "10.20",
                "close": "10.32",
                "volume": "5000",
            },
        ]
    )

    def fake_retry(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    def fake_intraday(*args, **kwargs):
        return eastmoney_raw

    def fake_auction(*args, **kwargs):
        return None

    def fake_sina(*args, **kwargs):
        return sina_raw

    monkeypatch.setattr("stock_data._today_ymd", lambda: "2026-06-04")
    monkeypatch.setattr("stock_data._intraday_market_closed_now", lambda: False)
    monkeypatch.setattr("stock_data._retry_ak_call", fake_retry)
    monkeypatch.setattr("stock_data._fetch_eastmoney_intraday_1min", fake_intraday)
    monkeypatch.setattr("stock_data._fetch_eastmoney_auction_snapshot", fake_auction)
    monkeypatch.setattr("stock_data._fetch_sina_intraday_1min", fake_sina)

    payload = StockDataFetcher().get_intraday_data(
        "600000",
        source="eastmoney",
        include_meta=True,
    )

    assert payload["auction"] is not None
    assert payload["auction"]["source"] == "sina"
    assert payload["auction"]["price"] == 10.32


def test_eastmoney_auction_failure_enters_cooldown(monkeypatch):
    _reset_eastmoney_auction_state(monkeypatch)
    logs = []

    class FailingSession:
        calls = 0
        trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, **kwargs):
            FailingSession.calls += 1
            raise requests.exceptions.ConnectionError("RemoteDisconnected")

    monkeypatch.setattr(requests, "Session", FailingSession)

    assert em_intraday.fetch_auction_snapshot("600000", logger=logs.append) is None
    assert FailingSession.calls >= 1
    assert logs

    calls_after_first = FailingSession.calls
    logs.clear()
    assert em_intraday.fetch_auction_snapshot("000001", logger=logs.append) is None
    assert FailingSession.calls == calls_after_first
    assert logs == []


def test_eastmoney_auction_success_parses_925_snapshot(monkeypatch):
    _reset_eastmoney_auction_state(monkeypatch)

    class FakeResponse:
        status_code = 200
        text = '{"data":{"trends":[]}}'

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "trends": [
                        "2026-06-04 09:24,10.00,10.01,10.02,9.99,1000,10000,10.00",
                        "2026-06-04 09:25,10.20,10.32,10.35,10.20,5000,52000,10.30",
                    ]
                }
            }

    class SuccessfulSession:
        trust_env = True

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get(self, **kwargs):
            assert kwargs["headers"]["Connection"] == "close"
            return FakeResponse()

    monkeypatch.setattr(requests, "Session", SuccessfulSession)

    snapshot = em_intraday.fetch_auction_snapshot("600000")

    assert snapshot is not None
    assert snapshot["trade_date"] == "2026-06-04"
    assert snapshot["price"] == 10.32
    assert snapshot["amount"] == 52000.0
